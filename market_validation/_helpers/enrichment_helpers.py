"""Free-tier enrichment helpers.

These do not make AI calls — they rely on web scraping, regex extraction,
MX verification, and plain search snippets. The AI-driven tier lives in
``services.enrichment``.
"""

from __future__ import annotations

import re
from typing import Any

from market_validation._helpers.queries import try_multi_search
from market_validation.log import get_logger

_log = get_logger("enrichment_helpers")


def email_source_label(source: str) -> str:
    """Human-readable label for an email-source key (used in company notes)."""
    return {
        "scraped": "Email source: scraped from website",
        "search": "Email source: found via search results",
        "adaptive_search_mx": "Email source: found via targeted search (MX verified)",
        "adaptive_search": "Email source: found via targeted search (unverified)",
        "adaptive_person_guess_mx": "Email source: GUESSED from contact name + domain MX (not verified as real mailbox)",
        "adaptive_generic_guess_mx": "Email source: GUESSED as info@domain (MX valid, but mailbox may not exist — verify before sending)",
    }.get(source, f"Email source: {source}")


def free_enrich_company(
    company_name: str,
    website: str | None,
    location: str | None,
    existing_notes: str | None = None,
) -> dict[str, Any]:
    """
    Tier 1+2 enrichment: free methods only, no AI calls.

    Tier 1 — website scraping + email pattern generation.
    Tier 2 — DuckDuckGo search for contact info.

    Returns a dict with keys: emails, phones, contacts, address, sources, tier.
    """
    from market_validation.web_scraper import (
        _extract_all_emails,
        _extract_all_phones,
        scrape_contact_info,
    )

    emails: list[str] = []
    phones: list[str] = []
    contacts: list[dict[str, str]] = []
    address = ""
    sources: list[str] = []
    # Track the source of each email: email_address -> "scraped" | "pattern" | "search"
    email_sources: dict[str, str] = {}

    # --- Tier 1a: Scrape the website (homepage + /contact, /about, etc.) ---
    if website:
        try:
            scraped = scrape_contact_info(website, delay=1.0)
            if scraped.get("emails"):
                for _em in scraped["emails"]:
                    email_sources.setdefault(_em.lower(), "scraped")
                emails.extend(scraped["emails"])
                sources.append("website_scrape")
            if scraped.get("phones"):
                phones.extend(scraped["phones"])
                if "website_scrape" not in sources:
                    sources.append("website_scrape")
            if scraped.get("address"):
                address = scraped["address"]
        except Exception as exc:
            # Website scraping is best-effort — most failures are network
            # (timeouts, 403s) or unparseable HTML. Debug level so long runs
            # aren't noisy; operators can flip to DEBUG when diagnosing.
            _log.debug("scrape_contact_info failed for %s: %s", website, exc)

    # --- Tier 1b: (removed — no pattern-generated emails, only real scraped ones) ---

    # --- Tier 1c: Extract from existing notes/snippets ---
    if existing_notes:
        note_emails = _extract_all_emails(existing_notes)
        note_phones = _extract_all_phones(existing_notes)
        if note_emails:
            for _em in note_emails:
                email_sources.setdefault(_em.lower(), "scraped")
            emails.extend(note_emails)
            sources.append("existing_notes")
        if note_phones:
            phones.extend(note_phones)
            if "existing_notes" not in sources:
                sources.append("existing_notes")

    # --- Tier 2: DuckDuckGo search (free, no AI) ---
    if not emails or not phones:
        loc_str = f" {location}" if location else ""
        query = f'"{company_name}"{loc_str} email phone contact'
        try:
            from market_validation.company_enrichment import is_plausible_email
            results = try_multi_search(query, num_results=5)
            for r in results:
                snippet = f"{r.get('title', '')} {r.get('snippet', '')} {r.get('url', '')}"
                found_emails = [e for e in _extract_all_emails(snippet) if is_plausible_email(e)]
                found_phones = _extract_all_phones(snippet)
                if found_emails:
                    for _em in found_emails:
                        email_sources.setdefault(_em.lower(), "search")
                    emails.extend(found_emails)
                if found_phones:
                    phones.extend(found_phones)
            if results:
                sources.append("search")
        except Exception as exc:
            _log.debug("tier-2 search failed for %r: %s", company_name, exc)

    # Deduplicate — prefer scraped emails over search-found ones
    seen_e: set[str] = set()
    unique_emails: list[str] = []
    _source_priority = {"scraped": 0, "search": 1}
    emails_with_prio = sorted(
        emails,
        key=lambda e: _source_priority.get(email_sources.get(e.lower(), "search"), 1),
    )
    for e in emails_with_prio:
        lower = e.lower()
        if lower not in seen_e:
            seen_e.add(lower)
            unique_emails.append(e)

    seen_p: set[str] = set()
    unique_phones: list[str] = []
    for p in phones:
        digits = re.sub(r"\D", "", p)
        if digits and digits not in seen_p:
            seen_p.add(digits)
            unique_phones.append(p)

    return {
        "emails": unique_emails,
        "phones": unique_phones,
        "contacts": contacts,
        "address": address,
        "sources": sources,
        "email_sources": email_sources,
    }


def adaptive_find_email(
    company_name: str,
    website: str | None,
    domain: str | None,
    contacts: list[dict[str, str]],
    location: str | None,
) -> dict[str, Any]:
    """
    Adaptive enrichment: pick the best next action based on what we already know.

    This function does NOT make AI calls. It uses:
      - Email pattern generation + MX verification
      - Person-based email construction + MX verification
      - Targeted web search (DuckDuckGo)

    Returns ``{"email": str|None, "source": str, "actions_tried": [...]}``.
    """
    from market_validation.company_enrichment import (
        _is_aggregator_domain,
        domain_from_url,
        verify_email,
    )
    from market_validation.web_scraper import _extract_all_emails

    actions_tried: list[str] = []
    found_email: str | None = None

    if not domain and website:
        domain = domain_from_url(website)

    # Facts-only policy: never save a pattern-guessed email (info@domain) or a
    # person-pattern guess. Only accept emails that were actually observed —
    # in search snippets, on the site, in mailto:, JSON-LD, etc.

    if domain and _is_aggregator_domain(domain):
        domain = None

    # --- Strategy: Targeted search for a real company email ---
    # Fire several query variants sequentially — stop on the first on-domain
    # or MX-verified hit. Each query reaches a different slice of the web:
    #   1) Generic company + email keyword (broadest)
    #   2) site:<domain> scoped (restricts to pages hosted on their own site —
    #      surfaces "mailto:" strings in cached Google results, menu PDFs, etc.)
    #   3) mailto: / @<domain> literal — targets pages with raw email syntax
    if company_name and not found_email:
        from market_validation.company_enrichment import is_plausible_email

        queries: list[str] = []
        loc_suffix = f" {location}" if location else ""
        queries.append(f'"{company_name}" email contact{loc_suffix}')
        if domain:
            queries.append(f'site:{domain} email OR contact OR mailto')
            queries.append(f'"{company_name}" "@{domain}"')
        queries.append(f'"{company_name}" mailto{loc_suffix}')

        on_domain_fallback: str | None = None

        for query in queries:
            action = f"search({query})"
            actions_tried.append(action)
            _log.info("  [adaptive] %s: %s", company_name, action)
            try:
                results = try_multi_search(query, num_results=5)
            except Exception as exc:
                _log.debug("adaptive search variant failed for %r: %s", query, exc)
                continue

            for r in results:
                snippet = f"{r.get('title', '')} {r.get('snippet', '')} {r.get('url', '')}"
                found_emails = [e for e in _extract_all_emails(snippet) if is_plausible_email(e)]
                if not found_emails:
                    continue
                preferred = [e for e in found_emails if domain and e.lower().endswith("@" + domain)]
                candidates_ordered = preferred + [e for e in found_emails if e not in preferred]
                for candidate in candidates_ordered:
                    vr = verify_email(candidate)
                    if vr["valid"]:
                        _log.info(
                            "  [adaptive] %s: found %s from search + MX verified",
                            company_name, candidate,
                        )
                        return {
                            "email": candidate,
                            "source": "adaptive_search_mx",
                            "actions_tried": actions_tried,
                        }
                # Remember an on-domain hit as a softer fallback — only use it
                # if NO later query yields an MX-verified address.
                if preferred and not on_domain_fallback:
                    on_domain_fallback = preferred[0]

        if on_domain_fallback:
            _log.info(
                "  [adaptive] %s: using on-domain fallback %s (unverified by MX)",
                company_name, on_domain_fallback,
            )
            return {
                "email": on_domain_fallback,
                "source": "adaptive_search",
                "actions_tried": actions_tried,
            }

    if not found_email:
        _log.info("  [adaptive] %s: no verifiable email found after %s, leaving blank", company_name, actions_tried)

    return {"email": found_email, "source": "adaptive_none", "actions_tried": actions_tried}

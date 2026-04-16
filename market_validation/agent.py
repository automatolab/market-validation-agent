"""
Market Validation Agent -- orchestrates the full research pipeline.

Provides the Agent class with four main methods: validate() for market
opportunity assessment, find() for multi-backend company discovery,
qualify() for AI-scored lead ranking, and enrich() for 3-tier contact
enrichment. AI calls are dispatched to the claude or opencode CLI.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from market_validation.log import get_logger

_log = get_logger("agent")


def _try_multi_search(query: str, num_results: int = 10, geography: str | None = None) -> list[dict[str, str]]:
    """
    Try direct search using multi-backend search.
    Falls back to empty list if all backends fail.
    When *geography* is provided, geo-aware backends constrain results to that area.
    """
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results, geography=geography)
    except Exception:
        return []


def _try_supplementary_search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    """Run slow scraped backends (BBB, Manta, etc.) once for a single query."""
    try:
        from market_validation.multi_search import supplementary_search
        return supplementary_search(query, num_results)
    except Exception:
        return []


def _free_enrich_company(
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
        scrape_contact_info, _extract_all_emails, _extract_all_phones,
    )
    from market_validation.company_enrichment import domain_from_url

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
        except Exception:
            pass

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
            results = _try_multi_search(query, num_results=5)
            for r in results:
                snippet = f"{r.get('title', '')} {r.get('snippet', '')} {r.get('url', '')}"
                found_emails = _extract_all_emails(snippet)
                found_phones = _extract_all_phones(snippet)
                if found_emails:
                    for _em in found_emails:
                        email_sources.setdefault(_em.lower(), "search")
                    emails.extend(found_emails)
                if found_phones:
                    phones.extend(found_phones)
            if results:
                sources.append("search")
        except Exception:
            pass

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


def _email_source_label(source: str) -> str:
    """Human-readable label for an email-source key (used in company notes)."""
    return {
        "scraped": "Email source: scraped from website",
        "search": "Email source: found via search results",
        "adaptive_search_mx": "Email source: found via targeted search (MX verified)",
        "adaptive_search": "Email source: found via targeted search (unverified)",
        "adaptive_person_guess_mx": "Email source: GUESSED from contact name + domain MX (not verified as real mailbox)",
        "adaptive_generic_guess_mx": "Email source: GUESSED as info@domain (MX valid, but mailbox may not exist — verify before sending)",
    }.get(source, f"Email source: {source}")


def _adaptive_find_email(
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
        generate_email_patterns, domain_from_url, verify_email,
    )
    from market_validation.web_scraper import _extract_all_emails

    actions_tried: list[str] = []
    found_email: str | None = None

    # Resolve domain if we have a website but no domain
    if not domain and website:
        domain = domain_from_url(website)

    # Order matters — always prefer real emails over guessed patterns.
    # 1) Search snippets (may surface a real email)
    # 2) Person-based construction (if we know names from AI contacts)
    # 3) Generic pattern (info@, contact@) as last-resort guess — clearly labeled

    # --- Strategy 1: Targeted search for real company email ---
    if company_name and not found_email:
        query = f'"{company_name}" email contact'
        if location:
            query += f" {location}"
        action = f"search({query})"
        actions_tried.append(action)
        _log.info("  [adaptive] %s: %s", company_name, action)

        try:
            results = _try_multi_search(query, num_results=5)
            for r in results:
                snippet = f"{r.get('title', '')} {r.get('snippet', '')} {r.get('url', '')}"
                found_emails = _extract_all_emails(snippet)
                if not found_emails:
                    continue
                # Prefer a candidate whose domain matches this company's domain
                preferred = [e for e in found_emails if domain and e.lower().endswith("@" + domain)]
                candidates_ordered = preferred + [e for e in found_emails if e not in preferred]
                for candidate in candidates_ordered:
                    vr = verify_email(candidate)
                    if vr["valid"]:
                        _log.info("  [adaptive] %s: found %s from search + MX verified", company_name, candidate)
                        return {"email": candidate, "source": "adaptive_search_mx", "actions_tried": actions_tried}
                # No MX-valid candidate — use the first preferred (or first) unverified
                fallback = candidates_ordered[0]
                _log.info("  [adaptive] %s: found %s from search (unverified)", company_name, fallback)
                return {"email": fallback, "source": "adaptive_search", "actions_tried": actions_tried}
        except Exception:
            pass

    # --- Strategy 2: Person-based email construction (requires contact names) ---
    if domain and contacts and not found_email:
        for c in contacts:
            name = c.get("name", "").strip()
            if not name or " " not in name:
                continue
            parts = name.lower().split()
            first, last = parts[0], parts[-1]
            first = re.sub(r"[^a-z]", "", first)
            last = re.sub(r"[^a-z]", "", last)
            if not first or not last:
                continue

            candidates = [
                f"{first}.{last}@{domain}",
                f"{first}{last}@{domain}",
                f"{first[0]}{last}@{domain}",
            ]
            action = f"person_email({name}@{domain})"
            actions_tried.append(action)
            _log.info("  [adaptive] %s: %s", company_name, action)

            # MX is per-domain, so all three resolve to the same answer.
            # Use the most common convention (first.last) — still a guess,
            # but anchored to a real person's name from the AI contacts.
            vr = verify_email(candidates[0])
            if vr["valid"]:
                _log.info(
                    "  [adaptive] %s: guessed %s via person pattern (MX valid, mailbox unverified)",
                    company_name, candidates[0],
                )
                return {
                    "email": candidates[0],
                    "source": "adaptive_person_guess_mx",
                    "actions_tried": actions_tried,
                }

    # --- Strategy 3: Generic pattern (info@, contact@) — last resort, clearly a guess ---
    if domain and not found_email:
        action = f"generic_pattern({domain})"
        actions_tried.append(action)
        _log.info("  [adaptive] %s: %s", company_name, action)
        try:
            patterns = generate_email_patterns(domain)
            for p in patterns:
                if p.get("valid"):
                    found_email = p["email"]
                    _log.info(
                        "  [adaptive] %s: guessed %s via generic pattern (MX valid, mailbox unverified)",
                        company_name, found_email,
                    )
                    return {
                        "email": found_email,
                        "source": "adaptive_generic_guess_mx",
                        "actions_tried": actions_tried,
                    }
        except Exception:
            pass

    if not found_email:
        _log.info("  [adaptive] %s: no email found after trying %s", company_name, actions_tried)

    return {"email": found_email, "source": "adaptive_none", "actions_tried": actions_tried}


def _summarize_backends(rows: list[dict[str, str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        backend = str(row.get("source", "unknown"))
        counts[backend] = counts.get(backend, 0) + 1
    return counts


def _tokenize_text(text: str) -> list[str]:
    return [t for t in re.split(r"\W+", text.lower()) if len(t) >= 3]


def _infer_market_profile(market: str, product: str | None) -> dict[str, Any]:
    text = f"{market} {product or ''}".lower()
    tokens = set(_tokenize_text(text))

    _category_kw_map: dict[str, tuple[str, ...]] = {
        "saas": ("saas", "software", "api", "platform", "cloud", "automation"),
        "food": ("restaurant", "food", "bbq", "barbecue", "catering", "cafe", "coffee", "dining"),
        "healthcare": ("clinic", "medical", "health", "dental", "hospital", "pharma"),
        "industrial": ("manufacturer", "manufacturing", "industrial", "factory", "supplier", "wholesale", "robot", "robotics", "drone", "automation", "aerospace", "defense", "semiconductor", "hardware"),
        "services": ("agency", "consulting", "consultant", "legal", "accounting", "services"),
    }

    if any(t in text for t in _category_kw_map["saas"]):
        category = "saas"
    elif any(t in text for t in _category_kw_map["food"]):
        category = "food"
    elif any(t in text for t in _category_kw_map["healthcare"]):
        category = "healthcare"
    elif any(t in text for t in _category_kw_map["industrial"]):
        category = "industrial"
    elif any(t in text for t in _category_kw_map["services"]):
        category = "services"
    else:
        category = "general"

    # Confidence: how strongly the input signals this category
    if category == "general":
        confidence = 30  # fell through to default
    else:
        match_count = sum(1 for kw in _category_kw_map[category] if kw in text)
        confidence = min(100, 40 + match_count * 25)

    positive_by_category: dict[str, set[str]] = {
        "food": {"restaurant", "dining", "catering", "grill", "kitchen", "eatery", "bbq", "barbecue", "smokehouse"},
        "saas": {"saas", "software", "platform", "api", "cloud", "automation", "tool", "solution", "app"},
        "healthcare": {"clinic", "medical", "health", "hospital", "dental", "care", "provider"},
        "industrial": {"manufacturer", "manufacturing", "industrial", "supplier", "factory", "distributor", "robot", "robotics", "drone", "automation", "aerospace", "semiconductor", "hardware", "systems"},
        "services": {"services", "agency", "consulting", "consultant", "firm", "provider"},
        "general": {"company", "business", "provider", "services"},
    }

    blocked_tokens = {"list of", "wikipedia"}
    if category == "food":
        blocked_tokens.update({"season", "episode", "joey chestnut", "chopped", "man v. food"})

    banned_name_tokens: set[str] = set()

    return {
        "category": category,
        "confidence": confidence,
        "tokens": tokens,
        "positive_tokens": positive_by_category.get(category, set()),
        "blocked_tokens": blocked_tokens,
        "banned_name_tokens": banned_name_tokens,
    }


def _ai_validate_companies(
    candidates: list[dict[str, Any]],
    market: str,
    geography: str,
    business_type: str,
    run_ai: Any,
) -> list[dict[str, Any]]:
    """
    Use Claude as the final quality gate before writing companies to the database.

    Sends all candidates in a single batch call. Claude:
    - Confirms each is a real operating business relevant to the market
    - Cleans the business name (strips ratings, platform names, page titles)
    - Deduplicates (marks duplicates as keep=false)
    - Rejects unrelated businesses, directories, maps, articles, social pages

    Returns only the validated entries with cleaned names.
    Falls back to the original list on any failure.
    """
    if not candidates:
        return []

    lines = []
    for i, c in enumerate(candidates):
        name = c.get("company_name", "")
        url = (c.get("website") or c.get("evidence_url") or "")[:80]
        snippet = (c.get("description") or "")[:180]
        lines.append(f'  {i}. name="{name}" url="{url}" snippet="{snippet}"')

    prompt = f"""You are a data quality agent for a market research pipeline.

We are building a lead list of: {business_type}
Geography: {geography} (include the wider metro area — nearby cities count)
Market context: {market}

Review every candidate below. For each, decide:
- Is it a REAL OPERATING BUSINESS relevant to {business_type}?
  REJECT: directories, maps, articles, recipes, social posts, unrelated companies, duplicates
  KEEP: real businesses even if in nearby cities within the same metro area
- What is the CLEAN business name? Strip ratings, page section prefixes (Menu |, Order |), platform suffixes (- Yelp, | TikTok), listicle language.
- Is it a DUPLICATE of another candidate? (keep only the first occurrence)

IMPORTANT: Be INCLUSIVE not exclusive. If a business plausibly operates in or serves the {geography} metro area, KEEP it.
Only reject businesses that are clearly in a DIFFERENT metro area (e.g. New York vs San Jose) or a different state.

Candidates:
{chr(10).join(lines)}

Return ONLY a JSON array — one object per candidate, in the same order. No markdown:
[
  {{"index": 0, "keep": true, "clean_name": "Business Name", "reason": "real business in metro area"}},
  {{"index": 1, "keep": false, "clean_name": "", "reason": "article, not a business"}},
  ...
]"""

    try:
        raw = run_ai(prompt)
        text = None
        if isinstance(raw, dict):
            companies_val = raw.get("companies")
            if isinstance(companies_val, list):
                # _parse_json_from_text wraps JSON arrays as {"companies": [...]}
                # Check if the items are validation results (have "index"/"keep") or
                # actual company objects (have "company_name"). Handle both.
                if companies_val and isinstance(companies_val[0], dict):
                    if "index" in companies_val[0] or "keep" in companies_val[0]:
                        # It's a validation array wrapped by _parse_json_from_text
                        import json as _j
                        text = _j.dumps(companies_val)
                    else:
                        # Claude returned companies, not validation results. Fall back.
                        return candidates
                elif not companies_val:
                    # Empty list — nothing to validate, return empty
                    return []
                else:
                    import json as _j
                    text = _j.dumps(companies_val)
            else:
                text = raw.get("text") or raw.get("content") or None
                if not text:
                    import json as _j
                    text = _j.dumps(raw)
        elif isinstance(raw, str):
            text = raw

        if not text:
            return candidates

        # Strip markdown fences if present
        import re as _re, json as _j
        text = _re.sub(r"^```[a-z]*\n?", "", text.strip())
        text = _re.sub(r"\n?```$", "", text.strip())
        parsed = _j.loads(text)

        validated: list[dict[str, Any]] = []
        for item in parsed:
            if not item.get("keep"):
                _log.info(
                    "[find:validate] REJECT [%s] %r — %s",
                    item.get("index"),
                    candidates[item["index"]].get("company_name", "?"),
                    item.get("reason", ""),
                )
                continue
            idx = item.get("index", -1)
            if not (0 <= idx < len(candidates)):
                continue
            c = dict(candidates[idx])
            clean = (item.get("clean_name") or "").strip()
            if clean:
                c["company_name"] = clean
            _log.info("[find:validate] KEEP  [%s] %r", idx, c["company_name"])
            validated.append(c)

        # Trust the AI: if it validated the response (parsed is non-empty) but
        # rejected everything, return the empty list. Only fall back on failure.
        if parsed:
            _log.info("[find:validate] Validation complete: %d/%d kept", len(validated), len(candidates))
            return validated
        # parsed was empty — AI returned nothing actionable
        return candidates

    except Exception as e:
        _log.warning("[find:validate] AI validation failed: %s — keeping all candidates", e, exc_info=True)
        return candidates


def _archetype_search_context(
    archetype_key: str, market: str, geography: str, product: str | None
) -> str:
    """
    Return an archetype-specific search context string that tells the AI
    what types of businesses to look for during find().
    """
    search_term = product or market

    if archetype_key == "b2b-industrial":
        return (
            f"We are researching the B2B supply chain for {search_term} in {geography}.\n"
            f"Find BOTH sides: (1) businesses that BUY/CONSUME {search_term} (restaurants, caterers, "
            f"food service, manufacturers, fabricators) AND (2) businesses that SELL/DISTRIBUTE "
            f"{search_term} (wholesalers, distributors, suppliers, importers, specialty markets).\n"
            f"Also look for: butcher shops, commissary kitchens, food trucks, catering companies, "
            f"industrial buyers, contract manufacturers, and any business that purchases {search_term} in bulk."
        )

    if archetype_key == "b2b-saas":
        return (
            f"We are researching companies that could buy {search_term} software in {geography}.\n"
            f"Find: companies currently using competitor products, companies with pain points that "
            f"{search_term} solves, growing companies that need {search_term} tooling, "
            f"companies with job postings mentioning {market}.\n"
            f"Also look for: companies recently funded, companies posting roles related to {market}, "
            f"and businesses that have outgrown manual processes in this space."
        )

    if archetype_key == "b2c-saas":
        return (
            f"We are researching consumer apps and products in the {search_term} space in {geography}.\n"
            f"Find: companies building consumer apps in {market}, indie developers and small studios, "
            f"startups with apps on the App Store or Google Play, and companies with active user communities.\n"
            f"Also look for: Product Hunt launches, social media presences, and freemium products in this category."
        )

    if archetype_key == "local-service":
        return (
            f"We are researching {search_term} businesses in {geography}.\n"
            f"Find: all {search_term} businesses including small/independent ones, chain locations, "
            f"new openings, food trucks, pop-ups, and catering operations in the metro area.\n"
            f"Also look for: businesses in nearby neighborhoods, recently opened locations, "
            f"businesses listed on Yelp/Google Maps, and mobile or home-based operations."
        )

    if archetype_key == "consumer-cpg":
        return (
            f"We are researching consumer packaged goods brands in the {search_term} category in {geography}.\n"
            f"Find: CPG brands producing {search_term}, DTC brands, brands carried in local retailers, "
            f"and emerging brands with e-commerce presence.\n"
            f"Also look for: co-manufacturers, private-label producers, brands on Amazon or Shopify, "
            f"and companies exhibiting at trade shows related to {market}."
        )

    if archetype_key == "marketplace":
        return (
            f"We are researching marketplace platforms in the {search_term} space in {geography}.\n"
            f"Find: platforms connecting buyers and sellers in {market}, existing marketplaces (even small ones), "
            f"directory sites that could become marketplaces, and companies aggregating supply or demand.\n"
            f"Also look for: gig platforms, booking platforms, listing sites, and peer-to-peer exchanges in this space."
        )

    if archetype_key == "healthcare":
        return (
            f"We are researching healthcare businesses related to {search_term} in {geography}.\n"
            f"Find: clinics, practices, and providers offering {search_term}, digital health companies, "
            f"medical device companies, and health systems with relevant departments.\n"
            f"Also look for: telehealth providers, specialty practices, ambulatory surgery centers, "
            f"diagnostic labs, and healthcare IT companies serving this segment."
        )

    if archetype_key == "services-agency":
        return (
            f"We are researching {search_term} service providers and agencies in {geography}.\n"
            f"Find: agencies, consulting firms, and freelancers specializing in {search_term}, "
            f"boutique firms, large agencies with {market} practices, and independent consultants.\n"
            f"Also look for: firms listed on Clutch or similar directories, companies with case studies "
            f"in {market}, and professionals with strong LinkedIn presence in this space."
        )

    # Fallback for unknown archetypes
    return (
        f"We are researching businesses related to {search_term} in {geography}.\n"
        f"Find: all types of businesses involved in {market}, including providers, suppliers, "
        f"buyers, and intermediaries."
    )


def _archetype_qualify_context(
    archetype_key: str, market: str, product: str | None
) -> str:
    """
    Return an archetype-specific qualification context string that tells the AI
    how to evaluate companies as leads during qualify().
    """
    search_term = product or market

    if archetype_key == "b2b-industrial":
        return (
            f"We are a {search_term} wholesale distributor / supplier. "
            f"Evaluate each company as a POTENTIAL BUYER of {search_term}.\n"
            f"A qualified lead is a restaurant, caterer, manufacturer, or food service business that:\n"
            f"- Uses {search_term} in significant volume (high-volume restaurant > small cafe)\n"
            f"- Has multiple locations or high foot traffic (more volume = better customer)\n"
            f"- Does catering or bulk orders\n"
            f"- Shows growth signals (expanding, hiring, new locations)\n\n"
            f"Score higher: established high-volume buyers, chain locations, large caterers, "
            f"businesses with clear bulk purchasing needs.\n"
            f"Score lower: small cafes with minimal {search_term} usage, businesses unlikely to buy wholesale, "
            f"competitors who are also distributors (mark as 'competitor' not 'qualified')."
        )

    if archetype_key == "b2b-saas":
        return (
            f"We sell {search_term} software. Evaluate each company as a potential buyer.\n"
            f"A qualified lead:\n"
            f"- Has 50+ employees (can afford enterprise software)\n"
            f"- Currently uses competitor products or manual processes for {market}\n"
            f"- Shows growth signals (hiring, funding, expansion)\n"
            f"- Has budget authority (look for VP/Director level contacts)\n\n"
            f"Score higher: mid-market and enterprise companies with clear need, "
            f"companies with job postings in {market}, recently funded startups scaling up.\n"
            f"Score lower: very small teams (<10 people), companies already locked into a competitor, "
            f"companies in unrelated industries."
        )

    if archetype_key == "b2c-saas":
        return (
            f"We are building a consumer app / B2C product in {search_term}. "
            f"Evaluate each company as a POTENTIAL COMPETITOR or PARTNERSHIP target.\n"
            f"A qualified lead:\n"
            f"- Has an active user base in a related category\n"
            f"- Shows strong engagement metrics (app ratings, social following, reviews)\n"
            f"- Could be a distribution partner or acquisition target\n"
            f"- Demonstrates product-market fit in an adjacent space\n\n"
            f"Score higher: companies with strong user engagement, growing download counts, "
            f"active communities.\n"
            f"Score lower: dormant apps, companies with poor ratings, unrelated consumer products."
        )

    if archetype_key == "local-service":
        return (
            f"We are researching the {search_term} market for competitive analysis and "
            f"potential customer/partnership opportunities.\n"
            f"Evaluate each company as a business operating in {search_term}.\n"
            f"A qualified lead:\n"
            f"- Is an active, operating {search_term} business (not permanently closed)\n"
            f"- Has visible foot traffic, reviews, or online presence\n"
            f"- Shows quality signals (good ratings, consistent reviews, active social media)\n"
            f"- Has growth indicators (new locations, catering arm, delivery, expanding hours)\n\n"
            f"Score higher: established businesses with strong reputations, multi-location operators, "
            f"businesses with catering or delivery revenue streams.\n"
            f"Score lower: businesses that appear closed or inactive, very low review counts "
            f"suggesting minimal traffic, businesses not actually in {search_term}."
        )

    if archetype_key == "consumer-cpg":
        return (
            f"We are a {search_term} CPG brand. Evaluate each company as a potential "
            f"retail partner, competitor, or distribution channel.\n"
            f"A qualified lead:\n"
            f"- Is a retailer that could carry {search_term} products (grocery, specialty, online)\n"
            f"- Is a competing brand whose shelf space or positioning we should understand\n"
            f"- Has strong retail velocity or DTC presence\n"
            f"- Shows growth signals (new store openings, expanded product lines)\n\n"
            f"Score higher: retailers with relevant category presence, growing DTC brands, "
            f"distributors with established retail relationships.\n"
            f"Score lower: unrelated retailers, brands in completely different categories, "
            f"businesses with no retail or e-commerce presence."
        )

    if archetype_key == "marketplace":
        return (
            f"We are building a marketplace in {search_term}. Evaluate each company as a "
            f"potential supply-side partner, demand-side participant, or competitor.\n"
            f"A qualified lead:\n"
            f"- Could be a supplier or provider on our platform\n"
            f"- Represents significant demand volume in {market}\n"
            f"- Is currently underserved by existing marketplace options\n"
            f"- Shows signals of needing better buyer-seller matching\n\n"
            f"Score higher: businesses with high transaction volume, those currently using "
            f"inefficient channels, providers with strong reputations but limited reach.\n"
            f"Score lower: businesses too small to generate meaningful GMV, those already "
            f"well-served by existing platforms."
        )

    if archetype_key == "healthcare":
        return (
            f"We are in the {search_term} healthcare space. Evaluate each company as a "
            f"potential customer, partner, or key account.\n"
            f"A qualified lead:\n"
            f"- Is a healthcare provider, health system, or practice relevant to {market}\n"
            f"- Has sufficient patient volume or revenue to justify the purchase\n"
            f"- Shows modernization signals (adopting new technology, expanding services)\n"
            f"- Has regulatory compliance infrastructure (HIPAA, EMR integration)\n\n"
            f"Score higher: multi-location practices, health systems, providers with "
            f"technology-forward reputations, practices in growth mode.\n"
            f"Score lower: very small solo practices with limited budgets, providers in "
            f"unrelated specialties, businesses with no clear connection to {search_term}."
        )

    if archetype_key == "services-agency":
        return (
            f"We are researching the {search_term} services market. Evaluate each company "
            f"as a competitor, potential partner, or acquisition target.\n"
            f"A qualified lead:\n"
            f"- Is an active agency or consultancy in {market}\n"
            f"- Has a clear specialization and client portfolio\n"
            f"- Shows revenue signals (team size, office presence, client logos)\n"
            f"- Demonstrates thought leadership or industry recognition\n\n"
            f"Score higher: firms with strong case studies, retainer-based revenue, "
            f"growing teams, and industry awards or recognition.\n"
            f"Score lower: solo freelancers with no web presence, inactive firms, "
            f"generalist agencies with no depth in {search_term}."
        )

    # Fallback
    return (
        f"Evaluate these companies as potential sales targets or competitors "
        f"in the {search_term} market.\n"
        f"A qualified lead is a business that is actively operating in or adjacent to {market}, "
        f"has visible revenue or activity signals, and could be a customer, partner, or competitor."
    )


def _ai_search_strategy(
    market: str,
    geography: str,
    product: str | None,
    run_ai: Any,
    archetype_context: str | None = None,
) -> dict[str, Any] | None:
    """
    Ask the LLM to generate a search strategy for this market.

    Returns a dict with:
      - queries: list of search strings to run
      - real_business_signals: tokens/phrases that indicate a real business
      - junk_signals: tokens/phrases that indicate a junk result
      - business_type: plain-English description (e.g. "BBQ restaurant")

    Returns None if the AI call fails.
    """
    _arch_hint = ""
    if archetype_context:
        _arch_hint = f"\nArchetype guidance:\n{archetype_context}\n"

    prompt = f"""You are a market research strategist. Given a market and geography, figure out:
1. What is the NATURE of this market? (product, service, ingredient/supply chain, technology, etc.)
2. Who are the TARGET BUSINESSES to research? (the ones that BUY, SELL, or PROVIDE this thing)
3. What search queries will find their actual business websites (not articles, reviews, or directories)?

Market: {market}
Geography: {geography}
Product/context: {product or 'general'}
{_arch_hint}
Think step by step:
- If this is a RAW INGREDIENT or PRODUCT (e.g. "brisket", "organic cotton", "steel"), the target businesses
  are those that BUY it (restaurants, manufacturers) AND those that SELL/DISTRIBUTE it (wholesalers, suppliers).
- If this is a SERVICE (e.g. "pet grooming", "accounting"), the target businesses are those that PROVIDE the service.
- If this is a TECHNOLOGY/SOFTWARE, the target businesses are companies building or selling it.
- If ambiguous, cover multiple angles.

Return ONLY this JSON (no markdown fences):
{{
  "market_nature": "<product|service|ingredient|technology|marketplace|other>",
  "business_type": "<one phrase describing the primary type of business to find>",
  "target_description": "<who are we looking for and why — 1 sentence>",
  "queries": [
    "<search query 1 — most targeted>",
    "<search query 2 — different angle (e.g. suppliers, distributors)>",
    "<search query 3 — local directories or listings>",
    "<search query 4 — catering, wholesale, or services>",
    "<search query 5 — nearby metro area / wider geography>",
    "<search query 6 — industry-specific terms>",
    "<search query 7 — another angle>",
    "<search query 8 — final angle>"
  ],
  "real_business_signals": ["<word/phrase in real business titles/URLs>", ...],
  "junk_signals": ["<word/phrase that indicates NOT a real business>", ...]
}}

Rules for queries:
- Each query should find a DIFFERENT type of business or angle (buyers, sellers, suppliers, providers)
- Include {geography} in most queries
- Aim for business homepages with contact info, not aggregator or review sites
- Think like a B2B sales researcher who needs phone numbers and emails"""

    try:
        result = run_ai(prompt)
        if isinstance(result, dict):
            if "queries" in result:
                return result
            if "text" in result:
                import json as _json
                return _json.loads(result["text"])
        if isinstance(result, str):
            import json as _json
            return _json.loads(result)
    except Exception as e:
        _log.warning("[find] AI search strategy failed: %s", e)
    return None


# Adjacent profiles to try when the primary profile underperforms.
# Ordered by likelihood: more specific neighbours first, general last.
_ADJACENT_PROFILES: dict[str, list[str]] = {
    "food": ["services", "general"],
    "saas": ["services", "general"],
    "healthcare": ["services", "general"],
    "industrial": ["services", "general"],
    "services": ["general"],
    "general": [],  # already the broadest fallback
}


def _queries_for_adjacent_profile(
    market: str, geography: str, product: str | None, category: str
) -> list[str]:
    """Return search queries shaped for an adjacent category."""
    search_term = product or market
    base = [
        f"{search_term} {geography}",
        f"{search_term} business {geography}",
    ]
    extra: dict[str, list[str]] = {
        "food": [f"{search_term} {geography} restaurant", f"best {search_term} {geography}"],
        "saas": [f"{search_term} software companies {geography}", f"{search_term} tools {geography}"],
        "healthcare": [f"{search_term} clinics {geography}", f"{search_term} providers {geography}"],
        "industrial": [f"{search_term} manufacturers {geography}", f"{search_term} suppliers {geography}"],
        "services": [f"{search_term} agencies {geography}", f"{search_term} firms {geography}"],
        "general": [f"{search_term} companies {geography}", f"{market} businesses {geography}"],
    }
    return _unique_in_order(base + extra.get(category, []))


def _try_source_urls(market: str, geography: str, product: str | None = None) -> list[dict[str, Any]]:
    """
    Scrape URLs from market-specific source config.
    """
    results = []
    
    try:
        from market_validation.source_config import load_source_config, get_directory_urls, get_search_queries, get_direct_urls

        try:
            from market_validation.web_scraper import quick_scrape
        except Exception:
            quick_scrape = None
        
        config = load_source_config(market)
        if not config:
            return []
        
        # Get directory URLs from config
        directories = get_directory_urls(config, market, geography, product)
        direct_urls = get_direct_urls(config)
        for d in direct_urls:
            url = (d.get("url") or "").replace("{market}", market.replace(" ", "+")).replace("{product}", (product or market).replace(" ", "+")).replace("{geography}", geography.replace(" ", "+"))
            if url:
                directories.append({"name": d.get("name", "direct"), "url": url, "type": d.get("type", "directory")})
        
        # Best-effort scraping from configured directories/URLs
        if quick_scrape is not None:
            for d in directories[:4]:  # Limit to avoid rate limits
                try:
                    scrape_result = quick_scrape(d["url"])
                    if scrape_result and not scrape_result.get("error"):
                        results.append({
                            "source": d["name"],
                            "type": d["type"],
                            "data": scrape_result,
                        })
                except Exception:
                    continue
        
        # Also do search queries from config
        queries = get_search_queries(config, market, geography, product)
        if product and product.strip().lower() != market.strip().lower():
            queries.extend(get_search_queries(config, market, geography, market))
        queries = list(dict.fromkeys(queries))
        for query in queries[:3]:
            search_results = _try_multi_search(query, 5, geography=geography)
            for r in search_results:
                results.append({
                    "source": r.get("source", "search"),
                    "type": "search",
                    "data": r,
                })
        
    except Exception:
        pass
    
    return results


def _iso_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


# Platform suffixes to strip from page titles used as company names
_TITLE_SUFFIXES = [
    r"\s*\|\s*TikTok$",
    r"\s*-\s*YouTube$",
    r"\s*\|\s*YouTube$",
    r"\s*-\s*Instagram$",
    r"\s*\|\s*Instagram$",
    r"\s*-\s*Facebook$",
    r"\s*\|\s*Facebook$",
    r"\s*\|\s*LinkedIn$",
    r"\s*-\s*LinkedIn$",
    r"\s*\|\s*Twitter$",
    r"\s*-\s*Yelp$",
    r"\s*\|\s*Yelp$",
    r"\s*-\s*TripAdvisor$",
    r"\s*\|\s*TripAdvisor$",
    r"\s*-\s*Google Maps$",
    r"\s*\|\s*Google Maps$",
    r"\s*-\s*DoorDash$",
    r"\s*\|\s*DoorDash$",
    r"\s*\|\s*Foursquare$",
    r"\s*\|\s*Zomato$",
    # Geo suffixes appended to business names (e.g. "SmokeHouseSanJoseCA")
    r",?\s+San\s+Jose[,\s]+CA[,]?$",
    r",?\s+San\s+Jose[,\s]+California[,]?$",
    r"SanJoseCA?$",
    r"SanJoseCa$",
    # MapQuest / directory address suffix: ", City, ST 00000, US - MapQuest"
    r",\s+[A-Za-z ]+,\s+[A-Z]{2}\s+\d{5}(?:,\s*US)?\s*-\s*MapQuest$",
    r"\s*-\s*MapQuest$",
    # Other directory suffixes
    r"\s*-\s*Foursquare$",
    r"\s*\|\s*MapQuest$",
]
_TITLE_SUFFIX_RE = re.compile(
    "|".join(_TITLE_SUFFIXES), re.IGNORECASE
)


_TITLE_PREFIX_RE = re.compile(
    r"^(?:Menu|Home|About|Order|Catering|Shop|Store|Contact|Gallery|Blog"
    r"|News|Events|Reviews|Jobs|Careers|Services|Products|Photos)\s*\|\s*",
    re.IGNORECASE,
)

_JUNK_TITLE_RE = re.compile(
    r"^(?:The\s+\d+\s+Best\b|Best\s+\w+\s+in\b|\d+\s+Best\b|Top\s+\d+\b"
    r"|Review:\s|First\s+Visit\b|Food\s+Adventure\b)",
    re.IGNORECASE,
)


def _clean_company_name(raw: str) -> str:
    """Strip platform suffixes/prefixes, CamelCase geo tags, and noise from page titles."""
    name = raw.strip()
    # Strip leading page-section prefixes like "Menu | " or "Home | "
    name = _TITLE_PREFIX_RE.sub("", name).strip()
    # Iteratively strip known suffixes (TikTok, YouTube, geo tags, etc.)
    prev = None
    while prev != name:
        prev = name
        name = _TITLE_SUFFIX_RE.sub("", name).strip()
    # Remove leading/trailing punctuation
    name = name.strip("|-– —\t")
    # Collapse internal whitespace
    name = re.sub(r"\s{2,}", " ", name).strip()
    return name or raw.strip()


def _normalize_companies(companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for c in companies:
        normalized.append(
            {
                "company_name": _clean_company_name(c.get("company_name") or c.get("name") or c.get("title", "") or "Unknown"),
                "website": c.get("website") or c.get("url", ""),
                "location": c.get("location") or c.get("address", ""),
                "phone": c.get("phone", ""),
                "description": c.get("description") or c.get("specialty", "") or c.get("notes", ""),
                "evidence_url": c.get("evidence_url") or c.get("url", ""),
                "source": c.get("source", "unknown"),
            }
        )
    return normalized


def _dedupe_companies(companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for c in companies:
        key = (
            c.get("website")
            or c.get("evidence_url")
            or " ".join(str(c.get("company_name", "")).strip().lower().split())
        )
        if not key:
            continue
        if key not in deduped:
            deduped[key] = c
    return list(deduped.values())


_JUNK_NAME_PATTERNS = [
    "search results for",
    "better business bureau",
    "privacy policy",
    "cookie policy",
    "terms of service",
    "site map",
    "yellow pages",
    "yelp search",
    # Video / social content
    "| tiktok",
    "- youtube",
    "- tiktok",
    "| youtube",
    "on tiktok",
    "on youtube",
    "- instagram",
    "| instagram",
    "- facebook",
    "| facebook",
    # Listicles / aggregators
    "top 10",
    "top 5",
    "10 best",
    "8 best",
    "5 best",
    "near me",
    " review:",
    "review: ",
    "- review",
    "| review",
    # Stores / extensions
    "chrome web store",
    "chrome extension",
    "app store",
    "google play",
    # Irrelevant patterns
    "first visit",
    "food adventure",
    "senior living",
    "senior community",
    "assisted living",
    "memory care",
    "job listing",
    "jobs available",
    "salary in",
    "text to speech",
    "voice reader",
    # Article/listicle titles
    "is so good, it",
    "irresistible ",
    "you need to try",
    "must try",
    "family destinations",
    "serves up the best",
    "guide to local",
    "craigslist:",
    "craigslist.org",
    # Menu item pages (not a restaurant homepage)
    " - american restaurant in",
    " - mexican restaurant in",
    "items/",
    "/menu/",
]

_BLOCKED_URL_HOSTS = {
    "bbb.org", "www.bbb.org",
    "wikipedia.org", "www.wikipedia.org",
    "opencorporates.com", "www.opencorporates.com",
    "yellowpages.com", "www.yellowpages.com",
    "yelp.com", "www.yelp.com",
    "tripadvisor.com", "www.tripadvisor.com",
    "google.com", "www.google.com",
    # Social / video / app stores
    "youtube.com", "www.youtube.com",
    "tiktok.com", "www.tiktok.com",
    "instagram.com", "www.instagram.com",
    "facebook.com", "www.facebook.com",
    "twitter.com", "www.twitter.com",
    "x.com",
    "linkedin.com", "www.linkedin.com",
    "chromewebstore.google.com",
    "chrome.google.com",
    # Review / listicle / aggregator sites
    "yelp.com", "www.yelp.com",
    "doordash.com", "www.doordash.com",
    "ubereats.com", "www.ubereats.com",
    "grubhub.com", "www.grubhub.com",
    "postmates.com",
    "seamless.com",
    "allmenus.com",
    "menupix.com",
    "zomato.com",
    "opentable.com",
    "restaurantji.com",
    "sirved.com",
    "foursquare.com",
    "reddit.com", "www.reddit.com",
    "quora.com", "www.quora.com",
    "pinterest.com", "www.pinterest.com",
    # News / media
    "yelp.com",
    "theguardian.com",
    "nytimes.com",
    "sfgate.com",
    "mercurynews.com",
    "bizjournals.com",
    # Misc junk
    "crunchbase.com",
    "dnb.com",
    "manta.com",
    "chamberofcommerce.com",
    "expertise.com",
    "thumbtack.com",
    "angieslist.com",
    "homeadvisor.com",
    "bark.com",
    # Map / directions sites (show business listings, not business homepages)
    "mapquest.com", "www.mapquest.com",
    "maps.apple.com",
    "waze.com", "www.waze.com",
    # Classifieds
    "craigslist.org", "sfbay.craigslist.org",
    # Local news / city guides (not businesses)
    "6amcity.com", "sjtoday.6amcity.com",
    "patch.com",
    "nextdoor.com",
    # Article / travel sites
    "familydestinationsguide.com",
    "onlyinyourstate.com",
    "roadsnacks.net",
    "wideopeneats.com",
    "lovefood.com",
}


def _is_junk_company(c: dict[str, Any]) -> bool:
    name = str(c.get("company_name", "")).lower().strip()
    if not name or len(name) < 3:
        return True
    if any(pat in name for pat in _JUNK_NAME_PATTERNS):
        return True
    # Block if website is a known directory/aggregator host
    url = str(c.get("website") or c.get("evidence_url") or "").strip()
    if url:
        try:
            from urllib.parse import urlparse
            host = (urlparse(url).netloc or "").lower().lstrip("www.")
            if host in {h.lstrip("www.") for h in _BLOCKED_URL_HOSTS}:
                return True
        except Exception:
            pass
    return False


def _filter_relevant_companies(
    companies: list[dict[str, Any]],
    market: str,
    product: str | None,
    extra_junk_signals: list[str] | None = None,
    extra_real_signals: list[str] | None = None,
) -> list[dict[str, Any]]:
    profile = _infer_market_profile(market, product)
    key_tokens = set(profile["tokens"]) | set(profile["positive_tokens"])
    if extra_real_signals:
        key_tokens |= {s.lower() for s in extra_real_signals}
    blocked_tokens = set(profile["blocked_tokens"])
    if extra_junk_signals:
        blocked_tokens |= {s.lower() for s in extra_junk_signals}
    banned_name_tokens = set(profile["banned_name_tokens"])

    filtered: list[dict[str, Any]] = []
    for c in companies:
        if _is_junk_company(c):
            continue
        company_name = str(c.get("company_name", "")).strip()
        hay = " ".join(
            str(c.get(field, "")).lower()
            for field in ("company_name", "description", "location", "website", "evidence_url")
        )
        low_name = company_name.lower()
        if banned_name_tokens and any(tok in low_name for tok in banned_name_tokens):
            continue
        if any(bt in hay for bt in blocked_tokens):
            continue
        if c.get("source") == "wikipedia":
            continue
        # If no key tokens matched but AI provided real signals, be more permissive
        if key_tokens and not any(token in hay for token in key_tokens):
            continue
        filtered.append(c)
    return filtered


def _unique_in_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _normalize_name_key(value: str) -> str:
    tokens = [t for t in re.split(r"\W+", str(value or "").lower()) if len(t) >= 2]
    return " ".join(tokens)


def _extract_phone_text(value: str) -> str:
    # Match international (+1-408-...) and domestic formats
    match = re.search(r"(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", str(value or ""))
    return match.group(0).strip() if match else ""


def _extract_email_text(value: str) -> str:
    match = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", str(value or ""))
    return match.group(0) if match else ""


def _extract_contact_from_search_result(r: dict[str, str]) -> dict[str, Any]:
    """Build a company dict from a raw search result, extracting phone/email/location
    from snippet text.  Works for all backends (Nominatim, DDGS, BBB, etc.)."""
    snippet = r.get("snippet", "")
    phone = ""
    email = ""
    location = ""

    # Nominatim encodes structured fields in the snippet as "display | phone=... | email=..."
    if r.get("source") == "nominatim" and snippet:
        for part in [p.strip() for p in snippet.split("|")]:
            if part.startswith("phone="):
                phone = part[len("phone="):].strip()
            elif part.startswith("email="):
                email = part[len("email="):].strip()
            elif not part.startswith("cuisine=") and not location:
                location = part

    # Fallback: regex extraction from raw snippet (works for all backends)
    if not phone:
        phone = _extract_phone_text(snippet)
    if not email:
        email = _extract_email_text(snippet)

    return {
        "company_name": r.get("title", ""),
        "website": r.get("url", ""),
        "location": location,
        "phone": phone,
        "email": email,
        "description": snippet,
        "evidence_url": r.get("url", ""),
        "source": r.get("source", "search"),
    }


def _print_validation_summary(
    market: str,
    geography: str,
    archetype_key: str,
    scorecard: dict,
    sizing: dict,
    competition: dict,
) -> None:
    """Print a concise, readable validation summary to the terminal."""
    verdict = scorecard.get("verdict", "unknown")
    overall = scorecard.get("overall_score", 0)

    verdict_icons = {
        "strong_go": "✓✓ STRONG GO",
        "go":        "✓  GO",
        "cautious":  "~  CAUTIOUS",
        "no_go":     "✗  NO GO",
    }
    verdict_label = verdict_icons.get(verdict, verdict.upper())

    # TAM formatting
    tam_low = sizing.get("tam_low") or 0
    tam_high = sizing.get("tam_high") or 0
    def _fmt_money(n: float) -> str:
        if n >= 1_000_000_000:
            return f"${n/1_000_000_000:.1f}B"
        if n >= 1_000_000:
            return f"${n/1_000_000:.0f}M"
        if n >= 1_000:
            return f"${n/1_000:.0f}K"
        return f"${n:.0f}"
    tam_str = f"{_fmt_money(tam_low)} – {_fmt_money(tam_high)}" if tam_high else "unknown"

    # Sub-scores
    attr  = scorecard.get("market_attractiveness")
    dem   = scorecard.get("demand_validation")
    comp  = scorecard.get("competitive_score")
    risk  = scorecard.get("risk_score")
    ue    = scorecard.get("unit_economics_score")
    sa    = scorecard.get("structural_attractiveness")
    ts    = scorecard.get("timing_score")
    icp   = scorecard.get("icp_clarity")

    def _bar(score, width: int = 20) -> str:
        if score is None:
            return " " * width
        filled = round((score / 100) * width)
        return "█" * filled + "░" * (width - filled)

    def _s(score) -> str:
        return f"{round(score):>3}/100" if score is not None else "    —  "

    sep = "─" * 58
    print()
    print(sep)
    print(f"  MARKET VALIDATION  ·  {market}  ·  {geography}")
    print(sep)
    print(f"  Archetype : {archetype_key}  ({scorecard.get('archetype_label', '')})")
    print(f"  TAM       : {tam_str}")
    competitors = competition.get("competitor_count") or competition.get("raw_candidate_count")
    if competitors:
        conc = competition.get("market_concentration", "")
        print(f"  Market    : {conc} · {competitors} competitors identified")
    print()
    print(f"  {'CORE SCORES':<28}  {'MODULE SCORES'}")
    print(f"  {'─'*26}  {'─'*26}")
    print(f"  Attractiveness  {_bar(attr,14)} {_s(attr)}  Unit Economics  {_s(ue)}")
    print(f"  Demand          {_bar(dem,14)} {_s(dem)}  Porter's SA     {_s(sa)}")
    print(f"  Competition     {_bar(100-(comp or 0),14)} {_s(100-(comp or 0))}  Timing          {_s(ts)}")
    print(f"  Risk (inv)      {_bar(100-(risk or 0),14)} {_s(100-(risk or 0))}  ICP Clarity     {_s(icp)}")
    print()
    print(f"  {'─'*54}")
    print(f"  OVERALL  {_bar(overall, 30)} {overall:.0f}/100")
    print(f"  VERDICT  {verdict_label}")
    print(f"  {'─'*54}")

    reasoning = scorecard.get("verdict_reasoning", "")
    if reasoning:
        # Word-wrap to ~54 chars
        words = reasoning.split()
        line, lines = [], []
        for w in words:
            if sum(len(x)+1 for x in line) + len(w) > 54:
                lines.append(" ".join(line))
                line = [w]
            else:
                line.append(w)
        if line:
            lines.append(" ".join(line))
        print()
        for l in lines:
            print(f"  {l}")

    next_steps = scorecard.get("next_steps") or []
    if next_steps:
        print()
        print("  NEXT STEPS")
        for i, step in enumerate(next_steps[:3], 1):
            # Word-wrap each step
            words = step.split()
            line, lines = [], []
            for w in words:
                if sum(len(x)+1 for x in line) + len(w) > 50:
                    lines.append(" ".join(line))
                    line = [w]
                else:
                    line.append(w)
            if line:
                lines.append(" ".join(line))
            print(f"  {i}. {lines[0]}")
            for cont in lines[1:]:
                print(f"     {cont}")

    key_risks = scorecard.get("key_risks") or []
    if key_risks:
        print()
        print("  KEY RISKS")
        for risk_item in key_risks[:2]:
            words = risk_item.split()
            line, lines = [], []
            for w in words:
                if sum(len(x)+1 for x in line) + len(w) > 51:
                    lines.append(" ".join(line))
                    line = [w]
                else:
                    line.append(w)
            if line:
                lines.append(" ".join(line))
            print(f"  ▲ {lines[0]}")
            for cont in lines[1:]:
                print(f"    {cont}")

    print(sep)
    print()


def _is_useful_business_url(url: str) -> bool:
    if not url:
        return False
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    if not host or "." not in host:
        return False
    blocked_hosts = {
        "wikipedia.org",
        "www.wikipedia.org",
        "bbb.org",
        "www.bbb.org",
        "opencorporates.com",
        "www.opencorporates.com",
        "google.com",
        "www.google.com",
        "yelp.com",
        "www.yelp.com",
        "tripadvisor.com",
        "www.tripadvisor.com",
        "yellowpages.com",
        "www.yellowpages.com",
    }
    if host in blocked_hosts:
        return False
    return True


def _build_contact_retry_queries(
    companies: list[dict[str, Any]], geography: str, max_companies: int = 6
) -> list[str]:
    targets = [
        c
        for c in companies
        if not str(c.get("website") or "").strip() or not str(c.get("phone") or "").strip()
    ][: max(1, max_companies)]

    queries: list[str] = []
    for c in targets:
        name = str(c.get("company_name") or "").strip()
        if not name:
            continue
        # One combined contact query per company (avoids rate-limit hammering)
        queries.append(f"{name} {geography} contact phone email")
        if not str(c.get("website") or "").strip():
            queries.append(f"{name} {geography} official website")
    return _unique_in_order(queries)


def _apply_contact_retry_rows(
    companies: list[dict[str, Any]], rows: list[dict[str, str]]
) -> tuple[list[dict[str, Any]], int]:
    if not companies or not rows:
        return companies, 0

    indexed: dict[str, int] = {}
    for idx, c in enumerate(companies):
        key = _normalize_name_key(c.get("company_name") or "")
        if key:
            indexed[key] = idx

    updates = 0
    for row in rows:
        title = str(row.get("title") or "")
        row_key = _normalize_name_key(title)
        if not row_key:
            continue

        match_idx = indexed.get(row_key)
        if match_idx is None:
            for name_key, idx in indexed.items():
                if row_key in name_key or name_key in row_key:
                    match_idx = idx
                    break
        if match_idx is None:
            continue

        c = companies[match_idx]
        changed = False

        candidate_url = str(row.get("url") or "").strip()
        if _is_useful_business_url(candidate_url) and not str(c.get("website") or "").strip():
            c["website"] = candidate_url
            c["evidence_url"] = c.get("evidence_url") or candidate_url
            changed = True

        if not str(c.get("phone") or "").strip():
            phone = _extract_phone_text(str(row.get("snippet") or ""))
            if phone:
                c["phone"] = phone
                changed = True

        # Extract email from snippet (Nominatim extratags or scraped text)
        if not str(c.get("email") or "").strip():
            snippet = str(row.get("snippet") or "")
            # Check for Nominatim structured "email=..." first
            email_match = re.search(r"email=([^\s|]+)", snippet)
            if email_match:
                c["email"] = email_match.group(1)
                changed = True
            else:
                found_email = _extract_email_text(snippet)
                if found_email:
                    c["email"] = found_email
                    changed = True

        if changed:
            c["source"] = c.get("source") or row.get("source") or "contact_retry"
            updates += 1

    return companies, updates


def _broaden_product_to_business_types(product: str | None, market: str, category: str) -> list[str]:
    """Turn a niche product term into broader business-type terms that search
    backends (Nominatim, directories) can actually find.

    Works across all categories — food, saas, healthcare, industrial, services, general.
    """
    broad: list[str] = []
    if not product:
        return broad
    p = product.lower().strip()
    m = market.lower().strip()

    # ── Food ──
    _food_map: dict[str, list[str]] = {
        "brisket": ["BBQ restaurant", "barbecue restaurant", "smokehouse", "BBQ"],
        "pulled pork": ["BBQ restaurant", "barbecue restaurant", "smokehouse"],
        "ribs": ["BBQ restaurant", "barbecue restaurant", "rib house"],
        "smoked meat": ["BBQ restaurant", "smokehouse", "barbecue"],
        "wings": ["wing restaurant", "sports bar", "chicken restaurant"],
        "pizza": ["pizza restaurant", "pizzeria"],
        "sushi": ["sushi restaurant", "Japanese restaurant"],
        "tacos": ["taco restaurant", "Mexican restaurant", "taqueria"],
        "ramen": ["ramen restaurant", "Japanese restaurant", "noodle shop"],
        "pho": ["pho restaurant", "Vietnamese restaurant"],
        "burger": ["burger restaurant", "hamburger restaurant", "grill"],
        "steak": ["steakhouse", "steak restaurant", "grill"],
        "seafood": ["seafood restaurant", "fish market"],
        "bagel": ["bagel shop", "bakery", "deli"],
        "donut": ["donut shop", "bakery"],
        "croissant": ["bakery", "pastry shop", "cafe"],
        "espresso": ["coffee shop", "cafe", "espresso bar"],
        "juice": ["juice bar", "smoothie shop", "cafe"],
        "ice cream": ["ice cream shop", "creamery", "dessert shop"],
        "acai": ["acai bowl shop", "smoothie shop", "health food"],
    }

    # ── SaaS / Tech ──
    _saas_map: dict[str, list[str]] = {
        "crm": ["CRM software company", "sales software", "customer management platform"],
        "erp": ["ERP software company", "enterprise software", "business management software"],
        "analytics": ["analytics platform", "data analytics company", "business intelligence"],
        "chatbot": ["chatbot company", "conversational AI", "customer support software"],
        "email marketing": ["email marketing platform", "marketing automation", "newsletter software"],
        "project management": ["project management software", "task management tool", "collaboration platform"],
        "accounting": ["accounting software", "bookkeeping software", "financial software"],
        "payroll": ["payroll software", "HR software", "workforce management"],
        "scheduling": ["scheduling software", "appointment booking", "calendar software"],
        "invoicing": ["invoicing software", "billing platform", "payment software"],
        "ecommerce": ["ecommerce platform", "online store builder", "shopping cart software"],
    }

    # ── Healthcare ──
    _healthcare_map: dict[str, list[str]] = {
        "dental implant": ["dental clinic", "implant dentist", "oral surgery"],
        "braces": ["orthodontist", "dental clinic", "orthodontic practice"],
        "physical therapy": ["physical therapy clinic", "rehabilitation center", "PT practice"],
        "chiropractic": ["chiropractor", "chiropractic clinic", "spine clinic"],
        "dermatology": ["dermatology clinic", "skin care clinic", "dermatologist"],
        "optometry": ["optometrist", "eye clinic", "vision center"],
        "mental health": ["mental health clinic", "therapy practice", "counseling center"],
        "fertility": ["fertility clinic", "IVF center", "reproductive health"],
        "veterinary": ["veterinary clinic", "animal hospital", "vet"],
        "pharmacy": ["pharmacy", "drugstore", "compounding pharmacy"],
    }

    # ── Industrial / Manufacturing ──
    _industrial_map: dict[str, list[str]] = {
        "pcb": ["PCB manufacturer", "circuit board company", "electronics manufacturer"],
        "cnc": ["CNC machining", "machine shop", "precision manufacturing"],
        "3d print": ["3D printing service", "additive manufacturing", "prototyping company"],
        "injection mold": ["injection molding company", "plastics manufacturer", "mold maker"],
        "steel": ["steel supplier", "metal fabricator", "steel manufacturer"],
        "bearing": ["bearing manufacturer", "bearing supplier", "industrial parts"],
        "valve": ["valve manufacturer", "valve supplier", "industrial equipment"],
        "sensor": ["sensor manufacturer", "IoT hardware", "electronics company"],
        "drone": ["drone manufacturer", "UAV company", "drone service"],
        "solar panel": ["solar panel manufacturer", "solar company", "renewable energy"],
        "battery": ["battery manufacturer", "energy storage company", "battery supplier"],
    }

    # ── Services ──
    _services_map: dict[str, list[str]] = {
        "seo": ["SEO agency", "digital marketing agency", "search marketing firm"],
        "web design": ["web design agency", "web development company", "digital agency"],
        "branding": ["branding agency", "brand design firm", "creative agency"],
        "tax": ["tax preparation", "CPA firm", "accounting firm"],
        "legal": ["law firm", "legal services", "attorney"],
        "insurance": ["insurance agency", "insurance broker", "insurance company"],
        "real estate": ["real estate agency", "realtor", "property management"],
        "landscaping": ["landscaping company", "lawn care service", "garden service"],
        "plumbing": ["plumber", "plumbing company", "plumbing service"],
        "electrical": ["electrician", "electrical contractor", "electrical service"],
        "hvac": ["HVAC company", "heating and cooling", "air conditioning service"],
        "cleaning": ["cleaning service", "janitorial service", "commercial cleaning"],
        "moving": ["moving company", "movers", "relocation service"],
    }

    # Pick the map for this category
    category_maps: dict[str, dict[str, list[str]]] = {
        "food": _food_map,
        "saas": _saas_map,
        "healthcare": _healthcare_map,
        "industrial": _industrial_map,
        "services": _services_map,
    }

    # Try category-specific map first
    cat_map = category_maps.get(category, {})
    for key, terms in cat_map.items():
        if key in p:
            broad.extend(terms)
            return broad

    # If no specific match, try ALL maps (product might not match detected category)
    if not broad:
        for _cat, _map in category_maps.items():
            if _cat == category:
                continue
            for key, terms in _map.items():
                if key in p:
                    broad.extend(terms)
                    return broad

    # Last resort: if market itself is a useful business-type term, use it
    if not broad and m != p:
        broad.append(market)

    return broad


def _primary_queries(market: str, geography: str, product: str | None) -> list[str]:
    search_term = product or market
    profile = _infer_market_profile(market, product)
    category = profile["category"]

    queries = [
        f"{search_term} {geography}",
        f"{market} {geography}",
        f"{search_term} companies {geography}",
        f"{search_term} providers {geography}",
    ]

    # Broaden niche product terms → searchable business types for ALL categories
    # (e.g. "brisket" → "BBQ restaurant", "crm" → "CRM software company")
    broadened = _broaden_product_to_business_types(product, market, category)
    for broad_term in broadened:
        queries.insert(1, f"{broad_term} {geography}")  # right after first query
    for broad_term in broadened:
        queries.append(f"{broad_term} near {geography}")

    if category == "food":
        queries.extend(
            [
                f"{search_term} {geography} restaurant",
                f"best {search_term} {geography}",
                f"{search_term} catering {geography}",
            ]
        )
    elif category == "saas":
        queries.extend(
            [
                f"{search_term} saas {geography}",
                f"{search_term} software companies {geography}",
                f"best {search_term} tools {geography}",
            ]
        )
    elif category == "healthcare":
        queries.extend(
            [
                f"{search_term} clinics {geography}",
                f"{search_term} medical providers {geography}",
            ]
        )
    elif category == "industrial":
        queries.extend(
            [
                f"{search_term} companies {geography}",
                f"{search_term} manufacturers {geography}",
                f"{search_term} startups {geography}",
                f"{search_term} firms {geography}",
            ]
        )
    elif category == "services":
        queries.extend(
            [
                f"{search_term} agencies {geography}",
                f"{search_term} consulting firms {geography}",
            ]
        )

    # People/contact-focused queries — help find businesses with reachable contacts nearby
    queries.extend(
        [
            f"{search_term} near {geography}",
            f"{search_term} owner {geography}",
            f"{search_term} business contact {geography}",
            f"{search_term} local business {geography}",
        ]
    )

    return _unique_in_order([q.strip() for q in queries if q.strip()])


def _ai_search_hints(market: str, geography: str, product: str | None) -> tuple[str, str]:
    search_term = product or market
    category = _infer_market_profile(market, product)["category"]

    if category == "food":
        return (
            "Yelp, Google Maps, YellowPages, TripAdvisor, official business websites",
            f'"{search_term} {geography}", "best {search_term} {geography}", "{search_term} catering {geography}"',
        )
    if category == "saas":
        return (
            "official company websites, Product Hunt, Crunchbase, G2 listings, LinkedIn company pages",
            f'"{search_term} saas {geography}", "{search_term} software company {geography}", "{search_term} startup {geography}"',
        )
    if category == "healthcare":
        return (
            "healthcare provider directories, official practice websites, local business listings",
            f'"{search_term} clinic {geography}", "{search_term} medical provider {geography}"',
        )
    if category == "industrial":
        return (
            "manufacturer/supplier directories, official company websites, business registries",
            f'"{search_term} manufacturer {geography}", "{search_term} supplier {geography}"',
        )
    if category == "services":
        return (
            "agency/freelancer directories, official firm websites, local business listings",
            f'"{search_term} agency {geography}", "{search_term} consulting firm {geography}"',
        )

    return (
        "official company websites, business directories, local listings",
        f'"{search_term} {geography}", "{market} companies {geography}"',
    )


def _find_quality_metrics(companies: list[dict[str, Any]]) -> dict[str, int]:
    total = len(companies)
    with_website = 0
    with_phone = 0
    with_location = 0
    for c in companies:
        if str(c.get("website") or c.get("evidence_url") or "").strip():
            with_website += 1
        if str(c.get("phone") or "").strip():
            with_phone += 1
        if str(c.get("location") or "").strip():
            with_location += 1
    return {
        "total": total,
        "with_website": with_website,
        "with_phone": with_phone,
        "with_location": with_location,
    }


def _has_contact_form_or_email_domain(url: str) -> bool:
    if not url:
        return False
    low = url.lower()
    return any(tok in low for tok in ("contact", "about", "support", "help"))


def _contactability_score(companies: list[dict[str, Any]]) -> dict[str, Any]:
    score_total = 0
    max_total = max(1, len(companies) * 7)

    website_count = 0
    phone_count = 0
    valid_domain_count = 0
    contact_hint_count = 0

    for c in companies:
        website = str(c.get("website") or c.get("evidence_url") or "").strip()
        phone = str(c.get("phone") or "").strip()

        if website:
            website_count += 1
            score_total += 2
            try:
                host = (urlparse(website).netloc or "").lower()
                if host and host not in {"", "wikipedia.org", "www.wikipedia.org", "bbb.org", "www.bbb.org", "opencorporates.com", "www.opencorporates.com"}:
                    valid_domain_count += 1
                    score_total += 2
            except Exception:
                pass

            if _has_contact_form_or_email_domain(website):
                contact_hint_count += 1
                score_total += 1

        if phone:
            phone_count += 1
            score_total += 2

    score_100 = int(round((score_total / max_total) * 100))
    score_100 = max(0, min(100, score_100))
    return {
        "score": score_100,
        "website_count": website_count,
        "phone_count": phone_count,
        "valid_domain_count": valid_domain_count,
        "contact_hint_count": contact_hint_count,
    }


def _quality_gate_thresholds(market: str, product: str | None) -> dict[str, int]:
    profile = _infer_market_profile(market, product)
    category = profile["category"]
    if category == "food":
        return {"min_total": 5, "min_with_website": 2, "min_contactability": 40}
    if category == "saas":
        return {"min_total": 6, "min_with_website": 4, "min_contactability": 55}
    if category == "healthcare":
        return {"min_total": 4, "min_with_website": 2, "min_contactability": 45}
    if category == "industrial":
        return {"min_total": 4, "min_with_website": 2, "min_contactability": 45}
    if category == "services":
        return {"min_total": 5, "min_with_website": 3, "min_contactability": 50}
    return {"min_total": 4, "min_with_website": 2, "min_contactability": 45}


def _passes_quality_gate(companies: list[dict[str, Any]], market: str, product: str | None) -> tuple[bool, dict[str, Any]]:
    metrics = _find_quality_metrics(companies)
    contactability = _contactability_score(companies)
    thresholds = _quality_gate_thresholds(market, product)
    passed = (
        metrics["total"] >= thresholds["min_total"]
        and metrics["with_website"] >= thresholds["min_with_website"]
        and contactability["score"] >= thresholds["min_contactability"]
    )
    info = {
        "metrics": metrics,
        "contactability": contactability,
        "thresholds": thresholds,
    }
    return passed, info


def _build_retry_queries(market: str, geography: str, product: str | None) -> list[str]:
    search_term = product or market
    profile = _infer_market_profile(market, product)
    category = profile["category"]
    retries = [
        f"{search_term} near {geography}",
        f"{search_term} business {geography}",
        f"{market} companies {geography}",
        f"best {search_term} in {geography}",
    ]

    if category == "food":
        retries.extend(
            [
                f"{search_term} catering {geography}",
                f"{search_term} restaurant {geography}",
            ]
        )
    elif category == "saas":
        retries.extend(
            [
                f"{search_term} saas {geography}",
                f"{search_term} software companies {geography}",
                f"{search_term} startup {geography}",
                f"{search_term} platform {geography}",
            ]
        )
    elif category == "healthcare":
        retries.extend(
            [
                f"{search_term} clinic {geography}",
                f"{search_term} medical practice {geography}",
                f"{search_term} healthcare provider {geography}",
            ]
        )
    elif category == "industrial":
        retries.extend(
            [
                f"{search_term} manufacturer {geography}",
                f"{search_term} company {geography}",
                f"{search_term} startup {geography}",
                f"{search_term} firm {geography}",
            ]
        )
    elif category == "services":
        retries.extend(
            [
                f"{search_term} agency {geography}",
                f"{search_term} consulting firm {geography}",
                f"{search_term} professional services {geography}",
            ]
        )

    return _unique_in_order([q.strip() for q in retries if q.strip()])


def _heuristic_qualification(
    companies: list[tuple[Any, Any, Any, Any, Any, Any]],
    market: str,
    product: str | None,
) -> list[dict[str, Any]]:
    profile = _infer_market_profile(market, product)
    positive_tokens = set(profile.get("positive_tokens") or set()) | set(profile.get("tokens") or set())
    positive_tokens = {t for t in positive_tokens if len(t) >= 3}
    if not positive_tokens:
        positive_tokens = {"company", "business", "service", "provider"}

    results: list[dict[str, Any]] = []
    for c in companies:
        company_id, company_name, notes, _phone, website, location = c
        text = " ".join(
            part.lower() for part in [str(company_name or ""), str(notes or ""), str(website or ""), str(location or "")]
        )

        hits = sum(1 for token in positive_tokens if token in text)
        score = min(95, 35 + hits * 18)

        if hits >= 2:
            status = "qualified"
            priority = "high" if hits >= 4 else "medium"
        elif hits == 1:
            status = "qualified"
            priority = "medium"
        else:
            status = "new"
            priority = "low"

        volume_estimate = None
        volume_unit = None
        if status == "qualified":
            volume_estimate = 900 if priority == "high" else 450
            volume_unit = "weekly deliveries"

        results.append(
            {
                "company_id": str(company_id),
                "status": status,
                "score": score,
                "priority": priority,
                "volume_estimate": volume_estimate,
                "volume_unit": volume_unit,
                "notes": f"Heuristic qualification (keyword matches={hits})",
            }
        )
    return results


def _normalize_qualification_status(status: Any) -> str:
    raw = str(status or "").strip().lower().replace("-", "_").replace(" ", "_")
    if raw in {"qualified", "contacted", "interested", "not_interested", "new"}:
        return raw
    if raw in {"not_relevant", "irrelevant", "disqualified", "reject", "rejected"}:
        return "not_interested"
    if raw in {"uncertain", "unknown", "maybe", "review", "needs_review"}:
        return "new"
    return "new"


def _normalize_priority(priority: Any, score: int) -> str:
    raw = str(priority or "").strip().lower()
    if raw in {"high", "medium", "low"}:
        return raw
    if score >= 80:
        return "high"
    if score >= 55:
        return "medium"
    return "low"


def _clamp_score(value: Any) -> int:
    parsed = int(_to_float(value) or 0)
    return max(0, min(100, parsed))


class Agent:
    """
    Market Validation Pipeline Agent.

    Pipeline: validate() -> find() -> qualify() -> enrich()/enrich_all().
    AI calls dispatched via claude or opencode CLI on PATH.
    """
    
    def __init__(self, research_id: str | None = None, root: str | Path = "."):
        self.research_id = research_id
        self.root = Path(root).resolve()
        
        from market_validation.research import PROJECT_ROOT
        if str(self.root).endswith("market_validation"):
            self.root = PROJECT_ROOT
        
        self.last_result: dict[str, Any] = {}
    
    @staticmethod
    def _detect_agent() -> str:
        """
        Detect best available AI agent to use for web-search prompts.
        Preference order: claude (Claude Code CLI) → opencode → none
        """
        import shutil
        if shutil.which("claude"):
            return "claude"
        if shutil.which("opencode"):
            return "opencode"
        return "none"

    @staticmethod
    def _parse_json_from_text(text: str) -> dict[str, Any] | None:
        """Extract the first valid JSON object or array from arbitrary text."""
        # Strip fenced code blocks
        if "```json" in text:
            text = text.split("```json", 1)[1].split("```", 1)[0]
        elif "```" in text:
            text = text.split("```", 1)[1].split("```", 1)[0]
        text = text.strip()

        start = text.find("{")
        arr_start = text.find("[")
        if arr_start >= 0 and (start < 0 or arr_start < start):
            start = arr_start

        if start < 0:
            return None

        if text[start] == "{":
            end = text.rfind("}")
            if end > start:
                try:
                    return json.loads(text[start : end + 1])
                except json.JSONDecodeError:
                    pass
        elif text[start] == "[":
            end = text.rfind("]")
            if end > start:
                try:
                    return {"companies": json.loads(text[start : end + 1])}
                except json.JSONDecodeError:
                    pass
        return None

    def _run_claude(self, prompt: str, timeout: int = 180) -> dict[str, Any]:
        """Run via Claude Code CLI (`claude -p`)."""
        try:
            result = subprocess.run(
                ["claude", "-p", prompt, "--output-format", "text"],
                capture_output=True, text=True, timeout=timeout,
                cwd=str(self.root),
            )
        except subprocess.TimeoutExpired:
            return {"result": "error", "error": "Timeout (claude)"}
        if result.returncode != 0:
            return {"result": "error", "error": result.stderr or "claude failed"}
        parsed = self._parse_json_from_text(result.stdout.strip())
        return parsed if parsed else {"result": "error", "error": "No JSON (claude)"}

    def _run_opencode(self, prompt: str, timeout: int = 180) -> dict[str, Any]:
        """Run via opencode CLI."""
        try:
            result = subprocess.run(
                ["opencode", "run", "--dangerously-skip-permissions", "--dir", str(self.root), prompt],
                capture_output=True, text=True, timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            return {"result": "error", "error": "Timeout (opencode)"}
        if result.returncode != 0:
            return {"result": "error", "error": result.stderr or "opencode failed"}
        parsed = self._parse_json_from_text(result.stdout.strip())
        return parsed if parsed else {"result": "error", "error": "No JSON (opencode)"}

    def _run(self, prompt: str, timeout: int = 180) -> dict[str, Any]:
        """
        Run a prompt via the best available AI agent.
        Tries: claude (Claude Code CLI) → opencode → error.
        Both CLIs can browse the web, so either can handle research queries.
        """
        agent = self._detect_agent()

        if agent == "claude":
            result = self._run_claude(prompt, timeout=timeout)
            if result.get("result") != "error":
                return result
            # claude failed — try opencode as fallback
            if self._detect_agent() != "none":
                import shutil
                if shutil.which("opencode"):
                    return self._run_opencode(prompt, timeout=timeout)
            return result

        if agent == "opencode":
            return self._run_opencode(prompt, timeout=timeout)

        return {"result": "error", "error": "No AI agent available (install claude or opencode)"}
    
    def validate(self, market: str, geography: str, product: str | None = None, archetype: str | None = None) -> dict[str, Any]:
        """
        STEP 0: Validate the market before company discovery.

        Runs four sub-modules (sizing, demand, competition, signals) and
        produces a validation scorecard with a go/no-go verdict.

        Each sub-module gathers data from free web sources and optionally
        uses AI synthesis for richer analysis.
        """
        from market_validation.market_sizing import estimate_market_size
        from market_validation.demand_analysis import analyze_demand
        from market_validation.competitive_landscape import analyze_competition
        from market_validation.market_signals import gather_market_signals
        from market_validation.validation_scorecard import compute_scorecard
        from market_validation.research import (
            create_validation, update_validation,
        )
        from market_validation.unit_economics import estimate_unit_economics
        from market_validation.porters_five_forces import analyze_porters_five_forces
        from market_validation.timing_analysis import analyze_timing
        from market_validation.customer_segments import identify_customer_segments

        print(f"[validate] Starting market validation: {product or market} in {geography}")

        # Detect archetype first (synchronous) — caller may override
        from market_validation.market_archetype import detect_archetype
        if archetype:
            archetype_key = archetype
            archetype_confidence = 100
        else:
            archetype_key, archetype_confidence = detect_archetype(market, product)
        print(f"[validate]   Archetype: {archetype_key} (confidence {archetype_confidence}%)")

        # Create validation record
        val = create_validation(
            research_id=self.research_id,
            market=market,
            geography=geography,
            root=self.root,
        )
        vid = val["validation_id"]
        update_validation(vid, {"status": "running"}, root=self.root)

        # Run all 4 sub-modules in parallel (each does its own web searches + AI call)
        from concurrent.futures import ThreadPoolExecutor, as_completed

        _defaults: dict[str, Any] = {
            "sizing": {},
            "demand": {"demand_score": 50, "demand_trend": "stable"},
            "competition": {"competitive_intensity": 50, "market_concentration": "moderate"},
            "signals": {"regulatory_risks": [], "technology_maturity": "growing"},
            "unit_economics": {},
            "porters": {},
            "timing": {},
            "customer_segments": {},
        }
        _tasks = {
            "sizing": (estimate_market_size, (market, geography, product), {"run_ai": self._run}),
            "demand": (analyze_demand, (market, geography, product), {"run_ai": self._run, "archetype": archetype_key}),
            "competition": (analyze_competition, (market, geography, product), {"run_ai": self._run}),
            "signals": (gather_market_signals, (market, geography, product), {"run_ai": self._run}),
            "unit_economics": (estimate_unit_economics, (market, geography, product), {"archetype": archetype_key, "run_ai": self._run}),
            "porters": (analyze_porters_five_forces, (market, geography, product), {"run_ai": self._run}),
            "timing": (analyze_timing, (market, geography, product), {"archetype": archetype_key, "run_ai": self._run}),
            "customer_segments": (identify_customer_segments, (market, geography, product), {"archetype": archetype_key, "run_ai": self._run}),
        }
        _labels = {
            "sizing": "Estimating market size (TAM/SAM/SOM)",
            "demand": "Analyzing demand signals",
            "competition": "Mapping competitive landscape",
            "signals": "Gathering market signals",
            "unit_economics": "Estimating unit economics",
            "porters": "Analyzing Porter's 5 forces",
            "timing": "Assessing market timing",
            "customer_segments": "Identifying customer segments",
        }
        results_map: dict[str, Any] = {}
        print("[validate]   Running 8 modules in parallel...")
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(fn, *args, **kwargs): key
                for key, (fn, args, kwargs) in _tasks.items()
            }
            for future in as_completed(futures):
                key = futures[future]
                try:
                    results_map[key] = future.result()
                    print(f"[validate]   ✓ {_labels[key]}")
                except Exception as e:
                    print(f"[validate]   ! {_labels[key]} failed: {e}")
                    results_map[key] = _defaults[key]

        sizing = results_map["sizing"]
        demand = results_map["demand"]
        competition = results_map["competition"]
        signals = results_map["signals"]
        unit_economics = results_map["unit_economics"]
        porters = results_map["porters"]
        timing = results_map["timing"]
        customer_segments = results_map["customer_segments"]

        # Re-run porters with competition context if it completed without it
        if porters and not porters.get("structural_attractiveness") and competition:
            try:
                porters = analyze_porters_five_forces(market, geography, product,
                                                      existing_competition=competition,
                                                      run_ai=self._run)
            except Exception:
                pass

        # Re-run timing with signals context if it completed without a score
        if timing and not timing.get("timing_score") and signals:
            try:
                timing = analyze_timing(market, geography, product,
                                       archetype=archetype_key,
                                       signals=signals,
                                       run_ai=self._run)
            except Exception:
                pass

        # Compute scorecard
        print("[validate]   Computing scorecard...")
        scorecard = compute_scorecard(
            sizing, demand, competition, signals,
            run_ai=self._run,
            unit_economics=unit_economics,
            porters=porters,
            timing=timing,
            customer_segments=customer_segments,
            archetype=archetype_key,
        )

        # Log what each module returned (helps diagnose missing fields)
        def _log_module(name: str, result: dict) -> None:
            non_none = {k: v for k, v in result.items() if v is not None and v != [] and v != {}}
            none_keys = [k for k, v in result.items() if v is None]
            print(f"[validate]   {name}: {len(non_none)} fields populated"
                  + (f", {len(none_keys)} None ({none_keys})" if none_keys else ""))
        _log_module("sizing", sizing)
        _log_module("demand", demand)
        _log_module("competition", competition)
        _log_module("signals", signals)
        _log_module("unit_economics", unit_economics)
        _log_module("porters", porters)
        _log_module("timing", timing)
        _log_module("customer_segments", customer_segments)

        # Store everything in the database
        db_fields: dict[str, Any] = {"status": "complete"}
        # Sizing
        for key in ("tam_low", "tam_high", "tam_confidence", "tam_sources",
                     "sam_low", "sam_high", "sam_confidence", "sam_sources",
                     "som_low", "som_high", "som_confidence", "som_sources"):
            if key in sizing and sizing[key] is not None:
                db_fields[key] = sizing[key]
        # Demand
        for key in ("demand_score", "demand_trend", "demand_seasonality",
                     "demand_pain_points", "demand_sources"):
            if key in demand and demand[key] is not None:
                db_fields[key] = demand[key]
        # Competition
        for key in ("competitive_intensity", "competitor_count", "market_concentration",
                     "direct_competitors", "indirect_competitors", "funding_signals",
                     "differentiation_opportunities"):
            if key in competition and competition[key] is not None:
                db_fields[key] = competition[key]
        # Signals
        for key in ("job_posting_volume", "news_sentiment", "regulatory_risks",
                     "technology_maturity", "signals_data"):
            if key in signals and signals[key] is not None:
                db_fields[key] = signals[key]
        # Scorecard
        db_fields.update({
            "market_attractiveness": scorecard.get("market_attractiveness"),
            "competitive_score": scorecard.get("competitive_score"),
            "demand_validation": scorecard.get("demand_validation"),
            "risk_score": scorecard.get("risk_score"),
            "overall_score": scorecard.get("overall_score"),
            "verdict": scorecard.get("verdict"),
            "verdict_reasoning": scorecard.get("verdict_reasoning"),
        })

        # Archetype
        db_fields["archetype"] = archetype_key
        db_fields["archetype_confidence"] = archetype_confidence
        db_fields["archetype_label"] = scorecard.get("archetype_label", "")

        # Unit economics
        for key in ("gross_margin_low", "gross_margin_high", "gross_margin_confidence",
                    "cac_estimate_low", "cac_estimate_high", "ltv_estimate_low",
                    "ltv_estimate_high", "payback_months", "unit_economics_score"):
            if key in unit_economics and unit_economics[key] is not None:
                db_fields[key] = unit_economics[key]
        if unit_economics:
            db_fields["unit_economics_data"] = unit_economics

        # Porter's 5 forces
        for key in ("supplier_power", "buyer_power", "substitute_threat",
                    "entry_barrier_score", "rivalry_score", "structural_attractiveness"):
            if key in porters and porters[key] is not None:
                db_fields[key] = porters[key]
        if porters:
            db_fields["porters_data"] = porters

        # Timing
        for key in ("timing_score", "timing_verdict"):
            if key in timing and timing[key] is not None:
                db_fields[key] = timing[key]
        if timing.get("enablers"):
            db_fields["timing_enablers"] = timing["enablers"]
        if timing.get("headwinds"):
            db_fields["timing_headwinds"] = timing["headwinds"]

        # Customer segments
        if customer_segments:
            db_fields["customer_segments_data"] = customer_segments
            if customer_segments.get("icp_clarity") is not None:
                db_fields["icp_clarity"] = customer_segments["icp_clarity"]
            if customer_segments.get("primary_segment"):
                seg = customer_segments["primary_segment"]
                db_fields["primary_segment"] = seg.get("name", "") if isinstance(seg, dict) else str(seg)

        # Actionable output from scorecard
        if scorecard.get("next_steps"):
            db_fields["next_steps"] = scorecard["next_steps"]
        if scorecard.get("key_risks"):
            db_fields["key_risks"] = scorecard["key_risks"]
        if scorecard.get("key_success_factors"):
            db_fields["key_success_factors"] = scorecard["key_success_factors"]
        if scorecard.get("archetype_red_flags"):
            db_fields["archetype_red_flags"] = scorecard["archetype_red_flags"]

        update_validation(vid, db_fields, root=self.root)

        verdict = scorecard.get("verdict", "unknown")
        overall = scorecard.get("overall_score", 0)
        _print_validation_summary(market, geography, archetype_key, scorecard, sizing, competition)

        return {
            "result": "ok",
            "validation_id": vid,
            "archetype": archetype_key,
            "sizing": sizing,
            "demand": demand,
            "competition": competition,
            "signals": signals,
            "unit_economics": unit_economics,
            "porters": porters,
            "timing": timing,
            "customer_segments": customer_segments,
            "scorecard": scorecard,
        }

    def find(self, market: str, geography: str, product: str | None = None) -> dict[str, Any]:
        """
        STEP 1: Find companies in a market.

        Searches web for businesses matching the criteria.
        Uses multi-backend search + source configs + opencode AI.
        Stores results in database.
        """
        search_term = product or market
        all_companies: list[dict[str, Any]] = []
        sources_used: list[str] = []
        source_health: list[dict[str, Any]] = []
        profile = _infer_market_profile(market, product)

        # Detect market archetype for tailored search and qualification prompts
        from market_validation.market_archetype import detect_archetype
        _archetype_key, _archetype_conf = detect_archetype(market, product)
        _arch_search_ctx = _archetype_search_context(_archetype_key, market, geography, product)
        print(f"[find] archetype={_archetype_key} (confidence={_archetype_conf}%)")

        # If the heuristic profile has low confidence, ask the LLM to generate
        # a proper search strategy instead of falling back to generic queries.
        ai_strategy: dict[str, Any] | None = None
        if profile.get("confidence", 100) < 50:
            print(f"[find] heuristic confidence {profile.get('confidence')}% — asking AI for search strategy...")
            ai_strategy = _ai_search_strategy(market, geography, product, self._run, archetype_context=_arch_search_ctx)
            if ai_strategy:
                btype = ai_strategy.get("business_type", market)
                print(f"[find] AI strategy: business_type='{btype}', {len(ai_strategy.get('queries', []))} queries")
                source_health.append({
                    "stage": "ai_search_strategy",
                    "business_type": btype,
                    "queries": ai_strategy.get("queries", []),
                    "status": "ok",
                })

        source_health.append(
            {
                "stage": "market_profile",
                "category": profile.get("category"),
                "confidence": profile.get("confidence"),
                "tokens": sorted(list(profile.get("tokens") or []))[:20],
                "status": "ok",
            }
        )

        # First, try direct free search using OSM-backed multi_search.
        # If AI provided a strategy, use those queries; otherwise use heuristic queries.
        if ai_strategy and ai_strategy.get("queries"):
            search_queries = ai_strategy["queries"]
        else:
            search_queries = _primary_queries(market=market, geography=geography, product=product)
        
        for query in search_queries:
            search_results = _try_multi_search(query, 15, geography=geography)
            backend_counts = _summarize_backends(search_results)
            source_health.append(
                {
                    "stage": "built_in_search",
                    "query": query,
                    "backends": backend_counts,
                    "results": len(search_results),
                    "status": "ok" if search_results else "empty",
                }
            )
            if search_results:
                sources_used.append("multi_search")
                for r in search_results:
                    all_companies.append(_extract_contact_from_search_result(r))
        
        # Second, try scraping URLs from source config
        source_results = _try_source_urls(market, geography, product)
        source_health.append(
            {
                "stage": "source_config",
                "queries_or_urls": "configured",
                "results": len(source_results),
                "status": "ok" if source_results else "empty",
            }
        )
        if source_results:
            sources_used.append("source_config")
            for r in source_results:
                data = r.get("data", {})
                if data.get("business_name"):
                    _sc_desc = f"{data.get('rating', '')} - {data.get('reviews_count', '')} reviews"
                    all_companies.append({
                        "company_name": data.get("business_name", ""),
                        "website": data.get("website", ""),
                        "location": data.get("address", ""),
                        "phone": data.get("phone", "") or _extract_phone_text(_sc_desc),
                        "email": data.get("email", "") or _extract_email_text(_sc_desc),
                        "description": _sc_desc,
                        "source": r.get("source", "config"),
                    })
        
        # Third, try supplementary backends (BBB, Manta, etc.) once with the broadest query
        supp_query = f"{market} {geography}"
        supp_results = _try_supplementary_search(supp_query, 10)
        source_health.append(
            {
                "stage": "supplementary_search",
                "query": supp_query,
                "results": len(supp_results),
                "backends": _summarize_backends(supp_results),
                "status": "ok" if supp_results else "empty",
            }
        )
        if supp_results:
            sources_used.append("supplementary")
            for r in supp_results:
                all_companies.append(_extract_contact_from_search_result(r))

        _ai_junk = ai_strategy.get("junk_signals", []) if ai_strategy else []
        _ai_real = ai_strategy.get("real_business_signals", []) if ai_strategy else []
        # If AI gave us a business_type, use it as an additional real signal and
        # override the search_term so filtering doesn't drop legit results.
        if ai_strategy and ai_strategy.get("business_type"):
            _ai_real += ai_strategy["business_type"].lower().split()

        unique_companies = _dedupe_companies(_normalize_companies(all_companies))
        unique_companies = _filter_relevant_companies(
            unique_companies, market=market, product=product,
            extra_junk_signals=_ai_junk, extra_real_signals=_ai_real,
        )

        # Deterministic quality gate + retry query pass
        quality_passed, quality_info = _passes_quality_gate(unique_companies, market=market, product=product)
        source_health.append(
            {
                "stage": "quality_gate_initial",
                "status": "pass" if quality_passed else "fail",
                "metrics": quality_info.get("metrics"),
                "contactability": quality_info.get("contactability"),
                "thresholds": quality_info.get("thresholds"),
            }
        )

        if not quality_passed:
            retry_queries = _build_retry_queries(market=market, geography=geography, product=product)
            retry_companies: list[dict[str, Any]] = []
            for query in retry_queries:
                retry_rows = _try_multi_search(query, 10, geography=geography)
                backend_counts = _summarize_backends(retry_rows)
                source_health.append(
                    {
                        "stage": "quality_gate_retry",
                        "query": query,
                        "backends": backend_counts,
                        "results": len(retry_rows),
                        "status": "ok" if retry_rows else "empty",
                    }
                )
                for r in retry_rows:
                    retry_companies.append(_extract_contact_from_search_result(r))

            if retry_companies:
                sources_used.append("quality_gate_retry")
                unique_companies = _dedupe_companies(
                    _normalize_companies(unique_companies + retry_companies)
                )
                unique_companies = _filter_relevant_companies(unique_companies, market=market, product=product)

            retry_passed, retry_info = _passes_quality_gate(unique_companies, market=market, product=product)
            source_health.append(
                {
                    "stage": "quality_gate_final",
                    "status": "pass" if retry_passed else "fail",
                    "metrics": retry_info.get("metrics"),
                    "contactability": retry_info.get("contactability"),
                    "thresholds": retry_info.get("thresholds"),
                }
            )

            # Profile switching: primary profile underperformed → try adjacent profiles
            if not retry_passed:
                adj_cats = _ADJACENT_PROFILES.get(profile["category"], [])
                source_health.append(
                    {
                        "stage": "profile_switch_check",
                        "original_category": profile["category"],
                        "confidence": profile.get("confidence", 50),
                        "adjacent_profiles": adj_cats,
                        "status": "attempting" if adj_cats else "skipped",
                    }
                )
                for adj_cat in adj_cats:
                    adj_queries = _queries_for_adjacent_profile(market, geography, product, adj_cat)
                    adj_companies: list[dict[str, Any]] = []
                    for query in adj_queries:
                        rows = _try_multi_search(query, 10, geography=geography)
                        source_health.append(
                            {
                                "stage": "profile_switch_search",
                                "adjacent_category": adj_cat,
                                "query": query,
                                "backends": _summarize_backends(rows),
                                "results": len(rows),
                                "status": "ok" if rows else "empty",
                            }
                        )
                        for r in rows:
                            adj_companies.append(_extract_contact_from_search_result(r))

                    if adj_companies:
                        merged = _dedupe_companies(
                            _normalize_companies(unique_companies + adj_companies)
                        )
                        merged = _filter_relevant_companies(merged, market=market, product=product)
                        adj_passed, adj_info = _passes_quality_gate(merged, market=market, product=product)
                        source_health.append(
                            {
                                "stage": "profile_switch_gate",
                                "adjacent_category": adj_cat,
                                "status": "pass" if adj_passed else "fail",
                                "metrics": adj_info.get("metrics"),
                                "contactability": adj_info.get("contactability"),
                                "added_companies": len(merged) - len(unique_companies),
                            }
                        )
                        # Accept the merged pool whether or not the gate fully passed —
                        # more candidates is always better heading into contactability retry.
                        if len(merged) > len(unique_companies):
                            unique_companies = merged
                            sources_used.append(f"profile_switch_{adj_cat}")
                        if adj_passed:
                            break  # gate passed — no need to try further adjacent profiles

        # Secondary gate: contactability enrichment pass using deterministic contact queries
        secondary_passed, secondary_info = _passes_quality_gate(unique_companies, market=market, product=product)
        source_health.append(
            {
                "stage": "quality_gate_contactability_initial",
                "status": "pass" if secondary_passed else "fail",
                "metrics": secondary_info.get("metrics"),
                "contactability": secondary_info.get("contactability"),
                "thresholds": secondary_info.get("thresholds"),
            }
        )

        if not secondary_passed:
            contact_queries = _build_contact_retry_queries(unique_companies, geography=geography)
            contact_rows: list[dict[str, str]] = []
            for query in contact_queries:
                rows = _try_multi_search(query, 8, geography=geography)
                contact_rows.extend(rows)
                source_health.append(
                    {
                        "stage": "quality_gate_contactability_retry",
                        "query": query,
                        "results": len(rows),
                        "backends": _summarize_backends(rows),
                        "status": "ok" if rows else "empty",
                    }
                )

            if contact_rows:
                updated_companies, updates = _apply_contact_retry_rows(unique_companies, contact_rows)
                unique_companies = _dedupe_companies(_normalize_companies(updated_companies))
                unique_companies = _filter_relevant_companies(unique_companies, market=market, product=product)
                if updates > 0:
                    sources_used.append("contactability_retry")
                source_health.append(
                    {
                        "stage": "quality_gate_contactability_updates",
                        "status": "ok",
                        "updated_companies": updates,
                    }
                )

            secondary_final_passed, secondary_final_info = _passes_quality_gate(
                unique_companies, market=market, product=product
            )
            source_health.append(
                {
                    "stage": "quality_gate_contactability_final",
                    "status": "pass" if secondary_final_passed else "fail",
                    "metrics": secondary_final_info.get("metrics"),
                    "contactability": secondary_final_info.get("contactability"),
                    "thresholds": secondary_final_info.get("thresholds"),
                }
            )
        
        # Determine if quality is poor enough to warrant opencode supplementation.
        # Trigger opencode when: no direct results OR final quality gate still failing
        # with fewer than the minimum expected companies.
        thresholds = _quality_gate_thresholds(market, product)
        final_quality_passed, _ = _passes_quality_gate(unique_companies, market=market, product=product)
        needs_opencode = (
            not unique_companies
            or (not final_quality_passed and len(unique_companies) < max(thresholds["min_total"] * 2, 10))
        )

        if needs_opencode:
            ai_sources, ai_queries = _ai_search_hints(market=market, geography=geography, product=product)
            prompt = f"""Find at least 15-20 businesses in {geography} related to {search_term}.

{_arch_search_ctx}

Be thorough — check multiple sources, neighborhoods, and related business types.

For each business, find:
- Company name, website, address, phone
- What they sell/offer related to {search_term}
- How established they are (reviews, years in business)

Search sources: {ai_sources}
Search queries: {ai_queries}

Return JSON:
{{
  "companies": [
    {{
      "company_name": "Business Name",
      "website": "https://...",
      "location": "Address",
      "phone": "555-123-4567",
      "description": "What they do",
      "evidence_url": "https://source..."
    }}
  ]
}}"""

            ai_agent = self._detect_agent()
            prompt += "\n\nIMPORTANT: Only include real operating businesses with a physical presence or active website. Do NOT include directories, aggregators, review sites, social media pages, or unrelated companies."
            ai_result = self._run(prompt, timeout=180)
            source_health.append(
                {
                    "stage": "ai_fallback" if not unique_companies else "ai_supplement",
                    "agent": ai_agent,
                    "results": len(ai_result.get("companies", [])) if isinstance(ai_result, dict) else 0,
                    "status": "ok" if isinstance(ai_result, dict) and ai_result.get("result") != "error" else "error",
                }
            )

            ai_label = f"ai:{ai_agent}"
            if isinstance(ai_result, dict) and ai_result.get("companies"):
                sources_used.append(ai_label)
                if unique_companies:
                    # Merge AI results with what we already have
                    merged = _dedupe_companies(
                        _normalize_companies(unique_companies + ai_result["companies"])
                    )
                    unique_companies = _filter_relevant_companies(merged, market=market, product=product)
                    result = {
                        "result": "ok",
                        "companies": unique_companies,
                        "sources_used": _unique_in_order(sources_used),
                        "method": f"direct_search+{ai_agent}",
                        "source_health": source_health,
                    }
                else:
                    ai_result["method"] = ai_agent
                    ai_result["sources_used"] = sources_used
                    result = ai_result
            elif unique_companies:
                result = {
                    "result": "ok",
                    "companies": unique_companies,
                    "sources_used": _unique_in_order(sources_used),
                    "method": "direct_search",
                    "source_health": source_health,
                }
            else:
                result = {"result": "error", "error": "No companies found"}
        else:
            result = {
                "result": "ok",
                "companies": unique_companies,
                "sources_used": _unique_in_order(sources_used),
                "method": "direct_search",
                "source_health": source_health,
            }
        
        if result.get("result") == "error":
            return result

        companies = _normalize_companies(result.get("companies", []))
        companies = _dedupe_companies(companies)
        companies = _filter_relevant_companies(
            companies, market=market, product=product,
            extra_junk_signals=_ai_junk, extra_real_signals=_ai_real,
        )

        # --- Iterative discovery loop ---
        # If we still have fewer than 15 companies, ask AI to generate new queries
        # based on what we found so far, then search again. Up to 2 rounds.
        _all_queries_used = list(search_queries)
        if not quality_passed:
            _all_queries_used.extend(_build_retry_queries(market=market, geography=geography, product=product))

        _biz_type_hint = (ai_strategy.get("business_type") if ai_strategy else None) or market
        for _iter_round in range(2):
            if len(companies) >= 15:
                break

            _found_names = [c.get("company_name", "") for c in companies if c.get("company_name")]
            _iter_prompt = f"""We found these {len(_found_names)} companies so far: {", ".join(_found_names[:30])}
We already searched: {", ".join(_all_queries_used[:20])}

{_arch_search_ctx}

Generate 5 MORE search queries to find additional {_biz_type_hint} in {geography} that we might have missed.
Think about: different neighborhoods, related business types, supplier directories, industry associations, different search terms.

Return JSON: {{"queries": ["query1", "query2", "query3", "query4", "query5"]}}"""

            _iter_ai_result = self._run(_iter_prompt, timeout=60)
            _iter_queries: list[str] = []
            if isinstance(_iter_ai_result, dict) and _iter_ai_result.get("queries"):
                _iter_queries = [q for q in _iter_ai_result["queries"] if isinstance(q, str)]

            if not _iter_queries:
                source_health.append({
                    "stage": f"iterative_discovery_round_{_iter_round + 1}",
                    "status": "no_new_queries",
                })
                break

            _all_queries_used.extend(_iter_queries)
            _iter_companies: list[dict[str, Any]] = []
            for _iq in _iter_queries:
                _iter_rows = _try_multi_search(_iq, 15, geography=geography)
                source_health.append({
                    "stage": f"iterative_discovery_round_{_iter_round + 1}",
                    "query": _iq,
                    "results": len(_iter_rows),
                    "backends": _summarize_backends(_iter_rows),
                    "status": "ok" if _iter_rows else "empty",
                })
                for r in _iter_rows:
                    _iter_companies.append(_extract_contact_from_search_result(r))

            if _iter_companies:
                sources_used.append(f"iterative_discovery_{_iter_round + 1}")
                companies = _dedupe_companies(
                    _normalize_companies(companies + _iter_companies)
                )
                companies = _filter_relevant_companies(
                    companies, market=market, product=product,
                    extra_junk_signals=_ai_junk, extra_real_signals=_ai_real,
                )
                print(f"[find] Iterative round {_iter_round + 1}: +{len(_iter_companies)} raw → {len(companies)} total after dedupe/filter")
            else:
                print(f"[find] Iterative round {_iter_round + 1}: no new companies found")
                break

        # Claude batch validation — the definitive gate before writing to DB.
        # One call reviews every candidate: confirms relevance, cleans names, dedupes.
        if companies:
            _biz_type = (ai_strategy.get("business_type") if ai_strategy else None) or market
            print(f"[find] Claude pre-save validation: {len(companies)} candidates → business_type='{_biz_type}'")
            companies = _ai_validate_companies(
                companies, market=market, geography=geography,
                business_type=_biz_type, run_ai=self._run,
            )
            print(f"[find] After validation: {len(companies)} companies confirmed")
            source_health.append({
                "stage": "ai_pre_save_validation",
                "candidates_in": len(result.get("companies", [])),
                "confirmed_out": len(companies),
                "status": "ok",
            })

        result["companies"] = companies

        # --- Pre-scrape pass: enrich companies with contact info before saving ---
        companies_to_scrape = [
            c for c in companies
            if c.get("website")
            and c["website"].startswith("http")
            and (not c.get("phone") or not c.get("email"))
        ][:20]

        if companies_to_scrape:
            import time as _time
            from concurrent.futures import ThreadPoolExecutor, as_completed

            from market_validation.web_scraper import quick_scrape

            print(f"[find] Pre-scraping {len(companies_to_scrape)} company websites for contact info...")

            def _safe_scrape(company: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
                try:
                    data = quick_scrape(company["website"])
                    return company, data
                except Exception:
                    return company, {}

            scrape_results: dict[int, dict[str, Any]] = {}
            batch_size = 4
            for batch_start in range(0, len(companies_to_scrape), batch_size):
                batch = companies_to_scrape[batch_start : batch_start + batch_size]
                with ThreadPoolExecutor(max_workers=4) as executor:
                    futures = {
                        executor.submit(_safe_scrape, c): c
                        for c in batch
                    }
                    for future in as_completed(futures, timeout=10):
                        try:
                            comp, data = future.result(timeout=10)
                            if data and not data.get("error"):
                                scrape_results[id(comp)] = data
                        except Exception:
                            pass
                # Delay between batches (skip after last batch)
                if batch_start + batch_size < len(companies_to_scrape):
                    _time.sleep(1)

            enriched = 0
            for c in companies:
                data = scrape_results.get(id(c))
                if not data:
                    continue
                if not c.get("phone") and data.get("phone"):
                    c["phone"] = data["phone"]
                    enriched += 1
                if not c.get("email") and data.get("email"):
                    c["email"] = data["email"]
                    enriched += 1
                if not c.get("description") and data.get("raw_text"):
                    c["description"] = data["raw_text"][:300]

            if enriched:
                print(f"[find] Pre-scrape enriched {enriched} contact fields")
                source_health.append({
                    "stage": "pre_scrape_enrichment",
                    "companies_scraped": len(scrape_results),
                    "fields_enriched": enriched,
                    "status": "ok",
                })

        # Store in database if we have research_id
        if self.research_id:
            from market_validation.research import add_company, _connect, _ensure_schema, resolve_db_path
            db = resolve_db_path(self.root)
            
            with _connect(db) as conn:
                _ensure_schema(conn)
                added = 0
                for c in companies:
                    r = add_company(
                        research_id=self.research_id,
                        company_name=c.get("company_name", "Unknown"),
                        market=market,
                        website=c.get("website"),
                        location=c.get("location"),
                        phone=c.get("phone"),
                        email=c.get("email"),
                        notes=c.get("description"),
                        raw_data=c,
                        root=self.root,
                    )
                    if r.get("result") == "ok":
                        added += 1
                result["companies_added"] = added
                # Persist a JSON summary of source_health on the research row for dashboard visibility
                try:
                    import json as _json
                    conn.execute(
                        "UPDATE researches SET last_source_health = ? WHERE id = ?",
                        (_json.dumps(source_health, ensure_ascii=True), self.research_id),
                    )
                except Exception:
                    # Don't fail the find() if we can't persist source_health
                    pass
        
        self.last_result = result
        result["sources_used"] = _unique_in_order(sources_used)
        result["source_health"] = source_health
        if "result" not in result:
            result["result"] = "ok"
        return result
    
    def qualify(self) -> dict[str, Any]:
        """
        STEP 2: Qualify companies - AI assessment of relevance and volume.
        
        Updates companies in database with priority scores and volume estimates.
        """
        if not self.research_id:
            return {"result": "error", "error": "No research_id set"}

        from market_validation.research import (
            _connect,
            _ensure_schema,
            get_research,
            resolve_db_path,
            update_company,
        )
        db = resolve_db_path(self.root)

        research = get_research(self.research_id, root=self.root)
        if research.get("result") != "ok":
            return {"result": "error", "error": "Research not found"}
        research_market = str(research.get("research", {}).get("market") or "")
        research_product = research.get("research", {}).get("product")

        # Detect archetype for tailored qualification prompts
        from market_validation.market_archetype import detect_archetype
        _qual_archetype_key, _qual_archetype_conf = detect_archetype(research_market, research_product)
        _qual_arch_ctx = _archetype_qualify_context(_qual_archetype_key, research_market, research_product)
        print(f"[qualify] archetype={_qual_archetype_key} (confidence={_qual_archetype_conf}%)")

        # Pull market validation context to sharpen qualification scoring
        market_context = ""
        try:
            from market_validation.research import get_validation_by_research
            val_result = get_validation_by_research(self.research_id, root=self.root)
            if val_result.get("result") == "ok" and val_result.get("validation"):
                v = val_result["validation"]
                verdict = v.get("verdict", "unknown")
                overall = v.get("overall_score", 0)
                demand_trend = v.get("demand_trend", "unknown")
                pain_points = v.get("demand_pain_points") or []
                competitive_intensity = v.get("competitive_intensity", 50)
                wtp = v.get("willingness_to_pay", "unknown")
                market_context = f"""
Market Validation Context (pre-computed):
- Market verdict: {verdict} (overall score: {overall}/100)
- Demand trend: {demand_trend}
- Competitive intensity: {competitive_intensity}/100
- Willingness to pay: {wtp}
- Identified customer pain points: {", ".join(pain_points[:3]) if pain_points else "none identified"}

Use this context to calibrate scores — companies in a {verdict.replace("_", " ")} market should reflect that reality.
"""
        except Exception:
            pass

        with _connect(db) as conn:
            _ensure_schema(conn)
            conn.row_factory = None
            companies = conn.execute(
                """SELECT id, company_name, notes, phone, website, location
                   FROM companies WHERE research_id = ? AND status = 'new'""",
                (self.research_id,)
            ).fetchall()

        if not companies:
            return {"result": "ok", "qualified": 0, "message": "No companies to qualify"}

        company_list = [{"id": str(c[0]), "name": str(c[1]), "notes": c[2], "phone": c[3], "website": c[4], "location": c[5]} for c in companies]

        def _qualify_batch(batch: list[dict]) -> list[dict]:
            prompt = f"""{_qual_arch_ctx}
{market_context}
For each company, assess:
1. Relevance score (0-100): how well do they match the target market?
2. Market potential signals - look for:
   - Growth indicators: expanding, hiring, new locations, investment/funding
   - Pain points: do they have a problem your product could solve?
   - Buying signals: are they spending in this category? Active customers?
   - Urgency: seasonal demand, recent news suggesting immediate need
3. Volume estimate: approximate revenue/size/usage with unit (e.g., "$500K/year", "800/week", "1000/monthly customers", "small/medium/large")
4. Priority tier: high (strong signals), medium (some signals), low (weak signals)
5. Status: qualified (clear fit), uncertain (maybe), not_relevant (no fit)

Companies:
{json.dumps(batch, indent=2)}

Return JSON:
{{
  "results": [
    {{
      "company_id": "id from list",
      "status": "qualified|uncertain|not_relevant",
      "score": 0-100,
      "priority": "high|medium|low",
      "volume_estimate": "numeric value or null",
      "volume_unit": "unit like $/year, /week, /month, customers, or small/medium/large",
      "market_signals": ["list of positive signals found"],
      "pain_points": ["specific problems that make them a good prospect"],
      "notes": "concise assessment with key reasons"
    }}
  ]
}}"""
            r = self._run(prompt, timeout=200)
            return r.get("results") if isinstance(r, dict) and r.get("results") else []

        # Batch into groups of 8 to avoid opencode timeouts
        BATCH_SIZE = 8
        all_results: list[dict] = []
        for i in range(0, len(company_list), BATCH_SIZE):
            batch = company_list[i:i + BATCH_SIZE]
            batch_results = _qualify_batch(batch)
            if batch_results:
                all_results.extend(batch_results)
            else:
                # Heuristic fallback for this batch only
                batch_companies = companies[i:i + BATCH_SIZE]
                all_results.extend(_heuristic_qualification(batch_companies, market=research_market, product=research_product))

        method = self._detect_agent() if all_results and not all(
            "Heuristic" in str(r.get("notes", "")) for r in all_results
        ) else "heuristic"

        result = {"result": "ok", "results": all_results, "method": method}

        qualified = 0
        for r in result.get("results", []):
            cid = r.get("company_id")
            volume_estimate = _to_float(r.get("volume_estimate"))
            volume_unit = r.get("volume_unit") or None
            score = _clamp_score(r.get("score"))
            status = _normalize_qualification_status(r.get("status", "new"))
            priority = _normalize_priority(r.get("priority"), score)

            # Combine notes with market_signals and pain_points for richer context
            notes_parts = []
            if r.get("notes"):
                notes_parts.append(str(r["notes"]))
            if r.get("market_signals"):
                signals = r["market_signals"] if isinstance(r["market_signals"], list) else [r["market_signals"]]
                notes_parts.append("Signals: " + "; ".join(str(s) for s in signals))
            if r.get("pain_points"):
                pains = r["pain_points"] if isinstance(r["pain_points"], list) else [r["pain_points"]]
                notes_parts.append("Pain points: " + "; ".join(str(p) for p in pains))
            combined_notes = " | ".join(notes_parts) if notes_parts else None

            fields = {
                "status": status,
                "priority_score": score,
                "priority_tier": priority,
                "volume_estimate": volume_estimate,
                "volume_unit": volume_unit,
                "notes": combined_notes,
            }
            update_company(cid, self.research_id, fields, root=self.root)
            if status == "qualified":
                qualified += 1

        self.last_result = result
        return {"result": "ok", "qualified": qualified, "assessed": len(companies), "method": result.get("method", "unknown")}
    
    def enrich(self, company_name: str, location: str | None = None) -> dict[str, Any]:
        """
        STEP 3: Enrich - Find contact info using a 3-tier approach.

        Tier 1 (free, fast): Website scraping + email patterns + existing notes.
        Tier 2 (free search): DuckDuckGo search for contact info.
        Tier 3 (AI, expensive): Multiple AI source searches run in parallel.

        AI calls are only made when Tier 1+2 fail to find both email AND phone.
        """
        # Look up existing website/notes from DB
        website = None
        existing_notes = None
        if self.research_id:
            try:
                from market_validation.research import _connect, _ensure_schema, resolve_db_path
                db = resolve_db_path(self.root)
                with _connect(db) as conn:
                    _ensure_schema(conn)
                    conn.row_factory = None
                    row = conn.execute(
                        """SELECT website, notes FROM companies
                           WHERE research_id = ? AND (company_name LIKE ? OR company_name LIKE ?)""",
                        (self.research_id, f"%{company_name}%", f"%{company_name.replace(' ', '%')}%")
                    ).fetchone()
                    if row:
                        website = row[0]
                        existing_notes = row[1]
            except Exception:
                pass

        # ---------- Tier 1 + 2: free methods ----------
        free_result = _free_enrich_company(company_name, website, location, existing_notes)

        all_emails: list[str] = list(free_result.get("emails", []))
        all_phones: list[str] = list(free_result.get("phones", []))
        all_contacts: list[dict[str, str]] = list(free_result.get("contacts", []))
        all_findings: dict[str, Any] = {}
        all_email_sources: dict[str, str] = dict(free_result.get("email_sources", {}))
        sources_tried: list[str] = list(free_result.get("sources", []))

        if free_result.get("address"):
            all_findings["address"] = free_result["address"]

        # ---------- Tier 3: AI sources (only if still missing email AND phone) ----------
        if not all_emails or not all_phones:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            searches: list[tuple[str, Any]] = [
                ("website", lambda: self._search_website(company_name, location)),
                ("linkedin", lambda: self._search_linkedin(company_name)),
                ("directories", lambda: self._search_directories(company_name, location)),
                ("news", lambda: self._search_news(company_name)),
                ("reviews", lambda: self._search_reviews(company_name, location)),
                ("social", lambda: self._search_social(company_name)),
                ("registry", lambda: self._search_registry(company_name, location)),
            ]

            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {
                    pool.submit(search_fn): source_name
                    for source_name, search_fn in searches
                }
                for future in as_completed(futures):
                    source_name = futures[future]
                    try:
                        result = future.result()
                    except Exception:
                        result = {"found": False}
                    if not isinstance(result, dict) or not result.get("found"):
                        continue
                    sources_tried.append(source_name)

                    for em in (result.get("emails") or []):
                        if em and em not in all_emails:
                            all_emails.append(em)
                    for ph in (result.get("phones") or []):
                        if ph and ph not in all_phones:
                            all_phones.append(ph)
                    for ct in (result.get("contacts") or result.get("employees_found") or []):
                        if isinstance(ct, dict) and ct not in all_contacts:
                            all_contacts.append(ct)

                    for key in ("website", "rating_estimate", "years_in_business", "pricing_perception"):
                        if result.get(key) and not all_findings.get(key):
                            all_findings[key] = result[key]

                    if result.get("notes"):
                        prev = all_findings.get("notes", "")
                        all_findings["notes"] = f"{prev} | {source_name}: {result['notes']}" if prev else f"{source_name}: {result['notes']}"

        # ---------- Adaptive step: pick next best action for missing email ----------
        adaptive_result = None
        if not all_emails:
            from market_validation.company_enrichment import domain_from_url as _dom
            domain = _dom(website) or _dom(all_findings.get("website"))
            adaptive_result = _adaptive_find_email(
                company_name=company_name,
                website=website or all_findings.get("website"),
                domain=domain,
                contacts=all_contacts,
                location=location,
            )
            if adaptive_result.get("email"):
                all_emails.append(adaptive_result["email"])
                sources_tried.append(adaptive_result["source"])

        all_findings["emails"] = all_emails
        all_findings["phones"] = all_phones
        all_findings["contacts"] = all_contacts
        all_findings["decision_makers"] = [c.get("name", "") for c in all_contacts if c.get("name")]
        all_findings["email_sources"] = all_email_sources

        # Update database
        if self.research_id and (all_emails or all_phones or all_contacts or all_findings.get("website")):
            self._update_company_from_findings(company_name, all_findings)

        result_dict: dict[str, Any] = {
            "result": "ok",
            "company": company_name,
            "sources_tried": sources_tried,
            "findings": all_findings,
        }
        if adaptive_result:
            result_dict["adaptive"] = {
                "actions_tried": adaptive_result.get("actions_tried", []),
                "source": adaptive_result.get("source"),
            }
        return result_dict

    def _update_company_from_findings(self, company_name: str, findings: dict):
        """Update company record with enriched contact data."""
        from market_validation.research import _connect, _ensure_schema, resolve_db_path, update_company

        db = resolve_db_path(self.root)
        updates: dict[str, Any] = {}

        with _connect(db) as conn:
            _ensure_schema(conn)
            conn.row_factory = None
            company = conn.execute(
                """SELECT id, phone, email, website FROM companies
                   WHERE research_id = ? AND (company_name LIKE ? OR company_name LIKE ?)""",
                (self.research_id, f"%{company_name}%", f"%{company_name.replace(' ', '%')}%")
            ).fetchone()

            if not company:
                return

            cid = str(company[0])
            existing_phone = company[1] or ""
            existing_email = company[2] or ""
            existing_website = company[3] or ""

            # Email — use first found if DB is empty, or upgrade pattern to scraped
            emails = findings.get("emails") or []
            email_sources = findings.get("email_sources", {})
            if emails:
                chosen_email = emails[0]
                chosen_src = email_sources.get(chosen_email.lower(), "unknown")

                # Set email if DB is empty
                if not existing_email:
                    updates["email"] = chosen_email

                # Track email source in notes
                if "email" in updates:
                    src = email_sources.get(updates["email"].lower(), "unknown")
                    if src == "scraped":
                        _src_note = "Email source: scraped from website"
                    elif src == "search":
                        _src_note = "Email source: found via search results"
                    else:
                        _src_note = f"Email source: {src}"
                    updates.setdefault("_email_source_note", _src_note)

            # Phone — use first found if DB is empty
            phones = findings.get("phones") or []
            if phones and not existing_phone:
                updates["phone"] = phones[0]

            # Website — use found if DB is empty
            if findings.get("website") and not existing_website:
                updates["website"] = findings["website"]

            # Contacts / decision makers — append to notes
            contacts = findings.get("contacts") or []
            note_parts = []
            if contacts:
                contact_lines = [f"{c.get('name', '?')} ({c.get('title', '?')})" for c in contacts[:5]]
                note_parts.append("Contacts: " + "; ".join(contact_lines))
            _popped_src_note = updates.pop("_email_source_note", None)
            if _popped_src_note:
                note_parts.append(_popped_src_note)
            if note_parts:
                updates["notes"] = " | ".join(note_parts)

        if updates:
            update_company(cid, self.research_id, updates, root=self.root)
    
    def _search_website(self, company: str, location: str | None) -> dict:
        """Source 1: Official website."""
        loc = f" {location}" if location else ""
        prompt = f"""Find the official website for "{company}"{loc} and extract contact information.

Search for their official website, then extract:
- Contact page: email addresses, phone numbers
- About/Team page: owners, founders, key decision makers and their titles
- Any contact forms, purchasing or sales contact info
- Signs of company size, growth, or market activity

Return JSON:
{{
  "found": true/false,
  "website": "url",
  "emails": ["email@..."],
  "phones": ["555-123-4567"],
  "contacts": [{{"name": "Name", "title": "Title"}}],
  "notes": "What you found"
}}"""
        return self._run(prompt, timeout=120) or {"found": False}
    
    def _search_linkedin(self, company: str) -> dict:
        """Source 2: LinkedIn (via web search)."""
        prompt = f"""Find people at "{company}" via web search.

Search: "{company}" owner LinkedIn, "{company}" founder, "{company}" management team

Return JSON:
{{
  "found": true/false,
  "employees_found": [
    {{"name": "Name", "title": "Title", "relevance": "..."}}
  ],
  "decision_makers": ["Names of key decision makers"],
  "notes": "How you found this"
}}"""
        return self._run(prompt, timeout=120) or {"found": False}
    
    def _search_directories(self, company: str, location: str | None) -> dict:
        """Source 3: Business directories."""
        loc = f" {location}" if location else ""
        prompt = f"""Find "{company}"{loc} in directories.

Search: Yelp, Google Maps, YellowPages, BBB, Crunchbase

Return JSON:
{{
  "found": true/false,
  "yelp": {{"rating": "...", "reviews": "..."}},
  "google": {{"rating": "...", "reviews": "..."}},
  "years_in_business": "...",
  "emails": ["email if listed"],
  "notes": "What directories had info"
}}"""
        return self._run(prompt, timeout=120) or {"found": False}
    
    def _search_news(self, company: str) -> dict:
        """Source 4: News archives."""
        prompt = f"""Find news about "{company}".

Search: "{company}" news, "{company}" press, "{company}" expansion

Return JSON:
{{
  "found": true/false,
  "articles": [
    {{"title": "...", "source": "...", "date": "...", "summary": "..."}}
  ],
  "notes": "Key news findings"
}}"""
        return self._run(prompt, timeout=120) or {"found": False}
    
    def _search_reviews(self, company: str, location: str | None) -> dict:
        """Source 5: Review sites."""
        loc = f" {location}" if location else ""
        prompt = f"""Analyze reviews for "{company}"{loc}.

Search: Yelp reviews, Google reviews

Look for:
- Sentiment (positive/negative)
- Volume indicators ("I come every week")
- Complaints
- What people praise

Return JSON:
{{
  "found": true/false,
  "rating_estimate": "4.5/5",
  "volume_indicators": ["Quotes suggesting customer volume"],
  "pricing_perception": "expensive/moderate/affordable",
  "notes": "Key review insights"
}}"""
        return self._run(prompt, timeout=120) or {"found": False}
    
    def _search_social(self, company: str) -> dict:
        """Source 6: Social media."""
        prompt = f"""Find social media for "{company}".

Search: "{company}" Instagram, Facebook, Twitter

Return JSON:
{{
  "found": true/false,
  "instagram": {{"url": "...", "followers": "..."}},
  "facebook": {{"url": "...", "likes": "..."}},
  "notes": "Social media presence"
}}"""
        return self._run(prompt, timeout=120) or {"found": False}
    
    def _search_registry(self, company: str, location: str | None) -> dict:
        """Source 7: Business registry."""
        loc = f" {location}" if location else ""
        prompt = f"""Find "{company}"{loc} in public business registries.

Search: state/national business registries, OpenCorporates, SEC EDGAR (if public), or local business registration databases.

Return JSON:
{{
  "found": true/false,
  "entity_type": "LLC/Corp/etc",
  "state": "state of registration",
  "officers": ["Name - Title"],
  "notes": "Registry findings"
}}"""
        return self._run(prompt, timeout=90) or {"found": False}

    def enrich_all(self, statuses: list[str] | None = None) -> dict[str, Any]:
        """
        Run enrichment (phone, email, contact) on all companies matching the given statuses.
        Default: qualified companies only.

        Uses a 3-tier approach + adaptive fallback to minimize expensive AI calls:
          Tier 1 (free/fast): Website scraping + email patterns + existing notes
          Tier 2 (free search): DuckDuckGo search for contact info
          Tier 3 (AI): Only for companies still missing BOTH email AND phone
          Adaptive: For companies still missing email after all tiers —
            picks the best next free action based on what data we already have
            (pattern generation + MX verify, person-based emails, targeted search).
        """
        if not self.research_id:
            return {"result": "error", "error": "No research_id set"}

        from market_validation.research import _connect, _ensure_schema, resolve_db_path, update_company

        if statuses is None:
            statuses = ["qualified"]

        db = resolve_db_path(self.root)
        placeholders = ",".join("?" * len(statuses))
        with _connect(db) as conn:
            _ensure_schema(conn)
            conn.row_factory = None
            companies = conn.execute(
                f"""SELECT id, company_name, website, location, phone, email, notes
                    FROM companies
                    WHERE research_id = ? AND status IN ({placeholders})
                    ORDER BY priority_score DESC NULLS LAST""",
                (self.research_id, *statuses),
            ).fetchall()

        if not companies:
            return {"result": "ok", "enriched": 0, "message": "No companies to enrich"}

        enriched = 0
        emails_found = 0
        phones_found = 0
        ai_calls = 0
        tier1_hits = 0
        tier2_hits = 0
        adaptive_hits = 0

        # Filter to companies that actually need enrichment
        to_enrich = [
            c for c in companies
            if not (c[4] and c[5])  # skip if already have both phone and email
        ]

        if not to_enrich:
            return {"result": "ok", "enriched": 0, "message": "All companies already enriched"}

        # -------- Phase 1: Parallel free tier (Tier 1 + 2) + adaptive --------
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from market_validation.company_enrichment import domain_from_url as _dom

        def _free_enrich_with_adaptive(company: tuple) -> tuple:
            """Run free enrichment + adaptive fallback for one company. Thread-safe."""
            cid, company_name, website, location, current_phone, current_email, current_notes = company

            free = _free_enrich_company(company_name, website, location, current_notes)

            free_emails = free.get("emails", [])
            free_phones = free.get("phones", [])
            email_sources = free.get("email_sources", {})

            updates: dict[str, Any] = {}
            got_email_free = False
            got_phone_free = False
            email_source_label = ""

            if free_emails and not current_email:
                chosen_email = free_emails[0]
                updates["email"] = chosen_email
                got_email_free = True
                # Determine source label for notes
                src = email_sources.get(chosen_email.lower(), "unknown")
                if src == "scraped":
                    email_source_label = "Email source: scraped from website"
                elif src == "search":
                    email_source_label = "Email source: found via search results"
                else:
                    email_source_label = f"Email source: {src}"
            if free_phones and not current_phone:
                updates["phone"] = free_phones[0]
                got_phone_free = True
            if free.get("address") and not location:
                updates["location"] = free["address"]
            if not website and free.get("website"):
                updates["website"] = free["website"]

            # Append email source to notes
            if email_source_label:
                base = current_notes or ""
                if base:
                    updates["notes"] = f"{base} | {email_source_label}"
                else:
                    updates["notes"] = email_source_label

            tier_label = None
            if got_email_free or got_phone_free:
                tier_label = "tier1" if any(
                    s in ("website_scrape", "existing_notes")
                    for s in free.get("sources", [])
                ) else "tier2"

            # Adaptive step: smart fallback for missing email (also free/thread-safe)
            adaptive_hit = False
            if not current_email and "email" not in updates:
                effective_website = website or updates.get("website")
                domain = _dom(effective_website)

                adaptive = _adaptive_find_email(
                    company_name=company_name,
                    website=effective_website,
                    domain=domain,
                    contacts=[],  # no AI contacts yet in this phase
                    location=location,
                )
                if adaptive.get("email"):
                    updates["email"] = adaptive["email"]
                    adaptive_hit = True
                    _adaptive_label = _email_source_label(adaptive.get("source", "adaptive"))
                    base = updates.get("notes") or current_notes or ""
                    if base:
                        updates["notes"] = f"{base} | {_adaptive_label}"
                    else:
                        updates["notes"] = _adaptive_label

            return (company, updates, got_email_free, got_phone_free, tier_label, adaptive_hit)

        _log.info("  [enrich] Phase 1: free enrichment for %d companies (parallel, max_workers=6)", len(to_enrich))
        free_results: list[tuple] = []
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {}
            for i, company in enumerate(to_enrich):
                # Small staggered delay between submissions to be polite to web servers
                if i > 0 and i % 6 == 0:
                    time.sleep(1.0)
                futures[executor.submit(_free_enrich_with_adaptive, company)] = company

            for future in as_completed(futures):
                try:
                    free_results.append(future.result())
                except Exception as exc:
                    company = futures[future]
                    _log.warning("  [enrich] free tier failed for %s: %s", company[1], exc)
                    # Still include with empty updates so Tier 3 can try
                    free_results.append((company, {}, False, False, None, False))

        # Tally free-tier stats and apply updates
        need_ai: list[tuple] = []  # (company, updates_so_far) for Tier 3
        for (company, updates, got_email_free, got_phone_free, tier_label, adaptive_hit) in free_results:
            cid, company_name, website, location, current_phone, current_email, current_notes = company

            if got_email_free:
                emails_found += 1
            if got_phone_free:
                phones_found += 1
            if tier_label == "tier1":
                tier1_hits += 1
            elif tier_label == "tier2":
                tier2_hits += 1
            if adaptive_hit:
                adaptive_hits += 1
                emails_found += 1

            still_missing_email = not current_email and not got_email_free and not adaptive_hit
            still_missing_phone = not current_phone and not got_phone_free

            if still_missing_email or still_missing_phone:
                need_ai.append((company, updates))
            elif updates:
                update_company(str(cid), self.research_id, updates, root=self.root)
                enriched += 1

        # -------- Phase 2: Sequential AI tier (Tier 3) for remaining companies --------
        if need_ai:
            _log.info("  [enrich] Phase 2: AI enrichment for %d companies (sequential)", len(need_ai))

        for company, updates in need_ai:
            cid, company_name, website, location, current_phone, current_email, current_notes = company

            website_hint = f"Their website is {website}." if website else f'Search for "{company_name}" official website first.'
            location_hint = f" Located in {location}." if location else ""

            prompt = f"""Find contact information for "{company_name}".{location_hint}
{website_hint}

Priority: find a direct phone number and a contact email address.
Also look for: owner name, purchasing/sales manager name and title.

Search:
- Their official website contact/about page
- Google: "{company_name} phone email contact"
- LinkedIn: "{company_name}" company page
- Business directories: Yelp, Google Maps, BBB, YellowPages

Return JSON only:
{{
  "company_name": "{company_name}",
  "phone": "best phone number found or null",
  "email": "best contact email found or null",
  "website": "official website URL or null",
  "location": "full street address or null",
  "contacts": [{{"name": "Name", "title": "Title"}}],
  "notes": "brief summary of what was found"
}}"""

            result = self._run(prompt, timeout=150)
            ai_calls += 1

            if result.get("result") != "error":
                if result.get("phone") and not current_phone and "phone" not in updates:
                    updates["phone"] = str(result["phone"])
                    phones_found += 1
                if result.get("email") and not current_email and "email" not in updates:
                    updates["email"] = str(result["email"])
                    emails_found += 1
                if result.get("website") and not website and "website" not in updates:
                    updates["website"] = str(result["website"])
                if result.get("location") and not location and "location" not in updates:
                    updates["location"] = str(result["location"])

                # Append contact findings to notes
                parts = []
                if result.get("contacts"):
                    contacts_str = "; ".join(
                        f"{c.get('name','?')} ({c.get('title','?')})"
                        for c in result["contacts"]
                        if isinstance(c, dict)
                    )
                    if contacts_str:
                        parts.append(f"Contacts: {contacts_str}")
                if result.get("notes"):
                    parts.append(str(result["notes"]))
                # Track AI email source in notes
                if result.get("email") and not current_email:
                    parts.append("Email source: found via AI search")
                if parts:
                    suffix = " | " + " | ".join(parts)
                    base_notes = updates.get("notes") or current_notes or ""
                    updates["notes"] = base_notes + suffix if base_notes else suffix

            # Run adaptive again with AI contacts for companies that still lack email
            if not current_email and "email" not in updates:
                ai_contacts: list[dict[str, str]] = []
                if result and isinstance(result, dict) and result.get("result") != "error":
                    ai_contacts = [c for c in (result.get("contacts") or []) if isinstance(c, dict)]

                effective_website = website or updates.get("website")
                domain = _dom(effective_website)

                adaptive = _adaptive_find_email(
                    company_name=company_name,
                    website=effective_website,
                    domain=domain,
                    contacts=ai_contacts,
                    location=location,
                )
                if adaptive.get("email"):
                    updates["email"] = adaptive["email"]
                    emails_found += 1
                    adaptive_hits += 1
                    _adaptive_label = _email_source_label(adaptive.get("source", "adaptive"))
                    base_notes = updates.get("notes") or current_notes or ""
                    if base_notes:
                        updates["notes"] = f"{base_notes} | {_adaptive_label}"
                    else:
                        updates["notes"] = _adaptive_label

            if updates:
                update_company(str(cid), self.research_id, updates, root=self.root)
                enriched += 1

        return {
            "result": "ok",
            "enriched": enriched,
            "emails_found": emails_found,
            "phones_found": phones_found,
            "total_companies": len(companies),
            "ai_calls": ai_calls,
            "tier1_hits": tier1_hits,
            "tier2_hits": tier2_hits,
            "adaptive_hits": adaptive_hits,
        }


    def research(
        self,
        market: str,
        geography: str,
        product: str | None = None,
        enrich_statuses: list[str] | None = None,
        validate: bool = False,
        archetype: str | None = None,
    ) -> dict[str, Any]:
        """
        Full pipeline: [validate →] find → qualify → enrich_all.

        This is the default way to run a complete market research.
        Automatically runs all steps and returns a combined summary.

        Args:
            market:          Market category (e.g. "BBQ restaurants", "robotics")
            geography:       Location (e.g. "San Jose, California")
            product:         Specific product/service within the market (optional)
            enrich_statuses: Which company statuses to enrich. Default: ["qualified", "new"]
            validate:        If True, run market validation (Step 0) before find.
        """
        if enrich_statuses is None:
            enrich_statuses = ["qualified", "new"]

        total_steps = 4 if validate else 3
        step = 0

        validate_result = None
        if validate:
            step += 1
            print(f"[research] Step {step}/{total_steps}: validate — {product or market} in {geography}")
            validate_result = self.validate(market, geography, product, archetype=archetype)
            verdict = validate_result.get("scorecard", {}).get("verdict", "unknown")
            overall = validate_result.get("scorecard", {}).get("overall_score", 0)
            print(f"[research] → verdict: {verdict} ({overall}/100)")

        step += 1
        print(f"[research] Step {step}/{total_steps}: find — {product or market} in {geography}")
        find_result = self.find(market, geography, product)
        companies_found = len(find_result.get("companies", []))
        print(f"[research] → {companies_found} companies found via {find_result.get('method')}")

        step += 1
        print(f"[research] Step {step}/{total_steps}: qualify")
        qualify_result = self.qualify()
        print(f"[research] → {qualify_result.get('qualified')}/{qualify_result.get('assessed')} qualified via {qualify_result.get('method')}")

        step += 1
        print(f"[research] Step {step}/{total_steps}: enrich_all (statuses={enrich_statuses})")
        enrich_result = self.enrich_all(statuses=enrich_statuses)
        print(f"[research] → enriched={enrich_result.get('enriched')}/{enrich_result.get('total_companies')} | phones={enrich_result.get('phones_found')} emails={enrich_result.get('emails_found')}")

        result: dict[str, Any] = {
            "result": "ok",
            "research_id": self.research_id,
            "find": find_result,
            "qualify": qualify_result,
            "enrich": enrich_result,
            "summary": {
                "companies_found": companies_found,
                "qualified": qualify_result.get("qualified", 0),
                "phones_found": enrich_result.get("phones_found", 0),
                "emails_found": enrich_result.get("emails_found", 0),
            },
        }
        if validate_result:
            result["validate"] = validate_result
            result["summary"]["verdict"] = validate_result.get("scorecard", {}).get("verdict")
            result["summary"]["overall_score"] = validate_result.get("scorecard", {}).get("overall_score")
        return result


def main():
    """CLI entry point."""
    import argparse
    parser = argparse.ArgumentParser(description="Market Research Agent")
    parser.add_argument("command", choices=["research", "validate", "find", "qualify", "enrich", "enrich-all"])
    parser.add_argument("--research-id", help="Research ID")
    parser.add_argument("--market", help="Market/product")
    parser.add_argument("--geography", help="Geography")
    parser.add_argument("--product", help="Specific product (optional)")
    parser.add_argument("--company", help="Company name for single enrich")
    parser.add_argument("--validate", action="store_true", help="Run market validation before research pipeline")
    parser.add_argument("--archetype", help="Override archetype detection (e.g. local-service, b2b-saas, b2b-industrial, consumer-cpg, marketplace, healthcare, services-agency)")

    args = parser.parse_args()
    agent = Agent(research_id=args.research_id)

    import json
    if args.command == "research":
        if not args.market or not args.geography:
            parser.error("research requires --market and --geography")
        from market_validation.research import create_research
        rid = create_research(
            name=f"{args.product or args.market} in {args.geography}",
            market=args.market,
            product=args.product,
            geography=args.geography,
        )["research_id"]
        agent.research_id = rid
        result = agent.research(args.market, args.geography, args.product, validate=args.validate, archetype=args.archetype)
    elif args.command == "validate":
        if not args.market or not args.geography:
            parser.error("validate requires --market and --geography")
        if not args.research_id:
            # Look for an existing research with the SAME market AND geography
            # to avoid polluting a different market's research with this validation.
            from market_validation.research import _connect, _ensure_schema, resolve_db_path, create_research as _cr
            _db = resolve_db_path(agent.root)
            with _connect(_db) as _conn:
                _ensure_schema(_conn)
                _existing = _conn.execute(
                    """SELECT r.id FROM researches r
                       WHERE LOWER(TRIM(r.market)) = LOWER(TRIM(?))
                         AND LOWER(TRIM(COALESCE(r.geography,''))) = LOWER(TRIM(?))
                       ORDER BY r.created_at DESC LIMIT 1""",
                    (args.market, args.geography),
                ).fetchone()
            if _existing:
                agent.research_id = _existing[0]
                print(f"[validate] reusing existing research {agent.research_id} ({args.market} / {args.geography})")
            else:
                rid = _cr(
                    name=f"Validation: {args.product or args.market} in {args.geography}",
                    market=args.market,
                    product=args.product,
                    geography=args.geography,
                )["research_id"]
                agent.research_id = rid
        result = agent.validate(args.market, args.geography, args.product, archetype=args.archetype)
    elif args.command == "find":
        result = agent.find(args.market, args.geography, args.product)
    elif args.command == "qualify":
        result = agent.qualify()
    elif args.command == "enrich":
        result = agent.enrich(args.company)
    elif args.command == "enrich-all":
        result = agent.enrich_all()

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

"""EnrichmentService — contact-info enrichment (Step 3 of pipeline).

Uses a 3-tier approach to minimize expensive AI calls:
  Tier 1 (free/fast):  Website scraping + existing notes
  Tier 2 (free/search): DuckDuckGo snippet mining
  Tier 3 (AI):         Only for companies still missing both email AND phone
  Adaptive:            Targeted search + MX verification for leftover gaps

Exposes two operations: ``enrich_one(company_name, location)`` for a single
company lookup, and ``enrich_all(statuses)`` to batch-process everything in
the current research project.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from market_validation._helpers.enrichment_helpers import (
    adaptive_find_email,
    email_source_label,
    free_enrich_company,
)
from market_validation._helpers.quality import is_useful_business_url
from market_validation.log import get_logger

_log = get_logger("enrichment_service")

RunAI = Callable[..., dict[str, Any]]


class EnrichmentService:
    """Fill in email/phone/contacts for companies using a 3-tier cascade."""

    def __init__(self, run_ai: RunAI, root: Path, research_id: str | None):
        self.run_ai = run_ai
        self.root = root
        self.research_id = research_id

    # ── Single-company enrichment ─────────────────────────────────────────

    def enrich_one(self, company_name: str, location: str | None = None) -> dict[str, Any]:
        """Enrich a single company. Uses Tier 1+2 first, then optionally Tier 3."""
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
                        (self.research_id, f"%{company_name}%", f"%{company_name.replace(' ', '%')}%"),
                    ).fetchone()
                    if row:
                        website = row[0]
                        existing_notes = row[1]
            except Exception as exc:
                # DB read is best-effort here — we can still try free enrichment
                # with just the name. Log so a corrupted DB shows up in logs.
                _log.warning("enrich: failed to load existing data for %r: %s", company_name, exc)

        free_result = free_enrich_company(company_name, website, location, existing_notes)

        all_emails: list[str] = list(free_result.get("emails", []))
        all_phones: list[str] = list(free_result.get("phones", []))
        all_contacts: list[dict[str, str]] = list(free_result.get("contacts", []))
        all_findings: dict[str, Any] = {}
        all_email_sources: dict[str, str] = dict(free_result.get("email_sources", {}))
        sources_tried: list[str] = list(free_result.get("sources", []))

        if free_result.get("address"):
            all_findings["address"] = free_result["address"]

        # Tier 3: AI sources (only if still missing email AND phone)
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
                futures = {pool.submit(fn): name for name, fn in searches}
                for future in as_completed(futures):
                    source_name = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        # Per-source AI probe failures are common (rate limits,
                        # timeouts, bad JSON) — log at debug; missing data just
                        # means we skip this source and try others.
                        _log.debug("enrich: %s probe failed for %r: %s", source_name, company_name, exc)
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
                        all_findings["notes"] = (
                            f"{prev} | {source_name}: {result['notes']}"
                            if prev else f"{source_name}: {result['notes']}"
                        )

        # Adaptive step: pick next best action for missing email
        adaptive_result = None
        if not all_emails:
            from market_validation.company_enrichment import domain_from_url as _dom
            domain = _dom(website) or _dom(all_findings.get("website"))
            adaptive_result = adaptive_find_email(
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

    # ── Batch enrichment ──────────────────────────────────────────────────

    def enrich_all(self, statuses: list[str] | None = None) -> dict[str, Any]:
        """Run enrichment on all companies matching the given statuses (default: qualified)."""
        if not self.research_id:
            return {"result": "error", "error": "No research_id set"}

        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from market_validation.company_enrichment import domain_from_url as _dom
        from market_validation.research import (
            _connect,
            _ensure_schema,
            resolve_db_path,
            update_company,
        )

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

        to_enrich = [c for c in companies if not (c[4] and c[5])]
        if not to_enrich:
            return {"result": "ok", "enriched": 0, "message": "All companies already enriched"}

        # ── Phase 1: Parallel free tier (Tier 1 + 2) + adaptive ───────────
        def _free_enrich_with_adaptive(company: tuple) -> tuple:
            """Run free enrichment + adaptive fallback for one company. Thread-safe."""
            cid, company_name, website, location, current_phone, current_email, current_notes = company

            free = free_enrich_company(company_name, website, location, current_notes)

            free_emails = free.get("emails", [])
            free_phones = free.get("phones", [])
            email_sources = free.get("email_sources", {})

            updates: dict[str, Any] = {}
            got_email_free = False
            got_phone_free = False
            email_label = ""

            if free_emails and not current_email:
                chosen_email = free_emails[0]
                updates["email"] = chosen_email
                got_email_free = True
                src = email_sources.get(chosen_email.lower(), "unknown")
                if src == "scraped":
                    email_label = "Email source: scraped from website"
                elif src == "search":
                    email_label = "Email source: found via search results"
                else:
                    email_label = f"Email source: {src}"
            if free_phones and not current_phone:
                updates["phone"] = free_phones[0]
                got_phone_free = True
            if free.get("address") and not location:
                updates["location"] = free["address"]
            if not website and free.get("website") and is_useful_business_url(str(free["website"])):
                updates["website"] = free["website"]

            if email_label:
                base = current_notes or ""
                updates["notes"] = f"{base} | {email_label}" if base else email_label

            tier_label = None
            if got_email_free or got_phone_free:
                tier_label = "tier1" if any(
                    s in ("website_scrape", "existing_notes")
                    for s in free.get("sources", [])
                ) else "tier2"

            # Adaptive step: smart fallback for missing email
            adaptive_hit = False
            if not current_email and "email" not in updates:
                effective_website = website or updates.get("website")
                domain = _dom(effective_website)

                adaptive = adaptive_find_email(
                    company_name=company_name,
                    website=effective_website,
                    domain=domain,
                    contacts=[],
                    location=location,
                )
                if adaptive.get("email"):
                    updates["email"] = adaptive["email"]
                    adaptive_hit = True
                    _adaptive_label = email_source_label(adaptive.get("source", "adaptive"))
                    base = updates.get("notes") or current_notes or ""
                    updates["notes"] = f"{base} | {_adaptive_label}" if base else _adaptive_label

            return (company, updates, got_email_free, got_phone_free, tier_label, adaptive_hit)

        _log.info("  [enrich] Phase 1: free enrichment for %d companies (parallel, max_workers=6)", len(to_enrich))
        free_results: list[tuple] = []
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {}
            for i, company in enumerate(to_enrich):
                if i > 0 and i % 6 == 0:
                    time.sleep(1.0)
                futures[executor.submit(_free_enrich_with_adaptive, company)] = company

            for future in as_completed(futures):
                try:
                    free_results.append(future.result())
                except Exception as exc:
                    company = futures[future]
                    _log.warning("  [enrich] free tier failed for %s: %s", company[1], exc)
                    free_results.append((company, {}, False, False, None, False))

        need_ai: list[tuple] = []
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

        # ── Phase 2: Sequential AI tier for remaining companies ───────────
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

            result = self.run_ai(prompt, timeout=150)
            ai_calls += 1

            if result.get("result") != "error":
                from market_validation.company_enrichment import is_plausible_email
                if result.get("phone") and not current_phone and "phone" not in updates:
                    updates["phone"] = str(result["phone"])
                    phones_found += 1
                # AI sometimes returns emails with embedded commentary like
                # "x@y.com (inferred from pattern)" — reject anything that
                # isn't a clean, plausible address.
                ai_email = str(result.get("email") or "").strip()
                ai_email_ok = is_plausible_email(ai_email)
                if ai_email_ok and not current_email and "email" not in updates:
                    updates["email"] = ai_email
                    emails_found += 1
                if result.get("website") and not website and "website" not in updates:
                    ai_site = str(result["website"]).strip()
                    # Reject AI-returned URLs that point at an aggregator/directory.
                    if is_useful_business_url(ai_site):
                        updates["website"] = ai_site
                if result.get("location") and not location and "location" not in updates:
                    updates["location"] = str(result["location"])

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
                if ai_email_ok and not current_email:
                    parts.append("Email source: found via AI search")
                elif ai_email and not ai_email_ok:
                    parts.append(f"AI returned unusable email: {ai_email[:80]}")
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

                adaptive = adaptive_find_email(
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
                    _adaptive_label = email_source_label(adaptive.get("source", "adaptive"))
                    base_notes = updates.get("notes") or current_notes or ""
                    updates["notes"] = (
                        f"{base_notes} | {_adaptive_label}" if base_notes else _adaptive_label
                    )

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

    # ── DB update + per-source AI probes ──────────────────────────────────

    def _update_company_from_findings(self, company_name: str, findings: dict) -> None:
        """Update company record with enriched contact data."""
        from market_validation.research import (
            _connect,
            _ensure_schema,
            resolve_db_path,
            update_company,
        )

        db = resolve_db_path(self.root)
        updates: dict[str, Any] = {}

        with _connect(db) as conn:
            _ensure_schema(conn)
            conn.row_factory = None
            company = conn.execute(
                """SELECT id, phone, email, website FROM companies
                   WHERE research_id = ? AND (company_name LIKE ? OR company_name LIKE ?)""",
                (self.research_id, f"%{company_name}%", f"%{company_name.replace(' ', '%')}%"),
            ).fetchone()

            if not company:
                return

            cid = str(company[0])
            existing_phone = company[1] or ""
            existing_email = company[2] or ""
            existing_website = company[3] or ""

            emails = findings.get("emails") or []
            email_sources = findings.get("email_sources", {})
            if emails:
                chosen_email = emails[0]

                if not existing_email:
                    updates["email"] = chosen_email

                if "email" in updates:
                    src = email_sources.get(updates["email"].lower(), "unknown")
                    if src == "scraped":
                        _src_note = "Email source: scraped from website"
                    elif src == "search":
                        _src_note = "Email source: found via search results"
                    else:
                        _src_note = f"Email source: {src}"
                    updates.setdefault("_email_source_note", _src_note)

            phones = findings.get("phones") or []
            if phones and not existing_phone:
                updates["phone"] = phones[0]

            if findings.get("website") and not existing_website and is_useful_business_url(str(findings["website"])):
                updates["website"] = findings["website"]

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

    # ── Per-source AI probes ─────────────────────────────────────────────

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
        return self.run_ai(prompt, timeout=120) or {"found": False}

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
        return self.run_ai(prompt, timeout=120) or {"found": False}

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
        return self.run_ai(prompt, timeout=120) or {"found": False}

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
        return self.run_ai(prompt, timeout=120) or {"found": False}

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
        return self.run_ai(prompt, timeout=120) or {"found": False}

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
        return self.run_ai(prompt, timeout=120) or {"found": False}

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
        return self.run_ai(prompt, timeout=90) or {"found": False}

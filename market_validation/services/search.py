"""SearchService — company discovery (Step 1 of pipeline).

Runs a multi-stage pipeline:
  1. Built-in multi-backend search (OSM-backed)
  2. Source-config URLs
  3. Supplementary backends (BBB, Manta)
  4. Quality-gate retry → adjacent-profile fallback → contactability retry
  5. AI fallback/supplement when quality is poor
  6. Iterative AI-generated queries (up to 2 rounds) until 15+ companies
  7. AI batch validation pre-save
  8. Pre-scrape contact enrichment
  9. Persist to DB if research_id is set
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

from market_validation._helpers.archetypes import archetype_search_context
from market_validation._helpers.common import (
    infer_market_profile,
    summarize_backends,
    unique_in_order,
)
from market_validation._helpers.companies import (
    dedupe_companies,
    filter_relevant_companies,
    normalize_companies,
)
from market_validation._helpers.contacts import (
    extract_contact_from_search_result,
    extract_email_text,
    extract_phone_text,
)
from market_validation._helpers.quality import (
    passes_quality_gate,
    quality_gate_thresholds,
)
from market_validation._helpers.queries import (
    ADJACENT_PROFILES,
    apply_contact_retry_rows,
    build_contact_retry_queries,
    build_retry_queries,
    primary_queries,
    queries_for_adjacent_profile,
    try_multi_search,
    try_source_urls,
    try_supplementary_search,
)
from market_validation._helpers.validation_helpers import (
    ai_search_hints,
    ai_search_strategy,
    ai_validate_companies,
)
from market_validation.log import get_logger

_log = get_logger("search_service")

RunAI = Callable[..., dict[str, Any]]
DetectAgent = Callable[[], str]


class SearchService:
    """Discover companies in a market via multi-backend + AI search."""

    def __init__(
        self,
        run_ai: RunAI,
        detect_agent: DetectAgent,
        root: Path,
        research_id: str | None,
    ):
        self.run_ai = run_ai
        self.detect_agent = detect_agent
        self.root = root
        self.research_id = research_id

    def run(self, market: str, geography: str, product: str | None = None) -> dict[str, Any]:
        search_term = product or market
        all_companies: list[dict[str, Any]] = []
        sources_used: list[str] = []
        source_health: list[dict[str, Any]] = []
        profile = infer_market_profile(market, product)

        from market_validation.market_archetype import detect_archetype
        _archetype_key, _archetype_conf = detect_archetype(market, product)
        _arch_search_ctx = archetype_search_context(_archetype_key, market, geography, product)
        print(f"[find] archetype={_archetype_key} (confidence={_archetype_conf}%)")

        # AI search strategy if heuristic confidence is low
        ai_strategy: dict[str, Any] | None = None
        if profile.get("confidence", 100) < 50:
            print(f"[find] heuristic confidence {profile.get('confidence')}% — asking AI for search strategy...")
            ai_strategy = ai_search_strategy(
                market, geography, product, self.run_ai, archetype_context=_arch_search_ctx,
            )
            if ai_strategy:
                btype = ai_strategy.get("business_type", market)
                print(f"[find] AI strategy: business_type='{btype}', {len(ai_strategy.get('queries', []))} queries")
                source_health.append({
                    "stage": "ai_search_strategy",
                    "business_type": btype,
                    "queries": ai_strategy.get("queries", []),
                    "status": "ok",
                })

        source_health.append({
            "stage": "market_profile",
            "category": profile.get("category"),
            "confidence": profile.get("confidence"),
            "tokens": sorted(list(profile.get("tokens") or []))[:20],
            "status": "ok",
        })

        if ai_strategy and ai_strategy.get("queries"):
            search_queries = ai_strategy["queries"]
        else:
            search_queries = primary_queries(market=market, geography=geography, product=product)

        for query in search_queries:
            search_results = try_multi_search(query, 15, geography=geography)
            source_health.append({
                "stage": "built_in_search",
                "query": query,
                "backends": summarize_backends(search_results),
                "results": len(search_results),
                "status": "ok" if search_results else "empty",
            })
            if search_results:
                sources_used.append("multi_search")
                for r in search_results:
                    all_companies.append(extract_contact_from_search_result(r))

        source_results = try_source_urls(market, geography, product)
        source_health.append({
            "stage": "source_config",
            "queries_or_urls": "configured",
            "results": len(source_results),
            "status": "ok" if source_results else "empty",
        })
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
                        "phone": data.get("phone", "") or extract_phone_text(_sc_desc),
                        "email": data.get("email", "") or extract_email_text(_sc_desc),
                        "description": _sc_desc,
                        "source": r.get("source", "config"),
                    })

        # Supplementary backends (BBB, Manta, etc.) — geo-aware so non-US
        # markets skip US-only directories entirely.
        supp_query = f"{market} {geography}"
        supp_results = try_supplementary_search(supp_query, 10, geography=geography)
        source_health.append({
            "stage": "supplementary_search",
            "query": supp_query,
            "results": len(supp_results),
            "backends": summarize_backends(supp_results),
            "status": "ok" if supp_results else "empty",
        })
        if supp_results:
            sources_used.append("supplementary")
            for r in supp_results:
                all_companies.append(extract_contact_from_search_result(r))

        _ai_junk = ai_strategy.get("junk_signals", []) if ai_strategy else []
        _ai_real = ai_strategy.get("real_business_signals", []) if ai_strategy else []
        if ai_strategy and ai_strategy.get("business_type"):
            _ai_real += ai_strategy["business_type"].lower().split()

        unique_companies = dedupe_companies(
            normalize_companies(all_companies), archetype=_archetype_key,
        )
        unique_companies = filter_relevant_companies(
            unique_companies, market=market, product=product,
            extra_junk_signals=_ai_junk, extra_real_signals=_ai_real,
        )

        # Quality gate + retry
        quality_passed, quality_info = passes_quality_gate(unique_companies, market=market, product=product)
        source_health.append({
            "stage": "quality_gate_initial",
            "status": "pass" if quality_passed else "fail",
            "metrics": quality_info.get("metrics"),
            "contactability": quality_info.get("contactability"),
            "thresholds": quality_info.get("thresholds"),
        })

        if not quality_passed:
            retry_queries = build_retry_queries(market=market, geography=geography, product=product)
            retry_companies: list[dict[str, Any]] = []
            for query in retry_queries:
                retry_rows = try_multi_search(query, 10, geography=geography)
                source_health.append({
                    "stage": "quality_gate_retry",
                    "query": query,
                    "backends": summarize_backends(retry_rows),
                    "results": len(retry_rows),
                    "status": "ok" if retry_rows else "empty",
                })
                for r in retry_rows:
                    retry_companies.append(extract_contact_from_search_result(r))

            if retry_companies:
                sources_used.append("quality_gate_retry")
                unique_companies = dedupe_companies(
                    normalize_companies(unique_companies + retry_companies),
                    archetype=_archetype_key,
                )
                unique_companies = filter_relevant_companies(unique_companies, market=market, product=product)

            retry_passed, retry_info = passes_quality_gate(unique_companies, market=market, product=product)
            source_health.append({
                "stage": "quality_gate_final",
                "status": "pass" if retry_passed else "fail",
                "metrics": retry_info.get("metrics"),
                "contactability": retry_info.get("contactability"),
                "thresholds": retry_info.get("thresholds"),
            })

            # Profile switching: primary profile underperformed → try adjacent
            if not retry_passed:
                adj_cats = ADJACENT_PROFILES.get(profile["category"], [])
                source_health.append({
                    "stage": "profile_switch_check",
                    "original_category": profile["category"],
                    "confidence": profile.get("confidence", 50),
                    "adjacent_profiles": adj_cats,
                    "status": "attempting" if adj_cats else "skipped",
                })
                for adj_cat in adj_cats:
                    adj_queries = queries_for_adjacent_profile(market, geography, product, adj_cat)
                    adj_companies: list[dict[str, Any]] = []
                    for query in adj_queries:
                        rows = try_multi_search(query, 10, geography=geography)
                        source_health.append({
                            "stage": "profile_switch_search",
                            "adjacent_category": adj_cat,
                            "query": query,
                            "backends": summarize_backends(rows),
                            "results": len(rows),
                            "status": "ok" if rows else "empty",
                        })
                        for r in rows:
                            adj_companies.append(extract_contact_from_search_result(r))

                    if adj_companies:
                        merged = dedupe_companies(
                            normalize_companies(unique_companies + adj_companies),
                            archetype=_archetype_key,
                        )
                        merged = filter_relevant_companies(merged, market=market, product=product)
                        adj_passed, adj_info = passes_quality_gate(merged, market=market, product=product)
                        source_health.append({
                            "stage": "profile_switch_gate",
                            "adjacent_category": adj_cat,
                            "status": "pass" if adj_passed else "fail",
                            "metrics": adj_info.get("metrics"),
                            "contactability": adj_info.get("contactability"),
                            "added_companies": len(merged) - len(unique_companies),
                        })
                        # Accept the merged pool whether or not the gate passed —
                        # more candidates is always better heading into contact retry.
                        if len(merged) > len(unique_companies):
                            unique_companies = merged
                            sources_used.append(f"profile_switch_{adj_cat}")
                        if adj_passed:
                            break

        # Secondary gate: contactability retry
        secondary_passed, secondary_info = passes_quality_gate(unique_companies, market=market, product=product)
        source_health.append({
            "stage": "quality_gate_contactability_initial",
            "status": "pass" if secondary_passed else "fail",
            "metrics": secondary_info.get("metrics"),
            "contactability": secondary_info.get("contactability"),
            "thresholds": secondary_info.get("thresholds"),
        })

        if not secondary_passed:
            contact_queries = build_contact_retry_queries(unique_companies, geography=geography)
            contact_rows: list[dict[str, str]] = []
            for query in contact_queries:
                rows = try_multi_search(query, 8, geography=geography)
                contact_rows.extend(rows)
                source_health.append({
                    "stage": "quality_gate_contactability_retry",
                    "query": query,
                    "results": len(rows),
                    "backends": summarize_backends(rows),
                    "status": "ok" if rows else "empty",
                })

            if contact_rows:
                updated_companies, updates = apply_contact_retry_rows(unique_companies, contact_rows)
                unique_companies = dedupe_companies(
                    normalize_companies(updated_companies),
                    archetype=_archetype_key,
                )
                unique_companies = filter_relevant_companies(unique_companies, market=market, product=product)
                if updates > 0:
                    sources_used.append("contactability_retry")
                source_health.append({
                    "stage": "quality_gate_contactability_updates",
                    "status": "ok",
                    "updated_companies": updates,
                })

            secondary_final_passed, secondary_final_info = passes_quality_gate(
                unique_companies, market=market, product=product,
            )
            source_health.append({
                "stage": "quality_gate_contactability_final",
                "status": "pass" if secondary_final_passed else "fail",
                "metrics": secondary_final_info.get("metrics"),
                "contactability": secondary_final_info.get("contactability"),
                "thresholds": secondary_final_info.get("thresholds"),
            })

        # AI fallback / supplement when quality is still poor
        thresholds = quality_gate_thresholds(market, product)
        final_quality_passed, _ = passes_quality_gate(unique_companies, market=market, product=product)
        needs_opencode = (
            not unique_companies
            or (not final_quality_passed and len(unique_companies) < max(thresholds["min_total"] * 2, 10))
        )

        if needs_opencode:
            ai_sources, ai_queries = ai_search_hints(market=market, geography=geography, product=product)
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

            ai_agent = self.detect_agent()
            prompt += "\n\nIMPORTANT: Only include real operating businesses with a physical presence or active website. Do NOT include directories, aggregators, review sites, social media pages, or unrelated companies."
            ai_result = self.run_ai(prompt, timeout=180)

            # AI may return either {"companies": [...]} (matches our prompt
            # schema) or a bare [...] array. Normalize to a dict here so the
            # rest of this branch sees a consistent shape.
            if isinstance(ai_result, list):
                ai_result = {"companies": ai_result}

            source_health.append({
                "stage": "ai_fallback" if not unique_companies else "ai_supplement",
                "agent": ai_agent,
                "results": len(ai_result.get("companies", [])) if isinstance(ai_result, dict) else 0,
                "status": "ok" if isinstance(ai_result, dict) and ai_result.get("result") != "error" else "error",
            })

            ai_label = f"ai:{ai_agent}"
            if isinstance(ai_result, dict) and ai_result.get("companies"):
                sources_used.append(ai_label)
                if unique_companies:
                    merged = dedupe_companies(
                        normalize_companies(unique_companies + ai_result["companies"]),
                        archetype=_archetype_key,
                    )
                    unique_companies = filter_relevant_companies(merged, market=market, product=product)
                    result = {
                        "result": "ok",
                        "companies": unique_companies,
                        "sources_used": unique_in_order(sources_used),
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
                    "sources_used": unique_in_order(sources_used),
                    "method": "direct_search",
                    "source_health": source_health,
                }
            else:
                result = {"result": "error", "error": "No companies found"}
        else:
            result = {
                "result": "ok",
                "companies": unique_companies,
                "sources_used": unique_in_order(sources_used),
                "method": "direct_search",
                "source_health": source_health,
            }

        if result.get("result") == "error":
            return result

        companies = normalize_companies(result.get("companies", []))
        companies = dedupe_companies(companies, archetype=_archetype_key)
        companies = filter_relevant_companies(
            companies, market=market, product=product,
            extra_junk_signals=_ai_junk, extra_real_signals=_ai_real,
        )

        # Iterative AI-generated queries (up to 2 rounds) until 15+ companies
        _all_queries_used = list(search_queries)
        if not quality_passed:
            _all_queries_used.extend(build_retry_queries(market=market, geography=geography, product=product))

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

            _iter_ai_result = self.run_ai(_iter_prompt, timeout=60)
            _iter_queries: list[str] = []
            # Accept either {"queries": [...]} (per prompt) or a bare array.
            if isinstance(_iter_ai_result, dict) and _iter_ai_result.get("queries"):
                _iter_queries = [q for q in _iter_ai_result["queries"] if isinstance(q, str)]
            elif isinstance(_iter_ai_result, list):
                _iter_queries = [q for q in _iter_ai_result if isinstance(q, str)]

            if not _iter_queries:
                source_health.append({
                    "stage": f"iterative_discovery_round_{_iter_round + 1}",
                    "status": "no_new_queries",
                })
                break

            _all_queries_used.extend(_iter_queries)
            _iter_companies: list[dict[str, Any]] = []
            for _iq in _iter_queries:
                _iter_rows = try_multi_search(_iq, 15, geography=geography)
                source_health.append({
                    "stage": f"iterative_discovery_round_{_iter_round + 1}",
                    "query": _iq,
                    "results": len(_iter_rows),
                    "backends": summarize_backends(_iter_rows),
                    "status": "ok" if _iter_rows else "empty",
                })
                for r in _iter_rows:
                    _iter_companies.append(extract_contact_from_search_result(r))

            if _iter_companies:
                sources_used.append(f"iterative_discovery_{_iter_round + 1}")
                companies = dedupe_companies(
                    normalize_companies(companies + _iter_companies),
                    archetype=_archetype_key,
                )
                companies = filter_relevant_companies(
                    companies, market=market, product=product,
                    extra_junk_signals=_ai_junk, extra_real_signals=_ai_real,
                )
                print(f"[find] Iterative round {_iter_round + 1}: +{len(_iter_companies)} raw → {len(companies)} total after dedupe/filter")
            else:
                print(f"[find] Iterative round {_iter_round + 1}: no new companies found")
                break

        # AI batch pre-save validation
        if companies:
            _biz_type = (ai_strategy.get("business_type") if ai_strategy else None) or market
            print(f"[find] Claude pre-save validation: {len(companies)} candidates → business_type='{_biz_type}'")
            companies = ai_validate_companies(
                companies, market=market, geography=geography,
                business_type=_biz_type, run_ai=self.run_ai,
            )
            print(f"[find] After validation: {len(companies)} companies confirmed")
            source_health.append({
                "stage": "ai_pre_save_validation",
                "candidates_in": len(result.get("companies", [])),
                "confirmed_out": len(companies),
                "status": "ok",
            })

        result["companies"] = companies

        # Pre-scrape pass for contact enrichment before DB write
        companies_to_scrape = [
            c for c in companies
            if c.get("website")
            and c["website"].startswith("http")
            and (not c.get("phone") or not c.get("email"))
        ][:20]

        if companies_to_scrape:
            self._pre_scrape(companies_to_scrape, companies, source_health)

        # Persist to DB if research_id is set
        if self.research_id:
            self._persist(companies, market, result, source_health)

        result["sources_used"] = unique_in_order(sources_used)
        result["source_health"] = source_health
        if "result" not in result:
            result["result"] = "ok"
        return result

    # ── Internal helpers ──────────────────────────────────────────────────

    def _pre_scrape(
        self,
        companies_to_scrape: list[dict[str, Any]],
        companies: list[dict[str, Any]],
        source_health: list[dict[str, Any]],
    ) -> None:
        import time as _time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from market_validation.company_enrichment import is_plausible_email
        from market_validation.web_scraper import quick_scrape

        print(f"[find] Pre-scraping {len(companies_to_scrape)} company websites for contact info...")

        def _safe_scrape(company: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
            try:
                data = quick_scrape(company["website"])
                return company, data
            except Exception as exc:
                # Scraping is best-effort — a single site failing shouldn't abort the
                # whole find(). Log at debug so it doesn't drown normal runs.
                _log.debug("pre-scrape failed for %s: %s", company.get("website"), exc)
                return company, {}

        scrape_results: dict[int, dict[str, Any]] = {}
        batch_size = 4
        per_batch_timeout = 25  # seconds — be generous, slow sites are common
        for batch_start in range(0, len(companies_to_scrape), batch_size):
            batch = companies_to_scrape[batch_start : batch_start + batch_size]
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {executor.submit(_safe_scrape, c): c for c in batch}
                try:
                    for future in as_completed(futures, timeout=per_batch_timeout):
                        try:
                            comp, data = future.result(timeout=2)
                            if data and not data.get("error"):
                                scrape_results[id(comp)] = data
                        except Exception as exc:
                            _log.debug("pre-scrape future failed: %s", exc)
                except TimeoutError:
                    # as_completed timed out waiting on the slowest futures.
                    # Pre-scrape is best-effort — keep what we got and move on
                    # rather than killing the whole find() pipeline.
                    unfinished = sum(1 for f in futures if not f.done())
                    _log.debug(
                        "pre-scrape batch timeout after %ss, %d/%d futures unfinished",
                        per_batch_timeout, unfinished, len(futures),
                    )
                    for f in futures:
                        if not f.done():
                            f.cancel()
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
            if not c.get("email") and data.get("email") and is_plausible_email(data["email"]):
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

    def _persist(
        self,
        companies: list[dict[str, Any]],
        market: str,
        result: dict[str, Any],
        source_health: list[dict[str, Any]],
    ) -> None:
        import json as _json

        from market_validation.research import (
            _connect,
            _ensure_schema,
            add_company,
            resolve_db_path,
        )

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
            try:
                conn.execute(
                    "UPDATE researches SET last_source_health = ? WHERE id = ?",
                    (_json.dumps(source_health, ensure_ascii=True), self.research_id),
                )
            except sqlite3.Error as exc:
                # source_health is dashboard metadata — losing it shouldn't
                # fail find(), but we want to know when it happens.
                _log.warning("failed to persist last_source_health for %s: %s", self.research_id, exc)

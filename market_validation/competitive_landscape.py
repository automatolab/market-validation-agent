"""
Competitive Landscape — maps and analyzes the competitive field.

Uses free web search to gather raw competitor candidates, then AI to:
- Filter out directories/aggregators/review sites (not real competitors)
- Categorize as direct/indirect/substitute
- Assess market concentration and barriers to entry

Can also incorporate companies already discovered by the find() step.
"""

from __future__ import annotations

import json
import time
from typing import Any, Callable


def _search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results)
    except Exception:
        return []


def _gather_raw_candidates(results: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    Extract URL candidates from search results.
    Only minimal filtering — skip obvious non-business domains.
    AI does the real classification.
    """
    candidates = []
    seen_domains: set[str] = set()
    # Only skip truly universal non-business sites
    always_skip = {
        "wikipedia.org", "reddit.com", "quora.com", "youtube.com",
        "twitter.com", "facebook.com", "instagram.com", "tiktok.com",
        "google.com", "bing.com", "yahoo.com", "apple.com", "amazon.com",
    }
    for r in results:
        url = r.get("url", "").strip()
        title = r.get("title", "").strip()
        if not url or not title:
            continue
        domain = url.split("//")[-1].split("/")[0].lower().lstrip("www.")
        if any(skip in domain for skip in always_skip):
            continue
        if domain in seen_domains:
            continue
        seen_domains.add(domain)
        candidates.append({
            "name": title,
            "url": url,
            "domain": domain,
            "snippet": r.get("snippet", "")[:200],
        })
    return candidates


def analyze_competition(
    market: str,
    geography: str,
    product: str | None = None,
    existing_companies: list[dict[str, Any]] | None = None,
    run_ai: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Analyze the competitive landscape for a market.

    Args:
        market: Market description
        geography: Target geography
        product: Specific product (optional)
        existing_companies: Companies from find() step (optional)
        run_ai: AI callable (Agent._run). Required for structured output.

    Returns:
        Dict with competitive_intensity, market_concentration, competitors, etc.
    """
    search_term = product or market
    all_candidates: list[dict[str, str]] = []
    funding_snippets: list[str] = []

    # Market-aware competitor discovery queries
    try:
        from market_validation.query_context import get_validation_queries
        _comp_ctx = get_validation_queries(market, geography, product)["competition"]
        competitor_queries = _comp_ctx["competitor"]
        funding_queries = _comp_ctx["funding"]
    except Exception:
        competitor_queries = [
            f"{market} companies {geography}",
            f"top {market} providers {geography}",
            f"{search_term} competitors market leaders",
            f"{market} startups {geography}",
        ]
        funding_queries = [
            f"{market} startup funding raised",
            f"{market} acquisition {geography}",
            f"site:crunchbase.com {market}",
        ]

    for query in competitor_queries:
        results = _search(query, num_results=10)
        all_candidates.extend(_gather_raw_candidates(results))
        time.sleep(1.2)
    for query in funding_queries:
        for r in _search(query, num_results=8):
            s = r.get("snippet", "").strip()
            if s:
                funding_snippets.append(s)
        time.sleep(1.2)

    # Deduplicate by domain
    seen_domains: set[str] = set()
    unique_candidates: list[dict[str, str]] = []
    for c in all_candidates:
        d = c.get("domain", "")
        if d and d not in seen_domains:
            seen_domains.add(d)
            unique_candidates.append(c)

    # Incorporate existing companies from find() if available
    if existing_companies:
        for ec in existing_companies:
            name = ec.get("company_name", "")
            website = ec.get("website", "")
            if not name:
                continue
            domain = website.split("//")[-1].split("/")[0].lstrip("www.") if website else ""
            if domain and domain in seen_domains:
                continue
            if domain:
                seen_domains.add(domain)
            unique_candidates.append({
                "name": name,
                "url": website or "",
                "domain": domain,
                "snippet": ec.get("notes", "")[:200],
            })

    raw_count = len(unique_candidates)

    result: dict[str, Any] = {
        "raw_candidate_count": raw_count,
        "competitors_found": unique_candidates[:30],
        "funding_snippet_count": len(funding_snippets),
        # Heuristic defaults, overwritten by AI
        "competitive_intensity": 50,
        "competitor_count": raw_count,
        "market_concentration": "moderate",
    }

    if not run_ai:
        return result

    # AI does the real work: classify candidates and assess competition
    candidate_text = "\n".join(
        f"- {c['name']} ({c['domain']}): {c.get('snippet', '')}"
        for c in unique_candidates[:25]
    )
    funding_text = "\n".join(f"- {s}" for s in funding_snippets[:10])

    prompt = f"""You are a competitive intelligence analyst. Analyze the competitive landscape for a business entering:

Market: {market}
Geography: {geography}
Product/Service to sell: {product or 'general'}

Raw search results ({raw_count} candidates — mix of real competitors, directories, and aggregators):
{candidate_text or '(no candidates found)'}

Funding/growth signals from news:
{funding_text or '(none found)'}

Your job:
1. Identify which candidates are REAL competitors (businesses that compete in this market) vs. directories, review sites, aggregators, or market research firms — exclude the latter.
2. Categorize real competitors as direct (same product/customer), indirect (different approach), or substitute.
3. Assess the overall competitive landscape.

Return ONLY this JSON (no markdown fences):
{{
    "competitive_intensity": <0-100, where 100 = extremely competitive for a new entrant>,
    "competitor_count": <count of real competitors only>,
    "market_concentration": "<fragmented|moderate|consolidated|monopolistic>",
    "direct_competitors": ["company name 1", "company name 2", ...],
    "indirect_competitors": ["company name 1", ...],
    "substitutes": ["substitute 1", ...],
    "funding_signals": ["specific funding event or signal", ...],
    "dominant_players": ["top 2-3 market leaders"],
    "barriers_to_entry": ["specific barrier 1", "specific barrier 2"],
    "notes": "1-2 sentences on competitive dynamics and what this means for a new entrant"
}}

Scoring guide for competitive_intensity:
- 80-100: Many well-funded players, strong brand loyalty, high switching costs
- 50-79: Moderate competition, room for differentiation
- 20-49: Few competitors, fragmented, low switching costs
- <20: Little competition or very niche"""

    ai_result = run_ai(prompt)
    parsed: dict[str, Any] = {}
    if isinstance(ai_result, dict):
        if "competitive_intensity" in ai_result:
            parsed = ai_result
        elif "text" in ai_result:
            try:
                parsed = json.loads(ai_result["text"])
            except (json.JSONDecodeError, TypeError):
                result["ai_raw"] = ai_result.get("text", str(ai_result))
    elif isinstance(ai_result, str):
        try:
            parsed = json.loads(ai_result)
        except (json.JSONDecodeError, TypeError):
            result["ai_raw"] = ai_result

    result.update(parsed)
    return result

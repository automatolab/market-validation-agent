"""
Market Sizing — estimates TAM/SAM/SOM from free public data + AI synthesis.

Data sources (all free, no API keys):
- DuckDuckGo web search for industry reports, census data, trade associations
- Statista/IBISWorld preview snippets
- Government sources (census.gov, bls.gov, bea.gov)

AI synthesis (claude/opencode) is required for structured TAM/SAM/SOM output.
Raw snippets are always returned alongside structured estimates.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any


def _search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results)
    except Exception:
        return []


def _collect_snippets(results: list[dict[str, str]]) -> list[str]:
    """Extract non-empty snippets from search results."""
    snippets = []
    for r in results:
        snippet = r.get("snippet", "").strip()
        title = r.get("title", "").strip()
        url = r.get("url", "").strip()
        if snippet:
            snippets.append(f"[{title}]({url}): {snippet}")
        elif title:
            snippets.append(f"[{title}]({url})")
    return snippets


def estimate_market_size(
    market: str,
    geography: str,
    product: str | None = None,
    run_ai: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Estimate TAM/SAM/SOM for a given market using web search + AI synthesis.

    Args:
        market: Market description (e.g. "BBQ restaurants")
        geography: Target geography (e.g. "San Jose, California")
        product: Specific product/service within the market (optional)
        run_ai: AI callable (Agent._run). Required for structured estimates.

    Returns:
        Dict with tam_low, tam_high, tam_confidence, sam_*, som_*, sources, etc.
    """
    search_term = product or market
    all_snippets: list[str] = []
    sources_used: list[str] = []

    try:
        from market_validation.query_context import get_validation_queries
        queries = get_validation_queries(market, geography, product)["sizing"]
    except Exception:
        queries = [
            f"{market} market size {geography}",
            f"{market} industry revenue total addressable market",
            f"{search_term} TAM market opportunity",
            f"site:census.gov {market} statistics",
            f"site:bls.gov {market} industry employment",
            f"{market} market report 2024 2025",
            f"{market} industry growth rate forecast",
        ]

    all_search_results: list[dict[str, str]] = []
    for query in queries:
        results = _search(query, num_results=8)
        snippets = _collect_snippets(results)
        if snippets:
            all_snippets.extend(snippets)
            sources_used.append(query)
            all_search_results.extend(results)
        time.sleep(1.2)

    # Deep-scrape top search result pages for richer content than snippets
    try:
        from market_validation.web_scraper import scrape_search_result_pages
        scraped_pages = scrape_search_result_pages(
            all_search_results, max_pages=4, max_chars_each=1200, delay=1.0
        )
        for page in scraped_pages:
            content = page.get("content", "").strip()
            title = page.get("title", "")
            url = page.get("url", "")
            if content and len(content) > 150:
                all_snippets.append(f"[Full page — {title}]({url}): {content[:800]}")
                sources_used.append(f"scraped: {url[:60]}")
    except Exception:
        pass

    # Pull free structured data sources (BLS, EDGAR, Wikipedia, Yelp) — no paid API needed
    try:
        from market_validation.free_data_sources import (
            bls_industry_data,
            edgar_search,
            wikipedia_industry_summary,
            yelp_local_market_data,
        )
        from market_validation.market_archetype import detect_archetype
        from market_validation.query_context import detect_market_category
        category = detect_market_category(market, product)
        archetype_key, _ = detect_archetype(market, product)

        bls = bls_industry_data(category)
        if bls.get("snippet"):
            all_snippets.insert(0, f"[BLS.gov]: {bls['snippet']}")
            sources_used.append("BLS.gov employment data")
        time.sleep(0.5)

        edgar_hits = edgar_search(f"{market} market size revenue", limit=5)
        for h in edgar_hits:
            if h.get("snippet"):
                all_snippets.append(f"[SEC EDGAR {h['company']} {h['form']} {h['filed']}]: {h['snippet']}")
        if edgar_hits:
            sources_used.append("SEC EDGAR 10-K filings")
        time.sleep(0.5)

        wiki = wikipedia_industry_summary(market)
        if wiki.get("snippet"):
            all_snippets.insert(0, f"[Wikipedia — {wiki['title']}]: {wiki['extract']}")
            sources_used.append(f"Wikipedia: {wiki['title']}")

        # Yelp local market density — valuable for local-service TAM estimation
        yelp_data: dict = {}
        if archetype_key == "local-service" or category in ("food", "retail"):
            yelp_data = yelp_local_market_data(search_term, geography)
            if yelp_data.get("snippet"):
                all_snippets.insert(0, f"[Yelp]: {yelp_data['snippet']}")
                sources_used.append(yelp_data.get("source", "Yelp"))
            time.sleep(0.5)
    except Exception:
        bls = {}
        yelp_data = {}

    # Deduplicate
    seen: set[str] = set()
    unique_snippets: list[str] = []
    for s in all_snippets:
        key = s[:100].lower()
        if key not in seen:
            seen.add(key)
            unique_snippets.append(s)

    result: dict[str, Any] = {
        "raw_snippets": unique_snippets[:35],
        "sources_used": sources_used,
        "snippet_count": len(unique_snippets),
        "bls_data": bls if "bls" in dir() else {},
        "yelp_data": yelp_data if "yelp_data" in dir() else {},
    }

    if not run_ai:
        return result

    snippet_text = "\n".join(f"- {s}" for s in unique_snippets[:28])

    # Surface BLS employment data explicitly so AI can use it for sizing
    _bls = result.get("bls_data") or {}
    bls_context = ""
    if _bls.get("employment"):
        bls_context = (
            f"\nBLS.gov structured data (authoritative):\n"
            f"- Industry: {_bls['label']}\n"
            f"- US employment: {_bls['employment']:,} workers\n"
            f"- Year-over-year change: {_bls['yoy_change_pct']:+.1f}% ({_bls['trend']})\n"
            f"- As of: {_bls['period']}\n"
        )

    # Yelp business count context for local-service market sizing
    _yelp = result.get("yelp_data") or {}
    yelp_context = ""
    if _yelp.get("total") or _yelp.get("business_count"):
        total = _yelp.get("total") or _yelp.get("business_count") or 0
        avg_r = _yelp.get("avg_rating")
        price_dist = _yelp.get("price_distribution") or {}
        yelp_context = (
            f"\nYelp local market data (authoritative for {geography}):\n"
            f"- Competing businesses found: {total:,}\n"
        )
        if avg_r:
            yelp_context += f"- Average rating: {avg_r}★\n"
        if price_dist and any(price_dist.values()):
            dominant = max(price_dist, key=price_dist.get)
            yelp_context += f"- Dominant price tier: {dominant}\n"
        yelp_context += "- Use business count × avg annual revenue per location to anchor SAM estimate.\n"

    prompt = f"""You are a market research analyst. Estimate the market size for:

Market: {market}
Geography: {geography}
Product/Service: {product or 'general market'}
{bls_context}{yelp_context}
Search result snippets (web + SEC filings + Wikipedia + Yelp):
{snippet_text or '(no snippets found — use your knowledge)'}

IMPORTANT — first determine what this market IS:
- If "{market}" is a RAW INGREDIENT or PRODUCT (e.g. brisket, lumber, organic cotton):
  TAM = total spending on this product nationally (all channels: retail, wholesale, foodservice).
  SAM = spending in {geography} specifically (restaurants, distributors, retail stores that buy it).
  Think about the SUPPLY CHAIN: who produces it, who distributes it, who consumes it.
- If "{market}" is a SERVICE (e.g. pet grooming, consulting):
  TAM = total industry revenue nationally. SAM = revenue in {geography}. SOM = new entrant capture.
- If "{market}" is a TECHNOLOGY/SOFTWARE:
  TAM = global/national software spend in category. SAM = target segment. SOM = realistic first-year ARR.

Return ONLY this JSON (numbers in USD, no markdown fences):
{{
    "tam_low": <total addressable market low estimate in dollars>,
    "tam_high": <total addressable market high estimate in dollars>,
    "tam_confidence": <0-100 confidence>,
    "sam_low": <serviceable addressable market low for {geography}>,
    "sam_high": <serviceable addressable market high for {geography}>,
    "sam_confidence": <0-100>,
    "som_low": <serviceable obtainable market low for a new entrant>,
    "som_high": <serviceable obtainable market high for a new entrant>,
    "som_confidence": <0-100>,
    "tam_sources": [
      {{"value": "$X", "source_url": "https://bls.gov/...", "source_authority": "primary_government", "evidence": "BLS Q3 2025 release: total food-services revenue ..."}},
      {{"value": "$Y", "source_url": "https://...", "source_authority": "trade_press_or_business_news", "evidence": "..."}}
    ],
    "sam_sources": [
      {{"value": "$X", "source_url": "...", "source_authority": "...", "evidence": "..."}}
    ],
    "som_sources": [
      {{"value": "$X", "source_url": "...", "source_authority": "...", "evidence": "..."}}
    ],
    "growth_rate": <annual growth rate as decimal e.g. 0.08 for 8%>,
    "growth_rate_source": {{"source_url": "...", "source_authority": "...", "evidence": "..."}},
    "notes": "1-2 sentences explaining what this market is and how you sized it"
}}

Rules:
- SAM must be a geographic subset of TAM. SOM is 1-5% of SAM for a new entrant.
- Every numeric estimate must include at least one entry in *_sources with an actual URL.
- source_authority must be one of: primary_government, paid_research_or_academic,
  trade_press_or_business_news, encyclopedic_or_community, general_web, ai_inference_no_source.
- Confidence calibration (do NOT inflate):
  - 80+ ONLY when a Tier-1 (BLS/SEC/Census/government) source corroborates.
  - 60-79 when 2+ Tier-2/3 sources (industry reports, trade press) agree.
  - 40-59 for a single Tier-3/4 source or strong AI inference with reasoning.
  - <40 when extrapolating without a citable source.
- BLS employment data is Tier-1 authoritative — use it to anchor TAM when available.
- Yelp business count × avg revenue per location is a strong SAM anchor for local markets.
- If no relevant snippets and no authoritative sources, set confidence < 40 and
  note the gap explicitly in the `notes` field."""

    ai_result = run_ai(prompt)

    parsed: dict[str, Any] = {}
    if isinstance(ai_result, dict):
        if "tam_low" in ai_result:
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
    # Enforce citation rules — drops uncited source entries and caps stated
    # confidence by the strongest source tier actually present.
    from market_validation._helpers.citations import RULES_FOR_SIZING, enforce_citations
    enforce_citations(result, RULES_FOR_SIZING)
    return result

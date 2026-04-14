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
from typing import Any, Callable


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

    for query in queries:
        results = _search(query, num_results=8)
        snippets = _collect_snippets(results)
        if snippets:
            all_snippets.extend(snippets)
            sources_used.append(query)
        time.sleep(1.2)

    # Pull free structured data sources (BLS, EDGAR, Wikipedia) — no API key needed
    try:
        from market_validation.free_data_sources import (
            bls_industry_data, edgar_search, wikipedia_industry_summary
        )
        from market_validation.query_context import detect_market_category
        category = detect_market_category(market, product)

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
    except Exception:
        bls = {}

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

    prompt = f"""You are a market research analyst. Estimate the market size for:

Market: {market}
Geography: {geography}
Product/Service: {product or 'general market'}
{bls_context}
Search result snippets (web + SEC filings + Wikipedia):
{snippet_text or '(no snippets found — use your knowledge)'}

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
    "tam_sources": ["cite snippet or source 1", "cite snippet or source 2"],
    "sam_sources": ["cite snippet or source"],
    "som_sources": ["cite snippet or source"],
    "growth_rate": <annual growth rate as decimal e.g. 0.08 for 8%>,
    "notes": "1-2 sentences on methodology and data quality"
}}

Rules:
- SAM must be a geographic subset of TAM. SOM is 1-5% of SAM for a new entrant.
- BLS employment data is authoritative — use it to anchor TAM estimates when available.
- Confidence: 75+ if BLS/EDGAR data corroborates, 50-74 if 3+ web snippets agree, <50 if extrapolated or guessed.
- If no relevant snippets, estimate from your training knowledge — but set confidence < 40."""

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
    return result

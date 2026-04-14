"""
Unit Economics — estimates gross margin, CAC, LTV, and payback period for a market.

Data pipeline:
1. Web search for pricing signals, margin benchmarks, and CAC signals
2. BLS wage data as a labor cost baseline
3. AI synthesis into structured unit economics output

AI synthesis (run_ai) is required for structured output. Without it, raw
gathered data is returned for inspection.
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
    """Extract non-empty snippets (with title and URL context) from search results."""
    snippets: list[str] = []
    for r in results:
        snippet = r.get("snippet", "").strip()
        title = r.get("title", "").strip()
        url = r.get("url", "").strip()
        if snippet:
            snippets.append(f"[{title}]({url}): {snippet}" if title else snippet)
        elif title:
            snippets.append(f"[{title}]({url})")
    return snippets


def estimate_unit_economics(
    market: str,
    geography: str,
    product: str | None = None,
    archetype: str = "b2b-industrial",
    run_ai: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Estimate unit economics (gross margin, CAC, LTV, payback) for a market.

    Args:
        market: Market description (e.g. "BBQ restaurants")
        geography: Target geography (e.g. "San Jose, California")
        product: Specific product/service within the market (optional)
        archetype: Archetype key from ARCHETYPES (used for margin benchmarks)
        run_ai: AI callable (Agent._run). Required for structured output.

    Returns:
        Dict with gross_margin_low/high, cac/ltv estimates, payback_months,
        unit_economics_score, and supporting signals.
    """
    from market_validation.market_archetype import get_archetype_config

    archetype_config = get_archetype_config(archetype)
    search_term = product or market
    all_snippets: list[str] = []

    # ------------------------------------------------------------------
    # 1. Pricing signals
    # ------------------------------------------------------------------
    pricing_results = _search(
        f'"{search_term}" pricing price per unit',
        num_results=8,
    )
    all_snippets.extend(_collect_snippets(pricing_results))
    time.sleep(1.2)

    cost_results = _search(
        f"how much does {search_term} cost",
        num_results=8,
    )
    all_snippets.extend(_collect_snippets(cost_results))
    time.sleep(1.2)

    # ------------------------------------------------------------------
    # 2. Margin benchmarks
    # ------------------------------------------------------------------
    margin_results = _search(
        f"{market} gross margin profit margin industry",
        num_results=8,
    )
    all_snippets.extend(_collect_snippets(margin_results))
    time.sleep(1.2)

    # ------------------------------------------------------------------
    # 3. CAC signals
    # ------------------------------------------------------------------
    cac_results = _search(
        f"cost to acquire customer {market}",
        num_results=8,
    )
    all_snippets.extend(_collect_snippets(cac_results))
    time.sleep(1.2)

    marketing_results = _search(
        f"marketing spend {market} customer acquisition",
        num_results=8,
    )
    all_snippets.extend(_collect_snippets(marketing_results))
    time.sleep(1.2)

    # ------------------------------------------------------------------
    # 4. BLS labor cost baseline
    # ------------------------------------------------------------------
    labor: dict[str, Any] = {}
    try:
        from market_validation.free_data_sources import bls_wages_data
        labor = bls_wages_data(archetype)
    except Exception:
        labor = {}

    # ------------------------------------------------------------------
    # Deduplicate snippets
    # ------------------------------------------------------------------
    seen: set[str] = set()
    unique_snippets: list[str] = []
    for s in all_snippets:
        key = s[:100].lower()
        if key not in seen:
            seen.add(key)
            unique_snippets.append(s)

    # ------------------------------------------------------------------
    # Build result with raw data — returned as-is if no AI callable
    # ------------------------------------------------------------------
    result: dict[str, Any] = {
        "market": market,
        "geography": geography,
        "product": product,
        "archetype": archetype,
        "archetype_label": archetype_config["label"],
        "raw_snippets": unique_snippets[:40],
        "snippet_count": len(unique_snippets),
        "bls_labor": labor,
    }

    if not run_ai:
        return result

    # ------------------------------------------------------------------
    # AI synthesis
    # ------------------------------------------------------------------
    snippet_text = "\n".join(f"- {s}" for s in unique_snippets[:30])

    typical_margins = archetype_config["typical_gross_margins"]
    cac_range = archetype_config["cac_range"]
    ltv_cac = archetype_config["ltv_cac_ratio"]

    labor_context = ""
    if labor.get("avg_hourly_wage"):
        labor_context = (
            f"\nBLS Labor Cost Baseline ({labor['label']}):\n"
            f"- Avg hourly wage: ${labor['avg_hourly_wage']:.2f}\n"
            f"- Avg annual wage: ${labor.get('avg_annual_wage', 0):,.0f}\n"
            f"- As of: {labor.get('period', 'N/A')}\n"
        )

    prompt = f"""You are a financial analyst specializing in unit economics. Estimate the unit economics for:

Market: {market}
Geography: {geography}
Product/Service: {product or 'general market'}
Archetype: {archetype_config['label']}

Archetype typical benchmarks (use as prior, adjust based on evidence):
- Gross margin range: {typical_margins['low']*100:.0f}% – {typical_margins['high']*100:.0f}% (mid: {typical_margins['mid']*100:.0f}%)
- CAC range: ${cac_range['low']} – ${cac_range['high']}
- LTV/CAC ratio range: {ltv_cac['low']}x – {ltv_cac['high']}x
{labor_context}
Web research snippets (pricing, margin, and CAC signals):
{snippet_text or '(no snippets found — use your knowledge and archetype benchmarks)'}

Return ONLY this JSON (no markdown fences):
{{
    "gross_margin_low": <lower bound gross margin as decimal e.g. 0.55>,
    "gross_margin_high": <upper bound gross margin as decimal e.g. 0.72>,
    "gross_margin_confidence": <0-100 confidence in this estimate>,
    "cac_estimate_low": <lower bound CAC in dollars>,
    "cac_estimate_high": <upper bound CAC in dollars>,
    "ltv_estimate_low": <lower bound LTV in dollars>,
    "ltv_estimate_high": <upper bound LTV in dollars>,
    "payback_months": <estimated months to recoup CAC at midpoint margin>,
    "unit_economics_score": <0-100 composite score>,
    "pricing_signals": ["specific pricing observation 1", "specific pricing observation 2"],
    "margin_driver": "<primary driver of margin structure e.g. 'labor and food costs dominate'>",
    "notes": "1-2 sentences on the unit economics picture and data quality"
}}

Scoring guide for unit_economics_score:
- 75+: gross margin >70%, LTV/CAC >5x, payback <6 months
- 50-74: gross margin 40-70%, LTV/CAC 3-5x, payback 6-18 months
- 25-49: gross margin 20-40%, LTV/CAC 2-3x, payback 18-36 months
- <25: gross margin <20% or LTV/CAC <2x

Rules:
- Anchor estimates to web evidence where available; fall back to archetype benchmarks.
- Pricing signals must be concrete (e.g. "wholesale brisket $4.50-6.00/lb", "SaaS seat $25-80/mo") not vague.
- gross_margin_confidence: 70+ if multiple snippets confirm; 40-69 if extrapolated; <40 if inferred from benchmarks only."""

    ai_result = run_ai(prompt)

    parsed: dict[str, Any] = {}
    if isinstance(ai_result, dict):
        if "gross_margin_low" in ai_result:
            parsed = ai_result
        elif "text" in ai_result:
            try:
                parsed = json.loads(ai_result["text"])
            except (json.JSONDecodeError, TypeError):
                result["ai_raw"] = ai_result.get("text", "")
    elif isinstance(ai_result, str):
        try:
            parsed = json.loads(ai_result)
        except (json.JSONDecodeError, TypeError):
            result["ai_raw"] = ai_result

    result.update(parsed)
    return result

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
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from market_validation.log import get_logger

_log = get_logger("unit_economics")


def _search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results)
    except Exception as exc:
        _log.debug("multi_search.quick_search failed for %r: %s", query, exc)
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

    # All five web searches plus BLS lookup are independent — fire them
    # concurrently. Was 5 × time.sleep(1.2) ≈ 6s of pure wait time.
    pricing_q = f'"{search_term}" pricing price per unit'
    cost_q = f"how much does {search_term} cost"
    margin_q = f"{market} gross margin profit margin industry"
    cac_q = f"cost to acquire customer {market}"
    marketing_q = f"marketing spend {market} customer acquisition"

    labor: dict[str, Any] = {}

    def _fetch_labor() -> dict[str, Any]:
        try:
            from market_validation.free_data_sources import bls_wages_data
            return bls_wages_data(archetype) or {}
        except Exception as exc:
            _log.debug("bls_wages_data failed: %s", exc)
            return {}

    with ThreadPoolExecutor(max_workers=6) as pool:
        f_price = pool.submit(_search, pricing_q, 8)
        f_cost = pool.submit(_search, cost_q, 8)
        f_margin = pool.submit(_search, margin_q, 8)
        f_cac = pool.submit(_search, cac_q, 8)
        f_mkt = pool.submit(_search, marketing_q, 8)
        f_lab = pool.submit(_fetch_labor)

        all_snippets: list[str] = []
        all_snippets.extend(_collect_snippets(f_price.result()))
        all_snippets.extend(_collect_snippets(f_cost.result()))
        all_snippets.extend(_collect_snippets(f_margin.result()))
        all_snippets.extend(_collect_snippets(f_cac.result()))
        all_snippets.extend(_collect_snippets(f_mkt.result()))
        labor = f_lab.result()

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
    "gross_margin_source": {{"source_url": "...", "source_authority": "primary_government|paid_research_or_academic|trade_press_or_business_news|encyclopedic_or_community|general_web|ai_inference_no_source", "evidence": "1 sentence cited from above"}},
    "cac_estimate_low": <lower bound CAC in dollars>,
    "cac_estimate_high": <upper bound CAC in dollars>,
    "cac_source": {{"source_url": "...", "source_authority": "...", "evidence": "..."}},
    "ltv_estimate_low": <lower bound LTV in dollars>,
    "ltv_estimate_high": <upper bound LTV in dollars>,
    "ltv_assumption_churn_years": <years assumed for LTV calculation>,
    "ltv_assumption_monthly_churn": <decimal e.g. 0.03 for 3% monthly churn>,
    "payback_months": <estimated months to recoup CAC at midpoint margin>,
    "unit_economics_score": <0-100 composite score>,
    "pricing_signals": [
      {{"signal": "wholesale brisket $4.50-6.00/lb", "source_url": "...", "evidence": "..."}}
    ],
    "margin_driver": "<primary driver of margin structure e.g. 'labor and food costs dominate'>",
    "notes": "1-2 sentences on the unit economics picture and data quality"
}}

Scoring guide for unit_economics_score:
- 75+: gross margin >70%, LTV/CAC >5x, payback <6 months
- 50-74: gross margin 40-70%, LTV/CAC 3-5x, payback 6-18 months
- 25-49: gross margin 20-40%, LTV/CAC 2-3x, payback 18-36 months
- <25: gross margin <20% or LTV/CAC <2x

Citation rules:
- Each estimate (gross_margin, cac, ltv) MUST have an attached *_source object.
- Pricing signals must be concrete (e.g. "wholesale brisket $4.50-6.00/lb") AND
  cite the URL or snippet they came from. No vague claims like "moderate margins".
- LTV must show its churn assumption explicitly — otherwise an analyst can't
  audit the math.
- Confidence calibration:
  - 75+ ONLY when a Tier-1/2 source (10-K filings, BLS, paid research) corroborates.
  - 50-74 when 2+ Tier-3 sources (trade press) agree or archetype benchmarks
    align with one citable source.
  - 25-49 when extrapolating from a single weak source.
  - <25 when inferred from archetype benchmarks alone."""

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
    from market_validation._helpers.citations import (
        RULES_FOR_UNIT_ECONOMICS,
        enforce_citations,
    )
    enforce_citations(result, RULES_FOR_UNIT_ECONOMICS)
    return result

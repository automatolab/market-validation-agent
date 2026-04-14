"""
Timing Analysis — assesses whether now is the right moment to enter a market.

Evaluates four timing dimensions:
- Enablers: new infrastructure/platforms reducing barriers
- Incumbent signals: are leaders investing or coasting?
- Adjacent market pull: related markets growing and creating spillover demand
- Regulatory window: laws opening or closing the opportunity

Produces a timing_score (0-100) and verdict (good|neutral|early|late|poor).

AI synthesis (claude/opencode) is required for structured output.
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


def _snippets(query: str, num_results: int = 10) -> list[str]:
    """Search and return non-empty snippets."""
    results = _search(query, num_results)
    return [r.get("snippet", "").strip() for r in results if r.get("snippet", "").strip()]


def analyze_timing(
    market: str,
    geography: str,
    product: str | None = None,
    archetype: str = "b2b-industrial",
    signals: dict[str, Any] | None = None,
    run_ai: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Analyze market timing — whether now is the right time to enter.

    Args:
        market: Market description (e.g. "BBQ catering San Jose")
        geography: Target geography (e.g. "San Jose, CA")
        product: Specific product or service (optional)
        archetype: Market archetype string used to tailor searches.
            Common values: "b2b-industrial", "local-service", "b2b-saas",
            "consumer", "marketplace".
        signals: Output from gather_market_signals() (optional). Avoids
            re-running searches by reusing news_sentiment, job_posting_volume,
            regulatory_risks, and key_trends from a prior run.
        run_ai: AI callable (Agent._run). Required for structured output.

    Returns:
        Dict with timing_score, timing_verdict, enablers, headwinds,
        adjacent_market_signal, incumbent_posture, regulatory_window, etc.
    """
    search_term = product or market
    _archetype = archetype.lower()

    # ------------------------------------------------------------------
    # Category 1 — Enablers
    # ------------------------------------------------------------------
    enabler_snippets: list[str] = []

    enabler_snippets.extend(_snippets(f"{market} new platform infrastructure enabling {geography}", 10))
    time.sleep(1.2)
    enabler_snippets.extend(_snippets(f"{market} technology reducing barrier cost", 10))
    time.sleep(1.2)

    if "local" in _archetype or "food" in _archetype or "restaurant" in _archetype:
        enabler_snippets.extend(_snippets(f"ghost kitchen shared kitchen {geography}", 10))
        time.sleep(1.2)
        enabler_snippets.extend(_snippets(f"food delivery growth {geography}", 10))
        time.sleep(1.2)

    if "saas" in _archetype or "software" in _archetype:
        enabler_snippets.extend(_snippets(f"API integration ecosystem {market}", 10))
        time.sleep(1.2)
        enabler_snippets.extend(_snippets(f"no-code low-code {market}", 10))
        time.sleep(1.2)

    enabler_snippets = enabler_snippets[:6]

    # ------------------------------------------------------------------
    # Category 2 — Incumbent signals
    # ------------------------------------------------------------------
    incumbent_snippets: list[str] = []

    incumbent_snippets.extend(_snippets(f"{market} {geography} leader investing expanding", 10))
    time.sleep(1.2)
    incumbent_snippets.extend(_snippets(f"{market} innovation slow stagnant incumbents", 10))
    time.sleep(1.2)
    incumbent_snippets.extend(_snippets(f"{market} acquisition funding recent 2024 2025", 10))
    time.sleep(1.2)

    incumbent_snippets = incumbent_snippets[:6]

    # ------------------------------------------------------------------
    # Category 3 — Adjacent market pull
    # ------------------------------------------------------------------
    adjacent_snippets: list[str] = []

    adjacent_snippets.extend(_snippets(f"{market} adjacent market growing trend", 10))
    time.sleep(1.2)

    if "food" in _archetype or "local" in _archetype or "restaurant" in _archetype:
        adjacent_snippets.extend(_snippets("food delivery ghost kitchen growth", 10))
        time.sleep(1.2)
        adjacent_snippets.extend(_snippets("meal prep catering growth", 10))
        time.sleep(1.2)

    if "saas" in _archetype or "software" in _archetype:
        adjacent_snippets.extend(_snippets(f"digital transformation {market} adoption", 10))
        time.sleep(1.2)

    adjacent_snippets = adjacent_snippets[:6]

    # ------------------------------------------------------------------
    # Category 4 — Regulatory window
    # ------------------------------------------------------------------
    regulatory_snippets: list[str] = []

    regulatory_snippets.extend(_snippets(f"{market} {geography} new regulation law 2024 2025 opportunity", 10))
    time.sleep(1.2)
    regulatory_snippets.extend(_snippets(f"{market} deregulation opening market", 10))
    time.sleep(1.2)

    regulatory_snippets = regulatory_snippets[:6]

    # ------------------------------------------------------------------
    # Existing signals context (avoid re-running if caller passed output
    # from gather_market_signals())
    # ------------------------------------------------------------------
    signals_context: dict[str, Any] = {}
    if signals:
        signals_context = {
            "news_sentiment": signals.get("news_sentiment"),
            "job_posting_volume": signals.get("job_posting_volume"),
            "regulatory_risks": signals.get("regulatory_risks", []),
            "key_trends": signals.get("key_trends", []),
            "timing_assessment": signals.get("timing_assessment"),
        }

    # ------------------------------------------------------------------
    # Raw return (no AI)
    # ------------------------------------------------------------------
    raw_data = {
        "enablers": enabler_snippets,
        "incumbents": incumbent_snippets,
        "adjacent": adjacent_snippets,
        "regulatory": regulatory_snippets,
    }

    if not run_ai:
        return {"raw_snippets": raw_data, "signals_context": signals_context}

    # ------------------------------------------------------------------
    # Build AI prompt
    # ------------------------------------------------------------------
    def _fmt(label: str, items: list[str]) -> str:
        if not items:
            return f"\n{label}:\n  (no data gathered)\n"
        lines = "\n".join(f"  - {s[:160]}" for s in items)
        return f"\n{label}:\n{lines}\n"

    snippets_text = (
        _fmt("ENABLERS (infrastructure/platforms reducing entry barriers)", enabler_snippets)
        + _fmt("INCUMBENT SIGNALS (are leaders investing or coasting?)", incumbent_snippets)
        + _fmt("ADJACENT MARKET PULL (related markets creating spillover demand)", adjacent_snippets)
        + _fmt("REGULATORY WINDOW (laws opening or closing the opportunity)", regulatory_snippets)
    )

    signals_text = ""
    if signals_context:
        risks_str = ", ".join(signals_context.get("regulatory_risks", [])) or "none noted"
        trends_str = ", ".join(signals_context.get("key_trends", [])) or "none noted"
        signals_text = f"""
Prior market signals data:
- News sentiment: {signals_context.get('news_sentiment', 'unknown')}
- Job posting volume: {signals_context.get('job_posting_volume', 'unknown')}
- Regulatory risks: {risks_str}
- Key trends: {trends_str}
- Timing assessment: {signals_context.get('timing_assessment', 'unknown')}
"""

    prompt = f"""You are a market timing analyst. Assess whether NOW is the right time to enter:

Market: {market}
Geography: {geography}
Product/Service: {product or 'general'}
Archetype: {archetype}
{signals_text}
Evidence gathered from web searches:
{snippets_text}

Return ONLY this JSON (no markdown fences):
{{
    "timing_score": <0-100>,
    "timing_verdict": "<good|neutral|early|late|poor>",
    "enablers": ["<specific enabler 1>", "<specific enabler 2>"],
    "headwinds": ["<specific headwind 1>", "<specific headwind 2>"],
    "adjacent_market_signal": "<positive|neutral|negative>",
    "adjacent_market_notes": "<1 sentence on adjacent market dynamics>",
    "incumbent_posture": "<investing|complacent|retrenching>",
    "regulatory_window": "<opening|neutral|closing>",
    "timing_notes": "<1-2 sentences on overall timing verdict>"
}}

Scoring guide for timing_score:
- 75+: Multiple clear enablers, incumbents complacent, regulatory tailwind, adjacent market pulling
- 50-74: Mixed signals, some enablers but headwinds present
- 25-49: Headwinds dominate, incumbents actively investing and defending, regulatory risk
- <25: Wrong timing — market is either too early (infrastructure not ready) or too late (saturated/declining)

Definitions:
- enablers: specific technologies, platforms, or structural changes that lower barriers RIGHT NOW
- headwinds: market forces, cost pressures, or trends working against a new entrant right now
- incumbent_posture: "investing" = actively spending/acquiring/innovating; "complacent" = coasting on legacy; "retrenching" = cutting back
- regulatory_window: "opening" = new law creates opportunity or removes barrier; "closing" = upcoming regulation adds risk
- If evidence is thin, apply neutral scores and note it in timing_notes"""

    ai_result = run_ai(prompt)

    result: dict[str, Any] = {
        "raw_snippets": raw_data,
        "signals_context": signals_context,
    }

    parsed: dict[str, Any] = {}
    if isinstance(ai_result, dict):
        if "timing_score" in ai_result:
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

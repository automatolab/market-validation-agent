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
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from market_validation.log import get_logger

_log = get_logger("timing_analysis")


def _search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results)
    except Exception as exc:
        _log.debug("multi_search.quick_search failed for %r: %s", query, exc)
        return []


def _snippets(query: str, num_results: int = 10) -> list[str]:
    """Search and return non-empty snippets."""
    results = _search(query, num_results)
    return [r.get("snippet", "").strip() for r in results if r.get("snippet", "").strip()]


def _parallel_snippets(queries: list[str], num_results: int = 10) -> list[str]:
    """Run ``_snippets`` for many queries concurrently and concatenate the
    results in the original order.

    Replaces the old serial pattern of ``for q in queries: snippets.extend(...);
    time.sleep(1.2)`` which dominated wall-clock time inside this module.
    Search backends are independent calls so there's no need to space them
    out — the multi_search rate-limit logic handles backend politeness.
    """
    if not queries:
        return []
    out: list[str] = []
    with ThreadPoolExecutor(max_workers=min(8, len(queries))) as pool:
        # ``pool.map`` preserves input order, which keeps snippet ordering
        # stable for tests and for prompt construction.
        for batch in pool.map(lambda q: _snippets(q, num_results), queries):
            out.extend(batch)
    return out


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
    _archetype = archetype.lower()
    _is_local = any(t in _archetype for t in ("local", "food", "restaurant"))
    _is_saas = "saas" in _archetype or "software" in _archetype

    # ------------------------------------------------------------------
    # Build the per-category query lists, then fetch them all in parallel.
    # The original code interleaved 15 ``_snippets`` calls with 1.2s sleeps,
    # serializing what should be independent web-search lookups.
    # ------------------------------------------------------------------
    enabler_queries: list[str] = [
        f"{market} new platform infrastructure enabling {geography}",
        f"{market} technology reducing barrier cost",
    ]
    if _is_local:
        enabler_queries += [
            f"ghost kitchen shared kitchen {geography}",
            f"food delivery growth {geography}",
        ]
    if _is_saas:
        enabler_queries += [
            f"API integration ecosystem {market}",
            f"no-code low-code {market}",
        ]

    incumbent_queries: list[str] = [
        f"{market} {geography} leader investing expanding",
        f"{market} innovation slow stagnant incumbents",
        f"{market} acquisition funding recent 2024 2025",
    ]

    adjacent_queries: list[str] = [
        f"{market} adjacent market growing trend",
    ]
    if _is_local:
        adjacent_queries += [
            "food delivery ghost kitchen growth",
            "meal prep catering growth",
        ]
    if _is_saas:
        adjacent_queries.append(f"digital transformation {market} adoption")

    regulatory_queries: list[str] = [
        f"{market} {geography} new regulation law 2024 2025 opportunity",
        f"{market} deregulation opening market",
    ]

    # Fetch every category concurrently. Inside each category the per-query
    # calls are ALSO concurrent (via _parallel_snippets), so total wall-clock
    # is roughly the slowest individual search rather than the sum.
    with ThreadPoolExecutor(max_workers=4) as pool:
        f_enab = pool.submit(_parallel_snippets, enabler_queries)
        f_incum = pool.submit(_parallel_snippets, incumbent_queries)
        f_adj = pool.submit(_parallel_snippets, adjacent_queries)
        f_reg = pool.submit(_parallel_snippets, regulatory_queries)
        enabler_snippets = f_enab.result()[:6]
        incumbent_snippets = f_incum.result()[:6]
        adjacent_snippets = f_adj.result()[:6]
        regulatory_snippets = f_reg.result()[:6]

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
        from market_validation.validation_scorecard import _flatten_strings
        risks_str = _flatten_strings(signals_context.get("regulatory_risks", [])) or "none noted"
        trends_str = _flatten_strings(signals_context.get("key_trends", [])) or "none noted"
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
    "enablers": [
      {{"enabler": "specific tech / platform / structural change", "evidence": "1-line snippet", "source_url": "..."}}
    ],
    "headwinds": [
      {{"headwind": "specific market force", "evidence": "1-line snippet", "source_url": "..."}}
    ],
    "adjacent_market_signal": "<positive|neutral|negative>",
    "adjacent_market_notes": "<1 sentence on adjacent market dynamics>",
    "incumbent_posture": "<investing|complacent|retrenching>",
    "incumbent_evidence": "1 sentence — recent funding / acquisition / layoff / R&D headline that shows posture",
    "regulatory_window": "<opening|neutral|closing>",
    "regulatory_evidence": "1 sentence — specific bill / rulemaking / deadline if any (or 'none found')",
    "timing_notes": "<1-2 sentences on overall timing verdict>"
}}

Citation rules:
- Every enabler and headwind MUST be backed by an evidence snippet drawn
  from the search blocks above. Don't list one you can't cite.
- incumbent_posture and regulatory_window must include their own evidence
  fields — if you can't cite a specific signal, return "neutral" and
  explicitly note the absence in timing_notes.

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
    from market_validation._helpers.citations import RULES_FOR_TIMING, enforce_citations
    enforce_citations(result, RULES_FOR_TIMING)
    return result

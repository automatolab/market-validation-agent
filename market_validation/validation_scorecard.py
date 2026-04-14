"""
Validation Scorecard — synthesizes market sizing, demand, competition,
and signal data into a go/no-go verdict.

Scoring is deterministic (no AI needed for the math).
AI is optionally used to generate a reasoning paragraph.
"""

from __future__ import annotations

from typing import Any, Callable


def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _normalize(value: float, lo: float, hi: float) -> float:
    """Normalize *value* into 0-100 given an expected [lo, hi] range."""
    if hi <= lo:
        return 50.0
    ratio = (value - lo) / (hi - lo)
    return _clamp(ratio * 100)


def score_market_attractiveness(sizing: dict[str, Any], demand: dict[str, Any]) -> float:
    """0-100 composite: bigger market + rising demand + higher growth = higher."""
    # TAM component — midpoint of range, normalized against $10B ceiling
    tam_low = _safe_float(sizing.get("tam_low"))
    tam_high = _safe_float(sizing.get("tam_high"))
    tam_mid = (tam_low + tam_high) / 2 if (tam_low + tam_high) > 0 else 0
    tam_score = _normalize(tam_mid, 0, 10_000_000_000)  # $10B ceiling

    # Growth rate component (from sizing — e.g. 0.08 = 8% CAGR)
    growth_rate = _safe_float(sizing.get("growth_rate"), 0.0)
    # 20%+ CAGR = great (100), 10% = solid (60), 0% = flat (30), negative = bad (0)
    growth_score = _clamp(growth_rate * 300 + 30)  # 0% → 30, 10% → 60, 23%+ → 100

    # Trend component from demand
    trend = demand.get("demand_trend", "stable")
    trend_map = {"rising": 85, "stable": 50, "falling": 15}
    trend_score = trend_map.get(trend, 50)

    # Demand confidence component
    demand_score = _safe_float(demand.get("demand_score"), 50)

    # Seasonality penalty: purely seasonal markets are harder to scale
    seasonality = str(demand.get("demand_seasonality", "none detected")).lower()
    seasonal_penalty = 10.0 if ("seasonal" in seasonality and "none" not in seasonality) else 0.0

    return _clamp(
        0.30 * tam_score
        + 0.15 * growth_score
        + 0.25 * trend_score
        + 0.30 * demand_score
        - seasonal_penalty
    )


def score_competitive(competition: dict[str, Any]) -> float:
    """0-100 where higher = MORE competitive (worse for new entrant)."""
    base = _clamp(_safe_float(competition.get("competitive_intensity"), 50))

    # Funding signals: more funded competitors = harder market
    funding_signals = competition.get("funding_signals") or []
    funding_count = len(funding_signals) if isinstance(funding_signals, list) else 0
    funding_penalty = min(15.0, funding_count * 5.0)  # up to +15 pts

    return _clamp(base + funding_penalty)


def score_demand_validation(demand: dict[str, Any]) -> float:
    """0-100 composite demand score adjusted for willingness-to-pay signal."""
    base = _clamp(_safe_float(demand.get("demand_score"), 50))

    # Willingness-to-pay modifier
    wtp = str(demand.get("willingness_to_pay", "unknown")).lower()
    wtp_map = {"high": 10.0, "medium": 0.0, "low": -10.0, "unknown": 0.0}
    wtp_adj = wtp_map.get(wtp, 0.0)

    return _clamp(base + wtp_adj)


def score_risk(competition: dict[str, Any], signals: dict[str, Any]) -> float:
    """0-100 where higher = MORE risk (worse)."""
    # Regulatory risk
    reg_risks = signals.get("regulatory_risks") or []
    reg_score = min(100, len(reg_risks) * 25) if isinstance(reg_risks, list) else 30

    # Competitive barrier (consolidated markets harder to enter)
    concentration = competition.get("market_concentration", "moderate")
    conc_map = {"fragmented": 15, "moderate": 40, "consolidated": 70, "monopolistic": 95}
    barrier_score = conc_map.get(concentration, 40)

    # Barriers to entry count (explicit barriers listed by AI)
    barriers = competition.get("barriers_to_entry") or []
    barrier_count_penalty = min(20.0, len(barriers) * 7.0) if isinstance(barriers, list) else 0.0

    # Technology risk
    maturity = signals.get("technology_maturity", "growing")
    tech_map = {"emerging": 65, "growing": 30, "mature": 45, "declining": 80}
    tech_score = tech_map.get(maturity, 40)

    # Timing assessment from market signals
    timing = signals.get("timing_assessment", "neutral")
    timing_map = {"good": 15, "neutral": 45, "poor": 80}
    timing_score = timing_map.get(timing, 45)

    # Job posting volume: high hiring = market is growing = lower risk
    job_volume = signals.get("job_posting_volume", "medium")
    job_map = {"high": -10.0, "medium": 0.0, "low": 10.0, "none": 20.0}
    job_adj = job_map.get(job_volume, 0.0)

    raw = (
        0.25 * reg_score
        + 0.25 * (barrier_score + barrier_count_penalty * 0.5)
        + 0.20 * tech_score
        + 0.20 * timing_score
        + 0.10 * 50  # neutral baseline for last 10%
    ) + job_adj

    return _clamp(raw)


def compute_scorecard(
    sizing: dict[str, Any],
    demand: dict[str, Any],
    competition: dict[str, Any],
    signals: dict[str, Any],
    run_ai: Callable[..., dict[str, Any]] | None = None,
    unit_economics: dict[str, Any] | None = None,
    porters: dict[str, Any] | None = None,
    timing: dict[str, Any] | None = None,
    customer_segments: dict[str, Any] | None = None,
    archetype: str | None = None,
) -> dict[str, Any]:
    """
    Compute the full validation scorecard.

    Returns a dict with individual scores, overall score, verdict,
    and optionally AI-generated reasoning.
    """
    # Load archetype config — fall back to generic weights if unavailable
    try:
        from market_validation.market_archetype import get_archetype_config
        archetype_key = archetype or sizing.get("archetype") or competition.get("archetype") or "b2b-industrial"
        arch = get_archetype_config(archetype_key)
    except Exception:
        archetype_key = archetype or sizing.get("archetype") or competition.get("archetype") or "b2b-industrial"
        arch = {
            "label": "General",
            "scoring_weights": {
                "attractiveness": 0.30,
                "demand": 0.25,
                "competitive": 0.25,
                "risk": 0.20,
            },
            "key_success_factors": [],
            "red_flags": [],
        }

    weights = arch["scoring_weights"]

    attractiveness = score_market_attractiveness(sizing, demand)
    competitive = score_competitive(competition)
    demand_val = score_demand_validation(demand)
    risk = score_risk(competition, signals)

    # Weighted overall: attractiveness & demand are positive, competition & risk are inverted
    overall = _clamp(
        weights["attractiveness"] * attractiveness
        + weights["demand"] * demand_val
        + weights["competitive"] * (100 - competitive)
        + weights["risk"] * (100 - risk)
    )

    # Blend unit economics score if available (max ±10 points)
    ue_score = _safe_float((unit_economics or {}).get("unit_economics_score"), -1)
    if ue_score >= 0:
        ue_adj = (ue_score - 50) / 5  # ±10 pts swing
        overall = _clamp(overall + ue_adj)

    # Blend structural attractiveness from Porter's (max ±8 points)
    sa_score = _safe_float((porters or {}).get("structural_attractiveness"), -1)
    if sa_score >= 0:
        sa_adj = (sa_score - 50) / 6.25  # ±8 pts swing
        overall = _clamp(overall + sa_adj)

    # Blend timing score if available (max ±7 points)
    t_score = _safe_float((timing or {}).get("timing_score"), -1)
    if t_score >= 0:
        t_adj = (t_score - 50) / 7  # ±7 pts swing
        overall = _clamp(overall + t_adj)

    # Blend ICP clarity from customer segments (max ±5 points)
    icp_score = _safe_float((customer_segments or {}).get("icp_clarity"), -1)
    if icp_score >= 0:
        icp_adj = (icp_score - 50) / 10  # ±5 pts swing
        overall = _clamp(overall + icp_adj)

    if overall >= 75:
        verdict = "strong_go"
    elif overall >= 55:
        verdict = "go"
    elif overall >= 35:
        verdict = "cautious"
    else:
        verdict = "no_go"

    result: dict[str, Any] = {
        "market_attractiveness": round(attractiveness, 1),
        "competitive_score": round(competitive, 1),
        "demand_validation": round(demand_val, 1),
        "risk_score": round(risk, 1),
        "overall_score": round(overall, 1),
        "verdict": verdict,
    }

    result["archetype"] = archetype_key
    result["archetype_label"] = arch["label"]
    result["key_success_factors"] = arch.get("key_success_factors", [])
    result["archetype_red_flags"] = arch.get("red_flags", [])
    if unit_economics and unit_economics.get("unit_economics_score") is not None:
        result["unit_economics_score"] = round(_safe_float(unit_economics.get("unit_economics_score")), 1)
    if porters and porters.get("structural_attractiveness") is not None:
        result["structural_attractiveness"] = round(_safe_float(porters.get("structural_attractiveness")), 1)
    if timing and timing.get("timing_score") is not None:
        result["timing_score"] = round(_safe_float(timing.get("timing_score")), 1)
    if customer_segments and customer_segments.get("icp_clarity") is not None:
        result["icp_clarity"] = round(_safe_float(customer_segments.get("icp_clarity")), 1)

    # AI reasoning — always generate when run_ai is available
    if run_ai:
        tam_low = sizing.get("tam_low") or 0
        tam_high = sizing.get("tam_high") or 0
        tam_str = f"${tam_low:,.0f} - ${tam_high:,.0f}" if tam_high > 0 else "unknown"

        growth_rate = sizing.get("growth_rate")
        growth_str = f"{growth_rate:.0%}" if growth_rate is not None else "unknown"

        pain_points = demand.get("demand_pain_points") or []
        barriers = competition.get("barriers_to_entry") or []
        reg_risks = signals.get("regulatory_risks") or []
        funding_signals = competition.get("funding_signals") or []
        key_trends = signals.get("key_trends") or []
        wtp = demand.get("willingness_to_pay", "unknown")
        seasonality = demand.get("demand_seasonality", "none detected")
        timing_assessment = signals.get("timing_assessment", "neutral")
        job_volume = signals.get("job_posting_volume", "unknown")
        direct_competitors = competition.get("direct_competitors") or []

        # Build archetype-aware extra context safely
        try:
            ue = unit_economics or {}
            po = porters or {}
            ti = timing or {}
            cs = customer_segments or {}

            gm_low = ue.get("gross_margin_low")
            gm_high = ue.get("gross_margin_high")
            if gm_low is not None and gm_high is not None:
                gm_str = f"{float(gm_low):.0%} - {float(gm_high):.0%}"
            else:
                gm_str = "unknown"

            primary_seg = cs.get("primary_segment") or {}

            extra_facts = f"""- Market archetype: {arch['label']}
- Unit economics score: {ue.get('unit_economics_score', 'unknown')}/100
- Gross margin range: {gm_str}
- Structural attractiveness (Porter's): {po.get('structural_attractiveness', 'unknown')}/100
- Dominant competitive force: {po.get('dominant_force', 'unknown')}
- Timing verdict: {ti.get('timing_verdict', 'unknown')} ({ti.get('timing_score', '?')}/100)
- Primary customer segment: {primary_seg.get('name', 'unknown')}
- ICP clarity: {cs.get('icp_clarity', 'unknown')}/100
- Key success factors for {arch['label']}: {', '.join(arch['key_success_factors'][:3])}
- Red flags for this archetype: {', '.join(arch['red_flags'][:2])}"""
        except Exception:
            extra_facts = f"- Market archetype: {arch['label']}"

        prompt = f"""You are a market analyst. Write a go/no-go recommendation for entering this market.

Scores:
- Market Attractiveness: {attractiveness:.0f}/100 (TAM size + growth + trend)
- Demand Validation: {demand_val:.0f}/100 (search trends + WTP + community)
- Competitive Intensity: {competitive:.0f}/100 (higher = harder market)
- Risk Score: {risk:.0f}/100 (higher = riskier)
- Overall: {overall:.0f}/100 → Verdict: {verdict.upper()}

Key facts:
- TAM range: {tam_str}
- Annual growth rate: {growth_str}
- Demand trend: {demand.get("demand_trend", "unknown")}
- Demand seasonality: {seasonality}
- Willingness to pay: {wtp}
- Market concentration: {competition.get("market_concentration", "unknown")}
- Competitor count: {competition.get("competitor_count", "unknown")}
- Direct competitors: {", ".join(direct_competitors[:3]) or "none identified"}
- Technology maturity: {signals.get("technology_maturity", "unknown")}
- Job posting volume: {job_volume}
- Timing assessment: {timing_assessment}
- Key market trends: {", ".join(key_trends[:2]) or "none identified"}
- Customer pain points: {", ".join(pain_points[:3]) or "none identified"}
- Barriers to entry: {", ".join(barriers[:2]) or "none identified"}
- Regulatory risks: {", ".join(reg_risks[:2]) or "none identified"}
- Funding signals: {", ".join(funding_signals[:2]) or "none identified"}
{extra_facts}

Return ONLY this JSON (no markdown fences):
{{
  "reasoning": "3-4 sentence paragraph explaining the verdict and the top 1-2 reasons to proceed or avoid. Be specific and data-driven.",
  "next_steps": [
    "Concrete action 1 (specific to this market and verdict, e.g. 'Interview 10 potential customers about X pain point before committing capital')",
    "Concrete action 2",
    "Concrete action 3"
  ],
  "key_risks": [
    "Most important risk specific to this market (not generic)",
    "Second most important risk"
  ]
}}

Rules:
- next_steps should be the 3 most important things to validate or do NEXT given this verdict
- For strong_go/go: next steps are about moving fast and validating assumptions
- For cautious: next steps are about de-risking before committing
- For no_go: next steps are about why to walk away or what would need to change
- key_risks must be specific to THIS market (e.g. "USDA inspection required adds 3-6 month delay"), not generic platitudes
- All items must be actionable sentences, not vague phrases"""

        ai_result = run_ai(prompt)
        reasoning = ""
        next_steps: list[str] = []
        key_risks: list[str] = []
        if isinstance(ai_result, dict):
            reasoning = ai_result.get("reasoning") or ai_result.get("text", "")
            next_steps = ai_result.get("next_steps") or []
            key_risks = ai_result.get("key_risks") or []
            if not reasoning and ai_result.get("result") != "error":
                reasoning = str(ai_result)
        elif isinstance(ai_result, str):
            reasoning = ai_result
        result["verdict_reasoning"] = reasoning.strip().strip('"').strip("'")
        if next_steps:
            result["next_steps"] = [s for s in next_steps if isinstance(s, str)]
        if key_risks:
            result["key_risks"] = [r for r in key_risks if isinstance(r, str)]

    return result

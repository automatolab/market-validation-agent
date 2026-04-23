"""ValidationService — market opportunity assessment (Step 0 of pipeline).

Runs 8 sub-modules in parallel (sizing, demand, competition, signals,
unit economics, Porter's 5 forces, timing, customer segments), composes a
scorecard, and persists results to the ``market_validations`` table.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from market_validation._helpers.validation_helpers import print_validation_summary
from market_validation.log import get_logger

_log = get_logger("validation_service")

RunAI = Callable[..., dict[str, Any]]


class ValidationService:
    """Runs market-opportunity validation for a (market, geography, product) triple."""

    def __init__(self, run_ai: RunAI, root: Path, research_id: str | None):
        self.run_ai = run_ai
        self.root = root
        self.research_id = research_id

    def run(
        self,
        market: str,
        geography: str,
        product: str | None = None,
        archetype: str | None = None,
    ) -> dict[str, Any]:
        """Run all validation sub-modules and return a combined result dict."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from market_validation.competitive_landscape import analyze_competition
        from market_validation.customer_segments import identify_customer_segments
        from market_validation.demand_analysis import analyze_demand
        from market_validation.market_archetype import detect_archetype
        from market_validation.market_signals import gather_market_signals
        from market_validation.market_sizing import estimate_market_size
        from market_validation.porters_five_forces import analyze_porters_five_forces
        from market_validation.research import create_validation, update_validation
        from market_validation.timing_analysis import analyze_timing
        from market_validation.unit_economics import estimate_unit_economics
        from market_validation.validation_scorecard import compute_scorecard

        print(f"[validate] Starting market validation: {product or market} in {geography}")

        # Detect archetype first (synchronous) — caller may override
        if archetype:
            archetype_key = archetype
            archetype_confidence = 100
        else:
            archetype_key, archetype_confidence = detect_archetype(market, product)
        print(f"[validate]   Archetype: {archetype_key} (confidence {archetype_confidence}%)")

        # Create validation record
        val = create_validation(
            research_id=self.research_id,
            market=market,
            geography=geography,
            root=self.root,
        )
        vid = val["validation_id"]
        update_validation(vid, {"status": "running"}, root=self.root)

        run_ai = self.run_ai

        _defaults: dict[str, Any] = {
            "sizing": {},
            "demand": {"demand_score": 50, "demand_trend": "stable"},
            "competition": {"competitive_intensity": 50, "market_concentration": "moderate"},
            "signals": {"regulatory_risks": [], "technology_maturity": "growing"},
            "unit_economics": {},
            "porters": {},
            "timing": {},
            "customer_segments": {},
        }
        _tasks = {
            "sizing": (estimate_market_size, (market, geography, product), {"run_ai": run_ai}),
            "demand": (analyze_demand, (market, geography, product), {"run_ai": run_ai, "archetype": archetype_key}),
            "competition": (analyze_competition, (market, geography, product), {"run_ai": run_ai}),
            "signals": (gather_market_signals, (market, geography, product), {"run_ai": run_ai}),
            "unit_economics": (estimate_unit_economics, (market, geography, product), {"archetype": archetype_key, "run_ai": run_ai}),
            "porters": (analyze_porters_five_forces, (market, geography, product), {"run_ai": run_ai}),
            "timing": (analyze_timing, (market, geography, product), {"archetype": archetype_key, "run_ai": run_ai}),
            "customer_segments": (identify_customer_segments, (market, geography, product), {"archetype": archetype_key, "run_ai": run_ai}),
        }
        _labels = {
            "sizing": "Estimating market size (TAM/SAM/SOM)",
            "demand": "Analyzing demand signals",
            "competition": "Mapping competitive landscape",
            "signals": "Gathering market signals",
            "unit_economics": "Estimating unit economics",
            "porters": "Analyzing Porter's 5 forces",
            "timing": "Assessing market timing",
            "customer_segments": "Identifying customer segments",
        }
        results_map: dict[str, Any] = {}
        print("[validate]   Running 8 modules in parallel...")
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(fn, *args, **kwargs): key
                for key, (fn, args, kwargs) in _tasks.items()
            }
            for future in as_completed(futures):
                key = futures[future]
                try:
                    results_map[key] = future.result()
                    print(f"[validate]   ✓ {_labels[key]}")
                except Exception as e:
                    print(f"[validate]   ! {_labels[key]} failed: {e}")
                    results_map[key] = _defaults[key]

        sizing = results_map["sizing"]
        demand = results_map["demand"]
        competition = results_map["competition"]
        signals = results_map["signals"]
        unit_economics = results_map["unit_economics"]
        porters = results_map["porters"]
        timing = results_map["timing"]
        customer_segments = results_map["customer_segments"]

        # Re-run porters with competition context if it completed without it
        if porters and not porters.get("structural_attractiveness") and competition:
            try:
                porters = analyze_porters_five_forces(
                    market, geography, product,
                    existing_competition=competition,
                    run_ai=run_ai,
                )
            except Exception as exc:
                # Keep the first-pass result; log so we can trace why the
                # context-aware re-run didn't help.
                _log.warning("porters re-run with competition context failed: %s", exc)

        # Re-run timing with signals context if it completed without a score
        if timing and not timing.get("timing_score") and signals:
            try:
                timing = analyze_timing(
                    market, geography, product,
                    archetype=archetype_key,
                    signals=signals,
                    run_ai=run_ai,
                )
            except Exception as exc:
                _log.warning("timing re-run with signals context failed: %s", exc)

        print("[validate]   Computing scorecard...")
        scorecard = compute_scorecard(
            sizing, demand, competition, signals,
            run_ai=run_ai,
            unit_economics=unit_economics,
            porters=porters,
            timing=timing,
            customer_segments=customer_segments,
            archetype=archetype_key,
        )

        def _log_module(name: str, result: dict) -> None:
            non_none = {k: v for k, v in result.items() if v is not None and v != [] and v != {}}
            none_keys = [k for k, v in result.items() if v is None]
            print(
                f"[validate]   {name}: {len(non_none)} fields populated"
                + (f", {len(none_keys)} None ({none_keys})" if none_keys else "")
            )
        _log_module("sizing", sizing)
        _log_module("demand", demand)
        _log_module("competition", competition)
        _log_module("signals", signals)
        _log_module("unit_economics", unit_economics)
        _log_module("porters", porters)
        _log_module("timing", timing)
        _log_module("customer_segments", customer_segments)

        db_fields: dict[str, Any] = {"status": "complete"}
        for key in ("tam_low", "tam_high", "tam_confidence", "tam_sources",
                     "sam_low", "sam_high", "sam_confidence", "sam_sources",
                     "som_low", "som_high", "som_confidence", "som_sources"):
            if key in sizing and sizing[key] is not None:
                db_fields[key] = sizing[key]
        for key in ("demand_score", "demand_trend", "demand_seasonality",
                     "demand_pain_points", "demand_sources"):
            if key in demand and demand[key] is not None:
                db_fields[key] = demand[key]
        for key in ("competitive_intensity", "competitor_count", "market_concentration",
                     "direct_competitors", "indirect_competitors", "funding_signals",
                     "differentiation_opportunities"):
            if key in competition and competition[key] is not None:
                db_fields[key] = competition[key]
        for key in ("job_posting_volume", "news_sentiment", "regulatory_risks",
                     "technology_maturity", "signals_data"):
            if key in signals and signals[key] is not None:
                db_fields[key] = signals[key]
        db_fields.update({
            "market_attractiveness": scorecard.get("market_attractiveness"),
            "competitive_score": scorecard.get("competitive_score"),
            "demand_validation": scorecard.get("demand_validation"),
            "risk_score": scorecard.get("risk_score"),
            "overall_score": scorecard.get("overall_score"),
            "verdict": scorecard.get("verdict"),
            "verdict_reasoning": scorecard.get("verdict_reasoning"),
        })

        db_fields["archetype"] = archetype_key
        db_fields["archetype_confidence"] = archetype_confidence
        db_fields["archetype_label"] = scorecard.get("archetype_label", "")

        for key in ("gross_margin_low", "gross_margin_high", "gross_margin_confidence",
                    "cac_estimate_low", "cac_estimate_high", "ltv_estimate_low",
                    "ltv_estimate_high", "payback_months", "unit_economics_score"):
            if key in unit_economics and unit_economics[key] is not None:
                db_fields[key] = unit_economics[key]
        if unit_economics:
            db_fields["unit_economics_data"] = unit_economics

        for key in ("supplier_power", "buyer_power", "substitute_threat",
                    "entry_barrier_score", "rivalry_score", "structural_attractiveness"):
            if key in porters and porters[key] is not None:
                db_fields[key] = porters[key]
        if porters:
            db_fields["porters_data"] = porters

        for key in ("timing_score", "timing_verdict"):
            if key in timing and timing[key] is not None:
                db_fields[key] = timing[key]
        if timing.get("enablers"):
            db_fields["timing_enablers"] = timing["enablers"]
        if timing.get("headwinds"):
            db_fields["timing_headwinds"] = timing["headwinds"]

        if customer_segments:
            db_fields["customer_segments_data"] = customer_segments
            if customer_segments.get("icp_clarity") is not None:
                db_fields["icp_clarity"] = customer_segments["icp_clarity"]
            if customer_segments.get("primary_segment"):
                seg = customer_segments["primary_segment"]
                db_fields["primary_segment"] = seg.get("name", "") if isinstance(seg, dict) else str(seg)

        if scorecard.get("next_steps"):
            db_fields["next_steps"] = scorecard["next_steps"]
        if scorecard.get("key_risks"):
            db_fields["key_risks"] = scorecard["key_risks"]
        if scorecard.get("key_success_factors"):
            db_fields["key_success_factors"] = scorecard["key_success_factors"]
        if scorecard.get("archetype_red_flags"):
            db_fields["archetype_red_flags"] = scorecard["archetype_red_flags"]

        update_validation(vid, db_fields, root=self.root)

        print_validation_summary(market, geography, archetype_key, scorecard, sizing, competition)

        return {
            "result": "ok",
            "validation_id": vid,
            "archetype": archetype_key,
            "sizing": sizing,
            "demand": demand,
            "competition": competition,
            "signals": signals,
            "unit_economics": unit_economics,
            "porters": porters,
            "timing": timing,
            "customer_segments": customer_segments,
            "scorecard": scorecard,
        }

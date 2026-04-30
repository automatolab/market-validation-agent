"""QualificationService — AI-scored lead ranking (Step 2 of pipeline).

For every ``new`` company in the research, asks the AI to score relevance,
detect growth signals, estimate volume, and assign a priority tier. Falls
back to a keyword-match heuristic when the AI is unavailable.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from market_validation._helpers.archetypes import archetype_qualify_context
from market_validation._helpers.common import to_float
from market_validation._helpers.qualification_helpers import (
    clamp_score,
    heuristic_qualification,
    normalize_priority,
    normalize_qualification_status,
)
from market_validation.log import get_logger
from market_validation.schemas import QualificationResult

_log = get_logger("qualification_service")

RunAI = Callable[..., dict[str, Any]]
DetectAgent = Callable[[], str]


class QualificationService:
    """Score companies in a research project against the target profile."""

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

    def run(self) -> dict[str, Any]:
        if not self.research_id:
            return {"result": "error", "error": "No research_id set"}

        from market_validation.market_archetype import detect_archetype
        from market_validation.research import (
            _connect,
            _ensure_schema,
            get_research,
            resolve_db_path,
            update_company,
        )

        db = resolve_db_path(self.root)

        research = get_research(self.research_id, root=self.root)
        if research.get("result") != "ok":
            return {"result": "error", "error": "Research not found"}
        research_market = str(research.get("research", {}).get("market") or "")
        research_product = research.get("research", {}).get("product")

        _qual_archetype_key, _qual_archetype_conf = detect_archetype(research_market, research_product)
        _qual_arch_ctx = archetype_qualify_context(_qual_archetype_key, research_market, research_product)
        print(f"[qualify] archetype={_qual_archetype_key} (confidence={_qual_archetype_conf}%)")

        market_context = self._build_market_context()

        with _connect(db) as conn:
            _ensure_schema(conn)
            conn.row_factory = None
            companies = conn.execute(
                """SELECT id, company_name, notes, phone, website, location
                   FROM companies WHERE research_id = ? AND status = 'new'""",
                (self.research_id,),
            ).fetchall()

        if not companies:
            return {"result": "ok", "qualified": 0, "message": "No companies to qualify"}

        company_list = [
            {"id": str(c[0]), "name": str(c[1]), "notes": c[2], "phone": c[3], "website": c[4], "location": c[5]}
            for c in companies
        ]

        BATCH_SIZE = 8
        all_results: list[dict] = []
        for i in range(0, len(company_list), BATCH_SIZE):
            batch = company_list[i:i + BATCH_SIZE]
            batch_results = self._qualify_batch(batch, _qual_arch_ctx, market_context)
            if batch_results:
                all_results.extend(batch_results)
            else:
                # Heuristic fallback for this batch only
                batch_companies = companies[i:i + BATCH_SIZE]
                all_results.extend(
                    heuristic_qualification(batch_companies, market=research_market, product=research_product)
                )

        method = self.detect_agent() if all_results and not all(
            "Heuristic" in str(r.get("notes", "")) for r in all_results
        ) else "heuristic"

        qualified = 0
        skipped = 0
        for r in all_results:
            parsed = self._parse_qualification(r)
            if parsed is None:
                skipped += 1
                continue

            combined_notes = self._compose_notes(r, parsed.notes)

            fields = {
                "status": parsed.status,
                "priority_score": parsed.score,
                "priority_tier": parsed.priority,
                "volume_estimate": parsed.volume_estimate,
                "volume_unit": parsed.volume_unit,
                "notes": combined_notes,
            }
            update_company(parsed.company_id, self.research_id, fields, root=self.root)
            if parsed.status == "qualified":
                qualified += 1

        return {
            "result": "ok",
            "qualified": qualified,
            "assessed": len(companies),
            "skipped": skipped,
            "method": method,
        }

    # ── Internal helpers ──────────────────────────────────────────────────

    def _parse_qualification(self, raw: dict[str, Any]) -> QualificationResult | None:
        """Validate one AI batch result into a ``QualificationResult``.

        Runs the legacy normalizers first (tolerant of AI drift) and *then*
        hands the cleaned fields to Pydantic as a typed-contract guardrail.
        Returns ``None`` if the item can't be made valid (missing company_id,
        unparseable score, etc.), logging the reason.
        """
        cid = raw.get("company_id")
        if not cid:
            _log.warning("[qualify] skipping result with no company_id: %r", raw)
            return None

        score = clamp_score(raw.get("score"))
        status = normalize_qualification_status(raw.get("status", "new"))
        priority = normalize_priority(raw.get("priority"), score)

        try:
            return QualificationResult(
                company_id=str(cid),
                status=status,
                score=score,
                priority=priority,
                volume_estimate=to_float(raw.get("volume_estimate")),
                volume_unit=raw.get("volume_unit") or None,
                notes=str(raw["notes"]) if raw.get("notes") else None,
            )
        except ValidationError as exc:
            _log.warning("[qualify] skipping %s — invalid result shape: %s", cid, exc.errors())
            return None

    @staticmethod
    def _compose_notes(raw: dict[str, Any], primary_notes: str | None) -> str | None:
        """Merge AI notes with market_signals + pain_points into a single field."""
        parts: list[str] = []
        if primary_notes:
            parts.append(primary_notes)
        if raw.get("market_signals"):
            sigs = raw["market_signals"] if isinstance(raw["market_signals"], list) else [raw["market_signals"]]
            parts.append("Signals: " + "; ".join(str(s) for s in sigs))
        if raw.get("pain_points"):
            pains = raw["pain_points"] if isinstance(raw["pain_points"], list) else [raw["pain_points"]]
            parts.append("Pain points: " + "; ".join(str(p) for p in pains))
        return " | ".join(parts) if parts else None

    def _build_market_context(self) -> str:
        """Pull market-validation context from DB to sharpen qualification scoring."""
        def _pain_point_summary(items: list) -> str:
            """Render pain_points (which may be dicts post-citation-rollout) as a string."""
            try:
                from market_validation.validation_scorecard import _flatten_strings
                return _flatten_strings(items)
            except Exception:
                return ", ".join(str(x) for x in (items or []))
        try:
            from market_validation.research import get_validation_by_research
            val_result = get_validation_by_research(self.research_id, root=self.root)
            if val_result.get("result") != "ok" or not val_result.get("validation"):
                return ""
            v = val_result["validation"]
            verdict = v.get("verdict", "unknown")
            overall = v.get("overall_score", 0)
            demand_trend = v.get("demand_trend", "unknown")
            pain_points = v.get("demand_pain_points") or []
            competitive_intensity = v.get("competitive_intensity", 50)
            wtp = v.get("willingness_to_pay", "unknown")
            return f"""
Market Validation Context (pre-computed):
- Market verdict: {verdict} (overall score: {overall}/100)
- Demand trend: {demand_trend}
- Competitive intensity: {competitive_intensity}/100
- Willingness to pay: {wtp}
- Identified customer pain points: {_pain_point_summary(pain_points[:3]) if pain_points else "none identified"}

Use this context to calibrate scores — companies in a {verdict.replace("_", " ")} market should reflect that reality.
"""
        except Exception as exc:
            # Market context is an optional quality enhancer — never let its
            # absence abort qualification. Log so a broken validation row
            # still shows up when diagnosing flat scores.
            _log.debug("qualify: market context unavailable for %s: %s", self.research_id, exc)
            return ""

    def _qualify_batch(
        self,
        batch: list[dict],
        archetype_context: str,
        market_context: str,
    ) -> list[dict]:
        prompt = f"""{archetype_context}
{market_context}
For each company, assess:
1. Relevance score (0-100): how well do they match the target market?
2. Market potential signals - look for:
   - Growth indicators: expanding, hiring, new locations, investment/funding
   - Pain points: do they have a problem your product could solve?
   - Buying signals: are they spending in this category? Active customers?
   - Urgency: seasonal demand, recent news suggesting immediate need
3. Volume estimate: approximate revenue/size/usage with unit (e.g., "$500K/year", "800/week", "1000/monthly customers", "small/medium/large").
   You MUST briefly justify the estimate (team size, location count, observed transactions, reviews) in `volume_basis`.
4. Priority tier: high (strong signals), medium (some signals), low (weak signals)
5. Status: qualified (clear fit), uncertain (maybe), not_relevant (no fit)

Companies:
{json.dumps(batch, indent=2)}

Return JSON:
{{
  "results": [
    {{
      "company_id": "id from list",
      "status": "qualified|uncertain|not_relevant",
      "score": 0-100,
      "score_evidence": "1 sentence citing the specific data point that supports the score",
      "priority": "high|medium|low",
      "volume_estimate": "numeric value or null",
      "volume_unit": "unit like $/year, /week, /month, customers, or small/medium/large",
      "volume_basis": "1 sentence on what observable signal led to this estimate (e.g. '3 visible locations × ~150 covers/day from Yelp reviews')",
      "market_signals": ["concrete signal with evidence — e.g. 'Hiring 5 sales reps (snippet from notes)'"],
      "pain_points": ["specific problems that make them a good prospect"],
      "notes": "concise assessment with key reasons"
    }}
  ]
}}

Grounding rules — do NOT invent claims:
- Only cite signals that appear in the company's notes / website / location fields above.
- If a company has empty notes, set status='uncertain', score≤45, and note 'no notes available'.
- volume_basis must reference an observable cue — never just a number with no justification."""
        r = self.run_ai(prompt, timeout=200)
        return r.get("results") if isinstance(r, dict) and r.get("results") else []

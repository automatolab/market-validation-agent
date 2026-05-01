"""
Market Validation Agent — orchestrator for the full research pipeline.

The ``Agent`` class is a thin facade that wires four services together:
validation, search, qualification, and enrichment. Each service owns its
own domain logic (see ``market_validation.services``); Agent owns the AI
runner (``_run``) and the research-level ``research()`` method that runs
the full pipeline end-to-end.

Private helpers have been moved to ``market_validation._helpers``; a few
are re-exported here for backward compatibility with existing tests.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

# ── Backward-compat re-exports for tests (tests/test_core.py) ──────────────
# Tests import ``_clean_company_name``, ``_is_junk_company``,
# ``_extract_phone_text``, ``_extract_email_text`` from this module. New code
# should import from ``market_validation._helpers`` directly.
from market_validation._helpers.companies import (
    clean_company_name as _clean_company_name,
)
from market_validation._helpers.companies import (
    is_junk_company as _is_junk_company,
)
from market_validation._helpers.contacts import (
    extract_email_text as _extract_email_text,
)
from market_validation._helpers.contacts import (
    extract_phone_text as _extract_phone_text,
)
from market_validation.log import get_logger
from market_validation.services import (
    EnrichmentService,
    QualificationService,
    SearchService,
    ValidationService,
)

_log = get_logger("agent")


class Agent:
    """
    Market Validation Pipeline Agent.

    Pipeline: validate() -> find() -> qualify() -> enrich()/enrich_all().
    AI calls dispatched via claude or opencode CLI on PATH.
    """

    def __init__(self, research_id: str | None = None, root: str | Path = "."):
        self.research_id = research_id
        self.root = Path(root).resolve()

        # When run from inside the package dir (e.g. a script that sets cwd to
        # market_validation/), ensure the DB lives at the project root.
        from market_validation.research import PROJECT_ROOT
        if str(self.root).endswith("market_validation"):
            self.root = PROJECT_ROOT

        self.last_result: dict[str, Any] = {}

        # Lazily-constructed services — built the first time each is used so
        # that changing self.research_id between calls is always honored.
        self._validation_service: ValidationService | None = None
        self._search_service: SearchService | None = None
        self._qualification_service: QualificationService | None = None
        self._enrichment_service: EnrichmentService | None = None

    # ── Service factories ────────────────────────────────────────────────
    # Each service is rebuilt whenever research_id changes so state stays
    # consistent with the current project context.

    def _get_validation_service(self) -> ValidationService:
        if self._validation_service is None or self._validation_service.research_id != self.research_id:
            self._validation_service = ValidationService(self._run, self.root, self.research_id)
        return self._validation_service

    def _get_search_service(self) -> SearchService:
        if self._search_service is None or self._search_service.research_id != self.research_id:
            self._search_service = SearchService(self._run, self._detect_agent, self.root, self.research_id)
        return self._search_service

    def _get_qualification_service(self) -> QualificationService:
        if self._qualification_service is None or self._qualification_service.research_id != self.research_id:
            self._qualification_service = QualificationService(self._run, self._detect_agent, self.root, self.research_id)
        return self._qualification_service

    def _get_enrichment_service(self) -> EnrichmentService:
        if self._enrichment_service is None or self._enrichment_service.research_id != self.research_id:
            self._enrichment_service = EnrichmentService(self._run, self.root, self.research_id)
        return self._enrichment_service

    # ── AI runner (claude / opencode CLI dispatch) ───────────────────────

    @staticmethod
    def _detect_agent() -> str:
        """Pick the best AI agent available on PATH: claude → opencode → none."""
        import shutil
        if shutil.which("claude"):
            return "claude"
        if shutil.which("opencode"):
            return "opencode"
        return "none"

    @staticmethod
    def _parse_json_from_text(text: str) -> dict[str, Any] | list[Any] | None:
        """Extract the first valid JSON object or array from arbitrary text.

        Returns a dict for object responses, a list for array responses, or
        None when nothing parses. Callers should ``isinstance``-check before
        treating the result as either shape — every existing caller already
        does, so the changed return type is compatible.
        """
        if "```json" in text:
            text = text.split("```json", 1)[1].split("```", 1)[0]
        elif "```" in text:
            text = text.split("```", 1)[1].split("```", 1)[0]
        text = text.strip()

        start = text.find("{")
        arr_start = text.find("[")
        if arr_start >= 0 and (start < 0 or arr_start < start):
            start = arr_start

        if start < 0:
            return None

        if text[start] == "{":
            end = text.rfind("}")
            if end > start:
                try:
                    return json.loads(text[start : end + 1])
                except json.JSONDecodeError:
                    pass
        elif text[start] == "[":
            end = text.rfind("]")
            if end > start:
                try:
                    # Return the array directly. Older code wrapped it as
                    # {"companies": [...]} which corrupted non-find callers
                    # (qualification, ai_validate_companies, enrichment) that
                    # expected the array shape verbatim.
                    return json.loads(text[start : end + 1])
                except json.JSONDecodeError:
                    pass
        return None

    def _run_claude(self, prompt: str, timeout: int = 180) -> dict[str, Any] | list[Any]:
        """Run via Claude Code CLI (`claude -p`)."""
        try:
            result = subprocess.run(
                ["claude", "-p", prompt, "--output-format", "text"],
                capture_output=True, text=True, timeout=timeout,
                cwd=str(self.root),
            )
        except subprocess.TimeoutExpired:
            return {"result": "error", "error": "Timeout (claude)"}
        if result.returncode != 0:
            return {"result": "error", "error": result.stderr or "claude failed"}
        parsed = self._parse_json_from_text(result.stdout.strip())
        if parsed is None:
            return {"result": "error", "error": "No JSON (claude)"}
        return parsed

    def _run_opencode(self, prompt: str, timeout: int = 180) -> dict[str, Any] | list[Any]:
        """Run via opencode CLI."""
        try:
            result = subprocess.run(
                ["opencode", "run", "--dangerously-skip-permissions", "--dir", str(self.root), prompt],
                capture_output=True, text=True, timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            return {"result": "error", "error": "Timeout (opencode)"}
        if result.returncode != 0:
            return {"result": "error", "error": result.stderr or "opencode failed"}
        parsed = self._parse_json_from_text(result.stdout.strip())
        if parsed is None:
            return {"result": "error", "error": "No JSON (opencode)"}
        return parsed

    def _run(self, prompt: str, timeout: int = 180) -> dict[str, Any] | list[Any]:
        """
        Run a prompt via the best available AI agent.

        Tries: claude (Claude Code CLI) → opencode → error.
        Both CLIs can browse the web, so either can handle research queries.
        """
        agent = self._detect_agent()

        if agent == "claude":
            result = self._run_claude(prompt, timeout=timeout)
            if result.get("result") != "error":
                return result
            # claude failed — try opencode as fallback
            import shutil
            if shutil.which("opencode"):
                return self._run_opencode(prompt, timeout=timeout)
            return result

        if agent == "opencode":
            return self._run_opencode(prompt, timeout=timeout)

        return {"result": "error", "error": "No AI agent available (install claude or opencode)"}

    # ── Public API — delegates to services ───────────────────────────────

    def validate(
        self,
        market: str,
        geography: str,
        product: str | None = None,
        archetype: str | None = None,
        skip_stages: list[str] | None = None,
        from_stage: str | None = None,
    ) -> dict[str, Any]:
        """STEP 0: Validate the market before company discovery.

        skip_stages: stage names to skip (load from DB / defaults).
        from_stage:  skip everything before this stage (resume mid-pipeline).
        """
        return self._get_validation_service().run(
            market, geography, product, archetype,
            skip_stages=skip_stages, from_stage=from_stage,
        )

    def find(self, market: str, geography: str, product: str | None = None) -> dict[str, Any]:
        """STEP 1: Find companies in a market via multi-backend + AI search."""
        result = self._get_search_service().run(market, geography, product)
        self.last_result = result
        return result

    def qualify(self) -> dict[str, Any]:
        """STEP 2: Qualify companies — AI assessment of relevance and volume."""
        result = self._get_qualification_service().run()
        self.last_result = result
        return result

    def enrich(self, company_name: str, location: str | None = None) -> dict[str, Any]:
        """STEP 3: Enrich a single company with contact info (3-tier cascade)."""
        return self._get_enrichment_service().enrich_one(company_name, location)

    def enrich_all(self, statuses: list[str] | None = None) -> dict[str, Any]:
        """Run enrichment on every company matching the given statuses."""
        return self._get_enrichment_service().enrich_all(statuses=statuses)

    def research(
        self,
        market: str,
        geography: str,
        product: str | None = None,
        enrich_statuses: list[str] | None = None,
        validate: bool = False,
        archetype: str | None = None,
        draft_emails: bool = False,
        from_stage: str | None = None,
        resume: bool = False,
    ) -> dict[str, Any]:
        """
        Full pipeline: [validate →] find → qualify → enrich_all [→ draft_emails].

        Args:
            market:          Market category (e.g. "BBQ restaurants", "robotics")
            geography:       Location (e.g. "San Jose, California")
            product:         Specific product/service within the market (optional)
            enrich_statuses: Which company statuses to enrich. Default: ["qualified", "new"]
            validate:        If True, run market validation (Step 0) before find.
            draft_emails:    If True, AI-draft a cold outreach email for every qualified
                             lead with an email on file and queue as pending. Runs in
                             parallel (4 workers) after enrichment, before returning.
            from_stage:      Skip every stage before this one. Useful when an earlier
                             run failed mid-pipeline. Valid: validate|find|qualify|enrich|drafts.
            resume:          When True (and ``from_stage`` is None), auto-resume from
                             the stage AFTER the last completed one (per
                             researches.last_completed_stage).
        """
        from market_validation.research import (
            PIPELINE_STAGES,
            get_last_completed_stage,
            mark_stage_completed,
        )

        if enrich_statuses is None:
            enrich_statuses = ["qualified", "new"]

        # Resolve checkpoint: explicit --from-stage wins; otherwise auto-resume
        # from after the last completed stage; otherwise run everything.
        skip_set: set[str] = set()
        if from_stage:
            if from_stage not in PIPELINE_STAGES:
                raise ValueError(
                    f"unknown from_stage {from_stage!r}; valid: {PIPELINE_STAGES}"
                )
            cutoff = PIPELINE_STAGES.index(from_stage)
            skip_set = set(PIPELINE_STAGES[:cutoff])
            print(f"[research] from_stage={from_stage} → skipping {sorted(skip_set)}")
        elif resume and self.research_id:
            last = get_last_completed_stage(self.research_id, root=self.root)
            if last and last in PIPELINE_STAGES:
                cutoff = PIPELINE_STAGES.index(last) + 1
                skip_set = set(PIPELINE_STAGES[:cutoff])
                print(f"[research] resume from after {last!r} → skipping {sorted(skip_set)}")
            elif last:
                print(f"[research] resume: unrecognized last stage {last!r} — running full pipeline")

        total_steps = 3 + (1 if validate else 0) + (1 if draft_emails else 0)
        step = 0

        def _checkpoint(stage: str) -> None:
            if self.research_id:
                try:
                    mark_stage_completed(self.research_id, stage, root=self.root)
                except Exception as exc:
                    print(f"[research] WARN: checkpoint {stage} failed: {exc}")

        validate_result = None
        if validate and "validate" not in skip_set:
            step += 1
            print(f"[research] Step {step}/{total_steps}: validate — {product or market} in {geography}")
            validate_result = self.validate(market, geography, product, archetype=archetype)
            verdict = validate_result.get("scorecard", {}).get("verdict", "unknown")
            overall = validate_result.get("scorecard", {}).get("overall_score", 0)
            print(f"[research] → verdict: {verdict} ({overall}/100)")
            _checkpoint("validate")
        elif validate:
            print("[research] ↻ skipping validate (resumed past it)")

        find_result: dict[str, Any] = {}
        companies_found = 0
        if "find" not in skip_set:
            step += 1
            print(f"[research] Step {step}/{total_steps}: find — {product or market} in {geography}")
            find_result = self.find(market, geography, product)
            companies_found = len(find_result.get("companies", []))
            print(f"[research] → {companies_found} companies found via {find_result.get('method')}")
            _checkpoint("find")
        else:
            print("[research] ↻ skipping find (resumed past it)")

        qualify_result: dict[str, Any] = {}
        if "qualify" not in skip_set:
            step += 1
            print(f"[research] Step {step}/{total_steps}: qualify")
            qualify_result = self.qualify()
            print(
                f"[research] → {qualify_result.get('qualified')}/{qualify_result.get('assessed')} "
                f"qualified via {qualify_result.get('method')}"
            )
            _checkpoint("qualify")
        else:
            print("[research] ↻ skipping qualify (resumed past it)")

        enrich_result: dict[str, Any] = {}
        if "enrich" not in skip_set:
            step += 1
            print(f"[research] Step {step}/{total_steps}: enrich_all (statuses={enrich_statuses})")
            enrich_result = self.enrich_all(statuses=enrich_statuses)
            print(
                f"[research] → enriched={enrich_result.get('enriched')}/{enrich_result.get('total_companies')}"
                f" | phones={enrich_result.get('phones_found')} emails={enrich_result.get('emails_found')}"
            )
            _checkpoint("enrich")
        else:
            print("[research] ↻ skipping enrich (resumed past it)")

        draft_result = None
        if draft_emails and self.research_id and "drafts" not in skip_set:
            step += 1
            print(f"[research] Step {step}/{total_steps}: draft_emails (qualified leads with email)")
            from market_validation.email_sender import draft_emails_for_research
            draft_result = draft_emails_for_research(
                research_id=self.research_id,
                statuses=["qualified"],
                skip_existing=True,
            )
            print(
                f"[research] → drafts queued={draft_result.get('drafted')} "
                f"skipped={draft_result.get('skipped')} failed={draft_result.get('failed')} "
                f"candidates={draft_result.get('candidates')}"
            )
            _checkpoint("drafts")

        result: dict[str, Any] = {
            "result": "ok",
            "research_id": self.research_id,
            "find": find_result,
            "qualify": qualify_result,
            "enrich": enrich_result,
            "skipped_stages": sorted(skip_set),
            "summary": {
                "companies_found": companies_found,
                "qualified": qualify_result.get("qualified", 0),
                "phones_found": enrich_result.get("phones_found", 0),
                "emails_found": enrich_result.get("emails_found", 0),
            },
        }
        if draft_result:
            result["drafts"] = draft_result
            result["summary"]["drafts_queued"] = draft_result.get("drafted", 0)
        if validate_result:
            result["validate"] = validate_result
            result["summary"]["verdict"] = validate_result.get("scorecard", {}).get("verdict")
            result["summary"]["overall_score"] = validate_result.get("scorecard", {}).get("overall_score")

        try:
            from market_validation.research_export import export_research_folder

            folder = export_research_folder(self.research_id)
            result["summary"]["export_folder"] = str(folder)
        except Exception as exc:
            # Non-fatal: the run succeeded even if the snapshot export fails.
            result["summary"]["export_folder_error"] = str(exc)

        return result


def main() -> None:
    """CLI entry point."""
    import argparse
    parser = argparse.ArgumentParser(description="Market Research Agent")
    parser.add_argument("command", choices=["research", "validate", "find", "qualify", "enrich", "enrich-all"])
    parser.add_argument("--research-id", help="Research ID")
    parser.add_argument("--market", help="Market/product")
    parser.add_argument("--geography", help="Geography")
    parser.add_argument("--product", help="Specific product (optional)")
    parser.add_argument("--company", help="Company name for single enrich")
    parser.add_argument("--validate", action="store_true", help="Run market validation before research pipeline")
    parser.add_argument(
        "--research-from-stage",
        help=(
            "Resume the research() pipeline from this stage. "
            "Valid: validate|find|qualify|enrich|drafts."
        ),
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Auto-resume from the last completed pipeline stage of this research_id.",
    )
    parser.add_argument(
        "--archetype",
        help=(
            "Override archetype detection (e.g. local-service, b2b-saas, b2b-industrial, "
            "consumer-cpg, marketplace, healthcare, services-agency)"
        ),
    )
    parser.add_argument(
        "--skip-stages",
        help=(
            "Comma-separated list of validation stages to skip (e.g. 'sizing,competition'). "
            "Skipped stages load values from the prior DB record when available. "
            "Stages: sizing, demand, competition, signals, unit_economics, porters, "
            "timing, customer_segments."
        ),
    )
    parser.add_argument(
        "--from-stage",
        help=(
            "Resume validation from this stage — every earlier stage is loaded from "
            "the prior DB record. Useful when an earlier run failed mid-way."
        ),
    )

    args = parser.parse_args()
    agent = Agent(research_id=args.research_id)

    if args.command == "research":
        if not args.market or not args.geography:
            parser.error("research requires --market and --geography")
        from market_validation.research import create_research
        rid = create_research(
            name=f"{args.product or args.market} in {args.geography}",
            market=args.market,
            product=args.product,
            geography=args.geography,
        )["research_id"]
        agent.research_id = rid
        result = agent.research(
            args.market, args.geography, args.product,
            validate=args.validate, archetype=args.archetype,
            from_stage=args.research_from_stage,
            resume=args.resume,
        )
    elif args.command == "validate":
        if not args.market or not args.geography:
            parser.error("validate requires --market and --geography")
        if not args.research_id:
            # Look for an existing research with the SAME market AND geography
            # to avoid polluting a different market's research with this validation.
            from market_validation.research import (
                _connect,
                _ensure_schema,
                resolve_db_path,
            )
            from market_validation.research import (
                create_research as _cr,
            )
            _db = resolve_db_path(agent.root)
            with _connect(_db) as _conn:
                _ensure_schema(_conn)
                _existing = _conn.execute(
                    """SELECT r.id FROM researches r
                       WHERE LOWER(TRIM(r.market)) = LOWER(TRIM(?))
                         AND LOWER(TRIM(COALESCE(r.geography,''))) = LOWER(TRIM(?))
                       ORDER BY r.created_at DESC LIMIT 1""",
                    (args.market, args.geography),
                ).fetchone()
            if _existing:
                agent.research_id = _existing[0]
                print(f"[validate] reusing existing research {agent.research_id} ({args.market} / {args.geography})")
            else:
                rid = _cr(
                    name=f"Validation: {args.product or args.market} in {args.geography}",
                    market=args.market,
                    product=args.product,
                    geography=args.geography,
                )["research_id"]
                agent.research_id = rid
        skip = [s.strip() for s in (args.skip_stages or "").split(",") if s.strip()]
        result = agent.validate(
            args.market, args.geography, args.product,
            archetype=args.archetype,
            skip_stages=skip or None,
            from_stage=args.from_stage,
        )
    elif args.command == "find":
        result = agent.find(args.market, args.geography, args.product)
    elif args.command == "qualify":
        result = agent.qualify()
    elif args.command == "enrich":
        result = agent.enrich(args.company)
    elif args.command == "enrich-all":
        result = agent.enrich_all()
    else:
        parser.error(f"Unknown command: {args.command}")

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

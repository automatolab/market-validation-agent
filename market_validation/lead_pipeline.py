from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from market_validation.environment import load_project_env
from market_validation.output_store import persist_stage_result

STAGE_SEQUENCE = (
    "research_ingest",
    "lead_qualify",
    "outreach_email",
    "reply_parse",
    "call_sheet_build",
)

ALLOWED_SOURCE_TYPES = {"search", "review_site", "directory", "internal_feed"}


@dataclass
class ConfigValidationResult:
    errors: list[str]
    warnings: list[str]


def _safe_segment(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    cleaned = cleaned.strip("-")
    return cleaned or "run"


def _slug(value: str) -> str:
    lowered = value.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")
    return slug or "market"


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_optional(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def _extract_first_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        raise ValueError("Empty model output")

    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    fenced_match = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", text, flags=re.IGNORECASE)
    if fenced_match:
        candidate = fenced_match.group(1)
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed

    decoder = json.JSONDecoder()
    for idx, char in enumerate(text):
        if char != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(text[idx:])
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            return obj

    raise ValueError("No JSON object found in model output")


def _parse_iso_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _days_since(value: str | None, now: datetime) -> int:
    parsed = _parse_iso_dt(value)
    if parsed is None:
        return 999
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta = now - parsed
    days = delta.days
    return days if days >= 0 else 0


def _json_read(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def load_config(config_path: Path, root: Path) -> tuple[dict[str, Any], list[str], Path]:
    warnings: list[str] = []
    resolved_config = config_path if config_path.is_absolute() else root / config_path
    if config_path.is_absolute() and config_path.exists():
        payload = _json_read(config_path)
        return payload, warnings, config_path

    if resolved_config.exists():
        payload = _json_read(resolved_config)
        return payload, warnings, resolved_config

    fallback = resolved_config.with_name("lead-pipeline.example.json")
    if fallback.exists():
        payload = _json_read(fallback)
        warnings.append(f"Config file not found: {resolved_config}. Loaded example template: {fallback}")
        return payload, warnings, fallback

    raise FileNotFoundError(f"Config file not found: {resolved_config}")


def validate_config(config: dict[str, Any], root: Path) -> ConfigValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    def _required_str(key: str) -> None:
        value = config.get(key)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"Missing or invalid '{key}'")

    _required_str("market")
    _required_str("geography")
    _required_str("target_product")

    max_companies = config.get("max_companies", 100)
    if not isinstance(max_companies, int) or max_companies <= 0:
        errors.append("'max_companies' must be a positive integer")

    source_configs = config.get("source_configs")
    if not isinstance(source_configs, list) or not source_configs:
        errors.append("'source_configs' must be a non-empty list")
    else:
        seen_source_ids: set[str] = set()
        enabled_count = 0
        for idx, source in enumerate(source_configs):
            if not isinstance(source, dict):
                errors.append(f"source_configs[{idx}] must be an object")
                continue
            for key in ("source_id", "source_type", "query", "region"):
                if not isinstance(source.get(key), str) or not str(source.get(key)).strip():
                    errors.append(f"source_configs[{idx}].{key} is required")

            source_id = str(source.get("source_id") or "").strip()
            if source_id:
                if source_id in seen_source_ids:
                    errors.append(f"Duplicate source_id: {source_id}")
                seen_source_ids.add(source_id)

            source_type = str(source.get("source_type") or "").strip()
            if source_type and source_type not in ALLOWED_SOURCE_TYPES:
                allowed = ", ".join(sorted(ALLOWED_SOURCE_TYPES))
                errors.append(f"source_configs[{idx}].source_type must be one of: {allowed}")

            enabled_value = source.get("enabled")
            if not isinstance(enabled_value, bool):
                errors.append(f"source_configs[{idx}].enabled must be boolean")
            elif enabled_value:
                enabled_count += 1

            auth_env = source.get("auth_env")
            if auth_env is not None:
                if not isinstance(auth_env, str) or not auth_env.strip():
                    errors.append(f"source_configs[{idx}].auth_env must be a non-empty string when provided")
                elif os.getenv(auth_env.strip()) is None:
                    warnings.append(
                        f"source_configs[{idx}] expects env var '{auth_env.strip()}', but it is not set in current environment"
                    )

        if enabled_count == 0:
            warnings.append("All source_configs are disabled; research-ingest will fail with missing_source_config")

    template = config.get("email_template")
    if not isinstance(template, dict):
        errors.append("'email_template' must be an object")
    else:
        for key in ("template_id", "subject_template", "body_template", "tone"):
            if not isinstance(template.get(key), str) or not str(template.get(key)).strip():
                errors.append(f"email_template.{key} is required")

    for key in ("default_model", "default_agent"):
        value = config.get(key)
        if value is not None and (not isinstance(value, str) or not value.strip()):
            errors.append(f"{key} must be a non-empty string when provided")

    required_mode_files = [
        root / "modes" / "_shared.md",
        root / "modes" / "research-ingest.md",
        root / "modes" / "lead-qualify.md",
        root / "modes" / "outreach-email.md",
        root / "modes" / "reply-parse.md",
        root / "modes" / "call-sheet-build.md",
    ]
    for path in required_mode_files:
        if not path.exists():
            errors.append(f"Missing mode file: {path}")

    return ConfigValidationResult(errors=errors, warnings=warnings)


def _run_stage_file(root: Path, run_id: str, stage: str) -> Path:
    run_dir = root / "output" / "runs" / _safe_segment(run_id)
    return run_dir / f"{stage.replace('_', '-')}.json"


def _load_stage_output(root: Path, run_id: str, stage: str) -> dict[str, Any]:
    path = _run_stage_file(root, run_id, stage)
    if not path.exists():
        raise FileNotFoundError(f"Missing prior stage output: {path}")
    return _json_read(path)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        text = raw_line.strip()
        if not text:
            continue
        payload = json.loads(text)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _interest_level(status: str) -> str:
    if status in {"call_ready", "replied_interested"}:
        return "high"
    if status in {"qualified", "emailed"}:
        return "medium"
    if status in {"replied_not_now", "new"}:
        return "low"
    return "unknown"


def _volume_level(value: Any) -> str:
    try:
        volume = int(value)
    except (TypeError, ValueError):
        return "unknown"
    if volume >= 500:
        return "high"
    if volume >= 200:
        return "medium"
    if volume > 0:
        return "low"
    return "unknown"


def build_stage_payload(
    *,
    stage: str,
    run_id: str,
    config: dict[str, Any],
    root: Path,
    input_payload: dict[str, Any] | None = None,
    messages_file: Path | None = None,
) -> dict[str, Any]:
    if input_payload is not None:
        payload = dict(input_payload)
        payload["run_id"] = run_id
        payload["stage"] = stage
        return payload

    market = str(config.get("market") or "").strip()

    if stage == "research_ingest":
        return {
            "run_id": run_id,
            "stage": stage,
            "market": market,
            "geography": str(config.get("geography") or "").strip(),
            "max_companies": int(config.get("max_companies", 100)),
            "source_configs": config.get("source_configs", []),
        }

    if stage == "lead_qualify":
        research = _load_stage_output(root, run_id, "research_ingest")
        return {
            "run_id": run_id,
            "stage": stage,
            "market": market,
            "target_product": str(config.get("target_product") or "").strip(),
            "companies": research.get("companies", []),
        }

    if stage == "outreach_email":
        qualified = _load_stage_output(root, run_id, "lead_qualify")
        qualified_companies = qualified.get("qualified_companies") if isinstance(qualified.get("qualified_companies"), list) else []
        companies: list[dict[str, Any]] = []
        for row in qualified_companies:
            if not isinstance(row, dict):
                continue
            if str(row.get("status") or "").strip() != "qualified":
                continue
            contact_email = str(row.get("contact_email") or "").strip()
            if not contact_email:
                continue
            companies.append(
                {
                    "company_id": str(row.get("company_id") or "").strip(),
                    "company_name": str(row.get("company_name") or "").strip(),
                    "contact_name": str(row.get("contact_name") or "").strip() or None,
                    "contact_email": contact_email,
                    "status": "qualified",
                    "claims": row.get("claims", []),
                }
            )
        return {
            "run_id": run_id,
            "stage": stage,
            "market": market,
            "template": config.get("email_template", {}),
            "companies": companies,
        }

    if stage == "reply_parse":
        messages: list[dict[str, Any]] = []
        if messages_file is not None and messages_file.exists():
            loaded = json.loads(messages_file.read_text(encoding="utf-8"))
            if isinstance(loaded, dict) and isinstance(loaded.get("messages"), list):
                messages = loaded.get("messages", [])
            elif isinstance(loaded, list):
                messages = loaded
            else:
                raise ValueError(f"Unsupported messages payload in {messages_file}")
        return {
            "run_id": run_id,
            "stage": stage,
            "messages": messages,
        }

    if stage == "call_sheet_build":
        leads_file = root / "output" / "leads" / "leads.jsonl"
        leads = _read_jsonl(leads_file)
        now = datetime.now(timezone.utc)

        records: list[dict[str, Any]] = []
        for lead in leads:
            status = str(lead.get("status") or "").strip()
            records.append(
                {
                    "company_id": str(lead.get("company_id") or "").strip(),
                    "company_name": str(lead.get("company_name") or "").strip(),
                    "status": status,
                    "priority_signals": {
                        "interest_level": _interest_level(status),
                        "volume_signal": _volume_level(lead.get("estimated_monthly_volume_lb")),
                        "recency_days": _days_since(str(lead.get("updated_at") or ""), now),
                    },
                    "latest_summary": str(lead.get("reply_summary") or lead.get("notes") or "").strip(),
                }
            )

        return {
            "run_id": run_id,
            "stage": stage,
            "market": market,
            "records": records,
        }

    raise ValueError(f"Unsupported stage: {stage}")


def build_stage_prompt(stage: str, payload: dict[str, Any], root: Path) -> str:
    shared = _read_optional(root / "modes" / "_shared.md")
    stage_file = root / "modes" / f"{stage.replace('_', '-')}.md"
    contract = _read_optional(stage_file)
    if not contract:
        raise FileNotFoundError(f"Stage mode file is missing or empty: {stage_file}")

    payload_json = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    header = (
        f"You are executing stage '{stage}'.\n"
        "Return exactly one JSON object and nothing else.\n"
        "No markdown fences and no extra prose.\n"
    )

    input_block = (
        "Input payload:\n"
        "```json\n"
        f"{payload_json}\n"
        "```\n"
    )

    sections = [header, shared, contract, input_block]
    return "\n\n".join(section for section in sections if section)


def invoke_stage_model(
    *,
    stage: str,
    payload: dict[str, Any],
    root: Path,
    model: str | None,
    agent: str | None,
) -> dict[str, Any]:
    prompt = build_stage_prompt(stage=stage, payload=payload, root=root)

    command = [
        "opencode",
        "run",
        "--dangerously-skip-permissions",
        "--dir",
        str(root),
    ]
    if model:
        command.extend(["--model", model])
    if agent:
        command.extend(["--agent", agent])
    command.append(prompt)

    completed = subprocess.run(
        command,
        cwd=str(root),
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        err = completed.stderr.strip() or completed.stdout.strip() or f"opencode exited {completed.returncode}"
        raise RuntimeError(err)

    parsed = _extract_first_json_object(completed.stdout)
    return parsed


def _resolve_model_agent(config: dict[str, Any], model: str | None, agent: str | None) -> tuple[str | None, str | None]:
    resolved_model = model or str(config.get("default_model") or "").strip() or os.getenv("OPENCODE_MODEL")
    resolved_agent = agent or str(config.get("default_agent") or "").strip() or os.getenv("OPENCODE_AGENT")
    return resolved_model, resolved_agent


def run_stage(
    *,
    stage: str,
    run_id: str,
    config: dict[str, Any],
    root: Path,
    model: str | None,
    agent: str | None,
    input_payload: dict[str, Any] | None = None,
    messages_file: Path | None = None,
) -> dict[str, Any]:
    payload = build_stage_payload(
        stage=stage,
        run_id=run_id,
        config=config,
        root=root,
        input_payload=input_payload,
        messages_file=messages_file,
    )

    stage_result = invoke_stage_model(
        stage=stage,
        payload=payload,
        root=root,
        model=model,
        agent=agent,
    )

    normalized = dict(stage_result)
    normalized["stage"] = stage
    normalized["run_id"] = run_id

    result_value = str(normalized.get("result") or "ok").strip().lower()
    normalized["result"] = result_value if result_value in {"ok", "failed"} else "failed"
    normalized.setdefault("warnings", [])
    normalized.setdefault("errors", [])
    normalized.setdefault("failure_mode", "none" if normalized["result"] == "ok" else "unknown")

    store_result = persist_stage_result(payload=normalized, root=root)

    return {
        "result": "ok" if normalized["result"] == "ok" else "failed",
        "run_id": run_id,
        "stage": stage,
        "stage_result": normalized,
        "store_result": store_result,
    }


def run_pipeline(
    *,
    run_id: str,
    config: dict[str, Any],
    root: Path,
    model: str | None,
    agent: str | None,
    messages_file: Path | None,
    start_stage: str | None,
    end_stage: str | None,
) -> dict[str, Any]:
    start_index = STAGE_SEQUENCE.index(start_stage) if start_stage else 0
    end_index = STAGE_SEQUENCE.index(end_stage) if end_stage else len(STAGE_SEQUENCE) - 1
    if end_index < start_index:
        raise ValueError("end_stage must be after or equal to start_stage")

    stages = STAGE_SEQUENCE[start_index : end_index + 1]
    stage_summaries: list[dict[str, Any]] = []

    for stage in stages:
        stage_messages_file = messages_file if stage == "reply_parse" else None
        summary = run_stage(
            stage=stage,
            run_id=run_id,
            config=config,
            root=root,
            model=model,
            agent=agent,
            input_payload=None,
            messages_file=stage_messages_file,
        )
        stage_summaries.append(
            {
                "stage": stage,
                "result": summary["stage_result"].get("result"),
                "failure_mode": summary["stage_result"].get("failure_mode"),
                "stage_file": summary["store_result"].get("stage_file"),
            }
        )
        if summary["stage_result"].get("result") != "ok":
            return {
                "result": "failed",
                "run_id": run_id,
                "stages": stage_summaries,
                "error": f"Stage failed: {stage}",
            }

    return {
        "result": "ok",
        "run_id": run_id,
        "stages": stage_summaries,
    }


def _default_run_id(market: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{_slug(market)}-{ts}"


def _load_input_payload(input_file: Path | None) -> dict[str, Any] | None:
    if input_file is None:
        return None
    payload = _json_read(input_file)
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Lead pipeline CLI for config check, stage runs, and full runs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def _add_shared(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--config", default="config/lead-pipeline.json")
        subparser.add_argument("--root", default=".")
        subparser.add_argument("--model", default=None)
        subparser.add_argument("--agent", default=None)

    check_parser = subparsers.add_parser("config-check", help="Validate lead pipeline config")
    check_parser.add_argument("--config", default="config/lead-pipeline.json")
    check_parser.add_argument("--root", default=".")

    stage_parser = subparsers.add_parser("stage-run", help="Run one stage from config context")
    _add_shared(stage_parser)
    stage_parser.add_argument("--stage", required=True, choices=STAGE_SEQUENCE)
    stage_parser.add_argument("--run-id", default=None)
    stage_parser.add_argument("--input-file", default=None)
    stage_parser.add_argument("--messages-file", default=None)

    run_parser = subparsers.add_parser("run", help="Run full staged lead pipeline")
    _add_shared(run_parser)
    run_parser.add_argument("--run-id", default=None)
    run_parser.add_argument("--messages-file", default=None)
    run_parser.add_argument("--start-stage", default=None, choices=STAGE_SEQUENCE)
    run_parser.add_argument("--end-stage", default=None, choices=STAGE_SEQUENCE)

    return parser


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=True))


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    root = Path(args.root).resolve()

    load_project_env(root=root)

    try:
        config, config_warnings, resolved_config_path = load_config(Path(args.config), root=root)
    except Exception as exc:
        _print_json({"result": "failed", "error": str(exc)})
        raise SystemExit(1)

    if args.command == "config-check":
        validation = validate_config(config=config, root=root)
        result_payload = {
            "result": "ok" if not validation.errors else "failed",
            "config": str(resolved_config_path),
            "errors": validation.errors,
            "warnings": config_warnings + validation.warnings,
        }
        _print_json(result_payload)
        if validation.errors:
            raise SystemExit(1)
        return

    validation = validate_config(config=config, root=root)
    if validation.errors:
        _print_json(
            {
                "result": "failed",
                "error": "Config validation failed",
                "errors": validation.errors,
                "warnings": config_warnings + validation.warnings,
            }
        )
        raise SystemExit(1)

    resolved_model, resolved_agent = _resolve_model_agent(config, args.model, args.agent)

    if args.command == "stage-run":
        run_id = args.run_id or _default_run_id(str(config.get("market") or "market"))
        input_payload = _load_input_payload(Path(args.input_file)) if args.input_file else None
        messages_file = Path(args.messages_file) if args.messages_file else None
        summary = run_stage(
            stage=args.stage,
            run_id=run_id,
            config=config,
            root=root,
            model=resolved_model,
            agent=resolved_agent,
            input_payload=input_payload,
            messages_file=messages_file,
        )
        summary["warnings"] = config_warnings + validation.warnings
        _print_json(summary)
        if summary.get("result") != "ok":
            raise SystemExit(1)
        return

    if args.command == "run":
        run_id = args.run_id or _default_run_id(str(config.get("market") or "market"))
        messages_file = Path(args.messages_file) if args.messages_file else None
        summary = run_pipeline(
            run_id=run_id,
            config=config,
            root=root,
            model=resolved_model,
            agent=resolved_agent,
            messages_file=messages_file,
            start_stage=args.start_stage,
            end_stage=args.end_stage,
        )
        summary["warnings"] = config_warnings + validation.warnings
        _print_json(summary)
        if summary.get("result") != "ok":
            raise SystemExit(1)
        return

    _print_json({"result": "failed", "error": f"Unsupported command: {args.command}"})
    raise SystemExit(1)


if __name__ == "__main__":
    main()

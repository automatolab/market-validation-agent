from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

KNOWN_STAGES = {
    "research_ingest",
    "lead_qualify",
    "outreach_email",
    "reply_parse",
    "call_sheet_build",
    "worker_result",
}

LEAD_STATUSES = {
    "new",
    "qualified",
    "emailed",
    "replied_interested",
    "replied_not_now",
    "do_not_contact",
    "call_ready",
    "scanning",
    "validated",
    "interviewing",
    "test_ready",
    "monitor",
    "rejected",
    "archived",
}

CALL_SHEET_EXPORT_STATUSES = ("call_ready", "replied_interested", "qualified")

STATUS_SUMMARY_ORDER = (
    "new",
    "qualified",
    "emailed",
    "replied_interested",
    "replied_not_now",
    "do_not_contact",
    "call_ready",
    "scanning",
    "validated",
    "interviewing",
    "test_ready",
    "monitor",
    "rejected",
    "archived",
)


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_segment(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    cleaned = cleaned.strip("-")
    return cleaned or "run"


def _normalize_stage(stage: str) -> str:
    normalized = stage.strip().lower().replace("-", "_").replace(" ", "_")
    if normalized not in KNOWN_STAGES:
        known = ", ".join(sorted(KNOWN_STAGES))
        raise ValueError(f"Unsupported stage '{stage}'. Expected one of: {known}")
    return normalized


def _normalize_status(status: str) -> str:
    normalized = status.strip().lower().replace("-", "_").replace(" ", "_")
    if normalized not in LEAD_STATUSES:
        known = ", ".join(sorted(LEAD_STATUSES))
        raise ValueError(f"Unsupported status '{status}'. Expected one of: {known}")
    return normalized


def _normalize_links(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    links: list[str] = []
    for value in values:
        link = str(value or "").strip()
        if link.startswith("http://") or link.startswith("https://"):
            links.append(link)
    deduped: list[str] = []
    seen: set[str] = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        deduped.append(link)
    return deduped


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _escape_md(value: Any) -> str:
    return str(value or "-").replace("|", "\\|")


def _ensure_dirs(root: Path) -> dict[str, Path]:
    output_dir = root / "output"
    runs_dir = output_dir / "runs"
    leads_dir = output_dir / "leads"
    call_sheets_dir = output_dir / "call-sheets"
    dashboard_dir = output_dir / "dashboard"

    runs_dir.mkdir(parents=True, exist_ok=True)
    leads_dir.mkdir(parents=True, exist_ok=True)
    call_sheets_dir.mkdir(parents=True, exist_ok=True)
    dashboard_dir.mkdir(parents=True, exist_ok=True)

    return {
        "output_dir": output_dir,
        "runs_dir": runs_dir,
        "leads_file": leads_dir / "leads.jsonl",
        "call_sheets_dir": call_sheets_dir,
        "dashboard_file": dashboard_dir / "summary.md",
    }


def _read_lead_index(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}

    index: dict[str, dict[str, Any]] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            continue
        company_id = str(payload.get("company_id") or "").strip()
        if not company_id:
            continue
        index[company_id] = payload
    return index


def _write_lead_index(path: Path, index: dict[str, dict[str, Any]]) -> None:
    rows = [index[key] for key in sorted(index)]
    lines = [json.dumps(row, ensure_ascii=True, sort_keys=True) for row in rows]
    text = "\n".join(lines)
    if text:
        text += "\n"
    path.write_text(text, encoding="utf-8")


def _upsert_lead(index: dict[str, dict[str, Any]], company_id: str, now_iso: str) -> dict[str, Any]:
    lead = index.get(company_id)
    if lead is None:
        lead = {
            "company_id": company_id,
            "company_name": "",
            "market": "",
            "status": "new",
            "source_links": [],
            "evidence_links": [],
            "last_stage": "",
            "updated_at": now_iso,
        }
        index[company_id] = lead
    else:
        lead["updated_at"] = now_iso
    return lead


def _merge_links(existing: Any, incoming: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in existing if isinstance(existing, list) else []:
        link = str(value or "").strip()
        if not link:
            continue
        if link in seen:
            continue
        seen.add(link)
        merged.append(link)
    for link in incoming:
        if link in seen:
            continue
        seen.add(link)
        merged.append(link)
    return merged


def _apply_research_ingest(payload: dict[str, Any], index: dict[str, dict[str, Any]], now_iso: str) -> int:
    market = str(payload.get("market") or "").strip()
    companies = payload.get("companies") if isinstance(payload.get("companies"), list) else []
    touched = 0

    for row in companies:
        if not isinstance(row, dict):
            continue
        company_id = str(row.get("company_id") or "").strip()
        if not company_id:
            continue

        lead = _upsert_lead(index, company_id, now_iso)
        company_name = str(row.get("company_name") or "").strip()
        if company_name:
            lead["company_name"] = company_name
        if market:
            lead["market"] = market

        source_records = row.get("source_records") if isinstance(row.get("source_records"), list) else []
        source_links: list[str] = []
        for source_record in source_records:
            if not isinstance(source_record, dict):
                continue
            url = str(source_record.get("url") or "").strip()
            if url.startswith("http://") or url.startswith("https://"):
                source_links.append(url)
        if source_links:
            lead["source_links"] = _merge_links(lead.get("source_links"), source_links)

        lead["last_stage"] = "research_ingest"
        if lead.get("status") not in LEAD_STATUSES:
            lead["status"] = "new"
        touched += 1

    return touched


def _apply_lead_qualify(payload: dict[str, Any], index: dict[str, dict[str, Any]], now_iso: str) -> int:
    market = str(payload.get("market") or "").strip()
    companies = payload.get("qualified_companies") if isinstance(payload.get("qualified_companies"), list) else []
    touched = 0

    for row in companies:
        if not isinstance(row, dict):
            continue
        company_id = str(row.get("company_id") or "").strip()
        if not company_id:
            continue

        status = _normalize_status(str(row.get("status") or "new"))
        lead = _upsert_lead(index, company_id, now_iso)
        company_name = str(row.get("company_name") or "").strip()
        if company_name:
            lead["company_name"] = company_name
        if market:
            lead["market"] = market

        claims = row.get("claims") if isinstance(row.get("claims"), list) else []
        evidence_links: list[str] = []
        for claim in claims:
            if not isinstance(claim, dict):
                continue
            claim_links = _normalize_links(claim.get("evidence_links"))
            if not claim_links:
                raise ValueError(f"Company {company_id} claim missing evidence link")
            evidence_links.extend(claim_links)

        if status == "qualified" and not evidence_links:
            raise ValueError(f"Company {company_id} qualified without evidence links")

        lead["status"] = status
        lead["qualification"] = str(row.get("qualification") or "").strip() or None
        lead["confidence"] = _to_float(row.get("confidence"))
        estimated_monthly_volume = (
            row.get("estimated_monthly_volume") if isinstance(row.get("estimated_monthly_volume"), dict) else {}
        )
        lead["estimated_monthly_volume_lb"] = _to_int(estimated_monthly_volume.get("value"))
        lead["estimated_monthly_volume_basis"] = str(estimated_monthly_volume.get("basis") or "").strip() or None
        lead["notes"] = str(row.get("notes") or "").strip() or None
        if evidence_links:
            lead["evidence_links"] = _merge_links(lead.get("evidence_links"), evidence_links)
        lead["last_stage"] = "lead_qualify"
        touched += 1

    return touched


def _apply_outreach_email(payload: dict[str, Any], index: dict[str, dict[str, Any]], now_iso: str) -> int:
    drafts = payload.get("drafts") if isinstance(payload.get("drafts"), list) else []
    touched = 0

    for row in drafts:
        if not isinstance(row, dict):
            continue
        company_id = str(row.get("company_id") or "").strip()
        if not company_id:
            continue

        lead = _upsert_lead(index, company_id, now_iso)
        status = _normalize_status(str(row.get("status") or "emailed"))
        lead["status"] = status
        lead["last_email_subject"] = str(row.get("subject") or "").strip() or None
        lead["last_email_body"] = str(row.get("body") or "").strip() or None
        lead["email_template_id"] = str(row.get("template_id") or "").strip() or None
        lead["last_stage"] = "outreach_email"
        touched += 1

    return touched


def _apply_reply_parse(payload: dict[str, Any], index: dict[str, dict[str, Any]], now_iso: str) -> int:
    updates = payload.get("updates") if isinstance(payload.get("updates"), list) else []
    touched = 0

    for row in updates:
        if not isinstance(row, dict):
            continue
        company_id = str(row.get("company_id") or "").strip()
        if not company_id:
            continue

        lead = _upsert_lead(index, company_id, now_iso)
        status = _normalize_status(str(row.get("status") or "qualified"))
        lead["status"] = status
        lead["reply_intent"] = str(row.get("intent") or "").strip() or None
        lead["reply_summary"] = str(row.get("summary") or "").strip() or None

        structured_fields = row.get("structured_fields") if isinstance(row.get("structured_fields"), dict) else {}
        lead["reply_requested_follow_up"] = bool(structured_fields.get("requested_follow_up", False))
        lead["reply_requested_sample"] = bool(structured_fields.get("requested_sample", False))
        lead["reply_budget_signal"] = str(structured_fields.get("budget_signal") or "").strip() or None
        lead["reply_timeframe_signal"] = str(structured_fields.get("timeframe_signal") or "").strip() or None
        lead["reply_contact_preference"] = str(structured_fields.get("contact_preference") or "").strip() or None
        lead["last_stage"] = "reply_parse"
        touched += 1

    return touched


def _apply_call_sheet_build(payload: dict[str, Any], index: dict[str, dict[str, Any]], now_iso: str) -> int:
    call_sheet = payload.get("call_sheet") if isinstance(payload.get("call_sheet"), list) else []
    touched = 0

    for row in call_sheet:
        if not isinstance(row, dict):
            continue
        company_id = str(row.get("company_id") or "").strip()
        if not company_id:
            continue

        lead = _upsert_lead(index, company_id, now_iso)
        status = _normalize_status(str(row.get("status") or "call_ready"))
        lead["status"] = status

        company_name = str(row.get("company_name") or "").strip()
        if company_name:
            lead["company_name"] = company_name

        lead["priority_score"] = _to_int(row.get("priority_score"))
        lead["priority_tier"] = str(row.get("priority_tier") or "").strip() or None
        lead["why_now"] = str(row.get("why_now") or "").strip() or None
        lead["next_action"] = str(row.get("next_action") or "").strip() or None
        lead["notes_for_caller"] = str(row.get("notes_for_caller") or "").strip() or None
        lead["last_stage"] = "call_sheet_build"
        touched += 1

    return touched


def _apply_worker_result(payload: dict[str, Any], index: dict[str, dict[str, Any]], now_iso: str) -> int:
    company_id = str(payload.get("id") or "").strip()
    if not company_id:
        return 0

    execution_status = str(payload.get("status") or "").strip().lower()
    market = str(payload.get("market") or "").strip()
    target_customer = str(payload.get("target_customer") or "").strip()
    report = str(payload.get("report") or "").strip()

    lead = _upsert_lead(index, company_id, now_iso)
    lead["market"] = market
    lead["company_name"] = target_customer or market or f"item-{company_id}"
    lead["report"] = report or None
    lead["last_stage"] = "worker_result"

    lead_status = str(payload.get("lead_status") or "").strip()
    if lead_status:
        lead["status"] = _normalize_status(lead_status)
    elif execution_status == "failed":
        lead["status"] = "monitor"
    elif execution_status == "completed":
        existing = str(lead.get("status") or "").strip()
        if not existing or _normalize_status(existing) == "new":
            lead["status"] = "validated"
        else:
            lead["status"] = _normalize_status(existing)

    error_value = str(payload.get("error") or "").strip()
    lead["last_error"] = error_value or None

    score_value = _to_float(payload.get("score"))
    lead["score"] = score_value
    return 1


def _apply_stage(stage: str, payload: dict[str, Any], index: dict[str, dict[str, Any]], now_iso: str) -> int:
    if stage == "research_ingest":
        return _apply_research_ingest(payload, index, now_iso)
    if stage == "lead_qualify":
        return _apply_lead_qualify(payload, index, now_iso)
    if stage == "outreach_email":
        return _apply_outreach_email(payload, index, now_iso)
    if stage == "reply_parse":
        return _apply_reply_parse(payload, index, now_iso)
    if stage == "call_sheet_build":
        return _apply_call_sheet_build(payload, index, now_iso)
    if stage == "worker_result":
        return _apply_worker_result(payload, index, now_iso)
    return 0


def _priority_sort_key(lead: dict[str, Any]) -> tuple[int, int, str]:
    status_order = {"call_ready": 0, "replied_interested": 1, "qualified": 2}
    status_rank = status_order.get(str(lead.get("status") or ""), 99)
    score = _to_int(lead.get("priority_score"))
    score_rank = -(score if score is not None else -1)
    company_name = str(lead.get("company_name") or lead.get("company_id") or "").lower()
    return (status_rank, score_rank, company_name)


def _write_call_sheet(path: Path, leads: list[dict[str, Any]], now_iso: str) -> None:
    selected = [
        lead
        for lead in leads
        if str(lead.get("status") or "") in CALL_SHEET_EXPORT_STATUSES
        and str(lead.get("status") or "") != "do_not_contact"
    ]
    selected.sort(key=_priority_sort_key)

    day = now_iso.split("T", 1)[0]
    lines = [
        f"# Call Sheet {day}",
        "",
        f"Generated: {now_iso}",
        "",
    ]

    if not selected:
        lines.append("No call-ready leads.")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    lines.extend(
        [
            "| Priority | Company | Status | Market | Next Action | Why Now |",
            "|----------|---------|--------|--------|-------------|---------|",
        ]
    )

    for lead in selected:
        lines.append(
            "| "
            + " | ".join(
                [
                    _escape_md(lead.get("priority_tier") or "-"),
                    _escape_md(lead.get("company_name") or lead.get("company_id")),
                    _escape_md(lead.get("status")),
                    _escape_md(lead.get("market") or "-"),
                    _escape_md(lead.get("next_action") or "-"),
                    _escape_md(lead.get("why_now") or "-"),
                ]
            )
            + " |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_dashboard_summary(path: Path, leads: list[dict[str, Any]], now_iso: str) -> None:
    counts = Counter(str(lead.get("status") or "new") for lead in leads)
    lines = [
        "# Lead Dashboard Summary",
        "",
        f"Generated: {now_iso}",
        f"Total leads: {len(leads)}",
        "",
        "| Status | Count |",
        "|--------|-------|",
    ]

    for status in STATUS_SUMMARY_ORDER:
        count = counts.get(status, 0)
        if count <= 0:
            continue
        lines.append(f"| {status} | {count} |")

    queue = [lead for lead in leads if str(lead.get("status") or "") in CALL_SHEET_EXPORT_STATUSES]
    queue.sort(key=_priority_sort_key)

    lines.extend(["", "## Priority Queue", ""])
    if not queue:
        lines.append("No call-ready leads.")
    else:
        lines.extend(
            [
                "| Company | Status | Priority | Next Action |",
                "|---------|--------|----------|-------------|",
            ]
        )
        for lead in queue[:20]:
            lines.append(
                "| "
                + " | ".join(
                    [
                        _escape_md(lead.get("company_name") or lead.get("company_id")),
                        _escape_md(lead.get("status")),
                        _escape_md(lead.get("priority_tier") or "-"),
                        _escape_md(lead.get("next_action") or "-"),
                    ]
                )
                + " |"
            )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _relative(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def persist_stage_result(payload: dict[str, Any], root: str | Path = ".") -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be a JSON object")

    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        raise ValueError("Missing run_id in payload")

    stage = _normalize_stage(str(payload.get("stage") or ""))
    result = str(payload.get("result") or "ok").strip().lower()
    if result not in {"ok", "failed"}:
        raise ValueError("Payload result must be 'ok' or 'failed'")

    root_path = Path(root).resolve()
    now_iso = _iso_now()
    paths = _ensure_dirs(root_path)

    run_dir = paths["runs_dir"] / _safe_segment(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    stage_file = run_dir / f"{stage.replace('_', '-')}.json"

    payload_to_write = dict(payload)
    payload_to_write["run_id"] = run_id
    payload_to_write["stage"] = stage
    payload_to_write["stored_at"] = now_iso
    stage_file.write_text(json.dumps(payload_to_write, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    lead_index = _read_lead_index(paths["leads_file"])
    leads_updated = 0
    if result == "ok":
        leads_updated = _apply_stage(stage, payload_to_write, lead_index, now_iso)
    _write_lead_index(paths["leads_file"], lead_index)

    all_leads = [lead_index[key] for key in sorted(lead_index)]
    call_sheet_file = paths["call_sheets_dir"] / f"{now_iso.split('T', 1)[0]}.md"
    _write_call_sheet(call_sheet_file, all_leads, now_iso)
    _write_dashboard_summary(paths["dashboard_file"], all_leads, now_iso)

    return {
        "result": "ok",
        "run_id": run_id,
        "stage": stage,
        "stage_file": _relative(stage_file, root_path),
        "leads_file": _relative(paths["leads_file"], root_path),
        "call_sheet_file": _relative(call_sheet_file, root_path),
        "dashboard_file": _relative(paths["dashboard_file"], root_path),
        "lead_count": len(all_leads),
        "leads_updated": leads_updated,
    }


def _read_payload(input_file: str | None) -> dict[str, Any]:
    if input_file:
        text = Path(input_file).read_text(encoding="utf-8")
    else:
        text = sys.stdin.read()
    if not text.strip():
        raise ValueError("Input payload is empty")
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("Input payload must be a JSON object")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Persist stage JSON outputs and materialize markdown views")
    parser.add_argument("--input-file", default=None, help="Path to a JSON payload file. If omitted, reads JSON from stdin")
    parser.add_argument("--run-id", default=None, help="Override run_id in payload")
    parser.add_argument("--stage", default=None, help="Override stage in payload")
    parser.add_argument("--root", default=".", help="Repository root path")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        payload = _read_payload(args.input_file)
        if args.run_id:
            payload["run_id"] = args.run_id
        if args.stage:
            payload["stage"] = args.stage
        result = persist_stage_result(payload=payload, root=args.root)
    except Exception as exc:  # pragma: no cover - CLI fallback
        print(json.dumps({"result": "failed", "error": str(exc)}, ensure_ascii=True))
        raise SystemExit(1)

    print(json.dumps(result, ensure_ascii=True))


if __name__ == "__main__":
    main()

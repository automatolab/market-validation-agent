from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from market_validation.db_store import _connect, _ensure_schema, resolve_db_path

CALL_SHEET_EXPORT_STATUSES = ("call_ready", "replied_interested", "qualified")


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_call_sheet_from_db(
    *,
    root: str | Path = ".",
    db_path: str | None = None,
    status_filter: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root=root_path, db_path=db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = sqlite3.Row

        query = """
        SELECT 
            l.company_id,
            l.company_name,
            l.status,
            l.priority_score,
            l.priority_tier,
            l.next_action,
            l.why_now,
            l.estimated_monthly_volume_lb,
            l.last_stage,
            l.updated_at,
            cse.notes_for_caller,
            (SELECT COUNT(*) FROM call_notes cn WHERE cn.company_id = l.company_id) as notes_count
        FROM leads l
        LEFT JOIN call_sheet_entries cse ON l.company_id = cse.company_id
        """

        conditions = []
        params = []

        if status_filter:
            conditions.append("l.status = ?")
            params.append(status_filter)
        else:
            placeholders = ",".join(["?" for _ in CALL_SHEET_EXPORT_STATUSES])
            conditions.append(f"l.status IN ({placeholders})")
            params.extend(list(CALL_SHEET_EXPORT_STATUSES))

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY CASE l.status WHEN 'call_ready' THEN 1 WHEN 'replied_interested' THEN 2 WHEN 'qualified' THEN 3 END, l.priority_score DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

    leads = [dict(row) for row in rows]
    return {
        "result": "ok",
        "database_file": str(db_file.relative_to(root_path)) if db_file.parent == root_path else str(db_file),
        "count": len(leads),
        "call_sheet": leads,
    }


def get_dashboard_summary_from_db(
    *,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root=root_path, db_path=db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = sqlite3.Row

        status_counts = conn.execute("""
            SELECT status, COUNT(*) as count FROM leads GROUP BY status
        """).fetchall()

        total_leads = sum(row["count"] for row in status_counts)

        placeholders = ",".join(["?" for _ in CALL_SHEET_EXPORT_STATUSES])
        priority_queue = conn.execute(f"""
            SELECT company_id, company_name, status, priority_score, priority_tier, next_action
            FROM leads
            WHERE status IN ({placeholders})
            ORDER BY CASE status WHEN 'call_ready' THEN 1 WHEN 'replied_interested' THEN 2 WHEN 'qualified' THEN 3 END,
                     priority_score DESC
            LIMIT 20
        """, list(CALL_SHEET_EXPORT_STATUSES)).fetchall()

        recent_activity = conn.execute("""
            SELECT run_id, stage, result, stored_at
            FROM stage_events
            ORDER BY stored_at DESC
            LIMIT 10
        """).fetchall()

    return {
        "result": "ok",
        "database_file": str(db_file.relative_to(root_path)) if db_file.parent == root_path else str(db_file),
        "total_leads": total_leads,
        "status_counts": {row["status"]: row["count"] for row in status_counts},
        "priority_queue": [dict(row) for row in priority_queue],
        "recent_activity": [dict(row) for row in recent_activity],
    }


def export_call_notes_for_company(
    company_id: str,
    *,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root=root_path, db_path=db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = sqlite3.Row

        lead = conn.execute("""
            SELECT company_id, company_name, status, priority_tier, next_action, why_now
            FROM leads WHERE company_id = ?
        """, (company_id,)).fetchone()

        notes = conn.execute("""
            SELECT id, author, note, meeting_at, next_action, created_at
            FROM call_notes
            WHERE company_id = ?
            ORDER BY created_at DESC
        """, (company_id,)).fetchall()

        outreach = conn.execute("""
            SELECT subject, body, template_id, created_at
            FROM outreach_drafts
            WHERE company_id = ?
            ORDER BY created_at DESC
            LIMIT 5
        """, (company_id,)).fetchall()

        replies = conn.execute("""
            SELECT intent, summary, structured_json, created_at
            FROM reply_updates
            WHERE company_id = ?
            ORDER BY created_at DESC
            LIMIT 5
        """, (company_id,)).fetchall()

    return {
        "result": "ok",
        "company": dict(lead) if lead else None,
        "call_notes": [dict(row) for row in notes],
        "outreach_emails": [dict(row) for row in outreach],
        "replies": [dict(row) for row in replies],
    }


def export_markdown_call_sheet(
    *,
    root: str | Path = ".",
    db_path: str | None = None,
    status_filter: str | None = None,
    limit: int = 50,
) -> str:
    data = get_call_sheet_from_db(root=root, db_path=db_path, status_filter=status_filter, limit=limit)
    if data["result"] != "ok":
        return f"# Error: {data.get('error', 'Unknown')}"

    now = _iso_now()
    lines = [
        f"# Call Sheet",
        "",
        f"Generated: {now}",
        f"Total: {data['count']} leads",
        "",
        "| Priority | Company | Status | Next Action | Notes |",
        "|----------|---------|--------|-------------|-------|",
    ]

    for lead in data["call_sheet"]:
        tier = lead.get("priority_tier") or "-"
        name = lead.get("company_name") or lead.get("company_id") or "-"
        status = lead.get("status") or "-"
        action = lead.get("next_action") or "-"
        notes = lead.get("notes_for_caller") or "-"
        lines.append(f"| {tier} | {name} | {status} | {action} | {notes} |")

    return "\n".join(lines)


def export_markdown_dashboard(
    *,
    root: str | Path = ".",
    db_path: str | None = None,
) -> str:
    data = get_dashboard_summary_from_db(root=root, db_path=db_path)
    if data["result"] != "ok":
        return f"# Error: {data.get('error', 'Unknown')}"

    now = _iso_now()
    lines = [
        "# Dashboard Summary",
        "",
        f"Generated: {now}",
        f"Total leads: {data['total_leads']}",
        "",
        "## Status Counts",
        "",
        "| Status | Count |",
        "|--------|-------|",
    ]

    status_order = ["new", "qualified", "emailed", "replied_interested", "replied_not_now", "call_ready", "scanning", "validated", "interviewing", "test_ready", "monitor", "rejected", "archived"]
    for status in status_order:
        count = data["status_counts"].get(status, 0)
        if count > 0:
            lines.append(f"| {status} | {count} |")

    lines.extend([
        "",
        "## Priority Queue",
        "",
        "| Company | Status | Priority | Next Action |",
        "|---------|--------|----------|-------------|",
    ])

    for lead in data["priority_queue"]:
        name = lead.get("company_name") or lead.get("company_id") or "-"
        status = lead.get("status") or "-"
        tier = lead.get("priority_tier") or "-"
        action = lead.get("next_action") or "-"
        lines.append(f"| {name} | {status} | {tier} | {action} |")

    return "\n".join(lines)


def build_parser() -> Any:
    import argparse
    parser = argparse.ArgumentParser(description="SQL-backed call sheet and dashboard exports")
    parser.add_argument("--root", default=".", help="Repository root path")
    parser.add_argument("--db-path", default=None, help="SQLite DB path")
    parser.add_argument("--output-json", action="store_true", help="Output as JSON")
    parser.add_argument("--output-markdown", action="store_true", help="Output as Markdown")

    subparsers = parser.add_subparsers(dest="command", required=True)

    call_sheet_parser = subparsers.add_parser("call-sheet", help="Export call sheet from DB")
    call_sheet_parser.add_argument("--status-filter", default=None, help="Filter by status")
    call_sheet_parser.add_argument("--limit", type=int, default=50, help="Max results")

    dashboard_parser = subparsers.add_parser("dashboard", help="Export dashboard summary from DB")

    notes_parser = subparsers.add_parser("company", help="Export full company data with notes")
    notes_parser.add_argument("--company-id", required=True, help="Company ID")

    return parser


def main() -> None:
    import json

    parser = build_parser()
    args = parser.parse_args()

    root = Path(args.root).resolve()

    try:
        if args.command == "call-sheet":
            if args.output_markdown:
                print(export_markdown_call_sheet(root=root, db_path=args.db_path, limit=args.limit))
            else:
                result = get_call_sheet_from_db(root=root, db_path=args.db_path, status_filter=args.status_filter, limit=args.limit)
                if args.output_json:
                    print(json.dumps(result, ensure_ascii=True))
                else:
                    print(json.dumps(result, ensure_ascii=True, indent=2))

        elif args.command == "dashboard":
            if args.output_markdown:
                print(export_markdown_dashboard(root=root, db_path=args.db_path))
            else:
                result = get_dashboard_summary_from_db(root=root, db_path=args.db_path)
                if args.output_json:
                    print(json.dumps(result, ensure_ascii=True))
                else:
                    print(json.dumps(result, ensure_ascii=True, indent=2))

        elif args.command == "company":
            result = export_call_notes_for_company(args.company_id, root=root, db_path=args.db_path)
            if args.output_json:
                print(json.dumps(result, ensure_ascii=True))
            else:
                print(json.dumps(result, ensure_ascii=True, indent=2))

    except Exception as exc:
        print(json.dumps({"result": "failed", "error": str(exc)}, ensure_ascii=True))
        raise SystemExit(1)

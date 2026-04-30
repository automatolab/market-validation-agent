from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from market_validation.research import (
    CompanyStatus,
    _connect,
    _ensure_schema,
    resolve_db_path,
)

# Statuses that indicate a company is ready for outbound or has expressed
# interest. These are the canonical CompanyStatus values — keep in sync.
CALL_SHEET_EXPORT_STATUSES = (
    CompanyStatus.QUALIFIED,
    CompanyStatus.INTERESTED,
    CompanyStatus.CONTACTED,
)


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
            c.id as company_id,
            c.company_name,
            c.status,
            c.priority_score,
            c.priority_tier,
            c.next_action,
            c.why_now,
            c.volume_estimate,
            c.volume_unit,
            c.volume_tier,
            c.updated_at,
            c.notes,
            c.phone,
            c.email,
            c.website,
            c.location,
            c.hours,
            (SELECT COUNT(*) FROM call_notes cn WHERE cn.company_id = c.id) as notes_count
        FROM companies c
        """

        conditions = []
        params = []

        if status_filter:
            conditions.append("c.status = ?")
            params.append(status_filter)
        else:
            placeholders = ",".join(["?" for _ in CALL_SHEET_EXPORT_STATUSES])
            conditions.append(f"c.status IN ({placeholders})")
            params.extend(list(CALL_SHEET_EXPORT_STATUSES))

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY c.priority_score DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

    companies = [dict(row) for row in rows]
    return {
        "result": "ok",
        "database_file": str(db_file.relative_to(root_path)) if db_file.parent == root_path else str(db_file),
        "count": len(companies),
        "call_sheet": companies,
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
            SELECT status, COUNT(*) as count FROM companies GROUP BY status
        """).fetchall()

        total_companies = sum(row["count"] for row in status_counts)

        placeholders = ",".join(["?" for _ in CALL_SHEET_EXPORT_STATUSES])
        priority_queue = conn.execute(f"""
            SELECT id, company_name, status, priority_score, priority_tier, next_action
            FROM companies
            WHERE status IN ({placeholders})
            ORDER BY priority_score DESC
            LIMIT 20
        """, list(CALL_SHEET_EXPORT_STATUSES)).fetchall()

        recent_activity = conn.execute("""
            SELECT id as event_id, 'company_added' as stage, company_name as description, created_at
            FROM companies
            ORDER BY created_at DESC
            LIMIT 10
        """).fetchall()

    return {
        "result": "ok",
        "database_file": str(db_file.relative_to(root_path)) if db_file.parent == root_path else str(db_file),
        "total_companies": total_companies,
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

        company = conn.execute("""
            SELECT id, company_name, status, priority_tier, next_action, notes
            FROM companies WHERE id = ?
        """, (company_id,)).fetchone()

        notes = conn.execute("""
            SELECT id, author, note, meeting_at, next_action, created_at
            FROM call_notes
            WHERE company_id = ?
            ORDER BY created_at DESC
        """, (company_id,)).fetchall()

    return {
        "result": "ok",
        "company": dict(company) if company else None,
        "call_notes": [dict(row) for row in notes],
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
        "# Call Sheet",
        "",
        f"Generated: {now}",
        f"Total: {data['count']} companies",
        "",
        "| # | Company | Phone | Email | Volume Est. |",
        "|----|---------|-------|-------|------------|",
    ]

    for i, company in enumerate(data["call_sheet"], 1):
        name = company.get("company_name") or "-"
        phone = company.get("phone") or "-"
        email = company.get("email") or "-"
        volume = f"{company.get('volume_estimate', '')} {company.get('volume_unit', '')}".strip() or "-"
        lines.append(f"| {i} | {name[:28]:<28} | {phone:<15} | {email:<25} | {volume:<20} |")

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    for company in data["call_sheet"]:
        name = company.get("company_name") or "-"
        notes = company.get("notes") or "No notes"
        if len(notes) > 100:
            notes = notes[:97] + "..."
        lines.append(f"### {name}")
        lines.append(notes)
        lines.append("")

    return "\n".join(lines)


# ── CRM-mapped CSV exports ─────────────────────────────────────────────────
# Each CRM expects different field names for the same underlying data.
# Manual copy-paste from a generic CSV is high-friction at any scale, so we
# emit per-CRM exports with the right headers up-front.

# Map of canonical company field → CRM-specific column header.
_CRM_FIELD_MAPS: dict[str, dict[str, str]] = {
    "hubspot": {
        # HubSpot Contact import schema (Companies + Contact merged into a
        # single row — HubSpot dedupes on Email).
        "company_name":     "Company name",
        "website":          "Company domain name",
        "phone":            "Phone Number",
        "email":            "Email",
        "location":         "Address",
        "notes":            "Notes",
        "priority_tier":    "Lead Status",
        "research_name":    "Lifecycle Stage",
        "volume_estimate":  "Annual Revenue",
    },
    "salesforce": {
        # Salesforce Lead import. Salesforce splits Company / FirstName / LastName,
        # but for cold lists we treat company_name as "Company".
        "company_name":     "Company",
        "website":          "Website",
        "phone":            "Phone",
        "email":            "Email",
        "location":         "Address",
        "notes":            "Description",
        "priority_tier":    "Rating",
        "research_name":    "Lead Source",
        "volume_estimate":  "AnnualRevenue",
    },
    "pipedrive": {
        # Pipedrive Person + Organization combined — closest to HubSpot's shape.
        "company_name":     "Organization name",
        "website":          "Organization - Website",
        "phone":            "Phone",
        "email":            "Email",
        "location":         "Organization - Address",
        "notes":            "Note",
        "priority_tier":    "Label",
        "research_name":    "Source",
        "volume_estimate":  "Organization - Annual revenue",
    },
}


def export_crm_csv(
    crm: str,
    *,
    research_id: str | None = None,
    status_filter: str | None = None,
    root: str | Path = ".",
    db_path: str | None = None,
    limit: int = 1000,
) -> str:
    """Export companies as CSV with column headers matching the target CRM.

    crm:      'hubspot' | 'salesforce' | 'pipedrive'
    research: optional research_id to scope export. Default: all researches.

    Only writes companies that have at least one of {email, phone} so the
    output isn't full of placeholder rows.
    """
    import csv as _csv
    import io as _io

    crm_key = crm.lower().strip()
    if crm_key not in _CRM_FIELD_MAPS:
        raise ValueError(
            f"unknown CRM {crm!r}; supported: {sorted(_CRM_FIELD_MAPS)}"
        )
    field_map = _CRM_FIELD_MAPS[crm_key]

    root_path = Path(root).resolve()
    db_file = resolve_db_path(root=root_path, db_path=db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = sqlite3.Row

        query = """
            SELECT c.company_name, c.website, c.phone, c.email,
                   c.location, c.notes, c.priority_tier,
                   c.volume_estimate, c.volume_unit,
                   r.name AS research_name
            FROM companies c
            LEFT JOIN researches r ON r.id = c.research_id
            WHERE (c.email IS NOT NULL AND TRIM(c.email) != '')
               OR (c.phone IS NOT NULL AND TRIM(c.phone) != '')
        """
        params: list[Any] = []
        if research_id:
            query += " AND c.research_id = ?"
            params.append(research_id)
        if status_filter:
            query += " AND c.status = ?"
            params.append(status_filter)
        else:
            placeholders = ",".join(["?" for _ in CALL_SHEET_EXPORT_STATUSES])
            query += f" AND c.status IN ({placeholders})"
            params.extend(list(CALL_SHEET_EXPORT_STATUSES))
        query += " ORDER BY c.priority_score DESC NULLS LAST LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

    headers = [field_map[k] for k in field_map]
    buffer = _io.StringIO()
    writer = _csv.writer(buffer)
    writer.writerow(headers)
    for row in rows:
        record = dict(row)
        # Combine volume_estimate + volume_unit into a single $-string for
        # CRMs whose AnnualRevenue field expects numeric — write the volume
        # as a dollar amount when available, else blank.
        vol = record.get("volume_estimate")
        if vol is not None and vol != "":
            try:
                record["volume_estimate"] = f"{int(float(vol)):d}"
            except (ValueError, TypeError):
                record["volume_estimate"] = str(vol)
        out_row = []
        for canonical_key in field_map:
            value = record.get(canonical_key) or ""
            # Strip null bytes; CSV writers don't enjoy them.
            if isinstance(value, str):
                value = value.replace("\x00", "").replace("\r\n", " ").replace("\n", " ")
            out_row.append(value)
        writer.writerow(out_row)
    return buffer.getvalue()


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
        f"Total companies: {data.get('total_companies', 0)}",
        "",
        "## Status Counts",
        "",
        "| Status | Count |",
        "|--------|-------|",
    ]

    # Use canonical CompanyStatus values — matches what add_company / update_company
    # actually write. Legacy statuses ("call_ready", "replied_interested",
    # "scanning", "test_ready", "validated") are mapped onto canonical values
    # by normalize_company_status before they hit the DB.
    for status in CompanyStatus.ALL:
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

    subparsers.add_parser("dashboard", help="Export dashboard summary from DB")

    notes_parser = subparsers.add_parser("company", help="Export full company data with notes")
    notes_parser.add_argument("--company-id", required=True, help="Company ID")

    crm_parser = subparsers.add_parser(
        "crm-export",
        help="Export companies as CSV with CRM-specific column headers",
    )
    crm_parser.add_argument(
        "--crm", required=True,
        choices=sorted(_CRM_FIELD_MAPS.keys()),
        help="Target CRM",
    )
    crm_parser.add_argument("--research-id", help="Scope to one research")
    crm_parser.add_argument("--status", dest="status_filter", help="Filter by status")
    crm_parser.add_argument("--limit", type=int, default=1000)
    crm_parser.add_argument("--output", help="Write CSV to file instead of stdout")

    return parser


def main() -> None:

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

        elif args.command == "crm-export":
            csv_text = export_crm_csv(
                args.crm,
                research_id=args.research_id,
                status_filter=args.status_filter,
                root=root,
                db_path=args.db_path,
                limit=args.limit,
            )
            if args.output:
                Path(args.output).write_text(csv_text)
                print(f"Wrote {args.output}")
            else:
                print(csv_text)

    except Exception as exc:
        print(json.dumps({"result": "failed", "error": str(exc)}, ensure_ascii=True))
        raise SystemExit(1) from exc

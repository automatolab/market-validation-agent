from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from market_validation.environment import load_project_env

DEFAULT_DB_PATH = "output/market-validation.sqlite3"


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_links(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    links: list[str] = []
    seen: set[str] = set()
    for item in value:
        link = str(item or "").strip()
        if not link:
            continue
        if not link.startswith("http://") and not link.startswith("https://"):
            continue
        if link in seen:
            continue
        seen.add(link)
        links.append(link)
    return links


def resolve_db_path(root: Path, db_path: str | None = None) -> Path:
    configured = db_path or os.getenv("MARKET_DB_PATH") or DEFAULT_DB_PATH
    candidate = Path(configured)
    if not candidate.is_absolute():
        candidate = root / candidate
    candidate.parent.mkdir(parents=True, exist_ok=True)
    return candidate.resolve()


def _connect(db_file: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_file)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS stage_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            result TEXT NOT NULL,
            failure_mode TEXT,
            warnings_json TEXT NOT NULL,
            errors_json TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            stored_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_stage_events_run_stage
            ON stage_events(run_id, stage, stored_at);

        CREATE TABLE IF NOT EXISTS leads (
            company_id TEXT PRIMARY KEY,
            company_name TEXT NOT NULL DEFAULT '',
            market TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'new',
            last_stage TEXT,
            updated_at TEXT,
            priority_score INTEGER,
            priority_tier TEXT,
            next_action TEXT,
            why_now TEXT,
            estimated_monthly_volume_lb INTEGER,
            contact_email TEXT,
            reply_intent TEXT,
            lead_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);

        CREATE TABLE IF NOT EXISTS lead_source_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            company_id TEXT NOT NULL,
            source_id TEXT,
            url TEXT NOT NULL,
            fetched_at TEXT,
            excerpt TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(run_id, company_id, source_id, url)
        );

        CREATE TABLE IF NOT EXISTS lead_claim_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            company_id TEXT NOT NULL,
            claim TEXT NOT NULL,
            evidence_url TEXT NOT NULL,
            evidence_excerpt TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(run_id, company_id, claim, evidence_url)
        );

        CREATE TABLE IF NOT EXISTS outreach_drafts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            company_id TEXT NOT NULL,
            status TEXT,
            subject TEXT,
            body TEXT,
            template_id TEXT,
            quality_checks_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(run_id, company_id, template_id)
        );

        CREATE TABLE IF NOT EXISTS reply_updates (
            message_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            company_id TEXT NOT NULL,
            status TEXT,
            intent TEXT,
            summary TEXT,
            structured_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS call_sheet_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            company_id TEXT NOT NULL,
            company_name TEXT,
            status TEXT,
            priority_score INTEGER,
            priority_tier TEXT,
            why_now TEXT,
            next_action TEXT,
            notes_for_caller TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(run_id, company_id)
        );

        CREATE TABLE IF NOT EXISTS call_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id TEXT NOT NULL,
            author TEXT NOT NULL,
            note TEXT NOT NULL,
            meeting_at TEXT,
            next_action TEXT,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_call_notes_company_created
            ON call_notes(company_id, created_at DESC);
        """
    )


def _relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _insert_stage_event(conn: sqlite3.Connection, payload: dict[str, Any], stored_at: str) -> None:
    conn.execute(
        """
        INSERT INTO stage_events (
            run_id,
            stage,
            result,
            failure_mode,
            warnings_json,
            errors_json,
            payload_json,
            stored_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(payload.get("run_id") or "").strip(),
            str(payload.get("stage") or "").strip(),
            str(payload.get("result") or "").strip().lower() or "ok",
            str(payload.get("failure_mode") or "").strip() or None,
            json.dumps(payload.get("warnings", []), ensure_ascii=True, sort_keys=True),
            json.dumps(payload.get("errors", []), ensure_ascii=True, sort_keys=True),
            json.dumps(payload, ensure_ascii=True, sort_keys=True),
            stored_at,
        ),
    )


def _upsert_leads(conn: sqlite3.Connection, leads: list[dict[str, Any]], stored_at: str) -> int:
    touched = 0
    for lead in leads:
        company_id = str(lead.get("company_id") or "").strip()
        if not company_id:
            continue

        conn.execute(
            """
            INSERT INTO leads (
                company_id,
                company_name,
                market,
                status,
                last_stage,
                updated_at,
                priority_score,
                priority_tier,
                next_action,
                why_now,
                estimated_monthly_volume_lb,
                contact_email,
                reply_intent,
                lead_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_id) DO UPDATE SET
                company_name = excluded.company_name,
                market = excluded.market,
                status = excluded.status,
                last_stage = excluded.last_stage,
                updated_at = excluded.updated_at,
                priority_score = excluded.priority_score,
                priority_tier = excluded.priority_tier,
                next_action = excluded.next_action,
                why_now = excluded.why_now,
                estimated_monthly_volume_lb = excluded.estimated_monthly_volume_lb,
                contact_email = excluded.contact_email,
                reply_intent = excluded.reply_intent,
                lead_json = excluded.lead_json
            """,
            (
                company_id,
                str(lead.get("company_name") or "").strip(),
                str(lead.get("market") or "").strip(),
                str(lead.get("status") or "new").strip() or "new",
                str(lead.get("last_stage") or "").strip() or None,
                str(lead.get("updated_at") or stored_at).strip(),
                _to_int(lead.get("priority_score")),
                str(lead.get("priority_tier") or "").strip() or None,
                str(lead.get("next_action") or "").strip() or None,
                str(lead.get("why_now") or "").strip() or None,
                _to_int(lead.get("estimated_monthly_volume_lb")),
                str(lead.get("contact_email") or "").strip() or None,
                str(lead.get("reply_intent") or "").strip() or None,
                json.dumps(lead, ensure_ascii=True, sort_keys=True),
                stored_at,
            ),
        )
        touched += 1
    return touched


def _insert_research_sources(conn: sqlite3.Connection, payload: dict[str, Any], stored_at: str) -> int:
    companies = payload.get("companies") if isinstance(payload.get("companies"), list) else []
    inserted = 0
    run_id = str(payload.get("run_id") or "").strip()

    for row in companies:
        if not isinstance(row, dict):
            continue
        company_id = str(row.get("company_id") or "").strip()
        if not company_id:
            continue
        source_records = row.get("source_records") if isinstance(row.get("source_records"), list) else []
        for source in source_records:
            if not isinstance(source, dict):
                continue
            url = str(source.get("url") or "").strip()
            if not url.startswith("http://") and not url.startswith("https://"):
                continue
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO lead_source_records (
                    run_id,
                    company_id,
                    source_id,
                    url,
                    fetched_at,
                    excerpt,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    company_id,
                    str(source.get("source_id") or "").strip() or None,
                    url,
                    str(source.get("fetched_at") or "").strip() or None,
                    str(source.get("excerpt") or "").strip() or None,
                    stored_at,
                ),
            )
            if cursor.rowcount > 0:
                inserted += 1

    return inserted


def _insert_qualification_claims(conn: sqlite3.Connection, payload: dict[str, Any], stored_at: str) -> int:
    companies = payload.get("qualified_companies") if isinstance(payload.get("qualified_companies"), list) else []
    inserted = 0
    run_id = str(payload.get("run_id") or "").strip()

    for row in companies:
        if not isinstance(row, dict):
            continue
        company_id = str(row.get("company_id") or "").strip()
        if not company_id:
            continue
        claims = row.get("claims") if isinstance(row.get("claims"), list) else []
        for claim in claims:
            if not isinstance(claim, dict):
                continue
            claim_text = str(claim.get("claim") or "").strip()
            if not claim_text:
                continue
            evidence_links = _normalize_links(claim.get("evidence_links"))
            evidence_excerpt = str(claim.get("evidence_excerpt") or "").strip() or None
            for evidence_url in evidence_links:
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO lead_claim_evidence (
                        run_id,
                        company_id,
                        claim,
                        evidence_url,
                        evidence_excerpt,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        company_id,
                        claim_text,
                        evidence_url,
                        evidence_excerpt,
                        stored_at,
                    ),
                )
                if cursor.rowcount > 0:
                    inserted += 1

    return inserted


def _insert_outreach_drafts(conn: sqlite3.Connection, payload: dict[str, Any], stored_at: str) -> int:
    drafts = payload.get("drafts") if isinstance(payload.get("drafts"), list) else []
    inserted = 0
    run_id = str(payload.get("run_id") or "").strip()

    for row in drafts:
        if not isinstance(row, dict):
            continue
        company_id = str(row.get("company_id") or "").strip()
        if not company_id:
            continue
        template_id = str(row.get("template_id") or "").strip()
        cursor = conn.execute(
            """
            INSERT OR REPLACE INTO outreach_drafts (
                run_id,
                company_id,
                status,
                subject,
                body,
                template_id,
                quality_checks_json,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                company_id,
                str(row.get("status") or "").strip() or None,
                str(row.get("subject") or "").strip() or None,
                str(row.get("body") or "").strip() or None,
                template_id or None,
                json.dumps(row.get("quality_checks") or {}, ensure_ascii=True, sort_keys=True),
                stored_at,
            ),
        )
        if cursor.rowcount > 0:
            inserted += 1

    return inserted


def _insert_reply_updates(conn: sqlite3.Connection, payload: dict[str, Any], stored_at: str) -> int:
    updates = payload.get("updates") if isinstance(payload.get("updates"), list) else []
    inserted = 0
    run_id = str(payload.get("run_id") or "").strip()

    for row in updates:
        if not isinstance(row, dict):
            continue
        message_id = str(row.get("message_id") or "").strip()
        company_id = str(row.get("company_id") or "").strip()
        if not message_id or not company_id:
            continue

        conn.execute(
            """
            INSERT INTO reply_updates (
                message_id,
                run_id,
                company_id,
                status,
                intent,
                summary,
                structured_json,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(message_id) DO UPDATE SET
                run_id = excluded.run_id,
                company_id = excluded.company_id,
                status = excluded.status,
                intent = excluded.intent,
                summary = excluded.summary,
                structured_json = excluded.structured_json,
                updated_at = excluded.updated_at
            """,
            (
                message_id,
                run_id,
                company_id,
                str(row.get("status") or "").strip() or None,
                str(row.get("intent") or "").strip() or None,
                str(row.get("summary") or "").strip() or None,
                json.dumps(row.get("structured_fields") or {}, ensure_ascii=True, sort_keys=True),
                stored_at,
                stored_at,
            ),
        )
        inserted += 1

    return inserted


def _insert_call_sheet_entries(conn: sqlite3.Connection, payload: dict[str, Any], stored_at: str) -> int:
    entries = payload.get("call_sheet") if isinstance(payload.get("call_sheet"), list) else []
    inserted = 0
    run_id = str(payload.get("run_id") or "").strip()

    for row in entries:
        if not isinstance(row, dict):
            continue
        company_id = str(row.get("company_id") or "").strip()
        if not company_id:
            continue

        cursor = conn.execute(
            """
            INSERT OR REPLACE INTO call_sheet_entries (
                run_id,
                company_id,
                company_name,
                status,
                priority_score,
                priority_tier,
                why_now,
                next_action,
                notes_for_caller,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                company_id,
                str(row.get("company_name") or "").strip() or None,
                str(row.get("status") or "").strip() or None,
                _to_int(row.get("priority_score")),
                str(row.get("priority_tier") or "").strip() or None,
                str(row.get("why_now") or "").strip() or None,
                str(row.get("next_action") or "").strip() or None,
                str(row.get("notes_for_caller") or "").strip() or None,
                stored_at,
            ),
        )
        if cursor.rowcount > 0:
            inserted += 1

    return inserted


def _insert_stage_specific_records(conn: sqlite3.Connection, payload: dict[str, Any], stored_at: str) -> int:
    result = str(payload.get("result") or "").strip().lower()
    if result and result != "ok":
        return 0

    stage = str(payload.get("stage") or "").strip()
    if stage == "research_ingest":
        return _insert_research_sources(conn, payload, stored_at)
    if stage == "lead_qualify":
        return _insert_qualification_claims(conn, payload, stored_at)
    if stage == "outreach_email":
        return _insert_outreach_drafts(conn, payload, stored_at)
    if stage == "reply_parse":
        return _insert_reply_updates(conn, payload, stored_at)
    if stage == "call_sheet_build":
        return _insert_call_sheet_entries(conn, payload, stored_at)
    return 0


def persist_pipeline_state_to_db(
    *,
    payload: dict[str, Any],
    leads: list[dict[str, Any]],
    root: Path,
    db_path: str | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")

    run_id = str(payload.get("run_id") or "").strip()
    stage = str(payload.get("stage") or "").strip()
    if not run_id:
        raise ValueError("Missing run_id in payload")
    if not stage:
        raise ValueError("Missing stage in payload")

    root_path = root.resolve()
    db_file = resolve_db_path(root=root_path, db_path=db_path)
    stored_at = str(payload.get("stored_at") or "").strip() or _iso_now()

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        _insert_stage_event(conn, payload, stored_at)
        leads_upserted = _upsert_leads(conn, leads, stored_at)
        stage_rows_inserted = _insert_stage_specific_records(conn, payload, stored_at)

    return {
        "result": "ok",
        "database_file": _relative(db_file, root_path),
        "leads_upserted": leads_upserted,
        "stage_rows_inserted": stage_rows_inserted,
    }


def add_call_note(
    *,
    company_id: str,
    author: str,
    note: str,
    root: str | Path = ".",
    db_path: str | None = None,
    meeting_at: str | None = None,
    next_action: str | None = None,
) -> dict[str, Any]:
    company_id_value = company_id.strip()
    author_value = author.strip()
    note_value = note.strip()
    if not company_id_value:
        raise ValueError("company_id is required")
    if not author_value:
        raise ValueError("author is required")
    if not note_value:
        raise ValueError("note is required")

    root_path = Path(root).resolve()
    db_file = resolve_db_path(root=root_path, db_path=db_path)
    created_at = _iso_now()

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO call_notes (
                company_id,
                author,
                note,
                meeting_at,
                next_action,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                company_id_value,
                author_value,
                note_value,
                (meeting_at or "").strip() or None,
                (next_action or "").strip() or None,
                created_at,
            ),
        )
        note_id = int(cursor.lastrowid)

    return {
        "result": "ok",
        "note_id": note_id,
        "company_id": company_id_value,
        "created_at": created_at,
        "database_file": _relative(db_file, root_path),
    }


def list_call_notes(
    *,
    root: str | Path = ".",
    db_path: str | None = None,
    company_id: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")

    root_path = Path(root).resolve()
    db_file = resolve_db_path(root=root_path, db_path=db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = sqlite3.Row

        if company_id:
            rows = conn.execute(
                """
                SELECT id, company_id, author, note, meeting_at, next_action, created_at
                FROM call_notes
                WHERE company_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (company_id.strip(), int(limit)),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, company_id, author, note, meeting_at, next_action, created_at
                FROM call_notes
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()

    notes = [dict(row) for row in rows]
    return {
        "result": "ok",
        "database_file": _relative(db_file, root_path),
        "count": len(notes),
        "notes": notes,
    }


def build_parser() -> Any:
    import argparse

    parser = argparse.ArgumentParser(description="Call notes CLI for market validation DB")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def _add_shared_flags(subparser: Any) -> None:
        subparser.add_argument("--root", default=".", help="Repository root path")
        subparser.add_argument(
            "--db-path",
            default=None,
            help="SQLite DB path. Defaults to MARKET_DB_PATH or output/market-validation.sqlite3",
        )

    add_parser = subparsers.add_parser("add", help="Add a call note")
    _add_shared_flags(add_parser)
    add_parser.add_argument("--company-id", required=True)
    add_parser.add_argument("--author", required=True)
    add_parser.add_argument("--note", required=True)
    add_parser.add_argument("--meeting-at", default=None)
    add_parser.add_argument("--next-action", default=None)

    list_parser = subparsers.add_parser("list", help="List call notes")
    _add_shared_flags(list_parser)
    list_parser.add_argument("--company-id", default=None)
    list_parser.add_argument("--limit", type=int, default=100)

    return parser


def main() -> None:
    import json

    parser = build_parser()
    args = parser.parse_args()

    root_arg = Path(getattr(args, "root", ".")).resolve()
    load_project_env(root=root_arg)

    try:
        if args.command == "add":
            result = add_call_note(
                company_id=args.company_id,
                author=args.author,
                note=args.note,
                root=root_arg,
                db_path=args.db_path,
                meeting_at=args.meeting_at,
                next_action=args.next_action,
            )
            print(json.dumps(result, ensure_ascii=True))
            return

        if args.command == "list":
            result = list_call_notes(
                root=root_arg,
                db_path=args.db_path,
                company_id=args.company_id,
                limit=args.limit,
            )
            print(json.dumps(result, ensure_ascii=True))
            return

        print(json.dumps({"result": "failed", "error": f"Unsupported command: {args.command}"}, ensure_ascii=True))
        raise SystemExit(1)
    except Exception as exc:
        print(json.dumps({"result": "failed", "error": str(exc)}, ensure_ascii=True))
        raise SystemExit(1)

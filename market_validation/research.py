from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_DB_PATH = "output/market-research.sqlite3"
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _connect(db_file: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_file)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def resolve_db_path(root: Path | str, db_path: str | None = None) -> Path:
    root = Path(root)
    if root.name == "market_validation" or str(root) == ".":
        root = PROJECT_ROOT
    if db_path:
        candidate = Path(db_path)
        if not candidate.is_absolute():
            candidate = root / candidate
    else:
        candidate = root / DEFAULT_DB_PATH
    candidate.parent.mkdir(parents=True, exist_ok=True)
    return candidate.resolve()


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS researches (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            market TEXT NOT NULL,
            product TEXT,
            geography TEXT,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_source_health TEXT
        );

        CREATE TABLE IF NOT EXISTS companies (
            id TEXT PRIMARY KEY,
            research_id TEXT NOT NULL,
            market TEXT NOT NULL,
            company_name TEXT NOT NULL,
            company_name_normalized TEXT,
            website TEXT,
            location TEXT,
            phone TEXT,
            email TEXT,
            status TEXT NOT NULL DEFAULT 'new',
            priority_score INTEGER,
            priority_tier TEXT,
            next_action TEXT,
            why_now TEXT,
            volume_estimate REAL,
            volume_unit TEXT,
            volume_basis TEXT,
            volume_tier TEXT,
            notes TEXT,
            menu_items TEXT,
            prices TEXT,
            hours TEXT,
            ratings TEXT,
            reviews_count INTEGER,
            raw_data TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (research_id) REFERENCES researches(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS call_notes (
            id TEXT PRIMARY KEY,
            company_id TEXT NOT NULL,
            research_id TEXT NOT NULL,
            author TEXT NOT NULL,
            note TEXT NOT NULL,
            meeting_at TEXT,
            next_action TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
            FOREIGN KEY (research_id) REFERENCES researches(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_companies_research ON companies(research_id);
        CREATE INDEX IF NOT EXISTS idx_companies_market ON companies(market);
        CREATE INDEX IF NOT EXISTS idx_companies_status ON companies(status);
        CREATE INDEX IF NOT EXISTS idx_companies_priority ON companies(priority_score DESC);
        CREATE INDEX IF NOT EXISTS idx_call_notes_company ON call_notes(company_id);
    """)
    # Ensure older databases get the new column for source health
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(researches)").fetchall()]
        if "last_source_health" not in cols:
            conn.execute("ALTER TABLE researches ADD COLUMN last_source_health TEXT")
    except Exception:
        # Be permissive: if PRAGMA or ALTER fails, continue without breaking
        pass


def create_research(
    name: str,
    market: str,
    product: str | None = None,
    geography: str | None = None,
    description: str | None = None,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    research_id = str(uuid.uuid4())[:8]
    now = _iso_now()

    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.execute(
            """INSERT INTO researches (id, name, market, product, geography, description, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)""",
            (research_id, name, market, product, geography, description, now, now),
        )

    return {
        "result": "ok",
        "research_id": research_id,
        "name": name,
        "market": market,
        "product": product,
        "geography": geography,
        "created_at": now,
    }


def list_researches(
    root: str | Path = ".",
    db_path: str | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = sqlite3.Row

        if status:
            rows = conn.execute(
                "SELECT * FROM researches WHERE status = ? ORDER BY created_at DESC",
                (status,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM researches ORDER BY created_at DESC"
            ).fetchall()

        researches = []
        for row in rows:
            r = dict(row)
            stats = conn.execute(
                """SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'qualified' THEN 1 ELSE 0 END) as qualified,
                    SUM(CASE WHEN status = 'contacted' THEN 1 ELSE 0 END) as contacted,
                    SUM(CASE WHEN status = 'interested' THEN 1 ELSE 0 END) as interested
                   FROM companies WHERE research_id = ?""",
                (r["id"],),
            ).fetchone()
            r["stats"] = dict(stats) if stats else {}
            researches.append(r)

    return {
        "result": "ok",
        "count": len(researches),
        "researches": researches,
    }


def get_research(
    research_id: str,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = sqlite3.Row

        research = conn.execute(
            "SELECT * FROM researches WHERE id = ?", (research_id,)
        ).fetchone()

        if not research:
            return {"result": "not_found", "research_id": research_id}

        companies = conn.execute(
            """SELECT c.*, 
               (SELECT COUNT(*) FROM call_notes WHERE company_id = c.id) as note_count
               FROM companies c 
               WHERE c.research_id = ? 
               ORDER BY c.priority_score DESC NULLS LAST, c.company_name""",
            (research_id,),
        ).fetchall()

        stats = conn.execute(
            """SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) as new_count,
                SUM(CASE WHEN status = 'qualified' THEN 1 ELSE 0 END) as qualified_count,
                SUM(CASE WHEN status = 'contacted' THEN 1 ELSE 0 END) as contacted_count,
                SUM(CASE WHEN status = 'interested' THEN 1 ELSE 0 END) as interested_count,
                SUM(CASE WHEN status = 'not_interested' THEN 1 ELSE 0 END) as not_interested_count,
                SUM(volume_estimate) as total_volume
               FROM companies WHERE research_id = ?""",
            (research_id,),
        ).fetchone()

    return {
        "result": "ok",
        "research": dict(research),
        "companies": [dict(c) for c in companies],
        "stats": dict(stats) if stats else {},
    }


def add_company(
    research_id: str,
    company_name: str,
    market: str,
    website: str | None = None,
    location: str | None = None,
    phone: str | None = None,
    email: str | None = None,
    hours: str | None = None,
    menu_items: list | None = None,
    prices: str | None = None,
    ratings: str | None = None,
    reviews_count: int | None = None,
    notes: str | None = None,
    raw_data: dict | None = None,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    company_id = str(uuid.uuid4())[:8]
    now = _iso_now()

    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        
        normalized_name = " ".join(company_name.strip().lower().split())

        existing = conn.execute(
            """SELECT id, company_name FROM companies 
               WHERE research_id = ? AND (
                   company_name = ? OR 
                   company_name_normalized = ? OR
                   company_name_normalized = ?
               )""",
            (research_id, company_name, normalized_name, f"%{normalized_name}%"),
        ).fetchone()

        if existing:
            return {
                "result": "skipped",
                "company_id": existing[0],
                "reason": f"Company already exists: '{existing[1]}'",
            }

        def _to_json(val):
            if val is None:
                return None
            if isinstance(val, dict):
                return json.dumps(val)
            if isinstance(val, list):
                return json.dumps(val)
            return val

        def _to_int(val):
            if val is None:
                return None
            if isinstance(val, int):
                return val
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return None

        conn.execute(
            """INSERT INTO companies 
               (id, research_id, market, company_name, company_name_normalized, website, location, phone, email, 
                status, hours, menu_items, prices, ratings, reviews_count, notes, raw_data, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                company_id,
                research_id,
                market,
                company_name,
                normalized_name,
                website,
                location,
                phone,
                email,
                hours,
                _to_json(menu_items),
                _to_json(prices) if prices else None,
                _to_json(ratings) if ratings else None,
                _to_int(reviews_count),
                notes,
                _to_json(raw_data),
                now,
                now,
            ),
        )

    return {
        "result": "ok",
        "company_id": company_id,
        "research_id": research_id,
    }


def delete_company(
    company_id: str,
    research_id: str,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        cursor = conn.execute(
            "DELETE FROM companies WHERE id = ? AND research_id = ?",
            (company_id, research_id),
        )

    return {
        "result": "ok",
        "company_id": company_id,
        "deleted": cursor.rowcount > 0,
    }


def update_company(
    company_id: str,
    research_id: str,
    fields: dict[str, Any],
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    now = _iso_now()
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    valid_fields = {
        "company_name", "company_name_normalized", "market",
        "status", "priority_score", "priority_tier", "next_action", "why_now",
        "volume_estimate", "volume_unit", "volume_basis", "volume_tier",
        "notes", "website", "location", "phone", "email", "hours",
        "menu_items", "prices", "ratings", "reviews_count", "raw_data",
        "source_records", "claims",
    }

    updates = []
    values = []
    for key, value in fields.items():
        if key in valid_fields:
            if key == "company_name":
                normalized = " ".join(str(value).strip().lower().split()) if value else None
                updates.append("company_name = ?")
                values.append(value)
                updates.append("company_name_normalized = ?")
                values.append(normalized)
                continue
            if key in ("source_records", "claims") and isinstance(value, list):
                value = json.dumps(value)
            if key in ("menu_items", "prices", "ratings", "raw_data") and isinstance(value, (list, dict)):
                value = json.dumps(value)
            updates.append(f"{key} = ?")
            values.append(value)

    if not updates:
        return {"result": "ok", "company_id": company_id, "updated": False}

    updates.append("updated_at = ?")
    values.append(now)
    values.extend([company_id, research_id])

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        cursor = conn.execute(
            f"UPDATE companies SET {', '.join(updates)} WHERE id = ? AND research_id = ?",
            values,
        )

    return {
        "result": "ok",
        "company_id": company_id,
        "updated": cursor.rowcount > 0,
    }


def add_call_note(
    company_id: str,
    research_id: str,
    author: str,
    note: str,
    meeting_at: str | None = None,
    next_action: str | None = None,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    note_id = str(uuid.uuid4())[:8]
    now = _iso_now()

    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.execute(
            """INSERT INTO call_notes (id, company_id, research_id, author, note, meeting_at, next_action, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (note_id, company_id, research_id, author, note, meeting_at, next_action, now),
        )

    return {
        "result": "ok",
        "note_id": note_id,
        "company_id": company_id,
    }


def search_companies(
    research_id: str | None = None,
    market: str | None = None,
    status: str | None = None,
    search: str | None = None,
    root: str | Path = ".",
    db_path: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = sqlite3.Row

        query = "SELECT * FROM companies WHERE 1=1"
        params = []

        if research_id:
            query += " AND research_id = ?"
            params.append(research_id)
        if market:
            query += " AND market = ?"
            params.append(market)
        if status:
            query += " AND status = ?"
            params.append(status)
        if search:
            query += " AND (company_name LIKE ? OR location LIKE ? OR notes LIKE ?)"
            search_term = f"%{search}%"
            params.extend([search_term, search_term, search_term])

        query += " ORDER BY priority_score DESC NULLS LAST, company_name LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

    return {
        "result": "ok",
        "count": len(rows),
        "companies": [dict(r) for r in rows],
    }


def export_markdown(
    research_id: str,
    root: str | Path = ".",
    db_path: str | None = None,
) -> str:
    data = get_research(research_id, root=root, db_path=db_path)
    if data.get("result") != "ok":
        return f"# Research not found: {research_id}"

    research = data["research"]
    companies = data.get("companies", [])
    stats = data.get("stats", {})

    lines = [
        f"# {research['name']}",
        "",
        f"**Market:** {research['market']}",
        f"**Product:** {research.get('product') or 'N/A'}",
        f"**Geography:** {research.get('geography') or 'N/A'}",
        f"**Status:** {research['status']}",
        f"**Created:** {research['created_at']}",
        "",
        "## Summary",
        "",
        f"- **Total Companies:** {stats.get('total', 0)}",
        f"- **Qualified:** {stats.get('qualified_count', 0)}",
        f"- **Contacted:** {stats.get('contacted_count', 0)}",
        f"- **Interested:** {stats.get('interested_count', 0)}",
        f"- **Not Interested:** {stats.get('not_interested_count', 0)}",
        f"- **Total Est. Volume:** {stats.get('total_volume') or 'N/A'}",
        "",
    ]

    if companies:
        by_status: dict[str, list] = {}
        for c in companies:
            status = c.get("status", "new")
            if status not in by_status:
                by_status[status] = []
            by_status[status].append(c)

        for status in ["qualified", "contacted", "interested", "new", "not_interested"]:
            if status not in by_status:
                continue
            comps = by_status[status]
            lines.append(f"## {status.replace('_', ' ').title()} ({len(comps)})")
            lines.append("")
            lines.append("| # | Company | Location | Volume | Priority | Notes |")
            lines.append("|---|---------|----------|--------|----------|-------|")
            for i, c in enumerate(comps, 1):
                vol = ""
                if c.get("volume_estimate"):
                    vol = f"{c['volume_estimate']} {c.get('volume_unit', 'units') or ''}"
                lines.append(
                    f"| {i} | {c['company_name']} | {c.get('location') or '-'} | {vol} | {c.get('priority_tier') or '-'} | {c.get('notes') or '-'} |"
                )
            lines.append("")

    return "\n".join(lines)


def build_parser() -> Any:
    import argparse
    parser = argparse.ArgumentParser(description="Market research database CLI")
    parser.add_argument("--root", default=".", help="Repository root path")
    parser.add_argument("--db-path", default=None, help="SQLite DB path")

    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create", help="Create research")
    create_parser.add_argument("--name", required=True)
    create_parser.add_argument("--market", required=True)
    create_parser.add_argument("--product")
    create_parser.add_argument("--geography")
    create_parser.add_argument("--description")

    list_parser = subparsers.add_parser("list", help="List researches")
    list_parser.add_argument("--status")

    get_parser = subparsers.add_parser("get", help="Get research")
    get_parser.add_argument("research_id")

    search_parser = subparsers.add_parser("search", help="Search companies")
    search_parser.add_argument("--research-id")
    search_parser.add_argument("--market")
    search_parser.add_argument("--status")
    search_parser.add_argument("--search")
    search_parser.add_argument("--limit", type=int, default=100)

    export_parser = subparsers.add_parser("export", help="Export as Markdown")
    export_parser.add_argument("research_id")
    export_parser.add_argument("--output")

    return parser


def main() -> None:
    import json

    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "create":
            result = create_research(
                name=args.name, market=args.market, product=args.product,
                geography=args.geography, description=args.description,
                root=args.root, db_path=args.db_path,
            )
            print(json.dumps(result, ensure_ascii=True))

        elif args.command == "list":
            result = list_researches(
                root=args.root, db_path=args.db_path, status=args.status,
            )
            print(json.dumps(result, ensure_ascii=True))

        elif args.command == "get":
            result = get_research(
                research_id=args.research_id,
                root=args.root, db_path=args.db_path,
            )
            print(json.dumps(result, ensure_ascii=True))

        elif args.command == "search":
            result = search_companies(
                research_id=args.research_id, market=args.market,
                status=args.status, search=args.search,
                root=args.root, db_path=args.db_path, limit=args.limit,
            )
            print(json.dumps(result, ensure_ascii=True))

        elif args.command == "export":
            md = export_markdown(
                research_id=args.research_id,
                root=args.root, db_path=args.db_path,
            )
            if args.output:
                Path(args.output).write_text(md)
                print(f"Exported to {args.output}")
            else:
                print(md)

    except Exception as exc:
        print(json.dumps({"result": "failed", "error": str(exc)}, ensure_ascii=True))
        raise SystemExit(1)

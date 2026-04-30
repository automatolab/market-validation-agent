from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from market_validation.log import get_logger

_log = get_logger("research")

DEFAULT_DB_PATH = "output/market-research.sqlite3"
PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ── Canonical status values (used everywhere) ──────────────────────────────
# Defined here so dashboard, qualification service, exports, and call sheets
# all agree on what counts as "qualified" vs. "contacted" vs. "rejected".

class CompanyStatus:
    """Canonical company.status values. Treat as a closed enum.

    Lifecycle:
        new           — discovered, awaiting qualification
        qualified     — passed qualification, ready to enrich/contact
        not_relevant  — qualifier rejected (wrong market / not a real biz)
        contacted     — outreach sent (email queued + sent)
        replied       — recipient replied
        interested    — replied with interest
        not_interested— replied no / unsubscribed / bounce
        skipped       — manually deferred
    """
    NEW = "new"
    QUALIFIED = "qualified"
    NOT_RELEVANT = "not_relevant"
    CONTACTED = "contacted"
    REPLIED = "replied"
    INTERESTED = "interested"
    NOT_INTERESTED = "not_interested"
    SKIPPED = "skipped"

    ALL: tuple[str, ...] = (
        "new", "qualified", "not_relevant", "contacted",
        "replied", "interested", "not_interested", "skipped",
    )


_VALID_COMPANY_STATUSES: frozenset[str] = frozenset(CompanyStatus.ALL)


def normalize_company_status(value: str | None) -> str:
    """Return a canonical company status. Maps deprecated values to current ones."""
    if not value:
        return CompanyStatus.NEW
    v = str(value).strip().lower()
    # Legacy / qualifier-output mappings
    _LEGACY_MAP = {
        "uncertain": CompanyStatus.NEW,
        "unknown": CompanyStatus.NEW,
        "rejected": CompanyStatus.NOT_RELEVANT,
        "irrelevant": CompanyStatus.NOT_RELEVANT,
        "lead": CompanyStatus.QUALIFIED,
        "call_ready": CompanyStatus.QUALIFIED,
        "validated": CompanyStatus.QUALIFIED,
        "replied_interested": CompanyStatus.INTERESTED,
        "replied_not_interested": CompanyStatus.NOT_INTERESTED,
        "no_reply": CompanyStatus.CONTACTED,
    }
    if v in _LEGACY_MAP:
        return _LEGACY_MAP[v]
    if v in _VALID_COMPANY_STATUSES:
        return v
    return CompanyStatus.NEW


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _add_columns_if_missing(conn: Any) -> None:
    """Add new columns to market_validations for archetype, unit economics, Porter's 5 forces, timing, and customer segments.

    DEPRECATED for new additions — new schema changes should be written as
    an alembic migration under ``migrations/versions/`` instead, run via
    ``market-db-migrate upgrade``. This function is kept so databases
    created before alembic was adopted still converge on startup without
    needing an explicit ``stamp + upgrade`` step.
    """
    new_columns = [
        # Outcome feedback (set 3-12 months after the verdict)
        ("outcome_recorded_at", "TEXT"),
        ("actual_outcome", "TEXT"),         # success | partial | failure | abandoned | pending
        ("outcome_notes", "TEXT"),
        ("outcome_revenue_actual", "REAL"),
        ("outcome_recorded_by", "TEXT"),
        # Pipeline checkpoint — last completed stage of Agent.research()
        # so partial/failed runs can resume mid-pipeline. Allowed values:
        # validate | find | qualify | enrich | drafts.
        # NOTE: this column lives on `researches`, not `market_validations`,
        # so the schema add below targets the right table separately.
        # Archetype
        ("archetype", "TEXT"),
        ("archetype_confidence", "INTEGER"),
        ("archetype_label", "TEXT"),
        # Unit economics
        ("gross_margin_low", "REAL"),
        ("gross_margin_high", "REAL"),
        ("gross_margin_confidence", "INTEGER"),
        ("cac_estimate_low", "REAL"),
        ("cac_estimate_high", "REAL"),
        ("ltv_estimate_low", "REAL"),
        ("ltv_estimate_high", "REAL"),
        ("payback_months", "REAL"),
        ("unit_economics_score", "REAL"),
        ("unit_economics_data", "TEXT"),
        # Porter's 5 forces
        ("supplier_power", "REAL"),
        ("buyer_power", "REAL"),
        ("substitute_threat", "REAL"),
        ("entry_barrier_score", "REAL"),
        ("rivalry_score", "REAL"),
        ("structural_attractiveness", "REAL"),
        ("porters_data", "TEXT"),
        # Timing
        ("timing_score", "REAL"),
        ("timing_verdict", "TEXT"),
        ("timing_enablers", "TEXT"),
        ("timing_headwinds", "TEXT"),
        # Customer segments
        ("customer_segments_data", "TEXT"),
        ("icp_clarity", "REAL"),
        ("primary_segment", "TEXT"),
        # Actionable output
        ("next_steps", "TEXT"),
        ("key_risks", "TEXT"),
        ("key_success_factors", "TEXT"),
        ("archetype_red_flags", "TEXT"),
        ("differentiation_opportunities", "TEXT"),
    ]
    for col_name, col_type in new_columns:
        try:
            conn.execute(
                f"ALTER TABLE market_validations ADD COLUMN {col_name} {col_type}"
            )
        except sqlite3.OperationalError as exc:
            # "duplicate column name" is the expected case — column already
            # exists from a prior migration. Anything else is a real schema
            # problem we want to see.
            if "duplicate column name" not in str(exc):
                _log.warning("ALTER TABLE market_validations ADD %s failed: %s", col_name, exc)


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Bootstrap the SQLite schema on a new or legacy database.

    Idempotent — uses ``CREATE TABLE IF NOT EXISTS`` and matches the baseline
    alembic migration (``migrations/versions/0001_baseline_schema.py``). For
    *new* schema changes, write an alembic migration instead of extending this
    function or ``_add_columns_if_missing``.
    """
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
        CREATE INDEX IF NOT EXISTS idx_companies_email_status ON companies(email, status);
        CREATE INDEX IF NOT EXISTS idx_companies_phone_status ON companies(phone, status);
        CREATE INDEX IF NOT EXISTS idx_companies_research_status ON companies(research_id, status);
        CREATE INDEX IF NOT EXISTS idx_call_notes_company ON call_notes(company_id);

        CREATE TABLE IF NOT EXISTS market_validations (
            id TEXT PRIMARY KEY,
            research_id TEXT NOT NULL,
            market TEXT NOT NULL,
            geography TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            -- Market Sizing
            tam_low REAL, tam_high REAL, tam_currency TEXT DEFAULT 'USD',
            tam_sources TEXT, tam_confidence INTEGER,
            sam_low REAL, sam_high REAL, sam_sources TEXT, sam_confidence INTEGER,
            som_low REAL, som_high REAL, som_sources TEXT, som_confidence INTEGER,
            -- Demand
            demand_score REAL, demand_trend TEXT, demand_seasonality TEXT,
            demand_pain_points TEXT, demand_sources TEXT,
            -- Competition
            competitive_intensity REAL, competitor_count INTEGER,
            market_concentration TEXT, direct_competitors TEXT,
            indirect_competitors TEXT, funding_signals TEXT,
            -- Signals
            job_posting_volume TEXT, news_sentiment TEXT,
            regulatory_risks TEXT, technology_maturity TEXT,
            signals_data TEXT,
            -- Scorecard
            market_attractiveness REAL, competitive_score REAL,
            demand_validation REAL, risk_score REAL,
            overall_score REAL, verdict TEXT, verdict_reasoning TEXT,
            -- Meta
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            FOREIGN KEY (research_id) REFERENCES researches(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_validations_research ON market_validations(research_id);
    """)
    # Ensure older databases get the new column for source health
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(researches)").fetchall()]
        if "last_source_health" not in cols:
            conn.execute("ALTER TABLE researches ADD COLUMN last_source_health TEXT")
        if "last_completed_stage" not in cols:
            conn.execute("ALTER TABLE researches ADD COLUMN last_completed_stage TEXT")
        if "last_stage_at" not in cols:
            conn.execute("ALTER TABLE researches ADD COLUMN last_stage_at TEXT")
    except sqlite3.OperationalError as exc:
        # Be permissive: if PRAGMA or ALTER fails, continue without breaking —
        # but log so a genuine schema problem is visible in the logs.
        _log.warning("researches column migration failed: %s", exc)
    _add_columns_if_missing(conn)


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

    # Normalize phone to E.164 with geography-derived country hint, so
    # different formats of the same number ("(555) 123-4567" vs.
    # "555.123.4567" vs. "+15551234567") collapse to one.
    if phone:
        try:
            from market_validation._helpers.contacts import detect_country, normalize_phone
            country_hint = detect_country(location)
            normalized_phone = normalize_phone(phone, country_hint=country_hint)
            if normalized_phone:
                phone = normalized_phone
        except Exception:
            # Keep original on any error — don't lose data over a normalization bug.
            pass

    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)

        # NFKD-fold so 'Café' and 'Cafe' dedupe correctly across writes.
        try:
            from market_validation._helpers.contacts import normalize_name_key
            normalized_name = normalize_name_key(company_name)
        except Exception:
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

    # Normalize status to a canonical value before writing — keeps qualifier
    # output ("uncertain") and dashboard filters ("call_ready") in sync.
    if "status" in fields and fields["status"]:
        fields = dict(fields)  # don't mutate caller's dict
        fields["status"] = normalize_company_status(fields["status"])

    # Normalize phone to E.164 if a phone is being written.
    if "phone" in fields and fields["phone"]:
        try:
            from market_validation._helpers.contacts import detect_country, normalize_phone
            hint = detect_country(fields.get("location"))
            normalized = normalize_phone(str(fields["phone"]), country_hint=hint)
            if normalized:
                fields = dict(fields)
                fields["phone"] = normalized
        except Exception:
            pass

    updates = []
    values = []
    for key, value in fields.items():
        if key in valid_fields:
            if key == "company_name":
                try:
                    from market_validation._helpers.contacts import normalize_name_key
                    normalized = normalize_name_key(str(value)) if value else None
                except Exception:
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
            if isinstance(value, str):
                value = value.replace("\x00", "")
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


def create_validation(
    research_id: str,
    market: str,
    geography: str | None = None,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    validation_id = str(uuid.uuid4())[:8]
    now = _iso_now()
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.execute(
            """INSERT INTO market_validations (id, research_id, market, geography, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, 'pending', ?, ?)""",
            (validation_id, research_id, market, geography, now, now),
        )

    return {
        "result": "ok",
        "validation_id": validation_id,
        "research_id": research_id,
        "created_at": now,
    }


def get_validation(
    validation_id: str,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM market_validations WHERE id = ?", (validation_id,)
        ).fetchone()

    if not row:
        return {"result": "not_found", "validation_id": validation_id}

    data = dict(row)
    # Parse JSON fields
    for key in ("tam_sources", "sam_sources", "som_sources", "demand_seasonality",
                "demand_pain_points", "demand_sources", "direct_competitors",
                "indirect_competitors", "funding_signals", "regulatory_risks", "signals_data"):
        if data.get(key):
            try:
                data[key] = json.loads(data[key])
            except (json.JSONDecodeError, TypeError):
                pass
    return {"result": "ok", "validation": data}


def get_validation_by_research(
    research_id: str,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM market_validations WHERE research_id = ? ORDER BY created_at DESC LIMIT 1",
            (research_id,),
        ).fetchone()

    if not row:
        return {"result": "not_found", "research_id": research_id}

    data = dict(row)
    for key in ("tam_sources", "sam_sources", "som_sources", "demand_seasonality",
                "demand_pain_points", "demand_sources", "direct_competitors",
                "indirect_competitors", "funding_signals", "regulatory_risks", "signals_data"):
        if data.get(key):
            try:
                data[key] = json.loads(data[key])
            except (json.JSONDecodeError, TypeError):
                pass
    return {"result": "ok", "validation": data}


def update_validation(
    validation_id: str,
    fields: dict[str, Any],
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    now = _iso_now()
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    valid_fields = {
        "status", "market", "geography",
        "tam_low", "tam_high", "tam_currency", "tam_sources", "tam_confidence",
        "sam_low", "sam_high", "sam_sources", "sam_confidence",
        "som_low", "som_high", "som_sources", "som_confidence",
        "demand_score", "demand_trend", "demand_seasonality",
        "demand_pain_points", "demand_sources",
        "competitive_intensity", "competitor_count", "market_concentration",
        "direct_competitors", "indirect_competitors", "funding_signals",
        "job_posting_volume", "news_sentiment", "regulatory_risks",
        "technology_maturity", "signals_data",
        "market_attractiveness", "competitive_score", "demand_validation",
        "risk_score", "overall_score", "verdict", "verdict_reasoning",
        # Archetype
        "archetype", "archetype_confidence", "archetype_label",
        # Unit economics
        "gross_margin_low", "gross_margin_high", "gross_margin_confidence",
        "cac_estimate_low", "cac_estimate_high", "ltv_estimate_low",
        "ltv_estimate_high", "payback_months", "unit_economics_score",
        "unit_economics_data",
        # Porter's 5 forces
        "supplier_power", "buyer_power", "substitute_threat",
        "entry_barrier_score", "rivalry_score", "structural_attractiveness",
        "porters_data",
        # Timing
        "timing_score", "timing_verdict", "timing_enablers", "timing_headwinds",
        # Customer segments
        "customer_segments_data", "icp_clarity", "primary_segment",
        # Actionable output
        "next_steps", "key_risks", "key_success_factors", "archetype_red_flags",
        # Outcome feedback (recorded 3-12 months after verdict)
        "outcome_recorded_at", "actual_outcome", "outcome_notes",
        "outcome_revenue_actual", "outcome_recorded_by",
    }

    json_fields = {
        "tam_sources", "sam_sources", "som_sources", "demand_seasonality",
        "demand_pain_points", "demand_sources", "direct_competitors",
        "indirect_competitors", "funding_signals", "regulatory_risks", "signals_data",
        # New JSON blob fields
        "unit_economics_data", "porters_data", "customer_segments_data",
        "timing_enablers", "timing_headwinds",
        # Actionable output (stored as JSON arrays)
        "next_steps", "key_risks", "key_success_factors", "archetype_red_flags",
    }

    updates = []
    values = []
    skipped_none: list[str] = []
    for key, value in fields.items():
        if key not in valid_fields:
            continue
        # Skip None — don't overwrite existing DB data with NULL.
        # AI sometimes returns null for fields where the heuristic already
        # computed a useful value; silently wiping those caused data loss.
        if value is None:
            skipped_none.append(key)
            continue
        if key in json_fields and isinstance(value, (list, dict)):
            value = json.dumps(value)
        # Strip null bytes — SQLite TEXT columns reject them
        if isinstance(value, str):
            value = value.replace("\x00", "")
        updates.append(f"{key} = ?")
        values.append(value)

    if skipped_none:
        import sys
        print(f"[update_validation] skipped {len(skipped_none)} None fields: {skipped_none}", file=sys.stderr)

    if not updates:
        return {"result": "ok", "validation_id": validation_id, "updated": False}

    updates.append("updated_at = ?")
    values.append(now)
    values.append(validation_id)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        cursor = conn.execute(
            f"UPDATE market_validations SET {', '.join(updates)} WHERE id = ?",
            values,
        )
    import sys
    print(f"[update_validation] saved {len(updates)-1} fields to {validation_id}", file=sys.stderr)

    return {
        "result": "ok",
        "validation_id": validation_id,
        "updated": cursor.rowcount > 0,
    }


# ── Pipeline checkpoint helpers ─────────────────────────────────────────────
# Used by Agent.research() to support --from-stage / --resume so a failed
# run picks up where it left off instead of redoing every stage.

PIPELINE_STAGES: tuple[str, ...] = ("validate", "find", "qualify", "enrich", "drafts")


def mark_stage_completed(
    research_id: str,
    stage: str,
    root: str | Path = ".",
    db_path: str | None = None,
) -> None:
    """Update `researches.last_completed_stage` after a successful stage.

    Silent no-op when the research_id is unknown — keeps the pipeline robust
    to missing rows in test scenarios.
    """
    if stage not in PIPELINE_STAGES:
        raise ValueError(f"unknown stage {stage!r}; valid: {PIPELINE_STAGES}")
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)
    with _connect(db_file) as conn:
        _ensure_schema(conn)
        try:
            conn.execute(
                "UPDATE researches SET last_completed_stage = ?, last_stage_at = ? WHERE id = ?",
                (stage, _iso_now(), research_id),
            )
        except sqlite3.Error as exc:
            _log.warning("mark_stage_completed failed for %s/%s: %s", research_id, stage, exc)


def get_last_completed_stage(
    research_id: str,
    root: str | Path = ".",
    db_path: str | None = None,
) -> str | None:
    """Return the most recently completed pipeline stage for a research, or None."""
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)
    with _connect(db_file) as conn:
        _ensure_schema(conn)
        try:
            row = conn.execute(
                "SELECT last_completed_stage FROM researches WHERE id = ?",
                (research_id,),
            ).fetchone()
        except sqlite3.Error:
            return None
    if row and row[0]:
        return str(row[0])
    return None


# ── Outcome feedback loop ──────────────────────────────────────────────────
# Records the actual market outcome 3-12 months after a verdict, so a
# calibration helper can answer: "are our 'go' verdicts predictive?"

VALID_OUTCOMES: frozenset[str] = frozenset({
    "success",      # entered, hit revenue/PMF target
    "partial",      # entered, mixed result
    "failure",      # entered, did not work
    "abandoned",    # never entered (no_go was correct, or pivoted)
    "pending",      # not yet measurable (default)
})


def record_validation_outcome(
    validation_id: str,
    actual_outcome: str,
    notes: str | None = None,
    revenue_actual: float | None = None,
    recorded_by: str | None = None,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    """Record the actual outcome of a validation 3-12 months after the verdict.

    Use ``actual_outcome`` ∈ {success, partial, failure, abandoned, pending}.
    Calling this updates the existing market_validations row in place; pass
    ``actual_outcome="pending"`` to clear a previous outcome (rare).
    """
    if actual_outcome not in VALID_OUTCOMES:
        return {
            "result": "error",
            "error": f"actual_outcome must be one of {sorted(VALID_OUTCOMES)}",
        }
    return update_validation(
        validation_id,
        {
            "outcome_recorded_at": _iso_now(),
            "actual_outcome": actual_outcome,
            "outcome_notes": notes,
            "outcome_revenue_actual": revenue_actual,
            "outcome_recorded_by": recorded_by,
        },
        root=root,
        db_path=db_path,
    )


def get_calibration_summary(
    root: str | Path = ".",
    db_path: str | None = None,
    min_outcomes: int = 3,
) -> dict[str, Any]:
    """Summarize how well past verdicts have correlated with actual outcomes.

    Bucket validations by verdict and report:
      - count of records in each bucket with a recorded outcome
      - hit rate: proportion that ended in success/partial (for go/strong_go)
                  or abandoned/failure (for no_go/cautious)
      - mean overall_score in each bucket

    Returns ``{"insufficient_data": True}`` when fewer than ``min_outcomes``
    rows have outcomes recorded — too few to draw conclusions from.
    """
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)
    with _connect(db_file) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            """SELECT verdict, overall_score, actual_outcome,
                      outcome_revenue_actual, outcome_recorded_at
               FROM market_validations
               WHERE actual_outcome IS NOT NULL AND actual_outcome != 'pending'"""
        ).fetchall()

    if not rows or len(rows) < min_outcomes:
        return {
            "result": "ok",
            "insufficient_data": True,
            "outcomes_recorded": len(rows),
            "min_required": min_outcomes,
        }

    by_verdict: dict[str, dict[str, Any]] = {}
    for verdict, score, outcome, _rev, _at in rows:
        v = verdict or "unknown"
        bucket = by_verdict.setdefault(
            v, {"count": 0, "scores": [], "outcomes": {}, "hits": 0}
        )
        bucket["count"] += 1
        bucket["scores"].append(score or 0)
        bucket["outcomes"][outcome] = bucket["outcomes"].get(outcome, 0) + 1
        # "hit" = the verdict was directionally correct.
        # go/strong_go expect positive outcomes; no_go/cautious expect negative.
        positive_outcome = outcome in ("success", "partial")
        negative_outcome = outcome in ("failure", "abandoned")
        if v in ("go", "strong_go") and positive_outcome:
            bucket["hits"] += 1
        elif v in ("no_go", "cautious") and negative_outcome:
            bucket["hits"] += 1

    summary: dict[str, dict[str, Any]] = {}
    for v, b in by_verdict.items():
        scores = b["scores"]
        summary[v] = {
            "count": b["count"],
            "mean_score": round(sum(scores) / len(scores), 1) if scores else 0,
            "hit_rate": round(b["hits"] / b["count"], 3) if b["count"] else 0.0,
            "outcomes": b["outcomes"],
        }

    return {
        "result": "ok",
        "insufficient_data": False,
        "outcomes_recorded": len(rows),
        "by_verdict": summary,
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

    outcome_parser = subparsers.add_parser(
        "record-outcome",
        help="Record actual market outcome 3-12 months after a verdict",
    )
    outcome_parser.add_argument("validation_id")
    outcome_parser.add_argument(
        "--outcome", required=True,
        choices=sorted(VALID_OUTCOMES),
        help="Actual outcome of entering (or not entering) this market",
    )
    outcome_parser.add_argument("--notes")
    outcome_parser.add_argument("--revenue", type=float, help="Actual revenue ($) if known")
    outcome_parser.add_argument("--by", help="Person/team recording the outcome")

    subparsers.add_parser(
        "calibration", help="Show how past verdicts have correlated with outcomes"
    )

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

        elif args.command == "record-outcome":
            result = record_validation_outcome(
                validation_id=args.validation_id,
                actual_outcome=args.outcome,
                notes=args.notes,
                revenue_actual=args.revenue,
                recorded_by=args.by,
                root=args.root,
                db_path=args.db_path,
            )
            print(json.dumps(result, ensure_ascii=True))

        elif args.command == "calibration":
            result = get_calibration_summary(root=args.root, db_path=args.db_path)
            print(json.dumps(result, ensure_ascii=True, indent=2))

    except Exception as exc:
        print(json.dumps({"result": "failed", "error": str(exc)}, ensure_ascii=True))
        raise SystemExit(1) from exc

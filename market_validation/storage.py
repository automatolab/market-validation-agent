from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import CallSheet, LeadRecord, LeadScore, LeadSummary, OutreachDraft, ReplyTrackingEntry, ValidationResponse


class ResearchRunRepository:
    """SQLite-backed storage for replayable market-validation research runs."""

    def __init__(self, db_path: str | None = None) -> None:
        configured_path = db_path or os.getenv("RESEARCH_DB_PATH", ".data/research_runs.db")
        self._db_path = Path(configured_path)
        self._lock = threading.Lock()
        self._ensure_parent_dir()
        self._initialize_schema()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def save_run(
        self,
        *,
        endpoint: str,
        request_payload: dict[str, Any],
        response: ValidationResponse,
    ) -> str:
        run_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc).isoformat()
        response_payload = response.model_dump()

        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO runs (
                        id,
                        created_at,
                        endpoint,
                        research_stage,
                        overall_verdict,
                        confidence_score,
                        evidence_coverage_score,
                        request_json,
                        response_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        created_at,
                        endpoint,
                        response_payload.get("research_stage"),
                        response_payload.get("overall_verdict"),
                        response_payload.get("confidence_score"),
                        response_payload.get("evidence_coverage_score"),
                        json.dumps(request_payload, ensure_ascii=True),
                        json.dumps(response_payload, ensure_ascii=True),
                    ),
                )

                self._insert_raw_sources(conn, run_id, response_payload.get("raw_sources") or [])
                self._insert_structured_evidence(
                    conn,
                    run_id,
                    response_payload.get("structured_evidence") or [],
                )
                conn.commit()

        return run_id

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        normalized_limit = max(1, min(200, int(limit)))
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT
                        id,
                        created_at,
                        endpoint,
                        research_stage,
                        overall_verdict,
                        confidence_score,
                        evidence_coverage_score
                    FROM runs
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (normalized_limit,),
                ).fetchall()

        return [
            {
                "id": row[0],
                "created_at": row[1],
                "endpoint": row[2],
                "research_stage": row[3],
                "overall_verdict": row[4],
                "confidence_score": row[5],
                "evidence_coverage_score": row[6],
            }
            for row in rows
        ]

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT
                        id,
                        created_at,
                        endpoint,
                        research_stage,
                        overall_verdict,
                        confidence_score,
                        evidence_coverage_score,
                        request_json,
                        response_json
                    FROM runs
                    WHERE id = ?
                    """,
                    (run_id,),
                ).fetchone()

                if row is None:
                    return None

                raw_rows = conn.execute(
                    """
                    SELECT
                        source_id,
                        query_label,
                        source_type,
                        source_title,
                        source_url,
                        snippet,
                        cleaned_text,
                        fetched,
                        trust_weight
                    FROM raw_sources
                    WHERE run_id = ?
                    ORDER BY source_id ASC
                    """,
                    (run_id,),
                ).fetchall()

                structured_rows = conn.execute(
                    """
                    SELECT
                        evidence_id,
                        source_id,
                        source_type,
                        entity,
                        fact_type,
                        value,
                        excerpt,
                        url,
                        confidence
                    FROM structured_evidence
                    WHERE run_id = ?
                    ORDER BY evidence_id ASC
                    """,
                    (run_id,),
                ).fetchall()

        return {
            "id": row[0],
            "created_at": row[1],
            "endpoint": row[2],
            "research_stage": row[3],
            "overall_verdict": row[4],
            "confidence_score": row[5],
            "evidence_coverage_score": row[6],
            "request": self._safe_json_load(row[7]),
            "response": self._safe_json_load(row[8]),
            "raw_sources": [
                {
                    "id": source[0],
                    "query_label": source[1],
                    "source_type": source[2],
                    "source_title": source[3],
                    "source_url": source[4],
                    "snippet": source[5],
                    "cleaned_text": source[6],
                    "fetched": bool(source[7]),
                    "trust_weight": source[8],
                }
                for source in raw_rows
            ],
            "structured_evidence": [
                {
                    "id": item[0],
                    "source_id": item[1],
                    "source_type": item[2],
                    "entity": item[3],
                    "fact_type": item[4],
                    "value": item[5],
                    "excerpt": item[6],
                    "url": item[7],
                    "confidence": item[8],
                }
                for item in structured_rows
            ],
        }

    def _insert_raw_sources(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        raw_sources: list[dict[str, Any]],
    ) -> None:
        for source in raw_sources:
            conn.execute(
                """
                INSERT INTO raw_sources (
                    run_id,
                    source_id,
                    query_label,
                    source_type,
                    source_title,
                    source_url,
                    snippet,
                    cleaned_text,
                    fetched,
                    trust_weight
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    source.get("id"),
                    source.get("query_label"),
                    source.get("source_type"),
                    source.get("source_title"),
                    source.get("source_url"),
                    source.get("snippet"),
                    source.get("cleaned_text"),
                    1 if source.get("fetched") else 0,
                    source.get("trust_weight"),
                ),
            )

    def _insert_structured_evidence(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        structured_evidence: list[dict[str, Any]],
    ) -> None:
        for evidence in structured_evidence:
            conn.execute(
                """
                INSERT INTO structured_evidence (
                    run_id,
                    evidence_id,
                    source_id,
                    source_type,
                    entity,
                    fact_type,
                    value,
                    excerpt,
                    url,
                    confidence
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    evidence.get("id"),
                    evidence.get("source_id"),
                    evidence.get("source_type"),
                    evidence.get("entity"),
                    evidence.get("fact_type"),
                    evidence.get("value"),
                    evidence.get("excerpt"),
                    evidence.get("url"),
                    evidence.get("confidence"),
                ),
            )

    def _ensure_parent_dir(self) -> None:
        if self._db_path.parent and not self._db_path.parent.exists():
            self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def _initialize_schema(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.executescript(
                    """
                    PRAGMA journal_mode=WAL;

                    CREATE TABLE IF NOT EXISTS runs (
                        id TEXT PRIMARY KEY,
                        created_at TEXT NOT NULL,
                        endpoint TEXT NOT NULL,
                        research_stage TEXT,
                        overall_verdict TEXT,
                        confidence_score REAL,
                        evidence_coverage_score REAL,
                        request_json TEXT NOT NULL,
                        response_json TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS raw_sources (
                        run_id TEXT NOT NULL,
                        source_id TEXT NOT NULL,
                        query_label TEXT,
                        source_type TEXT,
                        source_title TEXT,
                        source_url TEXT,
                        snippet TEXT,
                        cleaned_text TEXT,
                        fetched INTEGER,
                        trust_weight REAL,
                        PRIMARY KEY (run_id, source_id),
                        FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
                    );

                    CREATE TABLE IF NOT EXISTS structured_evidence (
                        run_id TEXT NOT NULL,
                        evidence_id TEXT NOT NULL,
                        source_id TEXT,
                        source_type TEXT,
                        entity TEXT,
                        fact_type TEXT,
                        value TEXT,
                        excerpt TEXT,
                        url TEXT,
                        confidence REAL,
                        PRIMARY KEY (run_id, evidence_id),
                        FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
                    );

                    CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_raw_sources_type ON raw_sources(source_type);
                    CREATE INDEX IF NOT EXISTS idx_structured_evidence_fact ON structured_evidence(fact_type);
                    """
                )
                conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _safe_json_load(self, raw: str) -> Any:
        try:
            return json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None


class PipelineRepository:
    """SQLite-backed storage for the brisket sales pipeline."""

    def __init__(self, db_path: str | None = None) -> None:
        configured_path = db_path or os.getenv("RESEARCH_DB_PATH", ".data/research_runs.db")
        self._db_path = Path(configured_path)
        self._lock = threading.Lock()
        self._ensure_parent_dir()
        self._initialize_schema()

    @property
    def db_path(self) -> Path:
        return self._db_path

    # ------------------------------------------------------------------
    # Leads
    # ------------------------------------------------------------------

    def save_lead(self, lead: LeadRecord) -> str:
        lead_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc).isoformat()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO leads (
                        id, created_at, name, website, phone, email,
                        location, category, menu_url,
                        source_urls_json, evidence_snippets_json,
                        demand_signals_json, pipeline_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        lead_id, created_at, lead.name, lead.website,
                        lead.phone, lead.email, lead.location,
                        lead.category, lead.menu_url,
                        json.dumps(lead.source_urls),
                        json.dumps(lead.evidence_snippets),
                        json.dumps(lead.demand_signals),
                        "discovered",
                    ),
                )
                conn.commit()
        return lead_id

    def get_lead(self, lead_id: str) -> dict[str, Any] | None:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM leads WHERE id = ?", (lead_id,)
                ).fetchone()
                if row is None:
                    return None
                return self._lead_row_to_dict(row)

    def list_leads(self, status_filter: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            with self._connect() as conn:
                if status_filter:
                    rows = conn.execute(
                        "SELECT * FROM leads WHERE pipeline_status = ? ORDER BY created_at DESC",
                        (status_filter,),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM leads ORDER BY created_at DESC"
                    ).fetchall()
                return [self._lead_row_to_dict(r) for r in rows]

    def update_lead_status(self, lead_id: str, status: str) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE leads SET pipeline_status = ? WHERE id = ?",
                    (status, lead_id),
                )
                conn.commit()

    def list_lead_summaries(self, status_filter: str | None = None) -> list[LeadSummary]:
        leads = self.list_leads(status_filter)
        summaries: list[LeadSummary] = []
        with self._lock:
            with self._connect() as conn:
                for lead in leads:
                    lead_id = lead["id"]
                    score_row = conn.execute(
                        "SELECT status, probability_buy FROM lead_scores WHERE lead_id = ? ORDER BY created_at DESC LIMIT 1",
                        (lead_id,),
                    ).fetchone()
                    sent_row = conn.execute(
                        "SELECT id FROM outreach_drafts WHERE lead_id = ? AND sent_at IS NOT NULL LIMIT 1",
                        (lead_id,),
                    ).fetchone()
                    reply_row = conn.execute(
                        "SELECT intent FROM reply_tracking WHERE lead_id = ? ORDER BY received_at DESC LIMIT 1",
                        (lead_id,),
                    ).fetchone()
                    summaries.append(LeadSummary(
                        id=lead_id,
                        name=lead["name"],
                        location=lead.get("location"),
                        email=lead.get("email"),
                        phone=lead.get("phone"),
                        pipeline_status=lead["pipeline_status"],
                        score_status=score_row[0] if score_row else None,
                        probability_buy=score_row[1] if score_row else None,
                        outreach_sent=sent_row is not None,
                        reply_intent=reply_row[0] if reply_row else None,
                        created_at=lead["created_at"],
                    ))
        return summaries

    # ------------------------------------------------------------------
    # Lead scores
    # ------------------------------------------------------------------

    def save_lead_score(self, lead_id: str, score: LeadScore) -> str:
        score_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc).isoformat()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO lead_scores (
                        id, lead_id, created_at,
                        probability_buy, estimated_volume_potential,
                        geographic_fit, pricing_tier_fit,
                        catering_event_potential, contactability,
                        confidence, status, rationale
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        score_id, lead_id, created_at,
                        score.probability_buy, score.estimated_volume_potential,
                        score.geographic_fit, score.pricing_tier_fit,
                        score.catering_event_potential, score.contactability,
                        score.confidence, score.status, score.rationale,
                    ),
                )
                conn.commit()
        return score_id

    def get_latest_score(self, lead_id: str) -> dict[str, Any] | None:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM lead_scores WHERE lead_id = ? ORDER BY created_at DESC LIMIT 1",
                    (lead_id,),
                ).fetchone()
                if row is None:
                    return None
                cols = [d[0] for d in conn.execute("SELECT * FROM lead_scores LIMIT 0").description]
                return dict(zip(cols, row))

    # ------------------------------------------------------------------
    # Outreach drafts
    # ------------------------------------------------------------------

    def save_outreach_draft(self, lead_id: str, draft: OutreachDraft) -> str:
        draft_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc).isoformat()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO outreach_drafts (
                        id, lead_id, created_at, intro, why_selected,
                        brisket_relevance, offer, cta,
                        first_email, follow_up_1, follow_up_2,
                        personalization_lines_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        draft_id, lead_id, created_at,
                        draft.intro, draft.why_selected,
                        draft.brisket_relevance, draft.offer, draft.cta,
                        draft.first_email, draft.follow_up_1, draft.follow_up_2,
                        json.dumps([p.model_dump() for p in draft.personalization_lines]),
                    ),
                )
                conn.commit()
        return draft_id

    def mark_outreach_sent(self, draft_id: str, sent_at: str, message_id: str) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE outreach_drafts SET sent_at = ?, sent_message_id = ? WHERE id = ?",
                    (sent_at, message_id, draft_id),
                )
                conn.commit()

    def get_latest_draft(self, lead_id: str) -> dict[str, Any] | None:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM outreach_drafts WHERE lead_id = ? ORDER BY created_at DESC LIMIT 1",
                    (lead_id,),
                ).fetchone()
                if row is None:
                    return None
                cols = [d[0] for d in conn.execute("SELECT * FROM outreach_drafts LIMIT 0").description]
                return dict(zip(cols, row))

    def get_all_sent_message_ids(self) -> set[str]:
        """Return all non-null sent_message_id values for reply matching."""
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT sent_message_id FROM outreach_drafts WHERE sent_message_id IS NOT NULL"
                ).fetchall()
                return {r[0] for r in rows}

    def get_draft_by_message_id(self, message_id: str) -> dict[str, Any] | None:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM outreach_drafts WHERE sent_message_id = ?",
                    (message_id,),
                ).fetchone()
                if row is None:
                    return None
                cols = [d[0] for d in conn.execute("SELECT * FROM outreach_drafts LIMIT 0").description]
                return dict(zip(cols, row))

    # ------------------------------------------------------------------
    # Reply tracking
    # ------------------------------------------------------------------

    def save_reply(
        self,
        lead_id: str,
        entry: ReplyTrackingEntry,
        raw_reply_text: str,
        received_at: str | None = None,
    ) -> str:
        reply_id = str(uuid.uuid4())
        ts = received_at or datetime.now(timezone.utc).isoformat()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO reply_tracking (
                        id, lead_id, received_at, raw_reply_text,
                        intent, company_status, thread_summary, follow_up_task
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        reply_id, lead_id, ts, raw_reply_text,
                        entry.intent, entry.company_status,
                        entry.thread_summary, entry.follow_up_task,
                    ),
                )
                conn.commit()
        return reply_id

    def get_replies(self, lead_id: str) -> list[dict[str, Any]]:
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM reply_tracking WHERE lead_id = ? ORDER BY received_at ASC",
                    (lead_id,),
                ).fetchall()
                cols = [d[0] for d in conn.execute("SELECT * FROM reply_tracking LIMIT 0").description]
                return [dict(zip(cols, r)) for r in rows]

    # ------------------------------------------------------------------
    # Call sheets
    # ------------------------------------------------------------------

    def save_call_sheet(self, lead_id: str, sheet: CallSheet) -> str:
        sheet_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc).isoformat()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO call_sheets (
                        id, lead_id, created_at, company_summary,
                        prior_emails_json, talking_points_json,
                        objections_json, next_step_suggestions_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        sheet_id, lead_id, created_at, sheet.company_summary,
                        json.dumps(sheet.prior_emails),
                        json.dumps(sheet.talking_points),
                        json.dumps(sheet.objections),
                        json.dumps(sheet.next_step_suggestions),
                    ),
                )
                conn.commit()
        return sheet_id

    def get_call_sheet(self, lead_id: str) -> dict[str, Any] | None:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM call_sheets WHERE lead_id = ? ORDER BY created_at DESC LIMIT 1",
                    (lead_id,),
                ).fetchone()
                if row is None:
                    return None
                cols = [d[0] for d in conn.execute("SELECT * FROM call_sheets LIMIT 0").description]
                d = dict(zip(cols, row))
                for key in ("prior_emails_json", "talking_points_json", "objections_json", "next_step_suggestions_json"):
                    d[key] = self._safe_json_load(d.get(key) or "[]") or []
                notes = conn.execute(
                    "SELECT id, created_at, note, author FROM call_notes WHERE lead_id = ? ORDER BY created_at ASC",
                    (lead_id,),
                ).fetchall()
                d["notes"] = [{"id": n[0], "created_at": n[1], "note": n[2], "author": n[3]} for n in notes]
                return d

    # ------------------------------------------------------------------
    # Call notes
    # ------------------------------------------------------------------

    def add_call_note(self, lead_id: str, note: str, author: str | None = None) -> str:
        note_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc).isoformat()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "INSERT INTO call_notes (id, lead_id, created_at, note, author) VALUES (?, ?, ?, ?, ?)",
                    (note_id, lead_id, created_at, note, author),
                )
                conn.commit()
        return note_id

    # ------------------------------------------------------------------
    # Pipeline state (e.g. last IMAP UID)
    # ------------------------------------------------------------------

    def get_state(self, key: str) -> str | None:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT value FROM pipeline_state WHERE key = ?", (key,)
                ).fetchone()
                return row[0] if row else None

    def set_state(self, key: str, value: str) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "INSERT INTO pipeline_state (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (key, value),
                )
                conn.commit()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _lead_row_to_dict(self, row: tuple[Any, ...]) -> dict[str, Any]:
        cols = ("id", "created_at", "name", "website", "phone", "email",
                "location", "category", "menu_url",
                "source_urls_json", "evidence_snippets_json",
                "demand_signals_json", "pipeline_status")
        d = dict(zip(cols, row))
        for key in ("source_urls_json", "evidence_snippets_json", "demand_signals_json"):
            d[key] = self._safe_json_load(d.get(key) or "[]") or []
        return d

    def _ensure_parent_dir(self) -> None:
        if self._db_path.parent and not self._db_path.parent.exists():
            self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def _initialize_schema(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.executescript(
                    """
                    PRAGMA journal_mode=WAL;

                    CREATE TABLE IF NOT EXISTS leads (
                        id TEXT PRIMARY KEY,
                        created_at TEXT NOT NULL,
                        name TEXT NOT NULL,
                        website TEXT,
                        phone TEXT,
                        email TEXT,
                        location TEXT,
                        category TEXT,
                        menu_url TEXT,
                        source_urls_json TEXT,
                        evidence_snippets_json TEXT,
                        demand_signals_json TEXT,
                        pipeline_status TEXT NOT NULL DEFAULT 'discovered'
                    );

                    CREATE TABLE IF NOT EXISTS lead_scores (
                        id TEXT PRIMARY KEY,
                        lead_id TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        probability_buy REAL,
                        estimated_volume_potential REAL,
                        geographic_fit REAL,
                        pricing_tier_fit REAL,
                        catering_event_potential REAL,
                        contactability REAL,
                        confidence REAL,
                        status TEXT,
                        rationale TEXT,
                        FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
                    );

                    CREATE TABLE IF NOT EXISTS outreach_drafts (
                        id TEXT PRIMARY KEY,
                        lead_id TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        sent_at TEXT,
                        sent_message_id TEXT,
                        intro TEXT,
                        why_selected TEXT,
                        brisket_relevance TEXT,
                        offer TEXT,
                        cta TEXT,
                        first_email TEXT,
                        follow_up_1 TEXT,
                        follow_up_2 TEXT,
                        personalization_lines_json TEXT,
                        FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
                    );

                    CREATE TABLE IF NOT EXISTS reply_tracking (
                        id TEXT PRIMARY KEY,
                        lead_id TEXT NOT NULL,
                        received_at TEXT NOT NULL,
                        raw_reply_text TEXT,
                        intent TEXT,
                        company_status TEXT,
                        thread_summary TEXT,
                        follow_up_task TEXT,
                        FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
                    );

                    CREATE TABLE IF NOT EXISTS call_sheets (
                        id TEXT PRIMARY KEY,
                        lead_id TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        company_summary TEXT,
                        prior_emails_json TEXT,
                        talking_points_json TEXT,
                        objections_json TEXT,
                        next_step_suggestions_json TEXT,
                        FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
                    );

                    CREATE TABLE IF NOT EXISTS call_notes (
                        id TEXT PRIMARY KEY,
                        lead_id TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        note TEXT NOT NULL,
                        author TEXT
                    );

                    CREATE TABLE IF NOT EXISTS pipeline_state (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(pipeline_status);
                    CREATE INDEX IF NOT EXISTS idx_lead_scores_lead ON lead_scores(lead_id);
                    CREATE INDEX IF NOT EXISTS idx_outreach_lead ON outreach_drafts(lead_id);
                    CREATE INDEX IF NOT EXISTS idx_reply_lead ON reply_tracking(lead_id);
                    CREATE INDEX IF NOT EXISTS idx_call_sheets_lead ON call_sheets(lead_id);
                    CREATE INDEX IF NOT EXISTS idx_call_notes_lead ON call_notes(lead_id);
                    """
                )
                conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _safe_json_load(self, raw: str) -> Any:
        try:
            return json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

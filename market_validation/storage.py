from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import ValidationResponse


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

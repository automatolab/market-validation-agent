from __future__ import annotations

import sqlite3
from pathlib import Path

from market_validation.db_store import add_call_note, list_call_notes
from market_validation.output_store import persist_stage_result


def _query_one(conn: sqlite3.Connection, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return int(row[0]) if row else 0


def test_persist_stage_result_writes_database_state(tmp_path: Path) -> None:
    payload = {
        "result": "ok",
        "stage": "research_ingest",
        "run_id": "brisket-db-001",
        "market": "Brisket",
        "companies": [
            {
                "company_id": "db-smoke-1",
                "company_name": "DB Smoke House",
                "website": "https://db-smoke.example.com",
                "location": "Austin, TX",
                "source_records": [
                    {
                        "source_id": "google",
                        "url": "https://example.com/db-smoke-1",
                        "fetched_at": "2026-04-09T00:00:00Z",
                        "excerpt": "Known for sliced brisket",
                    }
                ],
            }
        ],
        "warnings": [],
        "errors": [],
        "failure_mode": "none",
    }

    result = persist_stage_result(payload=payload, root=tmp_path)

    db_file = tmp_path / result["database_file"]
    assert db_file.exists()

    with sqlite3.connect(db_file) as conn:
        assert _query_one(conn, "SELECT COUNT(*) FROM stage_events") == 1
        assert _query_one(conn, "SELECT COUNT(*) FROM leads") == 1
        assert _query_one(conn, "SELECT COUNT(*) FROM lead_source_records") == 1


def test_add_and_list_call_notes(tmp_path: Path) -> None:
    add_result = add_call_note(
        company_id="db-smoke-2",
        author="caller-a",
        note="Purchasing manager asked for pricing sheet and Friday callback.",
        root=tmp_path,
        next_action="Call back Friday afternoon",
    )

    assert add_result["result"] == "ok"
    assert add_result["note_id"] > 0

    list_result = list_call_notes(root=tmp_path, company_id="db-smoke-2", limit=10)
    assert list_result["result"] == "ok"
    assert list_result["count"] == 1
    note = list_result["notes"][0]
    assert note["company_id"] == "db-smoke-2"
    assert note["author"] == "caller-a"

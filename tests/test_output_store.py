from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_validation.output_store import persist_stage_result


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        payload = json.loads(text)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def test_persist_stage_result_writes_run_payload_and_lead_state(tmp_path: Path) -> None:
    payload = {
        "result": "ok",
        "stage": "research_ingest",
        "run_id": "brisket-001",
        "market": "Brisket",
        "companies": [
            {
                "company_id": "smoke-house-1",
                "company_name": "Smoke House",
                "website": "https://smoke.example.com",
                "location": "Austin, TX",
                "source_records": [
                    {
                        "source_id": "yelp",
                        "url": "https://example.com/yelp/smoke-house",
                        "fetched_at": "2026-04-09T00:00:00Z",
                        "excerpt": "Known for brisket sandwiches",
                    }
                ],
            }
        ],
        "warnings": [],
        "errors": [],
        "failure_mode": "none",
    }

    result = persist_stage_result(payload=payload, root=tmp_path)

    stage_file = tmp_path / result["stage_file"]
    assert stage_file.exists()

    leads_file = tmp_path / "output" / "leads" / "leads.jsonl"
    leads = _read_jsonl(leads_file)
    assert len(leads) == 1
    lead = leads[0]
    assert lead["company_id"] == "smoke-house-1"
    assert lead["company_name"] == "Smoke House"
    assert lead["status"] == "new"
    assert lead["last_stage"] == "research_ingest"
    assert lead["source_links"] == ["https://example.com/yelp/smoke-house"]


def test_persist_stage_result_rejects_qualified_without_evidence_links(tmp_path: Path) -> None:
    payload = {
        "result": "ok",
        "stage": "lead_qualify",
        "run_id": "brisket-002",
        "market": "Brisket",
        "qualified_companies": [
            {
                "company_id": "smoke-house-2",
                "company_name": "Smoke House Two",
                "status": "qualified",
                "qualification": "qualified",
                "confidence": 0.91,
                "estimated_monthly_volume": {"value": 400, "unit": "lb", "basis": "menu density"},
                "claims": [
                    {
                        "claim": "Likely buys brisket weekly",
                        "evidence_links": [],
                        "evidence_excerpt": "Brisket-heavy menu",
                    }
                ],
                "notes": "Missing evidence URL should fail",
            }
        ],
        "warnings": [],
        "errors": [],
        "failure_mode": "none",
    }

    with pytest.raises(ValueError, match="missing evidence link"):
        persist_stage_result(payload=payload, root=tmp_path)


def test_persist_stage_result_builds_markdown_views_from_pipeline_updates(tmp_path: Path) -> None:
    run_id = "brisket-003"

    persist_stage_result(
        payload={
            "result": "ok",
            "stage": "research_ingest",
            "run_id": run_id,
            "market": "Brisket",
            "companies": [
                {
                    "company_id": "smoke-house-3",
                    "company_name": "Smoke House Three",
                    "website": "https://smoke-three.example.com",
                    "location": "Houston, TX",
                    "source_records": [
                        {
                            "source_id": "google",
                            "url": "https://example.com/google/smoke-house-three",
                            "fetched_at": "2026-04-09T00:00:00Z",
                            "excerpt": "Popular brisket plate",
                        }
                    ],
                }
            ],
            "warnings": [],
            "errors": [],
            "failure_mode": "none",
        },
        root=tmp_path,
    )

    persist_stage_result(
        payload={
            "result": "ok",
            "stage": "lead_qualify",
            "run_id": run_id,
            "market": "Brisket",
            "qualified_companies": [
                {
                    "company_id": "smoke-house-3",
                    "company_name": "Smoke House Three",
                    "status": "qualified",
                    "qualification": "qualified",
                    "confidence": 0.87,
                    "estimated_monthly_volume": {"value": 550, "unit": "lb", "basis": "menu mentions"},
                    "claims": [
                        {
                            "claim": "Menu shows multiple brisket dishes",
                            "evidence_links": ["https://example.com/menu/smoke-house-three"],
                            "evidence_excerpt": "Brisket tacos, sliced brisket, brisket sandwich",
                        }
                    ],
                    "notes": "High brisket signal",
                }
            ],
            "warnings": [],
            "errors": [],
            "failure_mode": "none",
        },
        root=tmp_path,
    )

    persist_stage_result(
        payload={
            "result": "ok",
            "stage": "outreach_email",
            "run_id": run_id,
            "drafts": [
                {
                    "company_id": "smoke-house-3",
                    "status": "emailed",
                    "subject": "Quick brisket supply question",
                    "body": "Would you be open to discussing brisket sourcing options?",
                    "template_id": "default-brisket-v1",
                    "quality_checks": {
                        "has_clear_ask": True,
                        "has_personalization": True,
                        "has_opt_out": True,
                        "mentions_evidence_context": True,
                    },
                }
            ],
            "warnings": [],
            "errors": [],
            "failure_mode": "none",
        },
        root=tmp_path,
    )

    persist_stage_result(
        payload={
            "result": "ok",
            "stage": "reply_parse",
            "run_id": run_id,
            "updates": [
                {
                    "message_id": "msg-1",
                    "company_id": "smoke-house-3",
                    "status": "replied_interested",
                    "intent": "interested",
                    "summary": "Asked for prices and delivery timing",
                    "structured_fields": {
                        "requested_follow_up": True,
                        "requested_sample": False,
                        "budget_signal": "open",
                        "timeframe_signal": "this month",
                        "contact_preference": "phone",
                    },
                }
            ],
            "warnings": [],
            "errors": [],
            "failure_mode": "none",
        },
        root=tmp_path,
    )

    result = persist_stage_result(
        payload={
            "result": "ok",
            "stage": "call_sheet_build",
            "run_id": run_id,
            "call_sheet": [
                {
                    "company_id": "smoke-house-3",
                    "company_name": "Smoke House Three",
                    "status": "call_ready",
                    "priority_score": 95,
                    "priority_tier": "P1",
                    "why_now": "Direct interest and active follow-up request",
                    "next_action": "Call purchasing manager within 24h",
                    "notes_for_caller": "Lead requested pricing and logistics details",
                }
            ],
            "warnings": [],
            "errors": [],
            "failure_mode": "none",
        },
        root=tmp_path,
    )

    leads = _read_jsonl(tmp_path / result["leads_file"])
    assert len(leads) == 1
    lead = leads[0]
    assert lead["status"] == "call_ready"
    assert lead["priority_tier"] == "P1"
    assert lead["priority_score"] == 95
    assert lead["next_action"] == "Call purchasing manager within 24h"
    assert lead["evidence_links"] == ["https://example.com/menu/smoke-house-three"]

    call_sheet_text = (tmp_path / result["call_sheet_file"]).read_text(encoding="utf-8")
    assert "Smoke House Three" in call_sheet_text
    assert "call_ready" in call_sheet_text

    dashboard_text = (tmp_path / result["dashboard_file"]).read_text(encoding="utf-8")
    assert "| call_ready | 1 |" in dashboard_text
    assert "Priority Queue" in dashboard_text


def test_persist_worker_result_stage_updates_lead_state(tmp_path: Path) -> None:
    payload = {
        "result": "ok",
        "stage": "worker_result",
        "run_id": "worker-001",
        "status": "completed",
        "id": "42",
        "report_num": "007",
        "market": "Brisket market",
        "target_customer": "Smoke House 42",
        "score": 6.2,
        "report": "reports/007-brisket-market-2026-04-09.md",
        "error": None,
    }

    result = persist_stage_result(payload=payload, root=tmp_path)
    leads = _read_jsonl(tmp_path / result["leads_file"])
    assert len(leads) == 1

    lead = leads[0]
    assert lead["company_id"] == "42"
    assert lead["company_name"] == "Smoke House 42"
    assert lead["market"] == "Brisket market"
    assert lead["status"] == "validated"
    assert lead["last_stage"] == "worker_result"
    assert lead["score"] == 6.2
    assert lead["report"] == "reports/007-brisket-market-2026-04-09.md"

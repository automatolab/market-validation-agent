from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pytest

from market_validation import batch_worker as worker


def _args(tmp_path: Path, **overrides: object) -> argparse.Namespace:
    payload: dict[str, object] = {
        "id": "10",
        "market": "AI QA agent",
        "geography": "US",
        "profile": "saas",
        "template": None,
        "report_num": "001",
        "date": "2026-04-09",
        "root": str(tmp_path),
        "model": None,
        "agent": None,
    }
    payload.update(overrides)
    return argparse.Namespace(**payload)


def test_extract_first_json_object_handles_fenced_json() -> None:
    raw = "status line\n```json\n{\"status\": \"validated\", \"score\": 7.2}\n```\n"
    parsed = worker._extract_first_json_object(raw)
    assert parsed == {"status": "validated", "score": 7.2}


def test_run_worker_writes_report_and_tracker_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    model_payload = {
        "target_customer": "Small CNC shops",
        "status": "qualified",
        "score": 7.42,
        "verdict": "Strong initial signal",
        "notes": "clear pain + willingness to pay",
        "report_markdown": "# Report\n\nLooks good.",
    }

    monkeypatch.setattr(worker, "invoke_opencode", lambda **_: model_payload)
    result = worker.run_worker(_args(tmp_path))

    assert result["status"] == "completed"
    assert result["report"] == "reports/001-ai-qa-agent-2026-04-09.md"
    assert (tmp_path / "reports" / "001-ai-qa-agent-2026-04-09.md").read_text(encoding="utf-8") == "# Report\n\nLooks good.\n"

    tracker_row = (tmp_path / "batch" / "tracker-additions" / "10.tsv").read_text(encoding="utf-8")
    assert tracker_row == (
        "1\t2026-04-09\tAI QA agent\tSmall CNC shops\tqualified\t7.42/10\t"
        "[001](reports/001-ai-qa-agent-2026-04-09.md)\tclear pain + willingness to pay\n"
    )


def test_run_worker_uses_fallbacks_for_missing_or_invalid_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    model_payload = {
        "target_customer": "",
        "status": "invalid",
        "score": "not-a-number",
        "verdict": "",
        "notes": "",
        "report_markdown": "",
    }

    monkeypatch.setattr(worker, "invoke_opencode", lambda **_: model_payload)
    result = worker.run_worker(_args(tmp_path, market="Vision inventory tracker", report_num="123"))

    assert result["target_customer"] == "Unknown target customer"
    assert result["score"] is None
    assert result["report"] == "reports/123-vision-inventory-tracker-2026-04-09.md"

    tracker_row = (tmp_path / "batch" / "tracker-additions" / "10.tsv").read_text(encoding="utf-8")
    assert "\trejected\tN/A\t" in tracker_row
    assert tracker_row.endswith("\trejected\n")

    report_text = (tmp_path / "reports" / "123-vision-inventory-tracker-2026-04-09.md").read_text(encoding="utf-8")
    assert "# Market Validation: Vision inventory tracker" in report_text
    assert "**Target Customer:** Unknown target customer" in report_text


def test_main_exits_nonzero_and_emits_failed_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def _boom(_: argparse.Namespace) -> dict[str, object]:
        raise RuntimeError("opencode unavailable")

    monkeypatch.setattr(worker, "run_worker", _boom)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "batch_worker.py",
            "--id",
            "55",
            "--market",
            "test market",
            "--report-num",
            "001",
            "--root",
            str(tmp_path),
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        worker.main()

    assert exc_info.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "failed"
    assert payload["id"] == "55"
    assert payload["market"] == "test market"
    assert "opencode unavailable" in payload["error"]


def test_run_worker_accepts_replied_interested_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    model_payload = {
        "target_customer": "Restaurant owner",
        "status": "replied_interested",
        "score": 8.1,
        "verdict": "Asked for follow-up call",
        "notes": "requested pricing discussion",
        "report_markdown": "# Reply\n\nInterested.",
    }

    monkeypatch.setattr(worker, "invoke_opencode", lambda **_: model_payload)
    result = worker.run_worker(_args(tmp_path, market="Brisket suppliers", report_num="002"))

    assert result["status"] == "completed"
    tracker_row = (tmp_path / "batch" / "tracker-additions" / "10.tsv").read_text(encoding="utf-8")
    assert "\treplied_interested\t" in tracker_row

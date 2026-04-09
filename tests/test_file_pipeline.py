from __future__ import annotations

from pathlib import Path

from market_validation.file_pipeline import (
    merge_tracker_additions,
    verify_pipeline,
)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_merge_tracker_additions_adds_and_updates_rows(tmp_path: Path) -> None:
    _write(
        tmp_path / "data" / "validation-tracker.md",
        "# Market Validation Tracker\n\n"
        "| # | Date | Market | Target Customer | Status | Score | Report | Notes |\n"
        "|---|------|--------|-----------------|--------|-------|--------|-------|\n"
        "| 1 | 2026-04-09 | AI QA agent | Small CNC shops | scanning | 5.20/10 | [001](reports/001-ai-qa-agent-2026-04-09.md) | initial pass |\n",
    )
    _write(tmp_path / "reports" / "001-ai-qa-agent-2026-04-09.md", "# old")

    add_dir = tmp_path / "batch" / "tracker-additions"
    _write(
        add_dir / "10.tsv",
        "1\t2026-04-10\tAI QA agent\tSmall CNC shops\tvalidated\t7.40/10\t[001](reports/001-ai-qa-agent-2026-04-10.md)\tstronger evidence\n",
    )
    _write(
        add_dir / "11.tsv",
        "2\t2026-04-10\tVision inventory tracker\tLab operations teams\tvalidated\t6.80/10\t[002](reports/002-vision-inventory-2026-04-10.md)\tnew idea\n",
    )

    result = merge_tracker_additions(tmp_path)

    assert result.processed_files == 2
    assert result.updated == 1
    assert result.added == 1

    tracker_text = (tmp_path / "data" / "validation-tracker.md").read_text(encoding="utf-8")
    assert "| 1 | 2026-04-10 | AI QA agent | Small CNC shops | validated | 7.40/10" in tracker_text
    assert "| 2 | 2026-04-10 | Vision inventory tracker | Lab operations teams | validated | 6.80/10" in tracker_text

    assert (tmp_path / "batch" / "tracker-additions" / "merged" / "10.tsv").exists()
    assert (tmp_path / "batch" / "tracker-additions" / "merged" / "11.tsv").exists()


def test_verify_pipeline_detects_missing_report_and_pending_additions(tmp_path: Path) -> None:
    _write(
        tmp_path / "data" / "validation-tracker.md",
        "# Market Validation Tracker\n\n"
        "| # | Date | Market | Target Customer | Status | Score | Report | Notes |\n"
        "|---|------|--------|-----------------|--------|-------|--------|-------|\n"
        "| 1 | 2026-04-10 | AI QA agent | Small CNC shops | validated | 7.10/10 | [001](reports/missing-file.md) | check |\n",
    )
    _write(
        tmp_path / "batch" / "tracker-additions" / "99.tsv",
        "99\t2026-04-10\tX\tY\tnew\tN/A\t[099](reports/099.md)\tpending\n",
    )

    result = verify_pipeline(tmp_path)

    assert result.errors
    assert any("report link missing" in item for item in result.errors)
    assert result.warnings
    assert any("pending tracker additions" in item for item in result.warnings)


def test_verify_pipeline_accepts_lead_pipeline_statuses(tmp_path: Path) -> None:
    _write(
        tmp_path / "data" / "validation-tracker.md",
        "# Market Validation Tracker\n\n"
        "| # | Date | Market | Target Customer | Status | Score | Report | Notes |\n"
        "|---|------|--------|-----------------|--------|-------|--------|-------|\n"
        "| 1 | 2026-04-10 | Brisket supply | Restaurant owner | call_ready | N/A | [001](reports/001-brisket-2026-04-10.md) | queued for sales call |\n",
    )
    _write(tmp_path / "reports" / "001-brisket-2026-04-10.md", "# report")

    result = verify_pipeline(tmp_path)

    assert result.errors == []

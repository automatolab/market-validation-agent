from __future__ import annotations

import json
from pathlib import Path

from market_validation import lead_pipeline


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _base_config() -> dict[str, object]:
    return {
        "market": "Brisket",
        "geography": "US",
        "target_product": "brisket",
        "max_companies": 100,
        "source_configs": [
            {
                "source_id": "google-brisket-us",
                "source_type": "search",
                "provider": "duckduckgo",
                "query": "brisket restaurant",
                "region": "US",
                "enabled": True,
                "auth_env": "TEST_SOURCE_API_KEY",
            }
        ],
        "email_template": {
            "template_id": "brisket-v1",
            "subject_template": "Subject {{company_name}}",
            "body_template": "Body {{company_name}}",
            "tone": "professional",
        },
        "default_model": "provider/model",
        "default_agent": "general",
    }


def _write_required_modes(root: Path) -> None:
    _write(root / "modes" / "_shared.md", "# shared\n")
    _write(root / "modes" / "research-ingest.md", "# stage\n")
    _write(root / "modes" / "lead-qualify.md", "# stage\n")
    _write(root / "modes" / "outreach-email.md", "# stage\n")
    _write(root / "modes" / "reply-parse.md", "# stage\n")
    _write(root / "modes" / "call-sheet-build.md", "# stage\n")


def test_validate_config_success(tmp_path: Path) -> None:
    _write_required_modes(tmp_path)
    result = lead_pipeline.validate_config(_base_config(), tmp_path)
    assert result.errors == []
    assert result.warnings == []


def test_validate_config_detects_missing_fields(tmp_path: Path) -> None:
    _write_required_modes(tmp_path)
    bad = _base_config()
    bad["source_configs"] = []
    bad["email_template"] = {}
    bad["market"] = ""
    result = lead_pipeline.validate_config(bad, tmp_path)
    assert result.errors
    assert any("source_configs" in item for item in result.errors)


def test_validate_config_rejects_bad_source_type_and_duplicate_id(tmp_path: Path) -> None:
    _write_required_modes(tmp_path)
    config = _base_config()
    config["source_configs"] = [
        {
            "source_id": "dup-id",
            "source_type": "search",
            "query": "a",
            "region": "US",
            "enabled": True,
        },
        {
            "source_id": "dup-id",
            "source_type": "unknown_type",
            "query": "b",
            "region": "US",
            "enabled": True,
        },
    ]

    result = lead_pipeline.validate_config(config, tmp_path)
    assert any("Duplicate source_id" in item for item in result.errors)
    assert any("source_type must be one of" in item for item in result.errors)


def test_validate_config_allows_free_sources(tmp_path: Path) -> None:
    _write_required_modes(tmp_path)
    config = _base_config()
    config["source_configs"] = [
        {
            "source_id": "yelp-free",
            "source_type": "directory",
            "provider": "yelp",
            "query": "brisket",
            "region": "US",
            "enabled": True,
        },
        {
            "source_id": "duckduckgo-free",
            "source_type": "search",
            "provider": "duckduckgo",
            "query": "BBQ restaurant Texas",
            "region": "US",
            "enabled": True,
        }
    ]

    result = lead_pipeline.validate_config(config, tmp_path)
    assert result.errors == []


def test_load_config_uses_root_relative_and_fallback(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    _write(config_dir / "lead-pipeline.example.json", json.dumps(_base_config(), ensure_ascii=True))

    payload, warnings, resolved = lead_pipeline.load_config(Path("config/lead-pipeline.json"), root=tmp_path)
    assert payload["market"] == "Brisket"
    assert warnings
    assert str(resolved).endswith("lead-pipeline.example.json")


def test_build_stage_payload_call_sheet_build_reads_lead_jsonl(tmp_path: Path) -> None:
    _write_required_modes(tmp_path)
    config = _base_config()

    _write(
        tmp_path / "output" / "leads" / "leads.jsonl",
        json.dumps(
            {
                "company_id": "abc-1",
                "company_name": "Smoke House",
                "status": "replied_interested",
                "estimated_monthly_volume_lb": 700,
                "reply_summary": "Asked for a call",
                "updated_at": "2026-04-09T00:00:00Z",
            },
            ensure_ascii=True,
        )
        + "\n",
    )

    payload = lead_pipeline.build_stage_payload(
        stage="call_sheet_build",
        run_id="brisket-001",
        config=config,
        root=tmp_path,
    )

    assert payload["stage"] == "call_sheet_build"
    records = payload["records"]
    assert len(records) == 1
    record = records[0]
    assert record["company_id"] == "abc-1"
    assert record["priority_signals"]["interest_level"] == "high"
    assert record["priority_signals"]["volume_signal"] == "high"

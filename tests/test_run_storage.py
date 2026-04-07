from __future__ import annotations

from fastapi.testclient import TestClient

from market_validation.engine import MarketValidationEngine
from market_validation.main import app
from market_validation.models import ValidationRequest
from market_validation.storage import ResearchRunRepository


client = TestClient(app)


def _sample_request() -> ValidationRequest:
    return ValidationRequest(
        idea="Premium brisket catering for office lunches and weddings",
        target_customer="Office managers and wedding planners",
        geography="Austin, TX",
        business_model="local business",
        profile="local_business",
        template="restaurant",
        evidence_inputs=[
            {
                "source_type": "pricing_page",
                "source_title": "Competitor pricing",
                "source_url": "https://example.com/pricing",
                "observed_fact": "Packages from $22 per person to $48 per head.",
                "strength": "high",
            },
            {
                "source_type": "review_site",
                "source_title": "Review listing",
                "source_url": "https://example.com/reviews",
                "observed_fact": "Reviews mention great taste but occasional late delivery.",
                "strength": "high",
            },
            {
                "source_type": "directory_listing",
                "source_title": "Local directory",
                "source_url": "https://example.com/directory",
                "observed_fact": "Top local catering providers listed with service areas.",
                "strength": "medium",
            },
        ],
    )


def test_repository_roundtrip(tmp_path) -> None:
    repo = ResearchRunRepository(db_path=str(tmp_path / "runs.db"))
    engine = MarketValidationEngine()
    request = _sample_request()
    response = engine.validate(request)

    run_id = repo.save_run(
        endpoint="/validate",
        request_payload=request.model_dump(),
        response=response,
    )

    assert run_id

    listed = repo.list_runs(limit=10)
    assert listed
    assert listed[0]["id"] == run_id

    record = repo.get_run(run_id)
    assert record is not None
    assert record["endpoint"] == "/validate"
    assert record["request"]["idea"].startswith("Premium brisket catering")
    assert isinstance(record["response"], dict)
    assert "research_pipeline" in record["response"]
    assert record["response"]["research_brief"]["source_sites"] is not None


def test_api_persists_runs_and_returns_run_id(monkeypatch, tmp_path) -> None:
    import market_validation.main as main_module

    repo = ResearchRunRepository(db_path=str(tmp_path / "api-runs.db"))
    monkeypatch.setattr(main_module, "_run_repository", repo)

    payload = _sample_request().model_dump()
    response = client.post("/validate", json=payload)

    assert response.status_code == 200
    body = response.json()
    run_id = body.get("run_id")
    assert isinstance(run_id, str)
    assert run_id

    list_response = client.get("/runs")
    assert list_response.status_code == 200
    runs = list_response.json()["runs"]
    assert any(item["id"] == run_id for item in runs)

    detail_response = client.get(f"/runs/{run_id}")
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["id"] == run_id
    assert isinstance(detail["raw_sources"], list)
    assert isinstance(detail["structured_evidence"], list)

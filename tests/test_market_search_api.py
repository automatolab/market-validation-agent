from market_validation.models import ValidationRequest
from market_validation.main import app
from fastapi.testclient import TestClient

client = TestClient(app)


class _StubResearchService:
    def build_validation_request(self, _payload):
        return ValidationRequest(
            idea="Business focused on brisket catering",
            target_customer="Event planners in Austin",
            geography="US",
            business_model="Local business",
            competitors=["Blacks BBQ", "Franklin BBQ"],
            assumptions=["Customers seek premium brisket catering for events"],
            evidence_inputs=[
                {
                    "source_type": "review_site",
                    "source_title": "Yelp listings",
                    "source_url": "https://www.yelp.com",
                    "observed_fact": "Customers mention wait times and premium pricing",
                    "strength": "high",
                }
            ],
            profile="local_business",
            template="restaurant",
        )


def test_validate_market_endpoint_accepts_market_only(monkeypatch) -> None:
    import market_validation.main as main_module

    monkeypatch.setattr(main_module, "_market_research_service", _StubResearchService())

    response = client.post(
        "/validate/market",
        json={
            "market": "brisket catering",
            "geography": "US",
            "profile": "local_business",
            "template": "restaurant",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["overall_verdict"] in {"promising", "mixed", "weak", "insufficient_evidence"}
    assert len(body["evidence_table"]) >= 2
    assert any(item["source_type"] == "review_site" for item in body["evidence_table"])


def test_validate_market_endpoint_handles_search_failure(monkeypatch) -> None:
    import market_validation.main as main_module

    class _FailingSearcher:
        def search(self, query: str, max_results: int):
            raise RuntimeError("rate limited")

    monkeypatch.setattr(main_module._market_research_service, "_searcher", _FailingSearcher())

    response = client.post(
        "/validate/market",
        json={
            "market": "brisket catering",
            "geography": "US",
            "profile": "local_business",
            "template": "restaurant",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["overall_verdict"] in {"promising", "mixed", "weak", "insufficient_evidence"}
    diagnostics = body["research_diagnostics"]
    assert diagnostics["queries_attempted"] > 0
    assert diagnostics["raw_source_count"] == 0
    assert diagnostics["status"] == "external_search_failed"
    assert diagnostics["search_errors"]

from fastapi.testclient import TestClient

from market_validation.main import app

client = TestClient(app)


def test_validate_contract_contains_required_sections() -> None:
    payload = {
        "idea": "AI scheduling assistant for outpatient clinics",
        "target_customer": "Clinic operations managers in small private practices",
        "geography": "US",
        "business_model": "B2B SaaS subscription",
        "competitors": ["NexHealth", "Luma Health"],
        "pricing_guess": "$149/month per location",
        "assumptions": ["Staff are overloaded with phone scheduling"],
        "constraints": ["No EHR integration in v1"],
        "profile": "saas",
        "template": "ai_saas",
        "evidence_inputs": [
            {
                "source_type": "review_site",
                "source_title": "G2 category",
                "source_url": "https://www.g2.com/categories/appointment-scheduling",
                "observed_fact": "Users complain about setup complexity and hidden costs",
                "strength": "high",
            }
        ],
    }

    response = client.post("/validate", json=payload)

    assert response.status_code == 200
    body = response.json()

    expected_keys = {
        "research_stage",
        "market_summary",
        "source_coverage_summary",
        "target_customer_clarity",
        "competitor_map",
        "demand_signals",
        "review_sentiment_summary",
        "pricing_snapshot",
        "pricing_reality_check",
        "distribution_difficulty",
        "research_plan",
        "risks",
        "unknowns",
        "market_score",
        "market_score_status",
        "market_score_basis",
        "evidence_coverage_score",
        "confidence_score",
        "overall_score",
        "overall_verdict",
        "scores",
        "next_validation_experiments",
        "evidence_table",
        "raw_sources",
        "structured_evidence",
        "evidence_graph_summary",
        "research_diagnostics",
        "research_brief",
        "research_pipeline",
    }
    assert expected_keys.issubset(body.keys())
    assert len(body["scores"]) >= 10
    assert body["overall_verdict"] in {"promising", "mixed", "weak", "insufficient_evidence"}
    assert body["research_stage"] in {"brief_only", "search_results_only", "partial_research", "complete_research"}
    assert body["market_score_status"] in {"scored", "provisional", "insufficient_evidence"}
    assert body["market_score_basis"] in {"evidence_backed", "partial_external_evidence", "founder_input_only"}
    if body["market_score"] is not None:
        assert 1.0 <= body["market_score"] <= 10.0
    assert body["source_coverage_summary"]["external_evidence_count"] >= 1
    assert 0.0 <= body["evidence_coverage_score"] <= 100.0
    assert 0.0 <= body["confidence_score"] <= 100.0
    assert len(body["evidence_table"]) >= 3
    assert isinstance(body["raw_sources"], list)
    assert isinstance(body["structured_evidence"], list)
    assert "contradictions" in body["evidence_graph_summary"]
    assert isinstance(body["research_diagnostics"], dict)

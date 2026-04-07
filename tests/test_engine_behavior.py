from market_validation.engine import MarketValidationEngine
from market_validation.models import ValidationRequest


class _StubLLMClient:
    def __init__(self, payload: dict[str, object]) -> None:
        self.enabled = True
        self._payload = payload

    def chat_json(self, system_prompt: str, user_prompt: str) -> dict[str, object] | None:
        del system_prompt, user_prompt
        return self._payload


def _score_lookup(result, dimension: str) -> float:
    for item in result.scores:
        if item.dimension == dimension:
            return item.weight
    raise AssertionError(f"Missing dimension: {dimension}")


def test_profile_tuning_changes_weights_and_score() -> None:
    engine = MarketValidationEngine()

    base_request = {
        "idea": "CRM automation toolkit for niche wholesalers",
        "target_customer": "Sales managers at regional wholesalers",
        "geography": "US",
        "business_model": "B2B SaaS subscription",
        "competitors": ["Competitor A", "Competitor B", "Competitor C"],
        "pricing_guess": "$199/mo",
        "assumptions": ["Email follow-up is manual and error-prone"],
        "constraints": ["Bootstrap budget under $40k"],
        "evidence_inputs": [
            {
                "source_type": "review_site",
                "source_title": "G2 CRM reviews",
                "source_url": "https://www.g2.com/categories/crm",
                "observed_fact": "Users mention follow-up automation gaps",
                "strength": "high",
            },
            {
                "source_type": "pricing_page",
                "source_title": "Competitor pricing",
                "source_url": "https://example.com/pricing",
                "observed_fact": "Plans start at $99 per month",
                "strength": "high",
            },
            {
                "source_type": "directory_listing",
                "source_title": "CRM alternatives",
                "source_url": "https://example.com/crm-alternatives",
                "observed_fact": "Multiple CRM competitors target regional wholesalers",
                "strength": "medium",
            },
        ],
    }

    general_result = engine.validate(ValidationRequest(**base_request, profile="general"))
    saas_result = engine.validate(ValidationRequest(**base_request, profile="saas"))

    assert _score_lookup(general_result, "retention_repeatability") != _score_lookup(
        saas_result,
        "retention_repeatability",
    )
    assert general_result.overall_score != saas_result.overall_score


def test_sparse_input_surfaces_unknowns_and_experiments() -> None:
    engine = MarketValidationEngine()

    result = engine.validate(
        ValidationRequest(
            idea="Platform for everyone to manage tasks",
            target_customer="Everyone",
            geography="global",
            business_model="one-time purchase",
            profile="general",
        )
    )

    assert result.research_stage == "brief_only"
    assert result.overall_verdict == "insufficient_evidence"
    assert result.market_score is None
    assert result.market_score_status == "insufficient_evidence"
    assert result.market_score_basis == "founder_input_only"
    assert all(item.score is None for item in result.scores)
    assert all(item.status == "insufficient_evidence" for item in result.scores)
    assert any("No explicit competitor set" in item for item in result.unknowns)
    assert any("No pricing guess" in item for item in result.unknowns)
    assert any("Need at least 3 competitor sources" in item for item in result.unknowns)
    experiment_names = {item.name for item in result.next_validation_experiments}
    assert experiment_names.intersection(
        {
            "Problem Interviews",
            "Segment Precision Test",
            "Pricing Smoke Test",
        }
    )


def test_template_can_override_profile_defaults() -> None:
    engine = MarketValidationEngine()

    request_without_template = ValidationRequest(
        idea="Neighborhood dining concept with rotating chef menu",
        target_customer="Urban professionals within 5 miles",
        geography="Austin, TX",
        business_model="local business",
        competitors=["Local Bistro", "Corner Kitchen"],
        pricing_guess="$25 average ticket",
        profile="general",
    )
    templated_payload = request_without_template.model_dump()
    templated_payload["template"] = "restaurant"
    request_with_template = ValidationRequest(**templated_payload)

    generic_result = engine.validate(request_without_template)
    templated_result = engine.validate(request_with_template)

    generic_distribution_weight = _score_lookup(generic_result, "distribution_ease")
    restaurant_dimensions = {item.dimension for item in templated_result.scores}

    assert generic_distribution_weight > 0.0
    assert "local_demand_density" in restaurant_dimensions
    assert "price_per_head_viability" in restaurant_dimensions

    experiment_names = {item.name for item in templated_result.next_validation_experiments}
    assert "Venue Planner Discovery Calls" in experiment_names


def test_partial_research_marks_scores_provisional() -> None:
    engine = MarketValidationEngine()

    result = engine.validate(
        ValidationRequest(
            idea="Brisket catering service for office lunches and private parties",
            target_customer="Office managers and party hosts",
            geography="Austin, TX",
            business_model="local business",
            profile="local_business",
            template="restaurant",
            evidence_inputs=[
                {
                    "source_type": "review_site",
                    "source_title": "Yelp results",
                    "source_url": "https://www.yelp.com/search?find_desc=brisket+catering",
                    "observed_fact": "Customers praise brisket tenderness but mention minimum order size.",
                    "strength": "high",
                }
            ],
        )
    )

    assert result.research_stage == "partial_research"
    assert result.overall_verdict == "insufficient_evidence"
    assert result.market_score is not None
    assert result.market_score_status == "provisional"
    assert result.market_score_basis == "partial_external_evidence"
    assert any(item.status == "provisional" for item in result.scores)


def test_llm_refines_dimension_scores_when_available() -> None:
    llm_client = _StubLLMClient(
        {
            "dimension_scores": [
                {
                    "dimension": "pain_intensity",
                    "score": 9.0,
                    "rationale": "External complaints consistently indicate urgent scheduling friction.",
                }
            ]
        }
    )
    engine = MarketValidationEngine(llm_client=llm_client)

    result = engine.validate(
        ValidationRequest(
            idea="Scheduling automation for clinics",
            target_customer="Clinic operations managers",
            geography="US",
            business_model="B2B SaaS subscription",
            profile="saas",
            evidence_inputs=[
                {
                    "source_type": "review_site",
                    "source_title": "G2 category",
                    "source_url": "https://www.g2.com/categories/appointment-scheduling",
                    "observed_fact": "Users report manual appointment confirmation causes delays.",
                    "strength": "high",
                },
                {
                    "source_type": "pricing_page",
                    "source_title": "Competitor pricing",
                    "source_url": "https://example.com/pricing",
                    "observed_fact": "Plans start at $129 per month per provider.",
                    "strength": "high",
                },
                {
                    "source_type": "directory_listing",
                    "source_title": "Software alternatives",
                    "source_url": "https://example.com/alternatives",
                    "observed_fact": "Multiple alternatives target clinics with scheduling tools.",
                    "strength": "medium",
                },
            ],
        )
    )

    pain_score = next(item for item in result.scores if item.dimension == "pain_intensity")
    assert pain_score.score is not None
    assert pain_score.score >= 7.5
    assert "urgent scheduling friction" in pain_score.rationale.lower()


def test_evidence_graph_detects_contradictions_and_pricing_bands() -> None:
    engine = MarketValidationEngine()

    result = engine.validate(
        ValidationRequest(
            idea="Premium brisket catering service",
            target_customer="Wedding planners and office managers",
            geography="Austin, TX",
            business_model="local business",
            profile="local_business",
            template="restaurant",
            evidence_inputs=[
                {
                    "source_type": "company_website",
                    "source_title": "Prime Smoke Catering",
                    "source_url": "https://primesmoke.example",
                    "observed_fact": "Premium brisket catering with chef-level quality and luxury presentation.",
                    "strength": "medium",
                },
                {
                    "source_type": "review_site",
                    "source_title": "Review profile",
                    "source_url": "https://reviews.example/primesmoke",
                    "observed_fact": "Some customers report inconsistent quality and late delivery despite premium pricing around $48 per head.",
                    "strength": "high",
                },
                {
                    "source_type": "pricing_page",
                    "source_title": "Pricing",
                    "source_url": "https://primesmoke.example/pricing",
                    "observed_fact": "Packages start at $22 per person and premium package is $48 per head.",
                    "strength": "high",
                },
            ],
        )
    )

    assert result.structured_evidence
    assert result.evidence_graph_summary.entity_count >= 1
    assert result.evidence_graph_summary.pricing_bands
    assert result.evidence_graph_summary.contradictions


def test_search_results_only_stage_caps_confidence_and_score() -> None:
    engine = MarketValidationEngine()

    result = engine.validate(
        ValidationRequest(
            idea="Brisket catering service",
            target_customer="Office managers",
            geography="Austin, TX",
            business_model="Local business",
            profile="local_business",
            template="restaurant",
            evidence_inputs=[
                {
                    "source_type": "review_site",
                    "source_title": "Search results snippet",
                    "source_url": "https://results.example/reviews",
                    "observed_fact": "Snippet says customers complain about late delivery and high pricing around $35 per head.",
                    "strength": "high",
                    "evidence_basis": "search_snippet",
                }
            ],
            research_diagnostics={
                "queries_attempted": 8,
                "fetch_attempted": 8,
                "fetch_success": 0,
                "search_errors": ["reviews:RuntimeError:rate_limited"],
            },
        )
    )

    assert result.research_stage == "search_results_only"
    assert result.overall_verdict == "insufficient_evidence"
    assert result.market_score is None
    assert result.market_score_status == "insufficient_evidence"
    assert result.market_score_basis == "partial_external_evidence"
    assert result.confidence_score <= 32.0
    assert any("snippet-only" in item.lower() for item in result.unknowns)


def test_brisket_pipeline_builds_lead_artifacts() -> None:
    engine = MarketValidationEngine()

    result = engine.validate(
        ValidationRequest(
            idea="Brisket catering pipeline for local restaurants and event hosts",
            target_customer="Restaurant owners and event managers",
            geography="Austin, TX",
            business_model="local business",
            profile="local_business",
            template="restaurant",
            competitors=["Smokehouse Catering", "Pitmasters BBQ"],
            evidence_inputs=[
                {
                    "source_type": "directory_listing",
                    "source_title": "Smokehouse Catering listing",
                    "source_url": "https://smokehouse.example/listing",
                    "observed_fact": "The listing highlights catering, event hosting, and brisket menu items.",
                    "strength": "high",
                },
                {
                    "source_type": "review_site",
                    "source_title": "Pitmasters reviews",
                    "source_url": "https://pitmasters.example/reviews",
                    "observed_fact": "Reviews mention brisket, smoker flavor, and bulk food operations for corporate lunches.",
                    "strength": "high",
                },
                {
                    "source_type": "pricing_page",
                    "source_title": "Event pricing",
                    "source_url": "https://smokehouse.example/menu-pricing",
                    "observed_fact": "Packages start at $24 per person and premium catering reaches $48 per head.",
                    "strength": "high",
                },
            ],
        )
    )

    pipeline = result.research_pipeline
    assert pipeline.thesis is not None
    assert pipeline.thesis.source_sites
    assert any("brisket" in signal.lower() or "catering" in signal.lower() for signal in pipeline.thesis.demand_signals)
    assert pipeline.lead_records
    assert pipeline.lead_scores
    assert pipeline.outreach_drafts
    assert pipeline.reply_tracking
    assert pipeline.call_sheets

    first_lead = pipeline.lead_records[0]
    assert first_lead.source_urls
    assert first_lead.evidence_snippets

    first_draft = pipeline.outreach_drafts[0]
    assert first_draft.personalization_lines
    assert any(line.evidence_refs for line in first_draft.personalization_lines)
    assert first_draft.first_email

    first_tracking = pipeline.reply_tracking[0]
    assert first_tracking.intent in {"pending", "no_reply", "not_now"}
    assert first_tracking.company_status in {
        "awaiting_reply",
        "follow_up_needed",
        "closed_lost",
    }

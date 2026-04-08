from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

ProfileName = Literal[
    "general",
    "saas",
    "local_business",
    "ecommerce",
    "service_business",
]
EvidenceStrength = Literal["low", "medium", "high"]
Verdict = Literal["promising", "mixed", "weak", "insufficient_evidence"]
ResearchStage = Literal["brief_only", "search_results_only", "partial_research", "complete_research"]
ScoreStatus = Literal["scored", "provisional", "insufficient_evidence"]
EvidenceBasis = Literal["fetched_page", "search_snippet", "direct_source", "unknown"]


class EvidenceInput(BaseModel):
    source_type: str = Field(min_length=2)
    source_title: str = Field(min_length=2)
    source_url: str | None = None
    observed_fact: str = Field(min_length=3)
    strength: EvidenceStrength = "medium"
    evidence_basis: EvidenceBasis = "unknown"


class ValidationRequest(BaseModel):
    idea: str = Field(min_length=5)
    target_customer: str = Field(min_length=3)
    geography: str = Field(default="global", min_length=2)
    business_model: str = Field(min_length=3)
    competitors: list[str] = Field(default_factory=list)
    pricing_guess: str | None = None
    assumptions: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    profile: ProfileName = "general"
    template: str | None = None
    evidence_inputs: list[EvidenceInput] = Field(default_factory=list)
    raw_sources: list["RawSourceRecord"] = Field(default_factory=list)
    structured_evidence: list["StructuredEvidenceItem"] = Field(default_factory=list)
    research_diagnostics: dict[str, Any] = Field(default_factory=dict)


class MarketSearchRequest(BaseModel):
    research_mode: Literal["standard", "deep"] = "standard"
    market: str = Field(min_length=3)
    geography: str = Field(default="global", min_length=2)
    profile: ProfileName = "general"
    template: str | None = None
    target_customer: str | None = None
    pricing_guess: str | None = None
    assumptions: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    max_search_results: int = Field(default=12, ge=3, le=30)
    minimum_evidence_rows: int = Field(default=10, ge=3, le=40)


class ResearchBrief(BaseModel):
    problem_statement: str
    target_customer: str
    geography: str
    business_model: str
    assumptions_to_test: list[str]
    constraints: list[str]
    key_questions: list[str]
    source_sites: list[str] = Field(default_factory=list)
    source_type: str = "mixed"
    company_types: list[str] = Field(default_factory=list)
    demand_signals: list[str] = Field(default_factory=list)


class LeadRecord(BaseModel):
    name: str
    website: str | None = None
    phone: str | None = None
    email: str | None = None
    location: str | None = None
    category: str | None = None
    menu_url: str | None = None
    source_urls: list[str] = Field(default_factory=list)
    evidence_snippets: list[str] = Field(default_factory=list)
    demand_signals: list[str] = Field(default_factory=list)


class LeadScore(BaseModel):
    lead_name: str
    probability_buy: float = Field(ge=0.0, le=1.0)
    estimated_volume_potential: float = Field(ge=0.0, le=1.0)
    geographic_fit: float = Field(ge=0.0, le=1.0)
    pricing_tier_fit: float = Field(ge=0.0, le=1.0)
    catering_event_potential: float = Field(ge=0.0, le=1.0)
    contactability: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    status: Literal["hot", "warm", "cold", "disqualified"]
    rationale: str


class PersonalizationLine(BaseModel):
    text: str
    evidence_refs: list[str] = Field(default_factory=list)


class OutreachDraft(BaseModel):
    lead_name: str
    intro: str
    why_selected: str
    brisket_relevance: str
    offer: str
    cta: str
    personalization_lines: list[PersonalizationLine] = Field(default_factory=list)
    first_email: str
    follow_up_1: str
    follow_up_2: str


class ReplyTrackingEntry(BaseModel):
    lead_name: str
    intent: Literal["pending", "interested", "objection", "pricing_request", "schedule_request", "not_now", "no_reply"]
    company_status: Literal["awaiting_reply", "follow_up_needed", "call_scheduled", "qualified", "closed_lost", "closed_won"]
    thread_summary: str
    follow_up_task: str


class CallSheet(BaseModel):
    lead_name: str
    company_summary: str
    prior_emails: list[str] = Field(default_factory=list)
    talking_points: list[str] = Field(default_factory=list)
    objections: list[str] = Field(default_factory=list)
    next_step_suggestions: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class MarketResearchPipeline(BaseModel):
    thesis: ResearchBrief | None = None
    lead_records: list[LeadRecord] = Field(default_factory=list)
    lead_scores: list[LeadScore] = Field(default_factory=list)
    outreach_drafts: list[OutreachDraft] = Field(default_factory=list)
    reply_tracking: list[ReplyTrackingEntry] = Field(default_factory=list)
    call_sheets: list[CallSheet] = Field(default_factory=list)


# --- Brisket pipeline request/response models ---

class DiscoverRequest(BaseModel):
    geography: str = Field(min_length=2)
    max_leads: int = Field(default=20, ge=1, le=50)
    keywords: list[str] = Field(
        default_factory=lambda: ["BBQ", "brisket", "smokehouse", "barbecue", "catering"]
    )


class NoteInput(BaseModel):
    note: str = Field(min_length=1)
    author: str | None = None


class ReplyInput(BaseModel):
    raw_reply_text: str = Field(min_length=1)


class LeadSummary(BaseModel):
    id: str
    name: str
    location: str | None = None
    email: str | None = None
    phone: str | None = None
    pipeline_status: str
    score_status: str | None = None
    probability_buy: float | None = None
    outreach_sent: bool = False
    reply_intent: str | None = None
    created_at: str


class CompetitorEntry(BaseModel):
    competitor: str
    inferred_positioning: str
    inferred_price_signal: str
    threat_level: Literal["low", "medium", "high"]


class DemandSignal(BaseModel):
    signal: str
    strength: EvidenceStrength
    source_refs: list[str] = Field(default_factory=list)


class DimensionScore(BaseModel):
    dimension: str
    score: float | None = Field(default=None, ge=1.0, le=10.0)
    status: ScoreStatus = "scored"
    provisional_based_on: Literal["founder_input_only", "partial_external_evidence"] | None = None
    weight: float = Field(gt=0.0)
    rationale: str


class ExperimentRecommendation(BaseModel):
    name: str
    hypothesis: str
    method: str
    success_criteria: str
    priority: int = Field(ge=1, le=5)
    effort: Literal["low", "medium", "high"]


class EvidenceRow(BaseModel):
    id: str
    source_type: str
    source_title: str
    source_url: str | None = None
    observed_fact: str
    strength: EvidenceStrength
    evidence_basis: EvidenceBasis = "unknown"


class SourceCoverageSummary(BaseModel):
    external_evidence_count: int = Field(ge=0)
    competitor_sources: int = Field(ge=0)
    pricing_sources: int = Field(ge=0)
    review_community_sources: int = Field(ge=0)
    fetched_evidence_count: int = Field(default=0, ge=0)
    snippet_evidence_count: int = Field(default=0, ge=0)
    direct_evidence_count: int = Field(default=0, ge=0)
    threshold_competitor_sources: int = 3
    threshold_pricing_sources: int = 2
    threshold_review_community_sources: int = 2
    meets_minimum_external_evidence: bool
    missing_requirements: list[str] = Field(default_factory=list)


class RawSourceRecord(BaseModel):
    id: str
    query_label: str
    source_type: str
    source_title: str
    source_url: str
    snippet: str
    cleaned_text: str
    fetched: bool = False
    trust_weight: float = Field(ge=0.0, le=1.0)


class StructuredEvidenceItem(BaseModel):
    id: str
    source_id: str
    source_type: str
    entity: str
    fact_type: str
    value: str
    excerpt: str
    url: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_basis: EvidenceBasis = "unknown"


class EvidenceThemeGroup(BaseModel):
    theme: str
    count: int = Field(ge=1)
    evidence_ids: list[str] = Field(default_factory=list)


class PricingBandSummary(BaseModel):
    band: Literal["low", "mid", "premium", "enterprise"]
    observation_count: int = Field(ge=1)
    min_price: float | None = Field(default=None, ge=0.0)
    max_price: float | None = Field(default=None, ge=0.0)
    evidence_ids: list[str] = Field(default_factory=list)


class EvidenceGraphSummary(BaseModel):
    entity_count: int = Field(default=0, ge=0)
    entities: list[str] = Field(default_factory=list)
    pricing_bands: list[PricingBandSummary] = Field(default_factory=list)
    complaint_themes: list[EvidenceThemeGroup] = Field(default_factory=list)
    praise_themes: list[EvidenceThemeGroup] = Field(default_factory=list)
    contradictions: list[str] = Field(default_factory=list)
    source_type_counts: dict[str, int] = Field(default_factory=dict)


class ValidationResponse(BaseModel):
    run_id: str | None = None
    research_stage: ResearchStage
    market_summary: str
    source_coverage_summary: SourceCoverageSummary
    target_customer_clarity: str
    competitor_map: list[CompetitorEntry]
    demand_signals: list[DemandSignal]
    review_sentiment_summary: str
    pricing_snapshot: str
    pricing_reality_check: str
    distribution_difficulty: str
    research_plan: list[str]
    risks: list[str]
    unknowns: list[str]
    market_score: float | None = Field(default=None, ge=1.0, le=10.0)
    market_score_status: ScoreStatus
    market_score_basis: Literal["evidence_backed", "partial_external_evidence", "founder_input_only"]
    evidence_coverage_score: float = Field(ge=0.0, le=100.0)
    confidence_score: float = Field(ge=0.0, le=100.0)
    overall_score: float | None = Field(default=None, ge=1.0, le=10.0)
    overall_verdict: Verdict
    scores: list[DimensionScore]
    next_validation_experiments: list[ExperimentRecommendation]
    evidence_table: list[EvidenceRow]
    raw_sources: list[RawSourceRecord] = Field(default_factory=list)
    structured_evidence: list[StructuredEvidenceItem] = Field(default_factory=list)
    evidence_graph_summary: EvidenceGraphSummary = Field(default_factory=EvidenceGraphSummary)
    research_diagnostics: dict[str, Any] = Field(default_factory=dict)
    research_brief: ResearchBrief
    research_pipeline: MarketResearchPipeline = Field(default_factory=MarketResearchPipeline)

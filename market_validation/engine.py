from __future__ import annotations

import json
import re
from typing import Any, Iterable
from urllib.parse import urlparse

from .llm import OllamaClient
from .models import (
    CallSheet,
    CompetitorEntry,
    DemandSignal,
    DimensionScore,
    EvidenceGraphSummary,
    EvidenceInput,
    EvidenceRow,
    EvidenceThemeGroup,
    ExperimentRecommendation,
    LeadRecord,
    LeadScore,
    MarketResearchPipeline,
    OutreachDraft,
    PricingBandSummary,
    PersonalizationLine,
    RawSourceRecord,
    ResearchBrief,
    ReplyTrackingEntry,
    SourceCoverageSummary,
    StructuredEvidenceItem,
    ValidationRequest,
    ValidationResponse,
)
from .profiles import DIMENSIONS, ProfileConfig, get_profile_config
from .templates import apply_template_to_profile, get_template

FOUNDER_SOURCE_TYPES = {
    "founder_input",
    "founder_assumption",
    "operational_constraint",
    "pricing_hypothesis",
    "founder_competitor_claim",
}


def _clamp(value: float, minimum: float = 1.0, maximum: float = 10.0) -> float:
    return max(minimum, min(maximum, value))


def _strength_to_points(strength: str) -> float:
    return {
        "high": 1.0,
        "medium": 0.66,
        "low": 0.33,
    }.get(strength, 0.33)


def _clean_label(dimension: str) -> str:
    return dimension.replace("_", " ")


def _extract_url(raw_value: str) -> str | None:
    match = re.search(r"https?://\S+", raw_value)
    if not match:
        return None
    return match.group(0).rstrip(".,)")


def _extract_competitor_name(raw_value: str) -> str:
    url = _extract_url(raw_value)
    if not url:
        return raw_value.strip()
    return raw_value.replace(url, "").replace("-", " ").strip() or url


def _contains_any(text: str, words: Iterable[str]) -> bool:
    return any(word in text for word in words)


def _extract_price_points(text: str) -> list[float]:
    points: list[float] = []
    for raw in re.findall(r"\$\s*\d+(?:\.\d+)?", text):
        normalized = raw.replace("$", "").strip()
        try:
            points.append(float(normalized))
        except ValueError:
            continue
    return points


class MarketValidationEngine:
    """Deterministic market validation engine with profile-based scoring."""

    def __init__(self, llm_client: OllamaClient | None = None) -> None:
        self._llm_client = llm_client or OllamaClient()

    def validate(self, request: ValidationRequest) -> ValidationResponse:
        # Step 1: Normalize inputs and collect base evidence
        profile = self._resolve_profile(request)
        evidence_table = self._collect_evidence(request)
        structured_evidence = request.structured_evidence or self._derive_structured_evidence(evidence_table)
        
        # Step 2: Validate evidence quality and determine research stage
        source_coverage_summary = self._assess_source_coverage(evidence_table)
        research_stage = self._determine_research_stage(
            source_coverage_summary,
            request.research_diagnostics,
        )
        
        # Step 3: Perform market validation scoring
        evidence_graph_summary = self._build_evidence_graph_summary(structured_evidence)
        research_brief = self._build_research_brief(request, evidence_table, structured_evidence)
        evidence_coverage_score = self._calculate_evidence_coverage_score(source_coverage_summary)

        competitor_names = self._derive_competitor_names(request, evidence_table, structured_evidence)
        competitor_map = self._build_competitor_map(competitor_names)

        demand_signals = self._build_demand_signals(request, evidence_table, structured_evidence)
        raw_scores = self._score_dimensions(request, evidence_table, competitor_map, profile)
        raw_scores = self._refine_scores_with_llm(
            request,
            evidence_table,
            raw_scores,
            research_stage,
        )

        market_score_raw = round(
            sum((score.score or 0.0) * score.weight for score in raw_scores),
            2,
        )
        unknowns = self._derive_unknowns(
            request,
            evidence_table,
            profile,
            source_coverage_summary,
            request.research_diagnostics,
        )
        risks = self._derive_risks(raw_scores)
        confidence = self._calculate_confidence(
            request,
            evidence_table,
            evidence_coverage_score,
            unknowns,
            source_coverage_summary,
            structured_evidence,
            evidence_graph_summary,
            request.research_diagnostics,
        )
        verdict = self._verdict(
            market_score_raw,
            confidence,
            source_coverage_summary,
            research_stage,
        )

        experiments = self._recommend_experiments(raw_scores, request, source_coverage_summary)
        scores = self._apply_score_stage(raw_scores, research_stage)
        market_score, market_score_status, market_score_basis = self._apply_market_score_stage(
            market_score_raw,
            research_stage,
        )
        
        # Step 4: Conditional lead generation based on quality gates
        if self._check_lead_generation_quality_gate(research_stage, source_coverage_summary):
            research_pipeline = self._build_research_pipeline(
                request,
                evidence_table,
                structured_evidence,
                competitor_map,
                demand_signals,
                research_brief,
            )
        else:
            # Fallback to research-only pipeline
            research_pipeline = MarketResearchPipeline(
                thesis=research_brief,
                lead_records=[],
                lead_scores=[],
                outreach_drafts=[],
                reply_tracking=[],
                call_sheets=[],
            )

        research_plan = self._build_research_plan(request, source_coverage_summary, research_stage)
        market_summary = self._build_market_summary(
            raw_scores,
            verdict,
            confidence,
            source_coverage_summary,
            research_stage,
            evidence_graph_summary,
        )

        customer_clarity_score = self._score_by_dimension(raw_scores, "customer_clarity", None)
        if customer_clarity_score is None:
            customer_clarity_score = self._target_specificity_score(request.target_customer)

        pricing_score = self._score_by_dimension(raw_scores, "willingness_to_pay", None)
        if pricing_score is None:
            pricing_score = self._score_by_dimension(raw_scores, "price_per_head_viability", 5.0)

        distribution_score = self._score_by_dimension(raw_scores, "distribution_ease", None)
        if distribution_score is None:
            distribution_score = self._score_by_dimension(raw_scores, "competitor_saturation", 5.0)

        return ValidationResponse(
            research_stage=research_stage,
            market_summary=market_summary,
            source_coverage_summary=source_coverage_summary,
            target_customer_clarity=self._target_customer_clarity_text(
                customer_clarity_score,
                request,
            ),
            competitor_map=competitor_map,
            demand_signals=demand_signals,
            review_sentiment_summary=self._review_sentiment_summary(
                evidence_table,
                source_coverage_summary,
            ),
            pricing_snapshot=self._pricing_snapshot(
                request,
                evidence_table,
                source_coverage_summary,
            ),
            pricing_reality_check=self._pricing_reality_check(
                request,
                evidence_table,
                pricing_score,
            ),
            distribution_difficulty=self._distribution_difficulty_text(
                distribution_score,
                request,
                research_stage,
            ),
            research_plan=research_plan,
            risks=risks,
            unknowns=unknowns,
            market_score=market_score,
            market_score_status=market_score_status,
            market_score_basis=market_score_basis,
            evidence_coverage_score=evidence_coverage_score,
            confidence_score=confidence,
            overall_score=market_score,
            overall_verdict=verdict,
            scores=scores,
            next_validation_experiments=experiments,
            evidence_table=evidence_table,
            raw_sources=request.raw_sources,
            structured_evidence=structured_evidence,
            evidence_graph_summary=evidence_graph_summary,
            research_diagnostics=request.research_diagnostics,
            research_brief=research_brief,
            research_pipeline=research_pipeline,
        )

    def _resolve_profile(self, request: ValidationRequest) -> ProfileConfig:
        template = get_template(request.template)
        profile_name = request.profile
        if template and template.profile_override:
            profile_name = template.profile_override
        profile = get_profile_config(profile_name)
        return apply_template_to_profile(profile, template)

    def _build_research_brief(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        structured_evidence: list[StructuredEvidenceItem],
    ) -> ResearchBrief:
        assumptions = request.assumptions or [
            "Pain is important enough that customers will change current behavior.",
            "A reachable distribution channel exists.",
        ]
        questions = [
            "How urgent is the target customer's pain today?",
            "What alternatives are customers already paying for?",
            "Which channel can reliably acquire first 10 customers?",
            "What proof would justify the pricing hypothesis?",
        ]
        source_sites = self._derive_source_sites(evidence_table)
        source_types = sorted({row.source_type for row in evidence_table if row.source_type})
        company_types = self._derive_company_types(request, evidence_table, structured_evidence)
        demand_signals = self._derive_thesis_demand_signals(request, evidence_table, structured_evidence)
        return ResearchBrief(
            problem_statement=request.idea,
            target_customer=request.target_customer,
            geography=request.geography,
            business_model=request.business_model,
            assumptions_to_test=assumptions,
            constraints=request.constraints,
            key_questions=questions,
            source_sites=source_sites,
            source_type=", ".join(source_types) if len(source_types) > 1 else (source_types[0] if source_types else "mixed"),
            company_types=company_types,
            demand_signals=demand_signals,
        )

    def _check_lead_generation_quality_gate(
        self,
        research_stage: str,
        source_coverage: SourceCoverageSummary,
    ) -> bool:
        """Fix 3 & 6: Ensure leads are only generated when evidence is high quality."""
        if research_stage != "complete_research":
            return False
            
        if source_coverage.fetched_evidence_count < 4:
            return False
            
        if not source_coverage.meets_minimum_external_evidence:
            return False
            
        return True

    def _build_research_pipeline(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        structured_evidence: list[StructuredEvidenceItem],
        competitor_map: list[CompetitorEntry],
        demand_signals: list[DemandSignal],
        research_brief: ResearchBrief,
    ) -> MarketResearchPipeline:
        lead_records = self._build_lead_records(request, evidence_table, structured_evidence, competitor_map)
        lead_scores = [self._score_lead(request, lead, evidence_table, structured_evidence) for lead in lead_records]
        outreach_drafts = [
            self._build_outreach_draft(request, lead, score, evidence_table, structured_evidence)
            for lead, score in zip(lead_records, lead_scores)
        ]
        reply_tracking = [
            self._build_reply_tracking_entry(lead, score)
            for lead, score in zip(lead_records, lead_scores)
        ]
        call_sheets = [
            self._build_call_sheet(request, lead, score, draft, evidence_table, structured_evidence)
            for lead, score, draft in zip(lead_records, lead_scores, outreach_drafts)
            if score.status in {"hot", "warm"}
        ]

        if not lead_records and request.competitors:
            fallback_leads = [
                LeadRecord(
                    name=self._clean_name(candidate),
                    source_urls=[],
                    evidence_snippets=[candidate],
                    demand_signals=[signal.signal for signal in demand_signals[:2]],
                )
                for candidate in request.competitors[:3]
            ]
            lead_records = fallback_leads
            lead_scores = [self._score_lead(request, lead, evidence_table, structured_evidence) for lead in lead_records]
            outreach_drafts = [
                self._build_outreach_draft(request, lead, score, evidence_table, structured_evidence)
                for lead, score in zip(lead_records, lead_scores)
            ]
            reply_tracking = [
                self._build_reply_tracking_entry(lead, score)
                for lead, score in zip(lead_records, lead_scores)
            ]
            call_sheets = [
                self._build_call_sheet(request, lead, score, draft, evidence_table, structured_evidence)
                for lead, score, draft in zip(lead_records, lead_scores, outreach_drafts)
                if score.status in {"hot", "warm"}
            ]

        return MarketResearchPipeline(
            thesis=research_brief,
            lead_records=lead_records[:8],
            lead_scores=lead_scores[:8],
            outreach_drafts=outreach_drafts[:8],
            reply_tracking=reply_tracking[:8],
            call_sheets=call_sheets[:8],
        )

    def _build_lead_records(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        structured_evidence: list[StructuredEvidenceItem],
        competitor_map: list[CompetitorEntry],
    ) -> list[LeadRecord]:
        candidates: list[tuple[str, list[EvidenceRow]]] = []
        seen: set[str] = set()

        def add_candidate(name: str, related: list[EvidenceRow]) -> None:
            cleaned = self._clean_name(name)
            if not cleaned:
                return
            lowered = cleaned.lower()
            if lowered in seen:
                return
            seen.add(lowered)
            candidates.append((cleaned, related))

        for competitor in request.competitors:
            related = self._related_evidence_for_candidate(competitor, evidence_table)
            add_candidate(competitor, related)

        for competitor in competitor_map:
            related = self._related_evidence_for_candidate(competitor.competitor, evidence_table)
            add_candidate(competitor.competitor, related)

        for item in structured_evidence:
            if item.entity.lower().startswith("unknown"):
                continue
            if item.fact_type not in {"competitor_positioning", "price_point", "price_per_head", "review_complaint_theme", "review_praise_theme"}:
                continue
            related = self._related_evidence_for_candidate(item.entity, evidence_table)
            add_candidate(item.entity, related)

        records: list[LeadRecord] = []
        for name, related in candidates[:8]:
            source_urls = self._unique_values([row.source_url for row in related if row.source_url])
            snippets = self._unique_values([self._lead_snippet(row) for row in related])
            category = self._infer_lead_category(name, related, request)
            website = next((row.source_url for row in related if row.source_type == "company_website" and row.source_url), None)
            menu_url = next((row.source_url for row in related if row.source_url and "menu" in row.source_url.lower()), None)
            contact_text = " ".join([row.observed_fact for row in related])
            phone = self._extract_phone(contact_text)
            email = self._extract_email(contact_text)
            demand_texts = self._derive_thesis_demand_signals(request, related, structured_evidence=None)
            records.append(
                LeadRecord(
                    name=name,
                    website=website,
                    phone=phone,
                    email=email,
                    location=self._infer_lead_location(related, request),
                    category=category,
                    menu_url=menu_url,
                    source_urls=source_urls,
                    evidence_snippets=snippets,
                    demand_signals=demand_texts,
                )
            )

        return records

    def _score_lead(
        self,
        request: ValidationRequest,
        lead: LeadRecord,
        evidence_table: list[EvidenceRow],
        structured_evidence: list[StructuredEvidenceItem],
    ) -> LeadScore:
        related = self._related_evidence_for_candidate(lead.name, evidence_table)
        related_text = " ".join([lead.category or "", lead.name, *lead.evidence_snippets, *lead.demand_signals]).lower()
        price_points = self._extract_price_points(related_text)
        has_contact = bool(lead.phone or lead.email or lead.website or lead.menu_url)
        event_terms = ("catering", "event", "wedding", "corporate", "office", "party", "bulk")
        food_terms = ("brisket", "bbq", "barbecue", "smoker", "pit", "menu")

        probability_buy = 0.34
        if _contains_any(related_text, event_terms):
            probability_buy += 0.2
        if _contains_any(related_text, food_terms):
            probability_buy += 0.16
        if any(row.source_type in {"review_site", "pricing_page", "directory_listing"} for row in related):
            probability_buy += 0.1

        estimated_volume_potential = 0.25
        if _contains_any(related_text, ("corporate", "office", "wedding", "party", "event")):
            estimated_volume_potential += 0.28
        if _contains_any(related_text, ("bulk", "minimum order", "per head", "per person")):
            estimated_volume_potential += 0.18

        geographic_fit = 0.7 if request.geography.lower() not in {"global", "worldwide"} else 0.5
        if lead.location and request.geography.lower() in lead.location.lower():
            geographic_fit = 0.95

        pricing_tier_fit = 0.45
        if any(price >= 40 for price in price_points):
            pricing_tier_fit = 0.88
        elif any(price >= 20 for price in price_points):
            pricing_tier_fit = 0.68
        if _contains_any(related_text, ("premium", "high-end", "luxury")):
            pricing_tier_fit = max(pricing_tier_fit, 0.82)

        catering_event_potential = 0.3
        if _contains_any(related_text, event_terms):
            catering_event_potential += 0.5
        if _contains_any(related_text, ("menu", "catering", "event hosting")):
            catering_event_potential += 0.12

        contactability = 0.35
        if lead.website:
            contactability += 0.28
        if lead.menu_url:
            contactability += 0.12
        if lead.phone:
            contactability += 0.12
        if lead.email:
            contactability += 0.13

        confidence = min(1.0, 0.22 + (0.1 * len(lead.evidence_snippets)) + (0.06 * len(related)) + (0.08 if structured_evidence else 0.0))
        weighted_score = (
            (probability_buy * 0.25)
            + (estimated_volume_potential * 0.15)
            + (geographic_fit * 0.15)
            + (pricing_tier_fit * 0.1)
            + (catering_event_potential * 0.15)
            + (contactability * 0.1)
            + (confidence * 0.1)
        )

        status = "disqualified"
        if weighted_score >= 0.75:
            status = "hot"
        elif weighted_score >= 0.58:
            status = "warm"
        elif weighted_score >= 0.4:
            status = "cold"

        rationale = (
            f"Derived from {len(related)} evidence rows with {len(lead.source_urls)} source URLs; "
            f"event signals={'yes' if _contains_any(related_text, event_terms) else 'no'}, "
            f"price anchors={len(price_points)}, contactability={'yes' if has_contact else 'limited'}."
        )

        return LeadScore(
            lead_name=lead.name,
            probability_buy=round(min(probability_buy, 1.0), 2),
            estimated_volume_potential=round(min(estimated_volume_potential, 1.0), 2),
            geographic_fit=round(min(geographic_fit, 1.0), 2),
            pricing_tier_fit=round(min(pricing_tier_fit, 1.0), 2),
            catering_event_potential=round(min(catering_event_potential, 1.0), 2),
            contactability=round(min(contactability, 1.0), 2),
            confidence=round(confidence, 2),
            status=status,
            rationale=rationale,
        )

    def _build_outreach_draft(
        self,
        request: ValidationRequest,
        lead: LeadRecord,
        score: LeadScore,
        evidence_table: list[EvidenceRow],
        structured_evidence: list[StructuredEvidenceItem],
    ) -> OutreachDraft:
        related = self._related_evidence_for_candidate(lead.name, evidence_table)
        snippets = [self._lead_snippet(row) for row in related[:3]] or lead.evidence_snippets[:3]
        evidence_refs = [row.id for row in related[:3]]
        personalization_lines = [
            PersonalizationLine(
                text=self._build_personalization_line(snippet, lead, request),
                evidence_refs=[ref] if ref else evidence_refs[:1],
            )
            for snippet, ref in zip(snippets, evidence_refs)
        ]
        while len(personalization_lines) < min(3, len(snippets)):
            personalization_lines.append(
                PersonalizationLine(text=f"{lead.name} showed public evidence relevant to brisket demand.", evidence_refs=evidence_refs[:1])
            )

        intro = f"Hi {lead.name}, I reviewed your public signals and wanted to reach out because they line up with brisket demand."
        why_selected = (
            f"You were selected because your public evidence points to {', '.join(lead.demand_signals[:2]) or 'catering and event demand'}."
        )
        brisket_relevance = (
            "Brisket is a fit here because the available evidence suggests recurring food-service, catering, or event-driven demand."
        )
        if score.status == "hot":
            offer = "I can share a short, evidence-backed brisket outreach angle tailored to your lead profile."
        elif score.status == "warm":
            offer = "I can share a concise test offer and a few brisket positioning ideas for your team."
        else:
            offer = "I can share a low-friction brisket demand test if this is worth a quick look."
        cta = "Would you be open to a 10-minute call this week to compare notes?"

        first_email = self._compose_email(lead.name, intro, why_selected, brisket_relevance, offer, cta, personalization_lines)
        follow_up_1 = self._compose_follow_up(lead_name=lead.name, sequence_number=1, lead=lead, score=score)
        follow_up_2 = self._compose_follow_up(lead_name=lead.name, sequence_number=2, lead=lead, score=score)

        return OutreachDraft(
            lead_name=lead.name,
            intro=intro,
            why_selected=why_selected,
            brisket_relevance=brisket_relevance,
            offer=offer,
            cta=cta,
            personalization_lines=personalization_lines,
            first_email=first_email,
            follow_up_1=follow_up_1,
            follow_up_2=follow_up_2,
        )

    def _build_reply_tracking_entry(self, lead: LeadRecord, score: LeadScore) -> ReplyTrackingEntry:
        if score.status in {"hot", "warm"}:
            intent = "pending"
            company_status = "awaiting_reply"
            follow_up_task = "Monitor inbox and send the next follow-up if no reply arrives."
        elif score.status == "cold":
            intent = "no_reply"
            company_status = "follow_up_needed"
            follow_up_task = "Queue a light follow-up or move the lead to a slower nurture list."
        else:
            intent = "not_now"
            company_status = "closed_lost"
            follow_up_task = "Do not pursue unless new evidence changes the fit."

        return ReplyTrackingEntry(
            lead_name=lead.name,
            intent=intent,
            company_status=company_status,
            thread_summary="No reply ingested yet; tracking is initialized from the outbound sequence.",
            follow_up_task=follow_up_task,
        )

    def _build_call_sheet(
        self,
        request: ValidationRequest,
        lead: LeadRecord,
        score: LeadScore,
        draft: OutreachDraft,
        evidence_table: list[EvidenceRow],
        structured_evidence: list[StructuredEvidenceItem],
    ) -> CallSheet:
        related = self._related_evidence_for_candidate(lead.name, evidence_table)
        summary_bits = [lead.category or "lead", lead.location or request.geography, lead.demand_signals[0] if lead.demand_signals else "brisket demand signals"]
        company_summary = f"{lead.name} is a {', '.join(bit for bit in summary_bits if bit)} target with {len(related)} supporting evidence rows."
        prior_emails = [draft.first_email, draft.follow_up_1, draft.follow_up_2]
        talking_points = self._unique_values(
            [
                *lead.demand_signals,
                *[self._lead_snippet(row) for row in related[:3]],
            ]
        )[:5]
        if not talking_points:
            talking_points = ["Confirm the business model, event volume, and brisket relevance."]

        objections = [
            "Confirm whether brisket is already on the menu or only a test concept.",
            "Check whether the current demand is mostly catering, events, or walk-in traffic.",
        ]
        if score.pricing_tier_fit < 0.6:
            objections.append("Probe price sensitivity and minimum order constraints.")
        if score.contactability < 0.5:
            objections.append("Verify the best contact path before the next follow-up.")

        next_steps = [
            "Ask how they currently source brisket-related demand.",
            "Confirm whether a catering or event test is the right first offer.",
        ]
        if score.status == "hot":
            next_steps.append("Move directly to a scheduled call and capture objections in the CRM notes.")
        elif score.status == "warm":
            next_steps.append("Send a targeted follow-up with one proof point and one CTA.")

        return CallSheet(
            lead_name=lead.name,
            company_summary=company_summary,
            prior_emails=prior_emails,
            talking_points=talking_points,
            objections=objections,
            next_step_suggestions=next_steps,
            notes=[],
        )

    def _build_personalization_line(self, snippet: str, lead: LeadRecord, request: ValidationRequest) -> str:
        text = snippet.strip()
        if len(text) > 140:
            text = f"{text[:137].rstrip()}..."
        return f"{lead.name}: {text or request.idea}"

    def _compose_email(
        self,
        lead_name: str,
        intro: str,
        why_selected: str,
        brisket_relevance: str,
        offer: str,
        cta: str,
        personalization_lines: list[PersonalizationLine],
    ) -> str:
        body = [intro, why_selected, brisket_relevance, offer]
        for line in personalization_lines[:3]:
            body.append(line.text)
        body.append(cta)
        return "\n\n".join(body).replace("Hi ", f"Hi {lead_name}, ", 1)

    def _compose_follow_up(self, lead_name: str, sequence_number: int, lead: LeadRecord, score: LeadScore) -> str:
        if sequence_number == 1:
            return (
                f"Hi {lead_name}, following up on the brisket outreach. "
                f"Your current fit looks {score.status} based on public evidence, and I can keep this short if it is not a priority."
            )
        return (
            f"Hi {lead_name}, last note from me on this. If brisket demand is on your roadmap, I can send a concise test plan tied to your public signals."
        )

    def _derive_source_sites(self, evidence_table: list[EvidenceRow]) -> list[str]:
        sites = []
        seen: set[str] = set()
        for row in evidence_table:
            if not row.source_url:
                continue
            host = urlparse(row.source_url).netloc.replace("www.", "").strip()
            if not host:
                continue
            normalized = host.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            sites.append(host)
        return sites[:8]

    def _derive_company_types(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        structured_evidence: list[StructuredEvidenceItem],
    ) -> list[str]:
        text = " ".join([request.idea, request.target_customer, request.business_model, *[row.observed_fact for row in evidence_table], *[item.value for item in structured_evidence]]).lower()
        types: list[str] = []
        if _contains_any(text, ("catering", "brisket", "bbq", "barbecue", "restaurant", "menu")):
            types.extend(["restaurants", "caterers", "barbecue operators"])
        if _contains_any(text, ("wedding", "corporate", "event", "party")):
            types.append("event-focused businesses")
        if _contains_any(text, ("bulk", "volume", "minimum order")):
            types.append("bulk food operators")
        if not types:
            types.append("service businesses")
        return self._unique_values(types)

    def _derive_thesis_demand_signals(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        structured_evidence: list[StructuredEvidenceItem] | None,
    ) -> list[str]:
        text = " ".join([request.idea, request.target_customer, *[row.observed_fact for row in evidence_table]])
        if structured_evidence:
            text = f"{text} {' '.join(item.value for item in structured_evidence)}"
        lowered = text.lower()

        signals: list[str] = []
        keyword_map = [
            ("restaurant category", ("restaurant",)),
            ("menu mentions brisket", ("brisket", "menu")),
            ("barbecue keywords", ("bbq", "barbecue", "smoker", "pit")),
            ("catering", ("catering",)),
            ("smoker / pit imagery", ("smoker", "pit")),
            ("reviews mentioning brisket", ("review", "brisket")),
            ("bulk food operations", ("bulk", "minimum order", "volume")),
            ("event hosting", ("event", "wedding", "corporate", "party")),
        ]
        for label, tokens in keyword_map:
            if _contains_any(lowered, tokens):
                signals.append(label)

        if not signals and ("brisket" in lowered or "catering" in lowered):
            signals.extend(["catering", "menu mentions brisket"])

        return self._unique_values(signals)

    def _related_evidence_for_candidate(self, candidate: str, evidence_table: list[EvidenceRow]) -> list[EvidenceRow]:
        cleaned = self._clean_name(candidate).lower()
        related: list[EvidenceRow] = []
        for row in evidence_table:
            haystack = " ".join([row.source_title, row.observed_fact, row.source_url or ""]).lower()
            if cleaned and cleaned in haystack:
                related.append(row)
            elif row.source_type in {"company_website", "directory_listing", "pricing_page", "review_site"} and not related:
                if row.source_title.lower() in cleaned or cleaned in row.source_title.lower():
                    related.append(row)
        if not related:
            related = [row for row in evidence_table if row.source_type in {"company_website", "directory_listing", "pricing_page", "review_site"}][:3]
        return related

    def _lead_snippet(self, row: EvidenceRow) -> str:
        parts = [row.source_title, row.observed_fact]
        if row.source_url:
            parts.append(row.source_url)
        return self._truncate_text(" - ".join(part for part in parts if part), 220)

    def _clean_name(self, candidate: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[|_/]+", " ", candidate or "")).strip(" -")

    def _unique_values(self, values: list[str]) -> list[str]:
        unique: list[str] = []
        seen: set[str] = set()
        for value in values:
            cleaned = self._clean_name(value)
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            unique.append(cleaned)
        return unique

    def _extract_phone(self, text: str) -> str | None:
        match = re.search(r"(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}", text)
        return match.group(0).strip() if match else None

    def _extract_email(self, text: str) -> str | None:
        match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
        return match.group(0).strip() if match else None

    def _infer_lead_location(self, related: list[EvidenceRow], request: ValidationRequest) -> str | None:
        geography = request.geography.strip()
        if geography and geography.lower() not in {"global", "worldwide"}:
            for row in related:
                haystack = f"{row.source_title} {row.observed_fact} {row.source_url or ''}".lower()
                if geography.lower() in haystack:
                    return geography
        return None

    def _infer_lead_category(self, name: str, related: list[EvidenceRow], request: ValidationRequest) -> str:
        text = " ".join([name, request.idea, *[row.observed_fact for row in related]]).lower()
        if _contains_any(text, ("catering", "event", "wedding", "corporate")):
            return "caterer"
        if _contains_any(text, ("bbq", "barbecue", "smoker", "pit", "brisket")):
            return "barbecue restaurant"
        if _contains_any(text, ("restaurant", "menu", "dining")):
            return "restaurant"
        return "local business"

    def _truncate_text(self, text: str, limit: int) -> str:
        cleaned = re.sub(r"\s+", " ", text or "").strip()
        if len(cleaned) <= limit:
            return cleaned
        return f"{cleaned[: max(0, limit - 3)].rstrip()}..."

    def _collect_evidence(self, request: ValidationRequest) -> list[EvidenceRow]:
        rows: list[EvidenceRow] = []
        evidence_index = 1

        def add_row(entry: EvidenceInput | None = None, **kwargs: str) -> None:
            nonlocal evidence_index
            if entry:
                row = EvidenceRow(
                    id=f"E{evidence_index}",
                    source_type=entry.source_type,
                    source_title=entry.source_title,
                    source_url=entry.source_url,
                    observed_fact=entry.observed_fact,
                    strength=entry.strength,
                    evidence_basis=entry.evidence_basis,
                )
            else:
                row = EvidenceRow(id=f"E{evidence_index}", **kwargs)
            rows.append(row)
            evidence_index += 1

        for item in request.evidence_inputs:
            add_row(entry=item)

        add_row(
            source_type="founder_input",
            source_title="Idea statement",
            source_url=None,
            observed_fact=f"Business idea: {request.idea}",
            strength="low",
            evidence_basis="direct_source",
        )

        for competitor in request.competitors:
            competitor_name = _extract_competitor_name(competitor)
            competitor_url = _extract_url(competitor)
            add_row(
                source_type="company_website" if competitor_url else "founder_competitor_claim",
                source_title=competitor_name,
                source_url=competitor_url,
                observed_fact=f"{competitor_name} is listed as an active competitor.",
                strength="medium",
                evidence_basis="direct_source",
            )

        if request.pricing_guess:
            add_row(
                source_type="pricing_hypothesis",
                source_title="Founder pricing guess",
                source_url=None,
                observed_fact=f"Initial pricing guess: {request.pricing_guess}",
                strength="low",
                evidence_basis="direct_source",
            )

        for assumption in request.assumptions:
            add_row(
                source_type="founder_assumption",
                source_title="Assumption",
                source_url=None,
                observed_fact=assumption,
                strength="low",
                evidence_basis="direct_source",
            )

        for constraint in request.constraints:
            add_row(
                source_type="operational_constraint",
                source_title="Constraint",
                source_url=None,
                observed_fact=constraint,
                strength="medium",
                evidence_basis="direct_source",
            )

        return rows

    def _derive_structured_evidence(
        self,
        evidence_table: list[EvidenceRow],
    ) -> list[StructuredEvidenceItem]:
        structured: list[StructuredEvidenceItem] = []
        confidence_caps = {
            "fetched_page": 0.92,
            "direct_source": 0.82,
            "search_snippet": 0.58,
            "unknown": 0.62,
        }

        def add_fact(
            row: EvidenceRow,
            entity: str,
            fact_type: str,
            value: str,
            confidence: float,
        ) -> None:
            confidence_cap = confidence_caps.get(row.evidence_basis, 0.62)
            structured.append(
                StructuredEvidenceItem(
                    id=f"F{len(structured) + 1}",
                    source_id=row.id,
                    source_type=row.source_type,
                    entity=entity or "Unknown entity",
                    fact_type=fact_type,
                    value=value[:140],
                    excerpt=row.observed_fact[:240],
                    url=row.source_url,
                    confidence=min(confidence, confidence_cap),
                    evidence_basis=row.evidence_basis,
                )
            )

        for row in evidence_table:
            base_entity = row.source_title.replace("(fetched)", "").strip()
            if row.source_url:
                host = urlparse(row.source_url).netloc.replace("www.", "").strip()
                host_name = host.split(".")[0].replace("-", " ").title() if host else ""
                if host_name:
                    base_entity = host_name

            fact_type = {
                "pricing_page": "price_point",
                "review_site": "review_signal",
                "customer_complaint": "review_complaint_theme",
                "forum_social": "community_signal",
                "directory_listing": "competitor_positioning",
                "company_website": "competitor_positioning",
                "market_report": "demand_signal",
                "trend_signal": "demand_signal",
                "job_post": "hiring_signal",
                "public_data": "demand_signal",
                "local_editorial": "competitor_positioning",
            }.get(row.source_type, "market_signal")

            confidence = {
                "high": 0.85,
                "medium": 0.65,
                "low": 0.45,
            }.get(row.strength, 0.45)

            add_fact(row, base_entity, fact_type, row.observed_fact, confidence)

            text = row.observed_fact.lower()
            if any(word in text for word in ("premium", "luxury", "high-end")):
                add_fact(row, base_entity, "competitor_positioning", "premium", min(0.95, confidence + 0.08))
            elif any(word in text for word in ("budget", "affordable", "cheap", "value")):
                add_fact(row, base_entity, "competitor_positioning", "budget", min(0.95, confidence + 0.08))

            for match in re.findall(r"\$\s*\d+(?:\.\d+)?(?:\s*(?:/|per)\s*(?:person|head|pp|plate|month|mo|year|yr))?", row.observed_fact, flags=re.I):
                fact_kind = "price_per_head" if any(token in match.lower() for token in ("person", "head", "pp", "plate")) else "price_point"
                add_fact(row, base_entity, fact_kind, match, min(0.95, confidence + 0.08))

            if any(word in text for word in ("late", "delay", "slow", "not on time")):
                add_fact(row, base_entity, "review_complaint_theme", "late_delivery", min(0.95, confidence + 0.05))
            if any(word in text for word in ("inconsistent", "dry", "cold", "quality issue")):
                add_fact(row, base_entity, "review_complaint_theme", "inconsistent_quality", min(0.95, confidence + 0.05))
            if any(word in text for word in ("expensive", "overpriced", "pricey")):
                add_fact(row, base_entity, "review_complaint_theme", "high_price", min(0.95, confidence + 0.05))
            if any(word in text for word in ("delicious", "tender", "great", "excellent")):
                add_fact(row, base_entity, "review_praise_theme", "food_quality", min(0.95, confidence + 0.05))

        return structured

    def _build_evidence_graph_summary(
        self,
        structured_evidence: list[StructuredEvidenceItem],
    ) -> EvidenceGraphSummary:
        if not structured_evidence:
            return EvidenceGraphSummary()

        entities = sorted(
            {
                item.entity
                for item in structured_evidence
                if item.entity and not item.entity.lower().startswith("unknown")
            }
        )

        source_type_counts: dict[str, int] = {}
        for item in structured_evidence:
            source_type_counts[item.source_type] = source_type_counts.get(item.source_type, 0) + 1

        complaint_counts: dict[str, list[str]] = {}
        praise_counts: dict[str, list[str]] = {}
        pricing_buckets: dict[str, dict[str, float | int | list[str] | None]] = {
            "low": {"count": 0, "min": None, "max": None, "ids": []},
            "mid": {"count": 0, "min": None, "max": None, "ids": []},
            "premium": {"count": 0, "min": None, "max": None, "ids": []},
            "enterprise": {"count": 0, "min": None, "max": None, "ids": []},
        }

        entity_positioning: dict[str, list[str]] = {}
        entity_complaints: dict[str, list[str]] = {}
        demand_signals: list[str] = []
        market_size_mentions: list[str] = []

        for item in structured_evidence:
            if item.fact_type == "review_complaint_theme":
                complaint_counts.setdefault(item.value, []).append(item.id)
                entity_complaints.setdefault(item.entity, []).append(item.value)
            elif item.fact_type == "review_praise_theme":
                praise_counts.setdefault(item.value, []).append(item.id)
            elif item.fact_type in {"price_point", "price_per_head"}:
                point = self._extract_price_point(item.value)
                if point is None:
                    continue
                if point < 20:
                    band = "low"
                elif point < 40:
                    band = "mid"
                elif point < 70:
                    band = "premium"
                else:
                    band = "enterprise"

                bucket = pricing_buckets[band]
                bucket["count"] = int(bucket["count"]) + 1
                bucket["ids"].append(item.id)
                bucket_min = bucket["min"]
                bucket_max = bucket["max"]
                bucket["min"] = point if bucket_min is None else min(float(bucket_min), point)
                bucket["max"] = point if bucket_max is None else max(float(bucket_max), point)
            elif item.fact_type == "competitor_positioning":
                entity_positioning.setdefault(item.entity, []).append(item.value.lower())
            elif item.fact_type == "demand_signal":
                demand_signals.append(item.value)
            elif item.fact_type in {"market_size", "tam_estimate", "growth_rate"}:
                market_size_mentions.append(item.value)

        pricing_bands: list[PricingBandSummary] = []
        for band, bucket in pricing_buckets.items():
            count = int(bucket["count"])
            if count <= 0:
                continue
            ids = [str(item_id) for item_id in bucket["ids"]]
            pricing_bands.append(
                PricingBandSummary(
                    band=band,
                    observation_count=count,
                    min_price=float(bucket["min"]) if bucket["min"] is not None else None,
                    max_price=float(bucket["max"]) if bucket["max"] is not None else None,
                    evidence_ids=ids,
                )
            )

        complaint_themes = [
            EvidenceThemeGroup(theme=theme, count=len(ids), evidence_ids=ids[:8])
            for theme, ids in sorted(complaint_counts.items(), key=lambda item: len(item[1]), reverse=True)
        ]
        praise_themes = [
            EvidenceThemeGroup(theme=theme, count=len(ids), evidence_ids=ids[:8])
            for theme, ids in sorted(praise_counts.items(), key=lambda item: len(item[1]), reverse=True)
        ]

        contradictions: list[str] = []
        for entity, positioning_values in entity_positioning.items():
            complaint_values = entity_complaints.get(entity, [])
            joined_positioning = " ".join(positioning_values)
            if "premium" in joined_positioning and any(
                issue in {"inconsistent_quality", "late_delivery"} for issue in complaint_values
            ):
                contradictions.append(
                    f"{entity} positions as premium, but complaints include quality/timing issues."
                )
            if "budget" in joined_positioning and "high_price" in complaint_values:
                contradictions.append(
                    f"{entity} positions as budget, but complaints repeatedly mention high pricing."
                )

        if len(demand_signals) >= 3 and "rising_demand" in " ".join(demand_signals).lower():
            contradictions.append("Multiple demand growth signals detected - verify with market size data.")
        if len(market_size_mentions) >= 2:
            contradictions.append(f"Market size mentions detected: {', '.join(market_size_mentions[:2])}")

        return EvidenceGraphSummary(
            entity_count=len(entities),
            entities=entities[:15],
            pricing_bands=pricing_bands,
            complaint_themes=complaint_themes[:8],
            praise_themes=praise_themes[:8],
            contradictions=contradictions[:8],
            source_type_counts=source_type_counts,
        )

    def _extract_price_point(self, text: str) -> float | None:
        match = re.search(r"\$\s*(\d+(?:\.\d+)?)", text)
        if not match:
            return None
        try:
            return float(match.group(1))
        except ValueError:
            return None

    def _derive_competitor_names(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        structured_evidence: list[StructuredEvidenceItem],
    ) -> list[str]:
        names: list[str] = []
        seen: set[str] = set()

        def add(name: str) -> None:
            candidate = name.strip()
            if not candidate:
                return
            lowered = candidate.lower()
            if lowered in seen:
                return
            seen.add(lowered)
            names.append(candidate)

        for competitor in request.competitors:
            add(_extract_competitor_name(competitor))

        for fact in structured_evidence:
            if not fact.entity:
                continue
            if fact.entity.lower().startswith("unknown"):
                continue
            if fact.fact_type in {
                "competitor_positioning",
                "price_point",
                "price_per_head",
                "review_complaint_theme",
                "review_praise_theme",
            }:
                add(fact.entity)

        for row in evidence_table:
            if row.source_type not in {"company_website", "directory_listing", "review_site", "pricing_page"}:
                continue

            if row.source_url:
                host = urlparse(row.source_url).netloc.replace("www.", "").strip()
                if host:
                    host_name = host.split(".")[0].replace("-", " ").title()
                    add(host_name)

            title = row.source_title.replace("(fetched)", "").strip()
            if title and not title.lower().startswith("supplemental web signal"):
                add(title)

            if len(names) >= 8:
                break

        return names[:8]

    def _build_competitor_map(self, competitors: list[str]) -> list[CompetitorEntry]:
        if not competitors:
            return []

        competition_count = len(competitors)
        threat = "high" if competition_count >= 6 else "medium" if competition_count >= 3 else "low"
        rows: list[CompetitorEntry] = []

        for competitor in competitors:
            normalized = competitor.lower()
            if _contains_any(normalized, ("enterprise", "mid-market", "global")):
                positioning = "Established at larger customer segment"
            elif _contains_any(normalized, ("local", "regional", "small business", "bbq", "catering")):
                positioning = "Focused on local or SMB segment"
            else:
                positioning = "Positioning not explicit from available evidence"

            if "$" in competitor or re.search(r"\d+\s*/\s*(mo|month|year|yr)", normalized):
                price_signal = "explicit price mentioned"
            elif "free" in normalized or "freemium" in normalized:
                price_signal = "free or freemium signal"
            else:
                price_signal = "unknown"

            rows.append(
                CompetitorEntry(
                    competitor=competitor,
                    inferred_positioning=positioning,
                    inferred_price_signal=price_signal,
                    threat_level=threat,
                )
            )

        return rows

    def _build_demand_signals(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        structured_evidence: list[StructuredEvidenceItem],
    ) -> list[DemandSignal]:
        high_signal_sources = {
            "review_site",
            "customer_complaint",
            "forum_social",
            "market_report",
            "public_data",
            "job_post",
            "trend_signal",
            "directory_listing",
        }
        signals: list[DemandSignal] = []

        for evidence in evidence_table:
            if evidence.source_type in high_signal_sources:
                signals.append(
                    DemandSignal(
                        signal=evidence.observed_fact,
                        strength=evidence.strength,
                        source_refs=[evidence.id],
                    )
                )

        for fact in structured_evidence:
            if fact.fact_type not in {
                "demand_signal",
                "review_complaint_theme",
                "review_praise_theme",
                "event_type_served",
                "hiring_signal",
            }:
                continue
            strength = "high" if fact.confidence >= 0.8 else "medium" if fact.confidence >= 0.6 else "low"
            signals.append(
                DemandSignal(
                    signal=f"{fact.entity}: {fact.fact_type} -> {fact.value}",
                    strength=strength,
                    source_refs=[fact.id],
                )
            )

        if not signals:
            text_blob = " ".join([request.idea, *request.assumptions]).lower()
            if _contains_any(
                text_blob,
                (
                    "manual",
                    "slow",
                    "expensive",
                    "error",
                    "overloaded",
                    "pain",
                    "problem",
                ),
            ):
                fallback_signal = "Input language suggests recurring problem pressure."
                if request.profile == "local_business" or request.template == "restaurant":
                    fallback_signal = (
                        "Input language suggests event-driven catering demand, "
                        "but external booking evidence is still limited."
                    )
                signals.append(
                    DemandSignal(
                        signal=fallback_signal,
                        strength="medium",
                        source_refs=[evidence.id for evidence in evidence_table[:1]],
                    )
                )

            competitor_refs = [
                evidence.id
                for evidence in evidence_table
                if evidence.source_type in {"company_website", "directory_listing"}
            ]
            if competitor_refs:
                signals.append(
                    DemandSignal(
                        signal="Named competitors suggest an existing demand baseline.",
                        strength="medium",
                        source_refs=competitor_refs[:3],
                    )
                )

            if request.pricing_guess:
                pricing_ref = next(
                    (
                        evidence.id
                        for evidence in evidence_table
                        if evidence.source_type == "pricing_hypothesis"
                    ),
                    None,
                )
                signals.append(
                    DemandSignal(
                        signal="A concrete pricing hypothesis enables willingness-to-pay testing.",
                        strength="low",
                        source_refs=[pricing_ref] if pricing_ref else [],
                    )
                )

        return signals[:6] or [
            DemandSignal(
                signal="Insufficient evidence to establish demand direction.",
                strength="low",
                source_refs=[],
            )
        ]

    def _score_dimensions(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        competitor_map: list[CompetitorEntry],
        profile: ProfileConfig,
    ) -> list[DimensionScore]:
        if profile.name == "local_business" and request.template == "restaurant":
            return self._score_restaurant_dimensions(request, evidence_table, competitor_map)
        return self._score_generic_dimensions(request, evidence_table, competitor_map, profile)

    def _refine_scores_with_llm(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        raw_scores: list[DimensionScore],
        research_stage: str,
    ) -> list[DimensionScore]:
        if research_stage in {"brief_only", "search_results_only"} or not self._llm_client.enabled:
            return raw_scores

        external_rows = [
            row for row in evidence_table if row.source_type not in FOUNDER_SOURCE_TYPES
        ]
        if not external_rows:
            return raw_scores

        supported_dimensions = [score.dimension for score in raw_scores]
        supported_lookup = set(supported_dimensions)
        if not supported_dimensions:
            return raw_scores

        evidence_rows = [
            {
                "source_type": row.source_type,
                "source_title": row.source_title,
                "observed_fact": row.observed_fact,
                "strength": row.strength,
            }
            for row in external_rows[:14]
        ]

        system_prompt = (
            "You are a market validation analyst. Score only the provided dimensions on a 1-10 scale "
            "using the supplied evidence. Return strict JSON with key dimension_scores containing "
            "objects with fields dimension, score, rationale. Do not add extra dimensions."
        )
        user_prompt = json.dumps(
            {
                "idea": request.idea,
                "target_customer": request.target_customer,
                "geography": request.geography,
                "business_model": request.business_model,
                "dimensions": supported_dimensions,
                "evidence_rows": evidence_rows,
            },
            ensure_ascii=True,
        )

        llm_payload = self._llm_client.chat_json(system_prompt=system_prompt, user_prompt=user_prompt)
        if not isinstance(llm_payload, dict):
            return raw_scores

        dimension_scores = llm_payload.get("dimension_scores")
        if not isinstance(dimension_scores, list):
            return raw_scores

        refined: dict[str, tuple[float, str]] = {}
        for row in dimension_scores:
            if not isinstance(row, dict):
                continue

            dimension = row.get("dimension")
            if not isinstance(dimension, str):
                continue
            normalized_dimension = dimension.strip()
            if normalized_dimension not in supported_lookup:
                continue

            score_value = row.get("score")
            try:
                parsed_score = _clamp(float(score_value))
            except (TypeError, ValueError):
                continue

            rationale_value = row.get("rationale")
            rationale = rationale_value.strip() if isinstance(rationale_value, str) else ""
            if not rationale:
                rationale = f"LLM-refined estimate for {_clean_label(normalized_dimension)}."

            refined[normalized_dimension] = (round(parsed_score, 2), rationale[:240])

        if not refined:
            return raw_scores

        merged_scores: list[DimensionScore] = []
        for score in raw_scores:
            updated = refined.get(score.dimension)
            if not updated:
                merged_scores.append(score)
                continue

            llm_score, llm_rationale = updated
            base_score = score.score if score.score is not None else llm_score
            blended_score = round(_clamp((0.4 * base_score) + (0.6 * llm_score)), 2)

            merged_scores.append(
                DimensionScore(
                    dimension=score.dimension,
                    score=blended_score,
                    weight=score.weight,
                    rationale=llm_rationale,
                )
            )

        return merged_scores

    def _score_restaurant_dimensions(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        competitor_map: list[CompetitorEntry],
    ) -> list[DimensionScore]:
        combined_text = " ".join(
            [
                request.idea,
                request.target_customer,
                request.business_model,
                request.pricing_guess or "",
                *request.assumptions,
                *request.constraints,
                *[row.observed_fact for row in evidence_table],
            ]
        ).lower()

        review_count = sum(1 for row in evidence_table if row.source_type in {"review_site", "forum_social", "customer_complaint"})
        pricing_count = sum(1 for row in evidence_table if row.source_type == "pricing_page")
        directory_count = sum(1 for row in evidence_table if row.source_type in {"directory_listing", "company_website"})
        competitor_count = len(competitor_map)
        event_hits = sum(
            1
            for word in ("wedding", "corporate", "event", "festival", "office", "party", "backyard")
            if word in combined_text
        )
        repeat_hits = sum(
            1
            for word in ("recurring", "repeat", "weekly", "monthly", "seasonal", "ongoing")
            if word in combined_text
        )
        geography_bonus = 1.0 if request.geography.lower() not in {"global", "worldwide"} else -1.0

        local_demand_density = _clamp(3.5 + geography_bonus + min(2.5, review_count * 0.8) + min(2.0, event_hits * 0.5))
        catering_event_frequency = _clamp(3.0 + min(3.5, event_hits * 0.8) + (1.0 if "catering" in combined_text else 0.0))

        pricing_points = []
        for row in evidence_table:
            if row.source_type in {"pricing_page", "review_site", "directory_listing", "company_website"}:
                pricing_points.extend(_extract_price_points(row.observed_fact))
        price_per_head_viability = _clamp(
            3.0
            + min(3.0, pricing_count * 1.2)
            + (1.0 if request.pricing_guess else 0.0)
            + (1.0 if pricing_points else 0.0)
        )

        saturation_pressure = max(1.0, float(max(competitor_count, directory_count)))
        competitor_saturation = _clamp(8.0 - min(5.0, saturation_pressure))

        complexity_penalty = min(3.0, float(len(request.constraints)))
        if _contains_any(combined_text, ("delivery", "staffing", "logistics", "cold chain", "labor")):
            complexity_penalty += 1.0
        operational_complexity = _clamp(7.0 - complexity_penalty)

        repeat_event_potential = _clamp(3.0 + min(2.5, repeat_hits * 1.1) + min(2.5, event_hits * 0.6) + (1.0 if "corporate" in combined_text else 0.0))

        weighted_scores = {
            "local_demand_density": (local_demand_density, 0.2, "Estimated from local signals, reviews, and event-oriented demand language."),
            "catering_event_frequency": (catering_event_frequency, 0.18, "Estimated from references to weddings, corporate events, parties, and festivals."),
            "price_per_head_viability": (price_per_head_viability, 0.18, "Estimated from observed pricing sources and ability to define package pricing."),
            "competitor_saturation": (competitor_saturation, 0.16, "Higher score means less crowding among local catering competitors."),
            "operational_complexity": (operational_complexity, 0.14, "Higher score means lower complexity in delivery, staffing, and service consistency."),
            "repeat_event_potential": (repeat_event_potential, 0.14, "Estimated from recurring event opportunities and repeat booking potential."),
        }

        scores: list[DimensionScore] = []
        for dimension, (score_value, weight, rationale) in weighted_scores.items():
            scores.append(
                DimensionScore(
                    dimension=dimension,
                    score=round(score_value, 2),
                    weight=weight,
                    rationale=rationale,
                )
            )
        return scores

    def _score_generic_dimensions(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        competitor_map: list[CompetitorEntry],
        profile: ProfileConfig,
    ) -> list[DimensionScore]:
        idea_text = request.idea.lower()
        target_text = request.target_customer.lower()
        model_text = request.business_model.lower()
        combined_text = " ".join(
            [
                request.idea,
                request.target_customer,
                request.business_model,
                request.pricing_guess or "",
                *request.assumptions,
                *request.constraints,
                *[row.observed_fact for row in evidence_table],
            ]
        ).lower()

        pain_hits = sum(
            1
            for keyword in (
                "manual",
                "slow",
                "error",
                "costly",
                "expensive",
                "urgent",
                "compliance",
                "overloaded",
                "friction",
                "problem",
            )
            if keyword in combined_text
        )
        external_pain_evidence = any(
            evidence.source_type in {"review_site", "customer_complaint", "forum_social"}
            for evidence in evidence_table
        )
        pain_score = _clamp(4.0 + min(4, pain_hits) + (1.0 if external_pain_evidence else -1.0))

        generic_target = _contains_any(
            target_text,
            ("everyone", "anyone", "all businesses", "general public", "all users"),
        )
        target_tokens = re.findall(r"[a-z0-9]+", target_text)
        role_markers = ("owner", "manager", "director", "lead", "operator", "founder", "team")
        customer_score = _clamp(
            3.0
            + min(4, len(target_tokens) // 3)
            + (1.0 if _contains_any(target_text, role_markers) else 0.0)
            - (2.0 if generic_target else 0.0)
        )

        competitor_price_signals = sum(
            1 for item in competitor_map if item.inferred_price_signal != "unknown"
        )
        external_pricing = sum(1 for row in evidence_table if row.source_type == "pricing_page")
        wtp_score = _clamp(
            3.0
            + (2.0 if request.pricing_guess else 0.0)
            + min(2, competitor_price_signals)
            + min(2, external_pricing)
            + (1.0 if "b2b" in model_text else 0.0)
            - (1.0 if "free" in (request.pricing_guess or "").lower() else 0.0)
        )

        competitor_count = len(competitor_map)
        if competitor_count == 0:
            competition_score = 5.0
        else:
            competition_score = _clamp(8.0 - min(5, competitor_count))
        if _contains_any(combined_text, ("niche", "specialized", "vertical")):
            competition_score = _clamp(competition_score + 1.0)

        differentiation_hits = sum(
            1
            for keyword in (
                "unique",
                "faster",
                "cheaper",
                "specialized",
                "vertical",
                "automation",
                "ai",
                "exclusive",
                "integrated",
            )
            if keyword in combined_text
        )
        differentiation_score = _clamp(3.0 + min(4, differentiation_hits))
        if competitor_count >= 5 and differentiation_hits == 0:
            differentiation_score = _clamp(differentiation_score - 2.0)

        has_distribution_signal = _contains_any(
            combined_text,
            ("seo", "ads", "referral", "partnership", "outbound", "sales", "audience", "community"),
        )
        distribution_score = 5.0
        if _contains_any(model_text, ("marketplace", "two-sided")):
            distribution_score -= 2.0
        if _contains_any(model_text, ("local", "service", "agency")) and request.geography.lower() != "global":
            distribution_score += 1.0
        if request.geography.lower() in {"global", "worldwide"}:
            distribution_score -= 1.0
        if has_distribution_signal:
            distribution_score += 1.0
        else:
            distribution_score -= 1.0
        distribution_score = _clamp(distribution_score)

        retention_score = 4.0
        if _contains_any(model_text, ("subscription", "saas", "retainer", "membership", "recurring")):
            retention_score += 3.0
        if _contains_any(combined_text, ("repeat", "reorder", "consumable", "ongoing")):
            retention_score += 2.0
        if _contains_any(model_text, ("one-time", "project-based")):
            retention_score -= 1.0
        retention_score = _clamp(retention_score)

        operational_score = 6.0
        operational_score -= min(3.0, float(len(request.constraints)))
        if _contains_any(combined_text, ("hardware", "inventory", "manufacturing", "logistics", "fulfillment")):
            operational_score -= 2.0
        if _contains_any(combined_text, ("software-only", "digital")):
            operational_score += 1.0
        operational_score = _clamp(operational_score)

        regulatory_score = 7.0
        if _contains_any(
            combined_text,
            (
                "health",
                "medical",
                "finance",
                "fintech",
                "insurance",
                "legal",
                "children",
                "government",
                "compliance",
            ),
        ):
            regulatory_score -= 3.0
        if request.geography.lower() in {"global", "worldwide", "eu", "european union"}:
            regulatory_score -= 1.0
        regulatory_score = _clamp(regulatory_score)

        speed_score = 5.0
        if _contains_any(model_text, ("service", "agency", "consulting", "local")):
            speed_score += 2.0
        if _contains_any(model_text, ("saas", "subscription")):
            speed_score += 1.0
        if _contains_any(idea_text, ("hardware", "manufacturing", "marketplace")):
            speed_score -= 2.0
        if regulatory_score <= 4.0:
            speed_score -= 1.0
        if operational_score <= 4.0:
            speed_score -= 1.0
        speed_score = _clamp(speed_score)

        brand_trust_score = 5.0
        if _contains_any(combined_text, ("established", "years", "experience", "trusted", "reputable")):
            brand_trust_score += 2.0
        if _contains_any(combined_text, ("new", "startup", "launching", "first")):
            brand_trust_score -= 1.0
        brand_trust_score = _clamp(brand_trust_score)

        team_capability_score = 5.0
        if request.competitors:
            team_capability_score += 1.0
        if _contains_any(combined_text, ("expert", "specialist", "veteran", "proven track record")):
            team_capability_score += 2.0
        if not request.assumptions:
            team_capability_score -= 1.0
        team_capability_score = _clamp(team_capability_score)

        tech_feasibility_score = 6.0
        if _contains_any(model_text, ("saas", "software", "api", "platform")):
            tech_feasibility_score += 2.0
        if _contains_any(combined_text, ("complex", "custom", "novel", "untested")):
            tech_feasibility_score -= 2.0
        if _contains_any(combined_text, ("existing", "proven", "off-the-shelf", "established")):
            tech_feasibility_score += 1.0
        tech_feasibility_score = _clamp(tech_feasibility_score)

        raw_scores: dict[str, tuple[float, str]] = {
            "pain_intensity": (
                round(pain_score, 2),
                "Estimated from problem-language intensity and customer complaint evidence.",
            ),
            "customer_clarity": (
                round(customer_score, 2),
                "Estimated from role specificity and segmentation detail in target customer.",
            ),
            "willingness_to_pay": (
                round(wtp_score, 2),
                "Estimated from pricing hypothesis strength and observable pricing signals.",
            ),
            "competition_intensity": (
                round(competition_score, 2),
                "Higher score means lower crowding or better whitespace.",
            ),
            "differentiation_potential": (
                round(differentiation_score, 2),
                "Estimated from uniqueness signals versus competitive density.",
            ),
            "distribution_ease": (
                round(distribution_score, 2),
                "Estimated from channel readiness, geography scope, and model complexity.",
            ),
            "retention_repeatability": (
                round(retention_score, 2),
                "Estimated from recurring value mechanics in the business model.",
            ),
            "operational_complexity": (
                round(operational_score, 2),
                "Higher score means lower operational burden to deliver consistently.",
            ),
            "regulatory_friction": (
                round(regulatory_score, 2),
                "Higher score means lower compliance or legal friction.",
            ),
            "speed_to_first_revenue": (
                round(speed_score, 2),
                "Estimated from go-to-market path length and delivery constraints.",
            ),
            "brand_trust": (
                round(brand_trust_score, 2),
                "Estimated from brand signals and years in business.",
            ),
            "team_capability": (
                round(team_capability_score, 2),
                "Estimated from team/experience signals in evidence.",
            ),
            "tech_feasibility": (
                round(tech_feasibility_score, 2),
                "Estimated from technology complexity and existing solutions.",
            ),
        }

        scored_dimensions: list[DimensionScore] = []
        for dimension in DIMENSIONS:
            score_value, rationale = raw_scores[dimension]
            scored_dimensions.append(
                DimensionScore(
                    dimension=dimension,
                    score=score_value,
                    weight=round(profile.weights.get(dimension, 0.1), 6),
                    rationale=rationale,
                )
            )

        return scored_dimensions

    def _assess_source_coverage(self, evidence_table: list[EvidenceRow]) -> SourceCoverageSummary:
        external_rows = [row for row in evidence_table if row.source_type not in FOUNDER_SOURCE_TYPES]
        fetched_evidence_count = sum(1 for row in external_rows if row.evidence_basis == "fetched_page")
        snippet_evidence_count = sum(1 for row in external_rows if row.evidence_basis == "search_snippet")
        direct_evidence_count = sum(1 for row in external_rows if row.evidence_basis == "direct_source")

        competitor_keys = {
            (row.source_url or row.source_title).lower()
            for row in external_rows
            if row.source_type in {"company_website", "directory_listing", "local_editorial"}
        }
        pricing_keys = {
            (row.source_url or row.source_title).lower()
            for row in external_rows
            if row.source_type == "pricing_page"
        }
        review_keys = {
            (row.source_url or row.source_title).lower()
            for row in external_rows
            if row.source_type in {"review_site", "forum_social", "customer_complaint"}
        }

        competitor_sources = len(competitor_keys)
        pricing_sources = len(pricing_keys)
        review_community_sources = len(review_keys)

        missing_requirements: list[str] = []
        if competitor_sources < 3:
            missing_requirements.append(
                f"Need at least 3 competitor sources (currently {competitor_sources})."
            )
        if pricing_sources < 2:
            missing_requirements.append(
                f"Need at least 2 pricing sources (currently {pricing_sources})."
            )
        if review_community_sources < 2:
            missing_requirements.append(
                f"Need at least 2 review/community sources (currently {review_community_sources})."
            )

        return SourceCoverageSummary(
            external_evidence_count=len(external_rows),
            competitor_sources=competitor_sources,
            pricing_sources=pricing_sources,
            review_community_sources=review_community_sources,
            fetched_evidence_count=fetched_evidence_count,
            snippet_evidence_count=snippet_evidence_count,
            direct_evidence_count=direct_evidence_count,
            meets_minimum_external_evidence=not missing_requirements,
            missing_requirements=missing_requirements,
        )

    def _calculate_evidence_coverage_score(self, summary: SourceCoverageSummary) -> float:
        competitor_ratio = min(1.0, summary.competitor_sources / float(summary.threshold_competitor_sources))
        pricing_ratio = min(1.0, summary.pricing_sources / float(summary.threshold_pricing_sources))
        review_ratio = min(1.0, summary.review_community_sources / float(summary.threshold_review_community_sources))
        external_ratio = min(1.0, summary.external_evidence_count / 12.0)
        total_external = max(1, summary.external_evidence_count)
        quality_ratio = min(
            1.0,
            (
                summary.fetched_evidence_count
                + (0.7 * summary.direct_evidence_count)
                + (0.25 * summary.snippet_evidence_count)
            )
            / float(total_external),
        )

        score = 100.0 * (
            0.3 * competitor_ratio
            + 0.2 * pricing_ratio
            + 0.2 * review_ratio
            + 0.1 * external_ratio
            + 0.2 * quality_ratio
        )
        return round(score, 2)

    def _determine_research_stage(
        self,
        source_coverage: SourceCoverageSummary,
        research_diagnostics: dict[str, Any] | None = None,
    ) -> str:
        """Fix 3 & 6: Stricter stage determination."""
        if source_coverage.external_evidence_count == 0:
            return "brief_only"

        diagnostics = research_diagnostics or {}
        fetch_success_count = source_coverage.fetched_evidence_count
        
        # If we have external results but zero fetches, it's snippet-only
        if fetch_success_count == 0 and source_coverage.snippet_evidence_count > 0:
            return "search_results_only"

        fetched_ratio = fetch_success_count / float(max(1, source_coverage.external_evidence_count))
        
        # Require strict thresholds for complete research
        if (
            source_coverage.meets_minimum_external_evidence
            and fetch_success_count >= 4
            and fetched_ratio >= 0.35
        ):
            return "complete_research"
            
        return "partial_research"

    def _apply_score_stage(self, scores: list[DimensionScore], research_stage: str) -> list[DimensionScore]:
        staged: list[DimensionScore] = []
        for score in scores:
            if research_stage in {"brief_only", "search_results_only"}:
                staged.append(
                    DimensionScore(
                        dimension=score.dimension,
                        score=None,
                        status="insufficient_evidence",
                        provisional_based_on=(
                            "founder_input_only"
                            if research_stage == "brief_only"
                            else "partial_external_evidence"
                        ),
                        weight=score.weight,
                        rationale=score.rationale,
                    )
                )
                continue
            if research_stage == "partial_research":
                staged.append(
                    DimensionScore(
                        dimension=score.dimension,
                        score=score.score,
                        status="provisional",
                        provisional_based_on="partial_external_evidence",
                        weight=score.weight,
                        rationale=score.rationale,
                    )
                )
                continue

            staged.append(
                DimensionScore(
                    dimension=score.dimension,
                    score=score.score,
                    status="scored",
                    provisional_based_on=None,
                    weight=score.weight,
                    rationale=score.rationale,
                )
            )
        return staged

    def _apply_market_score_stage(self, market_score: float, research_stage: str) -> tuple[float | None, str, str]:
        if research_stage == "brief_only":
            return None, "insufficient_evidence", "founder_input_only"
        if research_stage == "search_results_only":
            return None, "insufficient_evidence", "partial_external_evidence"
        if research_stage == "partial_research":
            return market_score, "provisional", "partial_external_evidence"
        return market_score, "scored", "evidence_backed"

    def _build_research_plan(
        self,
        request: ValidationRequest,
        source_coverage: SourceCoverageSummary,
        research_stage: str,
    ) -> list[str]:
        if research_stage == "complete_research":
            return [
                "Evidence thresholds are met. Proceed to segment-specific validation experiments and pilot conversion tests.",
                "Refine positioning and pricing with real buyer interviews before scale decisions.",
            ]

        if research_stage == "search_results_only":
            return [
                "Current run only contains search-result snippets; destination-page fetching failed or returned no usable content.",
                "Retry collection with smaller query batches and stronger backoff to recover from rate limits/timeouts.",
                "Fetch at least 4 destination pages that include explicit competitor, pricing, and review evidence before scoring.",
                "Preserve source URLs and include short quoted excerpts from fetched pages for each critical fact.",
            ]

        plan: list[str] = [
            "Collect at least 3 distinct competitor sources with links and positioning notes.",
            "Collect at least 2 pricing sources with explicit package or per-head prices.",
            "Collect at least 2 review/community sources capturing customer praise and complaints.",
        ]

        if request.profile == "local_business" or request.template == "restaurant":
            plan.extend(
                [
                    "Map local personas: office managers, wedding planners, private party hosts, and venue managers.",
                    "Capture event-type demand split (corporate lunches, weddings, parties, festivals).",
                ]
            )

        if source_coverage.external_evidence_count == 0:
            plan.insert(0, "Current run is brief-only. Fetch external web evidence before any hard market scoring.")

        return plan[:6]

    def _derive_unknowns(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        profile: ProfileConfig,
        source_coverage: SourceCoverageSummary,
        research_diagnostics: dict[str, Any] | None = None,
    ) -> list[str]:
        unknowns: list[str] = []
        evidence_types = {row.source_type for row in evidence_table}

        if not request.competitors:
            unknowns.append(
                "No explicit competitor set was provided; competition scoring has low certainty."
            )
        if not request.pricing_guess:
            unknowns.append(
                "No pricing guess was provided; willingness-to-pay is weakly evidenced."
            )
        if not request.evidence_inputs:
            unknowns.append(
                "No external evidence sources supplied; analysis relies mostly on founder inputs."
            )

        if not source_coverage.meets_minimum_external_evidence:
            unknowns.append(
                "External evidence coverage is below required minimum; verdict is provisional."
            )
            unknowns.extend(source_coverage.missing_requirements)

        if source_coverage.external_evidence_count > 0 and source_coverage.fetched_evidence_count == 0:
            unknowns.append(
                "External evidence is snippet-only; no fetched destination-page evidence was available."
            )

        diagnostics = research_diagnostics or {}
        queries_attempted = diagnostics.get("queries_attempted")
        raw_source_count = diagnostics.get("raw_source_count")
        fetch_attempted = diagnostics.get("fetch_attempted")
        fetch_success = diagnostics.get("fetch_success")
        if isinstance(queries_attempted, int) and isinstance(raw_source_count, int):
            if queries_attempted > 0 and raw_source_count == 0:
                unknowns.append(
                    f"External source discovery attempted {queries_attempted} queries but collected 0 sources."
                )

        if isinstance(fetch_attempted, int) and isinstance(fetch_success, int):
            if fetch_attempted > 0 and fetch_success == 0:
                unknowns.append(
                    f"Fetch attempted {fetch_attempted} times but 0 pages were successfully parsed."
                )

        return unknowns[:8]

    def _derive_risks(self, scores: list[DimensionScore]) -> list[str]:
        risks: list[str] = []
        for score in scores:
            if score.score is not None and score.score < 4.0:
                label = _clean_label(score.dimension)
                risks.append(f"Low {label} ({score.score}/10): {score.rationale}")

        if not risks:
            risks.append("No critical low-score risks identified from available evidence.")

        return risks[:5]

    def _calculate_confidence(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        evidence_coverage_score: float,
        unknowns: list[str],
        source_coverage: SourceCoverageSummary,
        structured_evidence: list[StructuredEvidenceItem],
        evidence_graph_summary: EvidenceGraphSummary,
        research_diagnostics: dict[str, Any] | None = None,
    ) -> float:
        base = 25.0
        coverage_contribution = evidence_coverage_score * 0.45
        unknowns_penalty = min(25.0, len(unknowns) * 4.0)
        
        entity_bonus = min(10.0, float(evidence_graph_summary.entity_count) * 1.5)
        structured_bonus = min(10.0, float(len(structured_evidence)) * 0.15)
        
        diagnostics = research_diagnostics or {}
        fetch_success = diagnostics.get("fetch_success", 0)
        fetch_bonus = min(10.0, float(fetch_success) * 2.0) if isinstance(fetch_success, (int, float)) else 0.0

        confidence = base + coverage_contribution - unknowns_penalty + entity_bonus + structured_bonus + fetch_bonus
        return round(max(5.0, min(100.0, confidence)), 2)

    def _verdict(
        self,
        market_score: float,
        confidence: float,
        source_coverage: SourceCoverageSummary,
        research_stage: str,
    ) -> str:
        if research_stage in {"brief_only", "search_results_only"} or confidence < 25.0:
            return "insufficient_evidence"

        if market_score >= 7.5 and confidence >= 65.0:
            return "promising"
        if market_score >= 5.5 and confidence >= 45.0:
            return "mixed"
        if market_score < 4.5:
            return "weak"
        
        return "mixed"

    def _recommend_experiments(
        self,
        scores: list[DimensionScore],
        request: ValidationRequest,
        source_coverage: SourceCoverageSummary,
    ) -> list[ExperimentRecommendation]:
        experiments: list[ExperimentRecommendation] = []
        
        low_scores = sorted(
            [s for s in scores if s.score is not None and s.score < 6.0],
            key=lambda s: s.score or 0.0,
        )

        for score in low_scores[:2]:
            if score.dimension == "willingness_to_pay" or score.dimension == "price_per_head_viability":
                experiments.append(
                    ExperimentRecommendation(
                        name="Pricing Page / Package Test",
                        hypothesis=f"Target customers will accept a price point near {request.pricing_guess or 'market average'}.",
                        method="Create a simple landing page or PDF menu with explicit pricing and a 'Book Now' or 'Join Waitlist' button.",
                        success_criteria="5% conversion rate from targeted traffic to button click.",
                        priority=1,
                        effort="medium",
                    )
                )
            elif score.dimension == "pain_intensity":
                experiments.append(
                    ExperimentRecommendation(
                        name="Problem Validation Interviews",
                        hypothesis="Customers view the current brisket alternatives as a significant pain point.",
                        method="Conduct 5-10 deep interviews with the target persona focusing on their last 3 purchases.",
                        success_criteria="70% of interviewees rank the problem as a top-3 priority.",
                        priority=1,
                        effort="medium",
                    )
                )
            elif score.dimension == "distribution_ease":
                experiments.append(
                    ExperimentRecommendation(
                        name="Channel Smoke Test",
                        hypothesis="We can acquire target customers for less than 20% of the initial order value.",
                        method="Run a $200 ad spend or 100-lead outbound campaign to the primary distribution channel.",
                        success_criteria="Cost per lead (CPL) below the target acquisition threshold.",
                        priority=2,
                        effort="low",
                    )
                )

        if not experiments:
            experiments.append(
                ExperimentRecommendation(
                    name="MVP Pre-sale / Letter of Intent",
                    hypothesis="Early adopters are willing to commit to a purchase before the full service is launched.",
                    method="Present the solution to 5 qualified leads and ask for a deposit or signed LOI.",
                    success_criteria="At least 1 signed LOI or paid deposit.",
                    priority=1,
                    effort="high",
                )
            )

        return experiments[:3]

    def _score_by_dimension(self, scores: list[DimensionScore], dimension: str, fallback: float | None) -> float | None:
        for score in scores:
            if score.dimension == dimension:
                return score.score
        return fallback

    def _target_specificity_score(self, target: str) -> float:
        tokens = re.findall(r"[a-z0-9]+", target.lower())
        if len(tokens) <= 2:
            return 3.0
        if len(tokens) <= 5:
            return 5.0
        return 7.5

    def _target_customer_clarity_text(self, score: float | None, request: ValidationRequest) -> str:
        if score is None:
            return "Insufficient evidence to judge customer clarity."
        if score >= 7.5:
            return f"Highly specific target: {request.target_customer}. Role and segment markers detected."
        if score >= 5.0:
            return f"Moderately defined target: {request.target_customer}. Some generic tokens present."
        return f"Generic or underspecified target: {request.target_customer}. Needs vertical or role-based narrowing."

    def _distribution_difficulty_text(self, score: float | None, request: ValidationRequest, stage: str) -> str:
        if score is None or stage == "brief_only":
            return "Insufficient evidence to assess distribution difficulty."
        if score >= 7.5:
            return "Low difficulty: Clear channels and local/niche focus identified."
        if score >= 5.0:
            return "Medium difficulty: Requires active channel testing and outbound effort."
        return "High difficulty: Crowded market or indirect distribution path."

    def _pricing_snapshot(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        source_coverage: SourceCoverageSummary,
    ) -> str:
        if source_coverage.pricing_sources == 0 and not request.pricing_guess:
            return "No pricing signals found in external evidence or founder input."
        
        prices = []
        for row in evidence_table:
            prices.extend(_extract_price_points(row.observed_fact))
        
        if not prices:
            return f"Founder guess: {request.pricing_guess or 'Unknown'}. No external price points extracted."
            
        avg_price = sum(prices) / len(prices)
        return f"Average observed: ${avg_price:.2f}. Range: ${min(prices):.2f} - ${max(prices):.2f}. Count: {len(prices)}."

    def _pricing_reality_check(
        self,
        request: ValidationRequest,
        evidence_table: list[EvidenceRow],
        score: float | None,
    ) -> str:
        if score is None:
            return "Insufficient evidence for pricing reality check."
        if score >= 7.5:
            return "Strong alignment: Founder pricing guess is supported by observed competitor data."
        if score >= 5.0:
            return "Mixed alignment: Founder guess is within market range but lacks explicit premium/value anchors."
        return "Poor alignment: Founder guess significantly diverges from observed market price points."

    def _review_sentiment_summary(
        self,
        evidence_table: list[EvidenceRow],
        source_coverage: SourceCoverageSummary,
    ) -> str:
        if source_coverage.review_community_sources == 0:
            return "No community or review evidence found to establish sentiment."
        
        complaints = sum(1 for row in evidence_table if row.source_type == "customer_complaint")
        reviews = sum(1 for row in evidence_table if row.source_type == "review_site")
        
        if complaints > reviews:
            return "Mainly critical: Community signals highlight recurring pain and service gaps."
        if reviews > 0:
            return "Mixed/Positive: Active review volume detected with identifiable praise and complaint themes."
        return "Neutral: Community presence detected but sentiment polarity is unclear."

    def _build_market_summary(
        self,
        scores: list[DimensionScore],
        verdict: str,
        confidence: float,
        source_coverage: SourceCoverageSummary,
        research_stage: str,
        graph_summary: EvidenceGraphSummary,
    ) -> str:
        if research_stage == "brief_only":
            return "Analysis is based entirely on founder input. External validation is required."
        
        top_scores = sorted(
            [s for s in scores if s.score is not None],
            key=lambda s: s.score or 0.0,
            reverse=True,
        )[:2]
        
        strengths = ", ".join([_clean_label(s.dimension) for s in top_scores])
        summary = f"The market validation verdict is '{verdict}' with {confidence}% confidence. "
        if strengths:
            summary += f"Core strengths identified in {strengths}. "
            
        summary += f"Analyzed {source_coverage.external_evidence_count} external sources and {graph_summary.entity_count} entities."
        return summary

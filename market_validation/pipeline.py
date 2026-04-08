from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any

from .email import EmailSender, InboxPoller, InboundReply
from .llm import OllamaClient
from .models import (
    CallSheet,
    LeadRecord,
    LeadScore,
    OutreachDraft,
    PersonalizationLine,
    ReplyTrackingEntry,
)
from .research import DuckDuckGoSearcher, WebResult
from .storage import PipelineRepository


_BRISKET_KEYWORDS = ["BBQ", "brisket", "smokehouse", "barbecue", "catering"]


class BrisketPipeline:
    """Orchestrates brisket lead discovery, scoring, outreach, reply handling,
    and call-sheet generation."""

    def __init__(
        self,
        repo: PipelineRepository,
        llm: OllamaClient | None = None,
        sender: EmailSender | None = None,
        poller: InboxPoller | None = None,
    ) -> None:
        self._repo = repo
        self._llm = llm or OllamaClient()
        self._sender = sender or EmailSender()
        self._poller = poller or InboxPoller()
        self._searcher = DuckDuckGoSearcher()

    # ------------------------------------------------------------------
    # 1. Discovery
    # ------------------------------------------------------------------

    def discover(
        self,
        geography: str,
        max_leads: int = 20,
        keywords: list[str] | None = None,
    ) -> list[str]:
        """Search for restaurants/caterers in *geography* that may buy brisket.
        Saves each discovered company as a lead in the DB.
        Returns list of lead_ids.
        """
        kws = keywords or _BRISKET_KEYWORDS
        queries = _build_discovery_queries(geography, kws)

        all_results: list[WebResult] = []
        seen_urls: set[str] = set()
        for query in queries:
            results = self._searcher.search(query, max_results=8)
            for r in results:
                if r.url not in seen_urls:
                    seen_urls.add(r.url)
                    all_results.append(r)
            if len(all_results) >= max_leads * 3:
                break

        companies = self._extract_companies(all_results, geography)
        lead_ids: list[str] = []
        seen_names: set[str] = set()
        for company in companies:
            name_key = company.name.lower().strip()
            if name_key in seen_names:
                continue
            seen_names.add(name_key)
            lead_id = self._repo.save_lead(company)
            lead_ids.append(lead_id)
            if len(lead_ids) >= max_leads:
                break

        return lead_ids

    # ------------------------------------------------------------------
    # 2. Scoring
    # ------------------------------------------------------------------

    def score_lead(self, lead_id: str) -> dict[str, Any]:
        """Score a lead for brisket purchase probability and volume.
        Saves score to DB. Returns score dict.
        """
        lead = self._repo.get_lead(lead_id)
        if lead is None:
            raise ValueError(f"Lead {lead_id} not found")

        score = self._llm_score_lead(lead)
        self._repo.save_lead_score(lead_id, score)

        status_map = {"hot": "scored_hot", "warm": "scored_warm", "cold": "scored_cold", "disqualified": "disqualified"}
        self._repo.update_lead_status(lead_id, status_map.get(score.status, "scored"))
        return score.model_dump()

    # ------------------------------------------------------------------
    # 3. Outreach — draft + send
    # ------------------------------------------------------------------

    def draft_outreach(self, lead_id: str) -> dict[str, Any]:
        """Draft a personalized outreach email for a lead.
        Saves draft to DB. Returns draft dict including draft_id.
        """
        lead = self._repo.get_lead(lead_id)
        if lead is None:
            raise ValueError(f"Lead {lead_id} not found")

        score_row = self._repo.get_latest_score(lead_id)
        draft = self._llm_draft_outreach(lead, score_row)
        draft_id = self._repo.save_outreach_draft(lead_id, draft)
        result = draft.model_dump()
        result["draft_id"] = draft_id
        return result

    def send_outreach(self, lead_id: str, draft_id: str | None = None) -> dict[str, Any]:
        """Send the outreach email for a lead (uses latest draft if draft_id omitted).
        Returns {"sent": bool, "draft_id": str}.
        """
        lead = self._repo.get_lead(lead_id)
        if lead is None:
            raise ValueError(f"Lead {lead_id} not found")

        if draft_id:
            # Fetch by iterating — simple approach; draft_id is returned from draft_outreach
            draft_row = self._repo.get_latest_draft(lead_id)
            if draft_row and draft_row["id"] != draft_id:
                draft_row = None
        else:
            draft_row = self._repo.get_latest_draft(lead_id)

        if draft_row is None:
            raise ValueError("No draft found — call draft_outreach first")

        to_email = lead.get("email") or ""
        if not to_email:
            return {"sent": False, "reason": "no_email_on_lead", "draft_id": draft_row["id"]}

        subject = f"Brisket Supply Partnership — {lead['name']}"
        body = draft_row.get("first_email") or ""

        ok, message_id = self._sender.send(
            to=to_email,
            subject=subject,
            body=body,
            lead_id=lead_id,
        )
        if ok and message_id:
            sent_at = datetime.now(timezone.utc).isoformat()
            self._repo.mark_outreach_sent(draft_row["id"], sent_at, message_id)
            self._repo.update_lead_status(lead_id, "outreach_sent")

        return {"sent": ok, "draft_id": draft_row["id"]}

    # ------------------------------------------------------------------
    # 4. Reply handling
    # ------------------------------------------------------------------

    def record_reply(self, lead_id: str, raw_reply_text: str) -> dict[str, Any]:
        """Manually record an inbound reply for a lead (human pastes it in).
        Classifies intent with LLM, saves to DB. Returns reply tracking dict.
        """
        lead = self._repo.get_lead(lead_id)
        if lead is None:
            raise ValueError(f"Lead {lead_id} not found")

        entry = self._llm_classify_reply(lead["name"], raw_reply_text)
        reply_id = self._repo.save_reply(
            lead_id,
            entry,
            raw_reply_text,
        )
        self._repo.update_lead_status(lead_id, entry.company_status)
        result = entry.model_dump()
        result["reply_id"] = reply_id
        return result

    def poll_replies(self) -> dict[str, Any]:
        """Poll IMAP inbox for new replies. Classifies each and saves to DB.
        Returns {"processed": int, "last_uid": int}.
        """
        since_uid = int(self._repo.get_state("last_imap_uid") or "0")
        sent_message_ids = self._repo.get_all_sent_message_ids()

        replies = self._poller.poll(
            since_uid=since_uid,
            sent_message_ids=sent_message_ids,
        )

        processed = 0
        max_uid = since_uid

        for reply in replies:
            lead_id = self._resolve_lead_id(reply, sent_message_ids)
            if lead_id is None:
                max_uid = max(max_uid, reply.imap_uid)
                continue

            lead = self._repo.get_lead(lead_id)
            if lead is None:
                max_uid = max(max_uid, reply.imap_uid)
                continue

            entry = self._llm_classify_reply(lead["name"], reply.body)
            self._repo.save_reply(
                lead_id,
                entry,
                reply.body,
                received_at=reply.received_at,
            )
            self._repo.update_lead_status(lead_id, entry.company_status)
            max_uid = max(max_uid, reply.imap_uid)
            processed += 1

        if max_uid > since_uid:
            self._repo.set_state("last_imap_uid", str(max_uid))

        return {"processed": processed, "last_uid": max_uid}

    # ------------------------------------------------------------------
    # 5. Call sheet
    # ------------------------------------------------------------------

    def generate_call_sheet(self, lead_id: str) -> dict[str, Any]:
        """Build a call sheet for a lead from all accumulated data.
        Saves to DB. Returns the call sheet dict.
        """
        lead = self._repo.get_lead(lead_id)
        if lead is None:
            raise ValueError(f"Lead {lead_id} not found")

        score_row = self._repo.get_latest_score(lead_id)
        draft_row = self._repo.get_latest_draft(lead_id)
        replies = self._repo.get_replies(lead_id)

        sheet = self._llm_build_call_sheet(lead, score_row, draft_row, replies)
        self._repo.save_call_sheet(lead_id, sheet)
        return self._repo.get_call_sheet(lead_id) or sheet.model_dump()

    # ------------------------------------------------------------------
    # LLM helpers
    # ------------------------------------------------------------------

    def _extract_companies(
        self, results: list[WebResult], geography: str
    ) -> list[LeadRecord]:
        """Use LLM to extract structured company records from search results.
        Falls back to heuristic extraction if LLM unavailable.
        """
        if not results:
            return []

        snippets = "\n\n".join(
            f"Title: {r.title}\nURL: {r.url}\nSnippet: {r.snippet}"
            for r in results[:30]
        )

        system = (
            "You extract company leads from web search results for a brisket wholesale sales campaign. "
            "Return a JSON object with a 'companies' array. Each company has: "
            "name (string, required), website (string or null), phone (string or null), "
            "email (string or null), location (string or null), category (string or null), "
            "menu_url (string or null), demand_signals (array of strings). "
            "Only include restaurants, BBQ joints, caterers, food-service businesses, or event venues. "
            "If a field is unknown leave it null. Return only valid JSON."
        )
        user = (
            f"Geography: {geography}\n\nSearch results:\n{snippets}\n\n"
            "Extract all distinct companies. Return JSON."
        )

        parsed = self._llm.chat_json(system, user) if self._llm.enabled else None
        companies_raw = (parsed or {}).get("companies") or []

        if not companies_raw:
            return _heuristic_extract(results, geography)

        out: list[LeadRecord] = []
        for c in companies_raw:
            if not isinstance(c, dict) or not c.get("name"):
                continue
            out.append(LeadRecord(
                name=str(c["name"]),
                website=c.get("website") or None,
                phone=c.get("phone") or None,
                email=c.get("email") or None,
                location=c.get("location") or geography,
                category=c.get("category") or None,
                menu_url=c.get("menu_url") or None,
                demand_signals=[str(s) for s in (c.get("demand_signals") or [])],
                source_urls=[r.url for r in results if c.get("name", "").lower() in r.title.lower()],
                evidence_snippets=[r.snippet for r in results if c.get("name", "").lower() in r.title.lower()][:3],
            ))
        return out

    def _llm_score_lead(self, lead: dict[str, Any]) -> LeadScore:
        system = (
            "You score restaurant/catering leads for likelihood of purchasing wholesale brisket. "
            "Return a JSON object with these float fields (0.0–1.0): probability_buy, "
            "estimated_volume_potential, geographic_fit, pricing_tier_fit, "
            "catering_event_potential, contactability, confidence. "
            "Also include: status (one of: hot, warm, cold, disqualified), rationale (string). "
            "hot = probability_buy >= 0.7, warm = 0.4–0.69, cold = 0.2–0.39, disqualified < 0.2. "
            "Return only valid JSON."
        )
        signals = lead.get("demand_signals_json") or []
        snippets = lead.get("evidence_snippets_json") or []
        user = (
            f"Company: {lead['name']}\n"
            f"Category: {lead.get('category') or 'unknown'}\n"
            f"Location: {lead.get('location') or 'unknown'}\n"
            f"Demand signals: {', '.join(signals) if signals else 'none'}\n"
            f"Evidence: {' | '.join(snippets[:3]) if snippets else 'none'}\n"
            "Score this lead for brisket wholesale purchasing potential."
        )

        parsed = self._llm.chat_json(system, user) if self._llm.enabled else None

        if parsed and isinstance(parsed, dict):
            return LeadScore(
                lead_name=lead["name"],
                probability_buy=_clamp(parsed.get("probability_buy", 0.5)),
                estimated_volume_potential=_clamp(parsed.get("estimated_volume_potential", 0.5)),
                geographic_fit=_clamp(parsed.get("geographic_fit", 0.5)),
                pricing_tier_fit=_clamp(parsed.get("pricing_tier_fit", 0.5)),
                catering_event_potential=_clamp(parsed.get("catering_event_potential", 0.5)),
                contactability=_clamp(parsed.get("contactability", 0.3 if not lead.get("email") else 0.8)),
                confidence=_clamp(parsed.get("confidence", 0.4)),
                status=parsed.get("status", "warm") if parsed.get("status") in {"hot", "warm", "cold", "disqualified"} else "warm",
                rationale=str(parsed.get("rationale", "LLM scored.")),
            )

        # Heuristic fallback
        return _heuristic_score(lead)

    def _llm_draft_outreach(
        self, lead: dict[str, Any], score_row: dict[str, Any] | None
    ) -> OutreachDraft:
        system = (
            "You write concise, personalized cold-outreach emails for a brisket wholesale supplier. "
            "Return a JSON object with: intro, why_selected, brisket_relevance, offer, cta, "
            "first_email, follow_up_1, follow_up_2 (all strings). "
            "first_email is the full text of the first email to send (150–200 words, professional, not pushy). "
            "follow_up_1 is a 7-day follow-up (80 words). follow_up_2 is a 14-day final follow-up (60 words). "
            "Return only valid JSON."
        )
        rationale = (score_row or {}).get("rationale", "")
        signals = lead.get("demand_signals_json") or []
        user = (
            f"Company: {lead['name']}\n"
            f"Category: {lead.get('category') or 'restaurant/caterer'}\n"
            f"Location: {lead.get('location') or 'unknown'}\n"
            f"Demand signals: {', '.join(signals) if signals else 'none'}\n"
            f"Scoring rationale: {rationale or 'n/a'}\n"
            "Write a 3-touch outreach sequence. Return JSON."
        )

        parsed = self._llm.chat_json(system, user) if self._llm.enabled else None

        if parsed and isinstance(parsed, dict) and parsed.get("first_email"):
            return OutreachDraft(
                lead_name=lead["name"],
                intro=str(parsed.get("intro", "")),
                why_selected=str(parsed.get("why_selected", "")),
                brisket_relevance=str(parsed.get("brisket_relevance", "")),
                offer=str(parsed.get("offer", "")),
                cta=str(parsed.get("cta", "")),
                first_email=str(parsed.get("first_email", "")),
                follow_up_1=str(parsed.get("follow_up_1", "")),
                follow_up_2=str(parsed.get("follow_up_2", "")),
                personalization_lines=[],
            )

        return _fallback_outreach_draft(lead)

    def _llm_classify_reply(self, company_name: str, reply_text: str) -> ReplyTrackingEntry:
        system = (
            "You classify email replies to brisket wholesale outreach. "
            "Return JSON with: intent (one of: interested, objection, pricing_request, "
            "schedule_request, not_now, no_reply), "
            "company_status (one of: awaiting_reply, follow_up_needed, call_scheduled, "
            "qualified, closed_lost, closed_won), "
            "thread_summary (1 sentence), follow_up_task (1 sentence action item). "
            "Return only valid JSON."
        )
        user = f"Company: {company_name}\n\nReply text:\n{reply_text[:2000]}"

        parsed = self._llm.chat_json(system, user) if self._llm.enabled else None

        valid_intents = {"interested", "objection", "pricing_request", "schedule_request", "not_now", "no_reply"}
        valid_statuses = {"awaiting_reply", "follow_up_needed", "call_scheduled", "qualified", "closed_lost", "closed_won"}

        if parsed and isinstance(parsed, dict):
            intent = parsed.get("intent", "interested")
            status = parsed.get("company_status", "follow_up_needed")
            return ReplyTrackingEntry(
                lead_name=company_name,
                intent=intent if intent in valid_intents else "interested",
                company_status=status if status in valid_statuses else "follow_up_needed",
                thread_summary=str(parsed.get("thread_summary", reply_text[:100])),
                follow_up_task=str(parsed.get("follow_up_task", "Review reply and respond.")),
            )

        return ReplyTrackingEntry(
            lead_name=company_name,
            intent="interested",
            company_status="follow_up_needed",
            thread_summary=reply_text[:120],
            follow_up_task="Review reply manually.",
        )

    def _llm_build_call_sheet(
        self,
        lead: dict[str, Any],
        score_row: dict[str, Any] | None,
        draft_row: dict[str, Any] | None,
        replies: list[dict[str, Any]],
    ) -> CallSheet:
        system = (
            "You prepare sales call sheets for a brisket wholesale supplier. "
            "Return JSON with: company_summary (2 sentences), "
            "talking_points (array of 4–6 strings), "
            "objections (array of 3–4 likely objections with short counters), "
            "next_step_suggestions (array of 2–3 strings). "
            "Return only valid JSON."
        )
        reply_summaries = [r.get("thread_summary", "") for r in replies if r.get("thread_summary")]
        prior_emails: list[str] = []
        if draft_row and draft_row.get("first_email"):
            prior_emails.append(draft_row["first_email"])

        user = (
            f"Company: {lead['name']}\n"
            f"Category: {lead.get('category') or 'restaurant/caterer'}\n"
            f"Location: {lead.get('location') or 'unknown'}\n"
            f"Score status: {(score_row or {}).get('status', 'unknown')}\n"
            f"Score rationale: {(score_row or {}).get('rationale', 'n/a')}\n"
            f"Reply history: {'; '.join(reply_summaries) or 'none yet'}\n"
            "Build a call sheet. Return JSON."
        )

        parsed = self._llm.chat_json(system, user) if self._llm.enabled else None

        if parsed and isinstance(parsed, dict) and parsed.get("talking_points"):
            return CallSheet(
                lead_name=lead["name"],
                company_summary=str(parsed.get("company_summary", "")),
                prior_emails=prior_emails,
                talking_points=[str(t) for t in (parsed.get("talking_points") or [])],
                objections=[str(o) for o in (parsed.get("objections") or [])],
                next_step_suggestions=[str(s) for s in (parsed.get("next_step_suggestions") or [])],
                notes=[],
            )

        return CallSheet(
            lead_name=lead["name"],
            company_summary=f"{lead['name']} is a potential brisket buyer in {lead.get('location', 'the area')}.",
            prior_emails=prior_emails,
            talking_points=[
                "Introduce wholesale brisket supply offer",
                "Ask about current meat supplier and volumes",
                "Discuss pricing and delivery schedule",
                "Mention quality and reliability guarantees",
            ],
            objections=["Price — compare per-lb cost vs current supplier", "Already have a supplier — ask about backup/overflow"],
            next_step_suggestions=["Send pricing sheet", "Schedule follow-up call"],
            notes=[],
        )

    def _resolve_lead_id(
        self, reply: InboundReply, sent_message_ids: set[str]
    ) -> str | None:
        # Direct header match
        if reply.lead_id:
            return reply.lead_id
        # In-Reply-To → find the draft → get its lead_id
        if reply.in_reply_to and reply.in_reply_to in sent_message_ids:
            draft = self._repo.get_draft_by_message_id(reply.in_reply_to)
            if draft:
                return draft.get("lead_id")
        return None


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

def _build_discovery_queries(geography: str, keywords: list[str]) -> list[str]:
    queries = []
    for kw in keywords:
        queries.append(f"{kw} restaurant {geography}")
        queries.append(f"{kw} catering {geography}")
    queries.append(f"BBQ smokehouse {geography} site:yelp.com")
    queries.append(f"brisket restaurant {geography} menu")
    return queries


def _heuristic_extract(results: list[WebResult], geography: str) -> list[LeadRecord]:
    out: list[LeadRecord] = []
    for r in results:
        name = re.sub(r"\s*[-|]\s*.*$", "", r.title).strip()
        if not name:
            continue
        out.append(LeadRecord(
            name=name,
            website=r.url,
            location=geography,
            source_urls=[r.url],
            evidence_snippets=[r.snippet],
            demand_signals=_extract_signals(r.snippet),
        ))
    return out


def _extract_signals(text: str) -> list[str]:
    signals = []
    for word in ("brisket", "BBQ", "barbecue", "smoked", "catering", "smokehouse"):
        if word.lower() in text.lower():
            signals.append(word)
    return signals


def _heuristic_score(lead: dict[str, Any]) -> LeadScore:
    signals = lead.get("demand_signals_json") or []
    has_brisket = any("brisket" in s.lower() for s in signals)
    has_bbq = any(s.lower() in ("bbq", "barbecue", "smoked") for s in signals)
    has_catering = any("cater" in s.lower() for s in signals)
    has_email = bool(lead.get("email"))

    prob = 0.35 + (0.2 if has_brisket else 0) + (0.1 if has_bbq else 0) + (0.1 if has_catering else 0)
    status = "hot" if prob >= 0.7 else "warm" if prob >= 0.4 else "cold"

    return LeadScore(
        lead_name=lead["name"],
        probability_buy=min(prob, 1.0),
        estimated_volume_potential=0.5 if has_catering else 0.3,
        geographic_fit=0.8,
        pricing_tier_fit=0.5,
        catering_event_potential=0.7 if has_catering else 0.3,
        contactability=0.8 if has_email else 0.4,
        confidence=0.3,
        status=status,
        rationale="Heuristic score based on demand signals (LLM unavailable).",
    )


def _fallback_outreach_draft(lead: dict[str, Any]) -> OutreachDraft:
    name = lead["name"]
    location = lead.get("location", "your area")
    first_email = (
        f"Hi {name} team,\n\n"
        f"I came across {name} while looking for great BBQ and catering operations in {location}. "
        "We supply premium-grade brisket to restaurants and caterers — consistent quality, "
        "reliable delivery, and competitive wholesale pricing.\n\n"
        "Would you be open to a quick conversation about your current meat sourcing? "
        "We'd love to send you a sample and pricing sheet.\n\n"
        "Best,\n[Your name]"
    )
    return OutreachDraft(
        lead_name=name,
        intro=f"Reaching out to {name} regarding wholesale brisket supply.",
        why_selected=f"{name} is a BBQ/catering operation in {location}.",
        brisket_relevance="Brisket is a core menu item for BBQ-focused operations.",
        offer="Premium wholesale brisket with sample and pricing sheet.",
        cta="Schedule a 10-minute call or request a free sample.",
        first_email=first_email,
        follow_up_1=f"Hi {name} team — just following up on my note from last week about wholesale brisket supply. Happy to send pricing if helpful. Let me know!",
        follow_up_2=f"Last note from me — if the timing isn't right for {name}, no worries. Feel free to reach out whenever brisket sourcing comes up.",
        personalization_lines=[],
    )


def _clamp(val: Any, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        return max(lo, min(hi, float(val)))
    except (TypeError, ValueError):
        return (lo + hi) / 2

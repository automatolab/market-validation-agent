from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from .engine import MarketValidationEngine
from .llm import OllamaClient
from .models import (
    DiscoverRequest,
    MarketSearchRequest,
    NoteInput,
    ReplyInput,
    ValidationRequest,
    ValidationResponse,
)
from .pipeline import BrisketPipeline
from .research import MarketResearchService
from .storage import PipelineRepository, ResearchRunRepository

app = FastAPI(
    title="Market Validation Agent",
    description="Evidence-backed business idea viability engine",
    version="0.1.0",
)

_ollama_client = OllamaClient()
_engine = MarketValidationEngine(llm_client=_ollama_client)
_market_research_service = MarketResearchService(llm_client=_ollama_client)
_run_repository = ResearchRunRepository()
_pipeline_repo = PipelineRepository()
_brisket_pipeline = BrisketPipeline(repo=_pipeline_repo, llm=_ollama_client)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/validate", response_model=ValidationResponse)
def validate(payload: ValidationRequest) -> ValidationResponse:
    result = _engine.validate(payload)
    run_id = _run_repository.save_run(
        endpoint="/validate",
        request_payload=payload.model_dump(),
        response=result,
    )
    return result.model_copy(update={"run_id": run_id})


@app.post("/validate/market", response_model=ValidationResponse)
def validate_market(payload: MarketSearchRequest) -> ValidationResponse:
    request = _market_research_service.build_validation_request(payload)
    result = _engine.validate(request)
    run_id = _run_repository.save_run(
        endpoint="/validate/market",
        request_payload=payload.model_dump(),
        response=result,
    )
    return result.model_copy(update={"run_id": run_id})


@app.get("/runs")
def list_runs(limit: int = Query(default=20, ge=1, le=200)) -> dict[str, object]:
    rows = _run_repository.list_runs(limit=limit)
    return {"count": len(rows), "runs": rows}


@app.get("/runs/{run_id}")
def get_run(run_id: str) -> dict[str, object]:
    run = _run_repository.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


# ---------------------------------------------------------------------------
# Brisket pipeline
# ---------------------------------------------------------------------------

@app.post("/pipeline/discover")
def pipeline_discover(payload: DiscoverRequest) -> dict[str, object]:
    """Search for restaurants/caterers and save them as leads."""
    lead_ids = _brisket_pipeline.discover(
        geography=payload.geography,
        max_leads=payload.max_leads,
        keywords=payload.keywords,
    )
    return {"discovered": len(lead_ids), "lead_ids": lead_ids}


@app.get("/pipeline/leads")
def pipeline_list_leads(
    status: str | None = Query(default=None),
) -> dict[str, object]:
    """Dashboard view — all leads with status, score, and reply intent."""
    summaries = _pipeline_repo.list_lead_summaries(status_filter=status)
    return {"count": len(summaries), "leads": [s.model_dump() for s in summaries]}


@app.get("/pipeline/leads/{lead_id}")
def pipeline_get_lead(lead_id: str) -> dict[str, object]:
    """Full detail for a single lead."""
    lead = _pipeline_repo.get_lead(lead_id)
    if lead is None:
        raise HTTPException(status_code=404, detail="Lead not found")
    score = _pipeline_repo.get_latest_score(lead_id)
    draft = _pipeline_repo.get_latest_draft(lead_id)
    replies = _pipeline_repo.get_replies(lead_id)
    call_sheet = _pipeline_repo.get_call_sheet(lead_id)
    return {
        "lead": lead,
        "score": score,
        "latest_draft": draft,
        "replies": replies,
        "call_sheet": call_sheet,
    }


@app.post("/pipeline/leads/{lead_id}/score")
def pipeline_score_lead(lead_id: str) -> dict[str, object]:
    """Score a lead for brisket purchase probability and estimated volume."""
    try:
        return _brisket_pipeline.score_lead(lead_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/pipeline/leads/{lead_id}/outreach")
def pipeline_outreach(lead_id: str) -> dict[str, object]:
    """Draft a personalized outreach email. Does not send — call /send to send."""
    try:
        return _brisket_pipeline.draft_outreach(lead_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/pipeline/leads/{lead_id}/send")
def pipeline_send(lead_id: str) -> dict[str, object]:
    """Send the latest outreach draft for this lead via SMTP."""
    try:
        return _brisket_pipeline.send_outreach(lead_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/pipeline/leads/{lead_id}/reply")
def pipeline_record_reply(lead_id: str, payload: ReplyInput) -> dict[str, object]:
    """Manually record an inbound reply (paste the email text here).
    The LLM classifies intent and updates the lead's pipeline status.
    """
    try:
        return _brisket_pipeline.record_reply(lead_id, payload.raw_reply_text)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.get("/pipeline/leads/{lead_id}/callsheet")
def pipeline_callsheet(lead_id: str) -> dict[str, object]:
    """Get (or generate) the call sheet for a lead, including saved notes."""
    try:
        return _brisket_pipeline.generate_call_sheet(lead_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/pipeline/leads/{lead_id}/notes")
def pipeline_add_note(lead_id: str, payload: NoteInput) -> dict[str, object]:
    """Save a call note for a lead (written during or after a sales call)."""
    lead = _pipeline_repo.get_lead(lead_id)
    if lead is None:
        raise HTTPException(status_code=404, detail="Lead not found")
    note_id = _pipeline_repo.add_call_note(lead_id, payload.note, payload.author)
    return {"note_id": note_id}


@app.post("/pipeline/poll-replies")
def pipeline_poll_replies() -> dict[str, object]:
    """Poll the configured IMAP inbox for new replies to outreach emails."""
    return _brisket_pipeline.poll_replies()

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from .engine import MarketValidationEngine
from .llm import OllamaClient
from .models import MarketSearchRequest, ValidationRequest, ValidationResponse
from .research import MarketResearchService
from .storage import ResearchRunRepository

app = FastAPI(
    title="Market Validation Agent",
    description="Evidence-backed business idea viability engine",
    version="0.1.0",
)

_ollama_client = OllamaClient()
_engine = MarketValidationEngine(llm_client=_ollama_client)
_market_research_service = MarketResearchService(llm_client=_ollama_client)
_run_repository = ResearchRunRepository()


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

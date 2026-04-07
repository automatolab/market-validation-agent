# Market Validation Agent

A market validation research agent that turns a business idea into an evidence-backed viability report with scoring, risks, and next experiments.

## Core Product

Given a business idea, customer, and market, the engine returns:

- research stage
- market summary
- source coverage summary
- raw source inventory (query -> source -> cleaned text)
- structured evidence facts (entity/fact/value/confidence)
- evidence graph summary (pricing bands, theme clusters, contradictions)
- research pipeline snapshot for lead ingestion, scoring, outreach drafts, reply tracking, and call sheets
- target customer clarity
- competitor map
- demand signals
- review/sentiment summary
- pricing snapshot
- pricing reality check
- distribution difficulty
- risks
- unknowns
- market score
- evidence coverage score
- confidence score
- next validation experiments
- evidence table with sources

## Research Stages

The engine now gates output quality by evidence maturity:

1. `brief_only`
  - No external evidence collected.
  - Returns `overall_verdict: insufficient_evidence`.
  - Hard dimension scores are suppressed (`score: null`, `status: insufficient_evidence`).

2. `search_results_only`
  - External search results exist, but destination page fetching failed or produced no usable fetched-page evidence.
  - Returns `overall_verdict: insufficient_evidence`.
  - Hard dimension scores are suppressed to avoid snippet-only overconfidence.

3. `partial_research`
  - Some evidence exists but thresholds are not met.
  - Returns provisional scores (`status: provisional`).
  - Market score is marked provisional via `market_score_status` and `market_score_basis`.

4. `complete_research`
  - Evidence thresholds are met.
  - Requires meaningful fetched-page evidence quality, not just high source count.
  - Returns full verdict and scored dimensions.

## Architecture

The implementation follows a 3-layer design:

1. Deterministic research pipeline: query planning, source collection, page extraction, source typing, structured fact extraction, coverage checks.
2. LLM-assisted interpretation: optional Ollama support for query expansion, nuanced fact extraction, and score rationale refinement.
3. Validation engine: coverage-aware scoring, verdict gating, contradiction-aware confidence, and final synthesis.

## Research Mission Loop

The market endpoint runs a mission-style loop instead of a one-shot prompt:

1. Build a query plan (competitors, pricing, reviews, directories, forums, editorial lists, job posts, trends, public data).
2. Collect and fetch sources.
3. Extract structured evidence facts (`entity`, `fact_type`, `value`, `confidence`, source trace).
4. Check coverage thresholds (competitor/pricing/review source counts + evidence volume).
5. If coverage is missing, generate gap queries and collect again.
6. Stop when thresholds are met or round limits are reached.

## Fixed Pipeline

1. Clarify the idea into a structured research brief.
2. Collect evidence from available inputs and supplied external evidence.
3. Extract structured facts about buyers, pain, pricing, and competition.
4. Score the market with a fixed rubric.
5. Synthesize a verdict with confidence, risks, unknowns, and next tests.

## API

### `POST /validate`

Request body:

```json
{
  "idea": "AI scheduling assistant for outpatient clinics",
  "target_customer": "Clinic operations managers in small private practices",
  "geography": "US",
  "business_model": "B2B SaaS subscription",
  "competitors": ["NexHealth", "Luma Health"],
  "pricing_guess": "$149/month per location",
  "assumptions": ["Staff are overloaded with phone scheduling"],
  "constraints": ["No integration in v1"],
  "profile": "saas",
  "template": "ai_saas",
  "evidence_inputs": [
    {
      "source_type": "review_site",
      "source_title": "G2 reviews for scheduling software",
      "source_url": "https://www.g2.com/categories/appointment-scheduling",
      "observed_fact": "Users complain about setup complexity and hidden costs",
      "strength": "high"
    }
  ]
}
```

### `POST /validate/market`

Minimal-input, live-search mode. You only provide the market phrase and optional tuning fields.

```json
{
  "market": "brisket catering",
  "geography": "US",
  "profile": "local_business",
  "template": "restaurant"
}
```

This endpoint performs live DuckDuckGo search across multiple query types, fetches result pages for richer evidence, converts those findings into evidence rows, and runs the same scoring pipeline.

In addition to `evidence_table`, the response now includes:

- `raw_sources`: cleaned source records used during the mission loop.
- `structured_evidence`: normalized facts extracted from raw pages.
- `evidence_graph_summary`: clustered pricing bands, complaint/praise themes, and contradiction flags.
- `research_pipeline`: thesis metadata plus lead records, scores, outreach drafts, reply tracking, and call sheets.
- `research_diagnostics`: query attempts, fetch stats, and search errors (if discovery fails).
- `run_id`: persistent run identifier for replay and audit.

## Run History and Replay

Every `/validate` and `/validate/market` call is persisted to SQLite so evidence can be replayed across sessions.

- Default database path: `.data/research_runs.db`
- Override with env: `RESEARCH_DB_PATH=/absolute/path/to/research_runs.db`

Replay endpoints:

- `GET /runs?limit=20` returns latest run summaries
- `GET /runs/{run_id}` returns full stored request/response plus raw sources and structured evidence

Evidence quality guardrails are enforced before a final verdict. If coverage is below minimum thresholds (competitor, pricing, and review/community sources), the API returns `overall_verdict: insufficient_evidence` instead of a weak/strong market claim.

Coverage thresholds:

- at least 3 competitor sources
- at least 2 pricing sources
- at least 2 review/community sources

Optional request fields:

- `research_mode`: `standard` or `deep`
- `minimum_evidence_rows`: evidence threshold before the report is returned

`deep` mode uses more query variants, fetches more pages, retries longer on rate limits, and waits for a larger evidence set before returning.

Run locally:

```bash
pip install -e .[dev]
uvicorn market_validation.main:app --reload
```

Optional: enable Ollama-backed context and score refinement:

```bash
export OLLAMA_API_BASE=http://100.122.77.81:11434
export OLLAMA_MODEL=gpt-oss:120b
```

When `OLLAMA_API_BASE` is set, the engine will use Ollama to enrich inferred customer context and refine dimension scoring rationale. If Ollama is unreachable, the API safely falls back to deterministic scoring.

Run tests:

```bash
pytest
```

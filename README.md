# Market Research Agent

General-purpose market research platform that discovers companies, qualifies leads, and tracks outreach - no API keys required.

## What This Repo Does

1. **Create Research Projects** - Define what market/product/geography to research
2. **Auto-Discover Sources** - Find relevant data sources (directories, news, social, trends)
3. **Gather Companies** - Discover businesses using free web sources
4. **Qualify Leads** - Assess relevance and estimate volume using AI + Google Trends
5. **Track Outreach** - Manage contacts, emails, and call notes
6. **Generate Reports** - Export findings as markdown

## Quick Start

```bash
# Create and run a research project
market-research-run run \
  --name "San Jose BBQ Restaurants" \
  --market "Brisket Supply" \
  --product "beef brisket" \
  --geography "San Jose, CA"

# List all researches
market-research list

# View research details
market-research get <research_id>

# Export as markdown
market-research export <research_id> --output report.md
```

## Auto-Detected Market Types

The system auto-detects your market type and discovers appropriate sources:

| Market Type | Keywords | Sources |
|-------------|----------|---------|
| **Restaurant** | restaurant, BBQ, cafe, catering, brisket | Yelp, TripAdvisor, YellowPages, OSM |
| **Retail** | store, shop, outlet, grocery | Bing, Yelp, YellowPages |
| **Tech** | software, SaaS, AI, platform | LinkedIn, News, Crunchbase |
| **Healthcare** | hospital, clinic, medical | Healthgrades, News |
| **Default** | (all others) | Bing, DuckDuckGo, OSM |

## Architecture

```
Research Project (UUID-based)
├── Sources (auto-discovered)
├── Companies (discovered + qualified)
│   ├── Claims (evidence-backed)
│   ├── Contacts (found/added)
│   └── Outreach (emails, calls)
├── Market Demand Data (Google Trends)
└── Call Notes
```

## Key Commands

```bash
# Research management
market-research create --name "..." --market "..." --geography "..."
market-research list
market-research get <id>
market-research export <id>

# Source discovery
market-source-discover --market "SaaS" --geography "US"

# Market trends
python -m market_validation.market_trends --keyword "brisket" --geography "US-CA"

# Lead pipeline (legacy)
market-lead-pipeline run --config config/lead-pipeline.json

# Call notes
market-call-notes add --company-id <id> --author "Sales" --note "..."
market-call-notes list --company-id <id>
```

## Free Data Sources (No API Keys)

- **Web Search**: DuckDuckGo, Bing
- **Directories**: Yelp, TripAdvisor, YellowPages, OpenStreetMap
- **News**: Hacker News, Google News
- **Social**: LinkedIn (search), Facebook
- **Trends**: Google Trends (pytrends)
- **Data**: Crunchbase, Healthgrades

## OpenCode-Driven Workflow

Each worker invocation builds one prompt payload from:

- `modes/_shared.md`
- `modes/validate.md`
- `batch/batch-prompt.md`
- runtime item metadata (market, geography, profile, report number, date)

Then it calls `opencode run` and expects strict JSON output.

For lead-generation workflows (for example brisket supply), the prompt contracts support a staged pipeline:

1. `research-ingest`
2. `lead-qualify`
3. `outreach-email`
4. `reply-parse`
5. `call-sheet-build`

Hard guarantees in these contracts:

- JSON-only outputs
- free-source only ingestion (auto-discovery uses DuckDuckGo, Yelp, TripAdvisor, YellowPages, Bing - no API keys required)
- evidence URLs required for every qualification claim
- lead statuses include `new`, `qualified`, `emailed`, `replied_interested`, `replied_not_now`, `do_not_contact`, `call_ready`

## Quick Start (No API Keys Needed)

```bash
# Install
pip install -e .

# Auto-discover sources and run pipeline
market-lead-pipeline run \
  --config <(echo '{
    "market": "Brisket",
    "geography": "US",
    "target_product": "brisket",
    "auto_discover_sources": true,
    "email_template": {"template_id": "v1", "subject_template": "Subject", "body_template": "Body", "tone": "professional"}
  }') \
  --run-id brisket-001

# Or discover sources first
market-source-discover --market Brisket --geography "Austin TX"
```

## File + DB Output Store

Stage JSON payloads are persisted to files, materialized into markdown views, and mirrored to SQLite for structured querying:

- `output/runs/{run_id}/{stage}.json` - per-stage canonical payloads
- `output/leads/leads.jsonl` - latest lead state per company
- `output/call-sheets/{YYYY-MM-DD}.md` - call sheet for human follow-up
- `output/dashboard/summary.md` - status and priority summary
- `output/market-validation.sqlite3` - relational store for leads, source evidence, drafts, replies, call sheets, and call notes

Database path defaults to `output/market-validation.sqlite3` and can be overridden with `MARKET_DB_PATH`.

Persist a stage payload from a file:

```bash
python store-output.py --input-file output/sample-stage.json
```

Or from stdin:

```bash
python store-output.py <<'JSON'
{
  "result": "ok",
  "stage": "lead_qualify",
  "run_id": "brisket-001",
  "market": "Brisket",
  "qualified_companies": []
}
JSON
```

You can also use the installed script:

```bash
market-output-store --input-file output/sample-stage.json
```

## Configuration and Context

Place market-specific context in config files (example template):

- `config/lead-pipeline.example.json`

Recommended pattern:

1. Copy it to `config/lead-pipeline.json`
2. Fill in `market`, `target_product`, `source_configs`, and `email_template`
3. Use those values when invoking staged workflows and prompts

A ready-to-use brisket config is included at `config/lead-pipeline.json`.

Notes:

- Keep provider/model defaults in config for consistency across runs.
- Source configs are operator-owned; prompts enforce configured-source-only behavior.
- Auto-discovery uses free sources: DuckDuckGo, Yelp, TripAdvisor, Bing, YellowPages (no API keys needed).

The worker writes:

1. report markdown: `reports/{###}-{market-slug}-{YYYY-MM-DD}.md`
2. staged tracker line: `batch/tracker-additions/{id}.tsv`
3. JSON completion payload to stdout

## OpenCode Slash Commands

This repo now includes project-local OpenCode slash commands in `.opencode/commands/`.

Use them from OpenCode TUI as:

- `/market-validation`
- `/market-validation-batch`
- `/market-validation-worker`
- `/market-validation-merge`
- `/market-validation-verify`
- `/market-validation-pipeline`
- `/market-validation-store-output`
- `/market-validation-config-check`
- `/market-validation-stage-run`
- `/market-validation-run`

Examples:

```text
/market-validation-batch --dry-run
/market-validation-batch --model "provider/model" --agent "general"
/market-validation-worker --id 1 --market "ai qa agent for cnc shops" --report-num 001 --model "provider/model"
/market-validation-pipeline
/market-validation-store-output --input-file output/sample-stage.json
/market-validation-config-check
/market-validation-stage-run --stage research_ingest --run-id brisket-001
/market-validation-run --run-id brisket-001
```

## Batch Input Format

`batch/batch-input.tsv` columns:

`id<TAB>market<TAB>geography<TAB>profile<TAB>template<TAB>notes`

## Run Batch Pipeline

```bash
bash batch/batch-runner.sh
```

Default batch mode now auto-persists each worker JSON result into the file output store (`worker_result` stage), so lead JSONL + markdown dashboard/call sheet stay current.

Use explicit OpenCode model/agent for this run:

```bash
bash batch/batch-runner.sh --model "provider/model" --agent "general"
```

Dry-run mode:

```bash
bash batch/batch-runner.sh --dry-run
```

## Merge and Verify

```bash
python merge-tracker.py
python verify-pipeline.py
```

## Worker (Single Item)

```bash
python -m market_validation.batch_worker \
  --id 1 \
  --market "ai qa agent for cnc shops" \
  --geography "US" \
  --profile "saas" \
  --report-num 001
```

Use explicit flags for worker invocation. This keeps execution deterministic and makes failures easier to diagnose.

OpenCode router inputs can still be flexible: shorthand positional input can be normalized by the router into an explicit-flag command before execution.

Optional OpenCode flags:

```bash
python -m market_validation.batch_worker \
  --id 1 \
  --market "ai qa agent for cnc shops" \
  --report-num 001 \
  --model "provider/model" \
  --agent "general"
```

Or set environment defaults:

- `OPENCODE_MODEL`
- `OPENCODE_AGENT`

If both are set, CLI flags (`--model`, `--agent`) take precedence.

### Worker + Auto Store Wrapper

Run one worker and automatically persist its JSON output to the file store:

```bash
bash batch/worker-and-store.sh --id 1 --market "ai qa agent for cnc shops" --report-num 001
```

This writes/updates:

- `output/runs/{run_id}/worker-result.json`
- `output/leads/leads.jsonl`
- `output/call-sheets/{YYYY-MM-DD}.md`
- `output/dashboard/summary.md`
- `output/market-validation.sqlite3`

## Call Notes Commands

Store and read human call notes in the database (for call sheets and follow-up context):

```bash
python call-notes.py add \
  --root . \
  --company-id smoke-house-3 \
  --author "caller-a" \
  --note "Purchasing manager asked for pricing and Friday callback" \
  --next-action "Call Friday afternoon"
```

```bash
python call-notes.py list --root . --company-id smoke-house-3 --limit 20
```

Installed script equivalent:

```bash
market-call-notes list --root . --limit 20
```

## Store Output Command

Persist any stage payload manually when needed:

```bash
python store-output.py --input-file output/sample-stage.json
```

Or via slash command:

```text
/market-validation-store-output --input-file output/sample-stage.json
```

You can override `run_id` or `stage` if needed:

```bash
python store-output.py --input-file output/sample-stage.json --run-id brisket-001 --stage lead_qualify
```

## Lead Pipeline Commands

Validate config:

```bash
python lead-pipeline.py config-check --config config/lead-pipeline.json
```

Run one stage:

```bash
python lead-pipeline.py stage-run --stage research_ingest --run-id brisket-001
```

Run full pipeline:

```bash
python lead-pipeline.py run --run-id brisket-001
```

Optional flags:

- `--start-stage` / `--end-stage`
- `--messages-file` (for reply parse stage)
- `--model` / `--agent`
- `--config` / `--root`

## Install

```bash
pip install -e .[dev]
```

## Tests

```bash
pytest
```

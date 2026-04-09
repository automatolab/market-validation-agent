# Market Validation Agent

Prompt-driven market validation pipeline designed for OpenCode workflows.

## What This Repo Is

This is a **mode-first** architecture:

1. prompts define behavior (`modes/*.md`)
2. each market item is processed by an isolated worker (`market_validation/batch_worker.py`)
3. workers stage tracker rows (`batch/tracker-additions/*.tsv`)
4. staged rows are merged into a canonical tracker (`data/validation-tracker.md`)
5. integrity checks run before review (`verify-pipeline.py`)

Business logic is prompt-driven; code is intentionally thin and deterministic.

## Project Layout

- `modes/_shared.md` - global market-validation rules
- `modes/validate.md` - single-item validation contract
- `modes/auto-pipeline.md` - full item pipeline contract
- `modes/batch.md` - batch orchestration contract
- `modes/research-ingest.md` - configured-source ingestion contract
- `modes/lead-qualify.md` - evidence-linked company qualification contract
- `modes/outreach-email.md` - template-driven outreach draft contract
- `modes/reply-parse.md` - inbound reply parsing/status contract
- `modes/call-sheet-build.md` - call sheet ranking contract
- `batch/batch-prompt.md` - worker prompt contract
- `batch/batch-runner.sh` - deterministic batch orchestrator
- `market_validation/batch_worker.py` - OpenCode-backed worker executor
- `market_validation/file_pipeline.py` - merge + verify core
- `merge-tracker.py` - merge CLI wrapper
- `verify-pipeline.py` - verify CLI wrapper
- `templates/states.yml` - canonical statuses
- `data/validation-tracker.md` - canonical tracker

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
- configured-source only ingestion (no autonomous source discovery)
- evidence URLs required for every qualification claim
- lead statuses include `new`, `qualified`, `emailed`, `replied_interested`, `replied_not_now`, `do_not_contact`, `call_ready`

## File-Based Output Store (No DB Required)

Stage JSON payloads can be persisted directly to files and materialized into markdown views:

- `output/runs/{run_id}/{stage}.json` - per-stage canonical payloads
- `output/leads/leads.jsonl` - latest lead state per company
- `output/call-sheets/{YYYY-MM-DD}.md` - call sheet for human follow-up
- `output/dashboard/summary.md` - status and priority summary

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

Notes:

- Keep provider/model defaults in config for consistency across runs.
- Keep secrets (API keys, tokens) out of JSON config; use environment variables.
- Source configs are operator-owned; prompts enforce configured-source-only behavior.

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

Examples:

```text
/market-validation-batch --dry-run
/market-validation-batch --model "provider/model" --agent "general"
/market-validation-worker --id 1 --market "ai qa agent for cnc shops" --report-num 001 --model "provider/model"
/market-validation-pipeline
/market-validation-store-output --input-file output/sample-stage.json
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

## Install

```bash
pip install -e .[dev]
```

## Tests

```bash
pytest
```

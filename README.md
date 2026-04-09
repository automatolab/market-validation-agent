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

Examples:

```text
/market-validation-batch --dry-run
/market-validation-batch --model "provider/model" --agent "general"
/market-validation-worker --id 1 --market "ai qa agent for cnc shops" --report-num 001 --model "provider/model"
/market-validation-pipeline
```

## Batch Input Format

`batch/batch-input.tsv` columns:

`id<TAB>market<TAB>geography<TAB>profile<TAB>template<TAB>notes`

## Run Batch Pipeline

```bash
bash batch/batch-runner.sh
```

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

## Install

```bash
pip install -e .[dev]
```

## Tests

```bash
pytest
```

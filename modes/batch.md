# Mode: batch

Batch mode processes many market ideas with isolated workers.

## Architecture

- Orchestrator reads `batch/batch-input.tsv`
- Each row spawns one worker invocation
- Worker writes report + staged tracker line
- Merge step consolidates staged lines into canonical tracker
- Verify step checks integrity

## Lead Pipeline Extension

For market workflows that include outbound and replies (for example brisket supply outreach), each worker can execute staged prompt contracts:

- `research-ingest`
- `lead-qualify`
- `outreach-email`
- `reply-parse`
- `call-sheet-build`

Each stage must emit strict JSON suitable for ingestion into a database-backed dashboard.

## Input Format

`id<TAB>market<TAB>geography<TAB>profile<TAB>template<TAB>notes`

## Retry Strategy

- Store status in `batch/batch-state.tsv`
- Retry only failed items
- Keep per-item logs in `batch/logs/`

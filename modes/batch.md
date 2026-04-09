# Mode: batch

Batch mode processes many market ideas with isolated workers.

## Architecture

- Orchestrator reads `batch/batch-input.tsv`
- Each row spawns one worker invocation
- Worker writes report + staged tracker line
- Merge step consolidates staged lines into canonical tracker
- Verify step checks integrity

## Input Format

`id<TAB>market<TAB>geography<TAB>profile<TAB>template<TAB>notes`

## Retry Strategy

- Store status in `batch/batch-state.tsv`
- Retry only failed items
- Keep per-item logs in `batch/logs/`

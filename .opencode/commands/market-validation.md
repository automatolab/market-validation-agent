---
description: Router for market-validation batch and worker commands
---

Market validation command router.

Raw arguments: "$ARGUMENTS"

Interpret the first token as a subcommand:

- `help` or empty: show concise usage and examples.
- `batch ...`: run `bash batch/batch-runner.sh ...` with remaining arguments.
- `worker ...`: run `python -m market_validation.batch_worker ...` with remaining arguments.
- `merge`: run `python merge-tracker.py`.
- `verify`: run `python verify-pipeline.py`.
- `pipeline`: run `python merge-tracker.py` then `python verify-pipeline.py`.

Execution rules:

1. Always run from repo root.
2. Pass arguments through exactly for `batch` and `worker`.
3. Summarize key output lines (processed, failures, warnings) in plain language.
4. If a command fails, report the failure reason and suggest the exact next corrective command.

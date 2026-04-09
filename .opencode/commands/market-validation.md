---
description: Router for market-validation batch and worker commands
---

Market validation command router.

Raw arguments: "$ARGUMENTS"

Interpret the first token as a subcommand:

- `help` or empty: show concise usage and examples.
- `batch ...`: run `bash batch/batch-runner.sh ...` with remaining arguments.
- `worker ...`: run `python -m market_validation.batch_worker ...` with remaining arguments.
- `store ...`: run `python store-output.py ...` with remaining arguments.
- `merge`: run `python merge-tracker.py`.
- `verify`: run `python verify-pipeline.py`.
- `pipeline`: run `python merge-tracker.py` then `python verify-pipeline.py`.

Execution rules:

1. Always run from repo root.
2. Pass arguments through exactly for `batch`; for `worker`, pass through exactly when required flags are present.
3. Summarize key output lines (processed, failures, warnings) in plain language.
4. If a command fails, report the failure reason and suggest the exact next corrective command.
5. For `worker`, keep the Python command contract strict but make router-level input flexible:
   - if required flags are present, pass through exactly
   - if missing, infer and execute one explicit-flag worker command
6. Prefer explicit-flag commands in all corrective suggestions.
7. For `store`, pass arguments through exactly and summarize output file paths.

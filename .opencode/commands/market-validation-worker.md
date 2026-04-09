---
description: Run one market-validation worker item
---

Run one worker item with provided arguments:

```bash
python -m market_validation.batch_worker $ARGUMENTS
```

Argument handling:

- Keep the Python worker strict, but make orchestration flexible.
- If user already provides `--id`, `--market`, and `--report-num`, pass through exactly.
- If required flags are missing, infer them from shorthand positional tokens and run one explicit-flag command:
  - leading number -> `--id`
  - trailing number -> `--report-num` (zero-padded to 3 digits)
  - remaining text -> `--market`
  - if `--report-num` is still missing, infer next sequential report number from `reports/*.md`
  - if `--id` is still missing, set it equal to inferred report number
- Preserve optional flags exactly as provided (`--geography`, `--profile`, `--template`, `--model`, `--agent`, `--date`, `--root`).
- If shorthand cannot be inferred safely, fail with one exact corrective command.

Expected minimum arguments:

- `--id`
- `--market`
- `--report-num`

Optional:

- `--geography`
- `--profile`
- `--template`
- `--model`
- `--agent`
- `--date`
- `--root`

After execution, parse the returned JSON and summarize:

- status
- report path
- any error
- include the resolved command when inference was used

Failure handling:

- If the command exits non-zero, include:
  1. failure reason (first useful CLI error line)
  2. one exact corrective command using explicit flags

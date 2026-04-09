---
description: Run one market-validation worker item
---

Run one worker item with provided arguments:

```bash
python -m market_validation.batch_worker $ARGUMENTS
```

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

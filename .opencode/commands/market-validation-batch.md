---
description: Run market-validation batch runner
---

Run the batch pipeline:

```bash
bash batch/batch-runner.sh $ARGUMENTS
```

After execution, summarize:

- total completed vs failed from `batch/batch-state.tsv` updates
- merge result line
- verify warnings/errors if any

If no arguments are provided, run default batch mode.

Common examples:

- `/market-validation-batch --dry-run`
- `/market-validation-batch --start-from 10 --retry-failed`
- `/market-validation-batch --model "provider/model" --agent "general"`

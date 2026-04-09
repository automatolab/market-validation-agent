---
description: Persist stage JSON output to file store
---

Persist one stage JSON payload into the file-based output store.

Run:

```bash
python store-output.py $ARGUMENTS
```

Common usage:

- `/market-validation-store-output --input-file output/sample-stage.json`
- `/market-validation-store-output --stage worker_result --run-id brisket-001 --input-file output/worker-result.json`

Summarize:

- stage file path
- leads file path
- call sheet and dashboard paths
- updated lead count

Failure handling:

- If store command fails, report the first actionable error line and show one exact corrective command.

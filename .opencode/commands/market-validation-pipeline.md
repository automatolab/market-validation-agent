---
description: Merge then verify market-validation pipeline
---

Run the deterministic post-processing pipeline in order:

```bash
python merge-tracker.py && python verify-pipeline.py
```

Summarize:

1. merge counts
2. verify warnings/errors
3. whether pipeline is green

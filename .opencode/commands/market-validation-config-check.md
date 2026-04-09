---
description: Validate lead pipeline config
---

Validate lead pipeline configuration and required prompt files.

Run:

```bash
python lead-pipeline.py config-check $ARGUMENTS
```

Default behavior (no arguments):

- validate `config/lead-pipeline.json`
- fallback to `config/lead-pipeline.example.json` if needed

Summarize:

- resolved config path
- validation errors and warnings

If validation fails, show the exact next command with a `--config` path.

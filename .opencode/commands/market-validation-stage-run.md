---
description: Run one lead pipeline stage from config
---

Run a single lead pipeline stage using config context and auto-store outputs.

Run:

```bash
python lead-pipeline.py stage-run $ARGUMENTS
```

Required argument:

- `--stage` one of:
  - `research_ingest`
  - `lead_qualify`
  - `outreach_email`
  - `reply_parse`
  - `call_sheet_build`

Common examples:

- `/market-validation-stage-run --stage research_ingest`
- `/market-validation-stage-run --stage lead_qualify --run-id brisket-001`
- `/market-validation-stage-run --stage reply_parse --run-id brisket-001 --messages-file output/inbound/messages.json`

Summarize:

- stage result (`ok`/`failed`)
- stored stage file path
- updated lead count
- call sheet/dashboard paths

If the stage fails, include first actionable error line and one exact corrective command.

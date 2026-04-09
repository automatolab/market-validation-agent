---
description: Run full staged lead pipeline from config
---

Run the full configured lead pipeline with automatic per-stage output persistence.

Run:

```bash
python lead-pipeline.py run $ARGUMENTS
```

Default stage order:

1. `research_ingest`
2. `lead_qualify`
3. `outreach_email`
4. `reply_parse`
5. `call_sheet_build`

Useful options:

- `--run-id brisket-001`
- `--start-stage lead_qualify`
- `--end-stage call_sheet_build`
- `--messages-file output/inbound/messages.json`
- `--model provider/model`
- `--agent general`

Summarize:

- overall result
- per-stage status and failure mode
- generated artifact paths (`output/runs`, leads JSONL, call sheet, dashboard)

If a stage fails, report the failed stage and exact next corrective command.

# Mode: call-sheet-build

## Goal

Build ranked call sheets for human follow-up from database-ready company records and reply outcomes.

## Required Input JSON

```json
{
  "run_id": "string",
  "market": "string",
  "records": [
    {
      "company_id": "string",
      "company_name": "string",
      "status": "qualified|emailed|replied_interested|replied_not_now|do_not_contact|new",
      "priority_signals": {
        "interest_level": "high|medium|low|unknown",
        "volume_signal": "high|medium|low|unknown",
        "recency_days": 0
      },
      "latest_summary": "string"
    }
  ]
}
```

## Output JSON Contract

```json
{
  "result": "ok|failed",
  "stage": "call_sheet_build",
  "run_id": "string",
  "call_sheet": [
    {
      "company_id": "string",
      "company_name": "string",
      "status": "call_ready|replied_interested|qualified",
      "priority_score": 0,
      "priority_tier": "P1|P2|P3",
      "why_now": "string",
      "next_action": "string",
      "notes_for_caller": "string"
    }
  ],
  "warnings": ["string"],
  "errors": ["string"],
  "failure_mode": "none|invalid_input|empty_records|unknown"
}
```

## Rules

- Return exactly one JSON object and nothing else.
- Exclude `do_not_contact` from call-sheet output.
- Promote high-intent records to `status: "call_ready"`.
- Keep `why_now` and `next_action` specific enough for a human caller to execute.

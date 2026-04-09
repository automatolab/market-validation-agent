# Mode: reply-parse

## Goal

Parse inbound company replies and map each to pipeline status + structured dashboard-ready fields.

## Required Input JSON

```json
{
  "run_id": "string",
  "messages": [
    {
      "message_id": "string",
      "company_id": "string",
      "from_email": "string",
      "subject": "string",
      "body": "string",
      "received_at": "ISO-8601"
    }
  ]
}
```

## Output JSON Contract

```json
{
  "result": "ok|failed",
  "stage": "reply_parse",
  "run_id": "string",
  "updates": [
    {
      "message_id": "string",
      "company_id": "string",
      "status": "replied_interested|replied_not_now|do_not_contact|qualified",
      "intent": "interested|not_now|not_a_fit|unsubscribe|question|other",
      "summary": "string",
      "structured_fields": {
        "requested_follow_up": true,
        "requested_sample": false,
        "budget_signal": "string|null",
        "timeframe_signal": "string|null",
        "contact_preference": "email|phone|unknown"
      }
    }
  ],
  "warnings": ["string"],
  "errors": ["string"],
  "failure_mode": "none|invalid_input|unparseable_message|unknown"
}
```

## Rules

- Return exactly one JSON object and nothing else.
- Use conservative classification when intent is ambiguous.
- If user asks to stop contact, set `status: "do_not_contact"`.
- Keep summaries factual and grounded in message text.

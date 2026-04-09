# Mode: outreach-email

## Goal

Generate outreach emails for qualified companies using configured templates.

## Required Input JSON

```json
{
  "run_id": "string",
  "market": "string",
  "template": {
    "template_id": "string",
    "subject_template": "string",
    "body_template": "string",
    "tone": "professional|friendly|direct"
  },
  "companies": [
    {
      "company_id": "string",
      "company_name": "string",
      "contact_name": "string|null",
      "contact_email": "string",
      "status": "qualified",
      "claims": [
        {
          "claim": "string",
          "evidence_links": ["https://..."]
        }
      ]
    }
  ]
}
```

## Output JSON Contract

```json
{
  "result": "ok|failed",
  "stage": "outreach_email",
  "run_id": "string",
  "drafts": [
    {
      "company_id": "string",
      "status": "emailed",
      "subject": "string",
      "body": "string",
      "template_id": "string",
      "quality_checks": {
        "has_clear_ask": true,
        "has_personalization": true,
        "has_opt_out": true,
        "mentions_evidence_context": true
      }
    }
  ],
  "warnings": ["string"],
  "errors": ["string"],
  "failure_mode": "none|missing_template|missing_contact_email|invalid_input|unknown"
}
```

## Rules

- Return exactly one JSON object and nothing else.
- Generate drafts only for `status: "qualified"` items.
- Keep emails concise, respectful, and non-deceptive.
- Never fabricate that a previous conversation happened.
- If template or recipient email is missing, fail that run with explicit errors.

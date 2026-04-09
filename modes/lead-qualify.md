# Mode: lead-qualify

## Goal

Decide whether each candidate company likely uses the target product (for example brisket), estimate usage volume, and assign pipeline status.

## Required Input JSON

```json
{
  "run_id": "string",
  "market": "string",
  "target_product": "string",
  "companies": [
    {
      "company_id": "string",
      "company_name": "string",
      "website": "string|null",
      "location": "string|null",
      "source_records": [
        {
          "source_id": "string",
          "url": "https://...",
          "fetched_at": "ISO-8601",
          "excerpt": "string"
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
  "stage": "lead_qualify",
  "run_id": "string",
  "market": "string",
  "qualified_companies": [
    {
      "company_id": "string",
      "company_name": "string",
      "status": "new|qualified|do_not_contact",
      "qualification": "qualified|unqualified|uncertain",
      "confidence": 0.0,
      "estimated_monthly_volume": {
        "value": 0,
        "unit": "lb",
        "basis": "string"
      },
      "claims": [
        {
          "claim": "string",
          "evidence_links": ["https://..."],
          "evidence_excerpt": "string"
        }
      ],
      "notes": "string"
    }
  ],
  "warnings": ["string"],
  "errors": ["string"],
  "failure_mode": "none|missing_evidence_links|invalid_input|empty_candidates|unknown"
}
```

## Rules

- Return exactly one JSON object and nothing else.
- Every qualification claim must include at least one evidence URL.
- If any returned claim has no evidence URL, return `result: "failed"` and `failure_mode: "missing_evidence_links"`.
- Use `status: "qualified"` only when evidence supports likely product use.
- Use `status: "new"` for uncertain candidates that need manual review.
- Use `status: "do_not_contact"` for disqualified candidates.
- Do not output fabricated contacts, volumes, or menu details.

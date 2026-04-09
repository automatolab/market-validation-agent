# Mode: research-ingest

## Goal

Collect candidate companies for one market from operator-configured sources only.

## Required Input JSON

```json
{
  "run_id": "string",
  "market": "string",
  "geography": "string",
  "max_companies": 100,
  "source_configs": [
    {
      "source_id": "string",
      "source_type": "search|review_site|directory|internal_feed",
      "query": "string",
      "region": "string",
      "enabled": true
    }
  ]
}
```

## Output JSON Contract

```json
{
  "result": "ok|failed",
  "stage": "research_ingest",
  "run_id": "string",
  "market": "string",
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
  ],
  "warnings": ["string"],
  "errors": ["string"],
  "failure_mode": "none|missing_source_config|source_unreachable|invalid_source_payload|rate_limited|empty_results|unknown"
}
```

## Rules

- Return exactly one JSON object and nothing else.
- Use only `source_configs` provided in input; do not discover new sources.
- If `source_configs` is empty or all disabled, return `result: "failed"` with `failure_mode: "missing_source_config"`.
- Every returned company must include at least one `source_records` item with an HTTPS URL.
- Deduplicate companies by normalized `company_name + location`.
- Do not infer business facts in this stage; only return source-backed candidates.

# Mode: lead-qualify

## Goal

Decide whether each candidate company likely uses the target product (for example brisket), estimate usage volume using market demand data, and assign pipeline status.

## Required Input JSON

```json
{
  "run_id": "string",
  "market": "string",
  "target_product": "string",
  "geography": "string",
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
  "market_demand_data": {
    "demand_level": "high|medium|low|unknown",
    "market_demand_score": 0,
    "keywords": {}
  }
}
```

## Output JSON Contract

```json
{
  "result": "ok|failed",
  "stage": "lead_qualify",
  "run_id": "string",
  "market": "string",
  "market_demand": {
    "demand_level": "high|medium|low|unknown",
    "demand_score": 0,
    "demand_basis": "string"
  },
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
      "volume_tier": "high|medium|low|unknown",
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

## Volume Estimation Guidelines

Use market demand data combined with company signals to estimate volume:

### Market Demand Multipliers (apply to baseline estimates)
- **High demand** (score > 50): 1.5x multiplier
- **Medium demand** (score 20-50): 1.0x multiplier
- **Low demand** (score < 20): 0.7x multiplier

### Company Signal Baseline (lb/month)
- **High volume** (500+ reviews or #1 ranked): 2,000-3,000 lb
- **Medium-high volume** (200-499 reviews): 1,000-2,000 lb
- **Medium volume** (50-199 reviews): 500-1,000 lb
- **Low volume** (<50 reviews): 200-500 lb

### Always cite your basis for volume estimates in the `basis` field.

## Rules

- Return exactly one JSON object and nothing else.
- Every qualification claim must include at least one evidence URL.
- If any returned claim has no evidence URL, return `result: "failed"` and `failure_mode: "missing_evidence_links"`.
- Use `status: "qualified"` only when evidence supports likely product use.
- Use `status: "new"` for uncertain candidates that need manual review.
- Use `status: "do_not_contact"` for disqualified candidates.
- Do not output fabricated contacts, volumes, or menu details.
- Volume estimates must include a `basis` explaining the calculation.

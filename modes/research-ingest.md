# Mode: research-ingest

## Goal

Collect candidate companies for one market from operator-configured sources, or auto-discover sources if configured.

## Input JSON (with optional auto-discovery)

```json
{
  "run_id": "string",
  "market": "string",
  "geography": "string",
  "target_product": "string",
  "max_companies": 100,
  "source_configs": [
    {
      "source_id": "string",
      "source_type": "search|review_site|directory|internal_feed",
      "query": "string",
      "region": "string",
      "enabled": true
    }
  ],
  "auto_discover_sources": true,
  "use_websearch_for_discovery": false,
  "max_discovered_sources": 5
}
```

## Auto-Discovery Behavior

If `auto_discover_sources` is `true` and `source_configs` is empty or missing, the pipeline will automatically discover data sources based on market type:

- **Restaurant/Food markets**: Discovers Yelp, Google Maps, and search sources
- **Retail markets**: Discovers search and Yelp sources  
- **Other markets**: Uses general search sources

If `use_websearch_for_discovery` is `true`, it will use web search to find relevant data sources for the market.

Example auto-discovery config:

```json
{
  "market": "Brisket",
  "geography": "US",
  "target_product": "brisket",
  "auto_discover_sources": true,
  "use_websearch_for_discovery": true,
  "max_discovered_sources": 5
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
- If `auto_discover_sources` is `true` and `source_configs` is empty, auto-discover sources based on market type.
- If `source_configs` is provided, use only those sources (auto-discovery is skipped).
- If `source_configs` is empty, all disabled, and auto-discovery fails, return `result: "failed"` with `failure_mode: "missing_source_config"`.
- Every returned company must include at least one `source_records` item with an HTTPS URL.
- Deduplicate companies by normalized `company_name + location`.
- Do not infer business facts in this stage; only return source-backed candidates.

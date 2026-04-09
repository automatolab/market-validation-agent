# Mode: auto-pipeline

When the user provides a market phrase, run the full validation pipeline:

1. Build market search request.
2. Run research mission and evidence extraction.
3. Score with stage-aware guardrails.
4. Save report.
5. Write staged tracker line.
6. Return JSON summary.

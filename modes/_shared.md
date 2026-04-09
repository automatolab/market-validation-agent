# Market Validation - Shared Mode Context

## Mission

Validate market opportunities with evidence-first reasoning. Treat each market idea as a unit of work that can be processed independently and merged into a canonical tracker.

## Global Rules

- Never invent market evidence, pricing, or competitor details.
- Prefer fetched-page evidence over search snippets.
- If evidence quality is weak, return `insufficient_evidence` style outcomes.
- Keep reports actionable: risks, unknowns, and next experiments.
- Write staged tracker rows to `batch/tracker-additions/` and merge later.

## Canonical Statuses

- `new`
- `scanning`
- `validated`
- `interviewing`
- `test_ready`
- `monitor`
- `rejected`
- `archived`

## Output Contract Per Item

1. Validation report in `reports/{###}-{market-slug}-{YYYY-MM-DD}.md`
2. One staged tracker row in `batch/tracker-additions/{id}.tsv`
3. JSON completion payload with score, paths, and error state

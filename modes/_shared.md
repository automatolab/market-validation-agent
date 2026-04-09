# Market Validation - Shared Mode Context

## Mission

Validate market opportunities with evidence-first reasoning. Treat each market idea as a unit of work that can be processed independently and merged into a canonical tracker.

## Global Rules

- Never invent market evidence, pricing, or competitor details.
- Prefer fetched-page evidence over search snippets.
- If evidence quality is weak, return `insufficient_evidence` style outcomes.
- Keep reports actionable: risks, unknowns, and next experiments.
- Write staged tracker rows to `batch/tracker-additions/` and merge later.

## Source Configuration Rules

- Do not discover new data sources on your own.
- Use only operator-configured sources from the ingestion registry.
- If no configured source can satisfy the task, return a failed JSON payload with `failure_mode: "missing_source_config"`.

## JSON Contract Rules

- Return exactly one JSON object and nothing else.
- Do not use markdown fences.
- Include machine-readable failure details (`result`, `failure_mode`, and `errors`) in all workflows.

## Evidence Link Rules

- Every qualification claim must include at least one evidence URL.
- If any claim has no evidence link, fail the item with `failure_mode: "missing_evidence_links"`.

## Canonical Statuses

- `new`
- `scanning`
- `validated`
- `interviewing`
- `test_ready`
- `monitor`
- `rejected`
- `archived`

## Lead Pipeline Statuses

- `new`
- `qualified`
- `emailed`
- `replied_interested`
- `replied_not_now`
- `do_not_contact`
- `call_ready`

## Output Contract Per Item

1. Validation report in `reports/{###}-{market-slug}-{YYYY-MM-DD}.md`
2. One staged tracker row in `batch/tracker-additions/{id}.tsv`
3. JSON completion payload with score, paths, and error state

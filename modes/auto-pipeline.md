# Mode: auto-pipeline

When the user provides a market phrase, run the configured lead-generation + validation pipeline.

## Stages

1. `research-ingest` - collect candidate companies from configured sources.
2. `lead-qualify` - assign `new|qualified|do_not_contact` with evidence links for every claim.
3. `outreach-email` - generate template-driven outreach drafts and mark as `emailed`.
4. `reply-parse` - classify inbound replies into `replied_interested|replied_not_now|do_not_contact|qualified`.
5. `call-sheet-build` - rank follow-up list and move high-intent leads to `call_ready`.
6. Persist all outputs to DB-ready JSON and return run summary JSON.

## Hard Rules

- JSON-only outputs at every stage.
- No source discovery; use configured source registry only.
- Any qualification claim without evidence links is a failed run.

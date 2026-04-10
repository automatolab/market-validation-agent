# Graph Report - .  (2026-04-09)

## Corpus Check
- Corpus is ~10,310 words - fits in a single context window. You may not need a graph.

## Summary
- 184 nodes · 332 edges · 18 communities detected
- Extraction: 92% EXTRACTED · 8% INFERRED · 0% AMBIGUOUS · INFERRED: 27 edges (avg confidence: 0.87)
- Token cost: 0 input · 0 output

## God Nodes (most connected - your core abstractions)
1. `persist_stage_result()` - 12 edges
2. `merge_tracker_additions()` - 11 edges
3. `Shared Global Market Validation Rules` - 11 edges
4. `run_worker()` - 10 edges
5. `main()` - 10 edges
6. `Market Validation Agent` - 9 edges
7. `_base_config()` - 8 edges
8. `_write_required_modes()` - 8 edges
9. `_apply_lead_qualify()` - 8 edges
10. `_apply_stage()` - 8 edges

## Surprising Connections (you probably didn't know these)
- `Shared Lead Pipeline Statuses` --semantically_similar_to--> `Lead Pipeline Statuses`  [INFERRED] [semantically similar]
  modes/_shared.md → README.md
- `Validate Mode` --semantically_similar_to--> `Batch Worker Prompt Contract`  [INFERRED] [semantically similar]
  modes/validate.md → batch/batch-prompt.md
- `Mode-First Architecture` --rationale_for--> `Shared Global Market Validation Rules`  [INFERRED]
  README.md → modes/_shared.md
- `Batch Worker Output Quality Policy` --conceptually_related_to--> `JSON Contract Rules`  [INFERRED]
  batch/batch-prompt.md → modes/_shared.md
- `Batch Architecture` --conceptually_related_to--> `Batch Runner Shell Script`  [INFERRED]
  modes/batch.md → README.md

## Hyperedges (group relationships)
- **Lead Generation Staged Pipeline** — researchingest_mode, leadqualify_mode, outreachemail_mode, replyparse_mode, callsheetbuild_mode [EXTRACTED 1.00]
- **Batch Worker Prompt Composition** — shared_global_rules, validate_mode, batchprompt_worker_prompt, readme_batch_worker_py [EXTRACTED 0.95]
- **Tracker Lifecycle Flow** — readme_batch_worker_py, readme_merge_tracker_py, tracker_validation_tracker, readme_verify_pipeline_py [EXTRACTED 0.95]

## Communities

### Community 0 - "Output Store & Apply Functions"
Cohesion: 0.17
Nodes (28): _apply_call_sheet_build(), _apply_lead_qualify(), _apply_outreach_email(), _apply_reply_parse(), _apply_research_ingest(), _apply_stage(), _apply_worker_result(), build_parser() (+20 more)

### Community 1 - "Lead Pipeline Core"
Cohesion: 0.15
Nodes (26): build_parser(), build_stage_payload(), build_stage_prompt(), ConfigValidationResult, _days_since(), _default_run_id(), _extract_first_json_object(), _interest_level() (+18 more)

### Community 2 - "Worker Prompt & Scoring Rules"
Cohesion: 0.12
Nodes (28): Auto-Pipeline Hard Rules, Batch Architecture, Batch Worker Output Quality Policy, Batch Worker Scoring Policy, Batch Worker Prompt Contract, Evidence URL Requirement for Qualification, Batch Runner Shell Script, Batch Worker Python Module (+20 more)

### Community 3 - "Pipeline Mode Definitions"
Cohesion: 0.11
Nodes (27): Auto-Pipeline Mode, Batch Mode, Batch Retry Strategy, Batch State File, Call Sheet Build Input Contract, Call Sheet Build Mode, Call Sheet Build Output Contract, Priority Tier Ranking P1/P2/P3 (+19 more)

### Community 4 - "File Pipeline & Tracker"
Cohesion: 0.25
Nodes (16): ensure_tracker_file(), extract_report_target(), main_merge_cli(), main_verify_cli(), merge_tracker_additions(), MergeResult, _move_to_merged(), normalize_status() (+8 more)

### Community 5 - "Batch Worker Execution"
Cohesion: 0.27
Nodes (13): build_opencode_prompt(), build_parser(), _ensure_output_dirs(), _extract_first_json_object(), fallback_report_markdown(), format_score(), invoke_opencode(), main() (+5 more)

### Community 6 - "Lead Pipeline Tests"
Cohesion: 0.47
Nodes (10): _base_config(), test_build_stage_payload_call_sheet_build_reads_lead_jsonl(), test_load_config_uses_root_relative_and_fallback(), test_validate_config_detects_missing_fields(), test_validate_config_rejects_bad_source_type_and_duplicate_id(), test_validate_config_rejects_foursquare_bearer_scheme(), test_validate_config_requires_auth_env_for_enabled_foursquare(), test_validate_config_success() (+2 more)

### Community 7 - "Batch Worker Tests"
Cohesion: 0.43
Nodes (4): _args(), test_run_worker_accepts_replied_interested_status(), test_run_worker_uses_fallbacks_for_missing_or_invalid_fields(), test_run_worker_writes_report_and_tracker_row()

### Community 8 - "Output Store Tests"
Cohesion: 0.53
Nodes (4): _read_jsonl(), test_persist_stage_result_builds_markdown_views_from_pipeline_updates(), test_persist_stage_result_writes_run_payload_and_lead_state(), test_persist_worker_result_stage_updates_lead_state()

### Community 9 - "File Pipeline Tests"
Cohesion: 0.7
Nodes (4): test_merge_tracker_additions_adds_and_updates_rows(), test_verify_pipeline_accepts_lead_pipeline_statuses(), test_verify_pipeline_detects_missing_report_and_pending_additions(), _write()

### Community 10 - "Environment Tests"
Cohesion: 1.0
Nodes (0): 

### Community 11 - "Package Init"
Cohesion: 1.0
Nodes (1): Prompt-driven market validation package.

### Community 12 - "Environment Config"
Cohesion: 1.0
Nodes (0): 

### Community 13 - "Store Output CLI"
Cohesion: 1.0
Nodes (0): 

### Community 14 - "Verify Pipeline CLI"
Cohesion: 1.0
Nodes (0): 

### Community 15 - "Merge Tracker CLI"
Cohesion: 1.0
Nodes (0): 

### Community 16 - "Status Definitions"
Cohesion: 1.0
Nodes (1): Canonical Statuses Template

### Community 17 - "OpenCode Commands"
Cohesion: 1.0
Nodes (1): OpenCode Slash Commands

## Knowledge Gaps
- **12 isolated node(s):** `Prompt-driven market validation package.`, `Lead Pipeline Statuses`, `Canonical Statuses Template`, `Lead Pipeline Config`, `OpenCode Slash Commands` (+7 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Environment Tests`** (2 nodes): `test_environment.py`, `test_load_project_env_reads_root_dotenv()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Package Init`** (2 nodes): `__init__.py`, `Prompt-driven market validation package.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Environment Config`** (2 nodes): `environment.py`, `load_project_env()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Store Output CLI`** (1 nodes): `store-output.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Verify Pipeline CLI`** (1 nodes): `verify-pipeline.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Merge Tracker CLI`** (1 nodes): `merge-tracker.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Status Definitions`** (1 nodes): `Canonical Statuses Template`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `OpenCode Commands`** (1 nodes): `OpenCode Slash Commands`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Shared Global Market Validation Rules` connect `Worker Prompt & Scoring Rules` to `Pipeline Mode Definitions`?**
  _High betweenness centrality (0.031) - this node is a cross-community bridge._
- **Why does `Batch Mode` connect `Pipeline Mode Definitions` to `Worker Prompt & Scoring Rules`?**
  _High betweenness centrality (0.021) - this node is a cross-community bridge._
- **Why does `Lead Qualify Mode` connect `Pipeline Mode Definitions` to `Worker Prompt & Scoring Rules`?**
  _High betweenness centrality (0.013) - this node is a cross-community bridge._
- **Are the 3 inferred relationships involving `Shared Global Market Validation Rules` (e.g. with `Validate Mode` and `Lead Qualify Mode`) actually correct?**
  _`Shared Global Market Validation Rules` has 3 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Prompt-driven market validation package.`, `Lead Pipeline Statuses`, `Canonical Statuses Template` to the rest of the system?**
  _12 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Worker Prompt & Scoring Rules` be split into smaller, more focused modules?**
  _Cohesion score 0.12 - nodes in this community are weakly interconnected._
- **Should `Pipeline Mode Definitions` be split into smaller, more focused modules?**
  _Cohesion score 0.11 - nodes in this community are weakly interconnected._
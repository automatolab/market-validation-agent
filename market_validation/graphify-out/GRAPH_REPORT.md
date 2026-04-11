# Graph Report - market_validation  (2026-04-10)

## Corpus Check
- 16 files · ~16,477 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 220 nodes · 423 edges · 16 communities detected
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS
- Token cost: 0 input · 0 output

## God Nodes (most connected - your core abstractions)
1. `ResearchManager` - 23 edges
2. `Agent` - 16 edges
3. `persist_stage_result()` - 13 edges
4. `merge_tracker_additions()` - 11 edges
5. `run_worker()` - 10 edges
6. `main()` - 10 edges
7. `_connect()` - 9 edges
8. `resolve_db_path()` - 9 edges
9. `_ensure_schema()` - 9 edges
10. `build_stage_payload()` - 8 edges

## Surprising Connections (you probably didn't know these)
- None detected - all connections are within the same source files.

## Communities

### Community 0 - "Community 0"
Cohesion: 0.11
Nodes (15): Agent, main(), Market Research Agent - Simple 3-step pipeline:  1. find()      - Discover compa, STEP 2: Qualify companies - AI assessment of relevance and volume., STEP 3: Enrich - Find contact info using 8 different sources.                  S, Update company record with enriched data., Source 1: Official website., Source 2: LinkedIn (via web search). (+7 more)

### Community 1 - "Community 1"
Cohesion: 0.16
Nodes (29): _apply_call_sheet_build(), _apply_lead_qualify(), _apply_outreach_email(), _apply_reply_parse(), _apply_research_ingest(), _apply_stage(), _apply_worker_result(), build_parser() (+21 more)

### Community 2 - "Community 2"
Cohesion: 0.14
Nodes (27): build_parser(), build_stage_payload(), build_stage_prompt(), ConfigValidationResult, _days_since(), _default_run_id(), _extract_first_json_object(), _interest_level() (+19 more)

### Community 3 - "Community 3"
Cohesion: 0.12
Nodes (2): main(), ResearchManager

### Community 4 - "Community 4"
Cohesion: 0.25
Nodes (16): ensure_tracker_file(), extract_report_target(), main_merge_cli(), main_verify_cli(), merge_tracker_additions(), MergeResult, _move_to_merged(), normalize_status() (+8 more)

### Community 5 - "Community 5"
Cohesion: 0.39
Nodes (16): add_call_note(), add_company(), add_contact(), build_parser(), _connect(), create_research(), _ensure_schema(), export_markdown() (+8 more)

### Community 6 - "Community 6"
Cohesion: 0.27
Nodes (13): build_opencode_prompt(), build_parser(), _ensure_output_dirs(), _extract_first_json_object(), fallback_report_markdown(), format_score(), invoke_opencode(), main() (+5 more)

### Community 7 - "Community 7"
Cohesion: 0.38
Nodes (9): build_parser(), build_reply_payload(), _decode_email_body(), _extract_headers(), fetch_and_build_replies(), fetch_email_replies(), _get_gmail_service(), _iso_now() (+1 more)

### Community 8 - "Community 8"
Cohesion: 0.5
Nodes (8): build_parser(), export_call_notes_for_company(), export_markdown_call_sheet(), export_markdown_dashboard(), get_call_sheet_from_db(), get_dashboard_summary_from_db(), _iso_now(), main()

### Community 9 - "Community 9"
Cohesion: 0.5
Nodes (7): build_parser(), _get_smtp_connection(), _iso_now(), main(), send_batch_emails(), send_email(), send_templated_email()

### Community 10 - "Community 10"
Cohesion: 0.6
Nodes (5): build_parser(), gather_companies(), main(), qualify_companies(), run_market_research()

### Community 11 - "Community 11"
Cohesion: 0.6
Nodes (4): build_parser(), enrich_company_contact(), enrich_research_companies(), main()

### Community 12 - "Community 12"
Cohesion: 0.8
Nodes (5): build_parser(), _detect_market_type(), discover_sources(), discover_sources_with_websearch(), main()

### Community 13 - "Community 13"
Cohesion: 0.67
Nodes (5): build_parser(), get_google_trends_data(), get_market_demand_report(), _iso_now(), main()

### Community 14 - "Community 14"
Cohesion: 1.0
Nodes (1): Prompt-driven market validation package.

### Community 15 - "Community 15"
Cohesion: 1.0
Nodes (0): 

## Knowledge Gaps
- **14 isolated node(s):** `Prompt-driven market validation package.`, `Market Research Agent - Simple 3-step pipeline:  1. find()      - Discover compa`, `Run opencode and return JSON.`, `STEP 1: Find companies in a market.                  Searches web for businesses`, `STEP 2: Qualify companies - AI assessment of relevance and volume.` (+9 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 14`** (2 nodes): `__init__.py`, `Prompt-driven market validation package.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 15`** (2 nodes): `environment.py`, `load_project_env()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What connects `Prompt-driven market validation package.`, `Market Research Agent - Simple 3-step pipeline:  1. find()      - Discover compa`, `Run opencode and return JSON.` to the rest of the system?**
  _14 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.11 - nodes in this community are weakly interconnected._
- **Should `Community 2` be split into smaller, more focused modules?**
  _Cohesion score 0.14 - nodes in this community are weakly interconnected._
- **Should `Community 3` be split into smaller, more focused modules?**
  _Cohesion score 0.12 - nodes in this community are weakly interconnected._
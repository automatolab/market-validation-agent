# Graph Report - market_validation  (2026-04-10)

## Corpus Check
- 12 files · ~12,983 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 143 nodes · 252 edges · 12 communities detected
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS
- Token cost: 0 input · 0 output

## God Nodes (most connected - your core abstractions)
1. `ResearchManager` - 23 edges
2. `Agent` - 16 edges
3. `_connect()` - 9 edges
4. `resolve_db_path()` - 9 edges
5. `_ensure_schema()` - 9 edges
6. `main()` - 7 edges
7. `main()` - 7 edges
8. `generate_html()` - 7 edges
9. `create_research()` - 6 edges
10. `get_research()` - 6 edges

## Surprising Connections (you probably didn't know these)
- None detected - all connections are within the same source files.

## Communities

### Community 0 - "Community 0"
Cohesion: 0.11
Nodes (15): Agent, main(), Market Research Agent - Simple 3-step pipeline:  1. find()      - Discover compa, STEP 2: Qualify companies - AI assessment of relevance and volume., STEP 3: Enrich - Find contact info using 8 different sources.                  S, Update company record with enriched data., Source 1: Official website., Source 2: LinkedIn (via web search). (+7 more)

### Community 1 - "Community 1"
Cohesion: 0.12
Nodes (2): main(), ResearchManager

### Community 2 - "Community 2"
Cohesion: 0.15
Nodes (21): approve_all_emails(), approve_email(), build_parser(), delete_email(), export_email_queue_markdown(), get_email_queue(), _get_smtp_connection(), _iso_now() (+13 more)

### Community 3 - "Community 3"
Cohesion: 0.42
Nodes (15): add_call_note(), add_company(), build_parser(), _connect(), create_research(), delete_company(), _ensure_schema(), export_markdown() (+7 more)

### Community 4 - "Community 4"
Cohesion: 0.35
Nodes (10): _escape_html(), generate_html(), _html_template(), _iso_now(), _load_data(), main(), _make_handler(), Dashboard HTML generator and optional local server.  Modes: - Static HTML: gener (+2 more)

### Community 5 - "Community 5"
Cohesion: 0.5
Nodes (8): build_parser(), export_call_notes_for_company(), export_markdown_call_sheet(), export_markdown_dashboard(), get_call_sheet_from_db(), get_dashboard_summary_from_db(), _iso_now(), main()

### Community 6 - "Community 6"
Cohesion: 0.6
Nodes (5): build_parser(), gather_companies(), main(), qualify_companies(), run_market_research()

### Community 7 - "Community 7"
Cohesion: 0.6
Nodes (4): build_parser(), enrich_company_contact(), enrich_research_companies(), main()

### Community 8 - "Community 8"
Cohesion: 0.8
Nodes (5): build_parser(), _detect_market_type(), discover_sources(), discover_sources_with_websearch(), main()

### Community 9 - "Community 9"
Cohesion: 0.67
Nodes (5): build_parser(), get_google_trends_data(), get_market_demand_report(), _iso_now(), main()

### Community 10 - "Community 10"
Cohesion: 1.0
Nodes (1): Prompt-driven market validation package.

### Community 11 - "Community 11"
Cohesion: 1.0
Nodes (0): 

## Knowledge Gaps
- **22 isolated node(s):** `Prompt-driven market validation package.`, `Dashboard HTML generator and optional local server.  Modes: - Static HTML: gener`, `Market Research Agent - Simple 3-step pipeline:  1. find()      - Discover compa`, `Run opencode and return JSON.`, `STEP 1: Find companies in a market.                  Searches web for businesses` (+17 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 10`** (2 nodes): `__init__.py`, `Prompt-driven market validation package.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 11`** (2 nodes): `environment.py`, `load_project_env()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What connects `Prompt-driven market validation package.`, `Dashboard HTML generator and optional local server.  Modes: - Static HTML: gener`, `Market Research Agent - Simple 3-step pipeline:  1. find()      - Discover compa` to the rest of the system?**
  _22 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.11 - nodes in this community are weakly interconnected._
- **Should `Community 1` be split into smaller, more focused modules?**
  _Cohesion score 0.12 - nodes in this community are weakly interconnected._
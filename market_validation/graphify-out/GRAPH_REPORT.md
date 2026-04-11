# Graph Report - market_validation  (2026-04-11)

## Corpus Check
- 15 files · ~18,155 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 215 nodes · 386 edges · 16 communities detected
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS
- Token cost: 0 input · 0 output

## God Nodes (most connected - your core abstractions)
1. `ResearchManager` - 23 edges
2. `Agent` - 16 edges
3. `quick_search()` - 10 edges
4. `_connect()` - 9 edges
5. `resolve_db_path()` - 9 edges
6. `_ensure_schema()` - 9 edges
7. `_infer_market_profile()` - 9 edges
8. `SearchResult` - 8 edges
9. `main()` - 7 edges
10. `main()` - 7 edges

## Surprising Connections (you probably didn't know these)
- `main()` --calls--> `Agent`  [EXTRACTED]
  market_validation/agent.py → market_validation/agent.py  _Bridges community 2 → community 0_

## Communities

### Community 0 - "Community 0"
Cohesion: 0.11
Nodes (33): _apply_contact_retry_rows(), _build_contact_retry_queries(), _build_retry_queries(), _clamp_score(), _contactability_score(), _dedupe_companies(), _extract_phone_text(), _filter_relevant_companies() (+25 more)

### Community 1 - "Community 1"
Cohesion: 0.12
Nodes (2): main(), ResearchManager

### Community 2 - "Community 2"
Cohesion: 0.15
Nodes (11): Agent, STEP 3: Enrich - Find contact info using 8 different sources.                  S, Update company record with enriched data., Source 1: Official website., Source 2: LinkedIn (via web search)., Source 3: Business directories., Source 4: News archives., Source 5: Review sites. (+3 more)

### Community 3 - "Community 3"
Cohesion: 0.15
Nodes (21): approve_all_emails(), approve_email(), build_parser(), delete_email(), export_email_queue_markdown(), get_email_queue(), _get_smtp_connection(), _iso_now() (+13 more)

### Community 4 - "Community 4"
Cohesion: 0.25
Nodes (16): _extract_location_hint(), _from_bbb(), _from_city_directory(), _from_ddgs(), _from_nominatim(), _from_opencorporates(), _from_wikipedia(), quick_search() (+8 more)

### Community 5 - "Community 5"
Cohesion: 0.15
Nodes (16): get_direct_urls(), get_directory_urls(), get_search_queries(), get_sources_dir(), list_available_sources(), load_source_config(), _load_yaml(), _normalize_market_key() (+8 more)

### Community 6 - "Community 6"
Cohesion: 0.42
Nodes (15): add_call_note(), add_company(), build_parser(), _connect(), create_research(), delete_company(), _ensure_schema(), export_markdown() (+7 more)

### Community 7 - "Community 7"
Cohesion: 0.35
Nodes (10): _escape_html(), generate_html(), _html_template(), _iso_now(), _load_data(), main(), _make_handler(), Dashboard HTML generator and optional local server.  Modes: - Static HTML: gener (+2 more)

### Community 8 - "Community 8"
Cohesion: 0.5
Nodes (8): build_parser(), export_call_notes_for_company(), export_markdown_call_sheet(), export_markdown_dashboard(), get_call_sheet_from_db(), get_dashboard_summary_from_db(), _iso_now(), main()

### Community 9 - "Community 9"
Cohesion: 0.32
Nodes (7): _extract_email(), _extract_phone(), is_playwright_available(), quick_scrape(), Lightweight free web scraper helpers.  No Playwright dependency required. Uses r, Optional check only; project works without Playwright., Best-effort scrape for a page and basic business details.     Returns a normaliz

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
- **40 isolated node(s):** `Lightweight free web scraper helpers.  No Playwright dependency required. Uses r`, `Optional check only; project works without Playwright.`, `Best-effort scrape for a page and basic business details.     Returns a normaliz`, `Free search helpers (no API keys required).  Backends (all free/no key): 1. Nomi`, `Best-effort parser; may return empty due to captcha/anti-bot.` (+35 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 14`** (2 nodes): `__init__.py`, `Prompt-driven market validation package.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 15`** (2 nodes): `environment.py`, `load_project_env()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Agent` connect `Community 2` to `Community 0`?**
  _High betweenness centrality (0.030) - this node is a cross-community bridge._
- **What connects `Lightweight free web scraper helpers.  No Playwright dependency required. Uses r`, `Optional check only; project works without Playwright.`, `Best-effort scrape for a page and basic business details.     Returns a normaliz` to the rest of the system?**
  _40 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.11 - nodes in this community are weakly interconnected._
- **Should `Community 1` be split into smaller, more focused modules?**
  _Cohesion score 0.12 - nodes in this community are weakly interconnected._
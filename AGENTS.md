# Market Validation System - Agents.md

## graphify

This project has a graphify knowledge graph at graphify-out/.

**Key files:**
- `graphify-out/graph.json` - Raw graph data (use `graphify query` to explore)
- `graphify-out/graph.html` - Interactive visualization (open in browser)
- `graphify-out/GRAPH_REPORT.md` - God nodes, communities, architecture

**Rules:**
- Before answering architecture/codebase questions, check the graph first
- Use `graphify query "<question>"` for BFS traversal of the graph
- After modifying code, rebuild: `python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('market_validation'))"`

---

## Market Research Agent

### Architecture

| Module | Class | Purpose |
|--------|-------|---------|
| `agent.py` | `Agent` | **Main interface** - find(), qualify(), enrich() |
| `research_manager.py` | `ResearchManager` | Database operations, call sheets |
| `research.py` | (functions) | Low-level SQLite CRUD + deduplication |
| `research_runner.py` | (functions) | Gather/qualify pipeline |
| `company_enrichment.py` | (functions) | Find contacts, emails |
| `dashboard.py` | (functions) | HTML dashboard + server with REST API |
| `dashboard_export.py` | (functions) | Reports, call sheets |
| `email_sender.py` | (functions) | Email queue (prep/approve/delete) |

### Usage (One-Call Pipeline)

```python
from market_validation.agent import Agent
from market_validation.research import create_research

# Create research and run full pipeline in one call
rid = create_research(
    name="My Market Research",
    market="<market_or_product>",
    product="<specific_product>",
    geography="<location>"
)["research_id"]

agent = Agent(research_id=rid)
result = agent.research("<market>", "<geography>", "<product>")
# Returns: {find_result, qualify_result, enrich_result, summary}

# Or use CLI:
# python -m market_validation.agent research --market "BBQ restaurants" --geography "San Jose, California" --product "smokers"
```

### Usage (Manual 3-Step)

```python
from market_validation.agent import Agent
from market_validation.research import create_research

# 1. Create research (generic - works for any market/product)
rid = create_research(
    name="My Market Research",
    market="<market_or_product>",
    product="<specific_product>",
    geography="<location>"
)["research_id"]

# 2. Create agent and run 3-step pipeline
agent = Agent(research_id=rid)

# Step 1: Find companies in a market
agent.find("<market>", "<geography>", "<product>")

# Step 2: Qualify (AI assessment + volume estimation)
agent.qualify()

# Step 3: Enrich specific companies (8 sources)
agent.enrich("<company_name>", "<geography>")
agent.enrich("<another_company>", "<geography>")

# 3. Manage with ResearchManager
from market_validation.research_manager import ResearchManager
manager = ResearchManager(research_id=rid)

# Get call sheet
sheet = manager.get_call_sheet(status="qualified")
print(f"{sheet['count']} companies ready")

# Add call notes
manager.add_call_note(
    company_id="<company_id>",
    note="<call notes>",
    author="<author>",
    next_action="<next action>"
)

# 4. Export call sheet
from market_validation.dashboard_export import export_markdown_call_sheet
print(export_markdown_call_sheet(status="qualified"))
```

### Agent Methods

| Method | Purpose |
|--------|---------|
| `research(market, geography, product?)` | **Full pipeline** - find → qualify → enrich_all (one call) |
| `find(market, geography, product?)` | Discover companies via web search |
| `qualify()` | AI assessment + volume estimation |
| `enrich(company_name, location?)` | Find contacts via 8 sources |
| `enrich_all(statuses?)` | Enrich all companies matching status (default: qualified/new) |

### Dashboard

```bash
# Start dashboard server (default port 8787)
python3 -m market_validation.dashboard

# Static HTML mode (no server)
python3 -m market_validation.dashboard --static

# Custom port
python3 -m market_validation.dashboard --port 9000
```

**Features:**
- Project selector with URL persistence (`?research_id=...`)
- Inline company row editing (click "Edit Row" → Save/Cancel/Delete)
- Email queue management (Approve/Edit/Delete)
- KPI dashboard (research count, companies, pending/sent emails)

### Duplicate Prevention

- **Companies**: Normalized name matching (case/space insensitive)
- **Contacts**: Removed - user emails company, not specific people

### Database

- `output/market-research.sqlite3` - Companies, call notes (contacts table removed)

### Search Backends (`multi_search.py`)

| Backend | Coverage | Notes |
|---------|----------|-------|
| Nominatim (OpenStreetMap) | Physical businesses worldwide | Best for local businesses with addresses |
| DuckDuckGo (DDGS) | General web | Best general-purpose fallback |
| Wikipedia | Reference only | Filtered out for local business queries |
| BBB | US businesses | Good for verifying established companies |
| OpenCorporates | Registered companies | Often blocked by captcha |
| Manta | US business directory | Good geographic coverage for all US regions |

### Qualify: Market Potential Signals

The `qualify()` step explicitly prompts AI to detect:
- **Growth indicators**: expansion, hiring, new locations, funding
- **Pain points**: specific problems that make a company a good prospect
- **Buying signals**: active spending in the category
- **Urgency signals**: seasonal demand, recent news

These are stored in company notes as: `Signals: ... | Pain points: ...`

### Verified Working (2024-04-10)

- ✅ One-call pipeline (research() - find → qualify → enrich_all)
- ✅ Simple 3-step pipeline (find/qualify/enrich)
- ✅ 8-source contact enrichment
- ✅ Duplicate prevention (companies)
- ✅ Gather/qualify pipeline
- ✅ Call sheet generation
- ✅ Add call notes
- ✅ Outreach message generation
- ✅ Dashboard server with inline editing
- ✅ REST API for CRUD operations

### Pipeline checkpoints + resume

Each successful stage of `Agent.research()` writes `researches.last_completed_stage`. A failed run can be resumed:

```python
agent.research(market, geo, product, resume=True)              # auto-resume after last completed
agent.research(market, geo, product, from_stage='enrich')      # explicit
```

Stages: `validate` → `find` → `qualify` → `enrich` → `drafts`.

CLI: `--resume` and `--research-from-stage <stage>`.

### Outcome feedback loop

After 3-12 months, record what actually happened so verdict accuracy can be measured:

```python
from market_validation.research import record_validation_outcome, get_calibration_summary
record_validation_outcome(validation_id, 'success', revenue_actual=120000)
get_calibration_summary()
# → {"by_verdict": {"strong_go": {"hit_rate": 0.85, ...}, ...}}
```

Valid outcomes: `success` | `partial` | `failure` | `abandoned` | `pending`.

CLI: `market-research record-outcome <vid> --outcome success`, `market-research calibration`.

### Citation enforcement

Every validation module post-validates AI output via `enforce_citations()`:

- Drops source entries with no `source_url`.
- Caps `*_confidence` by the strongest cited source's tier (1=gov, 6=AI inference).
- Appends `_citation_warnings` to the result.
- Scorecard downgrades verdict one tier when `completeness_score < 50`.

Source-tier mapping in `market_validation/_helpers/citations.py`. Per-module rules in the same file: `RULES_FOR_SIZING`, `RULES_FOR_COMPETITION`, `RULES_FOR_UNIT_ECONOMICS`, etc.

### CRM-mapped CSV exports

```bash
market-dashboard crm-export --crm hubspot --output leads.csv
market-dashboard crm-export --crm salesforce --research-id <rid>
market-dashboard crm-export --crm pipedrive
```

Field maps live in `dashboard_export._CRM_FIELD_MAPS`. Add a new CRM by extending that dict.

### Dashboard auth + rate limit

When binding to non-loopback (`--host 0.0.0.0`), set `MV_DASHBOARD_API_KEY` in `.env`. POSTs require:

- `X-API-Key: <your-key>` header
- `X-Requested-With: MarketValidationDashboard` header (CSRF defense)

After 10 failed auth attempts in 5 min from one IP, the IP is blocked for 15 min.

### Source-tier overrides per cache

```bash
MV_TREND_TTL_BLS_INDUSTRY_DATA=86400      # 24h for BLS
MV_TREND_FORCE_REFRESH=1                   # bypass cache for one run
```

### Optional dependencies

```bash
pip install -e '.[gmail]'    # Gmail reply/bounce tracking
pip install -e '.[intl]'     # Accurate international phone normalization (phonenumbers)
pip install -e '.[dev]'      # pytest + ruff + mypy
```

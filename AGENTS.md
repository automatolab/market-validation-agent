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

### Usage (Simple 3-Step Pipeline)

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
| `find(market, geography, product?)` | Discover companies via web search |
| `qualify()` | AI assessment + volume estimation |
| `enrich(company_name, location?)` | Find contacts via 8 sources |

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

### Verified Working (2024-04-10)

- ✅ Simple 3-step pipeline (find/qualify/enrich)
- ✅ 8-source contact enrichment
- ✅ Duplicate prevention (companies)
- ✅ Gather/qualify pipeline
- ✅ Call sheet generation
- ✅ Add call notes
- ✅ Outreach message generation
- ✅ Dashboard server with inline editing
- ✅ REST API for CRUD operations

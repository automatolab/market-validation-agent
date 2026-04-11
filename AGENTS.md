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
| `agent.py` | `Agent` | Deep research - web search, adaptive strategies |
| `research_manager.py` | `ResearchManager` | Database operations, call sheets |
| `research.py` | (functions) | Low-level SQLite CRUD |
| `research_runner.py` | (functions) | Gather/qualify pipeline |
| `company_enrichment.py` | (functions) | Find contacts, emails |
| `dashboard_export.py` | (functions) | Reports, call sheets |

### Usage

```python
from market_validation.agent import Agent
from market_validation.research_manager import ResearchManager
from market_validation.research import create_research

# 1. Create research
from market_validation.research_runner import gather_companies, qualify_companies
rid = create_research("Brisket BBQ", "brisket", "beef brisket", "San Jose, CA")["research_id"]

# 2. Gather companies
gather_companies(rid, "brisket", "beef brisket", "San Jose, CA")

# 3. Qualify companies
qualify_companies(rid, "brisket", "beef brisket")

# 4. Deep research with Agent
agent = Agent()

# Market intelligence
intel = agent.research_market_intelligence(market="wholesale brisket", geography="San Jose, CA")
print(intel["intelligence"]["key_trends"])

# Find companies adaptively
companies = agent.adaptive_research(
    goal="Find BBQ restaurants in San Jose that might buy brisket"
)
print(companies["initial_findings"])

# Deep research on specific company
deep = agent.research_company_deep(
    company_name="Smoking Pig BBQ",
    location="San Jose, CA",
    focus_areas=["contacts", "decision_makers", "social", "news"]
)
print(deep["data"]["contacts"])

# 5. Manage with ResearchManager
manager = ResearchManager(research_id=rid)

# Get call sheet
sheet = manager.get_call_sheet(status="qualified")
print(f"{sheet['count']} companies ready")

# Add call notes
manager.add_call_note(
    company_id="abc123",
    note="Interested in bulk pricing",
    author="Sales",
    next_action="Send quote"
)

# Enrich contacts
manager.enrich_contact_info("Restaurant Name", website="https://...")

# Generate outreach
outreach = manager.generate_outreach_message(
    company_name="Smoke House",
    contact_name="John",
    product="beef brisket",
    volume_estimate="200 lbs/week"
)
print(outreach["email_body"])

# 6. Export call sheet
from market_validation.dashboard_export import export_markdown_call_sheet
print(export_markdown_call_sheet(status="qualified"))
```

### Database

- `output/market-research.sqlite3` - Companies, contacts, call notes

### Verified Working (2024-04-10)

- ✅ Market intelligence research
- ✅ Adaptive company discovery
- ✅ Deep company research (contacts, social, news)
- ✅ Gather/qualify pipeline
- ✅ Call sheet generation
- ✅ Add call notes
- ✅ Contact enrichment
- ✅ Outreach message generation

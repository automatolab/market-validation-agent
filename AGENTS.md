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

## Market Validation System

### Main Modules

| Module | Class | Purpose |
|--------|-------|---------|
| `agent.py` | `Agent` | Deep research - web search, adaptive strategies |
| `research_manager.py` | `ResearchManager` | Database operations, call sheets |
| `research.py` | (functions) | Low-level SQLite CRUD |
| `research_runner.py` | (functions) | Gather/qualify pipeline |
| `company_enrichment.py` | (functions) | Find contacts, emails |
| `dashboard_export.py` | (functions) | Reports, call sheets |

### Quick Reference

```python
# Deep research (web searches)
from market_validation.agent import Agent
agent = Agent()
agent.adaptive_research(goal="...")
agent.research_company_deep(company_name="...")

# Database operations
from market_validation.research_manager import ResearchManager
manager = ResearchManager(research_id="abc")
manager.suggest_next_actions()
manager.get_call_sheet()
```

### Database
- `output/market-research.sqlite3` - Companies, contacts, call notes

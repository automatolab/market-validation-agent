# Market Validation Agent - Claude Code Skills

Specialized instructions for Claude Code when working with the Market Validation Agent project.

## Quick Reference

**Database:** `output/market-research.sqlite3`

**Main Interface:**
```python
from market_validation.agent import Agent
from market_validation.research import create_research

# Option 1: One-call pipeline (find → qualify → enrich_all)
rid = create_research(name='...', market='...', product='...', geography='...')['research_id']
agent = Agent(research_id=rid)
result = agent.research('<market>', '<geography>', '<product>')  # Full pipeline

# Option 2: Manual 3-step
agent = Agent(research_id=rid)
agent.find('<market>', '<geography>', '<product>')  # Discover companies
agent.qualify()                                      # Score and rank
agent.enrich('<company_name>', '<geography>')        # Find contacts
```

**Key Modules:**
- `agent.py` - Main interface (find/qualify/enrich)
- `research.py` - Database operations
- `research_manager.py` - High-level database operations
- `dashboard.py` - HTML dashboard + REST API server
- `email_sender.py` - Email queue (prep/approve/delete)

## Database Schema

```sql
-- Research projects
CREATE TABLE researches (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    market TEXT NOT NULL,
    product TEXT,
    geography TEXT,
    status TEXT DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Discovered companies
CREATE TABLE companies (
    id TEXT PRIMARY KEY,
    research_id TEXT NOT NULL,
    company_name TEXT NOT NULL,
    company_name_normalized TEXT,
    website TEXT,
    location TEXT,
    phone TEXT,
    email TEXT,
    status TEXT DEFAULT 'new',
    priority_score INTEGER,
    priority_tier TEXT,
    volume_estimate REAL,
    volume_unit TEXT,
    notes TEXT,
    menu_items TEXT,
    prices TEXT,
    hours TEXT,
    ratings TEXT,
    reviews_count INTEGER,
    raw_data TEXT,
    created_at TEXT,
    updated_at TEXT
);

-- Call notes
CREATE TABLE call_notes (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    research_id TEXT NOT NULL,
    author TEXT,
    note TEXT,
    meeting_at TEXT,
    next_action TEXT,
    created_at TEXT
);
```

## Common Tasks

### Export Call Sheet
```python
from market_validation.dashboard_export import export_markdown_call_sheet
print(export_markdown_call_sheet(status='qualified'))
```

### Add Call Notes
```python
from market_validation.research_manager import ResearchManager
manager = ResearchManager(research_id='<id>')
manager.add_call_note(company_id='<id>', author='<name>', note='<notes>', next_action='<action>')
```

### Start Dashboard
```bash
python3 -m market_validation.dashboard
```

### CLI Usage
```bash
# One-call pipeline (recommended default)
python -m market_validation.agent research --market "BBQ restaurants" --geography "San Jose, California" --product "smokers"

# Manual steps
python -m market_validation.agent find --market "BBQ restaurants" --geography "San Jose"
python -m market_validation.agent qualify --research-id <id>
python -m market_validation.agent enrich-all --research-id <id>
```

## Tips

1. Use `Agent.research()` for the full one-call pipeline (default)
2. Use manual 3-step if you need more control
3. Companies are deduplicated by normalized name
4. Email goes to company, not individual contacts
5. Dashboard server mode supports inline row editing

## File Locations

| File | Purpose |
|------|---------|
| `market_validation/agent.py` | Main interface |
| `market_validation/research.py` | Database layer |
| `market_validation/dashboard.py` | Dashboard + API |
| `output/market-research.sqlite3` | SQLite database |
| `output/call-sheets/` | Exported call sheets |

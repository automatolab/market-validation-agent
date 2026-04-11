# Market Validation Agent - Claude Code Skills

This file provides specialized instructions for Claude Code when working with the Market Validation Agent project.

## Project Overview

The Market Validation Agent is a tool for discovering companies in a market, qualifying leads with volume estimates, and tracking outreach - **no API keys required** for core functionality.

## Quick Reference

**Database:** `output/market-research.sqlite3`

**Key Modules:**
- `market_validation/agent.py` - **Main interface** (find/qualify/enrich)
- `market_validation/research_manager.py` - Database operations (CRUD)
- `market_validation/research.py` - Database layer (low-level CRUD + deduplication)
- `market_validation/research_runner.py` - Pipeline (gather, qualify)
- `market_validation/company_enrichment.py` - Find emails, contacts, phones
- `market_validation/dashboard_export.py` - Reports and call sheets

## Common Tasks

### Simple 3-Step Pipeline (Recommended)

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

# 2. Run 3-step pipeline
agent = Agent(research_id=rid)
agent.find("<market>", "<geography>", "<product>")  # Step 1: Find companies
agent.qualify()                                      # Step 2: Score/rank
agent.enrich("<company_name>", "<geography>")        # Step 3: Find contacts
```

### Gather Companies for Existing Research

```python
from market_validation.research_runner import gather_companies

result = gather_companies(
    research_id="<research_id>",
    market="<market>",
    product="<product>",
    geography="<location>"
)
print(f"Added {result['companies_added']} companies")
```

### View Results

```python
from market_validation.research import get_research, search_companies

# Get research with stats
research = get_research("<research_id>")
print(f"Status: {research['stats']}")

# Search companies
companies = search_companies(research_id="<research_id>", status="qualified", limit=20)
for c in companies["companies"]:
    print(f"{c['company_name']}: {c['priority_score']} score, email={c['email']}")
```

### Generate Call Sheet

```python
from market_validation.dashboard_export import export_markdown_call_sheet

md = export_markdown_call_sheet(status="qualified", limit=50)
print(md)
```

### Add Call Notes

```python
from market_validation.research_manager import ResearchManager

manager = ResearchManager(research_id="<research_id>")
manager.add_call_note(
    company_id="<company_id>",
    author="<author>",
    note="<call notes>",
    next_action="<next action>"
)
```

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
    company_name_normalized TEXT,  -- For deduplication
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
    menu_items TEXT,   -- JSON
    prices TEXT,       -- JSON
    hours TEXT,
    ratings TEXT,
    reviews_count INTEGER,
    raw_data TEXT,     -- JSON
    created_at TEXT,
    updated_at TEXT
);

-- Contacts at companies (deduplicated by name_normalized)
CREATE TABLE contacts (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    research_id TEXT NOT NULL,
    name TEXT,
    name_normalized TEXT,  -- For deduplication
    title TEXT,
    email TEXT,
    phone TEXT,
    source TEXT,
    created_at TEXT
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

**Duplicate Prevention:** Companies and contacts use normalized name matching (lowercase, trimmed) to prevent duplicates.

## Company Status Flow

```
new → qualified → emailed → replied_interested
                    ↓
              replied_not_now
                    ↓
              do_not_contact
```

## Environment Variables

**For Email (optional):**
- `SMTP_HOST` - SMTP server (default: smtp.gmail.com)
- `SMTP_PORT` - SMTP port (default: 587)
- `SMTP_USER` - SMTP username
- `SMTP_PASSWORD` - SMTP password/app password
- `FROM_EMAIL` - Sender email address

**For Gmail API (optional):**
- `credentials.json` - Google Cloud Console OAuth credentials

## Tips for Claude Code

1. **Use the Agent class** for the 3-step pipeline - `agent.py` with find(), qualify(), enrich()
2. **Use the ResearchManager** for database operations - `research_manager.py`
3. **Database is at `output/market-research.sqlite3`** - relative to project root
4. **Duplicate prevention is automatic** - companies and contacts are deduplicated
5. **Use qualified status filter** for call sheets - only shows relevant companies

## Testing New Changes

```bash
# Test database operations
cd market_validation
python3 -c "
from research import create_research, add_company, search_companies
r = create_research('Test', 'test', 'test', 'Test City')
c = add_company(r['research_id'], 'Test Co', 'test', phone='555-1234')
companies = search_companies(research_id=r['research_id'])
print(f'OK: {companies[\"count\"]} companies')
"

# Test dashboard export
python3 -c "
from dashboard_export import get_call_sheet_from_db
sheet = get_call_sheet_from_db()
print(f'Call sheet: {sheet[\"count\"]} companies')
"
```

## File Locations

| File | Purpose |
|------|---------|
| `market_validation/agent.py` | Deep research agent |
| `market_validation/research_manager.py` | Database manager |
| `market_validation/research.py` | Database layer |
| `market_validation/research_runner.py` | Pipeline runner |
| `market_validation/dashboard_export.py` | Reports and exports |
| `output/market-research.sqlite3` | SQLite database |
| `output/call-sheets/` | Exported call sheets |
| `output/dashboard/` | Dashboard exports |

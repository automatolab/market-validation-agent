# Market Validation Agent - Claude Code Skills

This file provides specialized instructions for Claude Code when working with the Market Validation Agent project.

## Project Overview

The Market Validation Agent is a tool for discovering companies in a market, qualifying leads with volume estimates, and tracking outreach - **no API keys required** for core functionality.

## Quick Reference

**Database:** `output/market-research.sqlite3`

**Key Modules:**
- `market_validation/agent.py` - **DEEP RESEARCH AGENT** (main agent for research)
- `market_validation/research_manager.py` - Database operations (CRUD)
- `market_validation/research.py` - Database layer (low-level CRUD)
- `market_validation/research_runner.py` - Pipeline (gather, qualify)
- `market_validation/company_enrichment.py` - Find emails, contacts, phones
- `market_validation/dashboard_export.py` - Reports and call sheets
- `market_validation/email_sender.py` - Email sending (SMTP)
- `market_validation/gmail_inbox.py` - Reply tracking (Gmail API)
- `market_validation/source_discovery.py` - Auto-discover sources
- `market_validation/market_trends.py` - Google Trends integration

## Common Tasks

### Create and Run a Research Project

```python
from market_validation.research_runner import gather_companies, qualify_companies
from market_validation.research import get_research, create_research

# Run full pipeline
result = gather_companies(research_id, market, product, geography)
qualify_companies(research_id, market, product)
```

### Gather Companies for Existing Research

```python
from market_validation.research_runner import gather_companies

result = gather_companies(
    research_id="abc12345",
    market="brisket",
    product="beef brisket",
    geography="San Jose, CA"
)
print(f"Added {result['companies_added']} companies")
```

### Qualify Companies

```python
from market_validation.research_runner import qualify_companies

result = qualify_companies(
    research_id="abc12345",
    market="brisket",
    product="beef brisket"
)
print(f"Qualified {result['qualified']} companies")
```

### View Results

```python
from market_validation.research import get_research, search_companies

# Get research with stats
research = get_research("abc12345")
print(f"Status: {research['stats']}")

# Search companies
companies = search_companies(research_id="abc12345", status="qualified", limit=20)
for c in companies["companies"]:
    print(f"{c['company_name']}: {c['priority_score']} score")
```

### Generate Call Sheet

```python
from market_validation.dashboard_export import get_call_sheet_from_db, export_markdown_call_sheet

# Get call sheet data
sheet = get_call_sheet_from_db(status_filter="qualified", limit=50)

# Export as markdown
md = export_markdown_call_sheet(status_filter="qualified", limit=50)
```

### Add Call Notes

```python
from market_validation.research import add_call_note

note = add_call_note(
    company_id="abc12345",
    research_id="xyz67890",
    author="Sales",
    note="Interested in bulk brisket, wants pricing sheet",
    next_action="Send pricing sheet Friday"
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
    status TEXT DEFAULT 'active'
);

-- Discovered companies
CREATE TABLE companies (
    id TEXT PRIMARY KEY,
    research_id TEXT NOT NULL,
    company_name TEXT NOT NULL,
    website TEXT,
    location TEXT,
    phone TEXT,
    status TEXT DEFAULT 'new',
    priority_score INTEGER,
    priority_tier TEXT,
    volume_estimate REAL,
    volume_unit TEXT,
    volume_basis TEXT,
    notes TEXT,
    menu_items TEXT,  -- JSON
    ratings TEXT,     -- JSON
    raw_data TEXT     -- JSON
);

-- Contacts at companies
CREATE TABLE contacts (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    research_id TEXT,
    name TEXT,
    title TEXT,
    email TEXT,
    phone TEXT
);

-- Call notes
CREATE TABLE call_notes (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    research_id TEXT,
    author TEXT,
    note TEXT,
    next_action TEXT
);
```

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

1. **Use the Agent class** for deep research - `agent.py`
2. **Use the ResearchManager** for database operations - `research_manager.py`
3. **Database is at `output/market-research.sqlite3`** - relative to project root
4. **Run gather before qualify** - companies must have status='new' to be qualified
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

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
from market_validation.research_runner import run_market_research, gather_companies, qualify_companies
from market_validation.research import get_research, search_companies

# Run full pipeline
result = run_market_research(
    name="San Jose BBQ",
    market="brisket",
    product="beef brisket",
    geography="San Jose, CA"
)
research_id = result["research_id"]
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

### Enrich with Contact Information

```python
from market_validation.company_enrichment import enrich_research_companies, enrich_company_contact

# Enrich all qualified companies in a research
result = enrich_research_companies(research_id="abc12345")
print(f"Found {result['emails_found']} emails, {result['contacts_added']} contacts")

# Or enrich a single company
contact_info = enrich_company_contact(
    company_name="Smoke House BBQ",
    website="https://smokehouse.com",
    location="San Jose, CA"
)
print(f"Emails: {contact_info['emails_found']}")
print(f"Contacts: {contact_info['contacts']}")
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

### Send Email to Company

```python
from market_validation.email_sender import send_templated_email

result = send_templated_email(
    to_email="contact@company.com",
    template={
        "subject_template": "Bulk Brisket Partnership - {{company_name}}",
        "body_template": "Hi {{contact_name}}, we supply brisket to restaurants like {{company_name}}..."
    },
    company_name="Smoke House BBQ",
    contact_name="John"
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

1. **Use the ResearchAgent class** for dynamic, AI-driven research workflows
2. **Let the agent suggest next actions** - it analyzes current state and recommends what to do
3. **Call `agent.suggest_next_actions()`** to get AI recommendations for the research
4. **The agent can decide to**: enrich contacts, generate outreach, make calls, research topics, etc.
5. **Database is at `output/market-research.sqlite3`** - relative to project root

## Dynamic Research Workflow

```python
from market_validation.agent import Agent

# Create the deep research agent
agent = Agent()

# ADAPTIVE RESEARCH - agent decides what to search based on goal
result = agent.adaptive_research(
    goal="Find BBQ restaurants in San Jose that serve brisket and might buy wholesale brisket",
    market="BBQ restaurants",
    geography="San Jose, CA"
)
print(result["initial_findings"])
print(result["recommended_next_steps"])

# MARKET INTELLIGENCE - understand the market
intel = agent.research_market_intelligence(
    market="wholesale brisket",
    geography="San Jose, CA"
)
print(intel["intelligence"]["key_trends"])
print(intel["intelligence"]["opportunities"])
print(intel["intelligence"]["recommended_search_queries"])

# DEEP RESEARCH on a single company
deep = agent.research_company_deep(
    company_name="Jackie's Place",
    location="San Jose, CA",
    focus_areas=["contacts", "decision_makers", "social", "news"]
)
print(deep["data"]["contacts"])
print(deep["data"]["news"])
print(deep["data"]["social_media"])
```

## Agent Capabilities

The `Agent` class is the **MAIN AGENT** for deep research:

1. **adaptive_research()** - Give it a GOAL, it figures out the strategy
2. **research_market_intelligence()** - Market size, trends, opportunities
3. **research_company_deep()** - Multi-phase deep dive on ONE company
4. **analyze_gaps()** - What info is missing and how to find it
5. **batch_research()** - Research multiple companies

### Why Agent?

Unlike static pipelines, this agent:
- **Adapts** search strategies based on what it finds
- **Digs deeper** when initial searches are insufficient  
- **Tries alternatives** when one approach fails
- **Thinks strategically** about what information is most valuable
- **Works for ANY market** - not just BBQ/restaurants

### Research Philosophy

The agent follows this research approach:

1. **Surface Search** - Get basic info (website, phone, address)
2. **Decision Maker Hunt** - Find owners, managers, purchasing
3. **Verification** - Cross-reference multiple sources
4. **Gap Analysis** - What info is still missing?
5. **Deep Dive** - If important gaps exist, dig deeper
6. **Alternative Strategies** - If stuck, try different angles
7. **Synthesis** - Combine findings into actionable insights

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
| `market_validation/research.py` | Core database module |
| `market_validation/research_runner.py` | Pipeline runner |
| `market_validation/dashboard_export.py` | Reports and exports |
| `output/market-research.sqlite3` | SQLite database |
| `output/call-sheets/` | Exported call sheets |
| `output/dashboard/` | Dashboard exports |

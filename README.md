# Market Validation Agent

General-purpose market research platform that discovers companies, qualifies leads, and tracks outreach - **no API keys required** for data gathering.

## What This System Does

For a given market/product/geography, the system:

1. **Discovers Companies** - Web search for businesses using free sources (Yelp, Google, YellowPages)
2. **Qualifies Leads** - AI assessment of relevance + volume estimation (e.g., "300+ lbs/week brisket")
3. **Enriches Contacts** - Finds emails, phone numbers, and key personnel from websites
4. **Tracks Companies** - Stores contacts, notes, status, and priority scores
5. **Generates Call Sheets** - Prioritized list with phones, emails, and contacts
6. **Handles Email Outreach** - Send templated emails, track replies (optional: requires SMTP + Gmail API)

## Quick Start

```bash
# 1. Install
pip install -e .

# 2. Create a research project
python -m market_validation.research create \
  --name "San Jose BBQ" \
  --market "brisket" \
  --product "beef brisket" \
  --geography "San Jose, CA"

# 3. Run the full pipeline
python -m market_validation.research_runner run \
  --name "San Jose BBQ" \
  --market "brisket" \
  --product "beef brisket" \
  --geography "San Jose, CA"
```

## Architecture

```
Research Project
├── researches (UUID-based research sessions)
├── companies (discovered businesses with qualification data)
├── contacts (people at each company)
└── call_notes (human call notes)
```

## Key Modules

### research.py - Database Operations
```python
from market_validation.research import (
    create_research,
    get_research,
    list_researches,
    add_company,
    update_company,
    add_contact,
    add_call_note,
    search_companies,
    export_markdown,
)
```

### research_runner.py - Full Pipeline
```python
from market_validation.research_runner import (
    run_market_research,   # Create + discover sources + trends
    gather_companies,       # Web search for businesses
    qualify_companies,       # AI assessment + volume estimation
)
```

### dashboard_export.py - Reports
```python
from market_validation.dashboard_export import (
    get_dashboard_summary_from_db,
    get_call_sheet_from_db,
    export_markdown_call_sheet,
    export_markdown_dashboard,
)
```

### company_enrichment.py - Contact Enrichment
```python
from market_validation.company_enrichment import (
    enrich_company_contact,      # Enrich single company
    enrich_research_companies,   # Enrich all in research
)
```

### research_manager.py - Database Operations
```python
from market_validation.research_manager import ResearchManager

# Manages research data in the database
manager = ResearchManager(research_id="abc12345")
manager.suggest_next_actions()      # AI suggests what to do next
manager.get_call_sheet()           # Get prioritized list
manager.add_call_note(...)         # Save call notes
manager.export_markdown()          # Export to markdown
```

### agent.py - Deep Research Agent (MAIN)
```python
from market_validation.agent import Agent

# Does the actual research work
agent = Agent()

# Adaptive research - agent figures out what to search
result = agent.adaptive_research(
    goal="Find BBQ restaurants in San Jose that might buy brisket",
    market="BBQ restaurants",
    geography="San Jose, CA"
)

# Market intelligence - understand the market
intel = agent.research_market_intelligence(
    market="brisket",
    geography="San Jose, CA"
)

# Deep research on a single company
deep = agent.research_company_deep(
    company_name="Smoke House BBQ",
    location="San Jose, CA",
    focus_areas=["contacts", "decision_makers", "pricing"]
)
```

**The Agent is the main research engine.** It:
- Adapts search strategies based on findings
- Digs deeper when info is insufficient
- Tries multiple approaches
- Works for ANY market/product/geography

## Complete Workflow

```python
from market_validation.research import create_research
from market_validation.research_runner import gather_companies, qualify_companies
from market_validation.company_enrichment import enrich_research_companies
from market_validation.dashboard_export import export_markdown_call_sheet

# 1. Create research
r = create_research("Brisket SJ", "brisket", "beef brisket", "San Jose, CA")
rid = r["research_id"]

# 2. Gather companies (web search)
gather_companies(rid, "brisket", "beef brisket", "San Jose, CA")

# 3. Qualify leads (AI assessment + volume)
qualify_companies(rid, "brisket", "beef brisket")

# 4. Enrich with contact info (emails, phones, contacts)
enrich_research_companies(rid)

# 5. Generate call sheet
call_sheet = export_markdown_call_sheet(status_filter="qualified")
print(call_sheet)
```

### email_sender.py - Email Outreach
```python
from market_validation.email_sender import (
    send_email,
    send_templated_email,
    send_batch_emails,
)
# Requires: SMTP_USER, SMTP_PASSWORD, FROM_EMAIL env vars
```

### gmail_inbox.py - Reply Tracking
```python
from market_validation.gmail_inbox import (
    fetch_email_replies,
    fetch_and_build_replies,
)
# Requires: credentials.json from Google Cloud Console
```

## Database

**Location:** `output/market-research.sqlite3`

**Schema:**
```sql
researches: id, name, market, product, geography, status
companies: id, research_id, company_name, website, location, phone,
           status, priority_score, priority_tier, volume_estimate,
           volume_unit, notes, menu_items, ratings, reviews_count, raw_data
contacts: id, company_id, name, title, email, phone, source
call_notes: id, company_id, author, note, meeting_at, next_action
```

## CLI Commands

```bash
# Research management
python -m market_validation.research list
python -m market_validation.research get <id>
python -m market_validation.research export <id> --output report.md

# Pipeline
python -m market_validation.research_runner gather <id> --market X --product Y --geography Z
python -m market_validation.research_runner qualify <id> --market X --product Y

# Dashboard
python -m market_validation.dashboard_export call-sheet --status qualified
python -m market_validation.dashboard_export summary
```

## Example Workflow: Brisket Supply

```python
from market_validation.research import create_research, search_companies
from market_validation.research_runner import gather_companies, qualify_companies

# 1. Create research
r = create_research("Brisket SJ", "brisket", "beef brisket", "San Jose, CA")
research_id = r["research_id"]

# 2. Gather companies (web search)
gather = gather_companies(research_id, "brisket", "beef brisket", "San Jose, CA")
print(f"Found {gather['companies_added']} companies")

# 3. Qualify (AI assessment + volume)
qualify = qualify_companies(research_id, "brisket", "beef brisket")
print(f"Qualified {qualify['qualified']} companies")

# 4. View results
companies = search_companies(research_id=research_id, status="qualified", limit=10)
for c in companies["companies"]:
    print(f"{c['company_name']}: {c['priority_score']} score, {c['volume_estimate']} {c['volume_unit']}")
```

## Data Sources (No API Keys Required)

| Source | Purpose |
|--------|---------|
| DuckDuckGo | Web search |
| Bing | Web search |
| Yelp | Restaurant/business discovery |
| YellowPages | Business directories |
| OpenStreetMap | Location data |
| Google Trends | Market demand data |

## Optional: Email Integration

For automated email sending and reply tracking:

**SMTP (send only):**
```bash
export SMTP_HOST=smtp.gmail.com
export SMTP_PORT=587
export SMTP_USER=your-email@gmail.com
export SMTP_PASSWORD=app-password
export FROM_EMAIL=your-email@gmail.com
```

**Gmail API (read replies):**
1. Create project at https://console.cloud.google.com
2. Enable Gmail API
3. Download credentials.json to project root
4. First run will prompt for authorization

## Free vs Paid Features

| Feature | Free | Requires |
|---------|------|----------|
| Company discovery | ✅ | Nothing |
| Lead qualification | ✅ | Nothing |
| Volume estimation | ✅ | Nothing |
| Call sheets | ✅ | Nothing |
| Email sending | ✅ | SMTP credentials |
| Reply tracking | ✅ | Gmail API credentials |
| Market trends | ✅ | pytrends (free) |

## Market Types

The system auto-detects market types and adjusts discovery:

| Type | Keywords | Best Sources |
|------|----------|--------------|
| Restaurant | restaurant, BBQ, cafe, catering | Yelp, TripAdvisor, YellowPages |
| Retail | store, shop, outlet | Bing, Yelp, YellowPages |
| Tech | software, SaaS, platform | LinkedIn, News, Crunchbase |
| Healthcare | hospital, clinic, medical | Healthgrades, News |
| Default | anything else | DuckDuckGo, Bing, OSM |

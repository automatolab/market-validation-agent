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
  --name "My Market Research" \
  --market "<market_or_product>" \
  --product "<specific_product>" \
  --geography "<location>"

# 3. Run the full pipeline
python -m market_validation.research_runner run \
  --name "My Market Research" \
  --market "<market_or_product>" \
  --product "<specific_product>" \
  --geography "<location>"
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

### agent.py - Main Interface (Recommended)
```python
from market_validation.agent import Agent

agent = Agent(research_id="xxx")
agent.find(market, geography, product?)   # Step 1: Discover companies
agent.qualify()                           # Step 2: Score/rank
agent.enrich(company_name, location?)     # Step 3: Find contacts
```

### research.py - Database Operations
```python
from market_validation.research import (
    create_research,
    get_research,
    list_researches,
    add_company,      # With duplicate prevention
    update_company,
    add_contact,     # With duplicate prevention
    add_call_note,
    search_companies,
    export_markdown,
)
```

### research_runner.py - Pipeline Functions
```python
from market_validation.research_runner import (
    gather_companies,    # Web search for businesses
    qualify_companies,   # AI assessment + volume estimation
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

### email_sender.py - Email Outreach
```python
from market_validation.email_sender import (
    send_email,           # Send single email
    send_batch_emails,    # Batch send with template
)
# Requires: SMTP credentials in .env
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

### agent.py - Market Research Agent (MAIN)
```python
from market_validation.agent import Agent

# Create agent (optionally with research_id)
agent = Agent(research_id="<research_id>")

# STEP 1: Find companies in a market
result = agent.find(
    market="<market_or_product>",
    geography="<location>",
    product="<specific_product>"  # optional
)
print(f"Found: {len(result.get('companies', []))} companies")

# STEP 2: Qualify - AI assessment of relevance + volume
result = agent.qualify()
print(f"Qualified: {result['qualified']} companies")

# STEP 3: Enrich - Find contacts via 8 sources
result = agent.enrich(
    company_name="<company_name>",
    location="<location>"
)
print(f"Sources: {result['sources_tried']}")
print(f"Email: {result['findings'].get('email')}")
print(f"Owner: {result['findings'].get('owners')}")
```

**The Agent has 3 simple methods:**

1. **find(market, geography, product?)** - Discovers companies via web search
2. **qualify()** - AI assessment + volume estimation (updates database)
3. **enrich(company_name, location?)** - Finds contacts via 8 sources:
   - Official website + contact/about pages
   - LinkedIn (indirect via web search)
   - Business directories (Yelp, Google, BBB)
   - News archives
   - Review sites (sentiment + volume hints)
   - Social media (Instagram, Facebook)
   - State business registry
   - Supplier pages

**Duplicate Prevention:**
- Companies: Normalized name matching (case/space insensitive)
- Contacts: Normalized name matching per company

## Complete Workflow

### Option 1: Simple 3-Step (Recommended)
```python
from market_validation.agent import Agent
from market_validation.research import create_research

# 1. Create research
r = create_research("My Research", "<market>", "<product>", "<geography>")
research_id = r["research_id"]

# 2. Create agent and run 3-step pipeline
agent = Agent(research_id=research_id)
agent.find("<market>", "<geography>", "<product>")  # Step 1
agent.qualify()                                      # Step 2

# 3. Enrich specific companies
agent.enrich("<company_name>", "<geography>")
agent.enrich("<another_company>", "<geography>")

# 4. Export call sheet
from market_validation.dashboard_export import export_markdown_call_sheet
print(export_markdown_call_sheet(status="qualified"))
```

### Option 2: Lower-Level Functions
```python
from market_validation.research import create_research
from market_validation.research_runner import gather_companies, qualify_companies
from market_validation.dashboard_export import export_markdown_call_sheet

# 1. Create research
r = create_research("My Research", "<market>", "<product>", "<geography>")
rid = r["research_id"]

# 2. Gather + qualify
gather_companies(rid, "<market>", "<product>", "<geography>")
qualify_companies(rid, "<market>", "<product>")

# 3. Generate call sheet
print(export_markdown_call_sheet(status="qualified"))
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
researches: id, name, market, product, geography, status, created_at

companies: id, research_id, company_name, company_name_normalized,
           website, location, phone, email, status,
           priority_score, priority_tier, volume_estimate, volume_unit,
           notes, menu_items, prices, hours, ratings, reviews_count, raw_data,
           created_at, updated_at

contacts: id, company_id, research_id, name, name_normalized,
          title, email, phone, source, created_at

call_notes: id, company_id, research_id, author, note,
            meeting_at, next_action, created_at
```

**Indexes:** Companies indexed by research_id, status, priority_score. Contacts indexed by company_id and name_normalized (for deduplication).

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

## Example Workflow

```python
from market_validation.agent import Agent
from market_validation.research import create_research

# 1. Create research
r = create_research("My Research", "<market>", "<product>", "<geography>")
research_id = r["research_id"]

# 2. Run 3-step pipeline with agent
agent = Agent(research_id=research_id)
agent.find("<market>", "<geography>", "<product>")  # Find companies
agent.qualify()                                      # Score them
agent.enrich("<company_name>", "<geography>")        # Find contacts

# 3. View results
from market_validation.research import search_companies
companies = search_companies(research_id=research_id, status="qualified", limit=10)
for c in companies["companies"]:
    print(f"{c['company_name']}: {c['priority_score']} score, {c['email'] or 'no email'}")
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

### Setup

1. Copy `.env.example` to `.env`
2. Edit `.env` with your SMTP credentials
3. For Gmail: Create an App Password at https://myaccount.google.com/security

### Option 1: Direct Send (Riskier)

```python
from market_validation.email_sender import send_email

send_email(
    to_email="lead@company.com",
    subject="Partnership Opportunity",
    body="Hi, I'd like to discuss..."
)
```

### Option 2: Review Queue (Recommended)

Prep emails → Review in dashboard → Approve to send

```python
from market_validation.email_sender import prep_email, get_email_queue, approve_email, export_email_queue_markdown

# Prep emails to queue
prep_email(
    to_email="lead@company.com",
    subject="Partnership Opportunity",
    body="Hi...",
    company_name="Acme Corp"
)

# View queue as markdown
print(export_email_queue_markdown())

# Edit before sending
from market_validation.email_sender import update_queued_email
update_queued_email("email_id", body="New improved body...")

# Approve and send
approve_email("email_id")  # One at a time
# OR
from market_validation.email_sender import approve_all_emails
approve_all_emails()  # Send all pending
```

### Gmail App Password

1. Go to https://myaccount.google.com/security
2. Enable 2-Step Verification
3. App Passwords → Create new → Mail → enter a name (e.g., "market-validation")
4. Copy the 16-character password to `SMTP_PASSWORD` in `.env`

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

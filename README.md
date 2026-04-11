# Market Validation Agent

A general-purpose market research platform that discovers companies, qualifies leads, and tracks outreach. **No API keys required** for core functionality.

## Features

- **Company Discovery** - Web search for businesses using free sources (Yelp, Google, YellowPages)
- **Lead Qualification** - AI assessment with volume estimation
- **Contact Enrichment** - Find emails and phones from 8 different sources
- **Call Sheets** - Prioritized lists with contact information
- **Email Outreach** - Send templated emails with review queue (requires SMTP)
- **Dashboard** - Interactive web interface with inline editing

## Quick Start

```bash
# Install
pip install -e .

# Run the 3-step pipeline
python3 -c "
from market_validation.agent import Agent
from market_validation.research import create_research

rid = create_research(
    name='My Market Research',
    market='<market_or_product>',
    product='<specific_product>',
    geography='<location>'
)['research_id']

agent = Agent(research_id=rid)
agent.find('<market>', '<geography>', '<product>')  # Discover companies
agent.qualify()                                      # Score and rank
agent.enrich('<company_name>', '<geography>')        # Find contacts
"
```

## Usage

### 3-Step Pipeline

```python
from market_validation.agent import Agent
from market_validation.research import create_research

# Create research project
rid = create_research(
    name='Brisket BBQ in San Jose',
    market='BBQ restaurants',
    product='brisket',
    geography='San Jose, CA'
)['research_id']

# Step 1: Find companies
agent = Agent(research_id=rid)
agent.find('BBQ restaurants', 'San Jose, CA', 'brisket')

# Step 2: Qualify leads
agent.qualify()

# Step 3: Enrich specific companies
agent.enrich('Restaurant Name', 'San Jose, CA')
```

### Dashboard

```bash
# Start interactive dashboard (default port 8787)
python3 -m market_validation.dashboard

# Generate static HTML file
python3 -m market_validation.dashboard --static
```

Features: Project selector, inline row editing, email queue management, KPI dashboard.

### Export Call Sheet

```python
from market_validation.dashboard_export import export_markdown_call_sheet

# Export qualified leads
print(export_markdown_call_sheet(status='qualified'))
```

### Email Outreach

```python
from market_validation.email_sender import prep_email, approve_email

# Queue email for review
prep_email(
    to_email='contact@company.com',
    subject='Partnership Opportunity',
    body='Hi, I would like to discuss...',
    company_name='Company Name'
)

# Review in dashboard, then approve to send
approve_email('email_id')
```

## Architecture

```
market_validation/
├── agent.py              # Main interface (find/qualify/enrich)
├── research.py           # Database operations
├── research_runner.py    # Gather and qualify pipeline
├── research_manager.py   # High-level database operations
├── company_enrichment.py # Contact finding (8 sources)
├── dashboard.py          # HTML dashboard + server
├── dashboard_export.py   # Reports and exports
├── email_sender.py       # Email queue system
├── market_trends.py      # Google Trends integration
└── source_discovery.py   # Market type detection
```

## Database

**Location:** `output/market-research.sqlite3`

**Schema:**
```sql
researches: id, name, market, product, geography, status, created_at

companies: id, research_id, company_name, website, location, phone, email,
           status, priority_score, priority_tier, volume_estimate, volume_unit,
           notes, created_at

call_notes: id, company_id, research_id, author, note, next_action, created_at
```

## Contact Sources

The enrichment step queries 8 sources:
1. Official website + contact/about pages
2. LinkedIn (indirect via web search results)
3. Business directories (Yelp, Google, BBB)
4. News archives
5. Review sites (sentiment + volume hints)
6. Social media (Instagram, Facebook)
7. State business registry
8. Supplier pages

## Email Setup (Optional)

1. Copy `.env.example` to `.env`
2. Add SMTP credentials:
   - `SMTP_HOST` - SMTP server (default: smtp.gmail.com)
   - `SMTP_PORT` - Port (default: 587)
   - `SMTP_USER` - Username
   - `SMTP_PASSWORD` - App password
   - `FROM_EMAIL` - Sender address

For Gmail, create an App Password at https://myaccount.google.com/security

## Duplicate Prevention

Companies are deduplicated using normalized name matching (case/space insensitive).

## License

MIT

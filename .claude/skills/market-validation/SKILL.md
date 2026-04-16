---
name: market-validation
description: Run a full market research pipeline — enter a topic, get companies, leads, contacts, and a live dashboard.
---

# Market Validation Agent

Run a full market research pipeline with a single command. The user provides a research topic and the agent discovers companies, qualifies leads, enriches contacts, and launches a live dashboard.

## How to use

When the user invokes `/market-validation`, follow these steps exactly:

### Step 1: Collect research details

Ask the user for the following (in a single clear prompt):

| Field           | Required | Example                        |
|-----------------|----------|--------------------------------|
| **Market**      | Yes      | "BBQ restaurants", "robotics"  |
| **Geography**   | Yes      | "San Jose, California"         |
| **Product**     | No       | "commercial smokers"           |
| **Description** | No       | Free-text context for the research |
| **Validate?**   | No       | If yes, runs market validation (sizing, demand, competition, signals) before discovery. Default: no |

If the user already provided these details in the same message (e.g. `/market-validation BBQ restaurants in San Jose`), parse them and skip asking.

### Step 2: Run the full pipeline

Execute the following Python code via the Bash tool. Replace the placeholders with the user's inputs. Use the current working directory — do NOT hardcode paths.

```bash
cd "$(git rev-parse --show-toplevel)" && .venv/bin/python -c "
from market_validation.research import create_research
from market_validation.agent import Agent

res = create_research(
    name='MARKET in GEOGRAPHY',
    market='MARKET',
    product='PRODUCT',           # Use None if not provided
    geography='GEOGRAPHY',
    description='DESCRIPTION',   # Use None if not provided
)
rid = res['research_id']
print(f'Research created: {rid}')

agent = Agent(research_id=rid)
result = agent.research(
    'MARKET', 'GEOGRAPHY', 'PRODUCT',
    validate=VALIDATE_BOOL,      # True or False
)

s = result['summary']
print()
print(f'=== Research Complete ===')
print(f'Research ID:      {rid}')
print(f'Companies found:  {s[\"companies_found\"]}')
print(f'Qualified:        {s[\"qualified\"]}')
print(f'Phones found:     {s[\"phones_found\"]}')
print(f'Emails found:     {s[\"emails_found\"]}')
if 'verdict' in s:
    print(f'Market verdict:   {s[\"verdict\"]} ({s[\"overall_score\"]}/100)')
"
```

**Important:** This step takes several minutes. Let the user know the pipeline is running and show progress as it prints. Use a generous timeout (10 minutes).

### Step 3: Launch the dashboard

After the pipeline completes, start the dashboard server **in the background**:

```bash
cd "$(git rev-parse --show-toplevel)" && .venv/bin/python -m market_validation.dashboard --port 8788
```

Tell the user:

> Dashboard is live at **http://127.0.0.1:8788** — opening in your browser now.

### Step 4: Show results summary

Present the user with a concise summary:

```
## Research Complete

- **Market:** {market}
- **Geography:** {geography}
- **Product:** {product}
- **Research ID:** {research_id}
- **Companies found:** {N}
- **Qualified leads:** {N}
- **Phones found:** {N}
- **Emails found:** {N}
- **Market verdict:** {verdict} ({score}/100)  ← only if validate=True

Dashboard: http://127.0.0.1:8788
```

## Additional commands

After the initial run, the user may ask to:

- **Run market validation only** — `Agent(research_id=rid).validate(market, geography, product)`
- **Re-qualify** — `Agent(research_id=rid).qualify()`
- **Enrich a specific company** — `Agent(research_id=rid).enrich('Company Name', 'Location')`
- **Enrich all** — `Agent(research_id=rid).enrich_all(statuses=['qualified', 'new'])`
- **Export call sheet** — `from market_validation.dashboard_export import export_markdown_call_sheet; print(export_markdown_call_sheet(status_filter='qualified'))`
- **Restart dashboard** — `python -m market_validation.dashboard`
- **Add call notes** — `ResearchManager(research_id=rid).add_call_note(company_id, note='...', author='...', next_action='...')`
- **Update company data** — `ResearchManager(research_id=rid).update_company_data(company_id, {...})`

### Email workflow

Emails are queued as JSON files in `output/email-queue/`, then synced to the `emails` table.

```python
from market_validation.email_sender import prep_email, approve_email, delete_email, get_email_queue

# Queue a draft
prep_email(to_email='...', subject='...', body='...', company_name='...', research_id='...')

# Review queue
get_email_queue(status='pending')

# Approve (sends immediately)
approve_email(email_id='...')

# Delete a draft
delete_email(email_id='...')
```

## Database Schema

**Database:** `output/market-research.sqlite3`

```sql
CREATE TABLE researches (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    market TEXT NOT NULL,
    product TEXT,
    geography TEXT,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_source_health TEXT
);

CREATE TABLE companies (
    id TEXT PRIMARY KEY,
    research_id TEXT NOT NULL,
    company_name TEXT NOT NULL,
    company_name_normalized TEXT,
    website TEXT,
    location TEXT,
    phone TEXT,
    email TEXT,
    status TEXT DEFAULT 'new',        -- new → qualified → contacted → etc.
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

CREATE TABLE market_validations (
    id TEXT PRIMARY KEY,
    research_id TEXT NOT NULL,
    market TEXT NOT NULL,
    geography TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    -- Market sizing (TAM/SAM/SOM)
    tam_low REAL, tam_high REAL, tam_currency TEXT DEFAULT 'USD',
    tam_sources TEXT, tam_confidence INTEGER,
    sam_low REAL, sam_high REAL, sam_sources TEXT, sam_confidence INTEGER,
    som_low REAL, som_high REAL, som_sources TEXT, som_confidence INTEGER,
    -- Demand
    demand_score REAL, demand_trend TEXT, demand_seasonality TEXT,
    demand_pain_points TEXT, demand_sources TEXT,
    -- Competition
    competitive_intensity REAL, competitor_count INTEGER,
    market_concentration TEXT, direct_competitors TEXT,
    indirect_competitors TEXT, funding_signals TEXT,
    -- Signals
    job_posting_volume TEXT, news_sentiment TEXT,
    regulatory_risks TEXT, technology_maturity TEXT,
    signals_data TEXT,
    -- Scorecard
    market_attractiveness REAL, competitive_score REAL,
    demand_validation REAL, risk_score REAL,
    overall_score REAL, verdict TEXT, verdict_reasoning TEXT,
    -- Meta
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    FOREIGN KEY (research_id) REFERENCES researches(id) ON DELETE CASCADE
);

CREATE TABLE emails (
    id TEXT PRIMARY KEY,
    research_id TEXT,
    company_id TEXT,
    company_name TEXT,
    contact_name TEXT,
    to_email TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT,
    sent_at TEXT,
    opened_at TEXT,
    replied_at TEXT,
    bounced_at TEXT,
    reply_snippet TEXT,
    message_id TEXT
);
```

## Key files

| File | Purpose |
|------|---------|
| `market_validation/agent.py` | Main pipeline: validate/find/qualify/enrich/research |
| `market_validation/research.py` | Database layer (create_research, CRUD) |
| `market_validation/research_manager.py` | High-level DB ops (call notes, company updates) |
| `market_validation/dashboard.py` | HTML dashboard + REST API server |
| `market_validation/dashboard_export.py` | Markdown call sheet export |
| `market_validation/multi_search.py` | Multi-backend company search |
| `market_validation/web_scraper.py` | Website scraping for contacts |
| `market_validation/email_sender.py` | Email queue (prep/approve/send/delete) |
| `market_validation/company_enrichment.py` | Contact enrichment helpers |
| `market_validation/market_sizing.py` | TAM/SAM/SOM estimation |
| `market_validation/demand_analysis.py` | Demand scoring |
| `market_validation/competitive_landscape.py` | Competition analysis |
| `market_validation/market_signals.py` | Job postings, news, regulatory signals |
| `market_validation/validation_scorecard.py` | Go/no-go scorecard |
| `output/market-research.sqlite3` | SQLite database |
| `output/email-queue/` | Queued email JSON files |

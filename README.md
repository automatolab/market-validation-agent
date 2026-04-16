# Market Validation Agent

A market research pipeline that validates opportunities, discovers companies, qualifies leads, enriches contacts, and manages email outreach. No external API keys required for core search and validation.

## Quick Start

```bash
# Install
pip install -e .

# Run the full pipeline
python3 -c "
from market_validation.agent import Agent
from market_validation.research import create_research

rid = create_research(
    name='BBQ in San Jose',
    market='BBQ restaurants',
    product='brisket',
    geography='San Jose, CA'
)['research_id']

agent = Agent(research_id=rid)
agent.validate('BBQ restaurants', 'San Jose, CA', 'brisket')
agent.find('BBQ restaurants', 'San Jose, CA', 'brisket')
agent.qualify()
agent.enrich_all()
"

# Launch the dashboard
python3 -m market_validation.dashboard --port 8788
```

## Pipeline

The agent runs a 5-step pipeline. Each step can be invoked independently.

### 1. Validate

`agent.validate(market, geography, product)` assesses the market opportunity before committing to company discovery. It runs four sub-modules in sequence:

- **Market sizing** (TAM/SAM/SOM estimates from free web sources)
- **Demand analysis** (trend direction, seasonality, demand score)
- **Competitive landscape** (intensity score, funding signals, Porter's 5 Forces)
- **Market signals** (timing analysis, customer segments, unit economics)

Results feed into a validation scorecard that produces a weighted composite score and a go/no-go verdict. Scoring weights are tuned per market archetype (7 archetypes: local-service, B2B SaaS, e-commerce, marketplace, hardware, consumer app, professional services).

### 2. Find

`agent.find(market, geography, product)` discovers companies using multiple free search backends:

- **Nominatim/OpenStreetMap** -- geo-constrained business lookup
- **DuckDuckGo** (DDGS) -- general web search with circuit breaker on rate limits
- **BBB** -- Better Business Bureau public page scraping
- **OpenCorporates** -- corporate registry search (best-effort, often captcha-blocked)
- **Manta** -- business directory scraping
- **Wikipedia** -- supplementary context

AI-generated search strategies (via `claude` or `opencode` CLI) expand query coverage. Optional `sources/*.yaml` files add market-specific queries and directory URLs. Each result includes `source_health` metadata showing which backend produced it.

### 3. Qualify

`agent.qualify()` scores and ranks discovered companies using AI assessment. Each company receives a relevance score, priority tier (high/medium/low), volume estimate, and market signal annotations.

### 4. Enrich

`agent.enrich(company_name, location)` or `agent.enrich_all()` finds contact information through a 3-tier process:

- **Tier 1** -- Website scraping: crawl company site for emails, phones, contact pages. Generate email pattern candidates from domain.
- **Tier 2** -- Web search: DuckDuckGo queries for contact info not found on site.
- **Tier 3** -- AI research: `claude`/`opencode` CLI for deeper contact discovery.

Found emails are verified via MX record lookup (dnspython or socket fallback). Common email patterns (info@, contact@, sales@) are generated and checked.

### 5. Email

Outreach is managed through a queue system:

- `prep_email()` drafts a personalized email and saves it as a JSON file in `output/email-queue/` with status "draft"
- Emails are reviewed, edited, and approved through the dashboard or `approve_email()`
- Approved emails send via SMTP (Gmail or any provider)
- Tracking (opens, replies, bounces) available via Gmail API integration (`pip install -e '.[gmail]'`)
- All email state is persisted in both JSON queue files and the SQLite database

## Dashboard

```bash
python3 -m market_validation.dashboard             # interactive server on port 8788
python3 -m market_validation.dashboard --static     # generate static HTML file
```

The dashboard provides:

- **Project selector** -- switch between research projects, filtered by geography
- **Validation scorecard** -- market attractiveness, competitive intensity, go/no-go verdict
- **Company table** -- inline editing, delete, pagination, status filtering, CSV export
- **Email queue** -- review drafts, edit subject/body, approve or reject, send
- **KPI summary** -- counts by status, enrichment coverage, email funnel metrics
- **Gmail sync** -- pull reply/bounce status from Gmail inbox

## Configuration

Copy `.env.example` to `.env` for email features:

```
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=you@gmail.com
SMTP_PASSWORD=your-app-password
FROM_EMAIL=you@gmail.com
```

For Gmail tracking, place OAuth credentials in `config/gmail_credentials.json` and install the gmail extra: `pip install -e '.[gmail]'`.

No API keys are needed for search, validation, or enrichment. AI features require either the `claude` CLI (Claude Code) or `opencode` CLI on PATH.

## Project Structure

```
market_validation/
    agent.py                 Main Agent class: validate/find/qualify/enrich pipeline
    multi_search.py          Multi-backend search (Nominatim, DDGS, BBB, OpenCorporates, Manta)
    web_scraper.py           HTTP scraping for contact info, competitor data, Yelp density
    company_enrichment.py    3-tier contact enrichment with MX verification
    market_archetype.py      7 archetype definitions with scoring weights and benchmarks
    validation_scorecard.py  Composite scoring and go/no-go verdict generation
    market_sizing.py         TAM/SAM/SOM estimation
    demand_analysis.py       Demand trend and seasonality analysis
    competitive_landscape.py Competitive intensity and funding signal detection
    porters_five_forces.py   Porter's 5 Forces framework scoring
    timing_analysis.py       Market timing assessment
    unit_economics.py        CAC, LTV, margin benchmarks per archetype
    customer_segments.py     Customer segment identification
    email_sender.py          SMTP sending, queue management, batch operations
    email_tracker.py         Open/reply/bounce tracking
    gmail_tracker.py         Gmail API integration for inbox sync
    dashboard.py             Interactive HTML dashboard and local HTTP server
    dashboard_export.py      Static reports, markdown call sheets, CSV export
    research.py              SQLite database operations and schema management
    research_runner.py       Pipeline orchestration (gather + qualify)
    research_manager.py      High-level database queries
    source_config.py         YAML source file loader
    source_discovery.py      Market type detection

sources/                     Market-specific YAML search configs
config/                      Gmail credentials, pipeline configs
output/                      SQLite database, email queue, generated reports
```

## Database

SQLite database at `output/market-research.sqlite3`. Core tables: `researches`, `companies`, `call_notes`, `emails`. Companies are deduplicated using normalized name matching.

## License

MIT

# Market Validation Agent

A market research pipeline that validates opportunities, discovers companies, qualifies leads, enriches contacts, and manages AI-drafted email outreach. No external API keys required for core search, enrichment, or validation.

## Quick Start

```bash
# Install
pip install -e .

# Run the full pipeline in one call — validate → find → qualify → enrich → draft emails
python3 -c "
from market_validation.agent import Agent
from market_validation.research import create_research

rid = create_research(
    name='BBQ in San Jose',
    market='BBQ restaurants',
    product='brisket',
    geography='San Jose, CA',
)['research_id']

Agent(research_id=rid).research(
    'BBQ restaurants', 'San Jose, CA', 'brisket',
    validate=True,        # optional: TAM/SAM/SOM + competition + demand before discovery
    draft_emails=True,    # optional: AI-draft cold emails for each qualified lead
)
"

# Launch the dashboard
python3 -m market_validation.dashboard --port 8788
```

Each step (`validate`, `find`, `qualify`, `enrich`, `enrich_all`) can also be invoked independently — see `Additional commands` below.

## Using with Claude Code

This repo ships a Claude Code skill at `.claude/skills/market-validation/SKILL.md`. With Claude Code running in this directory, just type:

```
/market-validation brisket near San Jose California
```

The skill will parse the arguments, create a research record, run the full pipeline (including AI email drafting), and launch the dashboard on http://127.0.0.1:8788. For remote / SSH workflows, pass `--host 0.0.0.0`.

If you're not using Claude Code, use the Python API directly (see Quick Start above) — the skill is just a thin wrapper around `Agent.research()`.

## Pipeline

5 stages, all packaged into `Agent.research(...)`.

### 1. Validate

`agent.validate(market, geography, product)` assesses the market opportunity before committing to company discovery:

- **Market sizing** (TAM/SAM/SOM from free web sources)
- **Demand analysis** (trend direction, seasonality, pain points)
- **Competitive landscape** (intensity, funding signals, Porter's 5 Forces)
- **Market signals** (timing, customer segments, unit economics)

Results feed a scorecard that produces a weighted composite score and a go/no-go verdict. Scoring weights are tuned per market archetype (7 archetypes: `local-service`, `b2b-saas`, `b2b-industrial`, `consumer-cpg`, `marketplace`, `healthcare`, `services-agency`).

### 2. Find

`agent.find(market, geography, product)` discovers companies across multiple free backends:

- **Nominatim / OpenStreetMap** — geo-bounded business lookup
- **DuckDuckGo** (DDGS) — general web search with session-level rate-limit fallback
- **BBB / Manta / OpenCorporates / Wikipedia** — supplementary scrapers; auto-invoked when the fast backends return sparse results

AI-generated search strategies (`claude` or `opencode` CLI) expand query coverage; Nominatim queries are auto-simplified when the raw AI query is too verbose for a geocoder. Optional `sources/*.yaml` files add market-specific queries and directory URLs. Each result includes `source_health` metadata.

After discovery, an AI pre-save validation pass (Claude) cleans names, rejects listicles / directory pages, and confirms each candidate is a real operating business.

### 3. Qualify

`agent.qualify()` scores and ranks companies using AI assessment. Each company gets a relevance score, priority tier (high/medium/low), volume estimate, and signal annotations.

### 4. Enrich

`agent.enrich(company_name, location)` or `agent.enrich_all()` finds contact info through a 3-tier process:

- **Tier 1 — deep website scrape**: a 2-level BFS crawl (homepage → contact/team/locations pages → deeper links from those). Up to 20 pages per company. Pulls emails from:
  - `mailto:` links
  - Plain text (headers, footers)
  - `schema.org/Restaurant` + `LocalBusiness` + `Organization` JSON-LD blocks (`email`, `telephone`)
  - Cloudflare `data-cfemail` hex-obfuscated emails (XOR decoded)
  - `[at]` / `(at)` / `[dot]` obfuscated emails
  - HTML-entity-encoded emails
  - `/sitemap.xml` contact-URL discovery
  - Linked PDF menus / catering packets (up to 3 per crawl)
  - Linked Facebook page `/about/` tabs (public info)

- **Tier 2 — targeted web search**: DuckDuckGo + Nominatim. Snippet-extracted emails are validated before use.

- **Tier 3 — AI research**: `claude` or `opencode` CLI for deeper contact discovery when free tiers miss.

- **Adaptive multi-query search**: for any company still missing an email, run up to 4 search variants in sequence:
  1. `"Company" email contact <location>`
  2. `site:domain.com email OR contact OR mailto`
  3. `"Company" "@domain.com"`
  4. `"Company" mailto`
  Stop on the first on-domain MX-verified hit.

**Facts-only policy**: emails are saved only when observed on a real page or search snippet. All writes are gated through `is_plausible_email()` which rejects invalid TLDs (`.loc`, `.corp`), aggregator domains (Yelp / NetWaiter / Toast / OpenTable / Eventective / Wix / Squarespace / social networks), and strings with embedded commentary (`"x@y.com (inferred)"`). Pattern-guessed emails like `info@<domain>` are never written to the database.

Websites are similarly gated — aggregator/directory URLs never populate the `website` field.

### 5. Email

Outreach is managed through a review-and-approve queue:

- **AI drafting** — `draft_email_for_company(company_id)` asks Claude (or opencode) to write a short cold email grounded in the company's notes, priority, market, and product. Output is `{subject, body}` (no markdown, no filler phrasing).
- **Bulk drafting** — `draft_emails_for_research(research_id)` drafts + queues for every qualified lead with an email, 4 workers in parallel. Integrated into `Agent.research(draft_emails=True)` so drafts are ready before the dashboard boots.
- **Review and send** — drafts live in `output/email-queue/<id>.json` and the `emails` table. Approve individual (`approve_email(id)`) or bulk (`approve_all_emails()`). Reject individual (`delete_email(id)`) or bulk (`reject_all_emails()`).
- **Send transport** — SMTP (Gmail or any provider). Configure via `.env`.
- **Tracking** — opens (pixel), replies + bounces (Gmail API). Install with `pip install -e '.[gmail]'`.

## Dashboard

```bash
python3 -m market_validation.dashboard                    # interactive server on 127.0.0.1:8788
python3 -m market_validation.dashboard --host 0.0.0.0     # bind to all interfaces (SSH port-forward)
python3 -m market_validation.dashboard --static           # generate a static HTML snapshot
```

The interactive dashboard provides:

- **Project selector** — filter companies, emails, and KPIs to a single research run (or view global totals)
- **KPI summary** — projects, companies, qualified, with-phone, with-email, pending / sent / replied emails; all react to the selected project
- **Validation scorecard** — market attractiveness, competitive intensity, go/no-go verdict (when `validate=True`)
- **Company table** — per-row modal edit (clean form, not inline inputs), delete, pagination, CSV export, add-company
- **Per-row Draft button** — AI-draft an email for that company, review in a modal (with Regenerate button), queue as pending
- **Email queue** — per-row Approve / Edit / Reject buttons; bulk Draft-all-qualified / Approve-all-pending / Reject-all-pending
- **Gmail sync** — pull reply + bounce status from Gmail (auto-refreshes every 60s)

All actions update the page via JSON endpoint (`/api/data`) — no full page reloads.

## Configuration

Copy `.env.example` to `.env` for email features:

```
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=you@gmail.com
SMTP_PASSWORD=your-app-password
FROM_EMAIL=you@gmail.com
```

For Gmail reply / bounce tracking, place OAuth credentials at `config/gmail_credentials.json` and install the gmail extra: `pip install -e '.[gmail]'`.

No API keys are needed for search, enrichment, or validation. AI features (pre-save validation, Tier 3 enrichment, email drafting) require either the `claude` CLI (Claude Code) or `opencode` CLI on PATH.

## Additional commands

```python
from market_validation.agent import Agent
agent = Agent(research_id=rid)

agent.validate(market, geography, product)              # step 1 only
agent.find(market, geography, product)                  # step 2 only
agent.qualify()                                         # step 3 only
agent.enrich('Company Name', 'Location')                # enrich one company
agent.enrich_all(statuses=['qualified', 'new'])         # enrich a whole research

# Email queue programmatic access
from market_validation.email_sender import (
    draft_email_for_company, draft_emails_for_research,
    prep_email, get_email_queue,
    approve_email, approve_all_emails,
    delete_email, reject_all_emails,
    update_queued_email,
)

# Export a markdown call sheet
from market_validation.dashboard_export import export_markdown_call_sheet
print(export_markdown_call_sheet(status_filter='qualified'))
```

## Project structure

```
market_validation/
    agent.py                 Main Agent class: research/validate/find/qualify/enrich pipeline
    multi_search.py          Multi-backend search (Nominatim, DDGS, BBB, OpenCorporates, Manta, Wikipedia)
    web_scraper.py           Deep website scraping (BFS + sitemap + PDFs + FB about pages + JSON-LD + cfemail)
    company_enrichment.py    Email validation, MX lookup, aggregator-domain filter
    market_archetype.py      7 archetype definitions with scoring weights
    validation_scorecard.py  Composite scoring and go/no-go verdict
    market_sizing.py         TAM/SAM/SOM estimation
    demand_analysis.py       Demand trend and seasonality
    competitive_landscape.py Competition intensity and funding signals
    porters_five_forces.py   Porter's 5 Forces framework scoring
    timing_analysis.py       Market timing assessment
    unit_economics.py        CAC, LTV, margin benchmarks per archetype
    customer_segments.py     Customer segment identification
    email_sender.py          AI drafting, SMTP sending, queue + bulk operations
    email_tracker.py         Open tracking pixel
    gmail_tracker.py         Gmail API reply + bounce sync
    dashboard.py             Interactive HTML dashboard + REST API server
    dashboard_export.py      Markdown call sheets, CSV export
    research.py              SQLite schema, create_research, CRUD
    research_manager.py      High-level DB ops (call notes, company updates)
    source_config.py         YAML source file loader
    source_discovery.py      Market type detection

.claude/skills/
    market-validation/       Claude Code skill definition
sources/                     Market-specific YAML search configs
config/                      Gmail OAuth credentials, pipeline configs
output/                      SQLite database, email queue, generated reports
```

## Database

SQLite database at `output/market-research.sqlite3`. Core tables: `researches`, `companies`, `call_notes`, `emails`, `market_validations`. Companies are deduplicated via normalized name matching (`_normalize_name_key`).

## License

MIT

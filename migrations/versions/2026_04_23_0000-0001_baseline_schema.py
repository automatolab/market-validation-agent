# Baseline schema
#
# Mirrors _ensure_schema() in research.py AND the incremental columns
# _add_columns_if_missing() used to ALTER-TABLE at runtime. All statements use
# CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS so running this
# migration against an existing database (built before alembic existed) is
# a no-op — the schema is already there, alembic just records the version.
#
# Existing DBs should be stamped at this revision via:
#   alembic stamp 0001_baseline
# (or equivalently: market_validation.db.migrations.stamp_baseline()).
#
# Revision ID: 0001_baseline
# Revises:
# Create Date: 2026-04-23 00:00:00
from __future__ import annotations

from alembic import op

revision = "0001_baseline"
down_revision = None
branch_labels = None
depends_on = None


# ── Core tables ──────────────────────────────────────────────────────────────

_RESEARCHES_SQL = """
CREATE TABLE IF NOT EXISTS researches (
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
"""

_COMPANIES_SQL = """
CREATE TABLE IF NOT EXISTS companies (
    id TEXT PRIMARY KEY,
    research_id TEXT NOT NULL,
    market TEXT NOT NULL,
    company_name TEXT NOT NULL,
    company_name_normalized TEXT,
    website TEXT,
    location TEXT,
    phone TEXT,
    email TEXT,
    status TEXT NOT NULL DEFAULT 'new',
    priority_score INTEGER,
    priority_tier TEXT,
    next_action TEXT,
    why_now TEXT,
    volume_estimate REAL,
    volume_unit TEXT,
    volume_basis TEXT,
    volume_tier TEXT,
    notes TEXT,
    menu_items TEXT,
    prices TEXT,
    hours TEXT,
    ratings TEXT,
    reviews_count INTEGER,
    raw_data TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (research_id) REFERENCES researches(id) ON DELETE CASCADE
);
"""

_CALL_NOTES_SQL = """
CREATE TABLE IF NOT EXISTS call_notes (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    research_id TEXT NOT NULL,
    author TEXT NOT NULL,
    note TEXT NOT NULL,
    meeting_at TEXT,
    next_action TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
    FOREIGN KEY (research_id) REFERENCES researches(id) ON DELETE CASCADE
);
"""

# market_validations schema includes every column that was historically added
# via _add_columns_if_missing(). A brand-new DB gets them all up-front.
_MARKET_VALIDATIONS_SQL = """
CREATE TABLE IF NOT EXISTS market_validations (
    id TEXT PRIMARY KEY,
    research_id TEXT NOT NULL,
    market TEXT NOT NULL,
    geography TEXT,
    status TEXT NOT NULL DEFAULT 'pending',

    -- Market Sizing
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
    differentiation_opportunities TEXT,

    -- Signals
    job_posting_volume TEXT, news_sentiment TEXT,
    regulatory_risks TEXT, technology_maturity TEXT,
    signals_data TEXT,

    -- Scorecard
    market_attractiveness REAL, competitive_score REAL,
    demand_validation REAL, risk_score REAL,
    overall_score REAL, verdict TEXT, verdict_reasoning TEXT,

    -- Archetype
    archetype TEXT, archetype_confidence INTEGER, archetype_label TEXT,

    -- Unit economics
    gross_margin_low REAL, gross_margin_high REAL, gross_margin_confidence INTEGER,
    cac_estimate_low REAL, cac_estimate_high REAL,
    ltv_estimate_low REAL, ltv_estimate_high REAL,
    payback_months REAL, unit_economics_score REAL,
    unit_economics_data TEXT,

    -- Porter's 5 forces
    supplier_power REAL, buyer_power REAL, substitute_threat REAL,
    entry_barrier_score REAL, rivalry_score REAL,
    structural_attractiveness REAL, porters_data TEXT,

    -- Timing
    timing_score REAL, timing_verdict TEXT,
    timing_enablers TEXT, timing_headwinds TEXT,

    -- Customer segments
    customer_segments_data TEXT, icp_clarity REAL, primary_segment TEXT,

    -- Actionable output
    next_steps TEXT, key_risks TEXT,
    key_success_factors TEXT, archetype_red_flags TEXT,

    -- Meta
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    FOREIGN KEY (research_id) REFERENCES researches(id) ON DELETE CASCADE
);
"""

_EMAILS_SQL = """
CREATE TABLE IF NOT EXISTS emails (
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
"""

_INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_companies_research ON companies(research_id);",
    "CREATE INDEX IF NOT EXISTS idx_companies_market ON companies(market);",
    "CREATE INDEX IF NOT EXISTS idx_companies_status ON companies(status);",
    "CREATE INDEX IF NOT EXISTS idx_companies_priority ON companies(priority_score DESC);",
    "CREATE INDEX IF NOT EXISTS idx_call_notes_company ON call_notes(company_id);",
    "CREATE INDEX IF NOT EXISTS idx_validations_research ON market_validations(research_id);",
]


def upgrade() -> None:
    op.execute(_RESEARCHES_SQL)
    op.execute(_COMPANIES_SQL)
    op.execute(_CALL_NOTES_SQL)
    op.execute(_MARKET_VALIDATIONS_SQL)
    op.execute(_EMAILS_SQL)
    for stmt in _INDEX_STATEMENTS:
        op.execute(stmt)


def downgrade() -> None:
    # Drop in reverse-dependency order. We don't expect downgrade to be used
    # in production — this is here for test parity.
    op.execute("DROP TABLE IF EXISTS emails;")
    op.execute("DROP TABLE IF EXISTS call_notes;")
    op.execute("DROP TABLE IF EXISTS market_validations;")
    op.execute("DROP TABLE IF EXISTS companies;")
    op.execute("DROP TABLE IF EXISTS researches;")

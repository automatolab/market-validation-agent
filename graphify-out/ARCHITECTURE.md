# Market Validation System Architecture

## Core Components

### 1. agent.py - Deep Research Agent (MAIN)
- Does actual web research
- Adapts search strategies
- Digs deeper when needed

### 2. research_manager.py - Research Manager
- Manages database operations
- Tracks companies/contacts
- Generates call sheets

### 3. research.py - Database Layer
- Low-level CRUD
- SQLite operations

### 4. research_runner.py - Pipeline
- Gather companies (web search)
- Qualify companies (AI assessment)

### 5. company_enrichment.py - Contact Enrichment
- Find emails/phones
- Find decision makers

### 6. dashboard_export.py - Reports
- Call sheets
- Dashboard summaries

## Data Flow
1. Agent does research (agent.py)
2. Manager stores data (research_manager.py → research.py)
3. Pipeline gathers/qualifies (research_runner.py)
4. Reports generated (dashboard_export.py)

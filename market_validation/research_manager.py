from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class ResearchManager:
    def __init__(self, research_id: str | None = None, root: str | Path = "."):
        self.research_id = research_id
        self.root = Path(root).resolve()
        from market_validation.research import PROJECT_ROOT
        if str(self.root).endswith("market_validation"):
            self.root = PROJECT_ROOT

    def _run_opencode(self, prompt: str, timeout: int = 180) -> dict[str, Any]:
        result = subprocess.run(
            [
                "opencode", "run", "--dangerously-skip-permissions", "--dir", str(self.root),
                prompt,
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            return {"result": "error", "error": result.stderr or "opencode failed"}

        output = result.stdout.strip()
        json_start = output.find("{")
        if json_start < 0:
            return {"result": "error", "error": "No JSON in output"}

        json_text = output[json_start:]
        json_end = json_text.rfind("}")
        if json_end > 0:
            json_text = json_text[:json_end+1]

        try:
            return json.loads(json_text)
        except json.JSONDecodeError as e:
            return {"result": "error", "error": f"JSON parse error: {e}"}

    def _extract_json(self, text: str) -> dict[str, Any]:
        json_start = text.find("{")
        if json_start < 0:
            return {}
        json_text = text[json_start:]
        json_end = json_text.rfind("}")
        if json_end > 0:
            json_text = json_text[:json_end+1]
        try:
            return json.loads(json_text)
        except:
            return {}

    def gather_companies(self, market: str, geography: str, product: str | None = None, max_companies: int = 50) -> dict[str, Any]:
        from market_validation.research_runner import gather_companies
        result = gather_companies(
            research_id=self.research_id,
            market=market,
            product=product,
            geography=geography,
            root=self.root,
        )
        return {
            "action": "gather_companies",
            "result": result,
            "companies_found": result.get("companies_found", 0),
            "companies_added": result.get("companies_added", 0),
        }

    def qualify_companies(self, market: str, product: str | None = None) -> dict[str, Any]:
        from market_validation.research_runner import qualify_companies
        result = qualify_companies(
            research_id=self.research_id,
            market=market,
            product=product,
            root=self.root,
        )
        return {
            "action": "qualify_companies",
            "result": result,
            "qualified_count": result.get("qualified", 0),
        }

    def enrich_contact_info(self, company_name: str, website: str | None = None, location: str | None = None) -> dict[str, Any]:
        from market_validation.company_enrichment import enrich_company_contact
        result = enrich_company_contact(company_name, website, location)
        return {
            "action": "enrich_contact",
            "company": company_name,
            "result": result,
        }

    def enrich_all_qualified(self, limit: int = 20) -> dict[str, Any]:
        from market_validation.research import _connect, _ensure_schema, resolve_db_path
        from market_validation.company_enrichment import enrich_company_contact
        from market_validation.research import update_company, add_contact

        db_file = resolve_db_path(self.root)
        with _connect(db_file) as conn:
            _ensure_schema(conn)
            companies = conn.execute(
                """SELECT id, research_id, company_name, website, location, email 
                   FROM companies 
                   WHERE research_id = ? AND status IN ('qualified', 'new')
                   AND (email IS NULL OR email = '')
                   ORDER BY priority_score DESC NULLS LAST
                   LIMIT ?""",
                (self.research_id, limit)
            ).fetchall()

        enriched = []
        errors = []

        for company in companies:
            company_id, research_id, company_name, website, location, current_email = company
            result = enrich_company_contact(company_name, website, location)

            if result.get("result") == "ok":
                emails = result.get("emails_found", [])
                contacts = result.get("contacts", [])

                if not current_email and emails:
                    update_company(company_id, research_id, {"email": emails[0]})

                for contact in contacts:
                    add_contact(
                        company_id=company_id,
                        research_id=research_id,
                        name=contact.get("name"),
                        title=contact.get("title"),
                        source=contact.get("source", "web_search"),
                    )

                enriched.append({
                    "company": company_name,
                    "email": emails[0] if emails else None,
                    "contacts": contacts,
                })
            else:
                errors.append({"company": company_name, "error": result.get("error")})

        return {
            "action": "enrich_all",
            "enriched_count": len(enriched),
            "enriched": enriched,
            "errors": errors,
        }

    def search_company_details(self, company_name: str, query: str | None = None) -> dict[str, Any]:
        search_query = query or f"{company_name} restaurant BBQ menu hours location contact"
        prompt = f"""Search for detailed information about "{company_name}".

Query: {search_query}

Return JSON with any information you find:
{{
  "company_name": "{company_name}",
  "websites": ["url1", "url2"],
  "phone_numbers": ["555-123-4567"],
  "email_addresses": ["contact@example.com"],
  "addresses": ["123 Main St, City, State"],
  "hours_of_operation": {{"monday": "9-5", "tuesday": "9-5"}},
  "menu_highlights": ["item1 - description", "item2 - description"],
  "price_range": "$15-30",
  "ratings": {{"yelp": "4.5", "google": "4.6"}},
  "owner_staff": [{{"name": "John", "title": "Owner"}}],
  "social_media": {{"instagram": "...", "facebook": "..."}},
  "additional_notes": "Any other relevant info"
}}

Only include fields where information was actually found."""
        return self._run_opencode(prompt)

    def find_decision_makers(self, company_name: str, website: str | None = None) -> dict[str, Any]:
        prompt = f"""Find decision-makers and key personnel at "{company_name}".

Search for: "{company_name} owner", "{company_name} manager", "{company_name} purchasing", "{company_name} chef"

Return JSON:
{{
  "company": "{company_name}",
  "decision_makers": [
    {{"name": "John Smith", "title": "Owner", "source": "linkedin", "confidence": "high"}},
    {{"name": "Jane Doe", "title": "Head Chef", "source": "website", "confidence": "medium"}}
  ],
  "departments": ["Kitchen", "Purchasing", "Management"],
  "contact_notes": "How these people were found"
}}"""
        return self._run_opencode(prompt)

    def estimate_volume(self, company_name: str, market: str, product: str) -> dict[str, Any]:
        prompt = f"""Estimate the weekly/monthly volume of {product} that "{company_name}" might purchase.

Consider:
- Restaurant type and size
- Menu items featuring {product}
- Price points and portion sizes
- Restaurant reviews mentioning {product}
- Location and customer volume

Return JSON:
{{
  "company": "{company_name}",
  "product": "{product}",
  "estimated_weekly_volume": "100-150 lbs",
  "estimated_monthly_volume": "400-600 lbs",
  "confidence": "medium",
  "reasoning": "Why this estimate based on available information",
  "evidence": ["Source 1", "Source 2"]
}}"""
        return self._run_opencode(prompt)

    def analyze_competitors(self, geography: str, market: str) -> dict[str, Any]:
        prompt = f"""Analyze the competitive landscape for {market} in {geography}.

Find:
- Top competitors in the area
- Their pricing strategies
- Market share estimates
- Customer reviews mentioning competitors
- Gaps in the market

Return JSON:
{{
  "market": "{market}",
  "geography": "{geography}",
  "competitors": [
    {{"name": "Competitor 1", "strength": "price", "market_share": "30%", "notes": "..."}},
    {{"name": "Competitor 2", "strength": "quality", "market_share": "25%", "notes": "..."}}
  ],
  "market_gaps": ["Opportunity 1", "Opportunity 2"],
  "pricing_analysis": "Price range in market"
}}"""
        return self._run_opencode(prompt)

    def generate_outreach_message(self, company_name: str, contact_name: str | None, product: str, volume_estimate: str | None = None, tone: str = "professional") -> dict[str, Any]:
        prompt = f"""Generate an outreach message for "{company_name}" about {product}.

Contact: {contact_name or "the owner/decision maker"}
Product: {product}
Estimated volume need: {volume_estimate or "to be discussed"}
Tone: {tone}

Return JSON:
{{
  "company": "{company_name}",
  "contact": "{contact_name or 'Unknown'}",
  "email_subject": "Subject line",
  "email_body": "Full email body text",
  "call_script": "Brief phone script if calling",
  "key_talking_points": ["Point 1", "Point 2"],
  "objection_handling": {{"price": "Response", "timing": "Response"}}
}}"""
        return self._run_opencode(prompt)

    def research_research(self, task: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
        ctx_str = json.dumps(context, indent=2) if context else "No additional context"
        prompt = f"""You are a market research agent. Complete this research task:

Task: {task}

Context:
{ctx_str}

Use web search to gather information. Return your findings as JSON:
{{
  "task": "{task}",
  "findings": [
    {{"source": "url or source name", "data": "information found", "reliability": "high/medium/low"}}
  ],
  "summary": "Key takeaways",
  "recommendations": ["Action 1", "Action 2"],
  "next_steps": ["Step 1", "Step 2"]
}}"""
        return self._run_opencode(prompt, timeout=300)

    def update_company_data(self, company_id: str, data: dict[str, Any]) -> dict[str, Any]:
        from market_validation.research import update_company
        result = update_company(company_id, self.research_id, data, root=self.root)
        return {
            "action": "update_company",
            "company_id": company_id,
            "updates": data,
            "result": result,
        }

    def get_company(self, company_id: str) -> dict[str, Any]:
        from market_validation.research import _connect, _ensure_schema, resolve_db_path
        db_file = resolve_db_path(self.root)
        with _connect(db_file) as conn:
            _ensure_schema(conn)
            company = conn.execute(
                "SELECT * FROM companies WHERE id = ?", (company_id,)
            ).fetchone()
        if company:
            cols = [desc[0] for desc in conn.execute("PRAGMA table_info(companies)").fetchall()]
            return dict(zip(cols, company))
        return {}

    def get_all_companies(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        from market_validation.research import search_companies
        result = search_companies(research_id=self.research_id, status=status, limit=limit)
        return result.get("companies", [])

    def add_call_note(self, company_id: str, note: str, author: str = "Agent", next_action: str | None = None) -> dict[str, Any]:
        from market_validation.research import add_call_note
        result = add_call_note(
            company_id=company_id,
            research_id=self.research_id,
            author=author,
            note=note,
            next_action=next_action,
        )
        return {
            "action": "add_call_note",
            "company_id": company_id,
            "note": note,
            "result": result,
        }

    def get_call_notes(self, company_id: str) -> list[dict[str, Any]]:
        from market_validation.research import _connect, _ensure_schema, resolve_db_path
        db_file = resolve_db_path(self.root)
        with _connect(db_file) as conn:
            _ensure_schema(conn)
            notes = conn.execute(
                "SELECT * FROM call_notes WHERE company_id = ? ORDER BY created_at DESC",
                (company_id,)
            ).fetchall()
        if notes:
            cols = [desc[0] for desc in conn.execute("PRAGMA table_info(call_notes)").fetchall()]
            return [dict(zip(cols, n)) for n in notes]
        return []

    def get_call_sheet(self, status: str | None = "qualified", limit: int = 50) -> dict[str, Any]:
        from market_validation.dashboard_export import get_call_sheet_from_db
        return get_call_sheet_from_db(status_filter=status, limit=limit, root=self.root)

    def export_call_sheet_markdown(self, status: str | None = "qualified", limit: int = 50) -> str:
        from market_validation.dashboard_export import export_markdown_call_sheet
        return export_markdown_call_sheet(status_filter=status, limit=limit, root=self.root)

    def get_research_summary(self) -> dict[str, Any]:
        from market_validation.research import get_research
        result = get_research(self.research_id, root=self.root)
        stats = result.get("stats", {})
        return {
            "research": result.get("research", {}),
            "stats": stats,
            "company_count": stats.get("total", 0),
            "qualified_count": stats.get("qualified_count", 0),
        }

    def suggest_next_actions(self) -> dict[str, Any]:
        companies = self.get_all_companies(status="qualified", limit=100)
        needs_enrichment = [c for c in companies if not c.get("email")]
        needs_calls = [c for c in companies if c.get("email")]

        prompt = f"""Analyze this research project and suggest next actions.

Research Summary:
- Total companies: {len(companies)}
- Qualified: {len(needs_calls)}
- Need contact info: {len(needs_enrichment)}

Companies needing enrichment: {[c['company_name'] for c in needs_enrichment[:10]]}

Return JSON:
{{
  "priority_actions": [
    {{"action": "enrich_contacts", "reason": "Why", "companies": ["list"]}},
    {{"action": "send_outreach", "reason": "Why", "companies": ["list"]}},
    {{"action": "make_calls", "reason": "Why", "companies": ["list"]}}
  ],
  "research_insights": ["Insight 1", "Insight 2"],
  "recommended_followups": ["Follow up on X", "Check Y"]
}}"""
        return self._run_opencode(prompt)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Research Agent - Dynamic market research tool")
    parser.add_argument("--research-id", required=True, help="Research ID to operate on")
    parser.add_argument("--root", default=".", help="Repository root")
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Suggest next actions
    subparsers.add_parser("suggest", help="Get suggested next actions")
    
    # Research task
    research_parser = subparsers.add_parser("research", help="Run a research task")
    research_parser.add_argument("--task", required=True, help="Research task description")
    
    # Enrich companies
    enrich_parser = subparsers.add_parser("enrich", help="Enrich company contacts")
    enrich_parser.add_argument("--limit", type=int, default=20, help="Max companies")
    
    # Generate call sheet
    subparsers.add_parser("call-sheet", help="Export call sheet")
    subparsers.add_parser("summary", help="Get research summary")
    
    args = parser.parse_args()
    
    agent = ResearchAgent(research_id=args.research_id, root=args.root)
    
    if args.command == "suggest":
        result = agent.suggest_next_actions()
        print(json.dumps(result, indent=2))
    elif args.command == "research":
        result = agent.research_research(args.task)
        print(json.dumps(result, indent=2))
    elif args.command == "enrich":
        result = agent.enrich_all_qualified(limit=args.limit)
        print(json.dumps(result, indent=2))
    elif args.command == "call-sheet":
        print(agent.export_call_sheet_markdown())
    elif args.command == "summary":
        print(json.dumps(agent.get_research_summary(), indent=2))


if __name__ == "__main__":
    main()

# Backwards compatibility alias
ResearchAgent = ResearchManager

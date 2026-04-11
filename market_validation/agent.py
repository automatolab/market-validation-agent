"""
Market Research Agent - Simple 3-step pipeline:

1. find()      - Discover companies in a market
2. qualify()   - Score and rank them  
3. enrich()    - Find contact info (8 sources)

Usage:
    from market_validation.agent import Agent
    
    agent = Agent()
    
    # Step 1: Find companies
    agent.find("brisket", "San Jose, CA")
    
    # Step 2: Qualify (AI assessment)
    agent.qualify()
    
    # Step 3: Enrich contact info
    agent.enrich("Smoking Pig BBQ")
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class Agent:
    """
    Market Research Pipeline Agent.
    
    Simple 3-step pipeline:
    1. find()    - Discover companies
    2. qualify() - Score them
    3. enrich()  - Find contacts
    """
    
    def __init__(self, research_id: str | None = None, root: str | Path = "."):
        self.research_id = research_id
        self.root = Path(root).resolve()
        
        from market_validation.research import PROJECT_ROOT
        if str(self.root).endswith("market_validation"):
            self.root = PROJECT_ROOT
        
        self.last_result: dict[str, Any] = {}
    
    def _run(self, prompt: str, timeout: int = 180) -> dict[str, Any]:
        """Run opencode and return JSON."""
        try:
            result = subprocess.run(
                ["opencode", "run", "--dangerously-skip-permissions", "--dir", str(self.root), prompt],
                capture_output=True, text=True, timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            return {"result": "error", "error": "Timeout"}
        
        if result.returncode != 0:
            return {"result": "error", "error": result.stderr or "Failed"}
        
        text = result.stdout.strip()
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end+1])
            except json.JSONDecodeError:
                return {"result": "error", "error": "JSON parse failed"}
        return {"result": "error", "error": "No JSON in output"}
    
    def find(self, market: str, geography: str, product: str | None = None) -> dict[str, Any]:
        """
        STEP 1: Find companies in a market.
        
        Searches web for businesses matching the criteria.
        Stores results in database.
        """
        search_term = product or market
        
        prompt = f"""Find businesses in {geography} that offer {search_term}.

For each business, find:
- Company name, website, address, phone
- What they sell/offer related to {search_term}
- How established they are (reviews, years in business)

Search sources: Yelp, Google Maps, YellowPages, business websites
Search queries: "{search_term} {geography}", "best {market} businesses {geography}"

Return JSON:
{{
  "companies": [
    {{
      "company_name": "Business Name",
      "website": "https://...",
      "location": "Address",
      "phone": "555-123-4567",
      "description": "What they do",
      "evidence_url": "https://source..."
    }}
  ]
}}"""
        
        result = self._run(prompt, timeout=180)
        
        if result.get("result") == "error":
            return result
        
        companies = result.get("companies", [])
        
        # Store in database if we have research_id
        if self.research_id:
            from market_validation.research import add_company, _connect, _ensure_schema, resolve_db_path
            db = resolve_db_path(self.root)
            
            with _connect(db) as conn:
                _ensure_schema(conn)
                added = 0
                for c in companies:
                    r = add_company(
                        research_id=self.research_id,
                        company_name=c.get("company_name", "Unknown"),
                        market=market,
                        website=c.get("website"),
                        location=c.get("location"),
                        phone=c.get("phone"),
                        notes=c.get("description"),
                        raw_data=c,
                        root=self.root,
                    )
                    if r.get("result") == "ok":
                        added += 1
                result["companies_added"] = added
        
        self.last_result = result
        return result
    
    def qualify(self) -> dict[str, Any]:
        """
        STEP 2: Qualify companies - AI assessment of relevance and volume.
        
        Updates companies in database with priority scores and volume estimates.
        """
        if not self.research_id:
            return {"result": "error", "error": "No research_id set"}
        
        from market_validation.research import _connect, _ensure_schema, resolve_db_path, update_company
        db = resolve_db_path(self.root)
        
        with _connect(db) as conn:
            _ensure_schema(conn)
            conn.row_factory = None
            companies = conn.execute(
                """SELECT id, company_name, notes, phone, website, location 
                   FROM companies WHERE research_id = ? AND status = 'new'""",
                (self.research_id,)
            ).fetchall()
        
        if not companies:
            return {"result": "ok", "qualified": 0, "message": "No companies to qualify"}
        
        company_list = [{"id": str(c[0]), "name": str(c[1]), "notes": c[2], "phone": c[3], "website": c[4], "location": c[5]} for c in companies]
        
        prompt = f"""Evaluate these companies for relevance to our market.

For each company, assess:
1. How relevant are they? (0-100)
2. Estimated volume (if B2B) or size (if B2C)
3. Priority tier: high/medium/low
4. Status: qualified/uncertain/not_relevant

Companies:
{json.dumps(company_list, indent=2)}

Return JSON:
{{
  "results": [
    {{
      "company_id": "id from list",
      "status": "qualified|uncertain|not_relevant",
      "score": 0-100,
      "priority": "high|medium|low",
      "volume_estimate": "estimate",
      "notes": "why qualified or not"
    }}
  ]
}}"""
        
        result = self._run(prompt, timeout=180)
        
        if result.get("result") == "error":
            return result
        
        qualified = 0
        for r in result.get("results", []):
            cid = r.get("company_id")
            fields = {
                "status": r.get("status", "new"),
                "priority_score": int(r.get("score", 0)),
                "priority_tier": r.get("priority", "low"),
                "volume_estimate": r.get("volume_estimate"),
                "notes": r.get("notes"),
            }
            update_company(cid, self.research_id, fields, root=self.root)
            if r.get("status") == "qualified":
                qualified += 1
        
        self.last_result = result
        return {"result": "ok", "qualified": qualified, "assessed": len(companies)}
    
    def enrich(self, company_name: str, location: str | None = None) -> dict[str, Any]:
        """
        STEP 3: Enrich - Find contact info using 8 different sources.
        
        Sources:
        1. Official website
        2. LinkedIn (indirect via web search)
        3. Business directories (Yelp, Google, BBB)
        4. News archives
        5. Review sites
        6. Social media
        7. Business registry
        8. Supplier pages
        """
        sources_tried = []
        all_findings = {}
        
        # Source 1: Official website
        result = self._search_website(company_name, location)
        if result.get("found"):
            sources_tried.append("website")
            all_findings.update(result)
        
        # Source 2: LinkedIn (via web search)
        result = self._search_linkedin(company_name)
        if result.get("found"):
            sources_tried.append("linkedin")
            all_findings.update(result)
        
        # Source 3: Business directories
        result = self._search_directories(company_name, location)
        if result.get("found"):
            sources_tried.append("directories")
            all_findings.update(result)
        
        # Source 4: News
        result = self._search_news(company_name)
        if result.get("found"):
            sources_tried.append("news")
            all_findings.update(result)
        
        # Source 5: Reviews
        result = self._search_reviews(company_name, location)
        if result.get("found"):
            sources_tried.append("reviews")
            all_findings.update(result)
        
        # Source 6: Social media
        result = self._search_social(company_name)
        if result.get("found"):
            sources_tried.append("social")
            all_findings.update(result)
        
        # Source 7: Business registry (optional - can be slow)
        result = self._search_registry(company_name, location)
        if result.get("found"):
            sources_tried.append("registry")
            all_findings.update(result)
        
        # Update database if we have research_id and company_id
        if self.research_id and all_findings:
            self._update_company_from_findings(company_name, all_findings)
        
        return {
            "result": "ok",
            "company": company_name,
            "sources_tried": sources_tried,
            "findings": all_findings,
        }
    
    def _update_company_from_findings(self, company_name: str, findings: dict):
        """Update company record with enriched data."""
        from market_validation.research import _connect, _ensure_schema, resolve_db_path, update_company, add_contact
        
        db = resolve_db_path(self.root)
        
        with _connect(db) as conn:
            _ensure_schema(conn)
            conn.row_factory = None
            company = conn.execute(
                """SELECT id FROM companies 
                   WHERE research_id = ? AND (company_name LIKE ? OR company_name LIKE ?)""",
                (self.research_id, f"%{company_name}%", f"%{company_name.replace(' ', '%')}%")
            ).fetchone()
        
        if company:
            cid = str(company[0])
            updates = {}
            
            # Email
            email = (findings.get("emails") or [None])[0]
            if email and not findings.get("email"):
                findings["email"] = email
            if email:
                updates["email"] = email
            
            # Phone (if we found additional)
            if findings.get("phones"):
                existing = conn.execute("SELECT phone FROM companies WHERE id = ?", (cid,)).fetchone()
                if existing and not existing[0]:
                    updates["phone"] = findings["phones"][0]
            
            if updates:
                update_company(cid, self.research_id, updates, root=self.root)
            
            # Add contacts
            contacts = findings.get("employees_found") or []
            for c in contacts[:5]:
                add_contact(
                    company_id=cid,
                    research_id=self.research_id,
                    name=c.get("name"),
                    title=c.get("title"),
                    source="enrichment",
                )
    
    def _search_website(self, company: str, location: str | None) -> dict:
        """Source 1: Official website."""
        loc = f" {location}" if location else ""
        prompt = f"""Find info for "{company}"{loc}.

Search for official website, then extract from it:
- Contact page: email, phone
- About page: owners, team
- Menu: items and prices
- Catering: info and contact

Return JSON:
{{
  "found": true/false,
  "website": "url",
  "emails": ["email@..."],
  "phones": ["555-123-4567"],
  "owners": ["Name - Title"],
  "catering_contact": "email or url",
  "notes": "What you found"
}}"""
        return self._run(prompt, timeout=120) or {"found": False}
    
    def _search_linkedin(self, company: str) -> dict:
        """Source 2: LinkedIn (via web search)."""
        prompt = f"""Find people at "{company}" via web search.

Search: "{company}" owner LinkedIn, "{company}" founder, "{company}" management team

Return JSON:
{{
  "found": true/false,
  "employees_found": [
    {{"name": "Name", "title": "Title", "relevance": "..."}}
  ],
  "decision_makers": ["Names of key decision makers"],
  "notes": "How you found this"
}}"""
        return self._run(prompt, timeout=120) or {"found": False}
    
    def _search_directories(self, company: str, location: str | None) -> dict:
        """Source 3: Business directories."""
        loc = f" {location}" if location else ""
        prompt = f"""Find "{company}"{loc} in directories.

Search: Yelp, Google Maps, YellowPages, BBB, Crunchbase

Return JSON:
{{
  "found": true/false,
  "yelp": {{"rating": "...", "reviews": "..."}},
  "google": {{"rating": "...", "reviews": "..."}},
  "years_in_business": "...",
  "emails": ["email if listed"],
  "notes": "What directories had info"
}}"""
        return self._run(prompt, timeout=120) or {"found": False}
    
    def _search_news(self, company: str) -> dict:
        """Source 4: News archives."""
        prompt = f"""Find news about "{company}".

Search: "{company}" news, "{company}" press, "{company}" expansion

Return JSON:
{{
  "found": true/false,
  "articles": [
    {{"title": "...", "source": "...", "date": "...", "summary": "..."}}
  ],
  "notes": "Key news findings"
}}"""
        return self._run(prompt, timeout=120) or {"found": False}
    
    def _search_reviews(self, company: str, location: str | None) -> dict:
        """Source 5: Review sites."""
        loc = f" {location}" if location else ""
        prompt = f"""Analyze reviews for "{company}"{loc}.

Search: Yelp reviews, Google reviews

Look for:
- Sentiment (positive/negative)
- Volume indicators ("I come every week")
- Complaints
- What people praise

Return JSON:
{{
  "found": true/false,
  "rating_estimate": "4.5/5",
  "volume_indicators": ["Quotes suggesting customer volume"],
  "pricing_perception": "expensive/moderate/affordable",
  "notes": "Key review insights"
}}"""
        return self._run(prompt, timeout=120) or {"found": False}
    
    def _search_social(self, company: str) -> dict:
        """Source 6: Social media."""
        prompt = f"""Find social media for "{company}".

Search: "{company}" Instagram, Facebook, Twitter

Return JSON:
{{
  "found": true/false,
  "instagram": {{"url": "...", "followers": "..."}},
  "facebook": {{"url": "...", "likes": "..."}},
  "notes": "Social media presence"
}}"""
        return self._run(prompt, timeout=120) or {"found": False}
    
    def _search_registry(self, company: str, location: str | None) -> dict:
        """Source 7: Business registry."""
        loc = f" {location}" if location else ""
        prompt = f"""Find "{company}"{loc} in business registry.

Search: CA Secretary of State business lookup

Return JSON:
{{
  "found": true/false,
  "entity_type": "LLC/Corp/etc",
  "officers": ["Name - Title"],
  "notes": "Registry findings"
}}"""
        return self._run(prompt, timeout=90) or {"found": False}


def main():
    """CLI entry point."""
    import argparse
    parser = argparse.ArgumentParser(description="Market Research Agent")
    parser.add_argument("command", choices=["find", "qualify", "enrich"])
    parser.add_argument("--research-id", help="Research ID")
    parser.add_argument("--market", help="Market/product")
    parser.add_argument("--geography", help="Geography")
    parser.add_argument("--company", help="Company name for enrich")
    
    args = parser.parse_args()
    agent = Agent(research_id=args.research_id)
    
    import json
    if args.command == "find":
        result = agent.find(args.market, args.geography)
    elif args.command == "qualify":
        result = agent.qualify()
    elif args.command == "enrich":
        result = agent.enrich(args.company)
    
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

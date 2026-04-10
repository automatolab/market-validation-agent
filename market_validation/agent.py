"""
Dynamic Research Agent - Generic market research that adapts based on findings.

Unlike hard-coded pipelines, this agent:
1. Analyzes what information is missing
2. Formulates multiple search strategies
3. Digs deeper when surface info is insufficient
4. Tries alternative approaches when one fails
5. Works for ANY market/product/geography
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
    A research agent that dynamically decides how to gather information
    based on current gaps and what it discovers.
    
    This is the MAIN agent for deep research. Use this for:
    - Finding companies in a market
    - Understanding market intelligence
    - Deep research on specific companies
    - Adaptive research based on goals
    """

    def __init__(self, research_id: str | None = None, root: str | Path = "."):
        self.research_id = research_id
        self.root = Path(root).resolve()
        
        # Import research utilities
        from market_validation.research import PROJECT_ROOT
        if str(self.root).endswith("market_validation"):
            self.root = PROJECT_ROOT
        
        # Track research state
        self.information_gaps: list[dict] = []
        self.search_attempts: list[dict] = []
        self.findings: list[dict] = []

    def _run_opencode(self, prompt: str, timeout: int = 180) -> dict[str, Any]:
        """Run an opencode command and return parsed JSON."""
        result = subprocess.run(
            [
                "opencode", "run", "--dangerously-skip-permissions", "--dir", str(self.root),
                prompt,
            ],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        
        self.search_attempts.append({
            "prompt_preview": prompt[:100],
            "success": result.returncode == 0,
            "timestamp": _iso_now(),
        })
        
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
        except json.JSONDecodeError:
            return {"result": "error", "error": "JSON parse failed"}

    def analyze_gaps(self, company_name: str, current_data: dict[str, Any]) -> dict[str, Any]:
        """Analyze what information is missing and how to find it."""
        
        missing = []
        strategies = []
        
        # Check for missing basic info
        if not current_data.get("website"):
            missing.append("website")
            strategies.append({
                "gap": "website",
                "strategy": f'Search for "{company_name}" official website',
                "search_queries": [
                    f'{company_name} restaurant official website',
                    f'{company_name} menu site:.com',
                ]
            })
        
        if not current_data.get("phone"):
            missing.append("phone")
            strategies.append({
                "gap": "phone",
                "strategy": f'Find contact phone for {company_name}',
                "search_queries": [
                    f'{company_name} phone number',
                    f'{company_name} contact',
                ]
            })
        
        if not current_data.get("email"):
            missing.append("email")
            strategies.append({
                "gap": "email",
                "strategy": f'Find email for {company_name} purchasing/owner',
                "search_queries": [
                    f'{company_name} email',
                    f'{company_name} owner email',
                    f'{company_name} catering email',
                    f'site:yelp.com "{company_name}" email',
                ]
            })
        
        if not current_data.get("contacts") or len(current_data.get("contacts", [])) == 0:
            missing.append("decision_makers")
            strategies.append({
                "gap": "decision_makers",
                "strategy": f'Find key people at {company_name}',
                "search_queries": [
                    f'{company_name} owner',
                    f'{company_name} linkedin',
                    f'{company_name} manager',
                    f'{company_name} chef',
                ]
            })
        
        return {
            "company": company_name,
            "missing_information": missing,
            "search_strategies": strategies,
            "has_enough_info": len(missing) == 0,
        }

    def deep_research_company(self, company_name: str, location: str | None = None, max_depth: int = 3) -> dict[str, Any]:
        """
        Do deep research on a company, digging deeper until we have enough info
        or decide we've exhausted reasonable search paths.
        """
        
        # Start with basic data
        current_data = {"company_name": company_name, "location": location}
        
        results = {
            "company_name": company_name,
            "search_depth": 0,
            "findings": [],
            "gaps_filled": [],
            "final_data": current_data,
        }
        
        depth = 0
        while depth < max_depth:
            depth += 1
            results["search_depth"] = depth
            
            # Analyze what we still need
            gap_analysis = self.analyze_gaps(company_name, current_data)
            
            if gap_analysis["has_enough_info"]:
                results["gaps_filled"] = list(gap_analysis["missing_information"])
                break
            
            # Try each missing piece with multiple strategies
            for strategy in gap_analysis["search_strategies"]:
                gap = strategy["gap"]
                
                # Try multiple search queries for this gap
                for query in strategy["search_queries"][:3]:  # Limit queries per gap
                    search_result = self._search_for_info(company_name, query, gap)
                    
                    if search_result.get("found"):
                        if gap == "website":
                            current_data["website"] = search_result.get("value")
                        elif gap == "phone":
                            current_data["phone"] = search_result.get("value")
                        elif gap == "email":
                            current_data["email"] = search_result.get("value")
                        elif gap == "decision_makers":
                            if "contacts" not in current_data:
                                current_data["contacts"] = []
                            current_data["contacts"].append(search_result.get("contact", {}))
                        
                        results["findings"].append({
                            "gap": gap,
                            "query": query,
                            "result": search_result,
                        })
                        
                        # If we found something important, try to dig deeper
                        if gap == "website" and search_result.get("value"):
                            # Try to find more info from the website
                            web_deep = self._search_from_website(
                                company_name, 
                                search_result.get("value"),
                                ["contact", "about", "team", "owner", "catering"]
                            )
                            if web_deep.get("found"):
                                results["findings"].append(web_deep)
                                current_data.update(web_deep.get("updates", {}))
        
        results["final_data"] = current_data
        self.findings.extend(results["findings"])
        
        return results

    def _search_for_info(self, company_name: str, query: str, info_type: str) -> dict[str, Any]:
        """Search for specific type of information about a company."""
        
        prompt = f"""Research task: Find {info_type} for "{company_name}".

Search query: {query}

Return JSON:
{{
  "found": true/false,
  "value": "the {info_type} found",
  "source": "where it was found",
  "confidence": "high/medium/low",
  "notes": "any additional context"
}}

If not found, return: {{"found": false}}"""
        
        return self._run_opencode(prompt)

    def _search_from_website(self, company_name: str, website: str, keywords: list[str]) -> dict[str, Any]:
        """Search within a website for specific information."""
        
        keyword_str = ", ".join(keywords)
        
        prompt = f"""Deep search on "{company_name}" website: {website}

Search for pages containing: {keyword_str}

Look for:
- Contact page for email/phone
- About page for owners/staff
- Catering page for decision makers
- Any page mentioning key personnel

Return JSON:
{{
  "found": true/false,
  "pages_searched": ["page1", "page2"],
  "updates": {{
    "additional_phones": ["..."],
    "additional_emails": ["..."],
    "owners_found": ["Name - Title"],
    "hours": "...",
    "menu_url": "...",
    "catering_info": "..."
  }},
  "notes": "What was found on each page"
}}"""
        
        return self._run_opencode(prompt)

    def research_market_intelligence(self, market: str, geography: str, product: str | None = None) -> dict[str, Any]:
        """
        Research market intelligence for a given market.
        This is the MAIN entry point for ANY market research.
        """
        
        search_term = product or market
        
        prompt = f"""You are a market research analyst. Research the market for {search_term} in {geography}.

This is NOT about finding specific companies - it's about understanding the MARKET.

Research these areas:

1. **Market Size & Demand**
   - How big is the market for {search_term} in {geography}?
   - What are the growth trends?

2. **Key Players**
   - Who are the major suppliers/competitors?
   - What are typical price ranges?

3. **Customer Segments**
   - Who buys {search_term}?
   - What are their needs?

4. **Trends & Challenges**
   - What trends are affecting this market?
   - What challenges do buyers face?

5. **Opportunities**
   - What gaps exist in the market?
   - What could a new entrant offer?

6. **Search Strategies**
   - What search queries would find relevant companies?
   - What sources are best for this type of market?

Return JSON:
{{
  "market": "{market}",
  "product": "{search_term}",
  "geography": "{geography}",
  "market_size": "estimated size with source",
  "key_trends": ["trend1", "trend2"],
  "customer_segments": ["segment1", "segment2"],
  "challenges": ["challenge1", "challenge2"],
  "opportunities": ["opportunity1", "opportunity2"],
  "recommended_search_queries": ["query1", "query2"],
  "recommended_sources": ["source1", "source2"],
  "notes": "Additional insights"
}}"""
        
        result = self._run_opencode(prompt, timeout=300)
        
        if result.get("result") == "error":
            return {"result": "error", "error": result.get("error")}
        
        return {
            "result": "ok",
            "research_type": "market_intelligence",
            "market": market,
            "geography": geography,
            "product": search_term,
            "intelligence": result,
        }

    def research_company_deep(
        self, 
        company_name: str, 
        location: str | None = None,
        website: str | None = None,
        focus_areas: list[str] | None = None
    ) -> dict[str, Any]:
        """
        Deep research on a SINGLE company.
        Tries multiple strategies and digs deeper when possible.
        
        Focus areas can include: contacts, pricing, reviews, social, news, suppliers, partnerships
        """
        
        if focus_areas is None:
            focus_areas = ["contacts", "decision_makers", "pricing", "reviews", "social"]
        
        # Start with basic info
        company_data = {
            "company_name": company_name,
            "location": location,
            "website": website,
            "phones": [],
            "emails": [],
            "contacts": [],
            "social_media": {},
            "reviews": {},
            "pricing_info": {},
            "news": [],
        }
        
        # Phase 1: Surface search
        surface_results = self._search_surface_info(company_name, location, website)
        company_data.update(surface_results)
        
        # Phase 2: Deep search based on what we found
        if not company_data.get("emails") or not company_data.get("contacts"):
            # Try specific searches for decision makers
            dm_results = self._search_decision_makers(company_name, website)
            if dm_results.get("contacts"):
                company_data["contacts"].extend(dm_results["contacts"])
        
        if not company_data.get("website"):
            # Try harder to find the website
            website_result = self._search_website(company_name, location)
            if website_result.get("found"):
                company_data["website"] = website_result.get("website")
        
        # Phase 3: Try to dig into specific focus areas
        for focus in focus_areas:
            if focus == "social":
                social = self._search_social_media(company_name)
                company_data["social_media"].update(social)
            elif focus == "news":
                news = self._search_news(company_name)
                company_data["news"] = news
            elif focus == "suppliers":
                suppliers = self._search_suppliers(company_name)
                company_data["suppliers"] = suppliers
        
        # Phase 4: Try website scraping if we have a URL
        if company_data.get("website"):
            web_data = self._scrape_website_intelligence(company_name, company_data["website"])
            company_data.update(web_data)
        
        return {
            "result": "ok",
            "company": company_name,
            "data": company_data,
            "completeness": self._calculate_completeness(company_data),
        }

    def _search_surface_info(self, company_name: str, location: str | None, website: str | None) -> dict[str, Any]:
        """Phase 1: Get surface-level info."""
        
        loc_str = f" in {location}" if location else ""
        
        prompt = f"""Quick research on "{company_name}"{loc_str}.

Find ALL of the following:
- Official website
- Phone number(s)
- Email address(es)
- Address
- Social media handles
- Google/Yelp rating
- Hours of operation

Return JSON:
{{
  "website": "url or null",
  "phones": ["phone1", "phone2"],
  "emails": ["email1", "email2"],
  "address": "full address or null",
  "social": {{"facebook": "...", "instagram": "...", "twitter": "..."}},
  "rating": {{"google": "4.5", "yelp": "4.2"}},
  "hours": "Mon-Fri 9-5 or similar"
}}"""
        
        result = self._run_opencode(prompt)
        
        if result.get("result") == "error":
            return {}
        
        return result

    def _search_decision_makers(self, company_name: str, website: str | None) -> dict[str, Any]:
        """Phase 2: Find key people at the company."""
        
        web_str = f"Check {website}" if website else "Search online"
        
        prompt = f"""Find key decision makers at "{company_name}".

Search for: 
- "{company_name} owner"
- "{company_name} founder"
- "{company_name} CEO"
- "{company_name} purchasing manager"
- "{company_name} linkedin"
- "{company_name} management team"

Look on:
- LinkedIn profiles
- Company website (about/team pages)
- News articles mentioning the company
- Business directories

Return JSON:
{{
  "contacts": [
    {{"name": "John Smith", "title": "Owner", "source": "linkedin", "profile_url": "..."}},
    {{"name": "Jane Doe", "title": "Purchasing Manager", "source": "website", "profile_url": "..."}}
  ],
  "departments": ["Management", "Kitchen", "Purchasing"],
  "notes": "How these people were found"
}}"""
        
        result = self._run_opencode(prompt, timeout=240)
        
        if result.get("result") == "error":
            return {"contacts": []}
        
        return result

    def _search_website(self, company_name: str, location: str | None) -> dict[str, Any]:
        """Try to find the official website."""
        
        loc_str = f" {location}" if location else ""
        
        prompt = f"""Find the official website for "{company_name}"{loc_str}.

Search for:
- "{company_name}" restaurant
- "{company_name}" official site
- "{company_name}" menu

Return JSON:
{{
  "found": true/false,
  "website": "url if found",
  "confidence": "high/medium/low",
  "source": "where found"
}}"""
        
        result = self._run_opencode(prompt)
        return result if isinstance(result, dict) else {"found": False}

    def _search_social_media(self, company_name: str) -> dict[str, Any]:
        """Find social media accounts."""
        
        prompt = f"""Find social media accounts for "{company_name}".

Search for:
- "{company_name}" facebook
- "{company_name}" instagram
- "{company_name}" twitter
- "{company_name}" yelp

Return JSON:
{{
  "facebook": "url or null",
  "instagram": "url or null",
  "twitter": "url or null",
  "yelp": "url or null",
  "linkedin": "url or null"
}}"""
        
        result = self._run_opencode(prompt)
        return result if isinstance(result, dict) else {}

    def _search_news(self, company_name: str) -> list[dict]:
        """Search for news about the company."""
        
        prompt = f"""Find recent news about "{company_name}".

Search for:
- "{company_name}" news 2024
- "{company_name}" press release
- "{company_name}" expansion
- "{company_name}" interview

Return JSON:
{{
  "articles": [
    {{"title": "Article Title", "source": "News Source", "date": "2024-01-01", "url": "..."}},
    {{"title": "Another Article", "source": "Source", "date": "2024-02-01", "url": "..."}}
  ]
}}"""
        
        result = self._run_opencode(prompt)
        return result.get("articles", []) if isinstance(result, dict) else []

    def _search_suppliers(self, company_name: str) -> dict[str, Any]:
        """Find suppliers or partners of the company."""
        
        prompt = f"""Research suppliers/partners for "{company_name}".

Search for:
- "{company_name}" suppliers
- "{company_name}" where they buy
- "{company_name}" partnerships

Return JSON:
{{
  "suppliers_mentioned": ["Supplier 1", "Supplier 2"],
  "partnerships": ["Partner 1", "Partner 2"],
  "notes": "How this info was found"
}}"""
        
        result = self._run_opencode(prompt)
        return result if isinstance(result, dict) else {}

    def _scrape_website_intelligence(self, company_name: str, website: str) -> dict[str, Any]:
        """Try to extract intelligence from the company website."""
        
        prompt = f"""Explore the website for "{company_name}": {website}

Look for and extract:
1. **Contact page** - emails, phone numbers, contact form
2. **About page** - company story, team, owners
3. **Menu** - items and prices
4. **Catering page** - catering options, minimums, contacts
5. **News/Blog** - recent updates
6. **Careers** - hiring info (indicates growth)

Return JSON:
{{
  "additional_phones": ["..."],
  "additional_emails": ["..."],
  "owners": ["Name - Title"],
  "team_members": ["Name - Title"],
  "menu_highlights": ["item - price"],
  "catering_info": {{"min_order": "...", "contact": "...", "options": ["..."]}},
  "company_description": "...",
  "years_in_business": "...",
  "notes": "What was found on each page"
}}"""
        
        result = self._run_opencode(prompt, timeout=240)
        return result if isinstance(result, dict) else {}

    def _calculate_completeness(self, data: dict) -> dict[str, Any]:
        """Calculate how complete the company data is."""
        
        fields = ["website", "phones", "emails", "contacts", "social_media"]
        present = sum(1 for f in fields if data.get(f))
        total = len(fields)
        
        score = int((present / total) * 100)
        
        missing = [f for f in fields if not data.get(f)]
        
        return {
            "score": score,
            "complete": score >= 70,
            "missing_fields": missing,
        }

    def batch_research(
        self, 
        companies: list[dict[str, Any]], 
        depth: str = "standard"
    ) -> dict[str, Any]:
        """
        Research multiple companies in batch.
        
        depth: "quick" (surface only), "standard" (recommended), "deep" (max digging)
        """
        
        max_depth = {"quick": 1, "standard": 2, "deep": 3}[depth]
        
        results = {
            "total": len(companies),
            "completed": 0,
            "companies": [],
            "summary": {
                "total_contacts_found": 0,
                "total_emails_found": 0,
                "total_phones_found": 0,
            }
        }
        
        for company in companies:
            name = company.get("company_name")
            location = company.get("location")
            website = company.get("website")
            
            if not name:
                continue
            
            # Research this company
            research_result = self.research_company_deep(
                company_name=name,
                location=location,
                website=website,
                max_depth=max_depth,
            )
            
            if research_result.get("result") == "ok":
                results["completed"] += 1
                results["companies"].append(research_result)
                
                # Update summary
                data = research_result.get("data", {})
                results["summary"]["total_contacts_found"] += len(data.get("contacts", []))
                results["summary"]["total_emails_found"] += len(data.get("emails", []))
                results["summary"]["total_phones_found"] += len(data.get("phones", []))
        
        return results

    def adaptive_research(
        self,
        goal: str,
        market: str | None = None,
        geography: str | None = None,
        initial_companies: list[dict] | None = None
    ) -> dict[str, Any]:
        """
        ADAPTIVE research - the agent decides what to search for based on the goal.
        
        This is the MAIN dynamic research method. You give it a GOAL and it figures out:
        1. What information to gather
        2. What search strategies to use
        3. When to dig deeper
        4. When to move on
        
        Example goals:
        - "Find BBQ restaurants in San Jose that might buy brisket"
        - "Find all SaaS companies in SF with 50+ employees"
        - "Research dental practices in Austin that do implants"
        """
        
        prompt = f"""You are a strategic market research agent. Your goal is:

GOAL: {goal}

{'(market: ' + market + ')' if market else ''}
{'(geography: ' + geography + ')' if geography else ''}

Step 1: Understand what information you need to achieve this goal
Step 2: Identify the best search strategies
Step 3: Execute searches
Step 4: Analyze results and decide if you need to dig deeper
Step 5: If information is insufficient, try alternative strategies
Step 6: Synthesize findings into actionable insights

Think strategically:
- What keywords will find relevant companies?
- What information proves relevance to the goal?
- How can you verify quality of findings?
- What gaps exist and how to fill them?

Return JSON:
{{
  "goal": "{goal}",
  "understanding": "How I interpreted the goal",
  "search_strategy": {{
    "primary_queries": ["query1", "query2"],
    "sources": ["source1", "source2"],
    "verification_methods": ["method1", "method2"]
  }},
  "initial_findings": [
    {{
      "company": "Company Name",
      "relevance": "why they fit the goal",
      "confidence": "high/medium/low",
      "contact_info": {{"email": "...", "phone": "..."}},
      "evidence": "what makes them relevant"
    }}
  ],
  "information_gaps": ["gap1", "gap2"],
  "recommended_next_steps": ["step1", "step2"],
  "final_recommendations": ["recommendation1"]
}}"""
        
        return self._run_opencode(prompt, timeout=300)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Dynamic Research Agent")
    parser.add_argument("--goal", required=True, help="Research goal")
    parser.add_argument("--market", help="Market/product")
    parser.add_argument("--geography", help="Geography")
    parser.add_argument("--depth", choices=["quick", "standard", "deep"], default="standard")
    
    args = parser.parse_args()
    
    agent = DynamicResearchAgent()
    
    result = agent.adaptive_research(
        goal=args.goal,
        market=args.market,
        geography=args.geography,
    )
    
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

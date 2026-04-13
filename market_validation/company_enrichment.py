from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _run_ai_prompt(prompt: str, timeout: int = 120, cwd: str = ".") -> str:
    """
    Run an AI prompt via the best available agent CLI.
    Tries: claude (Claude Code) → opencode → raises RuntimeError.
    Returns raw stdout text.
    """
    import shutil
    import subprocess as _sp

    if shutil.which("claude"):
        result = _sp.run(
            ["claude", "-p", prompt, "--output-format", "text"],
            capture_output=True, text=True, timeout=timeout, cwd=cwd,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()

    if shutil.which("opencode"):
        result = _sp.run(
            ["opencode", "run", "--dangerously-skip-permissions", "--dir", cwd, prompt],
            capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode == 0:
            return result.stdout.strip()

    raise RuntimeError("No AI agent available (install claude or opencode)")


def enrich_company_contact(
    company_name: str,
    website: str | None,
    location: str | None,
) -> dict[str, Any]:
    website_hint = f"Start by visiting their website at {website}." if website else f'Start by searching for "{company_name}" official website.'
    location_hint = f" They are located in {location}." if location else ""

    prompt = f"""Find contact information for "{company_name}".{location_hint}

{website_hint}

Find:
1. Contact email addresses (purchasing, sales, info@, owner@)
2. Phone numbers
3. Contact form URLs
4. Social media links (LinkedIn, Facebook)
5. Key personnel: owner, founder, decision maker, purchasing manager

Search queries to try:
- "{company_name} contact email"
- "{company_name} owner"
- "{company_name} purchasing manager"
- "{company_name} LinkedIn"

Return JSON:
{{
  "company_name": "{company_name}",
  "website": "url if found",
  "emails_found": ["email1@example.com"],
  "phones_found": ["555-123-4567"],
  "contacts": [
    {{"name": "John Smith", "title": "Owner", "source": "website"}},
    {{"name": "Jane Doe", "title": "Purchasing Manager", "source": "linkedin"}}
  ],
  "social_links": {{"linkedin": "...", "facebook": "..."}},
  "notes": "How this info was found"
}}

Only include fields where information was actually found. Return empty arrays/objects if nothing found."""

    try:
        output = _run_ai_prompt(prompt, timeout=120)

        json_start = output.find("{")
        if json_start < 0:
            return {
                "result": "failed",
                "error": "No JSON in output",
                "company_name": company_name,
            }

        json_text = output[json_start:]
        json_end = json_text.rfind("}")
        if json_end > 0:
            json_text = json_text[:json_end + 1]

        data = json.loads(json_text)
        return {
            "result": "ok",
            "company_name": company_name,
            **data,
        }

    except Exception as e:
        return {
            "result": "failed",
            "error": str(e),
            "company_name": company_name,
        }


def enrich_research_companies(
    research_id: str,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    from market_validation.research import _connect, _ensure_schema, resolve_db_path, update_company

    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = None
        companies = conn.execute(
            """SELECT id, company_name, website, phone, location, email 
               FROM companies 
               WHERE research_id = ? AND status IN ('qualified', 'new')
               ORDER BY priority_score DESC NULLS LAST""",
            (research_id,)
        ).fetchall()

    if not companies:
        return {"result": "ok", "research_id": research_id, "enriched": 0, "message": "No companies to enrich"}

    enriched_count = 0
    email_found_count = 0
    errors = []

    for company in companies:
        company_id = company[0]
        company_name = company[1]
        website = company[2]
        phone = company[3]
        location = company[4]
        current_email = company[5]

        if not website and not location and not company_name:
            continue

        result = enrich_company_contact(company_name, website, location)

        if result.get("result") == "ok":
            emails = result.get("emails_found", [])
            phones = result.get("phones_found", [])

            if current_email is None and emails:
                update_company(
                    company_id=company_id,
                    research_id=research_id,
                    fields={"email": emails[0]},
                    root=root_path,
                    db_path=db_path,
                )
                email_found_count += 1

            enriched_count += 1

            if not emails:
                errors.append(f"{company_name}: No contact info found")

    return {
        "result": "ok",
        "research_id": research_id,
        "total_companies": len(companies),
        "enriched": enriched_count,
        "emails_found": email_found_count,
        "errors": errors[:5],
    }


def build_parser() -> Any:
    import argparse
    parser = argparse.ArgumentParser(description="Enrich company data with contacts and emails")
    parser.add_argument("--root", default=".", help="Repository root path")
    parser.add_argument("--db-path", default=None, help="SQLite DB path")
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    enrich_parser = subparsers.add_parser("enrich", help="Enrich companies with contact info")
    enrich_parser.add_argument("research_id", help="Research ID")
    enrich_parser.add_argument("--limit", type=int, default=50, help="Max companies to enrich")
    
    single_parser = subparsers.add_parser("single", help="Enrich single company")
    single_parser.add_argument("--company-name", required=True, help="Company name")
    single_parser.add_argument("--website", help="Company website")
    single_parser.add_argument("--location", help="Company location")
    
    return parser


def main() -> None:
    import json

    parser = build_parser()
    args = parser.parse_args()

    if args.command == "single":
        result = enrich_company_contact(
            company_name=args.company_name,
            website=args.website,
            location=args.location,
        )
        print(json.dumps(result, ensure_ascii=True, indent=2))

    elif args.command == "enrich":
        result = enrich_research_companies(
            research_id=args.research_id,
            root=args.root,
            db_path=args.db_path,
        )
        print(json.dumps(result, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()

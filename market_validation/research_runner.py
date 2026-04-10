from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from market_validation.research import (
    create_research,
    get_research,
    add_company,
    update_company,
    add_contact,
    resolve_db_path,
    _connect,
    _ensure_schema,
)
from market_validation.source_discovery import discover_sources


def run_market_research(
    name: str,
    market: str,
    product: str | None,
    geography: str,
    max_companies: int = 100,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()

    research = create_research(
        name=name,
        market=market,
        product=product,
        geography=geography,
        root=root_path,
        db_path=db_path,
    )
    research_id = research["research_id"]

    sources = discover_sources(
        market=market,
        geography=geography,
        target_product=product,
        max_sources=8,
    )

    demand_data = None
    try:
        from market_validation.market_trends import get_market_demand_report
        demand_result = get_market_demand_report(
            target_product=product or market,
            geography=geography,
        )
        if demand_result.get("result") == "ok" and not demand_result.get("skipped"):
            demand_data = demand_result
    except Exception:
        pass

    return {
        "result": "ok",
        "research_id": research_id,
        "name": name,
        "market": market,
        "geography": geography,
        "sources_discovered": len(sources),
        "demand_data": demand_data,
    }


def gather_companies(
    research_id: str,
    market: str,
    product: str | None,
    geography: str,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    search_term = product or market

    try:
        result = subprocess.run(
            [
                "opencode", "run", "--dangerously-skip-permissions", "--dir", str(root_path),
                f"""Find businesses in {geography} that offer {search_term}.
For each business, gather as much info as possible:
- Company name, website, address, phone
- Hours of operation
- Products/services offered with {search_term} and prices
- Ratings (Yelp, Google, etc) and number of reviews
- Any other relevant info

Return JSON with detailed data:
{{
  "companies": [
    {{
      "company_name": "Business Name",
      "website": "https://...",
      "location": "Full Address",
      "phone": "555-123-4567",
      "hours": "Mon-Fri 11am-8pm",
      "ratings": {{"yelp": "4.5", "google": "4.6"}},
      "reviews_count": 500,
      "products": [
        {{"item": "Product Name", "price": "$18.95", "description": "Description"}}
      ],
      "description": "Business description",
      "evidence_url": "https://example.com/..."
    }}
  ]
}}

Search multiple sources: Yelp, Google Maps, Yellow Pages, business websites.
Return as many businesses as possible (aim for 20+).
Search for: "{search_term} {geography}", "best {market} businesses {geography}"""
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )

        if result.returncode != 0:
            return {"result": "failed", "error": result.stderr or "opencode failed"}

        output = result.stdout.strip()
        json_start = output.find("{")
        if json_start < 0:
            return {"result": "failed", "error": "No JSON in output"}

        data = json.loads(output[json_start:])
        companies = data.get("companies", [])
        added = 0

        for company in companies:
            add_result = add_company(
                research_id=research_id,
                company_name=company.get("company_name", "Unknown"),
                market=market,
                website=company.get("website"),
                location=company.get("location"),
                phone=company.get("phone"),
                email=company.get("email"),
                hours=company.get("hours"),
                menu_items=company.get("menu_items"),
                prices=company.get("prices"),
                ratings=company.get("ratings"),
                reviews_count=company.get("reviews_count"),
                notes=company.get("description") or company.get("notes"),
                raw_data=company,
                root=root_path,
                db_path=db_path,
            )
            if add_result.get("result") == "ok":
                added += 1

        return {
            "result": "ok",
            "research_id": research_id,
            "companies_found": len(companies),
            "companies_added": added,
        }

    except subprocess.TimeoutExpired:
        return {"result": "failed", "error": "Timeout"}
    except Exception as e:
        return {"result": "failed", "error": str(e)}


def qualify_companies(
    research_id: str,
    market: str,
    product: str | None,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    import sqlite3
    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = sqlite3.Row
        companies = conn.execute(
            "SELECT * FROM companies WHERE research_id = ? AND status = 'new'",
            (research_id,)
        ).fetchall()

    if not companies:
        return {"result": "ok", "research_id": research_id, "qualified": 0, "message": "No companies to qualify"}

    company_list = [dict(c) for c in companies]

    try:
        result = subprocess.run(
            [
                "opencode", "run", "--dangerously-skip-permissions", "--dir", str(root_path),
                f"""Evaluate these companies for {product or market} relevance.
Return JSON:
{{
  "results": [
    {{
      "company_id": "id from list",
      "status": "qualified|uncertain|not_relevant",
      "confidence": 0.0-1.0,
      "volume_estimate": "value unit or null",
      "volume_basis": "why estimate",
      "priority": "high|medium|low",
      "notes": "assessment"
    }}
  ]
}}
Companies: {json.dumps(company_list, indent=2)}"""
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )

        if result.returncode != 0:
            return {"result": "failed", "error": result.stderr or "opencode failed"}

        output = result.stdout.strip()
        json_start = output.find("{")
        if json_start < 0:
            return {"result": "failed", "error": "No JSON in output"}

        json_text = output[json_start:]
        json_end = json_text.rfind("}")
        if json_end > 0:
            json_text = json_text[:json_end+1]

        data = json.loads(json_text)
        qualified_count = 0

        for q in data.get("results", []):
            vol = q.get("volume_estimate", "")
            vol_value = None
            vol_unit = None
            if vol:
                import re
                match = re.search(r"([\d.]+)", str(vol))
                if match:
                    vol_value = float(match.group(1))
                    vol_unit = str(vol).replace(str(vol_value), "").strip()

            update_company(
                company_id=q.get("company_id"),
                research_id=research_id,
                fields={
                    "status": q.get("status", "new"),
                    "priority_score": int(float(q.get("confidence", 0)) * 100),
                    "priority_tier": q.get("priority", "low"),
                    "notes": q.get("notes"),
                    "volume_estimate": vol_value,
                    "volume_unit": vol_unit,
                    "volume_basis": q.get("volume_basis"),
                },
                root=root_path,
                db_path=db_path,
            )
            if q.get("status") == "qualified":
                qualified_count += 1

        return {
            "result": "ok",
            "research_id": research_id,
            "companies_evaluated": len(companies),
            "qualified": qualified_count,
        }

    except Exception as e:
        return {"result": "failed", "error": str(e)}


def build_parser() -> Any:
    import argparse
    parser = argparse.ArgumentParser(description="Market research CLI")
    parser.add_argument("--root", default=".", help="Repository root path")
    parser.add_argument("--db-path", default=None, help="SQLite DB path")

    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Create and run research")
    run_parser.add_argument("--name", required=True)
    run_parser.add_argument("--market", required=True)
    run_parser.add_argument("--product")
    run_parser.add_argument("--geography", required=True)
    run_parser.add_argument("--max-companies", type=int, default=100)

    gather_parser = subparsers.add_parser("gather", help="Gather companies")
    gather_parser.add_argument("research_id")
    gather_parser.add_argument("--market", required=True)
    gather_parser.add_argument("--product")
    gather_parser.add_argument("--geography", required=True)

    qualify_parser = subparsers.add_parser("qualify", help="Qualify companies")
    qualify_parser.add_argument("research_id")
    qualify_parser.add_argument("--market", required=True)
    qualify_parser.add_argument("--product")

    return parser


def main() -> None:
    import json

    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "run":
            research = run_market_research(
                name=args.name,
                market=args.market,
                product=args.product,
                geography=args.geography,
                max_companies=args.max_companies,
                root=args.root,
                db_path=args.db_path,
            )
            print(json.dumps(research, ensure_ascii=True, indent=2))

        elif args.command == "gather":
            result = gather_companies(
                research_id=args.research_id,
                market=args.market,
                product=args.product,
                geography=args.geography,
                root=args.root,
                db_path=args.db_path,
            )
            print(json.dumps(result, ensure_ascii=True, indent=2))

        elif args.command == "qualify":
            result = qualify_companies(
                research_id=args.research_id,
                market=args.market,
                product=args.product,
                root=args.root,
                db_path=args.db_path,
            )
            print(json.dumps(result, ensure_ascii=True, indent=2))

    except Exception as exc:
        print(json.dumps({"result": "failed", "error": str(exc)}, ensure_ascii=True))
        raise SystemExit(1)

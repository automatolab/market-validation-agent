from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from market_validation.environment import load_project_env

MARKET_SOURCE_TEMPLATES = {
    "restaurant": [
        {"source_type": "search", "provider": "duckduckgo", "query": "{query} {geography} restaurant directory"},
        {"source_type": "directory", "provider": "yelp", "query": "{query} {geography}"},
        {"source_type": "directory", "provider": "tripadvisor", "query": "{query} {geography}"},
        {"source_type": "directory", "provider": "overpass_osm", "query": "amenity=restaurant;cuisine=bbq;city={geography}"},
        {"source_type": "search", "provider": "bing", "query": "{query} {geography} BBQ restaurant"},
        {"source_type": "directory", "provider": "yellowpages", "query": "{query} {geography}"},
        {"source_type": "news", "provider": "news", "query": "{query} {geography} restaurant opening"},
        {"source_type": "internal_feed", "provider": "pytrends", "query": "{query} demand trends {geography}"},
    ],
    "retail": [
        {"source_type": "search", "provider": "duckduckgo", "query": "{query} {geography} store directory"},
        {"source_type": "directory", "provider": "overpass_osm", "query": "shop;city={geography}"},
        {"source_type": "search", "provider": "bing", "query": "{query} {geography} retail store"},
        {"source_type": "directory", "provider": "yelp", "query": "{query} {geography}"},
        {"source_type": "directory", "provider": "yellowpages", "query": "{query} {geography}"},
        {"source_type": "news", "provider": "news", "query": "{query} {geography} retail"},
        {"source_type": "internal_feed", "provider": "pytrends", "query": "{query} demand trends {geography}"},
    ],
    "tech": [
        {"source_type": "search", "provider": "duckduckgo", "query": "{query} {geography} technology company"},
        {"source_type": "search", "provider": "linkedin", "query": "{query} {geography} company"},
        {"source_type": "news", "provider": "news", "query": "{query} {geography} startup funding"},
        {"source_type": "news", "provider": "news", "query": "{query} {geography} technology news"},
        {"source_type": "directory", "provider": "crunchbase", "query": "{query} {geography}"},
        {"source_type": "internal_feed", "provider": "pytrends", "query": "{query} technology trends {geography}"},
    ],
    "healthcare": [
        {"source_type": "search", "provider": "duckduckgo", "query": "{query} {geography} hospital clinic"},
        {"source_type": "directory", "provider": "yelp", "query": "{query} {geography} healthcare"},
        {"source_type": "news", "provider": "news", "query": "{query} {geography} healthcare news"},
        {"source_type": "directory", "provider": "healthgrades", "query": "{query} {geography}"},
        {"source_type": "internal_feed", "provider": "pytrends", "query": "{query} healthcare demand {geography}"},
    ],
    "default": [
        {"source_type": "search", "provider": "duckduckgo", "query": "{query} {geography} business directory"},
        {"source_type": "search", "provider": "bing", "query": "{query} {geography}"},
        {"source_type": "directory", "provider": "overpass_osm", "query": "amenity=restaurant;city={geography}"},
        {"source_type": "directory", "provider": "yellowpages", "query": "{query} {geography}"},
        {"source_type": "news", "provider": "news", "query": "{query} {geography} news"},
        {"source_type": "internal_feed", "provider": "pytrends", "query": "{query} demand trends {geography}"},
    ],
}


def _detect_market_type(market: str, target_product: str) -> str:
    combined = f"{market} {target_product}".lower()
    restaurant_keywords = [
        "restaurant", "food", "cafe", "coffee", "bbq", "grill", "pizza",
        "sandwich", "deli", "catering", "brisket", "steakhouse", "burger",
        "taco", "sushi", "asian", "mexican", "italian", "bakery", "dessert",
        "pub", "bar", "diner", "eatery"
    ]
    retail_keywords = [
        "store", "shop", "retail", "outlet", "supermarket", "grocery",
        "clothing", "furniture", "electronics", "hardware", "pharmacy",
        "boutique", "market"
    ]
    tech_keywords = [
        "software", "saas", "tech", "app", "platform", "cloud", "ai",
        "ml", "data", "cyber", "security", "fintech", "healthtech",
        "robot", "robotics", "drone", "automation", "aerospace", "semiconductor",
        "hardware", "embedded", "iot", "sensor", "vision", "lidar"
    ]
    healthcare_keywords = [
        "hospital", "clinic", "medical", "health", "doctor", "dental",
        "pharma", "biotech", "wellness", "therapy", "rehab"
    ]

    for kw in restaurant_keywords:
        if kw in combined:
            return "restaurant"
    for kw in retail_keywords:
        if kw in combined:
            return "retail"
    for kw in tech_keywords:
        if kw in combined:
            return "tech"
    for kw in healthcare_keywords:
        if kw in combined:
            return "healthcare"
    return "default"


def discover_sources(
    market: str,
    geography: str,
    target_product: str | None = None,
    max_sources: int = 8,
) -> list[dict[str, Any]]:
    target_product = target_product or market
    market_type = _detect_market_type(market, target_product)
    templates = MARKET_SOURCE_TEMPLATES.get(market_type, MARKET_SOURCE_TEMPLATES["default"])

    discovered = []
    for tmpl in templates[:max_sources]:
        query = tmpl["query"].format(query=target_product, geography=geography)
        discovered.append({
            "source_id": f"auto-{tmpl['provider']}-{len(discovered)+1}",
            "source_type": tmpl["source_type"],
            "provider": tmpl["provider"],
            "query": query,
            "region": geography,
            "enabled": True,
            "auto_discovered": True,
            "api_required": False,
        })
    return discovered


def discover_sources_with_websearch(
    market: str,
    geography: str,
    target_product: str | None = None,
    max_results: int = 15,
    root: str | Path = ".",
) -> dict[str, Any]:
    try:
        from websearch import websearch
        has_websearch = True
    except ImportError:
        has_websearch = False

    target_product = target_product or market

    queries = [
        f"{target_product} {geography} restaurant directory",
        f"best {target_product} {geography} review site",
        f"{target_product} {geography} business listing",
        f"top rated {target_product} {geography}",
        f"{target_product} {geography} company",
        f"{target_product} industry news {geography}",
    ]

    all_results = []
    if has_websearch:
        for query in queries[:5]:
            try:
                results = websearch(query=query, numResults=8)
                all_results.extend(results)
            except Exception:
                pass

    sources = []
    seen_providers = set()

    for result in all_results[:max_results]:
        url = result.get("url", "")
        title = result.get("title", "")

        provider = "unknown"
        if "yelp.com" in url:
            provider = "yelp"
        elif "tripadvisor.com" in url:
            provider = "tripadvisor"
        elif "yellowpages.com" in url:
            provider = "yellowpages"
        elif "facebook.com" in url:
            provider = "facebook"
        elif "bing.com" in url:
            provider = "bing"
        elif "duckduckgo" in url:
            provider = "duckduckgo"
        elif "google.com/search" in url or "maps.google" in url:
            provider = "google_search"
        elif "news.google" in url or "news.ycombinator" in url:
            provider = "news"
        elif "linkedin.com" in url:
            provider = "linkedin"
        elif "crunchbase.com" in url:
            provider = "crunchbase"
        elif "healthgrades.com" in url:
            provider = "healthgrades"
        elif "manta.com" in url or "merchantcircle.com" in url:
            provider = "business_directory"
        else:
            continue

        if provider in seen_providers:
            continue

        source_type = "directory"
        if provider in ("facebook", "news", "linkedin", "crunchbase", "healthgrades"):
            source_type = "directory"
        elif provider in ("yelp", "tripadvisor", "yellowpages", "business_directory"):
            source_type = "directory"

        sources.append({
            "source_id": f"web-{provider}-{len(sources)+1}",
            "source_type": source_type,
            "provider": provider,
            "query": target_product,
            "region": geography,
            "enabled": True,
            "auto_discovered": True,
            "api_required": False,
            "discovered_url": url,
            "discovered_title": title,
        })
        seen_providers.add(provider)

    if not sources:
        sources = discover_sources(market, geography, target_product)

    return {
        "result": "ok",
        "market": market,
        "geography": geography,
        "target_product": target_product,
        "sources_discovered": len(sources),
        "sources": sources,
        "market_type_detected": _detect_market_type(market, target_product),
    }


def build_parser() -> Any:
    import argparse
    parser = argparse.ArgumentParser(description="Auto-discover data sources for market research (no API keys required)")
    parser.add_argument("--market", required=True, help="Market name (e.g., Brisket, SaaS, Healthcare)")
    parser.add_argument("--geography", required=True, help="Geographic region (e.g., US, Austin TX)")
    parser.add_argument("--target-product", default=None, help="Product/service to search for")
    parser.add_argument("--max-sources", type=int, default=8, help="Max sources to discover")
    parser.add_argument("--use-websearch", action="store_true", help="Use web search for discovery")
    parser.add_argument("--root", default=".", help="Repository root path")
    parser.add_argument("--output-json", action="store_true", help="Output as JSON for pipeline use")
    return parser


def main() -> None:
    import json

    parser = build_parser()
    args = parser.parse_args()

    root_path = Path(args.root).resolve()
    load_project_env(root=root_path)

    if args.use_websearch:
        result = discover_sources_with_websearch(
            market=args.market,
            geography=args.geography,
            target_product=args.target_product,
            max_results=args.max_sources,
            root=root_path,
        )
    else:
        sources = discover_sources(
            market=args.market,
            geography=args.geography,
            target_product=args.target_product,
            max_sources=args.max_sources,
        )
        result = {
            "result": "ok",
            "market": args.market,
            "geography": args.geography,
            "target_product": args.target_product or args.market,
            "sources_discovered": len(sources),
            "sources": sources,
            "market_type_detected": _detect_market_type(args.market, args.target_product or args.market),
        }

    if args.output_json:
        print(json.dumps(result, ensure_ascii=True))
    else:
        print(json.dumps(result, ensure_ascii=True, indent=2))
        print(f"\nDiscovered {result['sources_discovered']} sources for {result.get('market_type_detected', 'default')} market.")

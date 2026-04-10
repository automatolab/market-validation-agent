from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from market_validation.environment import load_project_env

MARKET_SOURCE_TEMPLATES = {
    "restaurant": [
        {"source_type": "directory", "provider": "yelp", "query": "{query} {geography}"},
        {"source_type": "directory", "provider": "google_maps", "query": "{query} {geography}"},
        {"source_type": "search", "provider": "duckduckgo", "query": "{query} restaurant {geography}"},
    ],
    "retail": [
        {"source_type": "search", "provider": "duckduckgo", "query": "{query} store {geography}"},
        {"source_type": "directory", "provider": "yelp", "query": "{query} {geography}"},
    ],
    "default": [
        {"source_type": "search", "provider": "duckduckgo", "query": "{query} {geography}"},
    ],
}


def _detect_market_type(market: str, target_product: str) -> str:
    combined = f"{market} {target_product}".lower()
    restaurant_keywords = ["restaurant", "food", "cafe", "bbq", "grill", "pizza", "sandwich", "deli", "catering", "brisket"]
    retail_keywords = ["store", "shop", "retail", "outlet", "supermarket", "grocery"]

    for kw in restaurant_keywords:
        if kw in combined:
            return "restaurant"
    for kw in retail_keywords:
        if kw in combined:
            return "retail"
    return "default"


def discover_sources(
    market: str,
    geography: str,
    target_product: str | None = None,
    max_sources: int = 5,
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
        })
    return discovered


def discover_sources_with_websearch(
    market: str,
    geography: str,
    target_product: str | None = None,
    max_results: int = 10,
    root: str | Path = ".",
) -> dict[str, Any]:
    from websearch import websearch

    target_product = target_product or market

    queries = [
        f"{target_product} {geography} directory listings",
        f"best {target_product} {geography} review site",
        f"{target_product} {geography} yellow pages",
    ]

    all_results = []
    for query in queries[:3]:
        try:
            results = websearch(query=query, numResults=5)
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
        elif "google.com/maps" in url or "google.com/search" in url:
            provider = "google_maps"
        elif "tripadvisor.com" in url:
            provider = "tripadvisor"
        elif "facebook.com" in url:
            provider = "facebook"
        elif "bing.com" in url:
            provider = "bing"
        elif "duckduckgo" in url:
            provider = "duckduckgo"

        if provider in seen_providers:
            continue

        source_type = "directory" if provider in ("yelp", "tripadvisor", "google_maps") else "search"

        sources.append({
            "source_id": f"web-{provider}-{len(sources)+1}",
            "source_type": source_type,
            "provider": provider,
            "query": target_product,
            "region": geography,
            "enabled": True,
            "auto_discovered": True,
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
    }


def build_parser() -> Any:
    import argparse
    parser = argparse.ArgumentParser(description="Auto-discover data sources for market validation")
    parser.add_argument("--market", required=True, help="Market name (e.g., Brisket)")
    parser.add_argument("--geography", required=True, help="Geographic region (e.g., US, Austin TX)")
    parser.add_argument("--target-product", default=None, help="Product/service to search for")
    parser.add_argument("--max-sources", type=int, default=5, help="Max sources to discover")
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
        }

    if args.output_json:
        print(json.dumps(result, ensure_ascii=True))
    else:
        print(json.dumps(result, ensure_ascii=True, indent=2))
        print(f"\nDiscovered {result['sources_discovered']} sources. Add these to your config/source_configs to run research_ingest.")

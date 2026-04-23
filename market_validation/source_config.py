"""
Market-specific data source configurations.

Load sources from YAML files in the sources/ directory.
These are ADDITIONAL to the agent's built-in search capabilities.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:  # pragma: no cover - optional dependency
    yaml = None


DEFAULT_SOURCE_CONFIGS: dict[str, dict[str, Any]] = {
    "restaurants": {
        "name": "restaurants",
        "description": "Generic sources for restaurant market research",
        "search_queries": [
            "{product} restaurant {geography}",
            "best {product} {geography}",
            "{product} near {geography}",
        ],
        "directories": [
            {
                "name": "Yelp",
                "search_url": "https://www.yelp.com/search?find_desc={product}&find_loc={geography}",
                "type": "restaurant",
            },
            {
                "name": "TripAdvisor",
                "search_url": "https://www.tripadvisor.com/Search?q={product}+{geography}",
                "type": "restaurant",
            },
            {
                "name": "Google Maps",
                "search_url": "https://www.google.com/maps/search/{product}+Restaurant/{geography}",
                "type": "maps",
            },
        ],
    },
    "tech-saas": {
        "name": "tech-saas",
        "description": "Generic sources for SaaS market research",
        "search_queries": [
            "{product} SaaS company {geography}",
            "{product} software startup {geography}",
            "best {product} tools {geography}",
        ],
        "directories": [
            {
                "name": "Crunchbase",
                "search_url": "https://www.crunchbase.com/discover/organization.companies?query={product}",
                "type": "startup",
            },
            {
                "name": "Product Hunt",
                "search_url": "https://www.producthunt.com/search?q={product}",
                "type": "startup",
            },
        ],
    },
    "general": {
        "name": "general",
        "description": "Fallback source configuration for generic markets",
        "search_queries": [
            "{market} companies {geography}",
            "{market} businesses {geography}",
            "{product} providers {geography}",
            "{product} service {geography}",
        ],
        "directories": [
            {
                "name": "BBB",
                "search_url": "https://www.bbb.org/search?find_country=USA&find_loc={geography}&find_text={product}",
                "type": "directory",
            },
            {
                "name": "OpenCorporates",
                "search_url": "https://opencorporates.com/companies?q={product}+{geography}",
                "type": "registry",
            },
        ],
    },
}


def get_sources_dir() -> Path:
    """Get the sources directory path."""
    from market_validation.research import PROJECT_ROOT
    return PROJECT_ROOT / "sources"


def load_source_config(market_type: str | None = None) -> dict[str, Any]:
    """
    Load source configuration for a market type.

    Args:
        market_type: Market type (e.g., 'brisket-bbq', 'restaurants', 'tech-saas')
                    If None, tries to auto-detect from keywords.

    Returns:
        Source configuration dict
    """
    sources_dir = get_sources_dir()

    normalized = _normalize_market_key(market_type)
    if normalized:
        config_file = sources_dir / f"{normalized}.yaml"
        if config_file.exists():
            loaded = _load_yaml(config_file)
            if loaded:
                return loaded
        if normalized in DEFAULT_SOURCE_CONFIGS:
            return copy.deepcopy(DEFAULT_SOURCE_CONFIGS[normalized])

    # Try auto-detection
    market_lower = normalized

    # Keyword matching
    if any(kw in market_lower for kw in ["restaurant", "food", "cafe", "coffee", "dining", "bbq", "barbecue", "catering", "brewery", "winery", "bakery", "deli", "butcher"]):
        config_file = sources_dir / "restaurants.yaml"
        if config_file.exists():
            loaded = _load_yaml(config_file)
            if loaded:
                return loaded
        return copy.deepcopy(DEFAULT_SOURCE_CONFIGS["restaurants"])

    if any(kw in market_lower for kw in ["saas", "software", "tech", "startup"]):
        config_file = sources_dir / "tech-saas.yaml"
        if config_file.exists():
            loaded = _load_yaml(config_file)
            if loaded:
                return loaded
        return copy.deepcopy(DEFAULT_SOURCE_CONFIGS["tech-saas"])

    return copy.deepcopy(DEFAULT_SOURCE_CONFIGS["general"])


def _normalize_market_key(value: str | None) -> str:
    if not value:
        return ""
    text = str(value).strip().lower()
    text = " ".join(text.split())

    aliases = {
        "bbq": "restaurants",
        "barbecue": "restaurants",
        "brisket": "restaurants",
        "restaurant": "restaurants",
        "restaurants": "restaurants",
        "food": "restaurants",
        "saas": "tech-saas",
        "software": "tech-saas",
        "tech": "tech-saas",
    }
    for key, target in aliases.items():
        if key in text:
            return target

    return text.replace("/", "-").replace("_", "-").replace(" ", "-")


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load and parse a YAML file."""
    if yaml is None:
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def get_search_queries(config: dict[str, Any], market: str, geography: str, product: str | None = None) -> list[str]:
    """Generate search queries from config template."""
    queries = config.get("search_queries", [])
    product = product or market

    results = []
    for q in queries:
        q = q.replace("{market}", market)
        q = q.replace("{product}", product)
        q = q.replace("{geography}", geography)
        results.append(q)

    return results


def get_directory_urls(config: dict[str, Any], market: str, geography: str, product: str | None = None) -> list[dict[str, Any]]:
    """Generate directory URLs from config template."""
    directories = config.get("directories", [])
    product = product or market

    results = []
    for d in directories:
        url = d.get("search_url", "")
        if url:
            url = url.replace("{market}", market.replace(" ", "+"))
            url = url.replace("{product}", product.replace(" ", "+"))
            url = url.replace("{geography}", geography.replace(" ", "+"))
            results.append({
                "name": d.get("name", "Unknown"),
                "url": url,
                "type": d.get("type", "directory"),
            })

    return results


def get_direct_urls(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Get direct URLs to scrape from config."""
    return config.get("urls", [])


def list_available_sources() -> list[str]:
    """List all available source configurations."""
    sources_dir = get_sources_dir()
    configs = set(DEFAULT_SOURCE_CONFIGS.keys())
    if sources_dir.exists():
        for f in sources_dir.glob("*.yaml"):
            configs.add(f.stem)
    return sorted(configs)

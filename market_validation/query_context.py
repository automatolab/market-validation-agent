"""
Query Context — generates market-aware search queries for each validation module.

Detects market vertical from keywords and produces category-specific
query sets for sizing, demand, competition, and signals modules.
This ensures validation works correctly for B2C, B2B, SaaS, food,
healthcare, industrial, and general markets alike.
"""

from __future__ import annotations

_CATEGORY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "saas": ("saas", "software", "api", "platform", "cloud", "automation", "app", "tool"),
    "food": ("restaurant", "food", "bbq", "barbecue", "catering", "cafe", "coffee", "dining", "grocery", "produce", "beverage", "butcher", "deli", "bakery", "brewery", "winery"),
    "healthcare": ("clinic", "medical", "health", "dental", "hospital", "pharma", "wellness", "therapy"),
    "industrial": ("manufacturer", "manufacturing", "industrial", "factory", "supplier", "wholesale", "robot", "robotics", "drone", "aerospace", "defense", "semiconductor", "hardware", "logistics"),
    "services": ("agency", "consulting", "consultant", "legal", "accounting", "services", "staffing", "marketing"),
    "retail": ("retail", "store", "shop", "ecommerce", "consumer", "brand", "direct-to-consumer"),
}


def detect_market_category(market: str, product: str | None = None) -> str:
    """Detect market vertical from keywords.

    Returns one of: saas, food, healthcare, industrial, services, retail, general.
    """
    text = f"{market} {product or ''}".lower()
    for category, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return category
    return "general"


def get_validation_queries(
    market: str,
    geography: str,
    product: str | None = None,
    category: str | None = None,
) -> dict:
    """
    Return tailored search query sets for each validation module.

    Returns dict with keys: category, sizing, demand, competition, signals.
    - sizing: list of search query strings
    - demand: dict with "volume" (intent-keyed dict) and "community" (list)
    - competition: dict with "competitor" (list) and "funding" (list)
    - signals: dict with "jobs", "news_positive", "news_negative", "news_general",
                          "regulatory", "tech" (each a list)
    """
    if category is None:
        category = detect_market_category(market, product)

    search_term = product or market

    return {
        "category": category,
        "sizing": _sizing_queries(market, geography, search_term, category),
        "demand": _demand_queries(market, geography, search_term, category),
        "competition": _competition_queries(market, geography, search_term, category),
        "signals": _signals_queries(market, geography, search_term, category),
    }


# ---------------------------------------------------------------------------
# Module-specific query builders
# ---------------------------------------------------------------------------

def _sizing_queries(market: str, geography: str, search_term: str, category: str) -> list[str]:
    base = [
        f"{market} market size {geography}",
        f"{market} industry revenue total addressable market",
        f"{search_term} TAM market opportunity",
        f"site:census.gov {market} statistics",
        f"site:bls.gov {market} industry employment",
        f"{market} market report 2024 2025",
        f"{market} industry growth rate forecast",
    ]
    extra: dict[str, list[str]] = {
        "food": [
            f"food service industry {geography} market size",
            f"{search_term} wholesale distribution market",
            "restaurant supply chain market size United States",
        ],
        "saas": [
            f"{search_term} software market size worldwide",
            f"SaaS {search_term} total addressable market",
            f"cloud {search_term} industry revenue 2024",
        ],
        "healthcare": [
            f"{search_term} healthcare market size {geography}",
            f"medical {search_term} industry revenue United States",
            f"site:cms.gov {market} statistics spending",
        ],
        "industrial": [
            f"{search_term} manufacturing market size {geography}",
            f"{search_term} B2B supply chain market revenue",
            f"industrial {search_term} market global size forecast",
        ],
        "services": [
            f"{search_term} professional services market {geography}",
            f"{search_term} consulting industry revenue size",
        ],
        "retail": [
            f"{search_term} consumer market size {geography}",
            f"{search_term} retail industry revenue statistics",
        ],
    }
    return _unique(base + extra.get(category, []))


def _demand_queries(market: str, geography: str, search_term: str, category: str) -> dict:
    """Return demand query groups: volume (intent-keyed dict) and community (list)."""
    volume: dict[str, str] = {
        "general": f"{search_term} {geography}",
        "transactional": f"buy {search_term} {geography}",
        "alternative": f"{search_term} alternative",
        "comparison": f"best {search_term} {geography}",
    }
    extra_volume: dict[str, dict[str, str]] = {
        "food": {
            "supplier": f"{search_term} supplier {geography}",
            "wholesale": f"{search_term} wholesale {geography}",
        },
        "saas": {
            "pricing": f"{search_term} pricing plans",
            "review": f"{search_term} reviews users",
        },
        "healthcare": {
            "provider": f"{search_term} provider {geography}",
            "coverage": f"{search_term} insurance coverage cost",
        },
        "industrial": {
            "distributor": f"{search_term} distributor {geography}",
            "oem": f"{search_term} OEM manufacturer quote",
        },
        "services": {
            "hire": f"hire {search_term} {geography}",
            "freelance": f"{search_term} freelance consultant rates",
        },
        "retail": {
            "shop": f"shop {search_term} online {geography}",
            "price": f"{search_term} price cheap {geography}",
        },
    }
    volume.update(extra_volume.get(category, {}))

    community: list[str] = [
        f"site:reddit.com {search_term} recommendation",
        f"site:reddit.com {search_term} problem frustrating",
        f"{search_term} review complaint",
    ]
    extra_community: dict[str, list[str]] = {
        "food": [
            f"site:reddit.com restaurant {search_term} supplier quality problem",
            f"{search_term} food service shortage supply issue",
        ],
        "saas": [
            f"site:reddit.com {search_term} software missing features",
            f"{search_term} SaaS pain points users complain",
        ],
        "healthcare": [
            f"site:reddit.com {search_term} medical access frustrating",
            f"{search_term} patient complaint coverage",
        ],
        "industrial": [
            f"site:reddit.com {search_term} sourcing supply chain problem",
            f"{search_term} manufacturer quality defects",
        ],
    }
    community.extend(extra_community.get(category, []))

    return {"volume": volume, "community": community[:6]}


def _competition_queries(market: str, geography: str, search_term: str, category: str) -> dict:
    competitor_queries = [
        f"{market} companies {geography}",
        f"top {market} providers {geography}",
        f"{search_term} competitors market leaders",
        f"{market} startups {geography}",
    ]
    funding_queries = [
        f"{market} startup funding raised",
        f"{market} acquisition {geography}",
        f"site:crunchbase.com {market}",
    ]
    extra_competitor: dict[str, list[str]] = {
        "food": [
            f"{search_term} distributors {geography}",
            f"food service suppliers {geography} B2B",
            f"{search_term} wholesale supplier United States",
        ],
        "saas": [
            f"{search_term} SaaS companies alternatives",
            f"best {search_term} software platforms 2024",
            f"site:g2.com {search_term}",
        ],
        "healthcare": [
            f"{search_term} healthcare companies {geography}",
            f"medical {search_term} provider network",
        ],
        "industrial": [
            f"{search_term} industrial suppliers manufacturers {geography}",
            f"{search_term} B2B distribution companies",
        ],
        "services": [
            f"{search_term} agencies firms {geography}",
            f"best {search_term} consulting firms list",
        ],
        "retail": [
            f"{search_term} brands retailers {geography}",
            f"top {search_term} consumer brands",
        ],
    }
    extra_funding: dict[str, list[str]] = {
        "food": [
            f"food tech {market} funding investment",
            "food service startup acquisition 2024",
        ],
        "saas": [
            f"{search_term} SaaS funding series A B round",
            f"{search_term} software startup acquisition",
        ],
    }
    return {
        "competitor": _unique(competitor_queries + extra_competitor.get(category, [])),
        "funding": _unique(funding_queries + extra_funding.get(category, [])),
    }


def _signals_queries(market: str, geography: str, search_term: str, category: str) -> dict:
    jobs_queries = [
        f"site:indeed.com {market} {geography}",
        f"site:linkedin.com/jobs {market} {geography}",
        f"{market} hiring jobs {geography}",
    ]
    news_positive = [f"{market} growth expansion success {geography}"]
    news_negative = [f"{market} shutdown decline layoff struggling"]
    news_general = [f"{market} {geography} news 2025 2026"]
    regulatory_queries = [
        f"{market} regulation law {geography}",
        f"{market} compliance requirements new rules",
    ]
    tech_queries = [
        f"{market} technology innovation trend",
        f"{market} adoption growth emerging",
    ]
    extra: dict[str, dict[str, list[str]]] = {
        "food": {
            "jobs": [
                f"food service distributor driver jobs {geography}",
                f"{search_term} kitchen prep jobs {geography}",
            ],
            "regulatory": [
                f"food safety regulation {geography} 2024 2025",
                "USDA meat inspection requirements commercial",
            ],
            "tech": [
                "food delivery supply chain technology trend",
                f"restaurant tech automation {geography}",
            ],
        },
        "saas": {
            "jobs": [
                f"{search_term} software engineer developer jobs",
                "SaaS product manager hiring 2024",
            ],
            "regulatory": [
                f"{search_term} data privacy GDPR CCPA compliance",
                "SaaS software regulation United States",
            ],
            "tech": [
                f"AI {search_term} automation trend 2024",
                f"{search_term} open source alternatives emerging",
            ],
        },
        "healthcare": {
            "jobs": [
                f"healthcare {search_term} jobs {geography}",
                f"medical {search_term} technician hiring",
            ],
            "regulatory": [
                f"HIPAA {search_term} compliance requirements",
                f"FDA {search_term} regulation approval pathway",
            ],
            "tech": [
                f"digital health {search_term} innovation trend",
                f"telemedicine {search_term} adoption growth",
            ],
        },
        "industrial": {
            "jobs": [
                f"manufacturing {search_term} jobs {geography}",
                f"supply chain logistics {search_term} hiring",
            ],
            "regulatory": [
                f"{search_term} industrial safety OSHA regulation",
                f"trade tariff import {search_term} United States",
            ],
            "tech": [
                f"automation robotics {search_term} Industry 4.0",
                f"{search_term} IoT sensor technology trend",
            ],
        },
    }
    cat = extra.get(category, {})
    return {
        "jobs": _unique(jobs_queries + cat.get("jobs", [])),
        "news_positive": news_positive,
        "news_negative": news_negative,
        "news_general": news_general,
        "regulatory": _unique(regulatory_queries + cat.get("regulatory", [])),
        "tech": _unique(tech_queries + cat.get("tech", [])),
    }


def _unique(lst: list[str]) -> list[str]:
    seen: set[str] = set()
    result = []
    for item in lst:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result

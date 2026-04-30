"""Search-query construction and multi-backend search wrappers.

Split across three concerns:
  - Wrappers that dispatch to ``multi_search`` / ``source_config`` backends.
  - Heuristic query generation (primary, retry, contact-retry).
  - Adjacent-profile fallback queries for when the primary category underperforms.
"""

from __future__ import annotations

from typing import Any

from market_validation._helpers.common import infer_market_profile, unique_in_order
from market_validation._helpers.contacts import (
    extract_email_text,
    extract_phone_text,
    normalize_name_key,
)
from market_validation._helpers.quality import is_useful_business_url
from market_validation.log import get_logger

_log = get_logger("queries")


# ── Backend wrappers ─────────────────────────────────────────────────────────

def try_multi_search(query: str, num_results: int = 10, geography: str | None = None) -> list[dict[str, str]]:
    """
    Try direct search using multi-backend search.
    Falls back to empty list if all backends fail.
    When *geography* is provided, geo-aware backends constrain results to that area.
    """
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results, geography=geography)
    except Exception as exc:
        # All backends failed — not uncommon during rate-limit bursts.
        # Debug level so we don't spam normal runs; upstream callers treat
        # an empty list as "try the next source".
        _log.debug("multi_search quick_search failed for %r: %s", query, exc)
        return []


def try_supplementary_search(
    query: str,
    num_results: int = 10,
    geography: str | None = None,
) -> list[dict[str, str]]:
    """Run slow scraped backends (BBB, Manta, etc.), routed by geography.

    BBB / Manta are US-only and get skipped automatically when *geography*
    indicates a non-US country, saving 5-10 seconds per call on
    international markets.
    """
    try:
        from market_validation.multi_search import supplementary_search
        return supplementary_search(query, num_results, geography=geography)
    except Exception as exc:
        _log.debug("supplementary_search failed for %r: %s", query, exc)
        return []


def try_source_urls(market: str, geography: str, product: str | None = None) -> list[dict[str, Any]]:
    """Scrape URLs from market-specific source config."""
    results = []

    try:
        from market_validation.source_config import (
            get_direct_urls,
            get_directory_urls,
            get_search_queries,
            load_source_config,
        )

        try:
            from market_validation.web_scraper import quick_scrape
        except ImportError as exc:
            _log.warning("web_scraper import failed, skipping scrape step: %s", exc)
            quick_scrape = None

        config = load_source_config(market)
        if not config:
            return []

        directories = get_directory_urls(config, market, geography, product)
        direct_urls = get_direct_urls(config)
        for d in direct_urls:
            url = (
                (d.get("url") or "")
                .replace("{market}", market.replace(" ", "+"))
                .replace("{product}", (product or market).replace(" ", "+"))
                .replace("{geography}", geography.replace(" ", "+"))
            )
            if url:
                directories.append({"name": d.get("name", "direct"), "url": url, "type": d.get("type", "directory")})

        if quick_scrape is not None:
            for d in directories[:4]:  # Limit to avoid rate limits
                try:
                    scrape_result = quick_scrape(d["url"])
                    if scrape_result and not scrape_result.get("error"):
                        results.append({
                            "source": d["name"],
                            "type": d["type"],
                            "data": scrape_result,
                        })
                except Exception as exc:
                    # Per-directory scrape failures are best-effort; log and
                    # move on so one broken directory doesn't kill the batch.
                    _log.debug("source_urls: scrape failed for %s: %s", d.get("url"), exc)
                    continue

        queries = get_search_queries(config, market, geography, product)
        if product and product.strip().lower() != market.strip().lower():
            queries.extend(get_search_queries(config, market, geography, market))
        queries = list(dict.fromkeys(queries))
        for query in queries[:3]:
            search_results = try_multi_search(query, 5, geography=geography)
            for r in search_results:
                results.append({
                    "source": r.get("source", "search"),
                    "type": "search",
                    "data": r,
                })

    except Exception as exc:
        # source_config is optional — a bad/missing config file shouldn't
        # abort find(); log so the broken config is discoverable.
        _log.warning("try_source_urls failed for market=%r: %s", market, exc)

    return results


# ── Product broadening ───────────────────────────────────────────────────────

def broaden_product_to_business_types(product: str | None, market: str, category: str) -> list[str]:
    """Turn a niche product term into broader business-type terms that search
    backends (Nominatim, directories) can actually find.

    Works across all categories — food, saas, healthcare, industrial, services, general.
    """
    broad: list[str] = []
    if not product:
        return broad
    p = product.lower().strip()
    m = market.lower().strip()

    _food_map: dict[str, list[str]] = {
        "brisket": ["BBQ restaurant", "barbecue restaurant", "smokehouse", "BBQ"],
        "pulled pork": ["BBQ restaurant", "barbecue restaurant", "smokehouse"],
        "ribs": ["BBQ restaurant", "barbecue restaurant", "rib house"],
        "smoked meat": ["BBQ restaurant", "smokehouse", "barbecue"],
        "wings": ["wing restaurant", "sports bar", "chicken restaurant"],
        "pizza": ["pizza restaurant", "pizzeria"],
        "sushi": ["sushi restaurant", "Japanese restaurant"],
        "tacos": ["taco restaurant", "Mexican restaurant", "taqueria"],
        "ramen": ["ramen restaurant", "Japanese restaurant", "noodle shop"],
        "pho": ["pho restaurant", "Vietnamese restaurant"],
        "burger": ["burger restaurant", "hamburger restaurant", "grill"],
        "steak": ["steakhouse", "steak restaurant", "grill"],
        "seafood": ["seafood restaurant", "fish market"],
        "bagel": ["bagel shop", "bakery", "deli"],
        "donut": ["donut shop", "bakery"],
        "croissant": ["bakery", "pastry shop", "cafe"],
        "espresso": ["coffee shop", "cafe", "espresso bar"],
        "juice": ["juice bar", "smoothie shop", "cafe"],
        "ice cream": ["ice cream shop", "creamery", "dessert shop"],
        "acai": ["acai bowl shop", "smoothie shop", "health food"],
    }

    _saas_map: dict[str, list[str]] = {
        "crm": ["CRM software company", "sales software", "customer management platform"],
        "erp": ["ERP software company", "enterprise software", "business management software"],
        "analytics": ["analytics platform", "data analytics company", "business intelligence"],
        "chatbot": ["chatbot company", "conversational AI", "customer support software"],
        "email marketing": ["email marketing platform", "marketing automation", "newsletter software"],
        "project management": ["project management software", "task management tool", "collaboration platform"],
        "accounting": ["accounting software", "bookkeeping software", "financial software"],
        "payroll": ["payroll software", "HR software", "workforce management"],
        "scheduling": ["scheduling software", "appointment booking", "calendar software"],
        "invoicing": ["invoicing software", "billing platform", "payment software"],
        "ecommerce": ["ecommerce platform", "online store builder", "shopping cart software"],
    }

    _healthcare_map: dict[str, list[str]] = {
        "dental implant": ["dental clinic", "implant dentist", "oral surgery"],
        "braces": ["orthodontist", "dental clinic", "orthodontic practice"],
        "physical therapy": ["physical therapy clinic", "rehabilitation center", "PT practice"],
        "chiropractic": ["chiropractor", "chiropractic clinic", "spine clinic"],
        "dermatology": ["dermatology clinic", "skin care clinic", "dermatologist"],
        "optometry": ["optometrist", "eye clinic", "vision center"],
        "mental health": ["mental health clinic", "therapy practice", "counseling center"],
        "fertility": ["fertility clinic", "IVF center", "reproductive health"],
        "veterinary": ["veterinary clinic", "animal hospital", "vet"],
        "pharmacy": ["pharmacy", "drugstore", "compounding pharmacy"],
    }

    _industrial_map: dict[str, list[str]] = {
        "pcb": ["PCB manufacturer", "circuit board company", "electronics manufacturer"],
        "cnc": ["CNC machining", "machine shop", "precision manufacturing"],
        "3d print": ["3D printing service", "additive manufacturing", "prototyping company"],
        "injection mold": ["injection molding company", "plastics manufacturer", "mold maker"],
        "steel": ["steel supplier", "metal fabricator", "steel manufacturer"],
        "bearing": ["bearing manufacturer", "bearing supplier", "industrial parts"],
        "valve": ["valve manufacturer", "valve supplier", "industrial equipment"],
        "sensor": ["sensor manufacturer", "IoT hardware", "electronics company"],
        "drone": ["drone manufacturer", "UAV company", "drone service"],
        "solar panel": ["solar panel manufacturer", "solar company", "renewable energy"],
        "battery": ["battery manufacturer", "energy storage company", "battery supplier"],
        # Agritech / commercial growers — buyers of automation hardware
        "hydroponic": ["commercial hydroponic farm", "hydroponic grower",
                       "vertical farm", "indoor farm", "greenhouse operation"],
        "vertical farm": ["vertical farm", "indoor vertical farm",
                          "controlled environment agriculture", "indoor agriculture"],
        "greenhouse": ["commercial greenhouse", "greenhouse grower",
                       "greenhouse operation", "greenhouse nursery"],
        "indoor farm": ["indoor farm", "vertical farm", "indoor agriculture",
                        "controlled environment agriculture"],
        "controlled environment": ["controlled environment agriculture",
                                   "indoor farm", "vertical farm",
                                   "commercial greenhouse"],
        "cannabis cultivation": ["cannabis cultivator", "licensed cannabis grower",
                                 "cannabis greenhouse", "indoor cannabis farm"],
        "irrigation": ["commercial irrigation company", "irrigation contractor",
                       "fertigation systems integrator", "agricultural irrigation"],
        "fertigation": ["fertigation systems integrator", "commercial greenhouse",
                        "controlled environment agriculture"],
        "grow light": ["commercial greenhouse", "indoor farm",
                       "controlled environment agriculture", "horticultural lighting dealer"],
        # Industrial automation / control systems — both buyers and integrators
        "automation system": ["industrial automation integrator",
                              "control systems integrator", "automation contractor",
                              "manufacturing facility", "processing plant"],
        "control system": ["control systems integrator", "SCADA integrator",
                           "industrial automation company", "PLC integrator"],
        "iot platform": ["industrial IoT integrator", "IIoT systems company",
                         "automation systems integrator"],
        "scada": ["SCADA integrator", "industrial control systems company",
                  "automation contractor"],
        "plc": ["PLC integrator", "industrial automation company",
                "control systems integrator"],
    }

    _services_map: dict[str, list[str]] = {
        "seo": ["SEO agency", "digital marketing agency", "search marketing firm"],
        "web design": ["web design agency", "web development company", "digital agency"],
        "branding": ["branding agency", "brand design firm", "creative agency"],
        "tax": ["tax preparation", "CPA firm", "accounting firm"],
        "legal": ["law firm", "legal services", "attorney"],
        "insurance": ["insurance agency", "insurance broker", "insurance company"],
        "real estate": ["real estate agency", "realtor", "property management"],
        "landscaping": ["landscaping company", "lawn care service", "garden service"],
        "plumbing": ["plumber", "plumbing company", "plumbing service"],
        "electrical": ["electrician", "electrical contractor", "electrical service"],
        "hvac": ["HVAC company", "heating and cooling", "air conditioning service"],
        "cleaning": ["cleaning service", "janitorial service", "commercial cleaning"],
        "moving": ["moving company", "movers", "relocation service"],
    }

    category_maps: dict[str, dict[str, list[str]]] = {
        "food": _food_map,
        "saas": _saas_map,
        "healthcare": _healthcare_map,
        "industrial": _industrial_map,
        "services": _services_map,
    }

    cat_map = category_maps.get(category, {})
    for key, terms in cat_map.items():
        if key in p:
            broad.extend(terms)
            return broad

    if not broad:
        for _cat, _map in category_maps.items():
            if _cat == category:
                continue
            for key, terms in _map.items():
                if key in p:
                    broad.extend(terms)
                    return broad

    if not broad and m != p:
        broad.append(market)

    return broad


# ── Query generation ─────────────────────────────────────────────────────────

def primary_queries(market: str, geography: str, product: str | None) -> list[str]:
    search_term = product or market
    profile = infer_market_profile(market, product)
    category = profile["category"]

    queries = [
        f"{search_term} {geography}",
        f"{market} {geography}",
        f"{search_term} companies {geography}",
        f"{search_term} providers {geography}",
    ]

    broadened = broaden_product_to_business_types(product, market, category)
    for broad_term in broadened:
        queries.insert(1, f"{broad_term} {geography}")
    for broad_term in broadened:
        queries.append(f"{broad_term} near {geography}")

    if category == "food":
        queries.extend([
            f"{search_term} {geography} restaurant",
            f"best {search_term} {geography}",
            f"{search_term} catering {geography}",
        ])
    elif category == "saas":
        queries.extend([
            f"{search_term} saas {geography}",
            f"{search_term} software companies {geography}",
            f"best {search_term} tools {geography}",
        ])
    elif category == "healthcare":
        queries.extend([
            f"{search_term} clinics {geography}",
            f"{search_term} medical providers {geography}",
        ])
    elif category == "industrial":
        queries.extend([
            f"{search_term} companies {geography}",
            f"{search_term} manufacturers {geography}",
            f"{search_term} startups {geography}",
            f"{search_term} firms {geography}",
        ])
    elif category == "services":
        queries.extend([
            f"{search_term} agencies {geography}",
            f"{search_term} consulting firms {geography}",
        ])

    queries.extend([
        f"{search_term} near {geography}",
        f"{search_term} owner {geography}",
        f"{search_term} business contact {geography}",
        f"{search_term} local business {geography}",
    ])

    return unique_in_order([q.strip() for q in queries if q.strip()])


def build_retry_queries(market: str, geography: str, product: str | None) -> list[str]:
    search_term = product or market
    profile = infer_market_profile(market, product)
    category = profile["category"]
    retries = [
        f"{search_term} near {geography}",
        f"{search_term} business {geography}",
        f"{market} companies {geography}",
        f"best {search_term} in {geography}",
    ]

    if category == "food":
        retries.extend([
            f"{search_term} catering {geography}",
            f"{search_term} restaurant {geography}",
        ])
    elif category == "saas":
        retries.extend([
            f"{search_term} saas {geography}",
            f"{search_term} software companies {geography}",
            f"{search_term} startup {geography}",
            f"{search_term} platform {geography}",
        ])
    elif category == "healthcare":
        retries.extend([
            f"{search_term} clinic {geography}",
            f"{search_term} medical practice {geography}",
            f"{search_term} healthcare provider {geography}",
        ])
    elif category == "industrial":
        retries.extend([
            f"{search_term} manufacturer {geography}",
            f"{search_term} company {geography}",
            f"{search_term} startup {geography}",
            f"{search_term} firm {geography}",
        ])
    elif category == "services":
        retries.extend([
            f"{search_term} agency {geography}",
            f"{search_term} consulting firm {geography}",
            f"{search_term} professional services {geography}",
        ])

    return unique_in_order([q.strip() for q in retries if q.strip()])


def build_contact_retry_queries(
    companies: list[dict[str, Any]], geography: str, max_companies: int = 6
) -> list[str]:
    targets = [
        c for c in companies
        if not str(c.get("website") or "").strip() or not str(c.get("phone") or "").strip()
    ][: max(1, max_companies)]

    queries: list[str] = []
    for c in targets:
        name = str(c.get("company_name") or "").strip()
        if not name:
            continue
        queries.append(f"{name} {geography} contact phone email")
        if not str(c.get("website") or "").strip():
            queries.append(f"{name} {geography} official website")
    return unique_in_order(queries)


def apply_contact_retry_rows(
    companies: list[dict[str, Any]], rows: list[dict[str, str]]
) -> tuple[list[dict[str, Any]], int]:
    if not companies or not rows:
        return companies, 0

    import re

    indexed: dict[str, int] = {}
    for idx, c in enumerate(companies):
        key = normalize_name_key(c.get("company_name") or "")
        if key:
            indexed[key] = idx

    updates = 0
    for row in rows:
        title = str(row.get("title") or "")
        row_key = normalize_name_key(title)
        if not row_key:
            continue

        match_idx = indexed.get(row_key)
        if match_idx is None:
            for name_key, idx in indexed.items():
                if row_key in name_key or name_key in row_key:
                    match_idx = idx
                    break
        if match_idx is None:
            continue

        c = companies[match_idx]
        changed = False

        candidate_url = str(row.get("url") or "").strip()
        if is_useful_business_url(candidate_url) and not str(c.get("website") or "").strip():
            c["website"] = candidate_url
            c["evidence_url"] = c.get("evidence_url") or candidate_url
            changed = True

        if not str(c.get("phone") or "").strip():
            phone = extract_phone_text(str(row.get("snippet") or ""))
            if phone:
                c["phone"] = phone
                changed = True

        # Extract email from snippet (Nominatim extratags or scraped text).
        # Gate through is_plausible_email so junk like "info@mail.loc" and
        # aggregator-domain emails don't slip in.
        if not str(c.get("email") or "").strip():
            from market_validation.company_enrichment import is_plausible_email
            snippet = str(row.get("snippet") or "")
            email_match = re.search(r"email=([^\s|]+)", snippet)
            candidate: str | None = None
            if email_match:
                candidate = email_match.group(1)
            else:
                candidate = extract_email_text(snippet)
            if candidate and is_plausible_email(candidate):
                c["email"] = candidate
                changed = True

        if changed:
            c["source"] = c.get("source") or row.get("source") or "contact_retry"
            updates += 1

    return companies, updates


# ── Adjacent-profile fallback ────────────────────────────────────────────────

# Adjacent profiles to try when the primary profile underperforms.
# Ordered by likelihood: more specific neighbours first, general last.
ADJACENT_PROFILES: dict[str, list[str]] = {
    "food": ["services", "general"],
    "saas": ["services", "general"],
    "healthcare": ["services", "general"],
    "industrial": ["services", "general"],
    "services": ["general"],
    "general": [],
}


def queries_for_adjacent_profile(
    market: str, geography: str, product: str | None, category: str
) -> list[str]:
    """Return search queries shaped for an adjacent category."""
    search_term = product or market
    base = [
        f"{search_term} {geography}",
        f"{search_term} business {geography}",
    ]
    extra: dict[str, list[str]] = {
        "food": [f"{search_term} {geography} restaurant", f"best {search_term} {geography}"],
        "saas": [f"{search_term} software companies {geography}", f"{search_term} tools {geography}"],
        "healthcare": [f"{search_term} clinics {geography}", f"{search_term} providers {geography}"],
        "industrial": [f"{search_term} manufacturers {geography}", f"{search_term} suppliers {geography}"],
        "services": [f"{search_term} agencies {geography}", f"{search_term} firms {geography}"],
        "general": [f"{search_term} companies {geography}", f"{market} businesses {geography}"],
    }
    return unique_in_order(base + extra.get(category, []))

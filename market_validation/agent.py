"""
Market Research Agent - Simple 3-step pipeline:

1. find()      - Discover companies in a market
2. qualify()   - Score and rank them  
3. enrich()    - Find contact info (8 sources)

Usage:
    from market_validation.agent import Agent
    
    agent = Agent(research_id="<research_id>")
    
    # Step 1: Find companies
    agent.find("<market>", "<geography>")
    
    # Step 2: Qualify (AI assessment)
    agent.qualify()
    
    # Step 3: Enrich contact info
    agent.enrich("<company_name>")
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def _try_multi_search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    """
    Try direct search using multi-backend search.
    Falls back to empty list if all backends fail.
    """
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results)
    except Exception:
        return []


def _summarize_backends(rows: list[dict[str, str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        backend = str(row.get("source", "unknown"))
        counts[backend] = counts.get(backend, 0) + 1
    return counts


def _tokenize_text(text: str) -> list[str]:
    return [t for t in re.split(r"\W+", text.lower()) if len(t) >= 3]


def _infer_market_profile(market: str, product: str | None) -> dict[str, Any]:
    text = f"{market} {product or ''}".lower()
    tokens = set(_tokenize_text(text))

    _category_kw_map: dict[str, tuple[str, ...]] = {
        "saas": ("saas", "software", "api", "platform", "cloud", "automation"),
        "food": ("restaurant", "food", "bbq", "barbecue", "catering", "cafe", "coffee", "dining"),
        "healthcare": ("clinic", "medical", "health", "dental", "hospital", "pharma"),
        "industrial": ("manufacturer", "manufacturing", "industrial", "factory", "supplier", "wholesale"),
        "services": ("agency", "consulting", "consultant", "legal", "accounting", "services"),
    }

    if any(t in text for t in _category_kw_map["saas"]):
        category = "saas"
    elif any(t in text for t in _category_kw_map["food"]):
        category = "food"
    elif any(t in text for t in _category_kw_map["healthcare"]):
        category = "healthcare"
    elif any(t in text for t in _category_kw_map["industrial"]):
        category = "industrial"
    elif any(t in text for t in _category_kw_map["services"]):
        category = "services"
    else:
        category = "general"

    # Confidence: how strongly the input signals this category
    if category == "general":
        confidence = 30  # fell through to default
    else:
        match_count = sum(1 for kw in _category_kw_map[category] if kw in text)
        confidence = min(100, 40 + match_count * 25)

    positive_by_category: dict[str, set[str]] = {
        "food": {"restaurant", "dining", "catering", "grill", "kitchen", "eatery", "bbq", "barbecue", "smokehouse"},
        "saas": {"saas", "software", "platform", "api", "cloud", "automation", "tool", "solution", "app"},
        "healthcare": {"clinic", "medical", "health", "hospital", "dental", "care", "provider"},
        "industrial": {"manufacturer", "manufacturing", "industrial", "supplier", "factory", "distributor"},
        "services": {"services", "agency", "consulting", "consultant", "firm", "provider"},
        "general": {"company", "business", "provider", "services"},
    }

    blocked_tokens = {"list of", "wikipedia"}
    if category == "food":
        blocked_tokens.update({"season", "episode", "joey chestnut", "chopped", "man v. food"})

    banned_name_tokens: set[str] = set()
    # Keep these exclusions narrow to brisket-focused research only
    if "brisket" in text:
        banned_name_tokens.update({"korean", "hot pot", "pho", "lechon"})

    return {
        "category": category,
        "confidence": confidence,
        "tokens": tokens,
        "positive_tokens": positive_by_category.get(category, set()),
        "blocked_tokens": blocked_tokens,
        "banned_name_tokens": banned_name_tokens,
    }


# Adjacent profiles to try when the primary profile underperforms.
# Ordered by likelihood: more specific neighbours first, general last.
_ADJACENT_PROFILES: dict[str, list[str]] = {
    "food": ["services", "general"],
    "saas": ["services", "general"],
    "healthcare": ["services", "general"],
    "industrial": ["services", "general"],
    "services": ["general"],
    "general": [],  # already the broadest fallback
}


def _queries_for_adjacent_profile(
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
    return _unique_in_order(base + extra.get(category, []))


def _try_source_urls(market: str, geography: str, product: str | None = None) -> list[dict[str, Any]]:
    """
    Scrape URLs from market-specific source config.
    """
    results = []
    
    try:
        from market_validation.source_config import load_source_config, get_directory_urls, get_search_queries, get_direct_urls

        try:
            from market_validation.web_scraper import quick_scrape
        except Exception:
            quick_scrape = None
        
        config = load_source_config(market)
        if not config:
            return []
        
        # Get directory URLs from config
        directories = get_directory_urls(config, market, geography, product)
        direct_urls = get_direct_urls(config)
        for d in direct_urls:
            url = (d.get("url") or "").replace("{market}", market.replace(" ", "+")).replace("{product}", (product or market).replace(" ", "+")).replace("{geography}", geography.replace(" ", "+"))
            if url:
                directories.append({"name": d.get("name", "direct"), "url": url, "type": d.get("type", "directory")})
        
        # Best-effort scraping from configured directories/URLs
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
                except Exception:
                    continue
        
        # Also do search queries from config
        queries = get_search_queries(config, market, geography, product)
        if product and product.strip().lower() != market.strip().lower():
            queries.extend(get_search_queries(config, market, geography, market))
        queries = list(dict.fromkeys(queries))
        for query in queries[:3]:
            search_results = _try_multi_search(query, 5)
            for r in search_results:
                results.append({
                    "source": r.get("source", "search"),
                    "type": "search",
                    "data": r,
                })
        
    except Exception:
        pass
    
    return results


def _iso_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _normalize_companies(companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for c in companies:
        normalized.append(
            {
                "company_name": c.get("company_name") or c.get("name") or c.get("title", "Unknown"),
                "website": c.get("website") or c.get("url", ""),
                "location": c.get("location") or c.get("address", ""),
                "phone": c.get("phone", ""),
                "description": c.get("description") or c.get("specialty", "") or c.get("notes", ""),
                "evidence_url": c.get("evidence_url") or c.get("url", ""),
                "source": c.get("source", "unknown"),
            }
        )
    return normalized


def _dedupe_companies(companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for c in companies:
        key = (
            c.get("website")
            or c.get("evidence_url")
            or " ".join(str(c.get("company_name", "")).strip().lower().split())
        )
        if not key:
            continue
        if key not in deduped:
            deduped[key] = c
    return list(deduped.values())


def _filter_relevant_companies(
    companies: list[dict[str, Any]],
    market: str,
    product: str | None,
) -> list[dict[str, Any]]:
    profile = _infer_market_profile(market, product)
    key_tokens = set(profile["tokens"]) | set(profile["positive_tokens"])
    blocked_tokens = set(profile["blocked_tokens"])
    banned_name_tokens = set(profile["banned_name_tokens"])

    filtered: list[dict[str, Any]] = []
    for c in companies:
        company_name = str(c.get("company_name", "")).strip()
        hay = " ".join(
            str(c.get(field, "")).lower()
            for field in ("company_name", "description", "location", "website", "evidence_url")
        )
        low_name = company_name.lower()
        if banned_name_tokens and any(tok in low_name for tok in banned_name_tokens):
            continue
        if c.get("source") == "city_directory" and str(c.get("company_name", "")).strip().lower() == "restaurants":
            continue
        if any(bt in hay for bt in blocked_tokens):
            continue
        if c.get("source") == "wikipedia":
            continue
        if any(token in hay for token in key_tokens):
            filtered.append(c)
    return filtered


def _unique_in_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _normalize_name_key(value: str) -> str:
    tokens = [t for t in re.split(r"\W+", str(value or "").lower()) if len(t) >= 2]
    return " ".join(tokens)


def _extract_phone_text(value: str) -> str:
    match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", str(value or ""))
    return match.group(0) if match else ""


def _is_useful_business_url(url: str) -> bool:
    if not url:
        return False
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False
    if not host or "." not in host:
        return False
    blocked_hosts = {
        "wikipedia.org",
        "www.wikipedia.org",
        "bbb.org",
        "www.bbb.org",
        "opencorporates.com",
        "www.opencorporates.com",
        "google.com",
        "www.google.com",
        "yelp.com",
        "www.yelp.com",
        "tripadvisor.com",
        "www.tripadvisor.com",
        "yellowpages.com",
        "www.yellowpages.com",
        "sanjose.org",
        "www.sanjose.org",
    }
    if host in blocked_hosts:
        return False
    return True


def _build_contact_retry_queries(
    companies: list[dict[str, Any]], geography: str, max_companies: int = 4
) -> list[str]:
    targets = [
        c
        for c in companies
        if not str(c.get("website") or "").strip() or not str(c.get("phone") or "").strip()
    ][: max(1, max_companies)]

    queries: list[str] = []
    for c in targets:
        name = str(c.get("company_name") or "").strip()
        if not name:
            continue
        queries.append(f"{name} {geography} official website")
        if not str(c.get("phone") or "").strip():
            queries.append(f"{name} {geography} phone")
        queries.append(f"{name} {geography} contact")
    return _unique_in_order(queries)


def _apply_contact_retry_rows(
    companies: list[dict[str, Any]], rows: list[dict[str, str]]
) -> tuple[list[dict[str, Any]], int]:
    if not companies or not rows:
        return companies, 0

    indexed: dict[str, int] = {}
    for idx, c in enumerate(companies):
        key = _normalize_name_key(c.get("company_name") or "")
        if key:
            indexed[key] = idx

    updates = 0
    for row in rows:
        title = str(row.get("title") or "")
        row_key = _normalize_name_key(title)
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
        if _is_useful_business_url(candidate_url) and not str(c.get("website") or "").strip():
            c["website"] = candidate_url
            c["evidence_url"] = c.get("evidence_url") or candidate_url
            changed = True

        if not str(c.get("phone") or "").strip():
            phone = _extract_phone_text(str(row.get("snippet") or ""))
            if phone:
                c["phone"] = phone
                changed = True

        if changed:
            c["source"] = c.get("source") or row.get("source") or "contact_retry"
            updates += 1

    return companies, updates


def _primary_queries(market: str, geography: str, product: str | None) -> list[str]:
    search_term = product or market
    profile = _infer_market_profile(market, product)
    category = profile["category"]

    queries = [
        f"{search_term} {geography}",
        f"{market} {geography}",
        f"{search_term} companies {geography}",
        f"{search_term} providers {geography}",
    ]

    if category == "food":
        queries.extend(
            [
                f"{search_term} {geography} restaurant",
                f"best {search_term} {geography}",
                f"{search_term} catering {geography}",
            ]
        )
    elif category == "saas":
        queries.extend(
            [
                f"{search_term} saas {geography}",
                f"{search_term} software companies {geography}",
                f"best {search_term} tools {geography}",
            ]
        )
    elif category == "healthcare":
        queries.extend(
            [
                f"{search_term} clinics {geography}",
                f"{search_term} medical providers {geography}",
            ]
        )
    elif category == "industrial":
        queries.extend(
            [
                f"{search_term} manufacturers {geography}",
                f"{search_term} suppliers {geography}",
            ]
        )
    elif category == "services":
        queries.extend(
            [
                f"{search_term} agencies {geography}",
                f"{search_term} consulting firms {geography}",
            ]
        )

    return _unique_in_order([q.strip() for q in queries if q.strip()])


def _opencode_hints(market: str, geography: str, product: str | None) -> tuple[str, str]:
    search_term = product or market
    category = _infer_market_profile(market, product)["category"]

    if category == "food":
        return (
            "Yelp, Google Maps, YellowPages, TripAdvisor, official business websites",
            f'"{search_term} {geography}", "best {search_term} {geography}", "{search_term} catering {geography}"',
        )
    if category == "saas":
        return (
            "official company websites, Product Hunt, Crunchbase, G2 listings, LinkedIn company pages",
            f'"{search_term} saas {geography}", "{search_term} software company {geography}", "{search_term} startup {geography}"',
        )
    if category == "healthcare":
        return (
            "healthcare provider directories, official practice websites, local business listings",
            f'"{search_term} clinic {geography}", "{search_term} medical provider {geography}"',
        )
    if category == "industrial":
        return (
            "manufacturer/supplier directories, official company websites, business registries",
            f'"{search_term} manufacturer {geography}", "{search_term} supplier {geography}"',
        )
    if category == "services":
        return (
            "agency/freelancer directories, official firm websites, local business listings",
            f'"{search_term} agency {geography}", "{search_term} consulting firm {geography}"',
        )

    return (
        "official company websites, business directories, local listings",
        f'"{search_term} {geography}", "{market} companies {geography}"',
    )


def _find_quality_metrics(companies: list[dict[str, Any]]) -> dict[str, int]:
    total = len(companies)
    with_website = 0
    with_phone = 0
    with_location = 0
    for c in companies:
        if str(c.get("website") or c.get("evidence_url") or "").strip():
            with_website += 1
        if str(c.get("phone") or "").strip():
            with_phone += 1
        if str(c.get("location") or "").strip():
            with_location += 1
    return {
        "total": total,
        "with_website": with_website,
        "with_phone": with_phone,
        "with_location": with_location,
    }


def _has_contact_form_or_email_domain(url: str) -> bool:
    if not url:
        return False
    low = url.lower()
    return any(tok in low for tok in ("contact", "about", "support", "help"))


def _contactability_score(companies: list[dict[str, Any]]) -> dict[str, Any]:
    score_total = 0
    max_total = max(1, len(companies) * 7)

    website_count = 0
    phone_count = 0
    valid_domain_count = 0
    contact_hint_count = 0

    for c in companies:
        website = str(c.get("website") or c.get("evidence_url") or "").strip()
        phone = str(c.get("phone") or "").strip()

        if website:
            website_count += 1
            score_total += 2
            try:
                host = (urlparse(website).netloc or "").lower()
                if host and host not in {"", "wikipedia.org", "www.wikipedia.org", "bbb.org", "www.bbb.org", "opencorporates.com", "www.opencorporates.com"}:
                    valid_domain_count += 1
                    score_total += 2
            except Exception:
                pass

            if _has_contact_form_or_email_domain(website):
                contact_hint_count += 1
                score_total += 1

        if phone:
            phone_count += 1
            score_total += 2

    score_100 = int(round((score_total / max_total) * 100))
    score_100 = max(0, min(100, score_100))
    return {
        "score": score_100,
        "website_count": website_count,
        "phone_count": phone_count,
        "valid_domain_count": valid_domain_count,
        "contact_hint_count": contact_hint_count,
    }


def _quality_gate_thresholds(market: str, product: str | None) -> dict[str, int]:
    profile = _infer_market_profile(market, product)
    category = profile["category"]
    if category == "food":
        return {"min_total": 5, "min_with_website": 2, "min_contactability": 40}
    if category == "saas":
        return {"min_total": 6, "min_with_website": 4, "min_contactability": 55}
    if category == "healthcare":
        return {"min_total": 4, "min_with_website": 2, "min_contactability": 45}
    if category == "industrial":
        return {"min_total": 4, "min_with_website": 2, "min_contactability": 45}
    if category == "services":
        return {"min_total": 5, "min_with_website": 3, "min_contactability": 50}
    return {"min_total": 4, "min_with_website": 2, "min_contactability": 45}


def _passes_quality_gate(companies: list[dict[str, Any]], market: str, product: str | None) -> tuple[bool, dict[str, Any]]:
    metrics = _find_quality_metrics(companies)
    contactability = _contactability_score(companies)
    thresholds = _quality_gate_thresholds(market, product)
    passed = (
        metrics["total"] >= thresholds["min_total"]
        and metrics["with_website"] >= thresholds["min_with_website"]
        and contactability["score"] >= thresholds["min_contactability"]
    )
    info = {
        "metrics": metrics,
        "contactability": contactability,
        "thresholds": thresholds,
    }
    return passed, info


def _build_retry_queries(market: str, geography: str, product: str | None) -> list[str]:
    search_term = product or market
    profile = _infer_market_profile(market, product)
    category = profile["category"]
    retries = [
        f"{search_term} near {geography}",
        f"{search_term} business {geography}",
        f"{market} companies {geography}",
        f"best {search_term} in {geography}",
    ]

    if category == "food":
        retries.extend(
            [
                f"barbecue smokehouse {geography}",
                f"texas bbq {geography}",
                f"bbq catering {geography}",
                f"{search_term} bbq {geography}",
            ]
        )
    elif category == "saas":
        retries.extend(
            [
                f"{search_term} saas {geography}",
                f"{search_term} software companies {geography}",
                f"{search_term} startup {geography}",
                f"{search_term} platform {geography}",
            ]
        )
    elif category == "healthcare":
        retries.extend(
            [
                f"{search_term} clinic {geography}",
                f"{search_term} medical practice {geography}",
                f"{search_term} healthcare provider {geography}",
            ]
        )
    elif category == "industrial":
        retries.extend(
            [
                f"{search_term} manufacturer {geography}",
                f"{search_term} industrial supplier {geography}",
                f"{search_term} wholesale {geography}",
            ]
        )
    elif category == "services":
        retries.extend(
            [
                f"{search_term} agency {geography}",
                f"{search_term} consulting firm {geography}",
                f"{search_term} professional services {geography}",
            ]
        )

    return _unique_in_order([q.strip() for q in retries if q.strip()])


def _heuristic_qualification(
    companies: list[tuple[Any, Any, Any, Any, Any, Any]],
    market: str,
    product: str | None,
) -> list[dict[str, Any]]:
    profile = _infer_market_profile(market, product)
    positive_tokens = set(profile.get("positive_tokens") or set()) | set(profile.get("tokens") or set())
    positive_tokens = {t for t in positive_tokens if len(t) >= 3}
    if not positive_tokens:
        positive_tokens = {"company", "business", "service", "provider"}

    results: list[dict[str, Any]] = []
    for c in companies:
        company_id, company_name, notes, _phone, website, location = c
        text = " ".join(
            part.lower() for part in [str(company_name or ""), str(notes or ""), str(website or ""), str(location or "")]
        )

        hits = sum(1 for token in positive_tokens if token in text)
        score = min(95, 35 + hits * 18)

        if hits >= 2:
            status = "qualified"
            priority = "high" if hits >= 4 else "medium"
        elif hits == 1:
            status = "qualified"
            priority = "medium"
        else:
            status = "new"
            priority = "low"

        volume_estimate = None
        if status == "qualified":
            volume_estimate = 900 if priority == "high" else 450

        results.append(
            {
                "company_id": str(company_id),
                "status": status,
                "score": score,
                "priority": priority,
                "volume_estimate": volume_estimate,
                "notes": f"Heuristic qualification (keyword matches={hits})",
            }
        )
    return results


def _normalize_qualification_status(status: Any) -> str:
    raw = str(status or "").strip().lower().replace("-", "_").replace(" ", "_")
    if raw in {"qualified", "contacted", "interested", "not_interested", "new"}:
        return raw
    if raw in {"not_relevant", "irrelevant", "disqualified", "reject", "rejected"}:
        return "not_interested"
    if raw in {"uncertain", "unknown", "maybe", "review", "needs_review"}:
        return "new"
    return "new"


def _normalize_priority(priority: Any, score: int) -> str:
    raw = str(priority or "").strip().lower()
    if raw in {"high", "medium", "low"}:
        return raw
    if score >= 80:
        return "high"
    if score >= 55:
        return "medium"
    return "low"


def _clamp_score(value: Any) -> int:
    parsed = int(_to_float(value) or 0)
    return max(0, min(100, parsed))


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
        
        # Try to find JSON in various formats:
        # 1. Standalone JSON object
        # 2. JSON inside code blocks (```json ... ```)
        # 3. JSON array inside code blocks
        
        # Extract fenced JSON block if present
        if "```json" in text:
            text = text.split("```json", 1)[1]
            text = text.split("```", 1)[0]
        elif "```" in text:
            text = text.split("```", 1)[1]
            text = text.split("```", 1)[0]
        text = text.strip()
        
        # Find JSON object or array
        start = text.find("{")
        if start < 0:
            start = text.find("[")
        
        if start >= 0:
            # Find matching closing bracket
            if text[start] == "{":
                end = text.rfind("}")
                if end > start:
                    try:
                        return json.loads(text[start:end+1])
                    except json.JSONDecodeError:
                        pass
            elif text[start] == "[":
                end = text.rfind("]")
                if end > start:
                    try:
                        return {"companies": json.loads(text[start:end+1])}
                    except json.JSONDecodeError:
                        pass
        
        return {"result": "error", "error": "No JSON in output"}
    
    def find(self, market: str, geography: str, product: str | None = None) -> dict[str, Any]:
        """
        STEP 1: Find companies in a market.
        
        Searches web for businesses matching the criteria.
        Uses multi-backend search + source configs + opencode AI.
        Stores results in database.
        """
        search_term = product or market
        all_companies: list[dict[str, Any]] = []
        sources_used: list[str] = []
        source_health: list[dict[str, Any]] = []
        profile = _infer_market_profile(market, product)
        source_health.append(
            {
                "stage": "market_profile",
                "category": profile.get("category"),
                "confidence": profile.get("confidence"),
                "tokens": sorted(list(profile.get("tokens") or []))[:20],
                "status": "ok",
            }
        )
        
        # First, try direct free search using OSM-backed multi_search.
        search_queries = _primary_queries(market=market, geography=geography, product=product)
        
        for query in search_queries:
            search_results = _try_multi_search(query, 10)
            backend_counts = _summarize_backends(search_results)
            source_health.append(
                {
                    "stage": "built_in_search",
                    "query": query,
                    "backends": backend_counts,
                    "results": len(search_results),
                    "status": "ok" if search_results else "empty",
                }
            )
            if search_results:
                sources_used.append("multi_search")
                for r in search_results:
                    all_companies.append({
                        "company_name": r.get("title", ""),
                        "website": r.get("url", ""),
                        "description": r.get("snippet", ""),
                        "evidence_url": r.get("url", ""),
                        "source": r.get("source", "search"),
                    })
        
        # Second, try scraping URLs from source config
        source_results = _try_source_urls(market, geography, product)
        source_health.append(
            {
                "stage": "source_config",
                "queries_or_urls": "configured",
                "results": len(source_results),
                "status": "ok" if source_results else "empty",
            }
        )
        if source_results:
            sources_used.append("source_config")
            for r in source_results:
                data = r.get("data", {})
                if data.get("business_name"):
                    all_companies.append({
                        "company_name": data.get("business_name", ""),
                        "website": data.get("website", ""),
                        "location": data.get("address", ""),
                        "phone": data.get("phone", ""),
                        "description": f"{data.get('rating', '')} - {data.get('reviews_count', '')} reviews",
                        "source": r.get("source", "config"),
                    })
        
        unique_companies = _dedupe_companies(_normalize_companies(all_companies))
        unique_companies = _filter_relevant_companies(unique_companies, market=market, product=product)

        # Deterministic quality gate + retry query pass
        quality_passed, quality_info = _passes_quality_gate(unique_companies, market=market, product=product)
        source_health.append(
            {
                "stage": "quality_gate_initial",
                "status": "pass" if quality_passed else "fail",
                "metrics": quality_info.get("metrics"),
                "contactability": quality_info.get("contactability"),
                "thresholds": quality_info.get("thresholds"),
            }
        )

        if not quality_passed:
            retry_queries = _build_retry_queries(market=market, geography=geography, product=product)
            retry_companies: list[dict[str, Any]] = []
            for query in retry_queries:
                retry_rows = _try_multi_search(query, 10)
                backend_counts = _summarize_backends(retry_rows)
                source_health.append(
                    {
                        "stage": "quality_gate_retry",
                        "query": query,
                        "backends": backend_counts,
                        "results": len(retry_rows),
                        "status": "ok" if retry_rows else "empty",
                    }
                )
                for r in retry_rows:
                    retry_companies.append(
                        {
                            "company_name": r.get("title", ""),
                            "website": r.get("url", ""),
                            "description": r.get("snippet", ""),
                            "evidence_url": r.get("url", ""),
                            "source": r.get("source", "search"),
                        }
                    )

            if retry_companies:
                sources_used.append("quality_gate_retry")
                unique_companies = _dedupe_companies(
                    _normalize_companies(unique_companies + retry_companies)
                )
                unique_companies = _filter_relevant_companies(unique_companies, market=market, product=product)

            retry_passed, retry_info = _passes_quality_gate(unique_companies, market=market, product=product)
            source_health.append(
                {
                    "stage": "quality_gate_final",
                    "status": "pass" if retry_passed else "fail",
                    "metrics": retry_info.get("metrics"),
                    "contactability": retry_info.get("contactability"),
                    "thresholds": retry_info.get("thresholds"),
                }
            )

            # Profile switching: primary profile underperformed → try adjacent profiles
            if not retry_passed:
                adj_cats = _ADJACENT_PROFILES.get(profile["category"], [])
                source_health.append(
                    {
                        "stage": "profile_switch_check",
                        "original_category": profile["category"],
                        "confidence": profile.get("confidence", 50),
                        "adjacent_profiles": adj_cats,
                        "status": "attempting" if adj_cats else "skipped",
                    }
                )
                for adj_cat in adj_cats:
                    adj_queries = _queries_for_adjacent_profile(market, geography, product, adj_cat)
                    adj_companies: list[dict[str, Any]] = []
                    for query in adj_queries:
                        rows = _try_multi_search(query, 10)
                        source_health.append(
                            {
                                "stage": "profile_switch_search",
                                "adjacent_category": adj_cat,
                                "query": query,
                                "backends": _summarize_backends(rows),
                                "results": len(rows),
                                "status": "ok" if rows else "empty",
                            }
                        )
                        for r in rows:
                            adj_companies.append(
                                {
                                    "company_name": r.get("title", ""),
                                    "website": r.get("url", ""),
                                    "description": r.get("snippet", ""),
                                    "evidence_url": r.get("url", ""),
                                    "source": r.get("source", "search"),
                                }
                            )

                    if adj_companies:
                        merged = _dedupe_companies(
                            _normalize_companies(unique_companies + adj_companies)
                        )
                        merged = _filter_relevant_companies(merged, market=market, product=product)
                        adj_passed, adj_info = _passes_quality_gate(merged, market=market, product=product)
                        source_health.append(
                            {
                                "stage": "profile_switch_gate",
                                "adjacent_category": adj_cat,
                                "status": "pass" if adj_passed else "fail",
                                "metrics": adj_info.get("metrics"),
                                "contactability": adj_info.get("contactability"),
                                "added_companies": len(merged) - len(unique_companies),
                            }
                        )
                        # Accept the merged pool whether or not the gate fully passed —
                        # more candidates is always better heading into contactability retry.
                        if len(merged) > len(unique_companies):
                            unique_companies = merged
                            sources_used.append(f"profile_switch_{adj_cat}")
                        if adj_passed:
                            break  # gate passed — no need to try further adjacent profiles

        # Secondary gate: contactability enrichment pass using deterministic contact queries
        secondary_passed, secondary_info = _passes_quality_gate(unique_companies, market=market, product=product)
        source_health.append(
            {
                "stage": "quality_gate_contactability_initial",
                "status": "pass" if secondary_passed else "fail",
                "metrics": secondary_info.get("metrics"),
                "contactability": secondary_info.get("contactability"),
                "thresholds": secondary_info.get("thresholds"),
            }
        )

        if not secondary_passed:
            contact_queries = _build_contact_retry_queries(unique_companies, geography=geography)
            contact_rows: list[dict[str, str]] = []
            for query in contact_queries:
                rows = _try_multi_search(query, 8)
                contact_rows.extend(rows)
                source_health.append(
                    {
                        "stage": "quality_gate_contactability_retry",
                        "query": query,
                        "results": len(rows),
                        "backends": _summarize_backends(rows),
                        "status": "ok" if rows else "empty",
                    }
                )

            if contact_rows:
                updated_companies, updates = _apply_contact_retry_rows(unique_companies, contact_rows)
                unique_companies = _dedupe_companies(_normalize_companies(updated_companies))
                unique_companies = _filter_relevant_companies(unique_companies, market=market, product=product)
                if updates > 0:
                    sources_used.append("contactability_retry")
                source_health.append(
                    {
                        "stage": "quality_gate_contactability_updates",
                        "status": "ok",
                        "updated_companies": updates,
                    }
                )

            secondary_final_passed, secondary_final_info = _passes_quality_gate(
                unique_companies, market=market, product=product
            )
            source_health.append(
                {
                    "stage": "quality_gate_contactability_final",
                    "status": "pass" if secondary_final_passed else "fail",
                    "metrics": secondary_final_info.get("metrics"),
                    "contactability": secondary_final_info.get("contactability"),
                    "thresholds": secondary_final_info.get("thresholds"),
                }
            )
        
        # If we have companies from direct search, use them
        if unique_companies:
            result = {
                "result": "ok",
                "companies": unique_companies,
                "sources_used": _unique_in_order(sources_used),
                "method": "direct_search" if sources_used else "opencode",
                "source_health": source_health,
            }
        else:
            # Fall back to opencode AI search
            sources_used = ["opencode"]
            opencode_sources, opencode_queries = _opencode_hints(market=market, geography=geography, product=product)
            prompt = f"""Find businesses in {geography} that offer {search_term}.

For each business, find:
- Company name, website, address, phone
- What they sell/offer related to {search_term}
- How established they are (reviews, years in business)

Search sources: {opencode_sources}
Search queries: {opencode_queries}

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
            result["method"] = "opencode"
            result["sources_used"] = sources_used
            source_health.append(
                {
                    "stage": "opencode_fallback",
                    "results": len(result.get("companies", [])) if isinstance(result, dict) else 0,
                    "status": "ok" if isinstance(result, dict) and result.get("result") != "error" else "error",
                }
            )
        
        if result.get("result") == "error":
            return result
        
        companies = _normalize_companies(result.get("companies", []))
        companies = _dedupe_companies(companies)
        companies = _filter_relevant_companies(companies, market=market, product=product)
        result["companies"] = companies
        
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
                # Persist a JSON summary of source_health on the research row for dashboard visibility
                try:
                    import json as _json
                    conn.execute(
                        "UPDATE researches SET last_source_health = ? WHERE id = ?",
                        (_json.dumps(source_health, ensure_ascii=True), self.research_id),
                    )
                except Exception:
                    # Don't fail the find() if we can't persist source_health
                    pass
        
        self.last_result = result
        result["sources_used"] = _unique_in_order(sources_used)
        result["source_health"] = source_health
        if "result" not in result:
            result["result"] = "ok"
        return result
    
    def qualify(self) -> dict[str, Any]:
        """
        STEP 2: Qualify companies - AI assessment of relevance and volume.
        
        Updates companies in database with priority scores and volume estimates.
        """
        if not self.research_id:
            return {"result": "error", "error": "No research_id set"}

        from market_validation.research import (
            _connect,
            _ensure_schema,
            get_research,
            resolve_db_path,
            update_company,
        )
        db = resolve_db_path(self.root)

        research = get_research(self.research_id, root=self.root)
        if research.get("result") != "ok":
            return {"result": "error", "error": "Research not found"}
        research_market = str(research.get("research", {}).get("market") or "")
        research_product = research.get("research", {}).get("product")
        
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

        if result.get("result") == "error" or not result.get("results"):
            fallback = _heuristic_qualification(companies, market=research_market, product=research_product)
            result = {"result": "ok", "results": fallback, "method": "heuristic"}
        else:
            result["method"] = "opencode"
        
        qualified = 0
        for r in result.get("results", []):
            cid = r.get("company_id")
            volume_estimate = _to_float(r.get("volume_estimate"))
            score = _clamp_score(r.get("score"))
            status = _normalize_qualification_status(r.get("status", "new"))
            priority = _normalize_priority(r.get("priority"), score)
            fields = {
                "status": status,
                "priority_score": score,
                "priority_tier": priority,
                "volume_estimate": volume_estimate,
                "notes": r.get("notes"),
            }
            update_company(cid, self.research_id, fields, root=self.root)
            if status == "qualified":
                qualified += 1

        self.last_result = result
        return {"result": "ok", "qualified": qualified, "assessed": len(companies), "method": result.get("method", "unknown")}
    
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
        from market_validation.research import _connect, _ensure_schema, resolve_db_path, update_company
        
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

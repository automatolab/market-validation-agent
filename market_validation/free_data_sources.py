"""
Free Data Sources — structured data from public APIs that require no API key.

Sources:
- BLS.gov v1 API      — industry employment counts, wage data, trends
- SEC EDGAR search    — 10-K filing snippets mentioning market size / revenue
- Reddit JSON API     — actual post text + scores for community sentiment
- Wikipedia REST API  — industry overview pages with size context
- HackerNews Algolia  — tech/SaaS community discussions (no key)

All functions return [] or {} on failure — callers always get usable output.
"""

from __future__ import annotations

import time
from typing import Any

import urllib.request
import urllib.parse
import json as _json


_HEADERS = {
    "User-Agent": "market-validation-agent/1.0 (research tool; contact: noreply@example.com)",
    "Accept": "application/json",
}


def _get(url: str, params: dict | None = None, timeout: int = 12) -> dict | list | None:
    """Simple GET with urllib (no requests dependency required)."""
    try:
        if params:
            url = url + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return _json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# BLS.gov v1 API — no key, 25 series/day / 500 req/day limit
# ---------------------------------------------------------------------------

# Maps our market categories to BLS CES (Current Employment Statistics) series.
# Format: CEU + supersector(2) + industry(6) + data_type(2)
# data_type 01 = All employees (thousands), 11 = Avg weekly earnings
_BLS_SERIES: dict[str, dict[str, str]] = {
    "food":         {"emp": "CEU7072200001", "label": "Food Services & Drinking Places"},
    "healthcare":   {"emp": "CEU6562000001", "label": "Health Care & Social Assistance"},
    "saas":         {"emp": "CEU5051800001", "label": "Software Publishers"},
    "services":     {"emp": "CEU6000000001", "label": "Professional & Business Services"},
    "industrial":   {"emp": "CEU3000000001", "label": "Manufacturing"},
    "retail":       {"emp": "CEU4200000001", "label": "Retail Trade"},
    "general":      {"emp": "CEU0000000001", "label": "Total Nonfarm"},
}


def bls_industry_data(category: str) -> dict[str, Any]:
    """
    Fetch BLS employment data for a market category.

    Returns dict with employment_thousands, yoy_change_pct, trend,
    label, and a formatted snippet for AI context.
    """
    series_info = _BLS_SERIES.get(category, _BLS_SERIES["general"])
    series_id = series_info["emp"]
    label = series_info["label"]

    url = f"https://api.bls.gov/publicAPI/v1/timeseries/data/{series_id}"
    data = _get(url)
    if not data:
        return {}

    series_data = (
        data.get("Results", {}).get("series", [{}])[0].get("data", [])
    )
    if len(series_data) < 13:
        return {}

    try:
        latest_val = float(series_data[0]["value"].replace(",", ""))
        year_ago_val = float(series_data[12]["value"].replace(",", ""))
        yoy_pct = (latest_val - year_ago_val) / year_ago_val * 100
        period = f"{series_data[0]['periodName']} {series_data[0]['year']}"

        trend = (
            "growing" if yoy_pct > 1.5
            else "declining" if yoy_pct < -1.5
            else "stable"
        )
        employment_total = round(latest_val * 1000)  # thousands → actual count

        snippet = (
            f"BLS {label}: {employment_total:,} employees as of {period}. "
            f"Year-over-year change: {yoy_pct:+.1f}% ({trend})."
        )
        return {
            "employment": employment_total,
            "yoy_change_pct": round(yoy_pct, 1),
            "trend": trend,
            "label": label,
            "period": period,
            "snippet": snippet,
        }
    except (ValueError, IndexError, KeyError):
        return {}


# ---------------------------------------------------------------------------
# SEC EDGAR full-text search — no key required
# ---------------------------------------------------------------------------

def edgar_search(query: str, form_type: str = "10-K", limit: int = 8) -> list[dict[str, Any]]:
    """
    Search SEC EDGAR full-text filings for market size mentions.

    Good for finding public companies that have quantified the market in
    their annual reports. Returns list of {company, form, filed, snippet}.
    """
    params = {
        "q": f'"{query}"',
        "forms": form_type,
        "dateRange": "custom",
        "startdt": "2023-01-01",
        "enddt": "2026-12-31",
    }
    data = _get("https://efts.sec.gov/LATEST/search-index", params=params)
    if not data:
        return []

    hits = data.get("hits", {}).get("hits", [])
    results = []
    for h in hits[:limit]:
        src = h.get("_source", {})
        names = src.get("display_names") or src.get("entity_name") or ["Unknown"]
        company = names[0] if isinstance(names, list) else names

        # Prefer highlighted snippet; fall back to period info
        highlight = h.get("highlight", {})
        body_snips = highlight.get("body", highlight.get("file_date", []))
        raw_snippet = body_snips[0] if body_snips else ""
        # Strip HTML tags from highlight
        snippet = raw_snippet.replace("<em>", "").replace("</em>", "").strip()
        if not snippet:
            snippet = f"Filed {src.get('file_date', '')} for period {src.get('period_of_report', '')}"

        results.append({
            "company": company,
            "form": src.get("form_type", form_type),
            "filed": src.get("file_date", ""),
            "snippet": snippet[:300],
            "url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={src.get('entity_id', '')}&type={form_type}",
        })

    return results


# ---------------------------------------------------------------------------
# Reddit JSON API — no key, User-Agent required
# ---------------------------------------------------------------------------

# Subreddits to target by market category for relevant community signal
_REDDIT_SUBREDDITS: dict[str, str] = {
    "food":       "BBQ+smoking+food+mealprep+foodservice+Cooking",
    "saas":       "SaaS+startups+entrepreneur+software+ProductManagement",
    "healthcare": "healthIT+medicine+nursing+HealthcareWorkers",
    "industrial": "manufacturing+supplychain+logistics+engineering",
    "services":   "consulting+freelance+smallbusiness+entrepreneur",
    "retail":     "ecommerce+retailnews+smallbusiness+entrepreneur",
    "general":    "smallbusiness+entrepreneur+startups+business",
}


def reddit_search(
    query: str,
    category: str = "general",
    time_filter: str = "year",
    limit: int = 25,
) -> list[dict[str, Any]]:
    """
    Search Reddit for posts matching a query, targeting category-relevant subreddits.

    Returns list of {title, score, subreddit, url, snippet, num_comments}.
    Includes upvote scores as a signal of how much the community cares about an issue.
    """
    subreddit = _REDDIT_SUBREDDITS.get(category, _REDDIT_SUBREDDITS["general"])
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    params = {"q": query, "restrict_sr": "1", "sort": "relevance", "t": time_filter, "limit": limit}

    data = _get(url, params=params)
    if not data:
        # Fall back to site-wide search if subreddit search fails
        data = _get("https://www.reddit.com/search.json", params={"q": query, "sort": "relevance", "t": time_filter, "limit": limit})
    if not data:
        return []

    posts = data.get("data", {}).get("children", [])

    # Build relevance word set from query for basic filtering
    query_words = {w.lower() for w in query.split() if len(w) > 3}

    results = []
    for p in posts:
        d = p.get("data", {})
        if not d:
            continue
        title = d.get("title", "")
        selftext = (d.get("selftext") or "").strip()
        if selftext in ("[deleted]", "[removed]"):
            selftext = ""

        # Relevance filter: at least one query word must appear in title or text
        combined = (title + " " + selftext).lower()
        if query_words and not any(w in combined for w in query_words):
            continue

        results.append({
            "title": title,
            "score": d.get("score", 0),
            "subreddit": d.get("subreddit", ""),
            "url": f"https://reddit.com{d.get('permalink', '')}",
            "snippet": selftext[:400] or title,
            "num_comments": d.get("num_comments", 0),
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# Wikipedia REST API — no key
# ---------------------------------------------------------------------------

def wikipedia_industry_summary(market: str) -> dict[str, Any]:
    """
    Fetch a Wikipedia industry overview page for market context.

    Tries several search variants to find the most relevant article.
    Returns {title, extract, url} or {} on failure.
    """
    # Try industry-level terms first, then narrow
    candidates = [
        f"{market} industry United States",
        f"{market} industry",
        f"{market} market",
        market,
    ]

    for candidate in candidates:
        # Search for matching pages
        search_data = _get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "list": "search",
                "srsearch": candidate,
                "format": "json",
                "srlimit": 3,
                "srprop": "snippet",
            },
        )
        if not search_data:
            continue

        results = search_data.get("query", {}).get("search", [])
        if not results:
            continue

        # Relevance words from this candidate (words > 3 chars)
        query_words = {w.lower() for w in candidate.split() if len(w) > 3}

        # Try each search result until one passes the title relevance check
        for result_item in results:
            title = result_item["title"]

            # Title must share at least one meaningful word with query to avoid
            # matching company profiles that rank above industry overview pages
            title_lower = title.lower()
            if query_words and not any(w in title_lower for w in query_words):
                continue

            safe_title = title.replace(" ", "_")
            summary = _get(f"https://en.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(safe_title)}")
            if not summary or summary.get("type") == "disambiguation":
                continue

            extract = summary.get("extract", "")
            if not extract or len(extract) < 50:
                continue

            return {
                "title": summary.get("title", title),
                "extract": extract[:600],
                "url": summary.get("content_urls", {}).get("desktop", {}).get("page", ""),
                "snippet": f"Wikipedia — {summary.get('title', title)}: {extract[:300]}",
            }

    return {}


# ---------------------------------------------------------------------------
# HackerNews Algolia API — no key (best for tech/SaaS markets)
# ---------------------------------------------------------------------------

def hackernews_search(query: str, limit: int = 20) -> list[dict[str, Any]]:
    """
    Search HackerNews posts and comments via Algolia API.

    Good signal for tech/SaaS markets — number of HN discussions and
    upvotes indicates developer/startup community interest.
    Returns list of {title, points, url, snippet, num_comments}.
    """
    params = {
        "query": query,
        "tags": "story",
        "hitsPerPage": limit,
    }
    data = _get("https://hn.algolia.com/api/v1/search", params=params)
    if not data:
        return []

    hits = data.get("hits", [])
    results = []
    for h in hits:
        title = h.get("title") or h.get("story_title") or ""
        if not title:
            continue
        results.append({
            "title": title,
            "points": h.get("points", 0),
            "url": h.get("url") or f"https://news.ycombinator.com/item?id={h.get('objectID', '')}",
            "snippet": title,
            "num_comments": h.get("num_comments", 0),
            "created_at": h.get("created_at", ""),
        })

    results.sort(key=lambda x: x["points"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# Convenience: fetch all relevant sources for a market
# ---------------------------------------------------------------------------

def gather_free_data(
    market: str,
    geography: str,
    product: str | None = None,
    category: str = "general",
) -> dict[str, Any]:
    """
    Fetch all free data sources in sequence and return a combined dict.

    Keys: bls, edgar, reddit, wikipedia, hackernews
    Safe to call even if individual sources fail.
    """
    search_term = product or market
    out: dict[str, Any] = {}

    # BLS employment data
    out["bls"] = bls_industry_data(category)
    time.sleep(0.5)

    # EDGAR — search for market size in 10-K filings
    out["edgar"] = edgar_search(f"{market} market size revenue", limit=6)
    time.sleep(0.5)

    # Wikipedia industry overview
    out["wikipedia"] = wikipedia_industry_summary(search_term)
    time.sleep(0.3)

    # Reddit — pain points and community sentiment
    reddit_results = reddit_search(f"{search_term} problem", limit=15)
    reddit_results += reddit_search(f"{search_term} recommendation", limit=10)
    # Deduplicate by URL
    seen_urls: set[str] = set()
    deduped = []
    for r in reddit_results:
        if r["url"] not in seen_urls:
            seen_urls.add(r["url"])
            deduped.append(r)
    out["reddit"] = sorted(deduped, key=lambda x: x["score"], reverse=True)[:20]
    time.sleep(0.5)

    # HackerNews — only for tech/SaaS markets (not useful for food/industrial)
    if category in ("saas", "general"):
        out["hackernews"] = hackernews_search(search_term, limit=15)
    else:
        out["hackernews"] = []

    return out

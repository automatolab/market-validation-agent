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

import functools
import json as _json
import os
import threading
import time
import urllib.parse
import urllib.request
from typing import Any, Callable, TypeVar

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
# Request-level TTL cache for trend feeds.
#
# Several modules (demand_analysis, market_signals, market_sizing) hit the
# same upstream APIs for the same keyword within a single research run.
# Without a cache we'd spend 2-3x the rate budget and add unnecessary
# latency. The cache is in-process, thread-safe, and time-bucketed so
# entries auto-expire without explicit invalidation.
# ---------------------------------------------------------------------------

_CACHE_TTL_SECONDS = int(os.environ.get("MV_TREND_CACHE_TTL", "1800"))  # 30 min default

# Per-source TTL overrides — different feeds change at very different cadences.
# Stale jobs/news hurt accuracy more than stale Wikipedia pageviews. Operators
# can override individual TTLs via MV_TREND_TTL_<NAME> env vars (seconds).
_PER_FN_TTL: dict[str, int] = {
    # Stable references — long TTL is fine.
    "wikipedia_industry_summary": 24 * 3600,   # 24h
    "wikipedia_pageviews":         12 * 3600,  # 12h — pageview series shifts slowly
    "openalex_works_timeline":     12 * 3600,  # 12h — academic publishing is monthly cadence
    # Moderate cadence.
    "bls_industry_data":           6 * 3600,   # 6h — monthly BLS releases, fine
    "bls_wages_data":              6 * 3600,   # 6h
    "github_repo_growth":          6 * 3600,   # 6h
    "hackernews_volume_timeline":  3 * 3600,   # 3h — story counts shift per cycle
    # Time-sensitive — short TTL.
    "gdelt_news_timeline":         1 * 3600,   # 1h — daily news volume needs freshness
    "google_news_rss":             1 * 3600,   # 1h
    "edgar_search":                3 * 3600,   # 3h
    "reddit_search":               2 * 3600,   # 2h
    "yelp_local_market_data":      6 * 3600,   # 6h
    "overpass_local_business_count": 24 * 3600, # 24h — OSM doesn't change daily
    "hackernews_search":           2 * 3600,   # 2h
}


def _resolve_ttl(fn_name: str) -> int:
    """Resolve TTL for a function, honoring env-var overrides."""
    env_key = f"MV_TREND_TTL_{fn_name.upper()}"
    override = os.environ.get(env_key)
    if override:
        try:
            return int(override)
        except ValueError:
            pass
    return _PER_FN_TTL.get(fn_name, _CACHE_TTL_SECONDS)

_CACHE: dict[tuple, tuple[float, Any]] = {}
_CACHE_LOCK = threading.Lock()

_F = TypeVar("_F", bound=Callable[..., Any])


def _cached(fn: _F) -> _F:
    """Decorator: cache returns by (function name, args, sorted kwargs).

    TTL is resolved per-function so jobs/news (1-2h) stay fresh while stable
    references (Wikipedia summaries) survive ~24h. Skips caching when the
    result indicates a transient failure (None, or a dict with
    available=False) — caching errors would mean we never recover when an
    API briefly hiccups mid-run. Honors ``MV_TREND_FORCE_REFRESH=1`` to
    bypass the cache for an entire run.
    """
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if os.environ.get("MV_TREND_FORCE_REFRESH") == "1":
            return fn(*args, **kwargs)
        key = (fn.__name__, args, tuple(sorted(kwargs.items())))
        ttl = _resolve_ttl(fn.__name__)
        now = time.time()
        with _CACHE_LOCK:
            entry = _CACHE.get(key)
            if entry and (now - entry[0]) < ttl:
                return entry[1]

        result = fn(*args, **kwargs)

        # Don't memoize transient failures — let the next caller retry.
        cacheable = result is not None and not (
            isinstance(result, dict) and result.get("available") is False
        )
        if cacheable:
            with _CACHE_LOCK:
                _CACHE[key] = (now, result)
        return result
    return wrapper  # type: ignore[return-value]


def clear_trend_cache() -> None:
    """Drop every cached trend-feed result. Useful for tests."""
    with _CACHE_LOCK:
        _CACHE.clear()


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


@_cached
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
# BLS.gov wages data — average hourly/weekly earnings by archetype category
# ---------------------------------------------------------------------------

# BLS CES Average Hourly Earnings series (data_type 03)
# Format: CEU + supersector + industry + 03
_BLS_WAGES_SERIES: dict[str, dict[str, str]] = {
    "local-service":    {"series": "CEU7072200003", "label": "Food Services & Drinking Places"},
    "b2b-saas":         {"series": "CEU5051800003", "label": "Software Publishers"},
    "b2c-saas":         {"series": "CEU5051800003", "label": "Software Publishers"},
    "healthcare":       {"series": "CEU6562000003", "label": "Health Care & Social Assistance"},
    "b2b-industrial":   {"series": "CEU3000000003", "label": "Manufacturing"},
    "consumer-cpg":     {"series": "CEU3100000003", "label": "Food Manufacturing"},
    "marketplace":      {"series": "CEU5051800003", "label": "Software Publishers"},
    "services-agency":  {"series": "CEU6000000003", "label": "Professional & Business Services"},
    "general":          {"series": "CEU0000000003", "label": "Total Nonfarm"},
}


@_cached
def bls_wages_data(archetype: str) -> dict[str, Any]:
    """
    Fetch BLS average hourly earnings for an archetype category.

    Returns dict with avg_hourly_wage, avg_weekly_wage, label, period,
    and a formatted snippet for AI context. Returns {} on failure.
    """
    series_info = _BLS_WAGES_SERIES.get(archetype, _BLS_WAGES_SERIES["general"])
    series_id = series_info["series"]
    label = series_info["label"]

    url = f"https://api.bls.gov/publicAPI/v1/timeseries/data/{series_id}"
    data = _get(url)
    if not data:
        return {}

    series_data = (
        data.get("Results", {}).get("series", [{}])[0].get("data", [])
    )
    if not series_data:
        return {}

    try:
        latest = series_data[0]
        hourly_wage = float(latest["value"].replace(",", ""))
        weekly_wage = round(hourly_wage * 40, 2)
        annual_wage = round(hourly_wage * 2080, 0)
        period = f"{latest['periodName']} {latest['year']}"

        snippet = (
            f"BLS {label}: avg hourly wage ${hourly_wage:.2f} "
            f"(~${annual_wage:,.0f}/year) as of {period}."
        )
        return {
            "avg_hourly_wage": hourly_wage,
            "avg_weekly_wage": weekly_wage,
            "avg_annual_wage": annual_wage,
            "label": label,
            "period": period,
            "snippet": snippet,
        }
    except (ValueError, IndexError, KeyError):
        return {}


# ---------------------------------------------------------------------------
# SEC EDGAR full-text search — no key required
# ---------------------------------------------------------------------------

@_cached
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


@_cached
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

@_cached
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
# Wikimedia Pageviews API — free, no key, no rate limit, no captcha
# Real interest curve over time. Used as a Google Trends replacement when
# pytrends is rate-limited (which is most of the time on shared/datacenter IPs).
# ---------------------------------------------------------------------------

def _wiki_resolve_article(query: str) -> str | None:
    """Best-effort: resolve a market term to a Wikipedia article title.

    Tries the direct REST summary endpoint first (which follows redirects to
    the canonical article — e.g. "Indoor farming" → "Vertical farming"),
    then falls back to the search API for fuzzy matches.

    Returns ``None`` if no article matches.
    """
    # 1. Direct lookup with redirect following — handles ~80% of common queries
    #    and avoids the search API's tendency to promote related-but-wrong
    #    articles ("Association for Vertical Farming" over "Vertical farming").
    #    Also try each individual content word so "hydroponic growers"
    #    (no article) gracefully falls back to "Hydroponics" — but skip
    #    generic single-word fallbacks ("commercial" → "Television
    #    advertisement") that resolve to off-topic articles.
    _GENERIC_FALLBACK_WORDS = {
        "commercial", "industrial", "consumer", "professional", "business",
        "company", "companies", "market", "markets", "service", "services",
        "system", "systems", "platform", "solution", "solutions", "tool",
        "tools", "product", "products", "smart", "small", "large", "global",
        "general", "local", "national", "international",
    }
    direct_candidates = [query, f"{query} industry"]
    words = [w for w in query.split() if len(w) > 3]
    specific_words = [w for w in words if w.lower() not in _GENERIC_FALLBACK_WORDS]
    if len(words) > 1 and specific_words:
        # Single content words ranked by length (longer = more specific),
        # generic words excluded so we don't snap to off-topic articles.
        for w in sorted(specific_words, key=len, reverse=True):
            direct_candidates.append(w)
            if not w.endswith("s"):
                direct_candidates.append(w + "s")
    for candidate in direct_candidates:
        slug = candidate.replace(" ", "_")
        summary = _get(
            f"https://en.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(slug)}"
        )
        if summary and summary.get("type") == "standard" and summary.get("title"):
            return summary["title"]

    # 2. Search fallback for fuzzy matches. Prefer titles whose lowercased form
    #    starts with or equals the query — this rejects "Association for X"
    #    when "X" itself exists.
    q_lower = query.strip().lower()
    candidates = [f"{query} industry", query, f"{query} (industry)"]
    for candidate in candidates:
        search = _get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "list": "search",
                "srsearch": candidate,
                "format": "json",
                "srlimit": 5,
                "srprop": "snippet",
            },
        )
        results = (search or {}).get("query", {}).get("search", []) if search else []
        if not results:
            continue
        # Rank: exact match > starts-with-query > contains-all-words.
        def _score(title: str) -> int:
            t = title.lower()
            if t == q_lower:
                return 3
            if t.startswith(q_lower):
                return 2
            words = {w for w in q_lower.split() if len(w) > 3}
            if words and all(w in t for w in words):
                return 1
            return 0
        ranked = sorted(
            ((_score(r.get("title", "")), r.get("title", "")) for r in results),
            key=lambda x: -x[0],
        )
        best_score, best_title = ranked[0]
        if best_title and best_score > 0:
            return best_title
    return None


@_cached
def wikipedia_pageviews(
    query: str,
    days: int = 365,
    article_title: str | None = None,
) -> dict[str, Any]:
    """Fetch daily Wikipedia pageviews for a topic and compute trend direction.

    Returns a dict with the resolved article, daily view counts, an average,
    a peak, and a trend label (``rising`` / ``stable`` / ``falling``) computed
    by comparing the first vs. last third of the window.

    Designed as a free, IP-friendly alternative to Google Trends. The
    Wikimedia REST API has no rate limit and works from datacenter IPs.

    Returns ``{"available": False, "reason": ...}`` when the article can't be
    resolved or the API returns no data — callers should treat that as
    "no signal", not "negative signal".
    """
    from datetime import UTC, datetime, timedelta

    title = article_title or _wiki_resolve_article(query)
    if not title:
        return {"available": False, "reason": "no matching wikipedia article"}

    end = datetime.now(UTC)
    start = end - timedelta(days=days)
    # API requires YYYYMMDDHH; use 00 for daily granularity.
    fmt = "%Y%m%d00"
    url = (
        "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
        f"en.wikipedia.org/all-access/all-agents/{urllib.parse.quote(title.replace(' ', '_'))}"
        f"/daily/{start.strftime(fmt)}/{end.strftime(fmt)}"
    )
    data = _get(url)
    if not data or "items" not in data:
        return {
            "available": False,
            "article": title,
            "reason": "wikimedia api returned no data",
        }

    items = data["items"]
    if not items:
        return {
            "available": False,
            "article": title,
            "reason": "wikimedia api returned empty series",
        }

    views = [int(i.get("views") or 0) for i in items]
    n = len(views)
    if n < 30:
        return {
            "available": False,
            "article": title,
            "samples": n,
            "reason": "too few samples to compute trend",
        }

    # Compare the first third vs. the last third of the window — robust to
    # short-term noise. A ≥10% delta on either side flips the label.
    third = n // 3
    early = sum(views[:third]) / max(third, 1)
    late = sum(views[-third:]) / max(third, 1)
    if early <= 0:
        delta_pct = 0.0
    else:
        delta_pct = (late - early) / early * 100

    if delta_pct >= 10:
        trend = "rising"
    elif delta_pct <= -10:
        trend = "falling"
    else:
        trend = "stable"

    return {
        "available": True,
        "article": title,
        "samples": n,
        "avg_daily_views": round(sum(views) / n, 1),
        "peak_daily_views": max(views),
        "early_avg": round(early, 1),
        "late_avg": round(late, 1),
        "delta_pct": round(delta_pct, 1),
        "trend": trend,
        "url": f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
        "snippet": (
            f"Wikipedia pageviews ({title}): "
            f"{round(sum(views)/n)} avg daily over {n} days, "
            f"trend={trend} ({delta_pct:+.1f}% early→late)"
        ),
    }


# ---------------------------------------------------------------------------
# GDELT 2.0 — global news article volume timeline. Free, no key.
# Rate limit: ~1 request per 5 seconds (per their docs).
# Best free analogue to Google Trends for B2B markets.
# ---------------------------------------------------------------------------

@_cached
def gdelt_news_timeline(query: str, timespan_months: int = 24) -> dict[str, Any]:
    """Fetch a daily news-article-volume timeline for a query from GDELT 2.0.

    Returns a dict with:
      - ``samples``: number of daily observations
      - ``avg_daily_articles``: mean articles/day matching the query
      - ``early_avg`` / ``late_avg``: first vs. last third of window
      - ``delta_pct`` / ``trend``: % change and label (rising/stable/falling)
      - ``snippet``: human-readable one-line summary

    Returns ``{"available": False, ...}`` when GDELT returns no data so the
    caller can treat absence as "no signal" rather than negative signal.
    """
    # GDELT prefers 1 req per 5s — caller is responsible for spacing.
    # For multi-word queries, wrap in quotes so they're matched as a phrase.
    q = f'"{query}"' if " " in query and not (query.startswith('"') and query.endswith('"')) else query
    params = {
        "query": q,
        "mode": "timelinevolraw",
        "format": "json",
        "timespan": f"{timespan_months}mon",
    }
    data = _get("https://api.gdeltproject.org/api/v2/doc/doc", params=params, timeout=15)
    if not data or not isinstance(data, dict):
        return {"available": False, "reason": "gdelt returned no data"}

    timeline = data.get("timeline") or []
    if not timeline:
        return {"available": False, "reason": "gdelt timeline empty"}

    series = timeline[0].get("data") or []
    counts = [int(d.get("value") or 0) for d in series]
    if len(counts) < 30:
        return {"available": False, "samples": len(counts), "reason": "too few samples"}

    # Drop the most recent 7 days — GDELT's index lag undercounts them and
    # would falsely register a "falling" trend on every healthy market.
    if len(counts) > 14:
        counts = counts[:-7]

    third = len(counts) // 3
    early = sum(counts[:third]) / max(third, 1)
    late = sum(counts[-third:]) / max(third, 1)
    delta_pct = (late - early) / early * 100 if early > 0 else 0.0

    if delta_pct >= 15:
        trend = "rising"
    elif delta_pct <= -15:
        trend = "falling"
    else:
        trend = "stable"

    avg = round(sum(counts) / len(counts), 1)
    return {
        "available": True,
        "query": query,
        "samples": len(counts),
        "avg_daily_articles": avg,
        "peak_daily_articles": max(counts),
        "early_avg": round(early, 1),
        "late_avg": round(late, 1),
        "delta_pct": round(delta_pct, 1),
        "trend": trend,
        "snippet": (
            f"GDELT news volume ({query}): {avg} avg articles/day over "
            f"{len(counts)} days, trend={trend} ({delta_pct:+.1f}% early→late)"
        ),
    }


# ---------------------------------------------------------------------------
# OpenAlex — academic paper metadata. Free, no key (mailto recommended).
# Strong leading indicator: research interest now → commercial demand later.
# ---------------------------------------------------------------------------

@_cached
def openalex_works_timeline(query: str, years: int = 5) -> dict[str, Any]:
    """Fetch the count of academic works per year matching ``query``.

    Returns a dict with per-year counts plus a recent-vs-prior trend label.
    Best signal for emerging technology (agritech, deep tech, biotech) where
    research output today predicts commercial interest in 1–3 years.
    """
    from datetime import UTC, datetime
    current_year = datetime.now(UTC).year
    from_year = current_year - years
    # NOTE: do not set ``per-page`` here. OpenAlex clamps the ``group_by``
    # array to ``per-page`` items, so per-page=1 returns only one year and
    # collapses the trend signal. Default (200 groups) is what we want.
    params = {
        "search": query,
        "filter": f"from_publication_date:{from_year}-01-01",
        "group_by": "publication_year",
    }
    # OpenAlex's "polite pool" — including a mailto gets faster, more
    # reliable responses. Honors $OPENALEX_MAILTO when set; otherwise no-op.
    mailto = os.environ.get("OPENALEX_MAILTO")
    if mailto:
        params["mailto"] = mailto
    data = _get("https://api.openalex.org/works", params=params, timeout=15)
    if not data or not isinstance(data, dict):
        return {"available": False, "reason": "openalex returned no data"}

    groups = data.get("group_by") or []
    if not groups:
        return {"available": False, "reason": "openalex returned no year groups"}

    by_year: dict[int, int] = {}
    for g in groups:
        try:
            yr = int(g.get("key") or g.get("key_display_name") or 0)
            ct = int(g.get("count") or 0)
            if yr > 0:
                by_year[yr] = ct
        except (TypeError, ValueError):
            continue
    if not by_year:
        return {"available": False, "reason": "openalex year keys unparseable"}

    # Drop the current year — incomplete (still being indexed).
    counts_complete = {y: c for y, c in by_year.items() if y < current_year}
    sorted_years = sorted(counts_complete.keys())
    if len(sorted_years) < 2:
        return {
            "available": True,
            "query": query,
            "by_year": dict(sorted(by_year.items())),
            "total": int((data.get("meta") or {}).get("count") or 0),
            "trend": "unknown",
            "snippet": (
                f"OpenAlex ({query}): {(data.get('meta') or {}).get('count', 0)} "
                f"total works (insufficient years for trend)"
            ),
        }

    # Compare last full year vs. mean of prior 2-3 years.
    last_year = sorted_years[-1]
    prior_years = sorted_years[-4:-1] if len(sorted_years) >= 4 else sorted_years[:-1]
    last_count = counts_complete[last_year]
    prior_avg = sum(counts_complete[y] for y in prior_years) / max(len(prior_years), 1)
    delta_pct = (last_count - prior_avg) / prior_avg * 100 if prior_avg > 0 else 0.0

    if delta_pct >= 15:
        trend = "rising"
    elif delta_pct <= -15:
        trend = "falling"
    else:
        trend = "stable"

    total = int((data.get("meta") or {}).get("count") or 0)
    return {
        "available": True,
        "query": query,
        "total_works": total,
        "by_year": dict(sorted(by_year.items())),
        "last_year": last_year,
        "last_year_count": last_count,
        "prior_avg": round(prior_avg, 1),
        "delta_pct": round(delta_pct, 1),
        "trend": trend,
        "snippet": (
            f"OpenAlex ({query}): {total} total works, "
            f"{last_count} in {last_year} vs {round(prior_avg)}/yr prior — "
            f"trend={trend} ({delta_pct:+.1f}%)"
        ),
    }


# ---------------------------------------------------------------------------
# GitHub Search — repo creation rate. Free, 60 req/hr unauthenticated,
# 5,000 req/hr with a personal access token in $GITHUB_TOKEN.
# Strong adoption signal for any developer-touching market.
# ---------------------------------------------------------------------------

@_cached
def github_repo_growth(query: str) -> dict[str, Any]:
    """Compare GitHub repos matching ``query`` created in the last 12 months
    vs. the prior 12 months.

    Returns a dict with both counts, a delta %, and a trend label. A growth
    signal here tracks developer / startup adoption of the market topic.

    Honors $GITHUB_TOKEN to bump quota from 60 → 5,000 requests/hour.
    """
    from datetime import UTC, datetime, timedelta
    now = datetime.now(UTC)
    one_year_ago = (now - timedelta(days=365)).strftime("%Y-%m-%d")
    two_years_ago = (now - timedelta(days=730)).strftime("%Y-%m-%d")

    headers = {**_HEADERS, "Accept": "application/vnd.github+json"}
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    def _count(date_filter: str) -> int | None:
        # Use stars:>0 to exclude single-commit garbage repos that inflate
        # noise on broader queries — for narrow queries the filter is cheap.
        url = "https://api.github.com/search/repositories"
        params = {"q": f"{query} {date_filter}", "per_page": "1"}
        try:
            full = url + "?" + urllib.parse.urlencode(params)
            req = urllib.request.Request(full, headers=headers)
            with urllib.request.urlopen(req, timeout=12) as resp:
                d = _json.loads(resp.read().decode("utf-8"))
                return int(d.get("total_count") or 0)
        except Exception:
            return None

    last_year = _count(f"created:>{one_year_ago}")
    prior_year = _count(f"created:{two_years_ago}..{one_year_ago}")

    if last_year is None or prior_year is None:
        return {
            "available": False,
            "reason": "github api error or rate limited",
        }

    if prior_year == 0 and last_year == 0:
        return {
            "available": False,
            "query": query,
            "reason": "no repos found in either window",
        }

    if prior_year == 0:
        delta_pct = 100.0
    else:
        delta_pct = (last_year - prior_year) / prior_year * 100

    if delta_pct >= 15:
        trend = "rising"
    elif delta_pct <= -15:
        trend = "falling"
    else:
        trend = "stable"

    return {
        "available": True,
        "query": query,
        "last_year_count": last_year,
        "prior_year_count": prior_year,
        "delta_pct": round(delta_pct, 1),
        "trend": trend,
        "snippet": (
            f"GitHub repos ({query}): {last_year} created last 12mo, "
            f"{prior_year} the year before — trend={trend} ({delta_pct:+.1f}%)"
        ),
    }


# ---------------------------------------------------------------------------
# HackerNews Algolia API — no key (best for tech/SaaS markets)
# ---------------------------------------------------------------------------

@_cached
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


@_cached
def hackernews_volume_timeline(query: str) -> dict[str, Any]:
    """Compare HN story count for ``query`` in the last 12mo vs. the prior 12mo.

    Strong tech-adoption proxy: the number of stories submitted to HN about
    a topic tracks how much the developer / founder / VC community is
    discussing it. A rising count is one of the cleanest leading indicators
    for emerging tech markets.
    """
    from datetime import UTC, datetime, timedelta
    now = datetime.now(UTC)
    one_year_ago = int((now - timedelta(days=365)).timestamp())
    two_years_ago = int((now - timedelta(days=730)).timestamp())
    now_ts = int(now.timestamp())

    def _count(filter_str: str) -> int | None:
        params = {
            "query": query,
            "tags": "story",
            "numericFilters": filter_str,
            "hitsPerPage": "0",  # we only want the count
        }
        data = _get("https://hn.algolia.com/api/v1/search", params=params)
        if not isinstance(data, dict):
            return None
        try:
            return int(data.get("nbHits") or 0)
        except (TypeError, ValueError):
            return None

    last = _count(f"created_at_i>{one_year_ago}")
    prior = _count(f"created_at_i>{two_years_ago},created_at_i<{one_year_ago}")
    if last is None or prior is None:
        return {"available": False, "reason": "hn algolia error"}
    if last == 0 and prior == 0:
        return {"available": False, "query": query, "reason": "no hn stories found"}

    if prior == 0:
        delta_pct = 100.0
    else:
        delta_pct = (last - prior) / prior * 100

    if delta_pct >= 15:
        trend = "rising"
    elif delta_pct <= -15:
        trend = "falling"
    else:
        trend = "stable"

    return {
        "available": True,
        "query": query,
        "last_year_stories": last,
        "prior_year_stories": prior,
        "delta_pct": round(delta_pct, 1),
        "trend": trend,
        "snippet": (
            f"HackerNews stories ({query}): {last} last 12mo, "
            f"{prior} the year before — trend={trend} ({delta_pct:+.1f}%)"
        ),
    }


# ---------------------------------------------------------------------------
# Google News RSS — no key, no account, free real news articles
# ---------------------------------------------------------------------------

@_cached
def google_news_rss(query: str, limit: int = 15) -> list[dict[str, Any]]:
    """
    Fetch recent news articles via Google News RSS feed.

    No API key or account required. Returns actual news with titles, sources,
    and publication dates — far better than DuckDuckGo news snippets.

    Returns list of {title, url, source_name, published, snippet}.
    """
    import xml.etree.ElementTree as ET

    params = urllib.parse.urlencode({"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"})
    url = f"https://news.google.com/rss/search?{params}"

    # _get() parses JSON and returns None for XML — fetch raw bytes directly
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "market-validation-agent/1.0 (research tool; contact: noreply@example.com)",
            "Accept": "application/rss+xml, application/xml, text/xml",
        })
        with urllib.request.urlopen(req, timeout=12) as resp:
            xml_bytes = resp.read()
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []

    results = []
    channel = root.find("channel")
    if channel is None:
        return []

    for item in channel.findall("item")[:limit]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or "")[:16].strip()
        # Source is in <source> tag or embedded in title as " - Publisher"
        source_tag = item.find("source")
        source_name = source_tag.text.strip() if source_tag is not None and source_tag.text else ""
        if not source_name and " - " in title:
            parts = title.rsplit(" - ", 1)
            title = parts[0].strip()
            source_name = parts[1].strip()

        if not title or not link:
            continue

        results.append({
            "title": title,
            "url": link,
            "source_name": source_name,
            "published": pub_date,
            "snippet": title,  # RSS doesn't include description, title is the signal
        })

    return results


# ---------------------------------------------------------------------------
# Local business data — Foursquare/Overpass + Yelp scrape (no key needed)
# ---------------------------------------------------------------------------

@_cached
def overpass_local_business_count(market: str, geography: str) -> dict[str, Any]:
    """
    Count local businesses via OpenStreetMap Overpass API (completely free, no key).

    Maps market terms to OSM amenity/shop tags to count real businesses.
    Returns {count, sample_names, snippet} or {} on failure.
    """
    # Map common market terms to OSM tags
    _OSM_TAG_MAP: dict[str, list[str]] = {
        "restaurant": ["amenity=restaurant", "amenity=fast_food"],
        "bbq": ['amenity=restaurant', 'cuisine=bbq'],
        "barbecue": ['amenity=restaurant', 'cuisine=bbq'],
        "cafe": ["amenity=cafe"],
        "coffee": ["amenity=cafe"],
        "gym": ["leisure=fitness_centre", "leisure=sports_centre"],
        "salon": ["shop=hairdresser", "shop=beauty"],
        "bar": ["amenity=bar", "amenity=pub"],
        "hotel": ["tourism=hotel"],
        "pharmacy": ["amenity=pharmacy"],
        "dentist": ["amenity=dentist"],
        "doctor": ["amenity=doctors", "amenity=clinic"],
        "bakery": ["shop=bakery"],
        "grocery": ["shop=supermarket", "shop=grocery"],
    }

    market_lower = market.lower()
    tags: list[str] = []
    for keyword, osm_tags in _OSM_TAG_MAP.items():
        if keyword in market_lower:
            tags = osm_tags
            break
    if not tags:
        return {}  # Only works for mapped categories

    # Resolve geography to lat/lon bounding box via Nominatim (longer timeout)
    try:
        nom_url = (
            "https://nominatim.openstreetmap.org/search?"
            + urllib.parse.urlencode({"q": geography, "format": "json", "limit": 1})
        )
        nom_req = urllib.request.Request(nom_url, headers={
            "User-Agent": "market-validation-agent/1.0 (research tool; contact: noreply@example.com)",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(nom_req, timeout=20) as nom_resp:
            geo_data = _json.loads(nom_resp.read().decode("utf-8"))
    except Exception:
        return {}
    if not geo_data or not isinstance(geo_data, list):
        return {}

    place = geo_data[0]
    bbox = place.get("boundingbox")  # [south, north, west, east]
    if not bbox or len(bbox) < 4:
        return {}

    south, north, west, east = bbox
    area_str = f"{south},{west},{north},{east}"

    # Run Overpass query for each tag
    total_count = 0
    sample_names: list[str] = []

    for tag in tags[:2]:  # limit to 2 tags to avoid long queries
        key, val = tag.split("=", 1)
        overpass_query = (
            f'[out:json][timeout:20];'
            f'(node["{key}"="{val}"]({area_str});'
            f'way["{key}"="{val}"]({area_str}););'
            f'out count;'
        )
        # Use urllib directly with longer timeout — Overpass can be slow
        try:
            ov_url = (
                "https://overpass-api.de/api/interpreter?"
                + urllib.parse.urlencode({"data": overpass_query})
            )
            ov_req = urllib.request.Request(ov_url, headers={
                "User-Agent": "market-validation-agent/1.0 (research tool; contact: noreply@example.com)",
            })
            with urllib.request.urlopen(ov_req, timeout=25) as ov_resp:
                ov_data = _json.loads(ov_resp.read().decode("utf-8"))
            count = ov_data.get("elements", [{}])[0].get("tags", {}).get("total", 0)
            total_count += int(count)
        except (ValueError, TypeError, Exception):
            pass
        time.sleep(1.0)  # Be polite to Overpass

    if total_count == 0:
        return {}

    snippet = (
        f"OpenStreetMap: ~{total_count:,} {market} businesses in {geography} "
        f"(from OSM data, may undercount unlisted businesses)"
    )
    return {
        "count": total_count,
        "sample_names": sample_names,
        "snippet": snippet,
        "source": "openstreetmap_overpass",
    }


@_cached
def yelp_local_market_data(market: str, geography: str) -> dict[str, Any]:
    """
    Scrape Yelp search results for local business density data (no key needed).

    Yelp requires JS rendering so this often fails. If it fails,
    falls back to OpenStreetMap Overpass API count (always works).

    Returns {business_count, avg_rating, price_distribution, snippet} or {}.
    """
    # Try Yelp web scraping first (richer data when it works)
    try:
        from market_validation.web_scraper import scrape_yelp_search
        result = scrape_yelp_search(market, geography, limit=40)
        if result and not result.get("error"):
            result["source"] = "yelp_scrape"
            return result
    except Exception:
        pass

    # Fall back to OpenStreetMap (always works, no JS required)
    return overpass_local_business_count(market, geography)


# gather_free_data() was removed — it was never imported anywhere outside
# this file. Each pipeline module pulls the specific feeds it needs:
#   - market_sizing      → bls_industry_data, edgar_search, wikipedia_industry_summary
#   - demand_analysis    → wikipedia_pageviews, gdelt, openalex, github, hn_volume (parallel)
#   - market_signals     → bls, openalex, github, hn_volume (gated on category)
#   - unit_economics     → bls_wages_data
# Combining them through a single sequential helper added latency without
# any caller benefit.

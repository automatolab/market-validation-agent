"""
Demand Analysis — assesses actual market demand using Google Trends,
search volume proxies, and community sentiment + AI synthesis.

Wires in the existing market_trends.py module (pytrends) which was
previously implemented but not connected to the pipeline.

AI synthesis (claude/opencode) is required for structured output.
"""

from __future__ import annotations

import json
import time
from typing import Any, Callable


def _search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results)
    except Exception:
        return []


def _trends_keywords(term: str, archetype: str) -> list[str]:
    """
    Return 2-3 pytrends keywords that are meaningful for this archetype.

    Deterministic — no AI call needed. The bare term is always first so
    the primary interest level is always captured. The second keyword is
    an intent modifier that signals purchase/adoption intent specifically
    for this kind of market.

    Skips any modifier whose key word already appears in the term
    (e.g. "CRM software" won't become "CRM software software").
    """
    t = term.lower()
    # Each archetype gets a prioritised list of (modifier_word, full_phrase) pairs.
    # We include a phrase only if its modifier isn't already in the search term.
    candidates_map: dict[str, list[tuple[str, str]]] = {
        "local-service":   [("near me", f"{term} near me"), ("hours", f"{term} open")],
        "b2b-saas":        [("software", f"{term} software"), ("tool", f"{term} tool"), ("platform", f"{term} platform")],
        "b2c-saas":        [("app", f"{term} app"), ("free", f"{term} free"), ("download", f"{term} download")],
        "b2b-industrial":  [("supplier", f"{term} supplier"), ("wholesale", f"{term} wholesale"), ("bulk", f"buy {term} bulk")],
        "consumer-cpg":    [("buy", f"buy {term}"), ("brand", f"{term} brand"), ("review", f"{term} review")],
        "marketplace":     [("marketplace", f"{term} marketplace"), ("hire", f"hire {term}"), ("platform", f"{term} platform")],
        "healthcare":      [("treatment", f"{term} treatment"), ("cost", f"{term} cost"), ("insurance", f"{term} insurance")],
        "services-agency": [("agency", f"{term} agency"), ("hire", f"hire {term}"), ("consultant", f"{term} consultant")],
    }
    fallback = [("review", f"{term} review"), ("price", f"{term} price")]
    candidates = candidates_map.get(archetype, fallback)

    result = [term]
    for modifier, phrase in candidates:
        if modifier not in t and len(result) < 3:
            result.append(phrase)
    return result


def _get_trends_data(product: str, geography: str, archetype: str = "general") -> dict[str, Any]:
    """Call existing market_trends module with archetype-appropriate keywords."""
    try:
        from market_validation.market_trends import get_market_demand_report
        geo_code = _geography_to_code(geography)
        keywords = _trends_keywords(product, archetype)
        return get_market_demand_report(product, geography=geo_code, keywords=keywords)
    except Exception as e:
        return {"result": "ok", "error": str(e), "skipped": True}


def _geography_to_code(geography: str) -> str:
    """Best-effort conversion of geography string to pytrends geo code."""
    geo = geography.lower().strip()
    country_map = {
        "united states": "US", "usa": "US", "us": "US",
        "united kingdom": "GB", "uk": "GB",
        "canada": "CA", "australia": "AU",
        "germany": "DE", "france": "FR", "japan": "JP",
        "india": "IN", "china": "CN", "brazil": "BR",
    }
    for name, code in country_map.items():
        if name in geo:
            return code
    us_states = {
        "california": "US-CA", "texas": "US-TX", "new york": "US-NY",
        "florida": "US-FL", "illinois": "US-IL", "pennsylvania": "US-PA",
        "ohio": "US-OH", "georgia": "US-GA", "michigan": "US-MI",
        "washington": "US-WA", "arizona": "US-AZ", "colorado": "US-CO",
        "massachusetts": "US-MA", "virginia": "US-VA", "oregon": "US-OR",
        "north carolina": "US-NC", "new jersey": "US-NJ", "minnesota": "US-MN",
    }
    for state, code in us_states.items():
        if state in geo:
            return code
    return "US"


def analyze_demand(
    market: str,
    geography: str,
    product: str | None = None,
    run_ai: Callable[..., dict[str, Any]] | None = None,
    archetype: str = "general",
) -> dict[str, Any]:
    """
    Analyze market demand using Google Trends + web search + AI synthesis.

    Pass archetype so pytrends uses intent keywords relevant to this market
    (e.g. "near me" for local-service, "software" for b2b-saas).

    Returns dict with demand_score, demand_trend, demand_pain_points, etc.
    """
    search_term = product or market

    # 1. Google Trends with archetype-aware keywords
    trends = _get_trends_data(search_term, geography, archetype=archetype)

    # 2. Search volume proxies — result counts across intent types (market-aware)
    try:
        from market_validation.query_context import get_validation_queries
        _demand_ctx = get_validation_queries(market, geography, product)["demand"]
        volume_queries: dict[str, str] = _demand_ctx["volume"]
        community_queries: list[str] = _demand_ctx["community"]
    except Exception:
        volume_queries = {
            "general": f"{search_term} {geography}",
            "transactional": f"buy {search_term} {geography}",
            "alternative": f"{search_term} alternative",
            "comparison": f"best {search_term} {geography}",
        }
        community_queries = [
            f"site:reddit.com {search_term} recommendation",
            f"site:reddit.com {search_term} problem frustrating",
            f"{search_term} review complaint",
        ]
    volume_counts: dict[str, int] = {}
    for intent, query in volume_queries.items():
        volume_counts[intent] = len(_search(query, num_results=20))
        time.sleep(1.2)

    # 3. Community sentiment — Reddit API first (richer than DuckDuckGo snippets),
    #    fall back to DuckDuckGo if Reddit is unavailable
    community_snippets: list[str] = []
    reddit_posts: list[dict] = []
    try:
        from market_validation.free_data_sources import reddit_search
        from market_validation.query_context import detect_market_category
        _reddit_category = detect_market_category(market, product)
        _pain = reddit_search(f"{search_term} problem frustrating", category=_reddit_category, limit=20)
        _rec = reddit_search(f"{search_term} recommendation best", category=_reddit_category, limit=15)
        reddit_posts = sorted(_pain + _rec, key=lambda x: x["score"], reverse=True)
        # Deduplicate by URL
        seen_reddit: set[str] = set()
        deduped: list[dict] = []
        for p in reddit_posts:
            if p["url"] not in seen_reddit:
                seen_reddit.add(p["url"])
                deduped.append(p)
        reddit_posts = deduped[:25]
        for p in reddit_posts:
            text = p["snippet"] or p["title"]
            if text:
                community_snippets.append(
                    f"[r/{p['subreddit']} ↑{p['score']}] {p['title']}: {text[:200]}"
                )
        time.sleep(0.5)
    except Exception:
        pass

    # Google News RSS — real news headlines about this market, no key needed
    news_snippets: list[str] = []
    try:
        from market_validation.free_data_sources import google_news_rss
        _news = google_news_rss(f"{search_term} {geography}", limit=12)
        if not _news:
            _news = google_news_rss(search_term, limit=8)
        for article in _news:
            title = article.get("title", "")
            src = article.get("source_name", "")
            pub = article.get("published", "")[:10]
            if title:
                news_snippets.append(f"[News: {src} {pub}] {title}")
        time.sleep(0.3)
    except Exception:
        pass

    # DuckDuckGo fallback if Reddit returned nothing
    if not community_snippets:
        for query in community_queries:
            for r in _search(query, num_results=8):
                s = r.get("snippet", "").strip()
                if s:
                    community_snippets.append(s)
            time.sleep(1.2)

    # Crowdfunding signal — Kickstarter/Indiegogo campaigns prove willingness to pay
    crowdfunding_snippets: list[str] = []
    try:
        for cq in [f"site:kickstarter.com {search_term}", f"site:indiegogo.com {search_term}"]:
            for r in _search(cq, num_results=5):
                s = r.get("snippet", "").strip()
                t = r.get("title", "").strip()
                if s or t:
                    crowdfunding_snippets.append(f"[Crowdfunding] {t}: {s[:150]}")
            time.sleep(1.0)
    except Exception:
        pass

    # Subreddit subscriber count as commitment signal
    subreddit_signals: list[str] = []
    try:
        from market_validation.free_data_sources import reddit_search
        # Search for subreddits about this market
        _subs = _search(f"site:reddit.com/r {search_term}", num_results=5)
        for r in _subs:
            url = r.get("url", "")
            title = r.get("title", "")
            snippet = r.get("snippet", "")
            if "reddit.com/r/" in url and snippet:
                subreddit_signals.append(f"[Subreddit] {title}: {snippet[:150]}")
        time.sleep(0.8)
    except Exception:
        pass

    result: dict[str, Any] = {
        "trends_data": trends,
        "search_volume": volume_counts,
        "community_snippet_count": len(community_snippets),
        "reddit_post_count": len(reddit_posts),
        "news_snippet_count": len(news_snippets),
        "crowdfunding_snippet_count": len(crowdfunding_snippets),
        "subreddit_signal_count": len(subreddit_signals),
    }

    if not run_ai:
        return result

    snippet_text = "\n".join(f"- {s}" for s in community_snippets[:20])
    trends_summary = json.dumps({
        "demand_level": trends.get("demand_level", "unknown"),
        "market_demand_score": trends.get("market_demand_score", 0),
        "keywords": {
            k: {"avg": v.get("interest_avg", 0), "trend": v.get("interest_trend", "unknown")}
            for k, v in (trends.get("keywords") or {}).items()
        },
    }, indent=2)

    crowdfunding_text = "\n".join(f"- {s}" for s in crowdfunding_snippets[:6]) if crowdfunding_snippets else "(none found)"
    subreddit_text = "\n".join(f"- {s}" for s in subreddit_signals[:4]) if subreddit_signals else "(none found)"

    prompt = f"""You are a demand analyst. Assess demand for:

Market: {market}
Geography: {geography}
Product: {product or 'general'}

Google Trends (0-100 interest scale, higher = more searches):
{trends_summary}

Search result counts across intent types (more = higher demand signal):
{json.dumps(volume_counts, indent=2)}

Community discussions (Reddit/forums) — {len(reddit_posts)} posts sorted by upvotes:
{snippet_text or '(none found)'}

Google News headlines ({len(news_snippets)} articles):
{chr(10).join(f"- {{s}}" for s in news_snippets[:10]) or "(none found)"}

Crowdfunding campaigns (Kickstarter/Indiegogo — proves willingness to pay):
{crowdfunding_text}

Subreddit / community presence signals:
{subreddit_text}

Return ONLY this JSON (no markdown fences):
{{
    "demand_score": <0-100 composite demand score>,
    "demand_trend": "<rising|stable|falling>",
    "demand_seasonality": "<seasonal pattern or 'none detected'>",
    "demand_pain_points": ["specific pain point 1", "specific pain point 2", "specific pain point 3"],
    "demand_sources": ["Google Trends", "Reddit community", ...],
    "willingness_to_pay": "<high|medium|low|unknown>",
    "crowdfunding_signal": "<strong|moderate|weak|none> — brief note if campaigns found",
    "notes": "2-3 sentences on demand strength, evidence quality, and key insight"
}}

Scoring guide:
- 75+: strong upward trend + active community + high search volume + crowdfunding evidence
- 50-74: moderate/mixed signals
- 25-49: weak signals or thin evidence
- <25: no meaningful demand detected
Pain points must be specific and actionable, not vague."""

    ai_result = run_ai(prompt)
    parsed: dict[str, Any] = {}
    if isinstance(ai_result, dict):
        if "demand_score" in ai_result:
            parsed = ai_result
        elif "text" in ai_result:
            try:
                parsed = json.loads(ai_result["text"])
            except (json.JSONDecodeError, TypeError):
                result["ai_raw"] = ai_result.get("text", str(ai_result))
    elif isinstance(ai_result, str):
        try:
            parsed = json.loads(ai_result)
        except (json.JSONDecodeError, TypeError):
            result["ai_raw"] = ai_result

    result.update(parsed)
    return result

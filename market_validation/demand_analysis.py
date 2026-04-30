"""
Demand Analysis — assesses actual market demand using Google Trends,
search volume proxies, and community sentiment + AI synthesis.

Wires in the existing market_trends.py module (pytrends) which was
previously implemented but not connected to the pipeline.

AI synthesis (claude/opencode) is required for structured output.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from market_validation.log import get_logger

_log = get_logger("demand_analysis")


def _search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results)
    except Exception as exc:
        _log.debug("multi_search.quick_search failed for %r: %s", query, exc)
        return []


_TREND_STOPWORDS: frozenset[str] = frozenset({
    "the", "a", "an", "and", "or", "for", "of", "to", "in", "on", "with",
    "system", "systems", "platform", "service", "services", "software",
    "solution", "solutions", "tools", "tool", "products", "product",
    "automation", "automated", "smart", "commercial", "industrial",
    "company", "companies", "business", "businesses",
})


def _shorten_for_trends(term: str) -> str:
    """Reduce a long product/market phrase to a short, high-volume search term.

    Google Trends and Wikipedia pageviews both want short, generic terms
    ("hydroponics", "vertical farming") rather than full product descriptions
    ("hydroponic plant automation systems"). This drops generic suffixes,
    keeps the most specific 1–2 content words, and falls back to the bare
    input if nothing is left.
    """
    words = [w.strip().lower() for w in term.split() if w.strip()]
    content = [w for w in words if w not in _TREND_STOPWORDS]
    if not content:
        return term.strip()
    if len(content) == 1:
        return content[0]
    # Prefer the first 2 content words — usually the head noun phrase.
    return " ".join(content[:2])


def _trends_keywords(term: str, archetype: str, market: str | None = None) -> list[str]:
    """Return up to 2 short, high-volume keywords for trend analysis.

    Both pytrends and Wikipedia pageviews require short generic terms to
    return useful data. We use the broader ``market`` term (if provided) as
    the primary keyword and a shortened version of ``term`` as a backup.
    The list is always deduped and trimmed to ≤ 2 items.

    ``archetype`` is accepted for API compatibility — modifier-suffixed
    phrases ("brisket supplier", "CRM software") are deliberately *not*
    appended anymore because they have negligible trend volume on their own.
    """
    del archetype  # kept for backward compatibility
    primary = _shorten_for_trends(market) if market else _shorten_for_trends(term)
    secondary = _shorten_for_trends(term)
    keywords: list[str] = []
    for kw in (primary, secondary):
        if kw and kw not in keywords:
            keywords.append(kw)
    return keywords[:2]


def _consensus_trend(labels: list[str]) -> str:
    """Pick a single trend label from a list of per-source labels.

    Rules:
      - "rising" wins if rising count ≥ falling count and rising > 0.
      - "falling" wins only when falling strictly outnumbers BOTH rising
        AND stable — i.e. it's the plurality directional signal.
      - "stable" otherwise when any stable source exists.
      - "unknown" only when nothing is rising / falling / stable.

    Bias is intentional: the original pipeline let a single "falling" source
    mark the whole market as declining — which kept biting us when GDELT or
    Wikipedia happened to dip on otherwise healthy markets. Now a falling
    label needs corroboration before it wins.
    """
    counts = {"rising": 0, "stable": 0, "falling": 0, "unknown": 0}
    for label in labels:
        if label in counts:
            counts[label] += 1
    if counts["rising"] >= counts["falling"] and counts["rising"] > 0:
        return "rising"
    if counts["falling"] > max(counts["rising"], counts["stable"]):
        return "falling"
    if counts["stable"] > 0:
        return "stable"
    return "unknown"


def _get_trends_data(
    product: str,
    geography: str,
    archetype: str = "general",
    market: str | None = None,
) -> dict[str, Any]:
    """Gather trend data from every free longitudinal source we have access to.

    Sources (all fetched in parallel):
      - Wikipedia pageviews (no rate limit, works from any IP)
      - GDELT news article volume (covers any topic in news)
      - OpenAlex academic publication count (best for emerging tech)
      - GitHub repo creation (best for developer-touching markets)
      - HackerNews story volume (best for tech / SaaS / startup markets)

    Returns a composite dict that callers can serialize directly into the AI
    prompt. ``interest_trend`` is a consensus label across all sources that
    returned data; missing sources are ignored, never treated as "falling".

    Note: Google Trends (pytrends) was removed because Google's anti-bot
    layer blocks data-center IPs with HTTP 429 on every request, so it
    only ever produced "no_data" noise that the AI had to be coached to
    ignore.
    """
    del geography  # geo filtering is handled elsewhere; trend feeds are global
    keywords = _trends_keywords(product, archetype, market=market)

    from market_validation.free_data_sources import (
        gdelt_news_timeline,
        github_repo_growth,
        hackernews_volume_timeline,
        openalex_works_timeline,
        wikipedia_pageviews,
    )

    # ── Fetch sources concurrently ───────────────────────────────────────
    # Each fetcher is independent and IO-bound; running them in parallel
    # cuts wall-clock from ~9s (sequential, GDELT's 5s sleep dominated) to
    # roughly the slowest single feed (~1.5-3s).
    def _safe(label: str, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            _log.debug("trend feed %s failed for %r: %s", label, args, exc)
            return None

    wiki_signals: list[dict[str, Any]] = []
    gdelt_signal: dict[str, Any] = {}
    openalex_signal: dict[str, Any] = {}
    github_signal: dict[str, Any] = {}
    hn_signal: dict[str, Any] = {}

    if keywords:
        primary = keywords[0]
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {
                pool.submit(_safe, "gdelt", gdelt_news_timeline, primary, timespan_months=24): "gdelt",
                pool.submit(_safe, "openalex", openalex_works_timeline, primary, years=5): "openalex",
                pool.submit(_safe, "github", github_repo_growth, primary): "github",
                pool.submit(_safe, "hackernews_volume", hackernews_volume_timeline, primary): "hackernews_volume",
                pool.submit(_safe_wiki_pageviews, wikipedia_pageviews, keywords): "wikipedia_pageviews",
            }
            for fut in as_completed(futures):
                label = futures[fut]
                value = fut.result()
                if label == "gdelt":
                    gdelt_signal = value or {}
                elif label == "openalex":
                    openalex_signal = value or {}
                elif label == "github":
                    github_signal = value or {}
                elif label == "hackernews_volume":
                    hn_signal = value or {}
                elif label == "wikipedia_pageviews":
                    wiki_signals = value or []

    # ── Compose & label ──────────────────────────────────────────────────
    composed: dict[str, Any] = {
        "keywords_used": keywords,
        "wikipedia_pageviews": wiki_signals,
        "gdelt": gdelt_signal,
        "openalex": openalex_signal,
        "github": github_signal,
        "hackernews_volume": hn_signal,
    }

    # Build per-source trend labels for consensus voting. Each source is
    # treated as one vote; sources that returned no data are skipped.
    per_source_labels: dict[str, str] = {}
    if wiki_signals:
        per_source_labels["wikipedia_pageviews"] = wiki_signals[0].get("trend", "unknown")
    if gdelt_signal.get("available"):
        per_source_labels["gdelt"] = gdelt_signal.get("trend", "unknown")
    if openalex_signal.get("available") and openalex_signal.get("trend") != "unknown":
        per_source_labels["openalex"] = openalex_signal.get("trend", "unknown")
    if github_signal.get("available"):
        per_source_labels["github"] = github_signal.get("trend", "unknown")
    if hn_signal.get("available"):
        per_source_labels["hackernews_volume"] = hn_signal.get("trend", "unknown")

    composed["per_source_trends"] = per_source_labels
    composed["sources_available"] = list(per_source_labels.keys())

    # Pick the primary source (preferred order) for legacy fields, and a
    # consensus label across all available sources.
    consensus = _consensus_trend(list(per_source_labels.values()))
    composed["interest_trend"] = consensus

    if wiki_signals:
        primary = wiki_signals[0]
        composed["primary_source"] = "wikipedia_pageviews"
        composed["interest_avg"] = primary.get("avg_daily_views")
        composed["delta_pct"] = primary.get("delta_pct")
    elif gdelt_signal.get("available"):
        composed["primary_source"] = "gdelt"
        composed["interest_avg"] = gdelt_signal.get("avg_daily_articles")
        composed["delta_pct"] = gdelt_signal.get("delta_pct")
    elif per_source_labels:
        # Fall back to any other available source so callers always see a
        # named primary when at least one signal exists.
        composed["primary_source"] = next(iter(per_source_labels))
    else:
        composed["primary_source"] = "none"

    return composed


def _safe_wiki_pageviews(fn, keywords: list[str]) -> list[dict[str, Any]]:
    """Resolve and dedup Wikipedia pageviews across keyword variants."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for kw in keywords:
        try:
            r = fn(kw, days=365)
        except Exception as exc:
            _log.debug("wikipedia_pageviews failed for %r: %s", kw, exc)
            continue
        if r.get("available") and r.get("article") not in seen:
            seen.add(r["article"])
            out.append(r)
    return out


def analyze_demand(
    market: str,
    geography: str,
    product: str | None = None,
    run_ai: Callable[..., dict[str, Any]] | None = None,
    archetype: str = "general",
) -> dict[str, Any]:
    """
    Analyze market demand from longitudinal trend feeds + web search + AI synthesis.

    Pass archetype so the search query builder uses intent keywords relevant
    to this market (e.g. "near me" for local-service, "software" for b2b-saas).

    Returns dict with demand_score, demand_trend, demand_pain_points, etc.
    """
    search_term = product or market

    # 1. Longitudinal trend feeds — Wikipedia pageviews, GDELT news, OpenAlex,
    #    GitHub repo growth, HackerNews volume. All fetched in parallel.
    trends = _get_trends_data(search_term, geography, archetype=archetype, market=market)

    # 2. Search volume proxies — result counts across intent types (market-aware)
    try:
        from market_validation.query_context import get_validation_queries
        _demand_ctx = get_validation_queries(market, geography, product)["demand"]
        volume_queries: dict[str, str] = _demand_ctx["volume"]
        community_queries: list[str] = _demand_ctx["community"]
    except Exception as exc:
        _log.debug("get_validation_queries failed, using defaults: %s", exc)
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
    # Run the volume queries in parallel — they're independent per-intent
    # search calls. Sequential 1.2s waits used to add 5-7s of dead time.
    volume_counts: dict[str, int] = {}
    if volume_queries:
        with ThreadPoolExecutor(max_workers=min(6, len(volume_queries))) as pool:
            future_to_intent = {
                pool.submit(_search, query, 20): intent
                for intent, query in volume_queries.items()
            }
            for fut in as_completed(future_to_intent):
                intent = future_to_intent[fut]
                try:
                    volume_counts[intent] = len(fut.result() or [])
                except Exception as exc:
                    _log.debug("volume search failed for intent=%s: %s", intent, exc)
                    volume_counts[intent] = 0

    # 3. Community/news/crowdfunding/subreddit signals — all independent
    # network calls, so run them in parallel. Each task returns (label,
    # payload) tuples that we route into named buckets afterward.
    from market_validation.free_data_sources import google_news_rss, reddit_search
    from market_validation.query_context import detect_market_category
    _reddit_category = detect_market_category(market, product)

    def _fetch_reddit_pain():
        return reddit_search(f"{search_term} problem frustrating", category=_reddit_category, limit=20)

    def _fetch_reddit_rec():
        return reddit_search(f"{search_term} recommendation best", category=_reddit_category, limit=15)

    def _fetch_news():
        articles = google_news_rss(f"{search_term} {geography}", limit=12)
        if not articles:
            articles = google_news_rss(search_term, limit=8)
        return articles

    def _fetch_subreddits():
        return _search(f"site:reddit.com/r {search_term}", num_results=5)

    def _fetch_kickstarter():
        return _search(f"site:kickstarter.com {search_term}", num_results=5)

    def _fetch_indiegogo():
        return _search(f"site:indiegogo.com {search_term}", num_results=5)

    parallel_fetches = {
        "reddit_pain": _fetch_reddit_pain,
        "reddit_rec": _fetch_reddit_rec,
        "news": _fetch_news,
        "subreddits": _fetch_subreddits,
        "kickstarter": _fetch_kickstarter,
        "indiegogo": _fetch_indiegogo,
    }

    results_by_key: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=len(parallel_fetches)) as pool:
        future_to_key = {pool.submit(fn): key for key, fn in parallel_fetches.items()}
        for fut in as_completed(future_to_key):
            key = future_to_key[fut]
            try:
                results_by_key[key] = fut.result()
            except Exception as exc:
                _log.debug("demand fetch %s failed: %s", key, exc)
                results_by_key[key] = []

    # Reddit posts — pain + recommendation, deduped, sorted by upvote score
    community_snippets: list[str] = []
    reddit_posts: list[dict] = []
    try:
        _pain = results_by_key.get("reddit_pain") or []
        _rec = results_by_key.get("reddit_rec") or []
        reddit_posts = sorted(_pain + _rec, key=lambda x: x["score"], reverse=True)
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
    except Exception as exc:
        _log.debug("reddit post composition failed: %s", exc)

    # Google News RSS — real news headlines
    news_snippets: list[str] = []
    for article in results_by_key.get("news") or []:
        title = article.get("title", "")
        src = article.get("source_name", "")
        pub = article.get("published", "")[:10]
        if title:
            news_snippets.append(f"[News: {src} {pub}] {title}")

    # DuckDuckGo fallback if Reddit returned nothing — fire community queries
    # in parallel, since they're also independent.
    if not community_snippets and community_queries:
        with ThreadPoolExecutor(max_workers=min(4, len(community_queries))) as pool:
            for rows in pool.map(lambda q: _search(q, num_results=8), community_queries):
                for r in rows or []:
                    s = r.get("snippet", "").strip()
                    if s:
                        community_snippets.append(s)

    # Crowdfunding signal — Kickstarter/Indiegogo
    crowdfunding_snippets: list[str] = []
    for label, key in (("Crowdfunding", "kickstarter"), ("Crowdfunding", "indiegogo")):
        for r in results_by_key.get(key) or []:
            s = r.get("snippet", "").strip()
            t = r.get("title", "").strip()
            if s or t:
                crowdfunding_snippets.append(f"[{label}] {t}: {s[:150]}")

    # Subreddit presence as commitment signal
    subreddit_signals: list[str] = []
    for r in results_by_key.get("subreddits") or []:
        url = r.get("url", "")
        title = r.get("title", "")
        snippet = r.get("snippet", "")
        if "reddit.com/r/" in url and snippet:
            subreddit_signals.append(f"[Subreddit] {title}: {snippet[:150]}")

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

    # Build a trend block from every source that returned data. Missing
    # sources are simply omitted — the AI is instructed to treat absence
    # as silence, not decline.
    wiki_signals = trends.get("wikipedia_pageviews") or []
    gdelt_signal = trends.get("gdelt") or {}
    openalex_signal = trends.get("openalex") or {}
    github_signal = trends.get("github") or {}
    hn_signal = trends.get("hackernews_volume") or {}

    trend_lines: list[str] = []
    available_sources: list[str] = []

    if wiki_signals:
        available_sources.append("Wikipedia pageviews")
        for w in wiki_signals[:2]:
            trend_lines.append(
                f"- Wikipedia pageviews [{w['article']}]: "
                f"avg {w['avg_daily_views']}/day, trend={w['trend']} "
                f"({w['delta_pct']:+.1f}% early→late over {w['samples']} days)"
            )

    if gdelt_signal.get("available"):
        available_sources.append("GDELT news volume")
        trend_lines.append(
            f"- GDELT news volume [{gdelt_signal.get('query', '')}]: "
            f"avg {gdelt_signal.get('avg_daily_articles', 0)} articles/day, "
            f"trend={gdelt_signal.get('trend', 'unknown')} "
            f"({gdelt_signal.get('delta_pct', 0):+.1f}% early→late over "
            f"{gdelt_signal.get('samples', 0)} days)"
        )

    if openalex_signal.get("available") and openalex_signal.get("trend") != "unknown":
        available_sources.append("OpenAlex academic publications")
        trend_lines.append(
            f"- OpenAlex publications [{openalex_signal.get('query', '')}]: "
            f"{openalex_signal.get('total_works', 0)} total works, "
            f"{openalex_signal.get('last_year_count', 0)} in "
            f"{openalex_signal.get('last_year', '')} vs "
            f"{openalex_signal.get('prior_avg', 0)}/yr prior — "
            f"trend={openalex_signal.get('trend', 'unknown')} "
            f"({openalex_signal.get('delta_pct', 0):+.1f}%)"
        )

    if github_signal.get("available"):
        available_sources.append("GitHub repo creation")
        trend_lines.append(
            f"- GitHub repos [{github_signal.get('query', '')}]: "
            f"{github_signal.get('last_year_count', 0)} created last 12mo, "
            f"{github_signal.get('prior_year_count', 0)} the year before — "
            f"trend={github_signal.get('trend', 'unknown')} "
            f"({github_signal.get('delta_pct', 0):+.1f}%)"
        )

    if hn_signal.get("available"):
        available_sources.append("HackerNews volume")
        trend_lines.append(
            f"- HackerNews stories [{hn_signal.get('query', '')}]: "
            f"{hn_signal.get('last_year_stories', 0)} last 12mo, "
            f"{hn_signal.get('prior_year_stories', 0)} the year before — "
            f"trend={hn_signal.get('trend', 'unknown')} "
            f"({hn_signal.get('delta_pct', 0):+.1f}%)"
        )

    if not trend_lines:
        trend_lines.append("(no longitudinal trend data available — treat trend as unknown, not falling)")
    trend_block = "\n".join(trend_lines)
    sources_hint = ", ".join(available_sources) if available_sources else "none"
    consensus_label = trends.get("interest_trend", "unknown")
    per_source_summary = trends.get("per_source_trends") or {}
    consensus_block = (
        f"Cross-source consensus trend: {consensus_label} "
        f"(per-source labels: {per_source_summary or 'none'})"
    )

    crowdfunding_text = "\n".join(f"- {s}" for s in crowdfunding_snippets[:6]) if crowdfunding_snippets else "(none found)"
    subreddit_text = "\n".join(f"- {s}" for s in subreddit_signals[:4]) if subreddit_signals else "(none found)"
    news_text = "\n".join(f"- {s}" for s in news_snippets[:10]) or "(none found)"

    prompt = f"""You are a demand analyst. Assess demand for:

Market: {market}
Geography: {geography}
Product: {product or 'general'}

Longitudinal interest signals (available trend sources: {sources_hint}):
{trend_block}

{consensus_block}

Search result counts across intent types (more = higher demand signal):
{json.dumps(volume_counts, indent=2)}

Community discussions (Reddit/forums) — {len(reddit_posts)} posts sorted by upvotes:
{snippet_text or '(none found)'}

Google News headlines ({len(news_snippets)} articles):
{news_text}

Crowdfunding campaigns (Kickstarter/Indiegogo — proves willingness to pay):
{crowdfunding_text}

Subreddit / community presence signals:
{subreddit_text}

Return ONLY this JSON (no markdown fences):
{{
    "demand_score": <0-100 composite demand score>,
    "demand_score_evidence": "1 sentence citing the strongest signal that supports the score",
    "demand_trend": "<rising|stable|falling|unknown>",
    "demand_trend_sources": [
      {{"source": "Wikipedia pageviews", "trend_label": "rising", "delta_pct": 12.0}},
      {{"source": "GDELT news volume", "trend_label": "stable", "delta_pct": 4.0}}
    ],
    "demand_seasonality": "<seasonal pattern description or 'none detected'>",
    "demand_seasonality_amplitude": <0.0 to 1.0 float — peak vs trough variance, e.g. 0.30 = 30% swing; 0.0 if not seasonal>,
    "demand_pain_points": [
      {{"pain_point": "specific problem", "evidence": "subreddit/news quote or paraphrase", "source_url": "..."}}
    ],
    "demand_sources": ["Wikipedia pageviews", "Reddit community", "GDELT news volume", ...],
    "willingness_to_pay": "<high|medium|low|unknown>",
    "willingness_to_pay_evidence": "1 sentence on why (price points observed, paid alternatives, etc.) — 'unknown' is acceptable",
    "crowdfunding_signal": "<strong|moderate|weak|none>",
    "crowdfunding_evidence": "campaign URLs or 'none found'",
    "notes": "2-3 sentences on demand strength, evidence quality, and key insight"
}}

Citation rules:
- Every entry in demand_trend_sources must correspond to a source actually
  listed in the Longitudinal interest signals block above. Don't invent.
- Each pain_point must include an evidence snippet drawn from the community
  text above; never list pain points without evidence support.
- If you can't cite evidence for willingness_to_pay, return "unknown" — do
  not guess "medium" as a default.

Trend rules (read carefully):
- Default to the consensus label above unless one source has dramatically
  stronger evidence than the others.
- Use "unknown" — NOT "falling" — when no longitudinal source is available.
- "falling" requires multiple positive signals of decline across sources
  (e.g. Wikipedia pageviews AND GDELT news both ≤ -10%).
- One source falling while others are rising/stable → call it "stable" or
  "rising", not "falling".
- OpenAlex / GitHub / HackerNews "rising" is a leading indicator — emerging
  technology research and developer adoption typically precede commercial
  demand by 1-3 years. Weight these heavily for deep-tech / SaaS / infra
  markets even when current Wikipedia pageviews are flat.
- Only cite sources you actually saw above in "demand_sources".

Scoring guide:
- 75+: strong upward trend across multiple sources + active community +
       high search volume + crowdfunding evidence
- 50-74: moderate/mixed signals (e.g. rising in one source, stable in others)
- 25-49: weak signals or thin evidence
- <25: no meaningful demand detected (only when *positive* signals are weak
       — never penalise for absent trend data)
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
    from market_validation._helpers.citations import RULES_FOR_DEMAND, enforce_citations
    enforce_citations(result, RULES_FOR_DEMAND)
    return result

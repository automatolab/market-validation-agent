"""
Market Signals — aggregates leading indicators for market validation + AI synthesis.

Gathers data on:
- Job posting volume (hiring activity = demand proxy)
- News sentiment (media coverage)
- Regulatory environment (risks and changes)
- Technology maturity (adoption stage)

AI synthesis (claude/opencode) is required for structured output.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from market_validation.log import get_logger

_log = get_logger("market_signals")


def _search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results)
    except Exception as exc:
        _log.debug("multi_search.quick_search failed for %r: %s", query, exc)
        return []


def _collect(query: str, num_results: int = 10) -> tuple[int, list[str]]:
    """Search and return (result_count, non-empty snippets)."""
    results = _search(query, num_results)
    snippets = [r.get("snippet", "").strip() for r in results if r.get("snippet", "").strip()]
    return len(results), snippets


def _parallel_collect(
    queries: list[str], num_results: int = 10
) -> tuple[int, list[str]]:
    """Run ``_collect`` for many queries concurrently.

    Returns (total_result_count, concatenated_snippets) preserving query order.
    Replaces the serial ``for q in queries: _collect(q); time.sleep(1.2)``
    pattern that dominated wall-clock time in this module.
    """
    if not queries:
        return 0, []
    total = 0
    out: list[str] = []
    with ThreadPoolExecutor(max_workers=min(8, len(queries))) as pool:
        for cnt, snips in pool.map(lambda q: _collect(q, num_results), queries):
            total += cnt
            out.extend(snips)
    return total, out


def gather_market_signals(
    market: str,
    geography: str,
    product: str | None = None,
    run_ai: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Gather leading market indicators and synthesize with AI.

    Returns dict with job_posting_volume, news_sentiment, regulatory_risks,
    technology_maturity, and signals_data.
    """
    signals: dict[str, Any] = {}

    # Load market-aware queries
    try:
        from market_validation.query_context import get_validation_queries
        _sig_ctx = get_validation_queries(market, geography, product)["signals"]
    except Exception:
        _sig_ctx = {
            "jobs": [
                f"site:indeed.com {market} {geography}",
                f"site:linkedin.com/jobs {market} {geography}",
                f"{market} hiring jobs {geography}",
            ],
            "news_positive": [f"{market} growth expansion success {geography}"],
            "news_negative": [f"{market} shutdown decline layoff struggling"],
            "news_general": [f"{market} {geography} news 2025 2026"],
            "regulatory": [
                f"{market} regulation law {geography}",
                f"{market} compliance requirements new rules",
            ],
            "tech": [
                f"{market} technology innovation trend",
                f"{market} adoption growth emerging",
            ],
        }

    # All four signal categories are independent web searches. We schedule
    # them as four parallel batches; within each batch, individual queries
    # also fan out via ``_parallel_collect`` so the longest single search
    # (not the sum) dominates wall-clock.
    jobs_queries = list(_sig_ctx["jobs"][:4])
    news_pos_q = _sig_ctx["news_positive"][0]
    news_neg_q = _sig_ctx["news_negative"][0]
    news_gen_q = _sig_ctx["news_general"][0]
    regulatory_queries = list(_sig_ctx["regulatory"][:3])
    tech_queries = list(_sig_ctx["tech"][:3])

    with ThreadPoolExecutor(max_workers=4) as pool:
        f_jobs = pool.submit(_parallel_collect, jobs_queries, 10)
        f_pos = pool.submit(_collect, news_pos_q, 10)
        f_neg = pool.submit(_collect, news_neg_q, 10)
        f_gen = pool.submit(_collect, news_gen_q, 10)
        f_reg = pool.submit(_parallel_collect, regulatory_queries, 8)
        f_tech = pool.submit(_parallel_collect, tech_queries, 8)
        total_jobs, job_snippets = f_jobs.result()
        pos_count, pos_snippets = f_pos.result()
        neg_count, neg_snippets = f_neg.result()
        gen_count, gen_snippets = f_gen.result()
        reg_total, reg_snippets = f_reg.result()
        tech_total, tech_snippets = f_tech.result()

    signals["jobs"] = {"total_count": total_jobs, "snippets": job_snippets[:5]}
    signals["news"] = {
        "positive_count": pos_count,
        "negative_count": neg_count,
        "general_count": gen_count,
        "snippets": (pos_snippets + neg_snippets + gen_snippets)[:12],
    }
    signals["regulatory"] = {"count": reg_total, "snippets": reg_snippets[:8]}
    signals["technology"] = {"count": tech_total, "snippets": tech_snippets[:8]}

    # BLS employment data + Google News RSS — both keyed on free APIs. Run
    # them concurrently with the BLS detect_market_category call sitting
    # inline (cheap, pure Python) and the network calls fanning out.
    bls_data: dict[str, Any] = {}
    news_articles: list[dict] = []
    try:
        from market_validation.free_data_sources import (
            bls_industry_data,
            google_news_rss,
        )
        from market_validation.query_context import detect_market_category
        _cat = detect_market_category(market, product)

        with ThreadPoolExecutor(max_workers=2) as pool:
            f_bls = pool.submit(bls_industry_data, _cat)
            f_news = pool.submit(google_news_rss, f"{market} {geography}", 15)
            try:
                bls_data = f_bls.result() or {}
            except Exception as exc:
                _log.debug("bls_industry_data failed: %s", exc)
            try:
                news_articles = f_news.result() or []
            except Exception as exc:
                _log.debug("google_news_rss failed: %s", exc)

        if not news_articles:
            try:
                news_articles = google_news_rss(market, limit=10)
            except Exception as exc:
                _log.debug("google_news_rss fallback failed: %s", exc)

        if bls_data.get("employment"):
            signals["bls"] = {"count": 1, "snippets": [bls_data["snippet"]]}
        if news_articles:
            signals["google_news"] = {
                "count": len(news_articles),
                "snippets": [
                    f"[{a.get('source_name','')} {a.get('published','')}] {a.get('title','')}"
                    for a in news_articles[:8]
                ],
            }
    except Exception as exc:
        _log.debug("market_signals BLS/news block failed: %s", exc)

    # Tech-maturity leading indicators: academic research (OpenAlex),
    # developer adoption (GitHub repo creation), community attention
    # (HackerNews story volume). These distinguish emerging vs. mature vs.
    # declining markets far better than search snippet counts — but only
    # for tech-leaning markets. For "BBQ catering" they all return ~0
    # results, so we skip them and save 1-3s per validation.
    _TECH_CATEGORIES = {"saas", "industrial", "general"}
    openalex_data: dict[str, Any] = {}
    github_data: dict[str, Any] = {}
    hn_volume_data: dict[str, Any] = {}
    try:
        from market_validation.free_data_sources import (
            github_repo_growth,
            hackernews_volume_timeline,
            openalex_works_timeline,
        )
        from market_validation.query_context import detect_market_category
        category = detect_market_category(market, product)
        if category in _TECH_CATEGORIES:
            # Three independent feeds — fetch concurrently to save ~1s.
            # NOTE: only `as_completed` is imported here; `ThreadPoolExecutor`
            # comes from the top-of-file import. Re-importing it here would
            # rebind the name as a function-local and trigger UnboundLocalError
            # at the earlier callsites in this same function.
            from concurrent.futures import as_completed
            tasks = {
                "openalex": (openalex_works_timeline, (market,), {"years": 5}),
                "github": (github_repo_growth, (market,), {}),
                "hn": (hackernews_volume_timeline, (market,), {}),
            }
            with ThreadPoolExecutor(max_workers=3) as pool:
                futs = {pool.submit(fn, *args, **kw): name for name, (fn, args, kw) in tasks.items()}
                for fut in as_completed(futs):
                    name = futs[fut]
                    try:
                        value = fut.result() or {}
                    except Exception as exc:
                        _log.debug("market_signals %s feed failed: %s", name, exc)
                        continue
                    if name == "openalex":
                        openalex_data = value
                    elif name == "github":
                        github_data = value
                    elif name == "hn":
                        hn_volume_data = value
    except Exception as exc:
        _log.debug("market_signals tech-maturity feeds failed: %s", exc)
    if openalex_data.get("available"):
        signals["openalex"] = {
            "count": openalex_data.get("total_works", 0),
            "snippets": [openalex_data.get("snippet", "")],
        }
    if github_data.get("available"):
        signals["github"] = {
            "count": github_data.get("last_year_count", 0),
            "snippets": [github_data.get("snippet", "")],
        }
    if hn_volume_data.get("available"):
        signals["hackernews_volume"] = {
            "count": hn_volume_data.get("last_year_stories", 0),
            "snippets": [hn_volume_data.get("snippet", "")],
        }

    result: dict[str, Any] = {
        "signals_data": signals,
        "bls_data": bls_data,
        "news_article_count": len(news_articles),
        "openalex_data": openalex_data,
        "github_data": github_data,
        "hackernews_volume_data": hn_volume_data,
    }

    # Magnitude-based sentiment score: -1 (all negative) to +1 (all positive).
    # Replaces the old directional-only "positive"/"mixed"/"negative" label —
    # callers can still use that, but the magnitude is what the scorecard
    # actually wants for weighting.
    _total_news = pos_count + neg_count
    sentiment_score = ((pos_count - neg_count) / _total_news) if _total_news > 0 else 0.0
    result["news_sentiment_score"] = round(sentiment_score, 3)
    result["news_sentiment_pos_count"] = pos_count
    result["news_sentiment_neg_count"] = neg_count

    if not run_ai:
        # Basic heuristics when no AI available
        result["job_posting_volume"] = "high" if total_jobs >= 15 else "medium" if total_jobs >= 5 else "low"
        result["news_sentiment"] = (
            "positive" if sentiment_score >= 0.4
            else "negative" if sentiment_score <= -0.4
            else "mixed"
        )
        result["regulatory_risks"] = []
        result["technology_maturity"] = "growing"
        return result

    # Build compact signal summary for AI
    all_snippets_text = ""
    for category, data in signals.items():
        snippets = data.get("snippets", [])
        count = data.get("total_count") or data.get("count") or (data.get("positive_count", 0) + data.get("negative_count", 0))
        if snippets or count:
            all_snippets_text += f"\n{category.upper()} ({count} results):\n"
            all_snippets_text += "\n".join(f"  - {s[:120]}" for s in snippets[:4])

    bls_context = ""
    if bls_data.get("employment"):
        bls_context = (
            f"\nBLS.gov authoritative employment data:\n"
            f"- {bls_data['label']}: {bls_data['employment']:,} US workers as of {bls_data['period']}\n"
            f"- Year-over-year: {bls_data['yoy_change_pct']:+.1f}% ({bls_data['trend']})\n"
        )

    # Google News headlines in AI context
    news_context = ""
    if news_articles:
        news_lines = "\n".join(
            f"  [{a.get('published','')} {a.get('source_name','')}] {a.get('title','')}"
            for a in news_articles[:10]
        )
        news_context = f"\nGoogle News headlines (real articles, no key needed):\n{news_lines}\n"

    maturity_lines: list[str] = []
    if openalex_data.get("available"):
        maturity_lines.append(f"  - OpenAlex: {openalex_data.get('snippet', '')}")
    if github_data.get("available"):
        maturity_lines.append(f"  - GitHub: {github_data.get('snippet', '')}")
    if hn_volume_data.get("available"):
        maturity_lines.append(f"  - HackerNews: {hn_volume_data.get('snippet', '')}")
    maturity_context = (
        "\nLeading-indicator feeds (research / developer / community attention):\n"
        + "\n".join(maturity_lines) + "\n"
        if maturity_lines else ""
    )

    prompt = f"""You are a market intelligence analyst. Assess market signals for:

Market: {market}
Geography: {geography}
Product: {product or 'general'}
{bls_context}
Signal data gathered from web searches:
- Job postings: {total_jobs} results across Indeed, LinkedIn, general
- Positive news signals: {pos_count} results
- Negative news signals: {neg_count} results
- Regulatory mentions: {signals['regulatory']['count']} results
- Technology/adoption mentions: {signals['technology']['count']} results
- Google News articles: {len(news_articles)} real headlines
{news_context}{maturity_context}{all_snippets_text}

Return ONLY this JSON (no markdown fences):
{{
    "job_posting_volume": "<high|medium|low|none>",
    "job_posting_evidence": "1 sentence — actual posting count + source if known",
    "news_sentiment": "<positive|mixed|negative>",
    "news_sentiment_score": <-1.0 to +1.0 float — magnitude not just direction>,
    "regulatory_risks": [
      {{"risk": "USDA inspection required", "source_url": "https://...", "evidence": "1 line of supporting text"}}
    ],
    "technology_maturity": "<emerging|growing|mature|declining>",
    "technology_maturity_sources": [
      {{"feed": "OpenAlex", "trend": "rising", "delta_pct": 25.0}},
      {{"feed": "GitHub", "trend": "rising", "delta_pct": 67.0}}
    ],
    "key_trends": [
      {{"trend": "specific trend", "evidence": "headline or quote", "source_url": "..."}}
    ],
    "timing_assessment": "<good|neutral|poor>",
    "timing_evidence": "1 sentence on what makes timing good/neutral/poor",
    "notes": "1-2 sentences on overall market timing and signals"
}}

Citation rules:
- Every regulatory_risk and key_trend MUST be backed by a snippet or URL
  drawn from the evidence block above. Don't list a risk you can't cite.
- technology_maturity_sources must reference the feeds actually listed in
  the leading-indicator block above (OpenAlex / GitHub / HackerNews).
- If evidence is thin, prefer "growing" + lower confidence in notes over
  "declining" — research/dev signals usually decline AFTER demand.

Definitions:
- job_posting_volume: use BLS employment trend if available, otherwise: high = 15+, medium = 5-14, low = 1-4
- technology_maturity: use the leading-indicator feeds when available —
  - emerging: OpenAlex rising AND HackerNews/GitHub rising, low total works
  - growing: GitHub/HackerNews rising, OpenAlex high and rising
  - mature: high totals across all three, all stable
  - declining: all three falling consistently
- If feeds disagree, prefer "growing" over "declining" — research and dev
  adoption usually decline AFTER commercial demand, so a single falling feed
  doesn't yet mean the market is dying.
- Regulatory risks: be specific (e.g. "USDA inspection required for commercial meat sales") not vague ("regulations exist")
- If evidence is thin, say so in notes rather than fabricating signals"""

    ai_result = run_ai(prompt)
    parsed: dict[str, Any] = {}
    if isinstance(ai_result, dict):
        if "technology_maturity" in ai_result:
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
    result.setdefault("regulatory_risks", [])
    result.setdefault("technology_maturity", "growing")
    from market_validation._helpers.citations import RULES_FOR_SIGNALS, enforce_citations
    enforce_citations(result, RULES_FOR_SIGNALS)
    return result

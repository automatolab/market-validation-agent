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
import time
from collections.abc import Callable
from typing import Any


def _search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results)
    except Exception:
        return []


def _collect(query: str, num_results: int = 10) -> tuple[int, list[str]]:
    """Search and return (result_count, non-empty snippets)."""
    results = _search(query, num_results)
    snippets = [r.get("snippet", "").strip() for r in results if r.get("snippet", "").strip()]
    return len(results), snippets


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

    # 1. Job postings
    job_total = 0
    job_snippets: list[str] = []
    for q in _sig_ctx["jobs"][:4]:
        cnt, snips = _collect(q, 10)
        job_total += cnt
        job_snippets.extend(snips)
        time.sleep(1.2)
    total_jobs = job_total
    signals["jobs"] = {"total_count": total_jobs, "snippets": job_snippets[:5]}

    # 2. News — positive and negative separately
    pos_count, pos_snippets = _collect(_sig_ctx["news_positive"][0], 10)
    time.sleep(1.2)
    neg_count, neg_snippets = _collect(_sig_ctx["news_negative"][0], 10)
    time.sleep(1.2)
    gen_count, gen_snippets = _collect(_sig_ctx["news_general"][0], 10)
    time.sleep(1.2)
    signals["news"] = {
        "positive_count": pos_count,
        "negative_count": neg_count,
        "general_count": gen_count,
        "snippets": (pos_snippets + neg_snippets + gen_snippets)[:12],
    }

    # 3. Regulatory environment (market-aware)
    reg_total = 0
    reg_snippets: list[str] = []
    for q in _sig_ctx["regulatory"][:3]:
        cnt, snips = _collect(q, 8)
        reg_total += cnt
        reg_snippets.extend(snips)
        time.sleep(1.2)
    signals["regulatory"] = {
        "count": reg_total,
        "snippets": reg_snippets[:8],
    }

    # 4. Technology maturity (market-aware)
    tech_total = 0
    tech_snippets: list[str] = []
    for q in _sig_ctx["tech"][:3]:
        cnt, snips = _collect(q, 8)
        tech_total += cnt
        tech_snippets.extend(snips)
        time.sleep(1.2)
    signals["technology"] = {
        "count": tech_total,
        "snippets": tech_snippets[:8],
    }

    # BLS employment data — authoritative job volume signal, no key needed
    bls_data: dict[str, Any] = {}
    try:
        from market_validation.free_data_sources import bls_industry_data
        from market_validation.query_context import detect_market_category
        _cat = detect_market_category(market, product)
        bls_data = bls_industry_data(_cat)
        if bls_data.get("employment"):
            signals["bls"] = {
                "count": 1,
                "snippets": [bls_data["snippet"]],
            }
        time.sleep(0.5)
    except Exception:
        pass

    # Google News RSS — real news headlines, completely free, no key
    news_articles: list[dict] = []
    try:
        from market_validation.free_data_sources import google_news_rss
        news_articles = google_news_rss(f"{market} {geography}", limit=15)
        if not news_articles:
            news_articles = google_news_rss(market, limit=10)
        if news_articles:
            signals["google_news"] = {
                "count": len(news_articles),
                "snippets": [
                    f"[{a.get('source_name','')} {a.get('published','')}] {a.get('title','')}"
                    for a in news_articles[:8]
                ],
            }
    except Exception:
        pass

    result: dict[str, Any] = {
        "signals_data": signals,
        "bls_data": bls_data,
        "news_article_count": len(news_articles),
    }

    if not run_ai:
        # Basic heuristics when no AI available
        result["job_posting_volume"] = "high" if total_jobs >= 15 else "medium" if total_jobs >= 5 else "low"
        result["news_sentiment"] = "positive" if pos_count > neg_count * 2 else "negative" if neg_count > pos_count * 2 else "mixed"
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
{news_context}{all_snippets_text}

Return ONLY this JSON (no markdown fences):
{{
    "job_posting_volume": "<high|medium|low|none>",
    "news_sentiment": "<positive|mixed|negative>",
    "regulatory_risks": ["specific risk 1", "specific risk 2"],
    "technology_maturity": "<emerging|growing|mature|declining>",
    "key_trends": ["specific trend 1", "specific trend 2"],
    "timing_assessment": "<good|neutral|poor>",
    "notes": "1-2 sentences on overall market timing and signals"
}}

Definitions:
- job_posting_volume: use BLS employment trend if available, otherwise: high = 15+, medium = 5-14, low = 1-4
- technology_maturity: emerging = pre-PMF, growing = rapid adoption, mature = commoditized, declining = being replaced
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
    return result

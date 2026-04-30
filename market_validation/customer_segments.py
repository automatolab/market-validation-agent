"""
Customer Segments — identifies who specifically would buy, what they spend,
and what triggers a purchase decision.

Gathers data on:
- Buyer personas (who actually makes the purchase decision)
- Budget signals (what they spend annually)
- Buying process (how they buy, how long it takes)
- Job posting proxy (budget allocation proxy via hiring patterns)
- Reddit signals (who's asking questions = who has the problem)

Produces primary and secondary segment profiles plus an ICP clarity score.

AI synthesis (claude/opencode) is required for structured output.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from market_validation.log import get_logger

_log = get_logger("customer_segments")


def _search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results)
    except Exception as exc:
        _log.debug("multi_search.quick_search failed for %r: %s", query, exc)
        return []


def _snippets(query: str, num_results: int = 10) -> list[str]:
    """Search and return non-empty snippets."""
    results = _search(query, num_results)
    return [r.get("snippet", "").strip() for r in results if r.get("snippet", "").strip()]


def _parallel_snippets(queries: list[str], num_results: int = 10) -> list[str]:
    """Run ``_snippets`` for many queries concurrently, preserving order.

    The serial pattern was 11 × ``time.sleep(1.2)`` (≈13s) inside this
    module; switching to a thread pool drops total wait to roughly the
    slowest individual search.
    """
    if not queries:
        return []
    out: list[str] = []
    with ThreadPoolExecutor(max_workers=min(8, len(queries))) as pool:
        for batch in pool.map(lambda q: _snippets(q, num_results), queries):
            out.extend(batch)
    return out


def identify_customer_segments(
    market: str,
    geography: str,
    product: str | None = None,
    archetype: str = "b2b-industrial",
    run_ai: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Identify customer segments — who buys, what they spend, and what triggers purchase.

    Args:
        market: Market description (e.g. "BBQ catering San Jose")
        geography: Target geography (e.g. "San Jose, CA")
        product: Specific product or service (optional)
        archetype: Market archetype string used to tailor searches.
            Common values: "b2b-industrial", "local-service", "b2b-saas",
            "consumer", "marketplace".
        run_ai: AI callable (Agent._run). Required for structured output.

    Returns:
        Dict with primary_segment, secondary_segment, icp_clarity score,
        total_reachable_buyers, avg_deal_size, sales_motion, and notes.
    """
    search_term = product or market

    persona_queries = [
        f"who buys {search_term} customer type",
        f"{market} target customer small business enterprise consumer",
        f"site:linkedin.com {market} buyer manager director purchasing",
    ]
    budget_queries = [
        f"{market} budget annual spend {geography}",
        f"{search_term} price enterprise SMB startup cost",
        f"how much do companies spend on {search_term}",
    ]
    process_queries = [
        f"how to sell {search_term} sales process",
        f"{market} procurement decision buying committee",
        f"{market} sales cycle length close time",
    ]
    job_queries = [
        f"site:indeed.com {market} manager director {geography}",
        f"{market} operations manager job description budget",
    ]

    # All four categories are independent — fetch each list in parallel,
    # and within each list the per-query searches also fan out concurrently.
    with ThreadPoolExecutor(max_workers=4) as pool:
        f_pers = pool.submit(_parallel_snippets, persona_queries)
        f_budg = pool.submit(_parallel_snippets, budget_queries)
        f_proc = pool.submit(_parallel_snippets, process_queries)
        f_jobs = pool.submit(_parallel_snippets, job_queries)
        persona_snippets = f_pers.result()[:6]
        budget_snippets = f_budg.result()[:6]
        process_snippets = f_proc.result()[:6]
        job_snippets = f_jobs.result()[:6]

    # ------------------------------------------------------------------
    # Reddit signals — subreddit distribution as customer segment proxy
    # ------------------------------------------------------------------
    reddit_posts: list[dict[str, Any]] = []
    reddit_context: str = ""
    try:
        from market_validation.free_data_sources import reddit_search
        from market_validation.market_archetype import detect_archetype
        _cat_key, _ = detect_archetype(market, product)
        reddit_posts = reddit_search(f"{search_term} recommendation", category=_cat_key, limit=15)
        if reddit_posts:
            # Summarise subreddit distribution as a segment signal
            subreddits: dict[str, int] = {}
            for post in reddit_posts:
                sr = post.get("subreddit", "unknown")
                subreddits[sr] = subreddits.get(sr, 0) + 1
            sr_summary = ", ".join(
                f"r/{sr} ({cnt})" for sr, cnt in sorted(subreddits.items(), key=lambda x: -x[1])
            )
            reddit_context = f"Reddit subreddit distribution (who's asking): {sr_summary}"
    except Exception as exc:
        _log.debug("customer_segments reddit signal failed: %s", exc)
        reddit_posts = []

    # ------------------------------------------------------------------
    # Raw return (no AI)
    # ------------------------------------------------------------------
    raw_data: dict[str, Any] = {
        "persona": persona_snippets,
        "budget": budget_snippets,
        "process": process_snippets,
        "jobs": job_snippets,
        "reddit_posts_count": len(reddit_posts),
    }

    if not run_ai:
        return {"raw_snippets": raw_data}

    # ------------------------------------------------------------------
    # Build AI prompt
    # ------------------------------------------------------------------
    def _fmt(label: str, items: list[str]) -> str:
        if not items:
            return f"\n{label}:\n  (no data gathered)\n"
        lines = "\n".join(f"  - {s[:160]}" for s in items)
        return f"\n{label}:\n{lines}\n"

    snippets_text = (
        _fmt("BUYER PERSONA (who makes the decision)", persona_snippets)
        + _fmt("BUDGET SIGNALS (what they spend)", budget_snippets)
        + _fmt("BUYING PROCESS (how they buy and how long it takes)", process_snippets)
        + _fmt("JOB POSTING PROXY (who has budget allocation)", job_snippets)
    )

    reddit_text = f"\n{reddit_context}\n" if reddit_context else ""

    prompt = f"""You are a customer research analyst. Identify the most likely buyer segments for:

Market: {market}
Geography: {geography}
Product/Service: {product or 'general'}
Archetype: {archetype}
{reddit_text}
Evidence gathered from web searches:
{snippets_text}

Return ONLY this JSON (no markdown fences):
{{
    "primary_segment": {{
        "name": "<descriptive segment name>",
        "buyer_persona": "<title + role of the human who actually decides, e.g. 'VP Operations at multi-site QSR'>",
        "size_estimate": "<estimated count in target geography>",
        "size_evidence": "<source_url or snippet that supports the count>",
        "annual_budget": "<range or typical annual spend on this category>",
        "budget_evidence": "<source_url or snippet>",
        "buying_trigger": "<what event or pain makes them buy>",
        "buying_process": "<how they evaluate and decide, e.g. rep relationship, RFP, self-serve>",
        "pain_points": ["<pain 1>", "<pain 2>"]
    }},
    "secondary_segment": {{
        "name": "<descriptive segment name>",
        "buyer_persona": "<title + role>",
        "size_estimate": "<estimated count>",
        "size_evidence": "<source_url or snippet>",
        "annual_budget": "<range>",
        "budget_evidence": "<source_url or snippet>",
        "buying_trigger": "<what triggers purchase>",
        "buying_process": "<how they evaluate and decide>",
        "pain_points": ["<pain 1>", "<pain 2>"]
    }},
    "persona_clarity": <0-100 — how confidently can we name the buyer's role and title>,
    "budget_clarity": <0-100 — how well do we know the annual spend bracket>,
    "trigger_clarity": <0-100 — how clear is the event that drives purchase>,
    "icp_clarity": <0-100 — average of the three above; do not exceed the lowest sub-score by more than 15>,
    "total_reachable_buyers": "<estimated total addressable buyers in geography>",
    "avg_deal_size": "<typical annual contract or transaction value>",
    "sales_motion": "<direct|channel|product-led|self-serve>",
    "notes": "<1-2 sentences on customer segment clarity and accessibility>"
}}

ICP clarity scoring guide:
- 75+: Clear identifiable buyer (named title + role), known budget bracket, defined trigger.
- 50-74: Buyer titled but budget or trigger unclear — needs further discovery.
- 25-49: Multiple possible buyers, unclear who actually decides.
- <25: No clear buyer identified — market definition too broad or too early.

Required: buyer_persona must include a specific title (e.g. "VP Operations",
"Head of IT") not a generic phrase like "decision-maker". If you can't name
the title, set persona_clarity < 30 honestly.

Each numeric size or budget estimate must include an `_evidence` field — a
source URL or quoted snippet. Estimates without evidence reduce the
corresponding clarity sub-score.

Be specific with size estimates and budgets. If evidence is thin, use ranges and note the uncertainty in notes."""

    ai_result = run_ai(prompt)

    result: dict[str, Any] = {"raw_snippets": raw_data}

    parsed: dict[str, Any] = {}
    if isinstance(ai_result, dict):
        if "primary_segment" in ai_result:
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

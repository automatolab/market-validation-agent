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
import time
from typing import Any, Callable


def _search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    try:
        from market_validation.multi_search import quick_search
        return quick_search(query, num_results)
    except Exception:
        return []


def _snippets(query: str, num_results: int = 10) -> list[str]:
    """Search and return non-empty snippets."""
    results = _search(query, num_results)
    return [r.get("snippet", "").strip() for r in results if r.get("snippet", "").strip()]


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

    # ------------------------------------------------------------------
    # Category 1 — Buyer persona
    # ------------------------------------------------------------------
    persona_snippets: list[str] = []

    persona_snippets.extend(_snippets(f"who buys {search_term} customer type", 10))
    time.sleep(1.2)
    persona_snippets.extend(_snippets(f"{market} target customer small business enterprise consumer", 10))
    time.sleep(1.2)
    persona_snippets.extend(_snippets(f"site:linkedin.com {market} buyer manager director purchasing", 10))
    time.sleep(1.2)

    persona_snippets = persona_snippets[:6]

    # ------------------------------------------------------------------
    # Category 2 — Budget signals
    # ------------------------------------------------------------------
    budget_snippets: list[str] = []

    budget_snippets.extend(_snippets(f"{market} budget annual spend {geography}", 10))
    time.sleep(1.2)
    budget_snippets.extend(_snippets(f"{search_term} price enterprise SMB startup cost", 10))
    time.sleep(1.2)
    budget_snippets.extend(_snippets(f"how much do companies spend on {search_term}", 10))
    time.sleep(1.2)

    budget_snippets = budget_snippets[:6]

    # ------------------------------------------------------------------
    # Category 3 — Buying process
    # ------------------------------------------------------------------
    process_snippets: list[str] = []

    process_snippets.extend(_snippets(f"how to sell {search_term} sales process", 10))
    time.sleep(1.2)
    process_snippets.extend(_snippets(f"{market} procurement decision buying committee", 10))
    time.sleep(1.2)
    process_snippets.extend(_snippets(f"{market} sales cycle length close time", 10))
    time.sleep(1.2)

    process_snippets = process_snippets[:6]

    # ------------------------------------------------------------------
    # Category 4 — Job posting proxy
    # ------------------------------------------------------------------
    job_snippets: list[str] = []

    job_snippets.extend(_snippets(f"site:indeed.com {market} manager director {geography}", 10))
    time.sleep(1.2)
    job_snippets.extend(_snippets(f"{market} operations manager job description budget", 10))
    time.sleep(1.2)

    job_snippets = job_snippets[:6]

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
    except Exception:
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
        "size_estimate": "<estimated count in target geography>",
        "annual_budget": "<range or typical annual spend on this category>",
        "buying_trigger": "<what event or pain makes them buy>",
        "buying_process": "<how they evaluate and decide, e.g. rep relationship, RFP, self-serve>",
        "pain_points": ["<pain 1>", "<pain 2>"]
    }},
    "secondary_segment": {{
        "name": "<descriptive segment name>",
        "size_estimate": "<estimated count in target geography>",
        "annual_budget": "<range or typical annual spend>",
        "buying_trigger": "<what triggers purchase>",
        "buying_process": "<how they evaluate and decide>",
        "pain_points": ["<pain 1>", "<pain 2>"]
    }},
    "icp_clarity": <0-100>,
    "total_reachable_buyers": "<estimated total addressable buyers in geography>",
    "avg_deal_size": "<typical annual contract or transaction value>",
    "sales_motion": "<direct|channel|product-led|self-serve>",
    "notes": "<1-2 sentences on customer segment clarity and accessibility>"
}}

ICP clarity scoring guide:
- 75+: Clear identifiable buyer, known budget, defined trigger — you know exactly who to call
- 50-74: Buyer identified but budget or trigger unclear — needs further discovery
- 25-49: Multiple possible buyers, unclear who actually decides — go-to-market is uncertain
- <25: No clear buyer identified — market definition too broad or too early

Sales motion definitions:
- direct: field/inside sales rep sells to buyer one-to-one
- channel: distributor, reseller, or partner sells on your behalf
- product-led: product drives acquisition, usage triggers conversion (PLG)
- self-serve: buyer finds and buys without a sales rep

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

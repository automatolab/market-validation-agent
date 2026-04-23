"""AI-driven helpers for the validate/find pipeline: candidate validation,
search-strategy prompting, heuristic search hints, and the terminal summary
printer."""

from __future__ import annotations

from typing import Any

from market_validation._helpers.common import infer_market_profile
from market_validation.log import get_logger

_log = get_logger("validation_helpers")


def ai_validate_companies(
    candidates: list[dict[str, Any]],
    market: str,
    geography: str,
    business_type: str,
    run_ai: Any,
) -> list[dict[str, Any]]:
    """
    Use Claude as the final quality gate before writing companies to the database.

    Sends all candidates in a single batch call. Claude:
    - Confirms each is a real operating business relevant to the market
    - Cleans the business name (strips ratings, platform names, page titles)
    - Deduplicates (marks duplicates as keep=false)
    - Rejects unrelated businesses, directories, maps, articles, social pages

    Returns only the validated entries with cleaned names.
    Falls back to the original list on any failure.
    """
    if not candidates:
        return []

    lines = []
    for i, c in enumerate(candidates):
        name = c.get("company_name", "")
        url = (c.get("website") or c.get("evidence_url") or "")[:80]
        snippet = (c.get("description") or "")[:180]
        lines.append(f'  {i}. name="{name}" url="{url}" snippet="{snippet}"')

    prompt = f"""You are a data quality agent for a market research pipeline.

We are building a lead list of: {business_type}
Geography: {geography} (include the wider metro area — nearby cities count)
Market context: {market}

Review every candidate below. For each, decide:
- Is it a REAL OPERATING BUSINESS relevant to {business_type}?
  REJECT: directories, maps, articles, recipes, social posts, unrelated companies, duplicates
  KEEP: real businesses even if in nearby cities within the same metro area
- What is the CLEAN business name? Strip ratings, page section prefixes (Menu |, Order |), platform suffixes (- Yelp, | TikTok), listicle language.
- Is it a DUPLICATE of another candidate? (keep only the first occurrence)

IMPORTANT: Be INCLUSIVE not exclusive. If a business plausibly operates in or serves the {geography} metro area, KEEP it.
Only reject businesses that are clearly in a DIFFERENT metro area (e.g. New York vs San Jose) or a different state.

Candidates:
{chr(10).join(lines)}

Return ONLY a JSON array — one object per candidate, in the same order. No markdown:
[
  {{"index": 0, "keep": true, "clean_name": "Business Name", "reason": "real business in metro area"}},
  {{"index": 1, "keep": false, "clean_name": "", "reason": "article, not a business"}},
  ...
]"""

    try:
        raw = run_ai(prompt)
        text = None
        if isinstance(raw, dict):
            companies_val = raw.get("companies")
            if isinstance(companies_val, list):
                # _parse_json_from_text wraps JSON arrays as {"companies": [...]}
                # Check if the items are validation results (have "index"/"keep") or
                # actual company objects (have "company_name"). Handle both.
                if companies_val and isinstance(companies_val[0], dict):
                    if "index" in companies_val[0] or "keep" in companies_val[0]:
                        import json as _j
                        text = _j.dumps(companies_val)
                    else:
                        # Claude returned companies, not validation results. Fall back.
                        return candidates
                elif not companies_val:
                    return []
                else:
                    import json as _j
                    text = _j.dumps(companies_val)
            else:
                text = raw.get("text") or raw.get("content") or None
                if not text:
                    import json as _j
                    text = _j.dumps(raw)
        elif isinstance(raw, str):
            text = raw

        if not text:
            return candidates

        import json as _j
        import re as _re
        text = _re.sub(r"^```[a-z]*\n?", "", text.strip())
        text = _re.sub(r"\n?```$", "", text.strip())
        parsed = _j.loads(text)

        validated: list[dict[str, Any]] = []
        for item in parsed:
            if not item.get("keep"):
                _log.info(
                    "[find:validate] REJECT [%s] %r — %s",
                    item.get("index"),
                    candidates[item["index"]].get("company_name", "?"),
                    item.get("reason", ""),
                )
                continue
            idx = item.get("index", -1)
            if not (0 <= idx < len(candidates)):
                continue
            c = dict(candidates[idx])
            clean = (item.get("clean_name") or "").strip()
            if clean:
                c["company_name"] = clean
            _log.info("[find:validate] KEEP  [%s] %r", idx, c["company_name"])
            validated.append(c)

        if parsed:
            _log.info("[find:validate] Validation complete: %d/%d kept", len(validated), len(candidates))
            return validated
        return candidates

    except Exception as e:
        _log.warning("[find:validate] AI validation failed: %s — keeping all candidates", e, exc_info=True)
        return candidates


def ai_search_strategy(
    market: str,
    geography: str,
    product: str | None,
    run_ai: Any,
    archetype_context: str | None = None,
) -> dict[str, Any] | None:
    """
    Ask the LLM to generate a search strategy for this market.

    Returns a dict with:
      - queries: list of search strings to run
      - real_business_signals: tokens/phrases that indicate a real business
      - junk_signals: tokens/phrases that indicate a junk result
      - business_type: plain-English description (e.g. "BBQ restaurant")

    Returns None if the AI call fails.
    """
    _arch_hint = ""
    if archetype_context:
        _arch_hint = f"\nArchetype guidance:\n{archetype_context}\n"

    prompt = f"""You are a market research strategist. Given a market and geography, figure out:
1. What is the NATURE of this market? (product, service, ingredient/supply chain, technology, etc.)
2. Who are the TARGET BUSINESSES to research? (the ones that BUY, SELL, or PROVIDE this thing)
3. What search queries will find their actual business websites (not articles, reviews, or directories)?

Market: {market}
Geography: {geography}
Product/context: {product or 'general'}
{_arch_hint}
Think step by step:
- If this is a RAW INGREDIENT or PRODUCT (e.g. "brisket", "organic cotton", "steel"), the target businesses
  are those that BUY it (restaurants, manufacturers) AND those that SELL/DISTRIBUTE it (wholesalers, suppliers).
- If this is a SERVICE (e.g. "pet grooming", "accounting"), the target businesses are those that PROVIDE the service.
- If this is a TECHNOLOGY/SOFTWARE, the target businesses are companies building or selling it.
- If ambiguous, cover multiple angles.

Return ONLY this JSON (no markdown fences):
{{
  "market_nature": "<product|service|ingredient|technology|marketplace|other>",
  "business_type": "<one phrase describing the primary type of business to find>",
  "target_description": "<who are we looking for and why — 1 sentence>",
  "queries": [
    "<search query 1 — most targeted>",
    "<search query 2 — different angle (e.g. suppliers, distributors)>",
    "<search query 3 — local directories or listings>",
    "<search query 4 — catering, wholesale, or services>",
    "<search query 5 — nearby metro area / wider geography>",
    "<search query 6 — industry-specific terms>",
    "<search query 7 — another angle>",
    "<search query 8 — final angle>"
  ],
  "real_business_signals": ["<word/phrase in real business titles/URLs>", ...],
  "junk_signals": ["<word/phrase that indicates NOT a real business>", ...]
}}

Rules for queries:
- Each query should find a DIFFERENT type of business or angle (buyers, sellers, suppliers, providers)
- Include {geography} in most queries
- Aim for business homepages with contact info, not aggregator or review sites
- Think like a B2B sales researcher who needs phone numbers and emails"""

    try:
        result = run_ai(prompt)
        if isinstance(result, dict):
            if "queries" in result:
                return result
            if "text" in result:
                import json as _json
                return _json.loads(result["text"])
        if isinstance(result, str):
            import json as _json
            return _json.loads(result)
    except Exception as e:
        _log.warning("[find] AI search strategy failed: %s", e)
    return None


def ai_search_hints(market: str, geography: str, product: str | None) -> tuple[str, str]:
    search_term = product or market
    category = infer_market_profile(market, product)["category"]

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


def print_validation_summary(
    market: str,
    geography: str,
    archetype_key: str,
    scorecard: dict,
    sizing: dict,
    competition: dict,
) -> None:
    """Print a concise, readable validation summary to the terminal."""
    verdict = scorecard.get("verdict", "unknown")
    overall = scorecard.get("overall_score", 0)

    verdict_icons = {
        "strong_go": "✓✓ STRONG GO",
        "go":        "✓  GO",
        "cautious":  "~  CAUTIOUS",
        "no_go":     "✗  NO GO",
    }
    verdict_label = verdict_icons.get(verdict, verdict.upper())

    tam_low = sizing.get("tam_low") or 0
    tam_high = sizing.get("tam_high") or 0

    def _fmt_money(n: float) -> str:
        if n >= 1_000_000_000:
            return f"${n/1_000_000_000:.1f}B"
        if n >= 1_000_000:
            return f"${n/1_000_000:.0f}M"
        if n >= 1_000:
            return f"${n/1_000:.0f}K"
        return f"${n:.0f}"

    tam_str = f"{_fmt_money(tam_low)} – {_fmt_money(tam_high)}" if tam_high else "unknown"

    attr  = scorecard.get("market_attractiveness")
    dem   = scorecard.get("demand_validation")
    comp  = scorecard.get("competitive_score")
    risk  = scorecard.get("risk_score")
    ue    = scorecard.get("unit_economics_score")
    sa    = scorecard.get("structural_attractiveness")
    ts    = scorecard.get("timing_score")
    icp   = scorecard.get("icp_clarity")

    def _bar(score, width: int = 20) -> str:
        if score is None:
            return " " * width
        filled = round((score / 100) * width)
        return "█" * filled + "░" * (width - filled)

    def _s(score) -> str:
        return f"{round(score):>3}/100" if score is not None else "    —  "

    sep = "─" * 58
    print()
    print(sep)
    print(f"  MARKET VALIDATION  ·  {market}  ·  {geography}")
    print(sep)
    print(f"  Archetype : {archetype_key}  ({scorecard.get('archetype_label', '')})")
    print(f"  TAM       : {tam_str}")
    competitors = competition.get("competitor_count") or competition.get("raw_candidate_count")
    if competitors:
        conc = competition.get("market_concentration", "")
        print(f"  Market    : {conc} · {competitors} competitors identified")
    print()
    print(f"  {'CORE SCORES':<28}  {'MODULE SCORES'}")
    print(f"  {'─'*26}  {'─'*26}")
    print(f"  Attractiveness  {_bar(attr,14)} {_s(attr)}  Unit Economics  {_s(ue)}")
    print(f"  Demand          {_bar(dem,14)} {_s(dem)}  Porter's SA     {_s(sa)}")
    print(f"  Competition     {_bar(100-(comp or 0),14)} {_s(100-(comp or 0))}  Timing          {_s(ts)}")
    print(f"  Risk (inv)      {_bar(100-(risk or 0),14)} {_s(100-(risk or 0))}  ICP Clarity     {_s(icp)}")
    print()
    print(f"  {'─'*54}")
    print(f"  OVERALL  {_bar(overall, 30)} {overall:.0f}/100")
    print(f"  VERDICT  {verdict_label}")
    print(f"  {'─'*54}")

    reasoning = scorecard.get("verdict_reasoning", "")
    if reasoning:
        words = reasoning.split()
        line, lines = [], []
        for w in words:
            if sum(len(x)+1 for x in line) + len(w) > 54:
                lines.append(" ".join(line))
                line = [w]
            else:
                line.append(w)
        if line:
            lines.append(" ".join(line))
        print()
        for ln in lines:
            print(f"  {ln}")

    next_steps = scorecard.get("next_steps") or []
    if next_steps:
        print()
        print("  NEXT STEPS")
        for i, step in enumerate(next_steps[:3], 1):
            words = step.split()
            line, lines = [], []
            for w in words:
                if sum(len(x)+1 for x in line) + len(w) > 50:
                    lines.append(" ".join(line))
                    line = [w]
                else:
                    line.append(w)
            if line:
                lines.append(" ".join(line))
            print(f"  {i}. {lines[0]}")
            for cont in lines[1:]:
                print(f"     {cont}")

    key_risks = scorecard.get("key_risks") or []
    if key_risks:
        print()
        print("  KEY RISKS")
        for risk_item in key_risks[:2]:
            words = risk_item.split()
            line, lines = [], []
            for w in words:
                if sum(len(x)+1 for x in line) + len(w) > 51:
                    lines.append(" ".join(line))
                    line = [w]
                else:
                    line.append(w)
            if line:
                lines.append(" ".join(line))
            print(f"  ▲ {lines[0]}")
            for cont in lines[1:]:
                print(f"    {cont}")

    print(sep)
    print()

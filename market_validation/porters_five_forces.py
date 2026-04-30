"""
Porter's Five Forces — analyzes structural market attractiveness for a new entrant.

Scores each of the five competitive forces (0-100, higher = more threatening):
- Supplier Power
- Buyer Power
- Substitute Threat
- Entry Barriers
- Rivalry Intensity

Derives a structural_attractiveness score: 100 minus weighted average of all forces.
Weights: supplier=0.20, buyer=0.20, substitutes=0.20, entry=0.25, rivalry=0.15.

AI synthesis (claude/opencode) is required for structured output.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from market_validation.log import get_logger

_log = get_logger("porters_five_forces")


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
    """Run ``_snippets`` for many queries concurrently, preserving order."""
    if not queries:
        return []
    out: list[str] = []
    with ThreadPoolExecutor(max_workers=min(8, len(queries))) as pool:
        for batch in pool.map(lambda q: _snippets(q, num_results), queries):
            out.extend(batch)
    return out


_DEFAULT_PORTER_WEIGHTS: dict[str, float] = {
    "supplier_power": 0.20,
    "buyer_power": 0.20,
    "substitute_threat": 0.20,
    "entry_barriers": 0.25,
    "rivalry_intensity": 0.15,
}


def _resolve_porter_weights(archetype: str | None) -> dict[str, float]:
    """Pull archetype-specific Porter weights, falling back to defaults.

    Each archetype config (market_archetype.ARCHETYPES) declares its own
    ``porter_weights``; missing archetypes or missing keys fall back to the
    classic 0.20/0.20/0.20/0.25/0.15 distribution.
    """
    if not archetype:
        return _DEFAULT_PORTER_WEIGHTS
    try:
        from market_validation.market_archetype import get_archetype_config
        config = get_archetype_config(archetype)
    except Exception:
        return _DEFAULT_PORTER_WEIGHTS
    weights = config.get("porter_weights")
    if not isinstance(weights, dict) or not weights:
        return _DEFAULT_PORTER_WEIGHTS
    out = dict(_DEFAULT_PORTER_WEIGHTS)
    for k, v in weights.items():
        if k in out and isinstance(v, (int, float)):
            out[k] = float(v)
    # Renormalize so weights sum to 1 (defensive — config errors shouldn't
    # quietly bias the score).
    total = sum(out.values())
    if total > 0 and abs(total - 1.0) > 0.001:
        out = {k: v / total for k, v in out.items()}
    return out


def analyze_porters_five_forces(
    market: str,
    geography: str,
    product: str | None = None,
    existing_competition: dict[str, Any] | None = None,
    run_ai: Callable[..., dict[str, Any]] | None = None,
    archetype: str | None = None,
) -> dict[str, Any]:
    """
    Analyze all five of Porter's competitive forces for a market.

    Args:
        market: Market description (e.g. "BBQ catering San Jose")
        geography: Target geography (e.g. "San Jose, CA")
        product: Specific product or service (optional)
        existing_competition: Output from analyze_competition() (optional).
            Used to seed rivalry data without redundant searches.
        run_ai: AI callable (Agent._run). Required for structured output.
        archetype: Market archetype key (e.g. "local-service"). When provided,
            the structural_attractiveness formula uses archetype-specific
            weights instead of the generic default.

    Returns:
        Dict with per-force scores and evidence, dominant_force,
        structural_attractiveness, and strategic_implication.
    """
    search_term = product or market
    weights = _resolve_porter_weights(archetype)

    supplier_queries = [
        f"{search_term} supplier concentration dominant market share",
        f"{search_term} commodity price volatile shortage",
        f"{search_term} supplier alternatives substitute input",
    ]
    buyer_queries = [
        f"{market} customer price sensitive switching easy",
        f"{market} buyer concentration large customer",
        f"{search_term} customers negotiate pricing discount",
    ]
    substitute_queries = [
        f"{search_term} alternatives substitute instead",
        f"instead of {search_term} use",
        f"alternative to {search_term}",
    ]
    entry_queries = [
        f"{market} barriers to entry capital requirements",
        f"{market} license permit certification required",
        f"{market} startup costs minimum viable",
    ]
    rivalry_query = f"{market} price war margin pressure competition"

    # All five forces are independent web searches — fan out across the
    # whole module instead of serializing 11 × time.sleep(1.2). Inside each
    # force the per-query searches also fan out via _parallel_snippets.
    with ThreadPoolExecutor(max_workers=5) as pool:
        f_sup = pool.submit(_parallel_snippets, supplier_queries)
        f_buy = pool.submit(_parallel_snippets, buyer_queries)
        f_sub = pool.submit(_parallel_snippets, substitute_queries)
        f_ent = pool.submit(_parallel_snippets, entry_queries)
        f_riv = pool.submit(_snippets, rivalry_query, 10)
        supplier_snippets = f_sup.result()[:5]
        buyer_snippets = f_buy.result()[:5]
        substitute_snippets = f_sub.result()[:5]
        entry_snippets = f_ent.result()[:5]
        rivalry_snippets = f_riv.result()[:5]

    # Seed rivalry from existing competition analysis if caller passed one.
    rivalry_context: dict[str, Any] = {}
    if existing_competition:
        rivalry_context = {
            "competitive_intensity": existing_competition.get("competitive_intensity"),
            "competitor_count": existing_competition.get("competitor_count"),
            "market_concentration": existing_competition.get("market_concentration"),
            "direct_competitors": existing_competition.get("direct_competitors", []),
            "dominant_players": existing_competition.get("dominant_players", []),
            "notes": existing_competition.get("notes", ""),
        }

    # ------------------------------------------------------------------
    # Raw return (no AI)
    # ------------------------------------------------------------------
    raw_snippets = {
        "supplier": supplier_snippets,
        "buyer": buyer_snippets,
        "substitutes": substitute_snippets,
        "entry": entry_snippets,
        "rivalry": rivalry_snippets,
    }

    if not run_ai:
        return {"raw_snippets": raw_snippets}

    # ------------------------------------------------------------------
    # Build AI prompt
    # ------------------------------------------------------------------
    def _fmt(label: str, items: list[str]) -> str:
        if not items:
            return f"\n{label}:\n  (no data gathered)\n"
        lines = "\n".join(f"  - {s[:160]}" for s in items)
        return f"\n{label}:\n{lines}\n"

    snippets_text = (
        _fmt("SUPPLIER POWER evidence", supplier_snippets)
        + _fmt("BUYER POWER evidence", buyer_snippets)
        + _fmt("SUBSTITUTE THREAT evidence", substitute_snippets)
        + _fmt("ENTRY BARRIERS evidence", entry_snippets)
        + _fmt("RIVALRY evidence", rivalry_snippets)
    )

    rivalry_context_text = ""
    if rivalry_context:
        rivalry_context_text = f"""
Existing competition data:
- Competitive intensity (0-100): {rivalry_context.get('competitive_intensity', 'unknown')}
- Competitor count: {rivalry_context.get('competitor_count', 'unknown')}
- Market concentration: {rivalry_context.get('market_concentration', 'unknown')}
- Direct competitors: {', '.join(rivalry_context.get('direct_competitors', [])) or 'none identified'}
- Dominant players: {', '.join(rivalry_context.get('dominant_players', [])) or 'none identified'}
- Notes: {rivalry_context.get('notes', '')}
"""

    weight_formula = (
        f"100 - ({weights['supplier_power']:.2f} * supplier_power + "
        f"{weights['buyer_power']:.2f} * buyer_power + "
        f"{weights['substitute_threat']:.2f} * substitute_threat + "
        f"{weights['entry_barriers']:.2f} * entry_barriers + "
        f"{weights['rivalry_intensity']:.2f} * rivalry_intensity)"
    )
    archetype_hint = (
        f"\nArchetype: {archetype} — weights below are tuned for this business model.\n"
        if archetype else ""
    )

    prompt = f"""You are a strategic analyst applying Porter's Five Forces framework. Assess the structural attractiveness for a new entrant in:

Market: {market}
Geography: {geography}
Product/Service: {product or 'general'}
{archetype_hint}{rivalry_context_text}
Evidence gathered from web searches (organized by force):
{snippets_text}

Score each force 0-100 where HIGHER = more threatening / stronger force (worse for a new entrant).

Compute structural_attractiveness using this exact formula:
  structural_attractiveness = {weight_formula}

Return ONLY this JSON (no markdown fences):
{{
    "supplier_power": <0-100>,
    "supplier_power_evidence": {{"text": "1 sentence citing specific evidence", "source_url": "..."}},
    "buyer_power": <0-100>,
    "buyer_power_evidence": {{"text": "...", "source_url": "..."}},
    "substitute_threat": <0-100>,
    "substitute_threat_evidence": {{"text": "...", "source_url": "..."}},
    "entry_barriers": <0-100>,
    "entry_barriers_evidence": {{"text": "...", "source_url": "..."}},
    "rivalry_intensity": <0-100>,
    "rivalry_intensity_evidence": {{"text": "...", "source_url": "..."}},
    "structural_attractiveness": <0-100, computed via formula above>,
    "dominant_force": "<supplier_power|buyer_power|substitute_threat|entry_barriers|rivalry_intensity>",
    "strategic_implication": "<1-2 sentences on what this means for a new entrant>"
}}

Citation rules:
- Each *_evidence object should reference a specific snippet from the
  evidence block above. If no evidence is available for a force, set the
  source_url to "" and the text to "no evidence gathered" and apply a
  conservative midpoint score (45-55).
- Do NOT invent funding rounds, customer counts, or barrier types you
  can't cite from the search results above.

Scoring reference:
- supplier_power: 80+ = few suppliers with pricing power; <40 = commodity inputs with many substitutes
- buyer_power: 80+ = few large buyers who can dictate terms; <40 = fragmented buyers with low switching
- substitute_threat: 80+ = many good alternatives exist; <40 = hard to substitute
- entry_barriers: 80+ = high capital/regulatory/brand barriers; <40 = easy to enter
- rivalry_intensity: 80+ = price wars, crowded market; <40 = low competition, high margins
- If evidence is thin, apply conservative midpoint scores and note the uncertainty in strategic_implication"""

    ai_result = run_ai(prompt)

    result: dict[str, Any] = {"raw_snippets": raw_snippets}
    if rivalry_context:
        result["rivalry_context_used"] = rivalry_context

    parsed: dict[str, Any] = {}
    if isinstance(ai_result, dict):
        if "supplier_power" in ai_result:
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

    # ------------------------------------------------------------------
    # Local fallback: compute structural_attractiveness if AI omitted it
    # ------------------------------------------------------------------
    force_keys = (
        "supplier_power",
        "buyer_power",
        "substitute_threat",
        "entry_barriers",
        "rivalry_intensity",
    )
    if "structural_attractiveness" not in result and all(k in result for k in force_keys):
        supplier = float(result["supplier_power"])
        buyer = float(result["buyer_power"])
        sub = float(result["substitute_threat"])
        entry = float(result["entry_barriers"])
        rivalry = float(result["rivalry_intensity"])
        result["structural_attractiveness"] = round(
            100 - (
                weights["supplier_power"] * supplier
                + weights["buyer_power"] * buyer
                + weights["substitute_threat"] * sub
                + weights["entry_barriers"] * entry
                + weights["rivalry_intensity"] * rivalry
            ),
            1,
        )

    result["porter_weights_used"] = weights
    if archetype:
        result["porter_archetype"] = archetype

    return result

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
import time
from collections.abc import Callable
from typing import Any


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


def analyze_porters_five_forces(
    market: str,
    geography: str,
    product: str | None = None,
    existing_competition: dict[str, Any] | None = None,
    run_ai: Callable[..., dict[str, Any]] | None = None,
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

    Returns:
        Dict with per-force scores and evidence, dominant_force,
        structural_attractiveness, and strategic_implication.
    """
    search_term = product or market

    # ------------------------------------------------------------------
    # Force 1 — Supplier Power
    # ------------------------------------------------------------------
    supplier_snippets: list[str] = []
    for query in [
        f"{search_term} supplier concentration dominant market share",
        f"{search_term} commodity price volatile shortage",
        f"{search_term} supplier alternatives substitute input",
    ]:
        supplier_snippets.extend(_snippets(query, 10))
        time.sleep(1.2)
    supplier_snippets = supplier_snippets[:5]

    # ------------------------------------------------------------------
    # Force 2 — Buyer Power
    # ------------------------------------------------------------------
    buyer_snippets: list[str] = []
    for query in [
        f"{market} customer price sensitive switching easy",
        f"{market} buyer concentration large customer",
        f"{search_term} customers negotiate pricing discount",
    ]:
        buyer_snippets.extend(_snippets(query, 10))
        time.sleep(1.2)
    buyer_snippets = buyer_snippets[:5]

    # ------------------------------------------------------------------
    # Force 3 — Substitute Threat
    # ------------------------------------------------------------------
    substitute_snippets: list[str] = []
    for query in [
        f"{search_term} alternatives substitute instead",
        f"instead of {search_term} use",
        f"alternative to {search_term}",
    ]:
        substitute_snippets.extend(_snippets(query, 10))
        time.sleep(1.2)
    substitute_snippets = substitute_snippets[:5]

    # ------------------------------------------------------------------
    # Force 4 — New Entry Barriers
    # ------------------------------------------------------------------
    entry_snippets: list[str] = []
    for query in [
        f"{market} barriers to entry capital requirements",
        f"{market} license permit certification required",
        f"{market} startup costs minimum viable",
    ]:
        entry_snippets.extend(_snippets(query, 10))
        time.sleep(1.2)
    entry_snippets = entry_snippets[:5]

    # ------------------------------------------------------------------
    # Force 5 — Rivalry
    # ------------------------------------------------------------------
    rivalry_snippets: list[str] = []

    # Seed from existing competition analysis if available
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

    rivalry_search_results = _snippets(f"{market} price war margin pressure competition", 10)
    rivalry_snippets.extend(rivalry_search_results)
    time.sleep(1.2)
    rivalry_snippets = rivalry_snippets[:5]

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

    prompt = f"""You are a strategic analyst applying Porter's Five Forces framework. Assess the structural attractiveness for a new entrant in:

Market: {market}
Geography: {geography}
Product/Service: {product or 'general'}
{rivalry_context_text}
Evidence gathered from web searches (organized by force):
{snippets_text}

Score each force 0-100 where HIGHER = more threatening / stronger force (worse for a new entrant).

Compute structural_attractiveness using this exact formula:
  structural_attractiveness = 100 - (0.20 * supplier_power + 0.20 * buyer_power + 0.20 * substitute_threat + 0.25 * entry_barriers + 0.15 * rivalry_intensity)

Return ONLY this JSON (no markdown fences):
{{
    "supplier_power": <0-100>,
    "supplier_power_evidence": "<1 sentence citing specific evidence>",
    "buyer_power": <0-100>,
    "buyer_power_evidence": "<1 sentence citing specific evidence>",
    "substitute_threat": <0-100>,
    "substitute_threat_evidence": "<1 sentence citing specific evidence>",
    "entry_barriers": <0-100>,
    "entry_barriers_evidence": "<1 sentence citing specific evidence>",
    "rivalry_intensity": <0-100>,
    "rivalry_intensity_evidence": "<1 sentence citing specific evidence>",
    "structural_attractiveness": <0-100, computed via formula above>,
    "dominant_force": "<supplier_power|buyer_power|substitute_threat|entry_barriers|rivalry_intensity>",
    "strategic_implication": "<1-2 sentences on what this means for a new entrant>"
}}

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
            100 - (0.20 * supplier + 0.20 * buyer + 0.20 * sub + 0.25 * entry + 0.15 * rivalry),
            1,
        )

    return result

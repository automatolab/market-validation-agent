"""Source-authority tiers + completeness scoring for AI validation outputs.

The validation modules (market_sizing, demand_analysis, competitive_landscape,
unit_economics, customer_segments, market_signals) ask the AI to return claims
backed by ``source_url`` and ``source_authority`` fields. This module:

- Defines a tier ranking so a confidence score can be calibrated against the
  evidence quality (BLS / SEC > industry reports > Wikipedia > web > AI guess).
- Provides ``score_source_authority(url)`` which infers the tier from a URL.
- Provides ``completeness_score(stages)`` which the scorecard uses to flag
  verdicts based on thin evidence.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

# Tier 1 — government / regulatory primary sources. Any claim sourced here
# should anchor confidence > 80.
_TIER_1_DOMAINS: frozenset[str] = frozenset({
    "bls.gov", "sec.gov", "edgar.sec.gov", "census.gov", "bea.gov",
    "irs.gov", "commerce.gov", "usda.gov", "fda.gov", "cdc.gov",
    "uspto.gov", "nih.gov", "hhs.gov", "doe.gov", "fcc.gov", "fec.gov",
    "ec.europa.eu", "ons.gov.uk", "gov.uk", "ec.gc.ca", "stats.govt.nz",
    "abs.gov.au", "destatis.de", "insee.fr", "istat.it",
    "data.gov", "data.gov.uk",
    "wto.org", "worldbank.org", "imf.org", "oecd.org", "un.org",
})

# Tier 2 — paid market research that we can cite (not fully scrape) plus
# primary academic sources.
_TIER_2_HOSTS: tuple[str, ...] = (
    "statista.com", "ibisworld.com", "grandviewresearch.com",
    "mordorintelligence.com", "marketsandmarkets.com",
    "fitchsolutions.com", "euromonitor.com", "gartner.com",
    "forrester.com", "mckinsey.com", "bcg.com",
    "openalex.org", "ncbi.nlm.nih.gov", "pubmed.ncbi.nlm.nih.gov",
    "scholar.google.com", "doi.org",
)

# Tier 3 — reputable trade press / business news. Quality varies by article.
_TIER_3_HOSTS: tuple[str, ...] = (
    "wsj.com", "ft.com", "bloomberg.com", "reuters.com", "economist.com",
    "techcrunch.com", "theinformation.com", "axios.com",
    "businessinsider.com", "fortune.com", "forbes.com", "inc.com",
    "businesswire.com", "prnewswire.com",
    "crunchbase.com", "pitchbook.com",
    "hbr.org", "sloanreview.mit.edu",
    "restaurantbusinessonline.com", "modernrestaurantmanagement.com",
    "fooddive.com", "supplychainbrain.com",
)

# Tier 4 — encyclopedic + community sources. Useful for context, weak as
# primary evidence.
_TIER_4_HOSTS: tuple[str, ...] = (
    "wikipedia.org", "wikimedia.org",
    "reddit.com", "quora.com", "news.ycombinator.com",
    "github.com", "stackoverflow.com",
)

TIER_LABELS: dict[int, str] = {
    1: "primary_government",
    2: "paid_research_or_academic",
    3: "trade_press_or_business_news",
    4: "encyclopedic_or_community",
    5: "general_web",
    6: "ai_inference_no_source",
}

# What confidence (0-100) is *justified* by a source at each tier when no
# further corroboration exists. Caller can corroborate to push higher.
TIER_BASELINE_CONFIDENCE: dict[int, int] = {
    1: 80,
    2: 70,
    3: 55,
    4: 40,
    5: 30,
    6: 20,
}


def _host(url: str | None) -> str:
    if not url:
        return ""
    try:
        return (urlparse(url).netloc or "").lower().removeprefix("www.")
    except Exception:
        return ""


def score_source_authority(url: str | None) -> int:
    """Return the tier (1=highest, 6=lowest) for a given source URL.

    Unknown / empty URL → tier 6 (AI inference). Domain-suffix matching is
    used so subdomains (data.bls.gov, www.census.gov) tier correctly.
    """
    host = _host(url)
    if not host:
        return 6
    for d in _TIER_1_DOMAINS:
        if host == d or host.endswith("." + d):
            return 1
    for d in _TIER_2_HOSTS:
        if host == d or host.endswith("." + d):
            return 2
    for d in _TIER_3_HOSTS:
        if host == d or host.endswith("." + d):
            return 3
    for d in _TIER_4_HOSTS:
        if host == d or host.endswith("." + d):
            return 4
    return 5  # general web


def calibrate_confidence(stated: int, source_url: str | None) -> int:
    """Cap a stated confidence by what the source tier justifies.

    Used to reign in AI claims like "confidence 90 — found on a SEO blog".
    Returns the lower of the stated value and the tier baseline. Stated
    values below the tier baseline pass through unchanged.
    """
    try:
        s = int(stated)
    except (ValueError, TypeError):
        return TIER_BASELINE_CONFIDENCE[6]
    if s < 0:
        s = 0
    if s > 100:
        s = 100
    tier = score_source_authority(source_url)
    cap = TIER_BASELINE_CONFIDENCE.get(tier, 30)
    return min(s, cap + 10)  # +10 slack for corroborating evidence


def completeness_score(stages: dict[str, dict[str, Any]]) -> int:
    """Compute a 0-100 completeness score across validation stages.

    A stage counts as "populated" when it has at least one non-empty value
    excluding ``raw_snippets`` / ``sources_used`` / ``method`` / ``snippet_count``.
    Used by the scorecard to flag verdicts based on thin evidence.
    """
    if not stages:
        return 0
    score = 0
    expected = max(len(stages), 1)
    skip_keys = {"raw_snippets", "sources_used", "method", "snippet_count",
                 "ai_raw", "available", "reason"}
    for stage, payload in stages.items():
        if not isinstance(payload, dict):
            continue
        useful_keys = [
            k for k, v in payload.items()
            if k not in skip_keys and v not in (None, "", [], {}, "unknown")
        ]
        if useful_keys:
            score += 1
    return int(round(100 * score / expected))


# Standardized JSON-grounding instruction text that prompts inject so AI is
# explicit about source authority and citations. Cheap import for prompts.
CITATION_PROMPT_INSTRUCTIONS = """
Each numeric estimate or claim must be grounded:
- Cite a `source_url` for every claim where you found evidence.
- Set `source_authority` to one of:
  primary_government / paid_research_or_academic / trade_press_or_business_news /
  encyclopedic_or_community / general_web / ai_inference_no_source
- Confidence rules:
  - >=80 only when a Tier-1 (government/SEC) source corroborates.
  - 60-80 when 2+ Tier-2/3 sources agree.
  - 40-60 for single Tier-3/4 source or strong AI inference with reasoning.
  - <40 when extrapolating without a citable source.
- Never inflate confidence to mask thin evidence. If unsure, say so.
"""


# ── Post-AI enforcement ─────────────────────────────────────────────────────
# These functions run AFTER the AI returns a parsed JSON payload and BEFORE
# the caller writes it to the DB. They mutate the payload to:
#   1. Drop source entries that have no usable URL.
#   2. Cap stated confidence by what the strongest cited source actually
#      justifies (so a Tier-5 blog can't anchor "85% confidence").
#   3. Append a `_citation_warnings` list so the operator/dashboard can see
#      which fields had thin or missing evidence.
#
# Without this layer, the citation prompts are advisory only — a model that
# ignores them silently gets full trust. With it, the system is contract-bound.

from dataclasses import dataclass


@dataclass(frozen=True)
class CitationRule:
    """Describes how to enforce citations on one claim.

    Attributes:
        claim_field: name of the field holding the claim value (e.g. "tam_low").
            Only used for warning messages; the rule fires when sources_field
            is present, regardless of whether claim_field is.
        sources_field: name of the field that holds a list of source dicts
            (e.g. "tam_sources"). Each entry SHOULD have a "source_url".
        confidence_field: optional — name of the confidence value to cap.
            When set, confidence is capped to the tier baseline of the
            strongest cited source + 10pt corroboration slack.
        min_sources: minimum number of cited sources required. When the
            actual count is below this, a warning is appended AND
            confidence is capped to ai_inference_no_source baseline (20).
    """
    claim_field: str
    sources_field: str
    confidence_field: str | None = None
    min_sources: int = 1


def _extract_url(entry: Any) -> str | None:
    """Pull a source_url from a citation entry that may be a dict or a string.

    Returns None when the entry has no usable URL — both for plain strings
    that aren't URLs and for dicts where ``source_url`` is empty.
    """
    if isinstance(entry, dict):
        url = entry.get("source_url") or entry.get("url") or ""
        return str(url).strip() or None
    if isinstance(entry, str):
        s = entry.strip()
        # If a string starts with http(s), treat as URL; otherwise it's just
        # a citation note (e.g. "BLS 2024 release") — no URL to validate.
        return s if s.startswith(("http://", "https://")) else None
    return None


def enforce_citations(
    payload: dict[str, Any],
    rules: list[CitationRule],
) -> dict[str, Any]:
    """Apply citation rules to a parsed AI payload, in place.

    Behavior:
      - Source entries missing a URL are removed from the sources list.
      - If too few cited sources remain (< rule.min_sources), a warning is
        emitted and confidence (if applicable) is capped at 20 (the
        ai_inference_no_source baseline).
      - If sources are present, confidence is capped at the tier baseline of
        the BEST cited source + 10pt corroboration slack.
      - Warnings are appended to payload["_citation_warnings"] so callers
        can surface or log them.

    Returns the same dict (mutated). Never raises — invalid input shapes are
    skipped with a warning rather than aborting the validation run.
    """
    if not isinstance(payload, dict):
        return payload  # nothing to do
    warnings: list[str] = list(payload.get("_citation_warnings") or [])

    for rule in rules:
        sources = payload.get(rule.sources_field)
        cited_urls: list[str] = []

        if isinstance(sources, list):
            kept: list[Any] = []
            for entry in sources:
                url = _extract_url(entry)
                if url:
                    cited_urls.append(url)
                    kept.append(entry)
                # Entries without URLs are silently dropped — they were
                # advisory-only ("BLS 2024") and we can't tier them.
            payload[rule.sources_field] = kept

        # Compute the best (lowest-numbered) tier across all cited URLs.
        best_tier = min(
            (score_source_authority(u) for u in cited_urls),
            default=6,
        )
        cap = TIER_BASELINE_CONFIDENCE.get(best_tier, 30) + 10

        # Below the source floor → cap hard at Tier-6 baseline.
        if len(cited_urls) < rule.min_sources:
            cap = TIER_BASELINE_CONFIDENCE[6]
            warnings.append(
                f"{rule.claim_field}: {len(cited_urls)} cited source(s), "
                f"need {rule.min_sources} — capping confidence at {cap}."
            )

        # Apply cap if confidence is tracked for this rule.
        if rule.confidence_field and rule.confidence_field in payload:
            try:
                stated = int(float(payload[rule.confidence_field]))
            except (TypeError, ValueError):
                stated = 0
            if stated > cap:
                warnings.append(
                    f"{rule.confidence_field}: stated {stated} capped to {cap} "
                    f"(best source tier: {TIER_LABELS.get(best_tier, '?')})"
                )
                payload[rule.confidence_field] = cap

    if warnings:
        payload["_citation_warnings"] = warnings
    return payload


# Per-module rule sets, kept close to the helper so each callsite is a
# one-liner: ``enforce_citations(parsed, RULES_FOR_SIZING)``.

RULES_FOR_SIZING = [
    CitationRule("tam_low", "tam_sources", confidence_field="tam_confidence"),
    CitationRule("sam_low", "sam_sources", confidence_field="sam_confidence"),
    CitationRule("som_low", "som_sources", confidence_field="som_confidence"),
]

RULES_FOR_COMPETITION = [
    CitationRule("direct_competitors", "direct_competitors", min_sources=1),
    CitationRule("indirect_competitors", "indirect_competitors", min_sources=0),
    CitationRule("funding_signals", "funding_signals", min_sources=0),
    CitationRule("dominant_players", "dominant_players", min_sources=0),
]

RULES_FOR_UNIT_ECONOMICS = [
    CitationRule("gross_margin_low", "gross_margin_source",
                 confidence_field="gross_margin_confidence"),
    CitationRule("cac_estimate_low", "cac_source"),
    CitationRule("pricing_signals", "pricing_signals", min_sources=0),
]

RULES_FOR_DEMAND = [
    CitationRule("demand_pain_points", "demand_pain_points", min_sources=0),
    CitationRule("demand_trend", "demand_trend_sources", min_sources=0),
]

RULES_FOR_SIGNALS = [
    CitationRule("regulatory_risks", "regulatory_risks", min_sources=0),
    CitationRule("key_trends", "key_trends", min_sources=0),
    CitationRule("technology_maturity", "technology_maturity_sources", min_sources=0),
]

RULES_FOR_TIMING = [
    CitationRule("enablers", "enablers", min_sources=0),
    CitationRule("headwinds", "headwinds", min_sources=0),
]

RULES_FOR_CUSTOMER_SEGMENTS = [
    # Both segment dicts may carry their own _evidence fields; keep flat.
]

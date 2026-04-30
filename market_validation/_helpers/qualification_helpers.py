"""Helpers for the qualification step: heuristic fallback scoring and
status/priority normalizers used when the AI response is missing fields."""

from __future__ import annotations

from typing import Any

from market_validation._helpers.common import infer_market_profile, to_float


def heuristic_qualification(
    companies: list[tuple[Any, Any, Any, Any, Any, Any]],
    market: str,
    product: str | None,
) -> list[dict[str, Any]]:
    profile = infer_market_profile(market, product)
    positive_tokens = set(profile.get("positive_tokens") or set()) | set(profile.get("tokens") or set())
    positive_tokens = {t for t in positive_tokens if len(t) >= 3}
    if not positive_tokens:
        positive_tokens = {"company", "business", "service", "provider"}

    results: list[dict[str, Any]] = []
    for c in companies:
        company_id, company_name, notes, _phone, website, location = c
        text = " ".join(
            part.lower() for part in [str(company_name or ""), str(notes or ""), str(website or ""), str(location or "")]
        )

        hits = sum(1 for token in positive_tokens if token in text)
        score = min(95, 35 + hits * 18)

        if hits >= 2:
            status = "qualified"
            priority = "high" if hits >= 4 else "medium"
        elif hits == 1:
            status = "qualified"
            priority = "medium"
        else:
            status = "new"
            priority = "low"

        volume_estimate = None
        volume_unit = None
        if status == "qualified":
            volume_estimate = 900 if priority == "high" else 450
            volume_unit = "weekly deliveries"

        results.append(
            {
                "company_id": str(company_id),
                "status": status,
                "score": score,
                "priority": priority,
                "volume_estimate": volume_estimate,
                "volume_unit": volume_unit,
                "notes": f"Heuristic qualification (keyword matches={hits})",
            }
        )
    return results


def normalize_qualification_status(status: Any) -> str:
    """Map AI/qualifier output to a canonical CompanyStatus value.

    "uncertain" maps to ``new`` (still needs work). "not_relevant" stays as
    ``not_relevant`` (rejected by qualifier — distinct from a recipient who
    said "not_interested" after we contacted them).
    """
    raw = str(status or "").strip().lower().replace("-", "_").replace(" ", "_")
    canonical = {
        "new", "qualified", "not_relevant", "contacted",
        "replied", "interested", "not_interested", "skipped",
    }
    if raw in canonical:
        return raw
    if raw in {"irrelevant", "disqualified", "reject", "rejected"}:
        return "not_relevant"
    if raw in {"uncertain", "unknown", "maybe", "review", "needs_review"}:
        return "new"
    return "new"


def normalize_priority(priority: Any, score: int) -> str:
    raw = str(priority or "").strip().lower()
    if raw in {"high", "medium", "low"}:
        return raw
    if score >= 80:
        return "high"
    if score >= 55:
        return "medium"
    return "low"


def clamp_score(value: Any) -> int:
    parsed = int(to_float(value) or 0)
    return max(0, min(100, parsed))

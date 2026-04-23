"""Small, widely-reused utilities: timestamps, number parsing, tokenization,
market-category inference, and ordered-unique helpers."""

from __future__ import annotations

import re
from datetime import UTC
from typing import Any


def iso_now() -> str:
    from datetime import datetime
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def unique_in_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def summarize_backends(rows: list[dict[str, str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        backend = str(row.get("source", "unknown"))
        counts[backend] = counts.get(backend, 0) + 1
    return counts


def tokenize_text(text: str) -> list[str]:
    return [t for t in re.split(r"\W+", text.lower()) if len(t) >= 3]


def infer_market_profile(market: str, product: str | None) -> dict[str, Any]:
    text = f"{market} {product or ''}".lower()
    tokens = set(tokenize_text(text))

    _category_kw_map: dict[str, tuple[str, ...]] = {
        "saas": ("saas", "software", "api", "platform", "cloud", "automation"),
        "food": ("restaurant", "food", "bbq", "barbecue", "catering", "cafe", "coffee", "dining"),
        "healthcare": ("clinic", "medical", "health", "dental", "hospital", "pharma"),
        "industrial": ("manufacturer", "manufacturing", "industrial", "factory", "supplier", "wholesale", "robot", "robotics", "drone", "automation", "aerospace", "defense", "semiconductor", "hardware"),
        "services": ("agency", "consulting", "consultant", "legal", "accounting", "services"),
    }

    if any(t in text for t in _category_kw_map["saas"]):
        category = "saas"
    elif any(t in text for t in _category_kw_map["food"]):
        category = "food"
    elif any(t in text for t in _category_kw_map["healthcare"]):
        category = "healthcare"
    elif any(t in text for t in _category_kw_map["industrial"]):
        category = "industrial"
    elif any(t in text for t in _category_kw_map["services"]):
        category = "services"
    else:
        category = "general"

    if category == "general":
        confidence = 30
    else:
        match_count = sum(1 for kw in _category_kw_map[category] if kw in text)
        confidence = min(100, 40 + match_count * 25)

    positive_by_category: dict[str, set[str]] = {
        "food": {"restaurant", "dining", "catering", "grill", "kitchen", "eatery", "bbq", "barbecue", "smokehouse"},
        "saas": {"saas", "software", "platform", "api", "cloud", "automation", "tool", "solution", "app"},
        "healthcare": {"clinic", "medical", "health", "hospital", "dental", "care", "provider"},
        "industrial": {"manufacturer", "manufacturing", "industrial", "supplier", "factory", "distributor", "robot", "robotics", "drone", "automation", "aerospace", "semiconductor", "hardware", "systems"},
        "services": {"services", "agency", "consulting", "consultant", "firm", "provider"},
        "general": {"company", "business", "provider", "services"},
    }

    blocked_tokens = {"list of", "wikipedia"}
    if category == "food":
        blocked_tokens.update({"season", "episode", "joey chestnut", "chopped", "man v. food"})

    banned_name_tokens: set[str] = set()

    return {
        "category": category,
        "confidence": confidence,
        "tokens": tokens,
        "positive_tokens": positive_by_category.get(category, set()),
        "blocked_tokens": blocked_tokens,
        "banned_name_tokens": banned_name_tokens,
    }

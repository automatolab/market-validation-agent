from __future__ import annotations

from dataclasses import dataclass

DIMENSIONS: tuple[str, ...] = (
    "pain_intensity",
    "customer_clarity",
    "willingness_to_pay",
    "competition_intensity",
    "differentiation_potential",
    "distribution_ease",
    "retention_repeatability",
    "operational_complexity",
    "regulatory_friction",
    "speed_to_first_revenue",
    "brand_trust",
    "team_capability",
    "tech_feasibility",
)

RESTAURANT_DIMENSIONS: tuple[str, ...] = (
    "local_demand_density",
    "catering_event_frequency",
    "price_per_head_viability",
    "competitor_saturation",
    "operational_complexity",
    "repeat_event_potential",
)

EXTENDED_DIMENSIONS: tuple[str, ...] = DIMENSIONS + (
    "local_demand_density",
    "catering_event_frequency",
    "price_per_head_viability",
    "competitor_saturation",
    "repeat_event_potential",
)

STANDARD_SOURCE_TYPES: tuple[str, ...] = (
    "company_website",
    "pricing_page",
    "review_site",
    "forum_social",
    "directory_listing",
    "market_report",
    "job_post",
    "trend_signal",
    "customer_complaint",
)


@dataclass(frozen=True)
class ProfileConfig:
    name: str
    weights: dict[str, float]
    source_priorities: tuple[str, ...]


def _normalize(weights: dict[str, float]) -> dict[str, float]:
    total = sum(weights.values())
    if total <= 0:
        return {dimension: 1.0 / len(DIMENSIONS) for dimension in DIMENSIONS}
    return {dimension: value / total for dimension, value in weights.items()}


def _build_weights(adjustments: dict[str, float] | None = None) -> dict[str, float]:
    base = {dimension: 1.0 for dimension in DIMENSIONS}
    if adjustments:
        for dimension, value in adjustments.items():
            if dimension in base:
                base[dimension] = max(0.1, value)
    return _normalize(base)


PROFILES: dict[str, ProfileConfig] = {
    "general": ProfileConfig(
        name="general",
        weights=_build_weights(),
        source_priorities=STANDARD_SOURCE_TYPES,
    ),
    "saas": ProfileConfig(
        name="saas",
        weights=_build_weights(
            {
                "willingness_to_pay": 1.4,
                "retention_repeatability": 1.5,
                "distribution_ease": 1.2,
                "speed_to_first_revenue": 1.2,
            }
        ),
        source_priorities=(
            "pricing_page",
            "review_site",
            "forum_social",
            "company_website",
            "job_post",
            "trend_signal",
            "market_report",
            "directory_listing",
            "customer_complaint",
        ),
    ),
    "local_business": ProfileConfig(
        name="local_business",
        weights=_build_weights(
            {
                "distribution_ease": 1.4,
                "speed_to_first_revenue": 1.5,
                "operational_complexity": 1.3,
                "retention_repeatability": 1.2,
            }
        ),
        source_priorities=(
            "directory_listing",
            "review_site",
            "customer_complaint",
            "company_website",
            "pricing_page",
            "forum_social",
            "trend_signal",
            "market_report",
            "job_post",
        ),
    ),
    "ecommerce": ProfileConfig(
        name="ecommerce",
        weights=_build_weights(
            {
                "distribution_ease": 1.5,
                "retention_repeatability": 1.4,
                "competition_intensity": 1.3,
                "willingness_to_pay": 1.2,
            }
        ),
        source_priorities=(
            "pricing_page",
            "review_site",
            "directory_listing",
            "trend_signal",
            "customer_complaint",
            "company_website",
            "forum_social",
            "market_report",
            "job_post",
        ),
    ),
    "service_business": ProfileConfig(
        name="service_business",
        weights=_build_weights(
            {
                "customer_clarity": 1.3,
                "differentiation_potential": 1.2,
                "speed_to_first_revenue": 1.5,
                "distribution_ease": 1.3,
            }
        ),
        source_priorities=(
            "directory_listing",
            "review_site",
            "company_website",
            "pricing_page",
            "forum_social",
            "customer_complaint",
            "market_report",
            "trend_signal",
            "job_post",
        ),
    ),
}


def get_profile_config(name: str) -> ProfileConfig:
    return PROFILES.get(name, PROFILES["general"])

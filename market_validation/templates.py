from __future__ import annotations

from dataclasses import dataclass

from .profiles import ProfileConfig


@dataclass(frozen=True)
class TemplateConfig:
    name: str
    profile_override: str | None
    weight_adjustments: dict[str, float]
    source_boost: tuple[str, ...]


TEMPLATES: dict[str, TemplateConfig] = {
    "restaurant": TemplateConfig(
        name="restaurant",
        profile_override="local_business",
        weight_adjustments={
            "distribution_ease": 1.7,
            "operational_complexity": 1.5,
            "retention_repeatability": 1.3,
        },
        source_boost=("directory_listing", "review_site", "customer_complaint"),
    ),
    "ai_saas": TemplateConfig(
        name="ai_saas",
        profile_override="saas",
        weight_adjustments={
            "willingness_to_pay": 1.6,
            "differentiation_potential": 1.4,
            "operational_complexity": 1.3,
        },
        source_boost=("pricing_page", "review_site", "job_post", "trend_signal"),
    ),
    "recruiting_agency": TemplateConfig(
        name="recruiting_agency",
        profile_override="service_business",
        weight_adjustments={
            "customer_clarity": 1.5,
            "speed_to_first_revenue": 1.4,
            "retention_repeatability": 1.3,
        },
        source_boost=("job_post", "directory_listing", "review_site"),
    ),
    "niche_shopify_store": TemplateConfig(
        name="niche_shopify_store",
        profile_override="ecommerce",
        weight_adjustments={
            "distribution_ease": 1.7,
            "competition_intensity": 1.4,
            "willingness_to_pay": 1.3,
        },
        source_boost=("pricing_page", "review_site", "trend_signal"),
    ),
    "local_service_business": TemplateConfig(
        name="local_service_business",
        profile_override="local_business",
        weight_adjustments={
            "speed_to_first_revenue": 1.7,
            "distribution_ease": 1.5,
            "customer_clarity": 1.3,
        },
        source_boost=("directory_listing", "review_site", "customer_complaint"),
    ),
    "fintech": TemplateConfig(
        name="fintech",
        profile_override="saas",
        weight_adjustments={
            "regulatory_friction": 0.6,
            "willingness_to_pay": 1.5,
            "distribution_ease": 1.3,
            "operational_complexity": 1.2,
        },
        source_boost=("pricing_page", "market_report", "job_post", "trend_signal"),
    ),
    "healthcare": TemplateConfig(
        name="healthcare",
        profile_override="saas",
        weight_adjustments={
            "regulatory_friction": 0.5,
            "differentiation_potential": 1.4,
            "customer_clarity": 1.3,
            "operational_complexity": 1.2,
        },
        source_boost=("pricing_page", "review_site", "market_report", "job_post"),
    ),
    "marketplace": TemplateConfig(
        name="marketplace",
        profile_override="saas",
        weight_adjustments={
            "distribution_ease": 0.7,
            "retention_repeatability": 1.5,
            "competition_intensity": 1.3,
            "speed_to_first_revenue": 1.2,
        },
        source_boost=("pricing_page", "review_site", "market_report", "forum_social"),
    ),
    "b2b_service": TemplateConfig(
        name="b2b_service",
        profile_override="service_business",
        weight_adjustments={
            "customer_clarity": 1.6,
            "willingness_to_pay": 1.5,
            "distribution_ease": 1.3,
            "speed_to_first_revenue": 1.2,
        },
        source_boost=("pricing_page", "directory_listing", "review_site", "job_post"),
    ),
    "mobile_app": TemplateConfig(
        name="mobile_app",
        profile_override="saas",
        weight_adjustments={
            "distribution_ease": 1.6,
            "retention_repeatability": 1.5,
            "operational_complexity": 1.2,
            "competition_intensity": 1.3,
        },
        source_boost=("review_site", "pricing_page", "forum_social", "trend_signal"),
    ),
}


def get_template(name: str | None) -> TemplateConfig | None:
    if not name:
        return None
    return TEMPLATES.get(name)


def apply_template_to_profile(profile: ProfileConfig, template: TemplateConfig | None) -> ProfileConfig:
    if template is None:
        return profile

    boosted = list(profile.source_priorities)
    for source_type in reversed(template.source_boost):
        if source_type in boosted:
            boosted.remove(source_type)
        boosted.insert(0, source_type)

    raw_weights = {dimension: value for dimension, value in profile.weights.items()}
    for dimension, multiplier in template.weight_adjustments.items():
        if dimension in raw_weights:
            raw_weights[dimension] = max(0.01, raw_weights[dimension] * multiplier)

    total = sum(raw_weights.values())
    normalized = {dimension: value / total for dimension, value in raw_weights.items()}

    return ProfileConfig(
        name=profile.name,
        weights=normalized,
        source_priorities=tuple(boosted),
    )

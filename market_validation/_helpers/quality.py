"""Quality-gate metrics used by the search service to decide when to retry
or escalate to AI supplementation."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from market_validation._helpers.common import infer_market_profile


def is_useful_business_url(url: str) -> bool:
    """True if *url* looks like a real company website (not an aggregator/directory).

    Delegates to ``_is_aggregator_domain`` so every site in the shared aggregator
    list (Yelp, Toast, NetWaiter, Eventective, Sagemenu, Wix subdomains, social
    networks, etc.) is rejected as a "website" — those are third-party platforms,
    not the company's own presence.
    """
    if not url:
        return False
    try:
        host = (urlparse(url).netloc or "").lower().removeprefix("www.")
    except Exception:
        return False
    if not host or "." not in host:
        return False
    from market_validation.company_enrichment import _is_aggregator_domain
    if _is_aggregator_domain(host):
        return False
    return True


def has_contact_form_or_email_domain(url: str) -> bool:
    if not url:
        return False
    low = url.lower()
    return any(tok in low for tok in ("contact", "about", "support", "help"))


def find_quality_metrics(companies: list[dict[str, Any]]) -> dict[str, int]:
    total = len(companies)
    with_website = 0
    with_phone = 0
    with_location = 0
    for c in companies:
        if str(c.get("website") or c.get("evidence_url") or "").strip():
            with_website += 1
        if str(c.get("phone") or "").strip():
            with_phone += 1
        if str(c.get("location") or "").strip():
            with_location += 1
    return {
        "total": total,
        "with_website": with_website,
        "with_phone": with_phone,
        "with_location": with_location,
    }


def contactability_score(companies: list[dict[str, Any]]) -> dict[str, Any]:
    score_total = 0
    max_total = max(1, len(companies) * 7)

    website_count = 0
    phone_count = 0
    valid_domain_count = 0
    contact_hint_count = 0

    for c in companies:
        website = str(c.get("website") or c.get("evidence_url") or "").strip()
        phone = str(c.get("phone") or "").strip()

        if website:
            website_count += 1
            score_total += 2
            try:
                host = (urlparse(website).netloc or "").lower()
                if host and host not in {"", "wikipedia.org", "www.wikipedia.org", "bbb.org", "www.bbb.org", "opencorporates.com", "www.opencorporates.com"}:
                    valid_domain_count += 1
                    score_total += 2
            except Exception:
                pass

            if has_contact_form_or_email_domain(website):
                contact_hint_count += 1
                score_total += 1

        if phone:
            phone_count += 1
            score_total += 2

    score_100 = int(round((score_total / max_total) * 100))
    score_100 = max(0, min(100, score_100))
    return {
        "score": score_100,
        "website_count": website_count,
        "phone_count": phone_count,
        "valid_domain_count": valid_domain_count,
        "contact_hint_count": contact_hint_count,
    }


def quality_gate_thresholds(market: str, product: str | None) -> dict[str, int]:
    profile = infer_market_profile(market, product)
    category = profile["category"]
    if category == "food":
        return {"min_total": 5, "min_with_website": 2, "min_contactability": 40}
    if category == "saas":
        return {"min_total": 6, "min_with_website": 4, "min_contactability": 55}
    if category == "healthcare":
        return {"min_total": 4, "min_with_website": 2, "min_contactability": 45}
    if category == "industrial":
        return {"min_total": 4, "min_with_website": 2, "min_contactability": 45}
    if category == "services":
        return {"min_total": 5, "min_with_website": 3, "min_contactability": 50}
    return {"min_total": 4, "min_with_website": 2, "min_contactability": 45}


def passes_quality_gate(companies: list[dict[str, Any]], market: str, product: str | None) -> tuple[bool, dict[str, Any]]:
    metrics = find_quality_metrics(companies)
    contactability = contactability_score(companies)
    thresholds = quality_gate_thresholds(market, product)
    passed = (
        metrics["total"] >= thresholds["min_total"]
        and metrics["with_website"] >= thresholds["min_with_website"]
        and contactability["score"] >= thresholds["min_contactability"]
    )
    info = {
        "metrics": metrics,
        "contactability": contactability,
        "thresholds": thresholds,
    }
    return passed, info

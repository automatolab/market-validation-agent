"""Company-level data helpers: name cleaning, dedup, junk/relevance filtering.

These operate on ``CompanyCandidate``-shaped dicts produced by search backends
before the data reaches the database.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from market_validation._helpers.common import infer_market_profile

# ── Name cleaning ────────────────────────────────────────────────────────────

_TITLE_SUFFIXES = [
    r"\s*\|\s*TikTok$",
    r"\s*-\s*YouTube$",
    r"\s*\|\s*YouTube$",
    r"\s*-\s*Instagram$",
    r"\s*\|\s*Instagram$",
    r"\s*-\s*Facebook$",
    r"\s*\|\s*Facebook$",
    r"\s*\|\s*LinkedIn$",
    r"\s*-\s*LinkedIn$",
    r"\s*\|\s*Twitter$",
    r"\s*-\s*Yelp$",
    r"\s*\|\s*Yelp$",
    r"\s*-\s*TripAdvisor$",
    r"\s*\|\s*TripAdvisor$",
    r"\s*-\s*Google Maps$",
    r"\s*\|\s*Google Maps$",
    r"\s*-\s*DoorDash$",
    r"\s*\|\s*DoorDash$",
    r"\s*\|\s*Foursquare$",
    r"\s*\|\s*Zomato$",
    # Geo suffixes appended to business names (e.g. "SmokeHouseSanJoseCA")
    r",?\s+San\s+Jose[,\s]+CA[,]?$",
    r",?\s+San\s+Jose[,\s]+California[,]?$",
    r"SanJoseCA?$",
    r"SanJoseCa$",
    # MapQuest / directory address suffix: ", City, ST 00000, US - MapQuest"
    r",\s+[A-Za-z ]+,\s+[A-Z]{2}\s+\d{5}(?:,\s*US)?\s*-\s*MapQuest$",
    r"\s*-\s*MapQuest$",
    # Other directory suffixes
    r"\s*-\s*Foursquare$",
    r"\s*\|\s*MapQuest$",
]
_TITLE_SUFFIX_RE = re.compile("|".join(_TITLE_SUFFIXES), re.IGNORECASE)

_TITLE_PREFIX_RE = re.compile(
    r"^(?:Menu|Home|About|Order|Catering|Shop|Store|Contact|Gallery|Blog"
    r"|News|Events|Reviews|Jobs|Careers|Services|Products|Photos)\s*\|\s*",
    re.IGNORECASE,
)

_JUNK_TITLE_RE = re.compile(
    r"^(?:The\s+\d+\s+Best\b|Best\s+\w+\s+in\b|\d+\s+Best\b|Top\s+\d+\b"
    r"|Review:\s|First\s+Visit\b|Food\s+Adventure\b)",
    re.IGNORECASE,
)


def clean_company_name(raw: str) -> str:
    """Strip platform suffixes/prefixes, CamelCase geo tags, and noise from page titles."""
    name = raw.strip()
    name = _TITLE_PREFIX_RE.sub("", name).strip()
    prev = None
    while prev != name:
        prev = name
        name = _TITLE_SUFFIX_RE.sub("", name).strip()
    name = name.strip("|-– —\t")
    name = re.sub(r"\s{2,}", " ", name).strip()
    return name or raw.strip()


def normalize_companies(companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for c in companies:
        normalized.append(
            {
                "company_name": clean_company_name(c.get("company_name") or c.get("name") or c.get("title", "") or "Unknown"),
                "website": c.get("website") or c.get("url", ""),
                "location": c.get("location") or c.get("address", ""),
                "phone": c.get("phone", ""),
                "description": c.get("description") or c.get("specialty", "") or c.get("notes", ""),
                "evidence_url": c.get("evidence_url") or c.get("url", ""),
                "source": c.get("source", "unknown"),
            }
        )
    return normalized


# ── Deduplication ────────────────────────────────────────────────────────────

_NAME_LEADING_ARTICLES = re.compile(r"^(?:the|a|an)\s+", re.IGNORECASE)
_NAME_TRAILING_SUFFIXES = re.compile(
    r"\s+(?:company|co|corp|corporation|inc|incorporated|llc|ltd|limited|group|holdings|enterprises|plc)\.?$",
    re.IGNORECASE,
)
_NAME_PUNCT = re.compile(r"[^\w\s]+")


def dedupe_key_website(url: str | None) -> str:
    """Canonicalize a URL for dedup. Strips scheme, www, trailing slash, case."""
    if not url:
        return ""
    u = str(url).strip().lower()
    u = re.sub(r"^https?://", "", u)
    if u.startswith("www."):
        u = u[4:]
    return u.rstrip("/")


def dedupe_key_name(name: str | None) -> str:
    """Canonicalize a company name for dedup.

    Strips leading articles (The/A/An), trailing corporate suffixes
    (Company/Inc/LLC/Corp/...), punctuation, and collapses whitespace.
    """
    if not name:
        return ""
    n = str(name).strip().lower()
    n = _NAME_LEADING_ARTICLES.sub("", n)
    n = _NAME_TRAILING_SUFFIXES.sub("", n)
    n = _NAME_PUNCT.sub(" ", n)
    return " ".join(n.split())


def dedupe_companies(companies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate by (canonical website) OR (canonical company name).

    A row matches an existing entry if either key collides — so
    "Smoking Pig BBQ Company" at https://www.smokingpigbbq.net/ and
    "Smoking Pig BBQ" at https://smokingpigbbq.net collapse to one.
    """
    deduped: list[dict[str, Any]] = []
    seen_web: dict[str, int] = {}
    seen_name: dict[str, int] = {}

    for c in companies:
        web_key = dedupe_key_website(c.get("website") or c.get("evidence_url"))
        name_key = dedupe_key_name(c.get("company_name"))
        if not web_key and not name_key:
            continue

        existing_idx: int | None = None
        if web_key and web_key in seen_web:
            existing_idx = seen_web[web_key]
        elif name_key and name_key in seen_name:
            existing_idx = seen_name[name_key]

        if existing_idx is not None:
            kept = deduped[existing_idx]
            for field in ("website", "phone", "email", "location", "description", "evidence_url"):
                if not kept.get(field) and c.get(field):
                    kept[field] = c[field]
            if web_key and web_key not in seen_web:
                seen_web[web_key] = existing_idx
            if name_key and name_key not in seen_name:
                seen_name[name_key] = existing_idx
            continue

        idx = len(deduped)
        deduped.append(c)
        if web_key:
            seen_web[web_key] = idx
        if name_key:
            seen_name[name_key] = idx

    return deduped


# ── Junk detection / relevance filtering ─────────────────────────────────────

_JUNK_NAME_PATTERNS = [
    "search results for",
    "better business bureau",
    "privacy policy",
    "cookie policy",
    "terms of service",
    "site map",
    "yellow pages",
    "yelp search",
    "| tiktok",
    "- youtube",
    "- tiktok",
    "| youtube",
    "on tiktok",
    "on youtube",
    "- instagram",
    "| instagram",
    "- facebook",
    "| facebook",
    "top 10",
    "top 5",
    "10 best",
    "8 best",
    "5 best",
    "near me",
    " review:",
    "review: ",
    "- review",
    "| review",
    "chrome web store",
    "chrome extension",
    "app store",
    "google play",
    "first visit",
    "food adventure",
    "senior living",
    "senior community",
    "assisted living",
    "memory care",
    "job listing",
    "jobs available",
    "salary in",
    "text to speech",
    "voice reader",
    "is so good, it",
    "irresistible ",
    "you need to try",
    "must try",
    "family destinations",
    "serves up the best",
    "guide to local",
    "craigslist:",
    "craigslist.org",
    " - american restaurant in",
    " - mexican restaurant in",
    "items/",
    "/menu/",
]

_BLOCKED_URL_HOSTS = {
    "bbb.org", "www.bbb.org",
    "wikipedia.org", "www.wikipedia.org",
    "opencorporates.com", "www.opencorporates.com",
    "yellowpages.com", "www.yellowpages.com",
    "yelp.com", "www.yelp.com",
    "tripadvisor.com", "www.tripadvisor.com",
    "google.com", "www.google.com",
    "youtube.com", "www.youtube.com",
    "tiktok.com", "www.tiktok.com",
    "instagram.com", "www.instagram.com",
    "facebook.com", "www.facebook.com",
    "twitter.com", "www.twitter.com",
    "x.com",
    "linkedin.com", "www.linkedin.com",
    "chromewebstore.google.com",
    "chrome.google.com",
    "doordash.com", "www.doordash.com",
    "ubereats.com", "www.ubereats.com",
    "grubhub.com", "www.grubhub.com",
    "postmates.com",
    "seamless.com",
    "allmenus.com",
    "menupix.com",
    "zomato.com",
    "opentable.com",
    "restaurantji.com",
    "sirved.com",
    "foursquare.com",
    "reddit.com", "www.reddit.com",
    "quora.com", "www.quora.com",
    "pinterest.com", "www.pinterest.com",
    "theguardian.com",
    "nytimes.com",
    "sfgate.com",
    "mercurynews.com",
    "bizjournals.com",
    "crunchbase.com",
    "dnb.com",
    "manta.com",
    "chamberofcommerce.com",
    "expertise.com",
    "thumbtack.com",
    "angieslist.com",
    "homeadvisor.com",
    "bark.com",
    "mapquest.com", "www.mapquest.com",
    "maps.apple.com",
    "waze.com", "www.waze.com",
    "craigslist.org", "sfbay.craigslist.org",
    "6amcity.com", "sjtoday.6amcity.com",
    "patch.com",
    "nextdoor.com",
    "familydestinationsguide.com",
    "onlyinyourstate.com",
    "roadsnacks.net",
    "wideopeneats.com",
    "lovefood.com",
}


def is_junk_company(c: dict[str, Any]) -> bool:
    name = str(c.get("company_name", "")).lower().strip()
    if not name or len(name) < 3:
        return True
    if any(pat in name for pat in _JUNK_NAME_PATTERNS):
        return True
    url = str(c.get("website") or c.get("evidence_url") or "").strip()
    if url:
        try:
            host = (urlparse(url).netloc or "").lower().removeprefix("www.")
            if host in {h.removeprefix("www.") for h in _BLOCKED_URL_HOSTS}:
                return True
        except Exception:
            pass
    return False


def filter_relevant_companies(
    companies: list[dict[str, Any]],
    market: str,
    product: str | None,
    extra_junk_signals: list[str] | None = None,
    extra_real_signals: list[str] | None = None,
) -> list[dict[str, Any]]:
    profile = infer_market_profile(market, product)
    key_tokens = set(profile["tokens"]) | set(profile["positive_tokens"])
    if extra_real_signals:
        key_tokens |= {s.lower() for s in extra_real_signals}
    blocked_tokens = set(profile["blocked_tokens"])
    if extra_junk_signals:
        blocked_tokens |= {s.lower() for s in extra_junk_signals}
    banned_name_tokens = set(profile["banned_name_tokens"])

    filtered: list[dict[str, Any]] = []
    for c in companies:
        if is_junk_company(c):
            continue
        company_name = str(c.get("company_name", "")).strip()
        hay = " ".join(
            str(c.get(field, "")).lower()
            for field in ("company_name", "description", "location", "website", "evidence_url")
        )
        low_name = company_name.lower()
        if banned_name_tokens and any(tok in low_name for tok in banned_name_tokens):
            continue
        if any(bt in hay for bt in blocked_tokens):
            continue
        if c.get("source") == "wikipedia":
            continue
        if key_tokens and not any(token in hay for token in key_tokens):
            continue
        filtered.append(c)
    return filtered

"""Company-level data helpers: name cleaning, dedup, junk/relevance filtering.

These operate on ``CompanyCandidate``-shaped dicts produced by search backends
before the data reaches the database.
"""

from __future__ import annotations

import re
import unicodedata
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


def _is_mostly_latin(text: str) -> bool:
    """True if >=70% of letters in *text* are Latin script.

    Used to gate NFKD folding — we want to fold 'Café'→'Cafe', but not
    accidentally collapse Japanese/Arabic/Cyrillic strings whose decomposed
    forms would lose meaningful characters. The 70% threshold tolerates the
    occasional non-Latin character in an otherwise Latin name (a Kanji on a
    sushi restaurant's sign, etc.).
    """
    if not text:
        return False
    letters = [ch for ch in text if ch.isalpha()]
    if not letters:
        return False
    latin = sum(1 for ch in letters if "LATIN" in unicodedata.name(ch, ""))
    return (latin / len(letters)) >= 0.7


def dedupe_key_name(name: str | None) -> str:
    """Canonicalize a company name for dedup.

    Strips leading articles (The/A/An), trailing corporate suffixes
    (Company/Inc/LLC/Corp/...), punctuation, and collapses whitespace. Folds
    unicode diacritics via NFKD only when the input is mostly Latin so that
    'Café' and 'Cafe' collapse to one entry without degrading CJK / Cyrillic
    / Arabic strings.
    """
    if not name:
        return ""
    n = str(name).strip()
    if _is_mostly_latin(n):
        # NFKD-fold so accented variants ('Café' / 'Cafe', 'Müller' / 'Muller') match
        n = unicodedata.normalize("NFKD", n)
        n = "".join(ch for ch in n if not unicodedata.combining(ch))
    n = n.lower()
    n = _NAME_LEADING_ARTICLES.sub("", n)
    n = _NAME_TRAILING_SUFFIXES.sub("", n)
    n = _NAME_PUNCT.sub(" ", n)
    return " ".join(n.split())


# Subsidiary / per-location patterns. e.g. "McDonald's #1234 San Jose",
# "Starbucks (Mountain View)", "Walgreens - Pharmacy 4567" — collapse all of
# these onto the parent brand for dedup so we don't email the same brand twice.
_SUBSIDIARY_TRAIL_RE = re.compile(
    r"\s*(?:#\d+|\(\d+\)|store\s*#?\d+|location\s*#?\d+|"
    r"unit\s*#?\d+|branch\s*#?\d+|shop\s*#?\d+|loc\s*#?\d+)$",
    re.IGNORECASE,
)
_PARENT_LOCATION_TRAIL_RE = re.compile(
    r"\s+[\-–—]\s+[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)?$"
)


# Archetypes where each franchise / branch / location is a DISTINCT lead.
# Collapsing "McDonald's #1234" and "McDonald's #5678" would lose half the
# call sheet for these markets.
_KEEP_LOCATIONS_DISTINCT_ARCHETYPES: frozenset[str] = frozenset({
    "local-service",
    "consumer-cpg",
    "healthcare",
})


def parent_brand_key(name: str | None, archetype: str | None = None) -> str | None:
    """Return a parent-brand dedup key when *name* looks like a subsidiary.

    Returns None when no subsidiary pattern matches OR when the archetype
    treats per-location franchises as distinct leads. Used to collapse
    "McDonald's #1234" and "McDonald's #5678" onto a single 'mcdonalds'
    parent — but ONLY for archetypes where corporate-level coverage is
    enough (b2b-saas, services-agency, b2b-industrial).

    For local-service / consumer-cpg / healthcare, each location is its own
    sales target; this function returns None so the canonical-name dedup
    keeps them separate.
    """
    if not name:
        return None
    if archetype and archetype in _KEEP_LOCATIONS_DISTINCT_ARCHETYPES:
        return None
    n = str(name).strip()
    matched = False
    while True:
        new_n = _SUBSIDIARY_TRAIL_RE.sub("", n).strip()
        if new_n == n:
            break
        n = new_n
        matched = True
    if matched and n:
        return dedupe_key_name(n)
    return None


def dedupe_companies(
    companies: list[dict[str, Any]],
    archetype: str | None = None,
) -> list[dict[str, Any]]:
    """Deduplicate by (canonical website) OR (canonical name) OR (parent brand).

    Collapses:
      - "Smoking Pig BBQ Company" at smokingpigbbq.net and "Smoking Pig BBQ"
        at smokingpigbbq.net (same canonical name + same canonical site).
      - "McDonald's #1234" and "McDonald's #5678" — ONLY when the archetype
        treats franchise locations as redundant (e.g. b2b-saas selling to
        the parent). Local-service / CPG / healthcare keep them distinct.
      - "Café" and "Cafe" (NFKD-normalized canonical name, Latin only).
    """
    deduped: list[dict[str, Any]] = []
    seen_web: dict[str, int] = {}
    seen_name: dict[str, int] = {}
    seen_parent: dict[str, int] = {}

    for c in companies:
        web_key = dedupe_key_website(c.get("website") or c.get("evidence_url"))
        name_key = dedupe_key_name(c.get("company_name"))
        parent_key = parent_brand_key(c.get("company_name"), archetype=archetype)
        if not web_key and not name_key:
            continue

        existing_idx: int | None = None
        if web_key and web_key in seen_web:
            existing_idx = seen_web[web_key]
        elif name_key and name_key in seen_name:
            existing_idx = seen_name[name_key]
        elif parent_key and parent_key in seen_parent:
            existing_idx = seen_parent[parent_key]

        if existing_idx is not None:
            kept = deduped[existing_idx]
            for field in ("website", "phone", "email", "location", "description", "evidence_url"):
                if not kept.get(field) and c.get(field):
                    kept[field] = c[field]
            if web_key and web_key not in seen_web:
                seen_web[web_key] = existing_idx
            if name_key and name_key not in seen_name:
                seen_name[name_key] = existing_idx
            if parent_key and parent_key not in seen_parent:
                seen_parent[parent_key] = existing_idx
            continue

        idx = len(deduped)
        deduped.append(c)
        if web_key:
            seen_web[web_key] = idx
        if name_key:
            seen_name[name_key] = idx
        if parent_key:
            seen_parent[parent_key] = idx

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
    "top 7",
    "top 12",
    "top 20",
    "10 best",
    "8 best",
    "5 best",
    "12 best",
    "20 best",
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
    # Expanded SEO-roundup patterns
    " you need to know",
    " you should know",
    "you can't miss",
    "essential guide",
    "ultimate guide",
    "complete guide",
    "definitive guide",
    "ranked from",
    "roundup",
    "round-up",
    " curated list",
    "comparison: ",
    "head to head",
    "head-to-head",
    "vs.: ",
    "alternatives to",
    "best alternatives",
    "what we know",
    "everything you",
    "things you",
]

# SEO listicle structural pattern — catches title formats not in the substring
# list: "The 12 Most Essential X", "10+ Top X to Try Now", etc.
#
# Tightened: requires a listicle adjective AND an explicit plural noun OR a
# trailing "you ... try" / "to try" phrase, so legitimate names like "The 5
# Stars Diner", "Top Hat Cafe", "Best Buy" don't get caught. The structural
# check used to fire on any "<number> <adjective>" prefix.
_LISTICLE_ADJ = (
    r"most|essential|unmissable|amazing|incredible|epic|legendary|hottest|"
    r"coolest|hidden|underrated|noteworthy|popular|favorite|finest|exceptional|"
    r"outstanding|remarkable|iconic|noteworthy|noteworthy"
)
_LISTICLE_STRUCTURAL_RE = re.compile(
    rf"^(?:the\s+)?\d{{1,3}}(?:\+|st|nd|rd|th)?\s+(?:{_LISTICLE_ADJ})\s+\w+",
    re.IGNORECASE,
)

# Numeric prefix + plural noun ("12 Restaurants in San Jose"): a listicle title
# almost never resolves to a single company. Require:
#   - leading number (2-99)
#   - followed by a word ending in `s` (plural noun)
#   - followed by a locator preposition (in/near/at/around)
# The number range excludes "0" / "1" so it doesn't catch "1 Hour Photo" or
# similar legitimate names.
_NUMERIC_LISTICLE_RE = re.compile(
    r"^(?:\d{2,3}|[2-9])\+?\s+[A-Za-z]+s\s+(?:in|near|at|around|to|for)\b",
    re.IGNORECASE,
)

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
    # General news domains — a company-name pulled from a news article title
    # is not a real company, and the article URL is not a real website.
    "theguardian.com", "nytimes.com", "wsj.com", "ft.com",
    "bloomberg.com", "reuters.com", "apnews.com", "ap.org",
    "npr.org", "bbc.com", "bbc.co.uk", "cnn.com",
    "washingtonpost.com", "latimes.com",
    "sfgate.com", "sfchronicle.com", "mercurynews.com",
    "nbcnews.com", "abcnews.go.com", "cbsnews.com", "foxnews.com",
    "businessinsider.com", "fortune.com", "forbes.com", "inc.com",
    "fastcompany.com", "axios.com", "voanews.com",
    "techcrunch.com", "theverge.com", "wired.com",
    "vice.com", "buzzfeed.com", "huffpost.com",
    "bizjournals.com", "patch.com",
    # Trade/agritech press — surfaced articles ABOUT growers as candidates
    "globalaginvesting.com", "agriinvestor.com", "agfundernews.com",
    "modernfarmer.com", "agritecture.com", "urbanvine.co",
    "growertalks.com", "hortidaily.com", "freshplaza.com",
    "thepacker.com", "agdaily.com",
    "optimistdaily.com", "smartcitiesdive.com", "fooddive.com",
    "supermarketnews.com",
    # Press release wire
    "prnewswire.com", "businesswire.com", "globenewswire.com",
    "marketwire.com", "einpresswire.com", "accesswire.com",
    # Academic / research publishers (PDFs of papers as "companies")
    "researchgate.net", "academia.edu", "arxiv.org",
    "pubmed.ncbi.nlm.nih.gov", "ncbi.nlm.nih.gov",
    "scholar.google.com", "doi.org",
    # Code repos / package indexes
    "github.com", "gitlab.com", "bitbucket.org",
    "pypi.org", "npmjs.com", "rubygems.org",
    # Crunchbase / data / lead-gen platforms
    "crunchbase.com", "dnb.com", "zoominfo.com", "rocketreach.co",
    "manta.com", "chamberofcommerce.com", "expertise.com",
    "thumbtack.com", "angieslist.com", "homeadvisor.com", "bark.com",
    # Maps / directions
    "mapquest.com", "www.mapquest.com",
    "maps.apple.com",
    "waze.com", "www.waze.com",
    # Local content / community
    "craigslist.org", "sfbay.craigslist.org",
    "6amcity.com", "sjtoday.6amcity.com",
    "nextdoor.com",
    "familydestinationsguide.com",
    "onlyinyourstate.com",
    "roadsnacks.net",
    "wideopeneats.com",
    "lovefood.com",
}

# URL path patterns that signal a news-article rather than a company website.
# Used as a soft check on top of host-blocklist for hosts we don't recognize
# but whose path screams "this is editorial content".
_ARTICLE_PATH_RE = re.compile(
    r"/(?:articles?|news|story|stories|blog|posts?|press(?:[-/]release)?|"
    r"opinion|review|category|tag|topic|p)/|"
    r"/\d{4}/(?:\d{2}/)?(?:\d{2}/)?",  # /2024/03/10/ date paths
    re.IGNORECASE,
)


def is_junk_company(c: dict[str, Any]) -> bool:
    raw_name = str(c.get("company_name", "")).strip()
    name = raw_name.lower()
    if not name or len(name) < 3:
        return True
    if any(pat in name for pat in _JUNK_NAME_PATTERNS):
        return True
    # Listicle structural patterns ("The 12 Essential...", "10+ Top X to Try")
    if _LISTICLE_STRUCTURAL_RE.match(raw_name):
        return True
    if _NUMERIC_LISTICLE_RE.match(raw_name):
        return True
    # Names longer than 8 words are almost always SEO blog post titles, not
    # actual business names. Real business names are 1-6 words.
    if raw_name and len(raw_name.split()) > 9:
        return True
    url = str(c.get("website") or c.get("evidence_url") or "").strip()
    if url:
        try:
            parsed_url = urlparse(url)
            host = (parsed_url.netloc or "").lower().removeprefix("www.")
            blocked_set = {h.removeprefix("www.") for h in _BLOCKED_URL_HOSTS}
            # Direct host match — fast path.
            if host in blocked_set:
                return True
            # Subdomain match — sub.example.com matches example.com
            if any(host.endswith("." + b) for b in blocked_set):
                return True
            # Article-path heuristic — /2024/03/10/, /article/, /news/, /blog/.
            # Catches news/article URLs on hosts not yet in the blocklist.
            if _ARTICLE_PATH_RE.search(parsed_url.path or ""):
                return True
            # Path contains ".pdf" — academic papers and downloadable assets
            # are content, not companies.
            if (parsed_url.path or "").lower().endswith(".pdf"):
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

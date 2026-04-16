"""
Multi-backend company search using free sources (no API keys required).

Queries Nominatim/OpenStreetMap, DuckDuckGo (DDGS), Wikipedia, BBB,
OpenCorporates, and Manta in parallel. All backends are best-effort
with automatic fallback; results include source attribution metadata.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


USER_AGENT = "market-validation-agent/0.1"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
BBB_SEARCH_URL = "https://www.bbb.org/search"
OPENCORPORATES_SEARCH_URL = "https://opencorporates.com/companies"

# Cache geocoded bounding boxes so we don't re-geocode the same geography
_GEO_BBOX_CACHE: dict[str, tuple[float, float, float, float] | None] = {}

# Circuit breaker: skip DDGS for the session once it's rate-limited
_DDGS_DISABLED = False

# Track last Nominatim request time (usage policy: max 1 req/sec)
_LAST_NOMINATIM_TIME: float = 0.0


@dataclass
class SearchResult:
    title: str
    url: str
    snippet: str
    source: str

    def to_dict(self) -> dict[str, str]:
        return {
            "title": self.title,
            "url": self.url,
            "snippet": self.snippet,
            "source": self.source,
        }


def _safe_get(url: str, params: dict[str, Any] | None = None, timeout: int = 20) -> requests.Response | None:
    try:
        resp = requests.get(
            url,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
        )
        # Retry once on 429 rate-limit
        if resp.status_code == 429:
            time.sleep(2)
            resp = requests.get(
                url,
                params=params,
                headers={"User-Agent": USER_AGENT},
                timeout=timeout,
            )
        return resp
    except Exception:
        return None


def _extract_location_hint(query: str) -> str:
    parts = [p.strip() for p in query.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[-2] + ", " + parts[-1]
    if parts:
        return parts[-1]
    return ""


def _nominatim_throttle() -> None:
    """Ensure at least 1.1s between Nominatim requests (their usage policy)."""
    global _LAST_NOMINATIM_TIME
    elapsed = time.time() - _LAST_NOMINATIM_TIME
    if elapsed < 1.1:
        time.sleep(1.1 - elapsed)
    _LAST_NOMINATIM_TIME = time.time()


def _geocode_bbox(geography: str) -> tuple[float, float, float, float] | None:
    """Geocode a geography string to a bounding box (lon1, lat1, lon2, lat2).

    Returns None if geocoding fails.  Results are cached in _GEO_BBOX_CACHE.
    """
    key = geography.strip().lower()
    if key in _GEO_BBOX_CACHE:
        return _GEO_BBOX_CACHE[key]

    _nominatim_throttle()
    resp = _safe_get(
        NOMINATIM_URL,
        {
            "q": geography,
            "format": "jsonv2",
            "limit": 1,
        },
        timeout=15,
    )
    if resp is None or resp.status_code != 200:
        _GEO_BBOX_CACHE[key] = None
        return None

    try:
        items = resp.json()
    except Exception:
        _GEO_BBOX_CACHE[key] = None
        return None

    if not items:
        _GEO_BBOX_CACHE[key] = None
        return None

    bbox = items[0].get("boundingbox")  # [south_lat, north_lat, west_lon, east_lon]
    if bbox and len(bbox) == 4:
        try:
            south, north, west, east = float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])
            # Expand bbox by ~50% to include wider metro / surrounding area
            lat_pad = (north - south) * 0.5
            lon_pad = (east - west) * 0.5
            result = (west - lon_pad, south - lat_pad, east + lon_pad, north + lat_pad)
            _GEO_BBOX_CACHE[key] = result
            return result
        except (ValueError, TypeError):
            pass

    _GEO_BBOX_CACHE[key] = None
    return None


def _parse_nominatim_results(payload: list[dict[str, Any]], num_results: int) -> list[SearchResult]:
    """Parse Nominatim JSON response items into SearchResult list."""
    results: list[SearchResult] = []
    for item in payload:
        display_name = item.get("display_name", "")
        if not display_name:
            continue

        tags = item.get("extratags") or {}
        name = (item.get("namedetails") or {}).get("name") or display_name.split(",")[0].strip()
        website = tags.get("website") or tags.get("contact:website") or ""
        cuisine = tags.get("cuisine", "")
        phone = tags.get("phone") or tags.get("contact:phone") or ""
        email = tags.get("email") or tags.get("contact:email") or ""

        snippet_parts = [display_name]
        if cuisine:
            snippet_parts.append(f"cuisine={cuisine}")
        if phone:
            snippet_parts.append(f"phone={phone}")
        if email:
            snippet_parts.append(f"email={email}")

        results.append(
            SearchResult(
                title=name,
                url=website,
                snippet=" | ".join(snippet_parts),
                source="nominatim",
            )
        )

    return results[:num_results]


# Filler words stripped when simplifying a verbose query for Nominatim,
# which interprets the whole string as a single place/amenity phrase.
_NOMINATIM_FILLER = {
    "the", "a", "an", "and", "or", "of", "for", "with", "in", "at", "on", "to",
    "best", "top", "near", "me", "local", "independent", "small", "large",
    "company", "companies", "business", "businesses", "service", "services",
    "supplier", "suppliers", "store", "stores", "shop", "shops", "custom", "whole",
    "bulk", "wholesale", "retail", "food", "purveyor", "purveyors",
}


def _simplify_for_nominatim(query: str, geography: str | None) -> str:
    """Reduce a verbose AI-generated query to a short Nominatim-friendly phrase.

    Strips filler tokens and tokens that appear in *geography* (which is already
    applied via viewbox). Keeps the first 3 remaining content tokens.
    """
    geo_tokens: set[str] = set()
    if geography:
        geo_tokens = {
            re.sub(r"[^\w]", "", t.lower())
            for t in re.split(r"[\s,]+", geography)
            if len(t) >= 2
        }
    tokens = [re.sub(r"[^\w]", "", t) for t in query.split()]
    kept: list[str] = []
    for tok in tokens:
        low = tok.lower()
        if not low or low in _NOMINATIM_FILLER or low in geo_tokens:
            continue
        kept.append(tok)
        # Nominatim is a geocoder — it matches the query as a single phrase
        # against OSM names. 2 content tokens is the sweet spot; 3+ usually
        # misses because no place name contains all three verbatim.
        if len(kept) >= 2:
            break
    return " ".join(kept) if kept else query


def _from_nominatim(query: str, num_results: int = 10, geography: str | None = None) -> list[SearchResult]:
    params: dict[str, Any] = {
        "q": query,
        "format": "jsonv2",
        "addressdetails": 1,
        "extratags": 1,
        "namedetails": 1,
        "limit": max(1, min(num_results, 40)),
    }

    used_bounding = False
    # If geography is provided, geocode it and apply viewbox bounding
    if geography:
        bbox = _geocode_bbox(geography)
        if bbox:
            # viewbox format: lon1,lat1,lon2,lat2 (west,south,east,north)
            params["viewbox"] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
            params["bounded"] = 1
            used_bounding = True

    _nominatim_throttle()
    resp = _safe_get(NOMINATIM_URL, params, timeout=20)
    if resp is None or resp.status_code != 200:
        return []

    try:
        payload = resp.json()
    except Exception:
        return []

    results = _parse_nominatim_results(payload, num_results)

    # Retry with a simplified query if the verbose form produced too few results.
    # Nominatim treats the whole query as a single phrase and fails on long AI queries.
    if len(results) < max(3, num_results // 3):
        simplified = _simplify_for_nominatim(query, geography)
        if simplified and simplified.lower() != query.lower():
            simpler = dict(params)
            simpler["q"] = simplified
            _nominatim_throttle()
            resp_s = _safe_get(NOMINATIM_URL, simpler, timeout=20)
            if resp_s and resp_s.status_code == 200:
                try:
                    payload_s = resp_s.json()
                    extra = _parse_nominatim_results(payload_s, num_results)
                    seen = {r.title.lower().strip() for r in results}
                    for r in extra:
                        if r.title.lower().strip() not in seen:
                            results.append(r)
                            seen.add(r.title.lower().strip())
                except Exception:
                    pass

    # Fallback: if bounded search returned too few results, retry without bounding
    # so we don't miss businesses just outside the bbox edge
    if used_bounding and len(results) < max(3, num_results // 3):
        unbounded_params = dict(params)
        unbounded_params.pop("viewbox", None)
        unbounded_params.pop("bounded", None)
        _nominatim_throttle()
        resp2 = _safe_get(NOMINATIM_URL, unbounded_params, timeout=20)
        if resp2 and resp2.status_code == 200:
            try:
                payload2 = resp2.json()
                extra = _parse_nominatim_results(payload2, num_results)
                # Merge, dedup by title
                seen = {r.title.lower().strip() for r in results}
                for r in extra:
                    if r.title.lower().strip() not in seen:
                        results.append(r)
                        seen.add(r.title.lower().strip())
            except Exception:
                pass

    return results[:num_results]


def _from_ddgs(query: str, num_results: int = 10, region: str | None = None) -> list[SearchResult]:
    global _DDGS_DISABLED
    if _DDGS_DISABLED:
        return []

    DDGS = None
    try:
        from ddgs import DDGS  # type: ignore[no-redef]
    except Exception:
        try:
            from duckduckgo_search import DDGS  # type: ignore[no-redef]
        except Exception:
            return []

    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            results: list[SearchResult] = []
            ddgs = DDGS()
            kwargs: dict[str, Any] = {"max_results": max(1, min(num_results, 25))}
            if region:
                kwargs["region"] = region  # e.g. "us-en", "wt-wt" (default)
            for row in ddgs.text(query, **kwargs):
                results.append(
                    SearchResult(
                        title=row.get("title", ""),
                        url=row.get("href", row.get("url", "")),
                        snippet=row.get("body", row.get("snippet", "")),
                        source="ddgs",
                    )
                )
            return results[:num_results]
        except Exception as exc:
            if "ratelimit" in str(exc).lower():
                if attempt < max_attempts - 1:
                    time.sleep(3)
                    continue
                # Rate-limited twice — disable DDGS for the rest of this session
                _DDGS_DISABLED = True
            return []
    return []


def _from_wikipedia(query: str, num_results: int = 8) -> list[SearchResult]:
    q_lower = query.lower()
    # Wikipedia is usually noisy for local lead discovery (returns people/shows/list pages).
    if any(tok in q_lower for tok in ("restaurant", "restaurants", "bbq", "barbecue", "brisket", "catering", "clinic", "medical", "saas", "software", "agency", "consulting", "manufacturer", "supplier")) and (
        "," in query or " near " in q_lower or " san jose" in q_lower or " california" in q_lower
    ):
        return []

    resp = _safe_get(
        WIKIPEDIA_API_URL,
        {
            "action": "query",
            "list": "search",
            "srsearch": query,
            "utf8": 1,
            "format": "json",
            "srlimit": max(1, min(num_results, 20)),
        },
        timeout=20,
    )
    if resp is None or resp.status_code != 200:
        return []

    try:
        payload = resp.json()
    except Exception:
        return []

    rows = payload.get("query", {}).get("search", [])
    tokens = [
        t
        for t in re.split(r"\s+", query.lower())
        if len(t) >= 3 and t not in {"restaurant", "restaurants", "company", "companies", "near", "the", "and", "for", "with", "san", "jose", "california"}
    ]
    results: list[SearchResult] = []
    for row in rows:
        title = row.get("title", "")
        page_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}" if title else ""
        snippet = re.sub(r"<[^>]+>", "", row.get("snippet", ""))
        hay = f"{title} {snippet}".lower()
        if tokens and not any(tok in hay for tok in tokens):
            continue
        results.append(SearchResult(title=title, url=page_url, snippet=snippet, source="wikipedia"))
    return results[:num_results]


def _from_bbb(query: str, num_results: int = 10) -> list[SearchResult]:
    location_hint = _extract_location_hint(query)
    tokens = [t for t in re.split(r"\s+", query.lower()) if t]
    filtered = [t for t in tokens if t not in {"restaurant", "restaurants", "company", "companies", "in", "near", "ca", "california", "san", "jose"}]
    find_text = " ".join(filtered[:4]) if filtered else query
    resp = _safe_get(
        BBB_SEARCH_URL,
        {
            "find_country": "USA",
            "find_loc": location_hint or "United States",
            "find_text": find_text,
        },
        timeout=12,
    )
    if resp is None or resp.status_code != 200:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    results: list[SearchResult] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "/us/" not in href or "/profile/" not in href:
            continue
        title = a.get_text(" ", strip=True)
        if not title:
            continue
        absolute = urljoin("https://www.bbb.org", href)
        if absolute in seen:
            continue
        seen.add(absolute)
        lower_title = title.lower()
        if filtered and not any(tok in lower_title for tok in filtered):
            continue

        results.append(
            SearchResult(
                title=title,
                url=absolute,
                snippet=f"BBB profile result for query: {find_text}",
                source="bbb",
            )
        )
        if len(results) >= num_results:
            break

    return results


def _from_opencorporates(query: str, num_results: int = 8) -> list[SearchResult]:
    """Best-effort parser; may return empty due to captcha/anti-bot."""
    resp = _safe_get(OPENCORPORATES_SEARCH_URL, {"q": query}, timeout=12)
    if resp is None or resp.status_code != 200:
        return []

    html = resp.text.lower()
    if "captcha" in html or "just a moment" in html:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    results: list[SearchResult] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "/companies/" not in href:
            continue
        title = a.get_text(" ", strip=True)
        if not title:
            continue
        absolute = urljoin("https://opencorporates.com", href)
        if absolute in seen:
            continue
        seen.add(absolute)
        results.append(
            SearchResult(
                title=title,
                url=absolute,
                snippet=f"OpenCorporates result for query: {query}",
                source="opencorporates",
            )
        )
        if len(results) >= num_results:
            break

    return results


def _from_manta(query: str, num_results: int = 8) -> list[SearchResult]:
    """Search Manta.com business directory — covers all US geographies."""
    resp = _safe_get("https://www.manta.com/search", {"search[q]": query}, timeout=20)
    if resp is None or resp.status_code != 200:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    results: list[SearchResult] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        if not text or len(text) < 3:
            continue
        if "/c/" not in href and "/mt/" not in href:
            continue
        absolute = urljoin("https://www.manta.com", href) if href.startswith("/") else href
        if absolute in seen:
            continue
        seen.add(absolute)
        results.append(
            SearchResult(
                title=text,
                url=absolute,
                snippet=f"Manta business directory result for: {query}",
                source="manta",
            )
        )
        if len(results) >= num_results:
            break

    return results


def search_all_backends(query: str, num_results: int = 10, geography: str | None = None) -> list[dict[str, str]]:
    """
    Run all free backends with polite throttling.
    Deduplicates by (title,url).
    When *geography* is provided, geo-aware backends constrain results to that area.
    """
    collected: list[SearchResult] = []

    # Geo-aware backends get the geography parameter
    geo_backends: list[tuple[str, Any]] = [
        ("nominatim", lambda q, n: _from_nominatim(q, n, geography=geography)),
        ("ddgs", lambda q, n: _from_ddgs(q, n, region=_geography_to_ddgs_region(geography))),
    ]
    plain_backends = [
        _from_wikipedia,
        _from_bbb,
        _from_opencorporates,
        _from_manta,
    ]

    for _label, backend in geo_backends:
        try:
            batch = backend(query, num_results)
            if batch:
                collected.extend(batch)
        except Exception:
            pass
        time.sleep(0.3)

    for backend in plain_backends:
        try:
            batch = backend(query, num_results)
            if batch:
                collected.extend(batch)
        except Exception:
            pass
        time.sleep(0.3)

    deduped: dict[tuple[str, str], SearchResult] = {}
    for r in collected:
        key = (r.title.lower().strip(), r.url.lower().strip())
        if key not in deduped:
            deduped[key] = r

    return [r.to_dict() for r in list(deduped.values())[: max(1, num_results)]]


def _geography_to_ddgs_region(geography: str | None) -> str | None:
    """Best-effort mapping from geography string to DuckDuckGo region code."""
    if not geography:
        return None
    geo = geography.lower().strip()
    # US states / common patterns
    us_states = {
        "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id",
        "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms",
        "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
        "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv",
        "wi", "wy",
    }
    parts = [p.strip().lower() for p in geo.replace("-", " ").split(",")]
    # Check if any part is a US state abbreviation or contains "us"
    for p in parts:
        if p in us_states or "united states" in p or p == "us" or p.startswith("us-"):
            return "us-en"
    if any("uk" in p or "united kingdom" in p or "england" in p for p in parts):
        return "uk-en"
    if any("canada" in p for p in parts):
        return "ca-en"
    if any("australia" in p for p in parts):
        return "au-en"
    return None


def supplementary_search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    """Run slow scraped backends (BBB, OpenCorporates, Manta, Wikipedia).

    Meant to be called once for the best query, not per-query.
    """
    batches: list[list[SearchResult]] = []
    for backend in [_from_wikipedia, _from_bbb, _from_opencorporates, _from_manta]:
        try:
            batch = backend(query, max(4, num_results // 2))
            if batch:
                batches.append(batch)
        except Exception:
            pass
        time.sleep(0.3)

    deduped: dict[tuple[str, str], SearchResult] = {}
    for batch in batches:
        for r in batch:
            key = (r.title.lower().strip(), r.url.lower().strip())
            if key not in deduped:
                deduped[key] = r

    return [r.to_dict() for r in list(deduped.values())[: max(1, num_results)]]


def quick_search(query: str, num_results: int = 10, geography: str | None = None) -> list[dict[str, str]]:
    """
    Fast search entrypoint — no API keys required.
    Primary backends: Nominatim + DuckDuckGo. When these return sparse results
    (e.g. DDG rate-limited), falls back to supplementary scrapers
    (Wikipedia, BBB, OpenCorporates, Manta) so we don't collapse to a single backend.

    When *geography* is provided, geo-aware backends constrain results to that area.
    """
    batches: list[list[SearchResult]] = []
    ddgs_region = _geography_to_ddgs_region(geography)

    # Nominatim — best for local/geo queries, now geo-bounded
    nom = _from_nominatim(query, num_results, geography=geography)
    batches.append(nom)

    # DuckDuckGo — general web search, region-scoped
    ddg = _from_ddgs(query, num_results, region=ddgs_region)
    if ddg:
        batches.append(ddg)

    # Fallback: if the fast backends returned too little, try the scraped backends
    # inline for this query. This restores pre-4288fd2 behavior where a single
    # quick_search() call would survive a DDG rate-limit.
    total_fast = sum(len(b) for b in batches)
    if total_fast < max(6, num_results // 2):
        for backend in (_from_wikipedia, _from_bbb, _from_opencorporates, _from_manta):
            try:
                batch = backend(query, max(3, num_results // 2))
                if batch:
                    batches.append(batch)
            except Exception:
                pass

    deduped: dict[tuple[str, str], SearchResult] = {}
    for batch in batches:
        for r in batch:
            key = (r.title.lower().strip(), r.url.lower().strip())
            if key not in deduped:
                deduped[key] = r

    return [r.to_dict() for r in list(deduped.values())[: max(1, num_results)]]

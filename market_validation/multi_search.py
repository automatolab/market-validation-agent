"""
Free search helpers (no API keys required).

Backends (all free/no key):
1. Nominatim (OpenStreetMap)
2. DuckDuckGo via `duckduckgo_search` (DDGS), if available
3. Wikipedia search API
4. BBB public search page scraping
5. OpenCorporates public search page (best-effort, often captcha)
6. Public city directory templates (best-effort)

All backends are best-effort and can return partial results depending on anti-bot policies.
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
        return requests.get(
            url,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
        )
    except Exception:
        return None


def _extract_location_hint(query: str) -> str:
    parts = [p.strip() for p in query.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[-2] + ", " + parts[-1]
    if parts:
        return parts[-1]
    return ""


def _from_nominatim(query: str, num_results: int = 10) -> list[SearchResult]:
    resp = _safe_get(
        NOMINATIM_URL,
        {
            "q": query,
            "format": "jsonv2",
            "addressdetails": 1,
            "extratags": 1,
            "namedetails": 1,
            "limit": max(1, min(num_results, 40)),
        },
        timeout=20,
    )
    if resp is None or resp.status_code != 200:
        return []

    try:
        payload = resp.json()
    except Exception:
        return []

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

        snippet_parts = [display_name]
        if cuisine:
            snippet_parts.append(f"cuisine={cuisine}")
        if phone:
            snippet_parts.append(f"phone={phone}")

        results.append(
            SearchResult(
                title=name,
                url=website,
                snippet=" | ".join(snippet_parts),
                source="nominatim",
            )
        )

    return results[:num_results]


def _from_ddgs(query: str, num_results: int = 10) -> list[SearchResult]:
    DDGS = None
    try:
        from ddgs import DDGS  # type: ignore[no-redef]
    except Exception:
        try:
            from duckduckgo_search import DDGS  # type: ignore[no-redef]
        except Exception:
            return []

    try:
        results: list[SearchResult] = []
        ddgs = DDGS()
        for row in ddgs.text(query, max_results=max(1, min(num_results, 25))):
            results.append(
                SearchResult(
                    title=row.get("title", ""),
                    url=row.get("href", row.get("url", "")),
                    snippet=row.get("body", row.get("snippet", "")),
                    source="ddgs",
                )
            )
        return results[:num_results]
    except Exception:
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
        timeout=25,
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
    resp = _safe_get(OPENCORPORATES_SEARCH_URL, {"q": query}, timeout=25)
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


def search_all_backends(query: str, num_results: int = 10) -> list[dict[str, str]]:
    """
    Run all free backends with polite throttling.
    Deduplicates by (title,url).
    """
    collected: list[SearchResult] = []
    backends = [
        _from_nominatim,
        _from_ddgs,
        _from_wikipedia,
        _from_bbb,
        _from_opencorporates,
        _from_manta,
    ]

    for backend in backends:
        try:
            batch = backend(query, num_results)
            if batch:
                collected.extend(batch)
        except Exception:
            pass
        time.sleep(0.6)

    deduped: dict[tuple[str, str], SearchResult] = {}
    for r in collected:
        key = (r.title.lower().strip(), r.url.lower().strip())
        if key not in deduped:
            deduped[key] = r

    return [r.to_dict() for r in list(deduped.values())[: max(1, num_results)]]


def quick_search(query: str, num_results: int = 10) -> list[dict[str, str]]:
    """
    Main search entrypoint.
    Strategy:
    - try Nominatim first
    - if sparse, try DDGS
    - then aggregate all backends
    """
    batches: list[list[SearchResult]] = []

    nom = _from_nominatim(query, num_results)
    batches.append(nom)

    ddg = _from_ddgs(query, num_results)
    if ddg:
        batches.append(ddg)

    if len(nom) < max(6, num_results):
        batches.append(_from_wikipedia(query, max(4, num_results // 2)))
        batches.append(_from_bbb(query, max(4, num_results // 2)))
        batches.append(_from_opencorporates(query, max(3, num_results // 3)))
        batches.append(_from_manta(query, max(3, num_results // 3)))

    deduped: dict[tuple[str, str], SearchResult] = {}
    for batch in batches:
        for r in batch:
            key = (r.title.lower().strip(), r.url.lower().strip())
            if key not in deduped:
                deduped[key] = r

    out = [r.to_dict() for r in list(deduped.values())[: max(1, num_results)]]
    if out:
        return out
    return search_all_backends(query, num_results=num_results)

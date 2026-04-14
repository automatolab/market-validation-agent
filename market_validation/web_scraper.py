"""
Lightweight web scraper helpers for market research.

No Playwright dependency required. Uses requests + BeautifulSoup.

Three tiers of scraping:
1. quick_scrape(url)          — contact info extraction (original, for company leads)
2. scrape_competitor(url)     — extract pricing, description, features from competitor sites
3. scrape_yelp_search()       — local business density, ratings, price range from Yelp
4. scrape_page_content(url)   — generic deep content extraction (industry report pages etc.)
"""

from __future__ import annotations

import re
import time
import urllib.parse
from typing import Any

import requests
from bs4 import BeautifulSoup


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Domains that are directories/aggregators — skip full scrape for competitor analysis
_SKIP_COMPETITOR_DOMAINS = {
    "yelp.com", "tripadvisor.com", "yellowpages.com", "bbb.org",
    "manta.com", "crunchbase.com", "linkedin.com", "facebook.com",
    "instagram.com", "twitter.com", "reddit.com", "wikipedia.org",
    "bloomberg.com", "reuters.com", "techcrunch.com", "forbes.com",
}


def _get(url: str, timeout: int = 15) -> requests.Response | None:
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout, allow_redirects=True)
        return resp if resp.status_code == 200 else None
    except Exception:
        return None


def _domain(url: str) -> str:
    return url.split("//")[-1].split("/")[0].lower().lstrip("www.")


def _extract_phone(text: str) -> str | None:
    match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
    return match.group(0) if match else None


def _extract_email(text: str) -> str | None:
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    return match.group(0) if match else None


def _extract_prices(text: str) -> list[str]:
    """Pull price mentions from page text."""
    # Match: $9.99, $12/month, $1,200/yr, $50k, free, "starting at $X"
    patterns = [
        r"\$[\d,]+(?:\.\d{1,2})?(?:/mo(?:nth)?|/yr(?:ear)?|/user|/seat|/month)?",
        r"\$[\d,]+[kK](?:\s*/\s*(?:mo|yr|year|month))?",
        r"\bfree(?:\s+tier|\s+plan|\s+forever)?\b",
        r"starting\s+(?:at\s+)?\$[\d,]+",
        r"from\s+\$[\d,]+",
    ]
    found = []
    for pattern in patterns:
        found.extend(re.findall(pattern, text, re.IGNORECASE))
    # Deduplicate while preserving order
    seen: set[str] = set()
    out = []
    for p in found:
        key = p.lower().strip()
        if key not in seen:
            seen.add(key)
            out.append(p.strip())
    return out[:10]


def _extract_meta_description(soup: BeautifulSoup) -> str:
    for tag in soup.find_all("meta"):
        name = tag.get("name", "").lower()
        prop = tag.get("property", "").lower()
        if name in ("description", "og:description") or prop in ("og:description",):
            content = tag.get("content", "").strip()
            if content:
                return content[:300]
    return ""


def _clean_text(soup: BeautifulSoup, max_chars: int = 3000) -> str:
    """Extract readable text, stripping nav/footer/script noise."""
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "aside"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    # Collapse whitespace
    text = re.sub(r"\s{2,}", " ", text)
    return text[:max_chars]


# ---------------------------------------------------------------------------
# 1. Original quick_scrape — contact extraction (unchanged API)
# ---------------------------------------------------------------------------

def quick_scrape(url: str) -> dict[str, Any]:
    """
    Best-effort scrape for basic business contact details.
    Returns a normalized dict, never raises.
    """
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
        if resp.status_code != 200:
            return {"url": url, "error": f"HTTP {resp.status_code}"}

        soup = BeautifulSoup(resp.text, "html.parser")
        title = (soup.title.string or "").strip() if soup.title else ""
        text = soup.get_text(" ", strip=True)

        website = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if href.startswith("http") and "google" not in href and "yelp" not in href:
                website = href
                break

        return {
            "url": url,
            "business_name": title,
            "address": "",
            "phone": _extract_phone(text),
            "email": _extract_email(text),
            "website": website,
            "rating": "",
            "reviews_count": "",
            "raw_text": text[:2000],
        }
    except Exception as exc:
        return {"url": url, "error": str(exc)}


# ---------------------------------------------------------------------------
# 2. scrape_competitor — extract positioning + pricing from competitor sites
# ---------------------------------------------------------------------------

def scrape_competitor(url: str) -> dict[str, Any]:
    """
    Scrape a competitor website for market research signals.

    Returns:
        name          — page title / company name
        description   — meta description or first paragraph
        price_signals — list of price strings found ($X/mo, free tier, etc.)
        features      — key feature phrases extracted from headings/bullets
        raw_snippet   — first 600 chars of clean body text
        error         — set if scraping failed
    """
    domain = _domain(url)
    if any(skip in domain for skip in _SKIP_COMPETITOR_DOMAINS):
        return {"url": url, "skipped": True, "reason": "directory/aggregator domain"}

    resp = _get(url)
    if resp is None:
        return {"url": url, "error": "fetch failed"}

    soup = BeautifulSoup(resp.text, "html.parser")
    title = (soup.title.string or "").strip() if soup.title else domain
    description = _extract_meta_description(soup)
    text = _clean_text(soup, max_chars=5000)
    price_signals = _extract_prices(text)

    # Extract feature hints from H1/H2/H3 headings and <li> bullets
    feature_texts: list[str] = []
    for tag in soup.find_all(["h1", "h2", "h3"]):
        t = tag.get_text(" ", strip=True)
        if 5 < len(t) < 120:
            feature_texts.append(t)
    for li in soup.find_all("li"):
        t = li.get_text(" ", strip=True)
        if 10 < len(t) < 100:
            feature_texts.append(t)

    # Deduplicate, keep top 10
    seen_ft: set[str] = set()
    features: list[str] = []
    for ft in feature_texts:
        key = ft.lower()[:60]
        if key not in seen_ft:
            seen_ft.add(key)
            features.append(ft)
        if len(features) >= 10:
            break

    return {
        "url": url,
        "name": title[:100],
        "description": description or text[:200],
        "price_signals": price_signals,
        "features": features,
        "raw_snippet": text[:600],
    }


def scrape_competitors_batch(
    urls: list[str],
    max_workers: int = 4,
    delay: float = 0.8,
) -> list[dict[str, Any]]:
    """
    Scrape multiple competitor URLs with polite rate limiting.
    Returns list of scrape_competitor() results.
    """
    results = []
    for url in urls:
        if not url or not url.startswith("http"):
            continue
        results.append(scrape_competitor(url))
        time.sleep(delay)
    return results


# ---------------------------------------------------------------------------
# 3. scrape_yelp_search — local business density signal (no API key)
# ---------------------------------------------------------------------------

def scrape_yelp_search(
    market: str,
    location: str,
    limit: int = 40,
) -> dict[str, Any]:
    """
    Scrape Yelp search results for a local market.

    Returns:
        business_count     — number of listings found (proxy for market density)
        avg_rating         — average star rating across listings
        price_distribution — {"$": N, "$$": N, "$$$": N, "$$$$": N}
        sample_names       — first 8 business names (for competitor context)
        categories         — most common subcategories
        snippet            — formatted summary for AI context
    """
    params = urllib.parse.urlencode({
        "find_desc": market,
        "find_loc": location,
        "limit": 40,
    })
    url = f"https://www.yelp.com/search?{params}"
    resp = _get(url, timeout=20)
    if resp is None:
        return {"error": "yelp fetch failed", "url": url}

    soup = BeautifulSoup(resp.text, "html.parser")
    text = resp.text

    # Check for bot block
    if "enable JavaScript" in text or "Access denied" in text or len(text) < 2000:
        return {"error": "yelp blocked (JS required)", "url": url}

    # --- Business count ---
    count = 0
    for tag in soup.find_all(["h1", "h2", "h3", "p", "span"]):
        t = tag.get_text(strip=True)
        m = re.search(r"([\d,]+)\s+(?:local\s+)?(?:results?|businesses)", t, re.I)
        if m:
            count = int(m.group(1).replace(",", ""))
            break

    # --- Ratings ---
    ratings: list[float] = []
    for tag in soup.find_all("div", {"aria-label": re.compile(r"star rating", re.I)}):
        label = tag.get("aria-label", "")
        m = re.search(r"([\d.]+)", label)
        if m:
            try:
                ratings.append(float(m.group(1)))
            except ValueError:
                pass
    # Fallback: look for rating numbers near "star"
    if not ratings:
        for m in re.finditer(r'"rating":\s*([\d.]+)', text):
            try:
                r = float(m.group(1))
                if 1.0 <= r <= 5.0:
                    ratings.append(r)
            except ValueError:
                pass

    avg_rating = round(sum(ratings) / len(ratings), 1) if ratings else None

    # --- Price distribution ---
    price_dist: dict[str, int] = {"$": 0, "$$": 0, "$$$": 0, "$$$$": 0}
    for m in re.finditer(r'(\${1,4})\b', text):
        key = m.group(1)
        if key in price_dist:
            price_dist[key] += 1

    # --- Business names ---
    names: list[str] = []
    for tag in soup.find_all("a", href=re.compile(r"/biz/")):
        t = tag.get_text(strip=True)
        if t and len(t) > 2 and t not in names:
            names.append(t)
        if len(names) >= 8:
            break

    # --- Categories ---
    categories: list[str] = []
    for tag in soup.find_all("a", href=re.compile(r"/search\?.*cflt=")):
        t = tag.get_text(strip=True)
        if t and t not in categories:
            categories.append(t)
        if len(categories) >= 6:
            break

    snippet = (
        f"Yelp search '{market}' in {location}: "
        f"{count or 'unknown number of'} businesses found"
    )
    if avg_rating:
        snippet += f", avg rating {avg_rating}★"
    if any(price_dist.values()):
        dominant = max(price_dist, key=price_dist.get)
        snippet += f", typical price range {dominant}"
    if names:
        snippet += f". Top names: {', '.join(names[:4])}"

    return {
        "business_count": count,
        "avg_rating": avg_rating,
        "price_distribution": price_dist,
        "sample_names": names,
        "categories": categories,
        "snippet": snippet,
        "url": url,
    }


# ---------------------------------------------------------------------------
# 4. scrape_page_content — generic deep content extraction
# ---------------------------------------------------------------------------

def scrape_page_content(url: str, max_chars: int = 2000) -> dict[str, Any]:
    """
    Fetch and extract clean text from any page.
    Useful for getting full content from industry report previews,
    association pages, or news articles beyond the search snippet.

    Returns {url, title, description, content, price_signals}
    """
    resp = _get(url)
    if resp is None:
        return {"url": url, "error": "fetch failed"}

    soup = BeautifulSoup(resp.text, "html.parser")
    title = (soup.title.string or "").strip() if soup.title else ""
    description = _extract_meta_description(soup)
    content = _clean_text(soup, max_chars=max_chars)
    price_signals = _extract_prices(content)

    return {
        "url": url,
        "title": title[:150],
        "description": description,
        "content": content,
        "price_signals": price_signals,
    }


def scrape_search_result_pages(
    search_results: list[dict[str, str]],
    max_pages: int = 5,
    max_chars_each: int = 1500,
    delay: float = 1.0,
) -> list[dict[str, Any]]:
    """
    Fetch full content for top N search result URLs.
    Skips known paywalled/JS-heavy domains.

    Returns list of scrape_page_content() results with non-empty content.
    """
    skip_domains = {
        "statista.com", "ibisworld.com", "grandviewresearch.com",
        "mordorintelligence.com", "marketsandmarkets.com",
        "businessinsider.com", "wsj.com", "ft.com", "bloomberg.com",
        "techcrunch.com", "reuters.com",  # often paywalled
    }
    results: list[dict[str, Any]] = []
    count = 0
    for r in search_results:
        url = r.get("url", "")
        if not url or not url.startswith("http"):
            continue
        domain = _domain(url)
        if any(skip in domain for skip in skip_domains):
            continue
        content = scrape_page_content(url, max_chars=max_chars_each)
        if content.get("content") and len(content["content"]) > 100:
            results.append(content)
            count += 1
        if count >= max_pages:
            break
        time.sleep(delay)
    return results


def is_playwright_available() -> bool:
    """Optional check only; project works without Playwright."""
    try:
        import playwright  # type: ignore
        return True
    except Exception:
        return False

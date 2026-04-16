"""
HTTP scraper for contact info, competitor data, and business directories.

Uses requests + BeautifulSoup (no Playwright required). Provides contact
extraction from company websites, competitor pricing/feature scraping,
Yelp business density lookups, and generic page content extraction.
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


def _extract_all_phones(text: str) -> list[str]:
    """Extract all unique phone numbers from text."""
    matches = re.findall(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
    seen: set[str] = set()
    out: list[str] = []
    for m in matches:
        digits = re.sub(r"\D", "", m)
        if digits not in seen and len(digits) == 10:
            seen.add(digits)
            out.append(m)
    return out[:10]


def _extract_email(text: str) -> str | None:
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    return match.group(0) if match else None


def _extract_all_emails(text: str) -> list[str]:
    """Extract all unique email addresses from text, filtering out common false positives."""
    _JUNK_PATTERNS = {
        "sentry", "webpack", "wixpress", "example.com", "email.com",
        "domain.com", "yoursite", "company.com", "test.com",
    }
    matches = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    seen: set[str] = set()
    out: list[str] = []
    for m in matches:
        lower = m.lower()
        if lower in seen:
            continue
        if any(junk in lower for junk in _JUNK_PATTERNS):
            continue
        # Skip image/file extensions masquerading as emails
        if lower.endswith((".png", ".jpg", ".gif", ".svg", ".js", ".css")):
            continue
        seen.add(lower)
        out.append(m)
    return out[:20]


def _extract_address(text: str) -> str | None:
    """Best-effort extraction of a US street address from text."""
    m = re.search(
        r"\d{1,5}\s+[A-Za-z0-9.\s]{2,40}(?:Street|St|Avenue|Ave|Boulevard|Blvd|Road|Rd|Drive|Dr|Lane|Ln|Way|Court|Ct|Place|Pl|Suite|Ste)[.,\s]+"
        r"[A-Za-z\s]+,\s*[A-Z]{2}\s+\d{5}",
        text, re.IGNORECASE,
    )
    return m.group(0).strip() if m else None


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
# 1a. scrape_contact_info — multi-page contact extraction
# ---------------------------------------------------------------------------

_CONTACT_PATHS = ("/contact", "/contact-us", "/about", "/about-us")
_EXTENDED_CONTACT_PATHS = (
    "/contact", "/contact-us", "/about", "/about-us",
    "/team", "/our-team", "/staff", "/leadership", "/people",
    "/connect", "/reach-us", "/get-in-touch", "/support",
)

# Patterns used to identify contact-related links on the homepage
_CONTACT_LINK_PATTERNS = re.compile(
    r"contact|about|team|staff|reach|connect|email|get.in.touch|support|leadership|people",
    re.IGNORECASE,
)

_MAX_DISCOVERED_LINKS = 5


def _discover_contact_links(soup: BeautifulSoup, base: str) -> list[str]:
    """Parse homepage HTML and return up to 5 internal links that look contact-related."""
    found: list[str] = []
    seen_paths: set[str] = set()
    base_domain = _domain(base)

    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "").strip()
        link_text = a_tag.get_text(" ", strip=True)

        # Resolve relative URLs
        if href.startswith("/"):
            full_url = base.rstrip("/") + href.split("?")[0].split("#")[0]
        elif href.startswith("http"):
            # Only follow internal links
            if _domain(href) != base_domain:
                continue
            full_url = href.split("?")[0].split("#")[0]
        else:
            continue

        path = full_url.replace(base.rstrip("/"), "").lower()
        if not path or path == "/":
            continue
        if path in seen_paths:
            continue

        # Check if link text or href path matches contact-related patterns
        if _CONTACT_LINK_PATTERNS.search(link_text) or _CONTACT_LINK_PATTERNS.search(path):
            seen_paths.add(path)
            found.append(full_url)
            if len(found) >= _MAX_DISCOVERED_LINKS:
                break

    return found


def _scrape_page_contacts(
    page_url: str,
    all_emails: list[str],
    all_phones: list[str],
) -> tuple[BeautifulSoup | None, str | None]:
    """Fetch a single page and extract emails/phones into the provided lists.

    Returns (soup, address_or_None) so the caller can do further processing.
    """
    resp = _get(page_url, timeout=15)
    if resp is None:
        return None, None

    text = resp.text
    # Extract from raw HTML (catches mailto: links etc.)
    all_emails.extend(_extract_all_emails(text))
    all_phones.extend(_extract_all_phones(text))

    soup = BeautifulSoup(text, "html.parser")

    # Extract mailto: and tel: links explicitly
    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "")
        if href.startswith("mailto:"):
            email = href.replace("mailto:", "").split("?")[0].strip()
            if email and "@" in email:
                all_emails.append(email)
        elif href.startswith("tel:"):
            phone = href.replace("tel:", "").strip()
            if phone:
                all_phones.append(re.sub(r"[^\d()+\-.\s]", "", phone))

    # Try to extract address
    clean = _clean_text(soup, max_chars=5000)
    address = _extract_address(clean)
    return soup, address


def scrape_contact_info(url: str, delay: float = 1.0) -> dict[str, Any]:
    """
    Scrape a website's homepage and discovered contact pages for emails,
    phones, contact names, and address.

    Strategy:
    1. Fetch the homepage and extract contacts + discover navigation links.
    2. Follow up to 5 discovered contact-related links from the homepage nav.
    3. If no contact links are discovered, fall back to hardcoded paths.

    Returns::

        {"emails": [...], "phones": [...], "contacts": [...], "address": "..."}

    Never raises — returns partial results on failure.
    """
    all_emails: list[str] = []
    all_phones: list[str] = []
    address: str | None = None
    contacts: list[dict[str, str]] = []
    pages_scraped: list[str] = []

    # Normalize base URL
    if not url.startswith("http"):
        url = "https://" + url
    base = url.rstrip("/")

    # --- Step 1: Fetch and scrape the homepage ---
    homepage_soup, homepage_address = _scrape_page_contacts(base, all_emails, all_phones)
    if homepage_soup is not None:
        pages_scraped.append(base)
        if homepage_address:
            address = homepage_address

    # --- Step 2: Discover contact-related links from the homepage ---
    discovered_links: list[str] = []
    if homepage_soup is not None:
        discovered_links = _discover_contact_links(homepage_soup, base)

    # --- Step 3: Choose which pages to follow ---
    if discovered_links:
        urls_to_try = discovered_links
    else:
        # Fallback: try the extended hardcoded paths
        urls_to_try = [base + path for path in _EXTENDED_CONTACT_PATHS]

    for page_url in urls_to_try:
        # Skip the homepage — already scraped
        if page_url.rstrip("/") == base:
            continue

        time.sleep(delay)
        soup, page_address = _scrape_page_contacts(page_url, all_emails, all_phones)
        if soup is not None:
            pages_scraped.append(page_url)
            if not address and page_address:
                address = page_address

    # Deduplicate
    seen_emails: set[str] = set()
    unique_emails: list[str] = []
    for e in all_emails:
        lower = e.lower()
        if lower not in seen_emails:
            seen_emails.add(lower)
            unique_emails.append(e)

    seen_phones: set[str] = set()
    unique_phones: list[str] = []
    for p in all_phones:
        digits = re.sub(r"\D", "", p)
        if digits and digits not in seen_phones:
            seen_phones.add(digits)
            unique_phones.append(p)

    return {
        "emails": unique_emails[:20],
        "phones": unique_phones[:10],
        "contacts": contacts,
        "address": address or "",
        "pages_scraped": pages_scraped,
    }


# ---------------------------------------------------------------------------
# 1b. Original quick_scrape — contact extraction (unchanged API)
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

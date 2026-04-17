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
        if resp.status_code == 200:
            return resp
        # Cloudflare challenge (403/503) — retry with curl_cffi browser impersonation
        if resp.status_code in (403, 503):
            return _get_cffi(url, timeout)
        return None
    except Exception:
        return None


def _get_cffi(url: str, timeout: int = 15) -> requests.Response | None:
    """Fallback fetch using curl_cffi to bypass Cloudflare browser checks."""
    try:
        from curl_cffi import requests as cf_requests
        resp = cf_requests.get(url, impersonate="chrome", timeout=timeout, allow_redirects=True)
        if resp.status_code == 200:
            # Wrap in a duck-typed object compatible with requests.Response
            wrapper = requests.models.Response()
            wrapper.status_code = resp.status_code
            wrapper._content = resp.content
            wrapper.encoding = resp.encoding or "utf-8"
            return wrapper
        return None
    except Exception:
        return None


def _domain(url: str) -> str:
    return url.split("//")[-1].split("/")[0].lower().lstrip("www.")


def _extract_phone(text: str) -> str | None:
    match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
    return match.group(0) if match else None


def _is_valid_us_phone(digits: str) -> bool:
    """Check if a 10-digit string looks like a real US phone number."""
    if len(digits) != 10:
        return False
    area = digits[:3]
    # US area codes: first digit 2-9, cannot be N11 (e.g. 411, 911)
    if area[0] in "01":
        return False
    if area[1] == area[2] == "1":
        return False
    # Exchange (next 3 digits): first digit 2-9
    if digits[3] in "01":
        return False
    # Reject obviously fake patterns (all same digit, sequential)
    if len(set(digits)) <= 2:
        return False
    return True


def _extract_all_phones(text: str) -> list[str]:
    """Extract all unique, valid US phone numbers from text."""
    matches = re.findall(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
    seen: set[str] = set()
    out: list[str] = []
    for m in matches:
        digits = re.sub(r"\D", "", m)
        if digits not in seen and _is_valid_us_phone(digits):
            seen.add(digits)
            out.append(m)
    return out[:10]


def _extract_email(text: str) -> str | None:
    """Return the first junk-filtered email found in *text*, or None.

    Uses ``_extract_all_emails`` (which strips Sentry / Wix / placeholder
    domains) and returns the first hit, so a raw ``noreply@sentry.io`` from
    embedded JS doesn't leak past this helper.
    """
    hits = _extract_all_emails(text)
    return hits[0] if hits else None


_EMAIL_JUNK_PATTERNS = {
    "sentry", "webpack", "wixpress", "example.com", "email.com",
    "domain.com", "yoursite", "company.com", "test.com",
    "mysite.com", "placeholder", "noreply", "no-reply",
    "sentry.io", "cloudflare", "squarespace.com", "wix.com",
}


def _extract_all_emails(text: str) -> list[str]:
    """Extract all unique email addresses from text, filtering out common false positives.

    Handles plain, HTML-entity-encoded (``&#64;``), and simple obfuscated forms
    (``name [at] domain [dot] com``, ``(at)``, ``{at}``).
    """
    import html as _html
    # Unescape all HTML entities (handles &#64;, &#46;, fully entity-encoded emails, etc.)
    decoded = _html.unescape(text)

    # De-obfuscate common patterns: "name [at] domain [dot] com" → "name@domain.com"
    deobf = re.sub(
        r"([A-Za-z0-9._%+-]+)\s*(?:\[at\]|\(at\)|\{at\}|\s+at\s+)\s*([A-Za-z0-9.-]+)\s*(?:\[dot\]|\(dot\)|\{dot\}|\s+dot\s+)\s*([A-Za-z]{2,})",
        r"\1@\2.\3",
        decoded,
        flags=re.IGNORECASE,
    )

    matches = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", deobf)
    seen: set[str] = set()
    out: list[str] = []
    for m in matches:
        lower = m.lower()
        if lower in seen:
            continue
        if any(junk in lower for junk in _EMAIL_JUNK_PATTERNS):
            continue
        # Skip image/file extensions masquerading as emails
        if lower.endswith((".png", ".jpg", ".gif", ".svg", ".js", ".css")):
            continue
        seen.add(lower)
        out.append(m)
    return out[:20]


def _decode_cfemail(hexstr: str) -> str | None:
    """Decode a Cloudflare-obfuscated email from its data-cfemail hex payload."""
    try:
        data = bytes.fromhex(hexstr)
    except ValueError:
        return None
    if len(data) < 2:
        return None
    key = data[0]
    decoded = "".join(chr(b ^ key) for b in data[1:])
    return decoded if "@" in decoded and "." in decoded else None


def _extract_jsonld_contacts(soup: BeautifulSoup) -> tuple[list[str], list[str]]:
    """Pull telephone/email from schema.org JSON-LD blocks (Restaurant, LocalBusiness, Organization)."""
    import json as _json

    emails: list[str] = []
    phones: list[str] = []

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            tel = node.get("telephone")
            if isinstance(tel, str) and tel.strip():
                phones.append(tel.strip())
            em = node.get("email")
            if isinstance(em, str) and "@" in em:
                emails.append(em.replace("mailto:", "").strip())
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    for tag in soup.find_all("script", type="application/ld+json"):
        raw = tag.string or tag.get_text() or ""
        if not raw.strip():
            continue
        try:
            _walk(_json.loads(raw))
        except Exception:
            # Some sites embed multiple JSON blobs concatenated; try each separately
            for chunk in re.split(r"}\s*{", raw):
                try:
                    _walk(_json.loads("{" + chunk.strip("{}") + "}"))
                except Exception:
                    continue

    return emails, phones


def _extract_cfemails(soup: BeautifulSoup) -> list[str]:
    """Decode all Cloudflare-obfuscated emails on the page."""
    out: list[str] = []
    for tag in soup.find_all(attrs={"data-cfemail": True}):
        dec = _decode_cfemail(tag.get("data-cfemail", ""))
        if dec:
            out.append(dec)
    return out


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


def _visible_text(soup: BeautifulSoup, max_chars: int = 10000) -> str:
    """Extract ALL visible text including footer/header (for contact extraction).

    Unlike _clean_text, this preserves footer/header where contact info lives,
    but still strips script/style/noscript tags.
    """
    clone = BeautifulSoup(str(soup), "html.parser")
    for tag in clone(["script", "style", "noscript"]):
        tag.decompose()
    text = clone.get_text(" ", strip=True)
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
    "/locations", "/location", "/stores", "/store-locator", "/find-us",
    "/careers", "/jobs", "/press", "/media",
)

# Patterns used to identify contact-related links anywhere on the site
_CONTACT_LINK_PATTERNS = re.compile(
    r"contact|about|team|staff|reach|connect|email|get.in.touch|support|leadership|"
    r"people|location|store|find.us|branch|office|press|careers|jobs",
    re.IGNORECASE,
)

_MAX_DISCOVERED_LINKS = 10  # per page
_MAX_TOTAL_PAGES = 20       # cap for the whole crawl
_MAX_PDFS_PER_CRAWL = 3     # fetch up to N PDFs referenced from the site
_MAX_SOCIAL_PAGES = 2       # fetch up to N public FB /about/ pages


def _discover_contact_links(soup: BeautifulSoup, base: str, already_seen: set[str]) -> list[str]:
    """Return internal links on *soup* that look contact-related and aren't already visited."""
    found: list[str] = []
    local_seen: set[str] = set()
    base_domain = _domain(base)

    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "").strip()
        link_text = a_tag.get_text(" ", strip=True)

        if href.startswith("/"):
            full_url = base.rstrip("/") + href.split("?")[0].split("#")[0]
        elif href.startswith("http"):
            if _domain(href) != base_domain:
                continue
            full_url = href.split("?")[0].split("#")[0]
        else:
            continue

        path = full_url.replace(base.rstrip("/"), "").lower()
        if not path or path == "/":
            continue
        if full_url in already_seen or full_url in local_seen:
            continue

        if _CONTACT_LINK_PATTERNS.search(link_text) or _CONTACT_LINK_PATTERNS.search(path):
            local_seen.add(full_url)
            found.append(full_url)
            if len(found) >= _MAX_DISCOVERED_LINKS:
                break

    return found


def _discover_sitemap_urls(base: str, base_domain: str) -> list[str]:
    """Fetch /sitemap.xml (if present) and return contact-related URLs for the same domain."""
    resp = _get(base.rstrip("/") + "/sitemap.xml", timeout=10)
    if resp is None:
        return []
    # Extract <loc>...</loc> URLs
    locs = re.findall(r"<loc>\s*([^<\s]+?)\s*</loc>", resp.text, re.IGNORECASE)
    out: list[str] = []
    for url in locs:
        if _domain(url) != base_domain:
            continue
        path_lower = url.lower()
        if _CONTACT_LINK_PATTERNS.search(path_lower):
            out.append(url.split("?")[0].split("#")[0])
        if len(out) >= _MAX_DISCOVERED_LINKS:
            break
    return out


def _discover_pdf_links(soup: BeautifulSoup, base: str) -> list[str]:
    """Return up to N internal PDF URLs linked from *soup*.

    Restaurants and catering businesses frequently publish menus / catering
    packets / press kits as PDFs that embed a real contact email.
    """
    base_domain = _domain(base)
    found: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or ".pdf" not in href.lower():
            continue
        if href.startswith("/"):
            full = base.rstrip("/") + href.split("?")[0].split("#")[0]
        elif href.startswith("http"):
            if _domain(href) != base_domain:
                continue
            full = href.split("?")[0].split("#")[0]
        else:
            continue
        if full in seen:
            continue
        seen.add(full)
        found.append(full)
        if len(found) >= _MAX_PDFS_PER_CRAWL:
            break
    return found


def _extract_pdf_text(url: str) -> str:
    """Download a PDF and return its extracted text, or '' on any failure."""
    try:
        from pypdf import PdfReader  # type: ignore
    except ImportError:
        return ""
    import io
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20, allow_redirects=True)
        if resp.status_code != 200 or not resp.content:
            return ""
        # Defensive: don't try to parse very large PDFs
        if len(resp.content) > 10 * 1024 * 1024:
            return ""
        reader = PdfReader(io.BytesIO(resp.content))
        # Cap at first 20 pages — restaurants' catering PDFs are tiny,
        # huge PDFs waste time.
        pages = reader.pages[:20]
        return "\n".join((p.extract_text() or "") for p in pages)
    except Exception:
        return ""


def _discover_social_about_links(soup: BeautifulSoup) -> list[str]:
    """Return public 'about' URLs for FB/Instagram pages linked from *soup*.

    Small businesses often publish their email on their Facebook page's
    ``/about/`` tab even when it's missing from their own website.
    """
    out: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href.startswith("http"):
            continue
        h = href.lower()
        about_url: str | None = None
        # Facebook page — append /about/
        m = re.match(r"https?://(?:www\.|m\.)?facebook\.com/([A-Za-z0-9.\-_]+)/?", h)
        if m and m.group(1) not in {"sharer", "dialog", "plugins", "tr", "events"}:
            page = m.group(1)
            about_url = f"https://www.facebook.com/{page}/about/"
        if about_url and about_url not in seen:
            seen.add(about_url)
            out.append(about_url)
        if len(out) >= _MAX_SOCIAL_PAGES:
            break
    return out


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
    soup = BeautifulSoup(text, "html.parser")

    # Use full visible text (including footer/header) for contact extraction —
    # restaurants commonly put phone/email/address in the footer only
    visible = _visible_text(soup, max_chars=10000)

    # Extract emails from raw HTML (catches mailto: href values + HTML-entity + [at] forms)
    all_emails.extend(_extract_all_emails(text))

    # Extract phones from visible text (including footer) — NOT raw HTML
    # which contains JS/CSS digit sequences that produce false positives
    all_phones.extend(_extract_all_phones(visible))

    # JSON-LD structured data (schema.org/Restaurant, LocalBusiness, Organization)
    # — restaurants and service businesses commonly publish real phone/email here
    jl_emails, jl_phones = _extract_jsonld_contacts(soup)
    all_emails.extend(jl_emails)
    all_phones.extend(jl_phones)

    # Cloudflare-obfuscated emails (data-cfemail="<hex>")
    all_emails.extend(_extract_cfemails(soup))

    # Extract mailto: and tel: links explicitly (most reliable source)
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
    address = _extract_address(visible)
    return soup, address


def scrape_contact_info(url: str, delay: float = 1.0) -> dict[str, Any]:
    """
    Scrape a site for emails, phones, and address with 2-level BFS crawl.

    Strategy:
    1. Fetch the homepage, extract contacts, and discover contact-related links.
    2. Shortcut: pull contact-related URLs from /sitemap.xml when present.
    3. Crawl level-1 pages (contact, about, locations, team, …) and harvest contacts.
       From each level-1 page, discover more links (team bios, per-location pages).
    4. Crawl level-2 pages (up to _MAX_TOTAL_PAGES total).
    5. If the homepage had no discoverable contact links at all, fall back to the
       hardcoded path list so we still try the obvious spots.

    Returns::

        {"emails": [...], "phones": [...], "contacts": [...],
         "address": "...", "pages_scraped": [...]}

    Never raises — returns partial results on failure.
    """
    all_emails: list[str] = []
    all_phones: list[str] = []
    address: str | None = None
    contacts: list[dict[str, str]] = []
    pages_scraped: list[str] = []

    if not url.startswith("http"):
        url = "https://" + url
    base = url.rstrip("/")
    base_domain = _domain(base)
    visited: set[str] = set()

    def _visit(page_url: str) -> BeautifulSoup | None:
        """Fetch + harvest contacts from one page. Updates closures in-place."""
        nonlocal address
        if page_url in visited or len(pages_scraped) >= _MAX_TOTAL_PAGES:
            return None
        visited.add(page_url)
        soup, page_address = _scrape_page_contacts(page_url, all_emails, all_phones)
        if soup is not None:
            pages_scraped.append(page_url)
            if not address and page_address:
                address = page_address
        return soup

    # --- Level 0: homepage ---
    homepage_soup = _visit(base)

    # --- Level-1 candidates: from nav + sitemap ---
    level1: list[str] = []
    if homepage_soup is not None:
        level1.extend(_discover_contact_links(homepage_soup, base, visited))
    sitemap_links = _discover_sitemap_urls(base, base_domain)
    for s in sitemap_links:
        if s not in level1:
            level1.append(s)

    # Fallback when nothing was discovered at all: use hardcoded paths
    if not level1:
        level1 = [base + path for path in _EXTENDED_CONTACT_PATHS]

    level2_candidates: list[str] = []
    pdf_candidates: list[str] = []
    social_candidates: list[str] = []
    if homepage_soup is not None:
        pdf_candidates.extend(_discover_pdf_links(homepage_soup, base))
        social_candidates.extend(_discover_social_about_links(homepage_soup))

    for page_url in level1:
        if len(pages_scraped) >= _MAX_TOTAL_PAGES:
            break
        if page_url.rstrip("/") == base:
            continue
        time.sleep(delay)
        soup = _visit(page_url)
        # Discover deeper contact-ish links, PDFs, and social-about links
        # from this page (team member bios, per-location pages, menu PDFs).
        if soup is not None:
            for deeper in _discover_contact_links(soup, base, visited):
                if deeper not in level2_candidates:
                    level2_candidates.append(deeper)
            for pdf in _discover_pdf_links(soup, base):
                if pdf not in pdf_candidates:
                    pdf_candidates.append(pdf)
            for social in _discover_social_about_links(soup):
                if social not in social_candidates:
                    social_candidates.append(social)

    # --- Level-2 crawl ---
    for page_url in level2_candidates:
        if len(pages_scraped) >= _MAX_TOTAL_PAGES:
            break
        time.sleep(delay)
        _visit(page_url)

    # --- PDFs: menu/catering PDFs often embed a real contact email ---
    for pdf_url in pdf_candidates[:_MAX_PDFS_PER_CRAWL]:
        if len(pages_scraped) >= _MAX_TOTAL_PAGES:
            break
        text = _extract_pdf_text(pdf_url)
        if text:
            all_emails.extend(_extract_all_emails(text))
            all_phones.extend(_extract_all_phones(text))
            pages_scraped.append(pdf_url)
            if not address:
                maybe_addr = _extract_address(text)
                if maybe_addr:
                    address = maybe_addr
        time.sleep(delay)

    # --- Public social-media about pages (FB mostly) ---
    for social_url in social_candidates[:_MAX_SOCIAL_PAGES]:
        if len(pages_scraped) >= _MAX_TOTAL_PAGES:
            break
        time.sleep(delay)
        _visit(social_url)

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

        # Only surface an outgoing link as "website" if it's the same host as
        # the scraped page — arbitrary outbound links (a competitor, a review
        # site, a payment processor) are not this company's website.
        website = ""
        self_host = _domain(url)
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if href.startswith("http") and _domain(href) == self_host:
                website = href
                break
        if not website:
            website = url  # fall back to the page we scraped

        candidate_email = _extract_email(text)
        try:
            from market_validation.company_enrichment import is_plausible_email
            if candidate_email and not is_plausible_email(candidate_email):
                candidate_email = None
        except Exception:
            pass

        return {
            "url": url,
            "business_name": title,
            "address": "",
            "phone": _extract_phone(text),
            "email": candidate_email,
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

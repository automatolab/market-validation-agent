"""
Lightweight free web scraper helpers.

No Playwright dependency required. Uses requests + BeautifulSoup.
If Playwright is installed, we expose availability but do not require it.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


USER_AGENT = "market-validation-agent/0.1"


def is_playwright_available() -> bool:
    """Optional check only; project works without Playwright."""
    try:
        import playwright  # type: ignore
        return True
    except Exception:
        return False


def _extract_phone(text: str) -> str | None:
    match = re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text)
    return match.group(0) if match else None


def _extract_email(text: str) -> str | None:
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    return match.group(0) if match else None


def quick_scrape(url: str) -> dict[str, Any]:
    """
    Best-effort scrape for a page and basic business details.
    Returns a normalized dict, never raises.
    """
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=20,
        )
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

"""Contact-field extraction from raw search snippets."""

from __future__ import annotations

import re
from typing import Any


def normalize_name_key(value: str) -> str:
    tokens = [t for t in re.split(r"\W+", str(value or "").lower()) if len(t) >= 2]
    return " ".join(tokens)


def extract_phone_text(value: str) -> str:
    # Match international (+1-408-...) and domestic formats
    match = re.search(r"(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", str(value or ""))
    return match.group(0).strip() if match else ""


def extract_email_text(value: str) -> str:
    match = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", str(value or ""))
    return match.group(0) if match else ""


def extract_contact_from_search_result(r: dict[str, str]) -> dict[str, Any]:
    """Build a company dict from a raw search result, extracting phone/email/location
    from snippet text.  Works for all backends (Nominatim, DDGS, BBB, etc.)."""
    snippet = r.get("snippet", "")
    phone = ""
    email = ""
    location = ""

    # Nominatim encodes structured fields in the snippet as "display | phone=... | email=..."
    if r.get("source") == "nominatim" and snippet:
        for part in [p.strip() for p in snippet.split("|")]:
            if part.startswith("phone="):
                phone = part[len("phone="):].strip()
            elif part.startswith("email="):
                email = part[len("email="):].strip()
            elif not part.startswith("cuisine=") and not location:
                location = part

    # Fallback: regex extraction from raw snippet (works for all backends)
    if not phone:
        phone = extract_phone_text(snippet)
    if not email:
        email = extract_email_text(snippet)

    return {
        "company_name": r.get("title", ""),
        "website": r.get("url", ""),
        "location": location,
        "phone": phone,
        "email": email,
        "description": snippet,
        "evidence_url": r.get("url", ""),
        "source": r.get("source", "search"),
    }

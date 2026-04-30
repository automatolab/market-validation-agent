"""Contact-field extraction from raw search snippets."""

from __future__ import annotations

import re
import unicodedata
from typing import Any


def normalize_name_key(value: str) -> str:
    """Lowercase, NFKD-fold accents (Latin only), and strip to alnum tokens.

    Folds 'Café' and 'Cafe', 'Müller' and 'Muller' to the same key so European
    market datasets dedupe correctly. CJK / Cyrillic / Arabic strings skip
    the NFKD step so they don't lose meaningful characters.
    """
    raw = str(value or "")
    if _is_mostly_latin_text(raw):
        folded = unicodedata.normalize("NFKD", raw)
        folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    else:
        folded = raw
    tokens = [t for t in re.split(r"\W+", folded.lower()) if len(t) >= 2]
    return " ".join(tokens)


def _is_mostly_latin_text(text: str) -> bool:
    """Internal: >=70% Latin letters → safe to NFKD-fold."""
    if not text:
        return False
    letters = [ch for ch in text if ch.isalpha()]
    if not letters:
        return False
    latin = sum(1 for ch in letters if "LATIN" in unicodedata.name(ch, ""))
    return (latin / len(letters)) >= 0.7


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


# ── Phone normalization (E.164 with international fallback) ─────────────────

# Country code → expected national digit length(s) (most common formats only).
# Used as a regex-based fallback when the optional `phonenumbers` library
# isn't installed.
_COUNTRY_DIGIT_LENGTHS: dict[str, tuple[int, ...]] = {
    "1":  (10,),        # US/CA
    "44": (10, 11),     # UK
    "33": (9,),         # France
    "49": (10, 11, 12), # Germany
    "61": (9,),         # Australia
    "81": (10, 11),     # Japan
    "86": (11,),        # China
    "91": (10,),        # India
    "55": (10, 11),     # Brazil
    "34": (9,),         # Spain
    "39": (9, 10, 11),  # Italy
    "31": (9,),         # Netherlands
    "46": (8, 9, 10),   # Sweden
    "47": (8,),         # Norway
    "45": (8,),         # Denmark
    "32": (8, 9),       # Belgium
    "41": (9,),         # Switzerland
    "43": (10, 11),     # Austria
}

_HINT_TO_CC: dict[str, str] = {
    "US": "1", "CA": "1", "GB": "44", "UK": "44", "FR": "33",
    "DE": "49", "AU": "61", "JP": "81", "IN": "91", "BR": "55",
    "ES": "34", "IT": "39", "NL": "31", "SE": "46", "NO": "47",
    "DK": "45", "BE": "32", "CH": "41", "AT": "43", "CN": "86",
}


def normalize_phone(raw: str, country_hint: str | None = None) -> str:
    """Normalize a phone number to E.164 (``+CC...``) when possible.

    Tries the optional ``phonenumbers`` library first for best accuracy, and
    falls back to a regex-based parser using common country-code lengths.
    Returns the original digits when no clean E.164 form can be derived.
    ``country_hint`` is an ISO-2 country code ("US", "GB", "FR") used to
    disambiguate domestic formats. Defaults to US.
    """
    if not raw:
        return ""
    s = str(raw).strip()

    # Best path — phonenumbers if installed.
    try:
        import phonenumbers  # type: ignore
        region = (country_hint or "US").upper()
        try:
            parsed = phonenumbers.parse(s, region)
            if phonenumbers.is_valid_number(parsed):
                return phonenumbers.format_number(
                    parsed, phonenumbers.PhoneNumberFormat.E164
                )
        except phonenumbers.NumberParseException:
            pass
    except ImportError:
        pass

    # Regex fallback.
    digits = re.sub(r"\D", "", s)
    if not digits:
        return ""
    starts_plus = s.lstrip().startswith("+")

    # +CC<rest> case: split country code off the front.
    if starts_plus or len(digits) > 10:
        for cc_len in (3, 2, 1):
            if len(digits) > cc_len:
                cc = digits[:cc_len]
                rest = digits[cc_len:]
                if cc in _COUNTRY_DIGIT_LENGTHS and len(rest) in _COUNTRY_DIGIT_LENGTHS[cc]:
                    return f"+{cc}{rest}"

    # Domestic form — apply country_hint default.
    cc = _HINT_TO_CC.get((country_hint or "US").upper(), "1")
    expected = _COUNTRY_DIGIT_LENGTHS.get(cc, (10,))
    if len(digits) in expected:
        return f"+{cc}{digits}"

    return f"+{digits}" if len(digits) >= 7 else digits


def is_valid_phone_intl(raw: str, country_hint: str | None = None) -> bool:
    """Check whether *raw* is a valid phone number for the given country hint.

    Uses ``phonenumbers`` if available; otherwise validates against the
    expected digit lengths in ``_COUNTRY_DIGIT_LENGTHS``. Replaces the old
    US-only ``_is_valid_us_phone`` for international markets.
    """
    if not raw:
        return False
    try:
        import phonenumbers  # type: ignore
        region = (country_hint or "US").upper()
        try:
            parsed = phonenumbers.parse(str(raw), region)
            return bool(phonenumbers.is_valid_number(parsed))
        except phonenumbers.NumberParseException:
            return False
    except ImportError:
        pass

    digits = re.sub(r"\D", "", str(raw))
    if not digits:
        return False
    cc = _HINT_TO_CC.get((country_hint or "US").upper(), "1")
    expected = _COUNTRY_DIGIT_LENGTHS.get(cc, (10,))
    # Accept either a domestic-length number or a +CC + national number
    if len(digits) in expected:
        return _basic_us_sanity(digits) if cc == "1" else True
    if len(digits) > min(expected) + len(cc):
        rest = digits[len(cc):] if digits.startswith(cc) else digits
        if len(rest) in expected:
            return _basic_us_sanity(rest) if cc == "1" else True
    return False


def _basic_us_sanity(digits: str) -> bool:
    """Quick US sanity check: NANP rules — first digit of area code 2-9, etc."""
    if len(digits) != 10:
        return False
    if digits[0] in "01":
        return False
    if digits[1] == digits[2] == "1":
        return False
    if digits[3] in "01":
        return False
    if len(set(digits)) <= 2:
        return False
    return True


# ── Geography → country hint ────────────────────────────────────────────────

# Common country/region keywords mapped to ISO-2 codes.
_COUNTRY_NAME_TO_ISO2: dict[str, str] = {
    "united states": "US", "usa": "US", "us": "US", "america": "US",
    "united kingdom": "GB", "uk": "GB", "britain": "GB", "england": "GB",
    "scotland": "GB", "wales": "GB",
    "canada": "CA",
    "france": "FR",
    "germany": "DE", "deutschland": "DE",
    "australia": "AU",
    "japan": "JP",
    "china": "CN",
    "india": "IN",
    "brazil": "BR", "brasil": "BR",
    "spain": "ES", "españa": "ES",
    "italy": "IT", "italia": "IT",
    "netherlands": "NL", "holland": "NL",
    "sweden": "SE",
    "norway": "NO",
    "denmark": "DK",
    "belgium": "BE",
    "switzerland": "CH",
    "austria": "AT",
    "ireland": "IE",
    "mexico": "MX", "méxico": "MX",
    "singapore": "SG",
    "new zealand": "NZ",
}

# US state abbreviations and full names → US.
_US_STATES = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id",
    "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms",
    "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
    "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv",
    "wi", "wy", "dc",
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey",
    "new mexico", "new york", "north carolina", "north dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
    "washington", "west virginia", "wisconsin", "wyoming",
}

# Canadian province codes / names → CA.
_CA_PROVINCES = {
    "on", "qc", "bc", "ab", "mb", "sk", "ns", "nb", "nl", "pe", "yt", "nt", "nu",
    "ontario", "quebec", "british columbia", "alberta", "manitoba", "saskatchewan",
    "nova scotia", "new brunswick", "newfoundland", "prince edward island",
    "yukon", "northwest territories", "nunavut",
}


def detect_country(geography: str | None) -> str:
    """Best-effort ISO-2 country detection from a freeform geography string.

    Returns "US" for unrecognised inputs (the historical default).
    """
    if not geography:
        return "US"
    g = geography.strip().lower()
    parts = [p.strip() for p in re.split(r"[,/]+", g) if p.strip()]
    last = parts[-1] if parts else g

    # Direct full-name match
    for name, iso in _COUNTRY_NAME_TO_ISO2.items():
        if name in g:
            return iso

    # Last token signals
    if last in _US_STATES or any(p in _US_STATES for p in parts):
        return "US"
    if last in _CA_PROVINCES or any(p in _CA_PROVINCES for p in parts):
        return "CA"

    return "US"

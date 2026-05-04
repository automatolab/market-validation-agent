"""
Contact enrichment utilities: email pattern generation and MX verification.

Generates candidate email addresses from company domains using common
patterns (info@, contact@, firstname.lastname@), then verifies deliverability
via MX and A record lookups using dnspython or socket fallback.
"""

from __future__ import annotations

import json
import re
import socket
from datetime import UTC
from pathlib import Path
from typing import Any

_COMMON_EMAIL_PREFIXES = (
    "info", "contact", "sales", "hello", "support", "admin",
    "office", "team", "general", "inquiries",
)

# Domains we must NEVER pattern-guess against — they're directories, online-ordering
# platforms, aggregators, or SaaS hosts. Generating `info@<these>` would send mail
# to the wrong company (or a marketing inbox the business doesn't control).
_AGGREGATOR_DOMAINS = frozenset({
    # Review / directory aggregators
    "yelp.com", "tripadvisor.com", "yellowpages.com", "superpages.com",
    "bbb.org", "manta.com", "opentable.com", "thumbtack.com", "angi.com",
    "angieslist.com", "zomato.com", "foursquare.com", "mapquest.com",
    "bizapedia.com", "localeze.com", "cylex.us.com", "expertise.com",
    "homeadvisor.com", "porch.com", "houzz.com", "yellowbook.com",
    "merchantcircle.com", "city-data.com", "buzzfile.com",
    # Ordering / reservation / table-management
    "netwaiter.com", "toasttab.com", "toast.com", "resy.com", "seated.com",
    "doordash.com", "grubhub.com", "ubereats.com", "chownow.com",
    "clover.com", "squareup.com", "touchbistro.com", "menufy.com",
    "eatstreet.com", "slicelife.com", "orderonline.com",
    # Event / booking platforms
    "eventective.com", "theknot.com", "weddingwire.com", "eventbrite.com",
    "sagemenu.com", "foodtruckavenue.com", "res-menu.net", "wholesaleseeker.com",
    "einnews.com", "sumferkitchens.com",
    # SaaS / B2B directories
    "g2.com", "capterra.com", "producthunt.com", "softwareadvice.com",
    "trustradius.com", "getapp.com", "saashub.com", "sourceforge.net",
    "alternativeto.net", "crozdesk.com",
    # Crunchbase / data / news aggregators
    "crunchbase.com", "dnb.com", "zoominfo.com", "rocketreach.co",
    "leadiq.com", "apollo.io", "lusha.com", "hunter.io", "snov.io",
    "owler.com", "pitchbook.com", "growjo.com",
    # Social / content hosts
    "facebook.com", "instagram.com", "twitter.com", "x.com", "linkedin.com",
    "tiktok.com", "youtube.com", "pinterest.com", "reddit.com", "quora.com",
    "medium.com", "substack.com",
    # Generic website builders / hosts (sometimes surface as "website")
    "wixsite.com", "squarespace.com", "weebly.com", "godaddysites.com",
    "myshopify.com", "blogspot.com", "wordpress.com",
    "carrd.co", "webflow.io", "site123.me",
    # Search / map / news properties
    "google.com", "maps.google.com", "bing.com", "apple.com",
    "wikipedia.org", "wikimedia.org",
    # General news / content publishers — emails on these are journalists
    # writing ABOUT a company, not the company itself.
    "nytimes.com", "wsj.com", "ft.com", "bloomberg.com", "reuters.com",
    "apnews.com", "ap.org", "npr.org", "bbc.com", "bbc.co.uk", "cnn.com",
    "theguardian.com", "washingtonpost.com", "latimes.com",
    "sfchronicle.com", "sfgate.com", "mercurynews.com",
    "nbcnews.com", "abcnews.go.com", "cbsnews.com", "foxnews.com",
    "businessinsider.com", "fortune.com", "forbes.com", "inc.com",
    "fastcompany.com", "axios.com", "voanews.com", "voa.gov",
    "techcrunch.com", "theinformation.com", "theverge.com", "wired.com",
    "vice.com", "buzzfeed.com", "huffpost.com",
    # Trade/agritech press — same rationale, came up in CA hydroponics run
    "globalaginvesting.com", "agriinvestor.com", "agfundernews.com",
    "modernfarmer.com", "agritecture.com", "urbanvine.co",
    "growertalks.com", "hortidaily.com", "freshplaza.com",
    "produceblue book.com", "thepacker.com", "agdaily.com",
    "optimistdaily.com", "smartcitiesdive.com", "fooddive.com",
    "supermarketnews.com", "winsightgrocerybusiness.com",
    "bizjournals.com", "patch.com",
    # Press release wire services
    "prnewswire.com", "businesswire.com", "globenewswire.com",
    "marketwire.com", "einpresswire.com", "accesswire.com",
    "newswire.com", "openpr.com",
    # Logistics / shipping (came up as cross-domain pickup — XPO Logistics
    # email surfaced for a hydroponic farm because of a freight-quote widget)
    "xpo.com", "fedex.com", "ups.com", "usps.com", "dhl.com",
    "shipstation.com", "easypost.com",
    # Stock photo / template hosts (emails on demo pages)
    "shutterstock.com", "istockphoto.com", "gettyimages.com",
    "unsplash.com", "pixabay.com", "pexels.com",
    # Misc cross-pollination domains we've seen in enrichment
    "thegrowcer.ca",  # Canadian container farm — kept appearing for unrelated US growers
})


def _is_aggregator_domain(domain: str | None) -> bool:
    """True if *domain* belongs to (or is a subdomain of) a known aggregator/directory."""
    if not domain:
        return False
    d = domain.lower().removeprefix("www.")
    if d in _AGGREGATOR_DOMAINS:
        return True
    # e.g. "samsbbqdiner.netwaiter.com" → matches "netwaiter.com"
    return any(d.endswith("." + agg) for agg in _AGGREGATOR_DOMAINS)


# Commonly-seen real public TLDs. 2-letter ccTLDs are also accepted universally.
_VALID_TLD_WHITELIST = frozenset({
    "com", "net", "org", "io", "co", "biz", "info", "me", "app", "ai",
    "store", "shop", "menu", "restaurant", "kitchen", "farm", "bar", "pub",
    "pizza", "cafe", "food", "life", "live", "club", "site", "online",
    "email", "link", "inc", "llc", "us",
})


def is_plausible_email(email: str | None) -> bool:
    """Return True if *email* looks like a real, writable email address.

    Rejects:
      - strings with embedded commentary ("x@y.com (inferred)")
      - unknown long TLDs (`.loc`, `.corp`)
      - aggregator / news / PR domains (`@yelp.com`, `@sfchronicle.com`)
      - placeholder local-parts (`example@`, `first@`, `yourname@`)
    Allows normal 2-letter ccTLDs.
    """
    if not email:
        return False
    e = email.strip()
    # No whitespace and no parenthetical comments inside the address
    if re.search(r"\s|\(|\)|<|>|,", e):
        return False
    m = re.fullmatch(r"([A-Za-z0-9._%+-]+)@([A-Za-z0-9.-]+)\.([A-Za-z]{2,})", e)
    if not m:
        return False
    local = m.group(1).lower()
    domain = (m.group(2) + "." + m.group(3)).lower()
    tld = m.group(3).lower()
    # TLD check — accept all 2-letter ccTLDs, else must be a known public TLD
    if len(tld) > 2 and tld not in _VALID_TLD_WHITELIST:
        return False
    if _is_aggregator_domain(domain):
        return False
    # Placeholder local-part check (example@gmail.com, first@plenty.ag, etc.)
    try:
        from market_validation.web_scraper import (
            _JUNK_LOCAL_SUBSTRINGS,
            _PLACEHOLDER_LOCAL_PARTS,
        )
        normalized = local.replace(".", "").replace("-", "").replace("_", "")
        if normalized in _PLACEHOLDER_LOCAL_PARTS:
            return False
        # Substring junk check — catches spam-trap addresses like
        # "medicare.fraud@..." that pass the format check but are clearly
        # honeypot/abuse mailboxes scraped off directory pages.
        if any(token in normalized for token in _JUNK_LOCAL_SUBSTRINGS):
            return False
    except ImportError:
        pass
    return True

# ---------------------------------------------------------------------------
# Email MX verification
# ---------------------------------------------------------------------------

_mx_cache: dict[str, dict] = {}


def _domain_from_email(email: str) -> str | None:
    """Extract domain part from an email address."""
    if "@" not in email:
        return None
    return email.rsplit("@", 1)[1].strip().lower()


def _check_mx(domain: str) -> dict:
    """
    Check if *domain* has MX or A records.  Results are cached per domain.
    Returns ``{"has_mx": bool, "mx_host": str|None, "method": str}``.

    Uses dnspython if available, otherwise falls back to socket.getaddrinfo.
    Timeout: 5 seconds.  Never raises.
    """
    if domain in _mx_cache:
        return _mx_cache[domain]

    result: dict = {"has_mx": False, "mx_host": None, "method": "none"}

    # --- Try dnspython first ---
    try:
        import dns.resolver  # type: ignore[import-untyped]

        resolver = dns.resolver.Resolver()
        resolver.lifetime = 5.0
        resolver.timeout = 5.0

        try:
            mx_records = resolver.resolve(domain, "MX")
            if mx_records:
                best = min(mx_records, key=lambda r: r.preference)
                result = {
                    "has_mx": True,
                    "mx_host": str(best.exchange).rstrip("."),
                    "method": "mx_check",
                }
                _mx_cache[domain] = result
                return result
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers):
            pass
        except Exception:
            pass

        # Fallback: check A record via dnspython
        try:
            a_records = resolver.resolve(domain, "A")
            if a_records:
                result = {
                    "has_mx": True,
                    "mx_host": str(a_records[0].address),
                    "method": "a_record_fallback",
                }
                _mx_cache[domain] = result
                return result
        except Exception:
            pass

        # No MX and no A → invalid
        result = {"has_mx": False, "mx_host": None, "method": "mx_check"}
        _mx_cache[domain] = result
        return result

    except ImportError:
        pass

    # --- Fallback: socket.getaddrinfo (no dnspython) ---
    old_timeout = socket.getdefaulttimeout()
    try:
        socket.setdefaulttimeout(5.0)
        addrs = socket.getaddrinfo(domain, 25, socket.AF_UNSPEC, socket.SOCK_STREAM)
        if addrs:
            result = {
                "has_mx": True,
                "mx_host": addrs[0][4][0],
                "method": "socket_fallback",
            }
        else:
            result = {"has_mx": False, "mx_host": None, "method": "socket_fallback"}
    except (TimeoutError, socket.gaierror, OSError):
        # Could not resolve at all → try plain A record
        try:
            socket.setdefaulttimeout(5.0)
            ip = socket.gethostbyname(domain)
            result = {
                "has_mx": True,
                "mx_host": ip,
                "method": "a_record_socket_fallback",
            }
        except Exception:
            result = {"has_mx": False, "mx_host": None, "method": "socket_fallback"}
    except Exception:
        result = {"has_mx": False, "mx_host": None, "method": "socket_fallback"}
    finally:
        socket.setdefaulttimeout(old_timeout)

    _mx_cache[domain] = result
    return result


def verify_email(email: str) -> dict:
    """
    Verify an email address via DNS MX / A record lookup on its domain.

    Returns ``{"email": ..., "valid": True/False, "method": "mx_check", "mx_host": ...}``.
    Never raises.
    """
    email = email.strip()
    domain = _domain_from_email(email)
    if not domain or "." not in domain:
        return {"email": email, "valid": False, "method": "mx_check", "mx_host": None}

    mx = _check_mx(domain)
    return {
        "email": email,
        "valid": mx["has_mx"],
        "method": mx["method"],
        "mx_host": mx["mx_host"],
    }


def verify_emails_batch(emails: list[str]) -> list[dict]:
    """Verify a list of emails.  Returns one result dict per input email."""
    return [verify_email(e) for e in emails]


def generate_email_patterns(domain: str) -> list[dict[str, Any]]:
    """
    Given a domain (e.g. ``acmebbq.com``), return common email patterns.

    Each entry includes ``"email"``, ``"pattern_generated": True``,
    and a ``"valid"`` field from MX verification. Returns [] for aggregator
    / directory domains — pattern-guessing against yelp.com, netwaiter.com,
    etc. would produce emails for the wrong company.
    """
    domain = domain.lower().strip().removeprefix("www.")
    if not domain or "." not in domain:
        return []
    if _is_aggregator_domain(domain):
        return []

    # Check domain MX once (cached), then apply result to all patterns
    mx = _check_mx(domain)
    domain_valid = mx["has_mx"]

    return [
        {
            "email": f"{prefix}@{domain}",
            "pattern_generated": True,
            "valid": domain_valid,
            "mx_host": mx["mx_host"],
        }
        for prefix in _COMMON_EMAIL_PREFIXES
    ]


def domain_from_url(url: str | None) -> str | None:
    """Extract bare domain from a URL, stripping scheme and www prefix."""
    if not url:
        return None
    host = url.split("//")[-1].split("/")[0].lower().removeprefix("www.")
    return host if "." in host else None


def _iso_now() -> str:
    from datetime import datetime
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _run_ai_prompt(prompt: str, timeout: int = 120, cwd: str = ".") -> str:
    """
    Run an AI prompt via the best available agent CLI.
    Tries: claude (Claude Code) → opencode → raises RuntimeError.
    Returns raw stdout text.
    """
    import shutil
    import subprocess as _sp

    if shutil.which("claude"):
        # Allow WebSearch + WebFetch so enrichment / qualification subprocesses
        # can actually look companies up. Without this they silently degrade
        # ("WebFetch and WebSearch tool permissions were not granted in this
        # session") and fall back to whatever's already in the prompt context,
        # which is why locations were empty for ~85% of rows in the first run.
        result = _sp.run(
            [
                "claude", "-p", prompt,
                "--output-format", "text",
                "--allowedTools", "WebSearch,WebFetch",
            ],
            capture_output=True, text=True, timeout=timeout, cwd=cwd,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()

    if shutil.which("opencode"):
        result = _sp.run(
            ["opencode", "run", "--dangerously-skip-permissions", "--dir", cwd, prompt],
            capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode == 0:
            return result.stdout.strip()

    raise RuntimeError("No AI agent available (install claude or opencode)")


def enrich_company_contact(
    company_name: str,
    website: str | None,
    location: str | None,
) -> dict[str, Any]:
    website_hint = f"Start by visiting their website at {website}." if website else f'Start by searching for "{company_name}" official website.'
    location_hint = f" They are located in {location}." if location else ""

    prompt = f"""Find contact information for "{company_name}".{location_hint}

{website_hint}

Find:
1. Contact email addresses (purchasing, sales, info@, owner@)
2. Phone numbers
3. Contact form URLs
4. Social media links (LinkedIn, Facebook)
5. Key personnel: owner, founder, decision maker, purchasing manager

Search queries to try:
- "{company_name} contact email"
- "{company_name} owner"
- "{company_name} purchasing manager"
- "{company_name} LinkedIn"

Return JSON:
{{
  "company_name": "{company_name}",
  "website": "url if found",
  "emails_found": ["email1@example.com"],
  "phones_found": ["555-123-4567"],
  "contacts": [
    {{"name": "John Smith", "title": "Owner", "source": "website"}},
    {{"name": "Jane Doe", "title": "Purchasing Manager", "source": "linkedin"}}
  ],
  "social_links": {{"linkedin": "...", "facebook": "..."}},
  "notes": "How this info was found"
}}

Only include fields where information was actually found. Return empty arrays/objects if nothing found."""

    try:
        output = _run_ai_prompt(prompt, timeout=120)

        json_start = output.find("{")
        if json_start < 0:
            return {
                "result": "failed",
                "error": "No JSON in output",
                "company_name": company_name,
            }

        json_text = output[json_start:]
        json_end = json_text.rfind("}")
        if json_end > 0:
            json_text = json_text[:json_end + 1]

        data = json.loads(json_text)
        return {
            "result": "ok",
            "company_name": company_name,
            **data,
        }

    except Exception as e:
        return {
            "result": "failed",
            "error": str(e),
            "company_name": company_name,
        }


def enrich_research_companies(
    research_id: str,
    root: str | Path = ".",
    db_path: str | None = None,
) -> dict[str, Any]:
    from market_validation.research import _connect, _ensure_schema, resolve_db_path, update_company

    root_path = Path(root).resolve()
    db_file = resolve_db_path(root_path, db_path)

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = None
        companies = conn.execute(
            """SELECT id, company_name, website, phone, location, email
               FROM companies
               WHERE research_id = ? AND status IN ('qualified', 'new')
               ORDER BY priority_score DESC NULLS LAST""",
            (research_id,)
        ).fetchall()

    if not companies:
        return {"result": "ok", "research_id": research_id, "enriched": 0, "message": "No companies to enrich"}

    enriched_count = 0
    email_found_count = 0
    errors = []

    for company in companies:
        company_id = company[0]
        company_name = company[1]
        website = company[2]
        company[3]
        location = company[4]
        current_email = company[5]

        if not website and not location and not company_name:
            continue

        result = enrich_company_contact(company_name, website, location)

        if result.get("result") == "ok":
            emails = result.get("emails_found", [])
            result.get("phones_found", [])

            if current_email is None and emails:
                update_company(
                    company_id=company_id,
                    research_id=research_id,
                    fields={"email": emails[0]},
                    root=root_path,
                    db_path=db_path,
                )
                email_found_count += 1

            enriched_count += 1

            if not emails:
                errors.append(f"{company_name}: No contact info found")

    return {
        "result": "ok",
        "research_id": research_id,
        "total_companies": len(companies),
        "enriched": enriched_count,
        "emails_found": email_found_count,
        "errors": errors[:5],
    }


def build_parser() -> Any:
    import argparse
    parser = argparse.ArgumentParser(description="Enrich company data with contacts and emails")
    parser.add_argument("--root", default=".", help="Repository root path")
    parser.add_argument("--db-path", default=None, help="SQLite DB path")

    subparsers = parser.add_subparsers(dest="command", required=True)

    enrich_parser = subparsers.add_parser("enrich", help="Enrich companies with contact info")
    enrich_parser.add_argument("research_id", help="Research ID")
    enrich_parser.add_argument("--limit", type=int, default=50, help="Max companies to enrich")

    single_parser = subparsers.add_parser("single", help="Enrich single company")
    single_parser.add_argument("--company-name", required=True, help="Company name")
    single_parser.add_argument("--website", help="Company website")
    single_parser.add_argument("--location", help="Company location")

    return parser


def main() -> None:
    import json

    parser = build_parser()
    args = parser.parse_args()

    if args.command == "single":
        result = enrich_company_contact(
            company_name=args.company_name,
            website=args.website,
            location=args.location,
        )
        print(json.dumps(result, ensure_ascii=True, indent=2))

    elif args.command == "enrich":
        result = enrich_research_companies(
            research_id=args.research_id,
            root=args.root,
            db_path=args.db_path,
        )
        print(json.dumps(result, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()

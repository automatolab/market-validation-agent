"""
Interactive web dashboard and local HTTP server (default port 8788).

Generates an HTML dashboard with project selector, validation scorecards,
company table (inline edit, delete, paginate, CSV export), email queue
management, and Gmail sync. Can also produce a static HTML file.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import UTC
from pathlib import Path
from typing import Any

from market_validation.environment import load_project_env
from market_validation.log import get_logger

_log = get_logger("dashboard")

load_project_env()


def _iso_now() -> str:
    from datetime import datetime

    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


_STATE_ABBR: dict[str, str] = {
    "california": "ca", "texas": "tx", "new york": "ny", "florida": "fl",
    "illinois": "il", "pennsylvania": "pa", "ohio": "oh", "georgia": "ga",
    "michigan": "mi", "washington": "wa", "arizona": "az", "colorado": "co",
    "massachusetts": "ma", "virginia": "va", "oregon": "or", "north carolina": "nc",
    "new jersey": "nj", "minnesota": "mn", "nevada": "nv", "utah": "ut",
}


# Display order for category groups in the company table. Earlier = higher
# priority bucket. Anything not matched falls through to "Other / Uncategorized".
CATEGORY_ORDER: list[str] = [
    "Commercial Grower / Farm",
    "Institutional / Research Greenhouse",
    "Hydroponics Retailer / Store",
    "Wholesale Nursery / Plant Supplier",
    "CEA Equipment / Tech Vendor",
    "Trade Association / Chamber",
    "Academic Program",
    "Directory / Listing",
    "Marketplace",
    "Consumer / Hobby",
    "News / Article / Blog",
    "Reference / Dictionary",
    "Other / Uncategorized",
]


def _categorize_company(company: dict[str, Any]) -> str:
    """Best-effort bucketing of a company into a display category.

    Pure heuristic over name/website/notes — only used by the dashboard's
    grouped view, not by any pipeline logic. Falls back to
    "Other / Uncategorized" when nothing matches.

    Order matters: detect article/blog URLs before vendor-domain rules so a
    blog post on a vendor's site is bucketed as an article, not as the vendor.
    """
    name = (company.get("company_name") or "").lower()
    site = (company.get("website") or "").lower()
    notes = (company.get("notes") or "").lower()
    blob = f"{name} {site} {notes}"

    # URL path segments (everything after the domain)
    after_proto = site.split("//", 1)[-1]
    domain_split = after_proto.split("/", 1)
    path_segments = [p for p in (domain_split[1] if len(domain_split) > 1 else "").split("/") if p]
    last_slug = path_segments[-1] if path_segments else ""

    if "allacronyms" in site or "wikipedia.org/wiki/" in site:
        return "Reference / Dictionary"

    if any(s in site for s in ("alibaba.", "amazon.com/", "ebay.com/")):
        return "Marketplace"

    if any(s in site for s in ("kickstarter.com", "indiegogo.com", "aerogarden.com")):
        return "Consumer / Hobby"

    if any(s in site for s in ("bizapedia.", "manta.com", "yellowpages.")):
        return "Directory / Listing"
    if "/member-directory/" in site or "facilities-map" in site or ".github.io/" in site:
        return "Directory / Listing"

    article_url_signals = (
        "/blog", "/article", "/news/", "/post/", "/blogs/", "blog.", "news.",
        "/2024", "/2025", "/tips/", "/guides/", "/learn/", "/seasonal-",
        "/industry-analysis/",
    )
    if any(s in site for s in article_url_signals):
        return "News / Article / Blog"
    if any(any(k in seg for k in ("news", "blog", "article")) for seg in path_segments):
        return "News / Article / Blog"
    article_title_signals = (
        "top 10", "top 25", "5 benefits", "this is when", "5 environmental",
        "leading vertical", "reshaping", "applying ai", "grow smarter",
        "future of controlled", "annual crop report", "gardeners flock",
        "vertical farming news", "market size", "forecasts",
    )
    if any(s in name for s in article_title_signals):
        return "News / Article / Blog"
    if last_slug.count("-") >= 8:
        return "News / Article / Blog"

    if any(s in blob for s in ("alliance", "chamber of commerce", "farm bureau", "trade association")):
        return "Trade Association / Chamber"

    if any(s in blob for s in (
        "graduate program", "graduate group", "degrees and programs",
        "horticulture technology", "community college",
    )):
        return "Academic Program"
    if "horticulture and agronomy" in name and "research" not in name:
        return "Academic Program"
    if "horticulture centre" in name or ("training" in blob and "campus" in blob):
        return "Academic Program"

    if "greenhouse" in name and any(s in blob for s in (
        "research", "uc davis", "ucdavis", "university", ".edu", "campus",
    )):
        return "Institutional / Research Greenhouse"
    if "research greenhouse" in blob or "plant growth facilit" in name:
        return "Institutional / Research Greenhouse"

    if any(s in name for s in (
        "hydroponics store", "hydroponic store", "growgeneration", "growbigogh",
        "grow supplies", "garden supply",
    )) and "wholesale" not in name:
        return "Hydroponics Retailer / Store"
    if "the hydroponic connection" in name:
        return "Hydroponics Retailer / Store"

    if any(s in site for s in ("secretled.com", "vitabeam.com", "waybeyond.io", "seresag.com")):
        return "CEA Equipment / Tech Vendor"
    if any(s in blob for s in (
        "led grow", "cea software", "automation system vendor", "cea specialist",
    )):
        return "CEA Equipment / Tech Vendor"

    if any(s in name for s in (
        "wholesale nursery", "wholesale plants", "tropical plants",
        "live plants supplier", "wholesale of plants", "plant nursery",
        "youngplants", "young plants",
    )):
        return "Wholesale Nursery / Plant Supplier"
    if "wholesale" in blob and "plant" in blob and "hydroponic" not in name:
        return "Wholesale Nursery / Plant Supplier"

    if any(s in blob for s in (
        "market farms", "commercial hydroponic grower",
        "leafy green", "year-round production", "1,500 acres", "1500 acres",
        "co-packer",
    )):
        return "Commercial Grower / Farm"

    return "Other / Uncategorized"


def _summarize_source_health(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Roll up search-pipeline source_health events into a tidy summary.

    Returns counts per stage, plus a list of failed/empty stages for
    operator visibility. Used by the dashboard to flag degraded runs
    (e.g. DDG cool-down, Nominatim 503) instead of silently underdelivering.
    """
    if not events:
        return {}
    by_status: dict[str, int] = {}
    failed_stages: list[str] = []
    backend_totals: dict[str, int] = {}
    for ev in events:
        status = str(ev.get("status") or "unknown")
        by_status[status] = by_status.get(status, 0) + 1
        if status in ("error", "fail", "empty") and ev.get("stage"):
            stage = str(ev["stage"])
            if stage not in failed_stages:
                failed_stages.append(stage)
        for backend, n in (ev.get("backends") or {}).items():
            backend_totals[backend] = backend_totals.get(backend, 0) + int(n or 0)
    return {
        "events_total": len(events),
        "by_status": by_status,
        "failed_or_empty_stages": failed_stages[:8],
        "backend_totals": backend_totals,
    }


def _geo_key(geo: str) -> str:
    """Normalize a geography string for fuzzy matching (e.g. 'San Jose, CA' ≈ 'San Jose California')."""
    g = geo.lower().strip()
    for name, abbr in _STATE_ABBR.items():
        g = g.replace(name, abbr)
    g = re.sub(r"[^\w\s]", " ", g)
    words = sorted(set(w for w in g.split() if len(w) > 1))
    return " ".join(words)


def _load_data() -> dict[str, Any]:
    from market_validation.email_sender import EMAIL_QUEUE_DIR
    from market_validation.research import _connect, _ensure_schema, resolve_db_path

    db_file = resolve_db_path(Path("."))
    _log.info("loading data from %s", db_file)
    researches: list[dict[str, Any]] = []
    companies: list[dict[str, Any]] = []

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = None

        research_rows = conn.execute(
            """
            SELECT id, name, market, product, geography, status, created_at,
                   last_source_health
            FROM researches
            ORDER BY created_at DESC
            """
        ).fetchall()

        for row in research_rows:
            stats = conn.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'qualified' THEN 1 ELSE 0 END) as qualified,
                    SUM(CASE WHEN status = 'contacted' THEN 1 ELSE 0 END) as contacted,
                    SUM(CASE WHEN email IS NOT NULL AND TRIM(email) <> '' THEN 1 ELSE 0 END) as with_email,
                    SUM(CASE WHEN phone IS NOT NULL AND TRIM(phone) <> '' THEN 1 ELSE 0 END) as with_phone
                FROM companies
                WHERE research_id = ?
                """,
                (row[0],),
            ).fetchone()

            # Parse source_health JSON blob into a per-stage summary so the
            # dashboard can show "DDG rate-limited", "Nominatim failed", etc.
            sh_summary: dict[str, Any] = {}
            sh_raw = row[7] if len(row) > 7 else None
            if sh_raw:
                try:
                    sh_data = json.loads(sh_raw) if isinstance(sh_raw, str) else sh_raw
                    if isinstance(sh_data, list):
                        sh_summary = _summarize_source_health(sh_data)
                except (ValueError, TypeError) as exc:
                    _log.debug("source_health parse failed for %s: %s", row[0], exc)

            researches.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "market": row[2],
                    "product": row[3],
                    "geography": row[4],
                    "status": row[5],
                    "created_at": row[6],
                    "total": stats[0] or 0,
                    "qualified": stats[1] or 0,
                    "contacted": stats[2] or 0,
                    "with_email": stats[3] or 0,
                    "with_phone": stats[4] or 0,
                    "source_health": sh_summary,
                }
            )

        company_rows = conn.execute(
            """
            SELECT
                c.id,
                c.research_id,
                c.company_name,
                c.website,
                c.location,
                c.phone,
                c.email,
                c.status,
                c.priority_score,
                c.priority_tier,
                c.volume_estimate,
                c.volume_unit,
                c.notes,
                c.created_at,
                r.name as research_name
            FROM companies c
            JOIN researches r ON r.id = c.research_id
            ORDER BY c.priority_score DESC NULLS LAST, c.company_name
            """
        ).fetchall()

        for row in company_rows:
            company = {
                "id": row[0],
                "research_id": row[1],
                "company_name": row[2],
                "website": row[3],
                "location": row[4],
                "phone": row[5],
                "email": row[6],
                "status": row[7],
                "priority_score": row[8],
                "priority_tier": row[9],
                "volume_estimate": row[10],
                "volume_unit": row[11],
                "notes": row[12],
                "created_at": row[13],
                "research_name": row[14],
            }
            company["category"] = _categorize_company(company)
            companies.append(company)

        # Load validation data for each research
        validations: dict[str, dict[str, Any]] = {}
        try:
            val_rows = conn.execute(
                """SELECT id, research_id, market, geography, status,
                          tam_low, tam_high, tam_currency, tam_confidence,
                          sam_low, sam_high, sam_confidence,
                          som_low, som_high, som_confidence,
                          demand_score, demand_trend, demand_pain_points, demand_seasonality,
                          competitive_intensity, competitor_count, market_concentration,
                          direct_competitors, indirect_competitors, funding_signals,
                          job_posting_volume, news_sentiment,
                          regulatory_risks, technology_maturity,
                          market_attractiveness, competitive_score,
                          demand_validation, risk_score,
                          overall_score, verdict, verdict_reasoning,
                          created_at,
                          archetype, archetype_label,
                          unit_economics_score, gross_margin_low, gross_margin_high,
                          cac_estimate_low, cac_estimate_high,
                          ltv_estimate_low, ltv_estimate_high, payback_months,
                          structural_attractiveness, timing_score, timing_verdict,
                          timing_enablers, timing_headwinds,
                          supplier_power, buyer_power, substitute_threat,
                          entry_barrier_score, rivalry_score,
                          icp_clarity, primary_segment,
                          differentiation_opportunities,
                          next_steps, key_risks, key_success_factors, archetype_red_flags
                   FROM market_validations
                   ORDER BY created_at DESC"""
            ).fetchall()
            col_names = [
                "id", "research_id", "market", "geography", "status",
                "tam_low", "tam_high", "tam_currency", "tam_confidence",
                "sam_low", "sam_high", "sam_confidence",
                "som_low", "som_high", "som_confidence",
                "demand_score", "demand_trend", "demand_pain_points", "demand_seasonality",
                "competitive_intensity", "competitor_count", "market_concentration",
                "direct_competitors", "indirect_competitors", "funding_signals",
                "job_posting_volume", "news_sentiment",
                "regulatory_risks", "technology_maturity",
                "market_attractiveness", "competitive_score",
                "demand_validation", "risk_score",
                "overall_score", "verdict", "verdict_reasoning",
                "created_at",
                "archetype", "archetype_label",
                "unit_economics_score", "gross_margin_low", "gross_margin_high",
                "cac_estimate_low", "cac_estimate_high",
                "ltv_estimate_low", "ltv_estimate_high", "payback_months",
                "structural_attractiveness", "timing_score", "timing_verdict",
                "timing_enablers", "timing_headwinds",
                "supplier_power", "buyer_power", "substitute_threat",
                "entry_barrier_score", "rivalry_score",
                "icp_clarity", "primary_segment",
                "differentiation_opportunities",
                "next_steps", "key_risks", "key_success_factors", "archetype_red_flags",
            ]
            for vrow in val_rows:
                vdict = dict(zip(col_names, vrow, strict=False))
                rid = vdict["research_id"]
                if rid not in validations:
                    validations[rid] = vdict
        except Exception as _val_err:
            _log.warning("failed to load market_validations: %s", _val_err)

    _log.info(
        "loaded: %d researches, %d companies, %d validations",
        len(researches), len(companies), len(validations),
    )

    # Build a geography-keyed fallback so validation-only runs can cross-link
    # to researches that have companies (and vice-versa).
    # e.g. "San Jose, CA" and "San Jose, California" normalise to the same key.
    val_by_geo: dict[str, dict[str, Any]] = {}
    for vdict in validations.values():
        gk = _geo_key(vdict.get("geography") or "")
        if gk and gk not in val_by_geo:
            val_by_geo[gk] = vdict

    # Attach validation to researches (exact match first, geo fallback second)
    for r in researches:
        own_val = validations.get(r["id"])
        if own_val:
            r["validation"] = own_val
        else:
            gk = _geo_key(r.get("geography") or "")
            cross = val_by_geo.get(gk)
            if cross:
                r["validation"] = dict(cross)
                r["validation"]["_cross_linked"] = True  # UI hint
                print(
                    f"[dashboard] cross-linked validation from research "
                    f"{cross['research_id']} → {r['id']} (geo={r.get('geography')})",
                    file=sys.stderr,
                )
            else:
                r["validation"] = None

    emails: list[dict[str, Any]] = []
    EMAIL_QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    for file in sorted(EMAIL_QUEUE_DIR.glob("*.json")):
        emails.append(json.loads(file.read_text()))

    return {
        "researches": researches,
        "companies": companies,
        "emails": emails,
        "validations": validations,
        "category_order": CATEGORY_ORDER,
    }



# ── Jinja2 environment (templates/dashboard.html + static/ dir) ──────────────
# We used to build the whole HTML page as a 1,500-line f-string and substitute
# tokens via str.replace(). That put HTML, CSS, and JavaScript inside a Python
# file — no syntax highlighting, no linting, and every dynamic value had to
# remember to pass through _escape_html() or risk an XSS. The Jinja2 rewrite
# flips the default: every {{ value }} is escaped; opting out takes | safe.

from jinja2 import Environment, PackageLoader, select_autoescape  # noqa: E402 — logical section boundary

_jinja_env = Environment(
    loader=PackageLoader("market_validation", "templates"),
    autoescape=select_autoescape(enabled_extensions=("html", "htm")),
)


def _render_dashboard(interactive: bool) -> str:
    """Render templates/dashboard.html with the current dashboard data."""
    data = _load_data()
    researches = data["researches"]
    companies = data["companies"]
    emails = data["emails"]

    # Counts shown in the header summary strip.
    pending_count = sum(1 for e in emails if e.get("status") == "pending")
    sent_count = sum(1 for e in emails if e.get("status") in ("sent", "opened", "replied", "bounced"))
    replied_count = sum(1 for e in emails if e.get("replied_at"))
    qualified_count = sum(1 for c in companies if c.get("status") == "qualified")
    phone_count = sum(1 for c in companies if c.get("phone"))
    email_count = sum(1 for c in companies if c.get("email"))

    # The payload is injected into a <script type="application/json"> tag and
    # parsed client-side. We neutralize "</" to avoid terminating the script
    # early if any string value happens to contain it — same trick the
    # original implementation used.
    payload_json = json.dumps(data, ensure_ascii=True).replace("</", "<\\/")

    template = _jinja_env.get_template("dashboard.html")
    return template.render(
        interactive=interactive,
        mode="server" if interactive else "prompt",
        generated_at=_iso_now(),
        researches=researches,
        research_count=len(researches),
        company_count=len(companies),
        qualified_count=qualified_count,
        phone_count=phone_count,
        email_count=email_count,
        pending_count=pending_count,
        sent_count=sent_count,
        replied_count=replied_count,
        payload_json=payload_json,
    )


def generate_html(
    output_path: str | Path = "output/dashboard.html",
    open_browser: bool = True,
    interactive: bool = False,
) -> str:
    """Render the dashboard to disk and optionally open it in a browser."""
    html = _render_dashboard(interactive=interactive)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)

    if open_browser:
        import platform

        if platform.system() == "Darwin":
            subprocess.run(["open", str(out)])
        elif platform.system() == "Windows":
            subprocess.run(["start", str(out)], shell=True)
        else:
            subprocess.run(["xdg-open", str(out)])

    return str(out)


def _make_handler(host: str, port: int):
    from http.server import BaseHTTPRequestHandler
    from urllib.parse import urlparse

    from market_validation.email_sender import (
        approve_all_emails,
        approve_email,
        delete_email,
        draft_email_for_company,
        draft_emails_for_research,
        prep_email,
        reject_all_emails,
        update_queued_email,
    )
    from market_validation.email_tracker import TRANSPARENT_GIF, record_open
    from market_validation.gmail_tracker import sync_all as gmail_sync_all
    from market_validation.research import add_company, delete_company, update_company

    # API key for write endpoints. When set, every POST must include a
    # matching X-API-Key header. Set MV_DASHBOARD_API_KEY in .env to enable.
    # When unset, the dashboard refuses to bind to non-loopback hosts (so we
    # don't accidentally expose unprotected mutation endpoints to the LAN).
    import hmac as _hmac
    import os as _os
    import threading as _threading
    import time as _time
    _expected_api_key = _os.environ.get("MV_DASHBOARD_API_KEY")
    if (host != "127.0.0.1" and host != "localhost") and not _expected_api_key:
        raise RuntimeError(
            f"Refusing to bind dashboard to {host} without auth. "
            "Set MV_DASHBOARD_API_KEY in .env to enable network access, "
            "or bind to 127.0.0.1 (default)."
        )

    # Per-IP auth-failure rate limit. After 10 failures in 5 min from one
    # IP, block that IP for 15 min so an attacker can't brute-force the key.
    _AUTH_FAIL_WINDOW_S = 300
    _AUTH_FAIL_THRESHOLD = 10
    _AUTH_BLOCK_DURATION_S = 900
    _auth_failures: dict[str, list[float]] = {}
    _auth_blocks: dict[str, float] = {}
    _auth_lock = _threading.Lock()

    def _record_auth_failure(client_ip: str) -> None:
        now = _time.time()
        with _auth_lock:
            history = _auth_failures.setdefault(client_ip, [])
            # Drop entries older than the window
            _auth_failures[client_ip] = [t for t in history if now - t < _AUTH_FAIL_WINDOW_S]
            _auth_failures[client_ip].append(now)
            if len(_auth_failures[client_ip]) >= _AUTH_FAIL_THRESHOLD:
                _auth_blocks[client_ip] = now + _AUTH_BLOCK_DURATION_S
                _log.warning(
                    "auth: blocking %s for %ds (%d failures in %ds)",
                    client_ip, _AUTH_BLOCK_DURATION_S,
                    len(_auth_failures[client_ip]), _AUTH_FAIL_WINDOW_S,
                )

    def _is_blocked(client_ip: str) -> bool:
        with _auth_lock:
            blocked_until = _auth_blocks.get(client_ip)
            if blocked_until and _time.time() < blocked_until:
                return True
            if blocked_until:
                _auth_blocks.pop(client_ip, None)
        return False

    class Handler(BaseHTTPRequestHandler):
        def _json(self, payload: dict[str, Any], status: int = 200) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _check_auth(self) -> bool:
            """Validate X-API-Key header when one is required.

            Returns True when the request is authorized (either no key
            configured AND request is from loopback, or a matching key was
            provided). On invalid key, records the failure and rate-limits
            the client IP after too many bad attempts.
            """
            client_ip = self.client_address[0] if self.client_address else ""
            if _is_blocked(client_ip):
                self._json(
                    {"result": "error", "error": "too many failed auth attempts; try later"},
                    status=429,
                )
                return False
            if not _expected_api_key:
                # No key configured — only allow localhost.
                if client_ip in ("127.0.0.1", "::1", "localhost"):
                    return True
                self._json(
                    {"result": "error", "error": "API key required for non-localhost access"},
                    status=401,
                )
                return False
            provided = self.headers.get("X-API-Key", "")
            # Constant-time comparison to defeat timing oracles on the key.
            if not _hmac.compare_digest(provided, _expected_api_key):
                _record_auth_failure(client_ip)
                self._json({"result": "error", "error": "invalid X-API-Key"}, status=401)
                return False
            return True

        def do_GET(self):
            from urllib.parse import urlparse as _up

            parsed = _up(self.path)
            path = parsed.path

            # Static files (CSS/JS) — served from market_validation/static/
            if path.startswith("/static/"):
                rel = path[len("/static/"):]
                # Reject path traversal
                if ".." in rel or rel.startswith("/"):
                    return self._json({"result": "error", "error": "bad path"}, 400)
                static_file = Path(__file__).parent / "static" / rel
                if not static_file.is_file():
                    return self._json({"result": "error", "error": "not found"}, 404)
                ctype = "text/css" if rel.endswith(".css") else (
                    "application/javascript" if rel.endswith(".js") else "application/octet-stream"
                )
                body = static_file.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", f"{ctype}; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(body)
                return

            # Open-tracking pixel
            if path.startswith("/api/email/track/open/"):
                email_id = path.split("/api/email/track/open/", 1)[1].strip("/")
                # Pass client IP so the tracker can demote Gmail-proxy opens
                # (image pre-fetch != real recipient open).
                client_ip = self.client_address[0] if self.client_address else None
                record_open(email_id, client_ip=client_ip)
                self.send_response(200)
                self.send_header("Content-Type", "image/gif")
                self.send_header("Content-Length", str(len(TRANSPARENT_GIF)))
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
                self.end_headers()
                self.wfile.write(TRANSPARENT_GIF)
                return


            if path == "/":
                html_path = Path("output/dashboard.html")
                generate_html(output_path=html_path, open_browser=False, interactive=True)
                data = html_path.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.end_headers()
                self.wfile.write(data)
                return

            if path == "/api/refresh":
                generate_html(open_browser=False, interactive=True)
                return self._json({"result": "ok"})

            if path == "/api/data":
                # Lightweight data endpoint — returns the same payload that's
                # embedded in the HTML, but without re-rendering the page.
                # Used by client-side refresh to avoid full page reloads.
                return self._json({"result": "ok", "data": _load_data()})

            if path.startswith("/api/validation/"):
                research_id = path.split("/api/validation/", 1)[1].strip("/")
                from market_validation.research import get_validation_by_research
                result = get_validation_by_research(research_id)
                return self._json(result)

            if path == "/api/calibration":
                from market_validation.research import get_calibration_summary
                return self._json(get_calibration_summary())

            return self._json({"result": "error", "error": "not found"}, 404)

        def do_POST(self):
            # All POST endpoints mutate state — require auth.
            if not self._check_auth():
                return
            # Lightweight CSRF defense: require X-Requested-With header.
            # Browsers add this header for fetch/XHR requests but NOT for
            # cross-origin form posts, which is what CSRF attacks rely on.
            # Cheaper than full token-double-submit and adequate for a
            # same-origin internal tool.
            if self.headers.get("X-Requested-With") != "MarketValidationDashboard":
                self._json(
                    {"result": "error", "error": "missing X-Requested-With header (CSRF)"},
                    status=403,
                )
                return
            path = urlparse(self.path).path
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8") if length else ""
            data = json.loads(raw) if raw else {}

            try:
                if path == "/api/company/add":
                    result = add_company(
                        research_id=data["research_id"],
                        company_name=data["company_name"],
                        market=data.get("market") or "general",
                        website=data.get("website"),
                        location=data.get("location"),
                        phone=data.get("phone"),
                        email=data.get("email"),
                        notes=data.get("notes"),
                    )
                    return self._json(result)

                if path == "/api/company/update":
                    result = update_company(
                        company_id=data["company_id"],
                        research_id=data["research_id"],
                        fields=data.get("fields") or {},
                    )
                    return self._json(result)

                if path == "/api/company/delete":
                    result = delete_company(
                        company_id=data["company_id"],
                        research_id=data["research_id"],
                    )
                    return self._json(result)

                if path == "/api/email/approve":
                    return self._json(approve_email(data["email_id"]))

                if path == "/api/email/update":
                    result = update_queued_email(
                        email_id=data["email_id"],
                        subject=data.get("subject"),
                        body=data.get("body"),
                    )
                    return self._json(result)

                if path == "/api/email/delete":
                    return self._json(delete_email(data["email_id"]))

                if path == "/api/email/sync":
                    return self._json(gmail_sync_all())

                if path == "/api/email/draft":
                    return self._json(draft_email_for_company(data["company_id"]))

                if path == "/api/email/queue":
                    result = prep_email(
                        to_email=data["to_email"],
                        subject=data["subject"],
                        body=data["body"],
                        company_name=data.get("company_name"),
                        contact_name=data.get("contact_name"),
                        research_id=data.get("research_id"),
                        company_id=data.get("company_id"),
                    )
                    return self._json(result)

                if path == "/api/email/draft-all":
                    statuses = data.get("statuses") or ["qualified"]
                    return self._json(
                        draft_emails_for_research(
                            research_id=data["research_id"],
                            statuses=statuses,
                            skip_existing=bool(data.get("skip_existing", True)),
                        )
                    )

                if path == "/api/email/approve-all":
                    return self._json(approve_all_emails())

                if path == "/api/email/reject-all":
                    return self._json(reject_all_emails())

                if path == "/api/validation/outcome":
                    from market_validation.research import record_validation_outcome
                    return self._json(record_validation_outcome(
                        validation_id=data["validation_id"],
                        actual_outcome=data["outcome"],
                        notes=data.get("notes"),
                        revenue_actual=data.get("revenue"),
                        recorded_by=data.get("recorded_by"),
                    ))

            except Exception as exc:
                return self._json({"result": "error", "error": str(exc)}, 400)

            return self._json({"result": "error", "error": "not found"}, 404)

        def log_message(self, format, *args):
            msg = format % args
            _log.info("%s %s", self.address_string(), msg)

    return Handler


def serve_dashboard(host: str = "127.0.0.1", port: int = 8788, open_browser: bool = True) -> str:
    from http.server import ThreadingHTTPServer

    handler = _make_handler(host, port)
    httpd = ThreadingHTTPServer((host, port), handler)
    url = f"http://{host}:{port}"

    if open_browser:
        import platform

        if platform.system() == "Darwin":
            subprocess.run(["open", url])
        elif platform.system() == "Windows":
            subprocess.run(["start", url], shell=True)
        else:
            subprocess.run(["xdg-open", url])

    print(f"Dashboard server running at {url}")
    httpd.serve_forever()
    return url


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Serve dashboard by default; use --static for file output")
    parser.add_argument("--static", action="store_true", help="Generate static dashboard.html instead of running server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8788)
    parser.add_argument("--no-open", action="store_true")
    args = parser.parse_args()

    if args.static:
        path = generate_html(open_browser=not args.no_open, interactive=False)
        print(f"Dashboard generated: {path}")
        return

    serve_dashboard(host=args.host, port=args.port, open_browser=not args.no_open)


if __name__ == "__main__":
    main()

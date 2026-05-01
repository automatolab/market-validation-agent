"""Per-research folder export — bundles every output of a single research run
into one self-contained directory under output/research/<slug>__<short_id>/.

This is on top of the shared SQLite DB (the single source of truth). The
folder is a flat snapshot of one research's data so the user can browse
it without firing up the dashboard.
"""

from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from market_validation.research import _connect, _ensure_schema, resolve_db_path


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _slugify(text: str, max_len: int = 60) -> str:
    """Lowercase, ASCII-fold, hyphen-separate, max length."""
    text = (text or "").lower()
    text = text.replace("—", "-").replace("–", "-").replace("&", "and")
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text[:max_len].rstrip("-") or "research"


def _research_dir(base: Path, research: dict[str, Any]) -> Path:
    short_id = (research.get("id") or "")[:8]
    slug = _slugify(research.get("name") or research.get("market") or "research")
    return base / f"{slug}__{short_id}"


def export_research_folder(
    research_id: str,
    base_dir: Path | str = "output/research",
) -> Path:
    """Materialize a single research's data into output/research/<slug>__<id>/.

    Idempotent: re-running overwrites the folder's contents with current state.
    Returns the folder path.
    """
    from market_validation.dashboard import _categorize_company

    base = Path(base_dir)
    db_file = resolve_db_path(Path("."))

    with _connect(db_file) as conn:
        _ensure_schema(conn)
        conn.row_factory = sqlite3.Row

        research_row = conn.execute(
            "SELECT * FROM researches WHERE id = ?", (research_id,)
        ).fetchone()
        if not research_row:
            raise ValueError(f"research not found: {research_id}")
        research = dict(research_row)

        companies = [
            dict(r) for r in conn.execute(
                """SELECT * FROM companies WHERE research_id = ?
                   ORDER BY priority_score DESC NULLS LAST, company_name""",
                (research_id,),
            ).fetchall()
        ]
        emails = [
            dict(r) for r in conn.execute(
                "SELECT * FROM emails WHERE research_id = ? ORDER BY created_at",
                (research_id,),
            ).fetchall()
        ]
        validation_row = conn.execute(
            """SELECT * FROM market_validations WHERE research_id = ?
               ORDER BY updated_at DESC LIMIT 1""",
            (research_id,),
        ).fetchone()
        validation = dict(validation_row) if validation_row else None

    folder = _research_dir(base, research)
    folder.mkdir(parents=True, exist_ok=True)

    _write_summary(folder, research, companies, emails, validation)
    _write_companies_csv(folder, companies)
    _write_companies_by_type(folder, companies, _categorize_company)
    _write_emails(folder, emails)
    if validation:
        _write_validation(folder, validation)

    return folder


def _write_summary(
    folder: Path,
    research: dict[str, Any],
    companies: list[dict[str, Any]],
    emails: list[dict[str, Any]],
    validation: dict[str, Any] | None,
) -> None:
    qualified = sum(1 for c in companies if c.get("status") == "qualified")
    with_email = sum(1 for c in companies if c.get("email"))
    with_phone = sum(1 for c in companies if c.get("phone"))
    pending = sum(1 for e in emails if e.get("status") == "pending")
    sent = sum(1 for e in emails if e.get("status") in ("sent", "opened", "replied", "bounced"))
    replied = sum(1 for e in emails if e.get("replied_at"))

    lines = [
        f"# {research.get('name', '(untitled)')}",
        "",
        f"- **Research ID:** `{research['id']}`",
        f"- **Market:** {research.get('market', '')}",
        f"- **Geography:** {research.get('geography') or '(none)'}",
        f"- **Product:** {research.get('product') or '(none)'}",
        f"- **Status:** {research.get('status', '')}",
        f"- **Created:** {research.get('created_at', '')}",
        f"- **Last exported:** {_iso_now()}",
    ]
    desc = research.get("description")
    if desc:
        lines += ["", "## Description", "", desc]

    lines += [
        "",
        "## Counts",
        "",
        f"- Companies: **{len(companies)}**",
        f"- Qualified: **{qualified}**",
        f"- With email: **{with_email}**",
        f"- With phone: **{with_phone}**",
        f"- Email queue: pending={pending} sent={sent} replied={replied}",
    ]

    if validation:
        lines += [
            "",
            "## Market validation verdict",
            "",
            f"- **Verdict:** {validation.get('verdict') or '(pending)'}",
            f"- **Overall score:** {validation.get('overall_score') or 'n/a'}/100",
            f"- **Reasoning:** {validation.get('verdict_reasoning') or '(none)'}",
            "",
            "See `validation.md` for the full TAM/SAM/SOM, demand, competitive, and signal data.",
        ]

    lines += [
        "",
        "## Files in this folder",
        "",
        "- `summary.md` — this file",
        "- `companies.csv` — every company in this research, all columns",
        "- `companies-by-type.md` — companies grouped by category (commercial grower, nursery, retailer, etc.)",
        "- `emails.md` — all queued + sent emails (subject, body, status)",
    ]
    if validation:
        lines += ["- `validation.md` — TAM/SAM/SOM + demand + competitive + signals scorecard"]

    (folder / "summary.md").write_text("\n".join(lines) + "\n")


def _write_companies_csv(folder: Path, companies: list[dict[str, Any]]) -> None:
    if not companies:
        (folder / "companies.csv").write_text("")
        return
    cols = [
        "id", "company_name", "website", "location", "phone", "email",
        "status", "priority_score", "priority_tier",
        "volume_estimate", "volume_unit", "notes", "created_at",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    writer.writeheader()
    for c in companies:
        writer.writerow({k: c.get(k, "") for k in cols})
    (folder / "companies.csv").write_text(buf.getvalue())


def _write_companies_by_type(
    folder: Path,
    companies: list[dict[str, Any]],
    categorize: Any,
) -> None:
    if not companies:
        (folder / "companies-by-type.md").write_text("# Companies by type\n\n_(no companies in this research)_\n")
        return

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for c in companies:
        grouped[categorize(c)].append(c)

    from market_validation.dashboard import CATEGORY_ORDER
    ordered_cats = [c for c in CATEGORY_ORDER if c in grouped] + [
        c for c in grouped if c not in CATEGORY_ORDER
    ]

    lines = [
        f"# Companies by type ({len(companies)} total)",
        "",
        f"_Generated {_iso_now()}_",
        "",
    ]

    for cat in ordered_cats:
        rows = grouped[cat]
        rows.sort(key=lambda r: -(r.get("priority_score") or 0))
        lines += [
            "",
            f"## {cat} ({len(rows)})",
            "",
            "| Score | Company | Phone | Email | Website | Status |",
            "|---|---|---|---|---|---|",
        ]
        for c in rows:
            score = c.get("priority_score")
            score_s = str(int(score)) if score is not None else "-"
            phone = c.get("phone") or "-"
            email = c.get("email") or "-"
            website = c.get("website") or "-"
            status = c.get("status") or "-"
            name = (c.get("company_name") or "").replace("|", "\\|")[:80]
            lines.append(f"| {score_s} | {name} | {phone} | {email} | {website} | {status} |")

    (folder / "companies-by-type.md").write_text("\n".join(lines) + "\n")


def _write_emails(folder: Path, emails: list[dict[str, Any]]) -> None:
    if not emails:
        (folder / "emails.md").write_text("# Emails\n\n_(no drafts in this research yet)_\n")
        return
    lines = [f"# Emails ({len(emails)} total)", "", f"_Generated {_iso_now()}_", ""]
    by_status: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for e in emails:
        by_status[e.get("status") or "unknown"].append(e)
    for status in ("pending", "sent", "opened", "replied", "bounced", "unknown"):
        rows = by_status.get(status, [])
        if not rows:
            continue
        lines += ["", f"## {status} ({len(rows)})", ""]
        for e in rows:
            lines += [
                "---",
                f"**To:** {e.get('to_email')}  ",
                f"**Company:** {e.get('company_name')}  ",
                f"**Subject:** {e.get('subject')}  ",
                f"**Status:** {e.get('status')} | sent_at: {e.get('sent_at') or '-'} | replied_at: {e.get('replied_at') or '-'}  ",
                "",
                "```",
                str(e.get("body") or ""),
                "```",
                "",
            ]
    (folder / "emails.md").write_text("\n".join(lines) + "\n")


def _write_validation(folder: Path, v: dict[str, Any]) -> None:
    def fmt(low: Any, high: Any) -> str:
        if low is None and high is None:
            return "n/a"
        if low is None:
            return f"≤ ${high:,.0f}"
        if high is None:
            return f"≥ ${low:,.0f}"
        return f"${low:,.0f} - ${high:,.0f}"

    lines = [
        f"# Market validation — {v.get('market', '')}",
        "",
        f"_Generated {_iso_now()}_",
        "",
        f"- **Verdict:** {v.get('verdict') or '(pending)'}",
        f"- **Overall score:** {v.get('overall_score') or 'n/a'}/100",
        f"- **Reasoning:** {v.get('verdict_reasoning') or '(none)'}",
        "",
        "## Sizing",
        "",
        f"- **TAM:** {fmt(v.get('tam_low'), v.get('tam_high'))} (confidence {v.get('tam_confidence') or 'n/a'})",
        f"- **SAM:** {fmt(v.get('sam_low'), v.get('sam_high'))} (confidence {v.get('sam_confidence') or 'n/a'})",
        f"- **SOM:** {fmt(v.get('som_low'), v.get('som_high'))} (confidence {v.get('som_confidence') or 'n/a'})",
        "",
        "## Demand",
        "",
        f"- **Score:** {v.get('demand_score') or 'n/a'}",
        f"- **Trend:** {v.get('demand_trend') or 'n/a'}",
        f"- **Seasonality:** {v.get('demand_seasonality') or 'n/a'}",
        f"- **Pain points:** {v.get('demand_pain_points') or 'n/a'}",
        "",
        "## Competition",
        "",
        f"- **Intensity:** {v.get('competitive_intensity') or 'n/a'}",
        f"- **Competitor count:** {v.get('competitor_count') or 'n/a'}",
        f"- **Concentration:** {v.get('market_concentration') or 'n/a'}",
        "",
        "## Sub-scores",
        "",
        f"- Market attractiveness: {v.get('market_attractiveness') or 'n/a'}",
        f"- Competitive: {v.get('competitive_score') or 'n/a'}",
        f"- Demand validation: {v.get('demand_validation') or 'n/a'}",
        f"- Risk: {v.get('risk_score') or 'n/a'}",
    ]
    (folder / "validation.md").write_text("\n".join(lines) + "\n")


def export_all_research_folders(base_dir: Path | str = "output/research") -> list[Path]:
    """Export every research in the DB. Useful for backfilling old runs."""
    db_file = resolve_db_path(Path("."))
    with _connect(db_file) as conn:
        _ensure_schema(conn)
        ids = [row[0] for row in conn.execute("SELECT id FROM researches").fetchall()]
    return [export_research_folder(rid, base_dir) for rid in ids]

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
from pathlib import Path
from typing import Any

from market_validation.environment import load_project_env
from market_validation.log import get_logger

_log = get_logger("dashboard")

load_project_env()


def _iso_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


_STATE_ABBR: dict[str, str] = {
    "california": "ca", "texas": "tx", "new york": "ny", "florida": "fl",
    "illinois": "il", "pennsylvania": "pa", "ohio": "oh", "georgia": "ga",
    "michigan": "mi", "washington": "wa", "arizona": "az", "colorado": "co",
    "massachusetts": "ma", "virginia": "va", "oregon": "or", "north carolina": "nc",
    "new jersey": "nj", "minnesota": "mn", "nevada": "nv", "utah": "ut",
}


def _geo_key(geo: str) -> str:
    """Normalize a geography string for fuzzy matching (e.g. 'San Jose, CA' ≈ 'San Jose California')."""
    g = geo.lower().strip()
    for name, abbr in _STATE_ABBR.items():
        g = g.replace(name, abbr)
    g = re.sub(r"[^\w\s]", " ", g)
    words = sorted(set(w for w in g.split() if len(w) > 1))
    return " ".join(words)


def _escape_html(value: Any) -> str:
    if value is None:
        return ""
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


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
            SELECT id, name, market, product, geography, status, created_at
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
            companies.append(
                {
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
            )

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
                vdict = dict(zip(col_names, vrow))
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
    for rid, vdict in validations.items():
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
    }


def _render_research_options(researches: list[dict[str, Any]]) -> str:
    options = ["<option value=''>All Research Projects</option>"]
    for r in researches:
        rid = _escape_html(r["id"])
        label = _escape_html(r.get("name") or rid)
        options.append(f"<option value='{rid}'>{label} ({rid})</option>")
    return "".join(options)


def _html_template(interactive: bool) -> str:
    mode = "server" if interactive else "prompt"
    return f"""<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <link rel='preconnect' href='https://fonts.googleapis.com'>
  <link rel='preconnect' href='https://fonts.gstatic.com' crossorigin>
  <link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap' rel='stylesheet'>
  <title>Market Research Dashboard</title>
  <style>
    :root {{
      --bg: #f3f5f9;
      --surface: #ffffff;
      --text: #182433;
      --muted: #5e7083;
      --line: #dde3eb;
      --brand: #2563eb;
      --brand-light: #eff4ff;
      --ok: #16a34a;
      --ok-light: #ecfdf3;
      --warn: #ca8a04;
      --warn-light: #fefce8;
      --danger: #dc2626;
      --danger-light: #fef2f2;
      --info: #0284c7;
      --info-light: #f0f9ff;
      --purple: #7c3aed;
      --purple-light: #f5f0ff;
      --radius: 12px;
      --shadow: 0 1px 3px rgba(24, 36, 51, 0.06), 0 8px 24px rgba(24, 36, 51, 0.06);
      --shadow-sm: 0 1px 2px rgba(24, 36, 51, 0.05);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; color: var(--text); background: var(--bg); font: 14px/1.5 'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif; -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale; }}
    .app {{ max-width: 1520px; margin: 0 auto; padding: 28px 24px; }}
    .header {{ display: flex; gap: 20px; justify-content: space-between; align-items: flex-end; margin-bottom: 24px; padding-bottom: 20px; border-bottom: 1px solid var(--line); }}
    .title h1 {{ margin: 0; font-size: clamp(20px, 2.5vw, 28px); font-weight: 700; letter-spacing: -0.02em; }}
    .title p {{ margin: 3px 0 0; color: var(--muted); font-size: 13px; }}
    .kpis {{ display: grid; grid-template-columns: repeat(4, minmax(90px, 1fr)); gap: 10px; width: min(760px, 100%); }}
    .kpi {{ background: var(--surface); border: 1px solid var(--line); border-radius: 10px; padding: 14px 14px 12px; box-shadow: var(--shadow-sm); transition: box-shadow 0.15s ease; }}
    .kpi:hover {{ box-shadow: var(--shadow); }}
    .kpi .v {{ font-size: 26px; font-weight: 700; letter-spacing: -0.02em; font-variant-numeric: tabular-nums; }}
    .kpi .l {{ font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; margin-top: 2px; }}
    .kpi.kpi-ok .v {{ color: var(--ok); }}
    .kpi.kpi-info .v {{ color: var(--info); }}
    .kpi.kpi-purple .v {{ color: var(--purple); }}
    .kpi.kpi-warn .v {{ color: var(--warn); }}
    .kpi.kpi-brand .v {{ color: var(--brand); }}
    .kpi.kpi-ok {{ border-bottom: 2px solid var(--ok); }}
    .kpi.kpi-info {{ border-bottom: 2px solid var(--info); }}
    .kpi.kpi-purple {{ border-bottom: 2px solid var(--purple); }}
    .kpi.kpi-warn {{ border-bottom: 2px solid var(--warn); }}
    .kpi.kpi-brand {{ border-bottom: 2px solid var(--brand); }}
    .panel {{ background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius); box-shadow: var(--shadow-sm); margin-bottom: 24px; overflow: hidden; }}
    .panel-head {{ padding: 16px 20px; border-bottom: 1px solid var(--line); display: flex; justify-content: space-between; align-items: center; background: #f8fafc; }}
    .panel-head h2 {{ margin: 0; font-size: 14px; font-weight: 600; letter-spacing: 0.02em; text-transform: uppercase; color: #475569; }}
    .panel-body {{ padding: 16px 20px; }}
    .toolbar {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 12px; align-items: center; }}
    .select {{ min-width: 280px; border: 1px solid var(--line); border-radius: 8px; padding: 9px 12px; font: inherit; background: #fff; color: var(--text); transition: border-color 0.15s; }}
    .select:focus {{ outline: none; border-color: var(--brand); box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1); }}
    .count-pill {{ border: 1px solid var(--line); border-radius: 999px; padding: 6px 12px; font-size: 12px; color: var(--muted); background: var(--bg); font-weight: 500; }}
    .btn-link {{ display: inline-block; border: 1px solid var(--brand); border-radius: 8px; padding: 7px 14px; background: var(--brand-light); color: var(--brand); text-decoration: none; font-size: 13px; font-weight: 500; transition: background 0.15s, box-shadow 0.15s; }}
    .btn-link:hover {{ background: #dce8ff; box-shadow: var(--shadow-sm); }}
    .table-wrap {{ width: 100%; overflow: auto; border: 1px solid var(--line); border-radius: 10px; }}
    table {{ width: 100%; min-width: 860px; border-collapse: collapse; }}
    th, td {{ padding: 10px 14px; text-align: left; border-bottom: 1px solid #eef2f6; vertical-align: top; }}
    th {{ background: #f8fafc; font-size: 11px; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; position: sticky; top: 0; z-index: 2; }}
    tbody tr:nth-child(even) td {{ background: #fafbfc; }}
    tbody tr:hover td {{ background: #f0f5ff; }}
    tbody tr:nth-child(even):hover td {{ background: #f0f5ff; }}
    td a {{ color: var(--brand); text-decoration: none; }}
    td a:hover {{ text-decoration: underline; }}
    .muted {{ color: var(--muted); }}
    .empty {{ color: var(--muted); margin: 0; padding: 24px 16px; text-align: center; font-size: 13px; }}
    .badge {{ display: inline-block; padding: 3px 10px; border-radius: 999px; font-size: 12px; font-weight: 500; border: 1px solid transparent; }}
    .priority-high {{ background: var(--ok-light); color: var(--ok); border-color: #bbf7d0; }}
    .priority-medium {{ background: var(--warn-light); color: var(--warn); border-color: #fef08a; }}
    .priority-low {{ background: #f1f5f9; color: #64748b; border-color: #e2e8f0; }}
    .status-pending {{ background: var(--warn-light); color: var(--warn); border-color: #fef08a; }}
    .status-sent {{ background: var(--ok-light); color: var(--ok); border-color: #bbf7d0; }}
    .status-opened {{ background: var(--info-light); color: var(--info); border-color: #bae6fd; }}
    .status-clicked {{ background: var(--purple-light); color: var(--purple); border-color: #ddd6fe; }}
    .status-replied {{ background: var(--ok-light); color: var(--ok); border-color: #bbf7d0; font-weight: 600; }}
    .status-bounced {{ background: var(--danger-light); color: var(--danger); border-color: #fecaca; }}
    .status-date {{ margin-top: 3px; font-size: 11px; color: var(--muted); }}
    .preview-cell {{ font-size: 13px; min-width: 260px; }}
    .sent-content {{ color: var(--muted); white-space: pre-wrap; line-height: 1.5; }}
    .reply-label {{ font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: var(--ok); margin-bottom: 4px; }}
    .reply-content {{ white-space: pre-wrap; line-height: 1.5; color: var(--text); }}
    .action-link {{ margin-right: 8px; white-space: nowrap; font-size: 13px; color: var(--brand); text-decoration: none; font-weight: 500; }}
    .action-link:hover {{ text-decoration: underline; }}
    .editing-row td {{ background: #fffef5; }}
    .cell-input {{ width: 100%; border: 1px solid var(--line); border-radius: 6px; padding: 7px 10px; font: inherit; background: #fff; transition: border-color 0.15s; }}
    .cell-input:focus {{ outline: none; border-color: var(--brand); box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1); }}
    .cell-split {{ display: flex; gap: 6px; }}
    .notes-cell {{ max-width: 200px; }}
    .notes-preview {{ display: -webkit-box; -webkit-line-clamp: 2; line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; font-size: 13px; line-height: 1.4; color: var(--muted); }}
    .notes-more {{ font-size: 11px; color: var(--brand); cursor: pointer; white-space: nowrap; border: none; background: none; padding: 2px 0; text-decoration: underline; }}
    .notes-more:hover {{ opacity: 0.75; }}
    .note-modal-overlay {{ display: none; position: fixed; inset: 0; background: rgba(15,23,42,0.4); backdrop-filter: blur(4px); z-index: 1000; align-items: center; justify-content: center; }}
    .note-modal-overlay.open {{ display: flex; }}
    .note-modal {{ background: #fff; border-radius: var(--radius); padding: 0; max-width: 520px; width: 90%; box-shadow: 0 20px 60px rgba(0,0,0,0.2); max-height: 80vh; display: flex; flex-direction: column; }}
    .note-modal-header {{ padding: 18px 20px 14px; border-bottom: 1px solid var(--line); }}
    .note-modal-header h3 {{ margin: 0; font-size: 16px; color: var(--text); }}
    .note-modal-body {{ padding: 16px 20px; overflow-y: auto; flex: 1; }}
    .note-section {{ margin-bottom: 14px; }}
    .note-section:last-child {{ margin-bottom: 0; }}
    .note-section-label {{ font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: var(--brand); margin-bottom: 4px; }}
    .note-section-text {{ font-size: 13px; line-height: 1.6; color: #475569; }}
    .note-section-text li {{ margin-bottom: 3px; }}
    .note-modal-footer {{ padding: 12px 20px; border-top: 1px solid var(--line); }}
    .note-modal-close {{ display: block; width: 100%; padding: 9px; border: 1px solid var(--line); border-radius: 8px; background: #f8fafc; cursor: pointer; font: inherit; font-size: 13px; font-weight: 500; color: var(--text); transition: background 0.15s; }}
    .note-modal-close:hover {{ background: #edf2f7; }}

    /* ── Pagination ── */
    .pagination {{ display: flex; align-items: center; justify-content: center; gap: 12px; padding: 12px 0 4px; }}
    .pagination button {{ border: 1px solid var(--line); border-radius: 8px; padding: 7px 14px; background: #fff; cursor: pointer; font: inherit; font-size: 13px; font-weight: 500; color: var(--text); transition: background 0.15s, border-color 0.15s; }}
    .pagination button:hover:not(:disabled) {{ background: var(--brand-light); border-color: var(--brand); color: var(--brand); }}
    .pagination button:disabled {{ opacity: 0.4; cursor: default; }}
    .pagination .page-info {{ font-size: 13px; color: var(--muted); font-weight: 500; }}

    /* ── Validation section ── */
    .val-section {{ margin-bottom: 20px; }}
    .val-section-title {{ font-size: 11px; font-weight: 600; text-transform: uppercase; color: var(--muted); letter-spacing: .05em; margin-bottom: 10px; padding-bottom: 6px; border-bottom: 1px solid var(--line); }}
    .val-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 14px; margin-bottom: 14px; }}
    .val-card {{ background: #f8fafc; border: 1px solid var(--line); border-radius: 10px; padding: 14px 16px; }}
    .val-kv {{ display: flex; justify-content: space-between; font-size: 13px; margin: 5px 0; }}
    .val-kv .k {{ color: var(--muted); }}
    .val-kv .v {{ font-weight: 600; color: var(--text); }}
    .val-bar {{ margin: 6px 0 10px; position: relative; }}
    .val-bar-header {{ display: flex; justify-content: space-between; font-size: 13px; }}
    .val-bar-header .score {{ font-weight: 600; font-variant-numeric: tabular-nums; }}
    .val-bar-track {{ background: #e8ecf0; border-radius: 6px; height: 6px; overflow: hidden; margin-top: 4px; }}
    .val-bar-fill {{ height: 100%; border-radius: 6px; transition: width 0.4s ease; }}
    .val-bar-label {{ font-size: 12px; cursor: default; }}
    .val-bar-tip {{ display: none; position: absolute; left: 0; top: 100%; margin-top: 4px; background: #1e293b; color: #f1f5f9; font-size: 11px; line-height: 1.4; padding: 6px 10px; border-radius: 6px; z-index: 10; max-width: 320px; white-space: normal; pointer-events: none; }}
    .val-bar:hover .val-bar-tip {{ display: block; }}

    /* ── Verdict hero ── */
    .val-hero {{ display: flex; align-items: center; gap: 20px; padding: 20px; background: #f8fafc; border: 1px solid var(--line); border-radius: 10px; margin-bottom: 16px; }}
    .val-hero-score {{ width: 80px; height: 80px; border-radius: 50%; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }}
    .val-hero-score span {{ font-size: 28px; font-weight: 800; color: #fff; font-variant-numeric: tabular-nums; }}
    .val-hero-body {{ flex: 1; min-width: 0; }}
    .val-hero-verdict {{ font-size: 18px; font-weight: 700; margin-bottom: 2px; }}
    .val-hero-reasoning {{ font-size: 13px; line-height: 1.6; color: #475569; }}

    /* ── Collapsible sections ── */
    .val-collapse-toggle {{ cursor: pointer; user-select: none; display: flex; align-items: center; gap: 6px; }}
    .val-collapse-toggle::before {{ content: '▸'; font-size: 11px; color: #94a3b8; transition: transform 0.15s; display: inline-block; }}
    .val-collapse-toggle.open::before {{ transform: rotate(90deg); }}
    .val-collapse-body {{ display: none; }}
    .val-collapse-body.open {{ display: block; }}
    .val-tag {{ display: inline-block; padding: 3px 10px; border-radius: 999px; font-size: 12px; background: #f1f5f9; color: #475569; margin: 2px; font-weight: 500; }}
    .val-archetype {{ background: var(--brand-light); color: var(--brand); border-radius: 8px; padding: 5px 14px; font-size: 13px; font-weight: 600; }}
    .val-list {{ padding-left: 16px; margin: 4px 0; font-size: 13px; color: #555; }}
    .val-list li {{ margin-bottom: 3px; }}
    .val-empty {{ color: var(--muted); font-size: 13px; font-style: italic; }}

    /* ── Status badges in table ── */
    .status-new {{ background: #f1f5f9; color: #64748b; border-color: #e2e8f0; }}
    .status-qualified {{ background: var(--ok-light); color: var(--ok); border-color: #bbf7d0; }}
    .status-contacted {{ background: var(--info-light); color: var(--info); border-color: #bae6fd; }}
    .status-interested {{ background: var(--purple-light); color: var(--purple); border-color: #ddd6fe; }}
    .status-not_interested {{ background: var(--danger-light); color: var(--danger); border-color: #fecaca; }}

    /* ── Table polish ── */
    th {{ white-space: nowrap; }}
    td .company-sub {{ font-size: 12px; color: var(--muted); margin-top: 2px; }}
    .phone-cell {{ white-space: nowrap; font-variant-numeric: tabular-nums; }}
    .email-cell {{ font-size: 13px; word-break: break-all; }}
    .score-cell {{ text-align: center; font-variant-numeric: tabular-nums; }}
    .location-cell {{ font-size: 13px; max-width: 180px; }}

    /* ── Empty state ── */
    .empty-state {{ text-align: center; padding: 48px 24px; color: var(--muted); }}
    .empty-state .empty-icon {{ width: 48px; height: 48px; margin: 0 auto 12px; background: #f1f5f9; border-radius: 12px; display: flex; align-items: center; justify-content: center; }}
    .empty-state .empty-icon svg {{ width: 24px; height: 24px; fill: none; stroke: #94a3b8; stroke-width: 1.5; }}
    .empty-state p {{ margin: 4px 0; font-size: 14px; line-height: 1.5; }}

    @media (max-width: 980px) {{
      .app {{ padding: 16px; }}
      .header {{ flex-direction: column; align-items: stretch; gap: 16px; }}
      .kpis {{ grid-template-columns: repeat(4, minmax(80px, 1fr)); width: 100%; }}
      .select {{ min-width: 100%; }}
      .panel-body {{ padding: 14px 16px; }}
    }}
    @media (max-width: 640px) {{
      .kpis {{ grid-template-columns: repeat(2, 1fr); }}
      .kpi .v {{ font-size: 22px; }}
      table {{ min-width: 600px; }}
    }}
  </style>
</head>
<body>
  <div id="note-modal-overlay" class="note-modal-overlay" onclick="if(event.target===this)closeNoteModal()">
    <div class="note-modal">
      <div class="note-modal-header"><h3 id="note-modal-company"></h3></div>
      <div class="note-modal-body" id="note-modal-body"></div>
      <div class="note-modal-footer"><button class="note-modal-close" onclick="closeNoteModal()">Close</button></div>
    </div>
  </div>
  <div id="email-modal-overlay" class="note-modal-overlay" onclick="if(event.target===this)closeEmailModal()">
    <div class="note-modal" style="max-width:640px;width:92%">
      <div class="note-modal-header">
        <h3 id="email-modal-title">Draft email</h3>
        <div class="muted" id="email-modal-meta" style="font-size:12px;margin-top:4px"></div>
      </div>
      <div class="note-modal-body">
        <label style="display:block;font-size:12px;color:var(--muted);margin-bottom:4px">Subject</label>
        <input id="email-modal-subject" class="cell-input" style="width:100%;margin-bottom:12px" />
        <label style="display:block;font-size:12px;color:var(--muted);margin-bottom:4px">Body</label>
        <textarea id="email-modal-body-text" class="cell-input" style="width:100%;min-height:220px;font-family:inherit"></textarea>
        <div id="email-modal-status" class="muted" style="font-size:12px;margin-top:8px;min-height:16px"></div>
      </div>
      <div class="note-modal-footer" style="display:flex;gap:8px">
        <button class="note-modal-close" onclick="closeEmailModal()">Cancel</button>
        <button class="note-modal-close" id="email-modal-regen" onclick="regenDraft()" style="background:#e2e8f0;color:#0f172a">Regenerate</button>
        <button class="note-modal-close" id="email-modal-queue" onclick="queueDraft()" style="background:#2563eb;color:#fff">Queue draft</button>
      </div>
    </div>
  </div>
  <div id="company-modal-overlay" class="note-modal-overlay" onclick="if(event.target===this)closeCompanyModal()">
    <div class="note-modal" style="max-width:680px;width:92%">
      <div class="note-modal-header">
        <h3 id="company-modal-title">Edit company</h3>
        <div class="muted" id="company-modal-meta" style="font-size:12px;margin-top:4px"></div>
      </div>
      <div class="note-modal-body">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
          <div style="grid-column:1 / -1">
            <label style="display:block;font-size:12px;color:var(--muted);margin-bottom:4px">Company name</label>
            <input id="cm-company_name" class="cell-input" style="width:100%" />
          </div>
          <div style="grid-column:1 / -1">
            <label style="display:block;font-size:12px;color:var(--muted);margin-bottom:4px">Website</label>
            <input id="cm-website" class="cell-input" style="width:100%" />
          </div>
          <div style="grid-column:1 / -1">
            <label style="display:block;font-size:12px;color:var(--muted);margin-bottom:4px">Location</label>
            <input id="cm-location" class="cell-input" style="width:100%" />
          </div>
          <div>
            <label style="display:block;font-size:12px;color:var(--muted);margin-bottom:4px">Phone</label>
            <input id="cm-phone" class="cell-input" style="width:100%" />
          </div>
          <div>
            <label style="display:block;font-size:12px;color:var(--muted);margin-bottom:4px">Email</label>
            <input id="cm-email" class="cell-input" style="width:100%" />
          </div>
          <div>
            <label style="display:block;font-size:12px;color:var(--muted);margin-bottom:4px">Priority</label>
            <select id="cm-priority_tier" class="cell-input" style="width:100%">
              <option value="high">high</option>
              <option value="medium">medium</option>
              <option value="low">low</option>
            </select>
          </div>
          <div>
            <label style="display:block;font-size:12px;color:var(--muted);margin-bottom:4px">Status</label>
            <select id="cm-status" class="cell-input" style="width:100%">
              <option value="new">new</option>
              <option value="qualified">qualified</option>
              <option value="contacted">contacted</option>
              <option value="interested">interested</option>
              <option value="not_interested">not_interested</option>
            </select>
          </div>
          <div style="grid-column:1 / -1">
            <label style="display:block;font-size:12px;color:var(--muted);margin-bottom:4px">Notes</label>
            <textarea id="cm-notes" class="cell-input" style="width:100%;min-height:140px;font-family:inherit"></textarea>
          </div>
        </div>
        <div id="company-modal-status" class="muted" style="font-size:12px;margin-top:8px;min-height:16px"></div>
      </div>
      <div class="note-modal-footer" style="display:flex;gap:8px">
        <button class="note-modal-close" onclick="closeCompanyModal()">Cancel</button>
        <button class="note-modal-close" id="company-modal-save" onclick="saveCompanyModal()" style="background:#2563eb;color:#fff">Save</button>
      </div>
    </div>
  </div>
  <div class='app'>
    <div class='header'>
      <div class='title'>
        <h1>Market Research Dashboard</h1>
        <p>Generated __GENERATED_AT__</p>
      </div>
      <div class='kpis'>
        <div class='kpi'><div class='v' id='kpiProjects'>__RESEARCH_COUNT__</div><div class='l'>Projects</div></div>
        <div class='kpi kpi-brand'><div class='v' id='kpiCompanies'>__COMPANY_COUNT__</div><div class='l'>Companies</div></div>
        <div class='kpi kpi-ok'><div class='v' id='kpiQualified'>__QUALIFIED_COUNT__</div><div class='l'>Qualified</div></div>
        <div class='kpi kpi-purple'><div class='v' id='kpiPhone'>__PHONE_COUNT__</div><div class='l'>With Phone</div></div>
        <div class='kpi kpi-info'><div class='v' id='kpiEmail'>__EMAIL_COUNT__</div><div class='l'>With Email</div></div>
        <div class='kpi kpi-warn'><div class='v' id='kpiPending'>__PENDING_COUNT__</div><div class='l'>Pending</div></div>
        <div class='kpi'><div class='v' id='kpiSent'>__SENT_COUNT__</div><div class='l'>Sent</div></div>
        <div class='kpi kpi-ok'><div class='v' id='kpiReplied'>__REPLIED_COUNT__</div><div class='l'>Replied</div></div>
      </div>
    </div>
    <section class='panel'>
      <div class='panel-head'><h2>Filters</h2></div>
      <div class='panel-body'>
        <div class='toolbar'>
          <select id='researchSelect' class='select'>__RESEARCH_OPTIONS__</select>
          <span id='currentResearchLabel' class='count-pill'>All Research Projects</span>
          <span class='count-pill'>Mode: {mode}</span>
        </div>
      </div>
    </section>
    <section class='panel' id='validationPanel' style='display:none'>
      <div class='panel-head'><h2>Market Validation</h2><span id='verdictBadge' class='count-pill'></span></div>
      <div class='panel-body'>
        <p id='validationSubtitle' class='muted' style='margin:0 0 16px;font-size:13px;line-height:1.5'></p>
        <div id='validationWrap'></div>
      </div>
    </section>
    <section class='panel'>
      <div class='panel-head'><h2 id='companiesTitle'>Companies</h2></div>
      <div class='panel-body'>
        <div class='toolbar'>
          <span id='companyCount' class='count-pill'>0 rows</span>
          <a class='btn-link' href='#' onclick='addCompanyRow(); return false;'>Add Company</a>
          <a class='btn-link' href='#' onclick='exportCSV(); return false;'>Export CSV</a>
          <a class='btn-link' href='#' onclick='draftAllQualified(); return false;'>Draft Emails (qualified)</a>
        </div>
        <div id='companiesWrap' class='table-wrap'></div>
        <div id='companiesPagination' class='pagination'></div>
      </div>
    </section>
    <section class='panel' id='emailPanel'>
      <div class='panel-head'>
        <h2 id='emailsTitle'>Email Queue</h2>
        <span id='syncStatus' class='count-pill' style='font-size:12px;color:var(--muted)'></span>
      </div>
      <div class='panel-body'>
        <div class='toolbar'>
          <span id='emailCount' class='count-pill'>0 rows</span>
          <a class='btn-link' href='#' onclick='approveAllEmails(); return false;'>Approve all pending</a>
          <a class='btn-link' href='#' onclick='rejectAllEmails(); return false;'>Reject all pending</a>
        </div>
        <div id='emailsWrap' class='table-wrap'></div>
      </div>
    </section>
  </div>

  <script id='dashboard-data' type='application/json'>__PAYLOAD_JSON__</script>
  <script>
    const DATA = JSON.parse(document.getElementById('dashboard-data').textContent);
    const params = new URLSearchParams(window.location.search);
    // Auto-select: URL param > first research with validation > first research
    let selectedResearchId = params.get('research_id') || (function() {{
      const withVal = DATA.researches.find(r => r.validation);
      return withVal ? withVal.id : (DATA.researches[0] ? DATA.researches[0].id : '');
    }})();
    const INTERACTIVE = __INTERACTIVE__;
    let editingCompanyId = null;
    let currentPage = 1;
    const PAGE_SIZE = 50;

    function esc(v) {{
      if (v === null || v === undefined) return '';
      return String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/\"/g, '&quot;');
    }}

    function escSingle(v) {{
      return String(v || '').split('\\\\').join('\\\\\\\\').split("'").join("\\'").split('\\n').join('\\\\n');
    }}

    function selectedResearch() {{
      if (!selectedResearchId) return null;
      return DATA.researches.find((r) => r.id === selectedResearchId) || null;
    }}

    function renderValidation() {{
      try {{
      const panel = document.getElementById('validationPanel');
      const wrap = document.getElementById('validationWrap');
      const badge = document.getElementById('verdictBadge');
      const ref = selectedResearch();
      console.log('[dashboard] renderValidation: ref=' + (ref ? ref.id : 'null') + ' has_validation=' + !!(ref && ref.validation) + (ref && ref.validation && ref.validation._cross_linked ? ' (cross-linked)' : ''));
      if (!ref || !ref.validation) {{ panel.style.display = 'none'; return; }}
      panel.style.display = '';
      const v = ref.validation;
      const verdictColors = {{ strong_go: '#16a34a', go: '#2563eb', cautious: '#ca8a04', no_go: '#dc2626' }};
      const verdictLabels = {{ strong_go: 'STRONG GO', go: 'GO', cautious: 'CAUTIOUS', no_go: 'NO GO' }};
      const vc = verdictColors[v.verdict] || '#5e7083';
      badge.style.display = 'none';  // verdict now in hero, hide header badge

      // ── Subtitle ──
      const mkt = esc(v.market || ref.market || '');
      const geo = esc(v.geography || ref.geography || '');
      document.getElementById('validationSubtitle').innerHTML =
        `Opportunity assessment for <strong>${{mkt}}</strong> in <strong>${{geo}}</strong>. Scores reflect this specific market and geography.`;

      const fmt = (n) => n == null ? '-' : typeof n === 'number' ? n.toLocaleString('en-US', {{style:'currency',currency:'USD',maximumFractionDigits:0}}) : n;
      const pct = (n) => n == null ? '-' : (Math.round(n * 100)) + '%';
      const scoreColor = (s) => s == null ? '#94a3b8' : s >= 65 ? '#16a34a' : s >= 40 ? '#ca8a04' : '#dc2626';

      const bar = (score, label, desc) => {{
        const s = score != null ? Math.round(score) : null;
        const sc = scoreColor(s);
        const tip = desc ? `<div class="val-bar-tip">${{desc}}</div>` : '';
        return `<div class="val-bar"><div class="val-bar-header"><span class="val-bar-label">${{label}}</span><span class="score" style="color:${{sc}}">${{s != null ? s : '-'}}/100</span></div><div class="val-bar-track"><div class="val-bar-fill" style="width:${{Math.min(100,s||0)}}%;background:${{sc}}"></div></div>${{tip}}</div>`;
      }};
      const tag = (txt, color) => `<span class="val-tag" ${{color ? 'style="background:'+color+'"' : ''}}>${{esc(txt)}}</span>`;
      const parseList = (val) => {{
        if (!val) return [];
        if (Array.isArray(val)) return val;
        try {{ return JSON.parse(val); }} catch(e) {{ return [String(val)]; }}
      }};

      let html = '';
      const row2 = (a, b) => `<div class="val-grid">${{a}}${{b}}</div>`;
      const card = (content, bg, border) => `<div class="val-card"${{bg || border ? ' style="' + (bg ? 'background:'+bg+';' : '') + (border ? 'border-color:'+border : '') + '"' : ''}}>${{content}}</div>`;
      const kv = (k, v2) => `<div class="val-kv"><span class="k">${{k}}</span><span class="v">${{v2}}</span></div>`;

      // Collapsible section helper — starts collapsed by default
      let _collapseId = 0;
      const collapsible = (title, content, startOpen) => {{
        const id = 'vc' + (++_collapseId);
        const openCls = startOpen ? ' open' : '';
        return `<div class="val-section">` +
          `<div class="val-section-title val-collapse-toggle${{openCls}}" onclick="this.classList.toggle('open');document.getElementById('${{id}}').classList.toggle('open')">${{title}}</div>` +
          `<div class="val-collapse-body${{openCls}}" id="${{id}}">${{content}}</div></div>`;
      }};

      // ── Archetype weights lookup ──────────────────────────────────────────
      const archetypeWeights = {{
        'local-service':    {{ attractiveness: 20, demand: 35, competitive: 30, risk: 15 }},
        'b2b-saas':         {{ attractiveness: 35, demand: 30, competitive: 20, risk: 15 }},
        'b2c-saas':         {{ attractiveness: 30, demand: 35, competitive: 20, risk: 15 }},
        'b2b-industrial':   {{ attractiveness: 25, demand: 25, competitive: 30, risk: 20 }},
        'consumer-cpg':     {{ attractiveness: 30, demand: 35, competitive: 15, risk: 20 }},
        'marketplace':      {{ attractiveness: 40, demand: 30, competitive: 15, risk: 15 }},
        'healthcare':       {{ attractiveness: 25, demand: 25, competitive: 20, risk: 30 }},
        'services-agency':  {{ attractiveness: 25, demand: 30, competitive: 25, risk: 20 }},
      }};
      const aw = archetypeWeights[v.archetype] || {{ attractiveness: 30, demand: 25, competitive: 25, risk: 20 }};

      // ── 1. VERDICT HERO — the first thing you see ─────────────────────────
      {{
        const overall = v.overall_score != null ? Math.round(v.overall_score) : 0;
        const verdictText = verdictLabels[v.verdict] || (v.verdict || 'N/A').toUpperCase();
        let heroHtml = `<div class="val-hero" style="border-left:4px solid ${{vc}}">`;
        heroHtml += `<div class="val-hero-score" style="background:${{vc}}"><span>${{overall}}</span></div>`;
        heroHtml += `<div class="val-hero-body">`;
        heroHtml += `<div class="val-hero-verdict" style="color:${{vc}}">${{verdictText}}</div>`;
        if (v.verdict_reasoning) {{
          heroHtml += `<div class="val-hero-reasoning">${{esc(v.verdict_reasoning)}}</div>`;
        }}
        heroHtml += `</div></div>`;
        html += heroHtml;
      }}

      // ── 2. ARCHETYPE + WEIGHTS — single compact row ───────────────────────
      if (v.archetype_label) {{
        let archHtml = `<div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">`;
        archHtml += `<span class="val-archetype">${{esc(v.archetype_label)}}</span>`;
        archHtml += `<div style="display:flex;gap:4px;flex-wrap:wrap">`;
        archHtml += `<span class="val-tag" style="background:#f0f5ff;color:#2563eb;font-weight:600;font-size:11px">Attractiveness ${{aw.attractiveness}}%</span>`;
        archHtml += `<span class="val-tag" style="background:#f0f5ff;color:#2563eb;font-weight:600;font-size:11px">Demand ${{aw.demand}}%</span>`;
        archHtml += `<span class="val-tag" style="background:#f0f5ff;color:#2563eb;font-weight:600;font-size:11px">Competition ${{aw.competitive}}%</span>`;
        archHtml += `<span class="val-tag" style="background:#f0f5ff;color:#2563eb;font-weight:600;font-size:11px">Risk ${{aw.risk}}%</span>`;
        archHtml += `</div></div>`;
        html += card(archHtml, '#fafbfc', '#e2e8f0') + '<div style="height:10px"></div>';
      }}

      // ── 3. CORE + MODULE SCORES — color-coded per score ───────────────────
      {{
        let coreHtml = `<div style="font-size:11px;font-weight:700;text-transform:uppercase;color:#475569;margin-bottom:10px;letter-spacing:0.03em">Core Scores <span style="font-weight:500;text-transform:none;color:#94a3b8">— weighted, drive verdict</span></div>`;
        coreHtml += bar(v.market_attractiveness, 'Attractiveness (' + aw.attractiveness + '%)', 'TAM size, growth rate, and demand trend in this geography');
        coreHtml += bar(v.demand_validation, 'Demand (' + aw.demand + '%)', 'Search trends, willingness to pay, and customer need signals');
        coreHtml += bar(v.competitive_score != null ? 100-v.competitive_score : null, 'Competition (' + aw.competitive + '%)', 'Inverted — higher means less competition, easier entry');
        coreHtml += bar(v.risk_score != null ? 100-v.risk_score : null, 'Risk (' + aw.risk + '%)', 'Inverted — higher means fewer regulatory, tech, and barrier risks');

        let modHtml = `<div style="font-size:11px;font-weight:700;text-transform:uppercase;color:#475569;margin-bottom:10px;letter-spacing:0.03em">Module Scores <span style="font-weight:500;text-transform:none;color:#94a3b8">— fine-tune overall</span></div>`;
        if (v.unit_economics_score != null) modHtml += bar(v.unit_economics_score, 'Unit Economics', 'Gross margins, CAC, LTV, and payback period viability');
        if (v.structural_attractiveness != null) modHtml += bar(v.structural_attractiveness, "Porter's Five Forces", 'Supplier power, buyer power, substitutes, barriers, rivalry');
        if (v.timing_score != null) modHtml += bar(v.timing_score, 'Market Timing' + (v.timing_verdict ? ' (' + esc(v.timing_verdict) + ')' : ''), 'Enablers vs headwinds — is now the right time to enter?');
        if (v.icp_clarity != null) modHtml += bar(v.icp_clarity, 'ICP Clarity', 'How well-defined is the ideal customer profile');

        html += row2(card(coreHtml), card(modHtml));
      }}

      // ── 4. NEXT STEPS + KEY RISKS — always visible, actionable ────────────
      {{
        const nextSteps = parseList(v.next_steps);
        const keyRisks = parseList(v.key_risks);
        if (nextSteps.length || keyRisks.length) {{
          let nsHtml = '', krHtml = '';
          if (nextSteps.length) {{
            let content = '<div style="font-size:11px;font-weight:700;text-transform:uppercase;color:#16a34a;margin-bottom:8px;letter-spacing:0.03em">Next Steps</div>';
            nextSteps.forEach((s, i) => {{
              content += `<div style="display:flex;gap:8px;margin-bottom:8px;font-size:13px;line-height:1.5"><span style="background:#16a34a;color:#fff;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:600;margin-top:1px">${{i+1}}</span><span>${{esc(s)}}</span></div>`;
            }});
            nsHtml = card(content, '#f0fdf4', '#bbf7d0');
          }}
          if (keyRisks.length) {{
            let content = '<div style="font-size:11px;font-weight:700;text-transform:uppercase;color:#dc2626;margin-bottom:8px;letter-spacing:0.03em">Key Risks</div>';
            keyRisks.forEach(r => {{
              content += `<div style="display:flex;gap:8px;margin-bottom:8px;font-size:13px;line-height:1.5"><span style="color:#dc2626;min-width:14px;margin-top:1px">▲</span><span>${{esc(r)}}</span></div>`;
            }});
            krHtml = card(content, '#fef2f2', '#fecaca');
          }}
          html += row2(nsHtml, krHtml);
        }}
      }}

      // ── 5. DETAILS — collapsible sections, start collapsed ────────────────

      // Market Sizing + Unit Economics
      {{
        let sizHtml = '';
        sizHtml += kv('TAM', `${{fmt(v.tam_low)}} – ${{fmt(v.tam_high)}} <span class="muted">(conf: ${{v.tam_confidence||'-'}}%)</span>`);
        sizHtml += kv('SAM', `${{fmt(v.sam_low)}} – ${{fmt(v.sam_high)}} <span class="muted">(conf: ${{v.sam_confidence||'-'}}%)</span>`);
        sizHtml += kv('SOM', `${{fmt(v.som_low)}} – ${{fmt(v.som_high)}} <span class="muted">(conf: ${{v.som_confidence||'-'}}%)</span>`);
        if (v.gross_margin_low != null) sizHtml += kv('Gross Margin', `${{pct(v.gross_margin_low)}} – ${{pct(v.gross_margin_high)}}`);
        if (v.primary_segment) sizHtml += kv('Primary Segment', esc(v.primary_segment));

        let econHtml = '';
        if (v.cac_estimate_low != null || v.cac_estimate_high != null) {{
          econHtml += kv('CAC', `${{fmt(v.cac_estimate_low)}} – ${{fmt(v.cac_estimate_high)}}`);
        }}
        if (v.ltv_estimate_low != null || v.ltv_estimate_high != null) {{
          econHtml += kv('LTV', `${{fmt(v.ltv_estimate_low)}} – ${{fmt(v.ltv_estimate_high)}}`);
        }}
        if (v.payback_months != null) {{
          econHtml += kv('Payback Period', `${{v.payback_months}} months`);
        }}
        if (v.cac_estimate_low != null && v.ltv_estimate_low != null) {{
          const ltv_cac = v.ltv_estimate_low / (v.cac_estimate_high || 1);
          econHtml += kv('LTV:CAC (conservative)', ltv_cac.toFixed(1) + 'x');
        }}

        const sizContent = row2(
          card('<div style="font-size:11px;font-weight:700;text-transform:uppercase;color:#475569;margin-bottom:8px;letter-spacing:0.03em">Market Sizing</div>' + sizHtml),
          econHtml ? card('<div style="font-size:11px;font-weight:700;text-transform:uppercase;color:#475569;margin-bottom:8px;letter-spacing:0.03em">Unit Economics</div>' + econHtml) : ''
        );
        html += collapsible('Market Sizing & Unit Economics', sizContent, false);
      }}

      // ── Demand Analysis ───────────────────────────────────────────────────
      {{
        const painPoints = parseList(v.demand_pain_points);
        let demHtml = '';
        demHtml += kv('Trend', `<span style="color:${{v.demand_trend==='rising'?'#16a34a':v.demand_trend==='falling'?'#dc2626':'#ca8a04'}};font-weight:700">${{esc(v.demand_trend||'-')}}</span>`);
        demHtml += kv('Demand Score', `${{v.demand_score != null ? Math.round(v.demand_score) : '-'}}/100`);
        if (v.demand_seasonality) demHtml += kv('Seasonality', esc(v.demand_seasonality));
        demHtml += kv('Hiring Activity', esc(v.job_posting_volume||'-'));
        demHtml += kv('News Sentiment', esc(v.news_sentiment||'-'));
        if (painPoints.length) {{
          demHtml += `<div style="margin-top:8px;font-size:11px;font-weight:700;text-transform:uppercase;color:#64748b;margin-bottom:4px;letter-spacing:0.03em">Pain Points</div>`;
          painPoints.forEach(p => {{
            demHtml += `<div style="font-size:13px;margin-bottom:4px;padding-left:10px;border-left:2px solid #e2e8f0">${{esc(p)}}</div>`;
          }});
        }}
        html += collapsible('Demand Analysis', card(demHtml), false);
      }}

      // ── Competitive Landscape ─────────────────────────────────────────────
      {{
        const directComps = parseList(v.direct_competitors);
        const indirectComps = parseList(v.indirect_competitors);
        const fundingSignals = parseList(v.funding_signals);
        let compHtml = '';
        compHtml += kv('Intensity', `${{v.competitive_intensity != null ? Math.round(v.competitive_intensity) : '-'}}/100`);
        compHtml += kv('Concentration', esc(v.market_concentration||'-'));
        compHtml += kv('Competitor Count', v.competitor_count || '-');
        if (directComps.length) {{
          compHtml += `<div style="margin-top:8px;font-size:12px;font-weight:700;text-transform:uppercase;color:#666;margin-bottom:4px">Direct Competitors</div>`;
          compHtml += `<div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:8px">`;
          directComps.slice(0,12).forEach(c => {{ compHtml += tag(c, '#fee2e2'); }});
          compHtml += `</div>`;
        }}
        if (indirectComps.length) {{
          compHtml += `<div style="font-size:12px;font-weight:700;text-transform:uppercase;color:#666;margin-bottom:4px">Indirect / Substitutes</div>`;
          compHtml += `<div style="display:flex;flex-wrap:wrap;gap:4px;margin-bottom:8px">`;
          indirectComps.slice(0,8).forEach(c => {{ compHtml += tag(c, '#fef3c7'); }});
          compHtml += `</div>`;
        }}
        if (fundingSignals.length) {{
          compHtml += `<div style="font-size:12px;font-weight:700;text-transform:uppercase;color:#666;margin-bottom:4px">Funding Signals</div>`;
          fundingSignals.slice(0,3).forEach(s => {{
            compHtml += `<div style="font-size:12px;margin-bottom:3px;color:#555">• ${{esc(s)}}</div>`;
          }});
        }}
        const diffOpps = parseList(v.differentiation_opportunities);
        if (diffOpps.length) {{
          compHtml += `<div style="font-size:12px;font-weight:700;text-transform:uppercase;color:#1d7b3a;margin-top:8px;margin-bottom:4px">Differentiation Opportunities</div>`;
          diffOpps.slice(0,4).forEach(d => {{
            compHtml += `<div style="font-size:12px;margin-bottom:3px;color:#155724">→ ${{esc(d)}}</div>`;
          }});
        }}
        html += collapsible('Competitive Landscape', card(compHtml), false);
      }}

      // ── Porter's Five Forces ──────────────────────────────────────────────
      {{
        const hasPorters = v.supplier_power != null || v.buyer_power != null || v.substitute_threat != null || v.entry_barrier_score != null || v.rivalry_score != null;
        if (hasPorters) {{
          let pfHtml = '';
          const pfBar = (score, label) => {{
            const s = score != null ? Math.round(score) : null;
            const col = s == null ? '#ccc' : s < 40 ? '#1d7b3a' : s < 65 ? '#996900' : '#c41e3a';
            const label2 = s == null ? 'n/a' : (s < 40 ? 'low' : s < 65 ? 'medium' : 'high');
            return `<div style="margin:5px 0"><div style="display:flex;justify-content:space-between;font-size:13px"><span>${{label}}</span><span style="font-weight:600;color:${{col}}">${{s != null ? s : '-'}}/100 <span style="font-size:11px">${{label2}}</span></span></div><div style="background:#e8ecf0;border-radius:4px;height:5px;overflow:hidden;margin-top:2px"><div style="width:${{Math.min(100,s||0)}}%;height:100%;background:${{col}};border-radius:4px"></div></div></div>`;
          }};
          pfHtml += pfBar(v.supplier_power, 'Supplier Power');
          pfHtml += pfBar(v.buyer_power, 'Buyer Power');
          pfHtml += pfBar(v.substitute_threat, 'Substitute Threat');
          pfHtml += pfBar(v.entry_barrier_score, 'Barriers to Entry');
          pfHtml += pfBar(v.rivalry_score, 'Competitive Rivalry');
          if (v.structural_attractiveness != null) {{
            pfHtml += `<div style="margin-top:8px;font-size:12px;color:#555">Overall structural attractiveness: <strong>${{Math.round(v.structural_attractiveness)}}/100</strong></div>`;
          }}
          html += collapsible("Porter's Five Forces", card(pfHtml), false);
        }}
      }}

      // ── Market Timing ─────────────────────────────────────────────────────
      {{
        const enablers = parseList(v.timing_enablers);
        const headwinds = parseList(v.timing_headwinds);
        if (enablers.length || headwinds.length || v.timing_score != null) {{
          let timHtml = '';
          if (v.timing_score != null) {{
            timHtml += kv('Timing Score', `${{Math.round(v.timing_score)}}/100 ${{v.timing_verdict ? '(' + esc(v.timing_verdict) + ')' : ''}}`);
          }}
          if (enablers.length || headwinds.length) {{
            timHtml += `<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:8px">`;
            if (enablers.length) {{
              timHtml += `<div><div style="font-size:12px;font-weight:700;color:#1d7b3a;margin-bottom:4px">Enablers</div>`;
              enablers.forEach(e => {{ timHtml += `<div style="font-size:12px;margin-bottom:3px">✓ ${{esc(e)}}</div>`; }});
              timHtml += `</div>`;
            }}
            if (headwinds.length) {{
              timHtml += `<div><div style="font-size:12px;font-weight:700;color:#c41e3a;margin-bottom:4px">Headwinds</div>`;
              headwinds.forEach(h => {{ timHtml += `<div style="font-size:12px;margin-bottom:3px">⚠ ${{esc(h)}}</div>`; }});
              timHtml += `</div>`;
            }}
            timHtml += `</div>`;
          }}
          if (v.regulatory_risks) {{
            const regList = parseList(v.regulatory_risks);
            if (regList.length) {{
              timHtml += `<div style="margin-top:8px;font-size:12px;font-weight:700;text-transform:uppercase;color:#64748b;margin-bottom:4px">Regulatory</div>`;
              regList.forEach(r => {{ timHtml += `<div style="font-size:12px;margin-bottom:3px;color:#475569;padding-left:10px;border-left:2px solid #e2e8f0">${{esc(r)}}</div>`; }});
            }} else {{
              timHtml += kv('Regulatory', esc(v.regulatory_risks));
            }}
          }}
          if (v.technology_maturity) timHtml += kv('Tech Maturity', esc(v.technology_maturity));
          html += collapsible('Market Timing & Signals', card(timHtml), false);
        }}
      }}

      // ── Success Factors + Red Flags — archetype boilerplate, collapsed ────
      {{
        const ksf = parseList(v.key_success_factors);
        const redFlags = parseList(v.archetype_red_flags);
        if (ksf.length || redFlags.length) {{
          let ksfHtml = '', rfHtml = '';
          if (ksf.length) {{
            let content = `<div style="font-size:11px;font-weight:700;text-transform:uppercase;color:#16a34a;margin-bottom:6px;letter-spacing:0.03em">Success Factors</div>`;
            ksf.forEach(f => {{ content += `<div style="font-size:13px;margin-bottom:4px;color:#475569">✓ ${{esc(f)}}</div>`; }});
            ksfHtml = card(content, '#fff', '#bbf7d0');
          }}
          if (redFlags.length) {{
            let content = `<div style="font-size:11px;font-weight:700;text-transform:uppercase;color:#dc2626;margin-bottom:6px;letter-spacing:0.03em">Red Flags to Watch</div>`;
            redFlags.forEach(f => {{ content += `<div style="font-size:13px;margin-bottom:4px;color:#475569">${{esc(f)}}</div>`; }});
            rfHtml = card(content, '#fff', '#fecaca');
          }}
          html += collapsible('Archetype Benchmarks — ' + esc(v.archetype_label || ''), row2(ksfHtml, rfHtml), false);
        }}
      }}

      wrap.innerHTML = html;
      }} catch(err) {{
        document.getElementById('validationPanel').style.display = '';
        document.getElementById('validationWrap').innerHTML = '<div style="color:red;padding:12px;font-family:monospace">renderValidation error: ' + err + '</div>';
        console.error('renderValidation error:', err);
      }}
    }}

    function companyById(id) {{
      return DATA.companies.find((x) => x.id === id) || null;
    }}

    function priorityClass(v) {{
      const t = (v || '').toLowerCase();
      if (t === 'high') return 'priority-high';
      if (t === 'medium') return 'priority-medium';
      return 'priority-low';
    }}

    function statusClass(v) {{
      const s = (v || '').toLowerCase();
      if (s === 'replied') return 'status-replied';
      if (s === 'bounced') return 'status-bounced';
      if (s === 'clicked') return 'status-clicked';
      if (s === 'opened') return 'status-opened';
      if (s === 'sent') return 'status-sent';
      return 'status-pending';
    }}

    async function syncGmail() {{
      if (!INTERACTIVE) return;
      const el = document.getElementById('syncStatus');
      el.textContent = 'Syncing Gmail…';
      try {{
        const res = await apiPost('/api/email/sync', {{}});
        if (res.result === 'ok') {{
          const parts = [];
          if (res.replied_count) parts.push(`${{res.replied_count}} replied`);
          if (res.bounced_count) parts.push(`${{res.bounced_count}} bounced`);
          el.textContent = parts.length ? `Gmail: ${{parts.join(', ')}}` : `Gmail synced ${{res.synced_at ? res.synced_at.slice(11,16) + ' UTC' : ''}}`;
          if (res.replied_count || res.bounced_count) {{
            setTimeout(() => window.location.reload(), 600);
          }}
        }} else {{
          el.textContent = res.error ? `Gmail: ${{res.error}}` : 'Gmail sync failed';
        }}
      }} catch(e) {{
        el.textContent = 'Gmail offline';
      }}
    }}

    // --- Email drafting / bulk approval ------------------------------------

    let draftModalContext = null;  // {{ company_id, research_id }}

    function openEmailModal(ctx, draft) {{
      draftModalContext = ctx;
      const verb = ctx.mode === 'edit' ? 'Edit draft' : 'Draft email';
      document.getElementById('email-modal-title').textContent = draft.company_name
        ? `${{verb}} → ${{draft.company_name}}`
        : verb;
      document.getElementById('email-modal-meta').textContent = draft.to_email || '';
      document.getElementById('email-modal-subject').value = draft.subject || '';
      document.getElementById('email-modal-body-text').value = draft.body || '';
      document.getElementById('email-modal-status').textContent = '';
      document.getElementById('email-modal-queue').textContent =
        ctx.mode === 'edit' ? 'Save changes' : 'Queue draft';
      document.getElementById('email-modal-overlay').classList.add('open');
    }}

    function closeEmailModal() {{
      document.getElementById('email-modal-overlay').classList.remove('open');
      draftModalContext = null;
    }}

    async function draftEmailForCompany(companyId) {{
      if (!INTERACTIVE) {{ alert('Drafting requires interactive mode'); return; }}
      const c = companyById(companyId);
      if (!c || !c.email) {{ alert('Company has no email on file'); return; }}

      openEmailModal(
        {{ mode: 'new', company_id: companyId, research_id: c.research_id, to_email: c.email, company_name: c.company_name }},
        {{ company_name: c.company_name, to_email: c.email, subject: '(generating…)', body: 'Asking Claude to draft…' }}
      );

      const res = await apiPost('/api/email/draft', {{ company_id: companyId }});
      if (res.result !== 'ok') {{
        document.getElementById('email-modal-status').textContent = `Error: ${{res.error || 'draft failed'}}`;
        document.getElementById('email-modal-subject').value = '';
        document.getElementById('email-modal-body-text').value = '';
        return;
      }}
      document.getElementById('email-modal-subject').value = res.subject || '';
      document.getElementById('email-modal-body-text').value = res.body || '';
      document.getElementById('email-modal-meta').textContent = res.to_email || '';
    }}

    async function regenDraft() {{
      if (!draftModalContext) return;
      const cid = draftModalContext.company_id;
      document.getElementById('email-modal-status').textContent = 'Regenerating…';
      const res = await apiPost('/api/email/draft', {{ company_id: cid }});
      if (res.result !== 'ok') {{
        document.getElementById('email-modal-status').textContent = `Error: ${{res.error || 'draft failed'}}`;
        return;
      }}
      document.getElementById('email-modal-subject').value = res.subject || '';
      document.getElementById('email-modal-body-text').value = res.body || '';
      document.getElementById('email-modal-status').textContent = 'Regenerated.';
    }}

    async function queueDraft() {{
      if (!draftModalContext) return;
      const subject = document.getElementById('email-modal-subject').value.trim();
      const body = document.getElementById('email-modal-body-text').value.trim();
      if (!subject || !body) {{
        document.getElementById('email-modal-status').textContent = 'Subject and body are required.';
        return;
      }}
      const btn = document.getElementById('email-modal-queue');
      btn.disabled = true;
      const originalLabel = btn.textContent;
      btn.textContent = draftModalContext.mode === 'edit' ? 'Saving…' : 'Queueing…';

      let res;
      if (draftModalContext.mode === 'edit') {{
        res = await apiPost('/api/email/update', {{
          email_id: draftModalContext.email_id,
          subject, body,
        }});
      }} else {{
        res = await apiPost('/api/email/queue', {{
          to_email: draftModalContext.to_email,
          subject, body,
          company_name: draftModalContext.company_name,
          research_id: draftModalContext.research_id,
          company_id: draftModalContext.company_id,
        }});
      }}

      btn.disabled = false;
      btn.textContent = originalLabel;

      if (res.result === 'ok') {{
        closeEmailModal();
        refreshDataFromServer();
      }} else {{
        document.getElementById('email-modal-status').textContent = `Error: ${{res.error || 'save failed'}}`;
      }}
    }}

    async function draftAllQualified() {{
      if (!INTERACTIVE) return;
      if (!selectedResearchId) {{
        alert('Select a research project first.');
        return;
      }}
      const proj = selectedResearch();
      const projName = proj ? proj.name : selectedResearchId;
      if (!confirm(`Draft emails for every qualified company with an email in "${{projName}}"? This calls Claude once per company.`)) return;
      const res = await apiPost('/api/email/draft-all', {{ research_id: selectedResearchId }});
      if (res.result === 'ok') {{
        alert(`Drafted ${{res.drafted}} · skipped ${{res.skipped}} · failed ${{res.failed}} (candidates: ${{res.candidates}})`);
        refreshDataFromServer();
      }} else {{
        alert(`Error: ${{res.error || 'draft-all failed'}}`);
      }}
    }}

    async function approveAllEmails() {{
      if (!INTERACTIVE) return;
      if (!confirm('Send every pending email now? This cannot be undone.')) return;
      const res = await apiPost('/api/email/approve-all', {{}});
      if (res.result === 'ok') {{
        alert(`Sent ${{res.sent}} · failed ${{res.failed}}`);
        refreshDataFromServer();
      }} else {{
        alert(`Error: ${{res.error || 'approve-all failed'}}`);
      }}
    }}

    async function rejectAllEmails() {{
      if (!INTERACTIVE) return;
      if (!confirm('Delete every pending draft? This cannot be undone.')) return;
      const res = await apiPost('/api/email/reject-all', {{}});
      if (res.result === 'ok') {{
        alert(`Deleted ${{res.deleted}} pending drafts`);
        refreshDataFromServer();
      }} else {{
        alert(`Error: ${{res.error || 'reject-all failed'}}`);
      }}
    }}

    function filteredCompanies() {{
      return DATA.companies.filter((c) => {{
        if (selectedResearchId && c.research_id !== selectedResearchId) return false;
        return true;
      }});
    }}

    function filteredEmails() {{
      return DATA.emails.filter((e) => {{
        if (selectedResearchId) {{
          if (e.research_id && e.research_id !== selectedResearchId) return false;
          if (!e.research_id) {{
            const match = DATA.companies.find((c) => c.company_name === e.company_name && c.research_id === selectedResearchId);
            if (!match) return false;
          }}
        }}
        return true;
      }});
    }}

    function setResearchLabel() {{
      const ref = selectedResearch();
      const label = ref ? `${{ref.name}} (${{ref.id}})` : 'All Research Projects';
      document.getElementById('currentResearchLabel').textContent = label;
      document.getElementById('researchSelect').value = selectedResearchId;
    }}

    function renderCompanies() {{
      const allRows = filteredCompanies();
      const current = selectedResearch();
      const totalRows = allRows.length;
      const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
      if (currentPage > totalPages) currentPage = totalPages;
      if (currentPage < 1) currentPage = 1;
      const startIdx = (currentPage - 1) * PAGE_SIZE;
      const endIdx = Math.min(startIdx + PAGE_SIZE, totalRows);
      const rows = allRows.slice(startIdx, endIdx);

      console.log('[dashboard] renderCompanies: selectedResearchId=' + (selectedResearchId||'(all)') + ' rows=' + totalRows + ' total_companies=' + DATA.companies.length + ' page=' + currentPage + '/' + totalPages);
      document.getElementById('companiesTitle').textContent = current ? `Companies - ${{current.name}}` : 'Companies';
      document.getElementById('companyCount').textContent = totalRows === 0 ? '0 rows' : `${{startIdx + 1}}-${{endIdx}} of ${{totalRows}} rows`;

      if (!totalRows) {{
        const hasValidationOnly = current && current.total === 0 && current.validation;
        const msg = hasValidationOnly
          ? 'No companies discovered yet — this research only ran <strong>validate</strong>. Run <code>find()</code> to discover companies.'
          : 'No companies found for this project.';
        document.getElementById('companiesWrap').innerHTML = `<div class="empty-state"><div class="empty-icon"><svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" stroke-linecap="round" stroke-linejoin="round"/></svg></div><p>${{msg}}</p></div>`;
        document.getElementById('companiesPagination').innerHTML = '';
        return;
      }}

      let body = '';
      for (const c of rows) {{
        const isEditing = editingCompanyId === c.id;
        const email = c.email ? `<a href="mailto:${{esc(c.email)}}">${{esc(c.email)}}</a>` : '<span class="muted">-</span>';
        const phoneHref = c.phone ? String(c.phone).replace(/\\s+/g, '') : '';
        const phone = c.phone ? `<a href="tel:${{esc(phoneHref)}}">${{esc(c.phone)}}</a>` : '<span class="muted">-</span>';
        const websiteHost = c.website ? (() => {{ try {{ return new URL(c.website).hostname.replace(/^www\\./, ''); }} catch(e) {{ return c.website; }} }})() : '';
        const websiteCell = c.website ? `<a href="${{esc(c.website)}}" target="_blank" rel="noopener" title="${{esc(c.website)}}">${{esc(websiteHost)}}</a>` : '<span class="muted">-</span>';
        const notesRaw = c.notes || '';
        const notesEsc = esc(notesRaw);
        const notesCell = notesRaw
          ? `<div class="notes-preview">${{notesEsc}}</div>${{notesRaw.length > 100 ? `<button class="notes-more" onclick="showNoteModal('${{esc(c.id)}}')">show more</button>` : ''}}`
          : '-';

        if (isEditing) {{
          const pri = (c.priority_tier || 'low').toLowerCase();
          const st = (c.status || 'new').toLowerCase();
          body += `
            <tr class="editing-row">
              <td><input class="cell-input" id="edit-company_name-${{esc(c.id)}}" value="${{esc(c.company_name || '')}}" /></td>
              <td><input class="cell-input" id="edit-website-${{esc(c.id)}}" value="${{esc(c.website || '')}}" /></td>
              <td><input class="cell-input" id="edit-location-${{esc(c.id)}}" value="${{esc(c.location || '')}}" /></td>
              <td><input class="cell-input" id="edit-phone-${{esc(c.id)}}" value="${{esc(c.phone || '')}}" /></td>
              <td><input class="cell-input" id="edit-email-${{esc(c.id)}}" value="${{esc(c.email || '')}}" /></td>
              <td>
                <select class="cell-input" id="edit-priority_tier-${{esc(c.id)}}">
                  <option value="high" ${{pri === 'high' ? 'selected' : ''}}>high</option>
                  <option value="medium" ${{pri === 'medium' ? 'selected' : ''}}>medium</option>
                  <option value="low" ${{pri === 'low' ? 'selected' : ''}}>low</option>
                </select>
              </td>
              <td>
                <select class="cell-input" id="edit-status-${{esc(c.id)}}">
                  <option value="new" ${{st === 'new' ? 'selected' : ''}}>new</option>
                  <option value="qualified" ${{st === 'qualified' ? 'selected' : ''}}>qualified</option>
                  <option value="contacted" ${{st === 'contacted' ? 'selected' : ''}}>contacted</option>
                  <option value="interested" ${{st === 'interested' ? 'selected' : ''}}>interested</option>
                  <option value="not_interested" ${{st === 'not_interested' ? 'selected' : ''}}>not_interested</option>
                </select>
              </td>
              <td><textarea class="cell-input" id="edit-notes-${{esc(c.id)}}" rows="2">${{esc(c.notes || '')}}</textarea></td>
              <td>
                <a class="action-link" href="#" onclick="saveEditCompany('${{esc(c.id)}}'); return false;">Save</a>
                <a class="action-link" href="#" onclick="cancelEditCompany(); return false;">Cancel</a>
                <a class="action-link" href="#" onclick="deleteCompany('${{esc(c.id)}}'); return false;">Delete</a>
              </td>
            </tr>
          `;
          continue;
        }}

        const statusCls = 'status-' + (c.status || 'new').toLowerCase().replace(/\\s+/g, '_');
        body += `
          <tr>
            <td><strong>${{esc(c.company_name)}}</strong><div class="muted" style="font-size:12px">${{esc(c.research_name || '')}}</div></td>
            <td style="font-size:13px">${{websiteCell}}</td>
            <td class="location-cell">${{esc(c.location || '')}}</td>
            <td class="phone-cell">${{phone}}</td>
            <td class="email-cell">${{email}}</td>
            <td class="score-cell">${{c.priority_score != null ? Math.round(c.priority_score) : '-'}}</td><td><span class="badge ${{priorityClass(c.priority_tier)}}">${{esc(c.priority_tier || 'low')}}</span></td>
            <td><span class="badge ${{statusCls}}">${{esc((c.status || 'new').replace(/_/g, ' '))}}</span></td>
            <td class="notes-cell">${{notesCell}}</td>
            <td style="white-space:nowrap">
              <a class="action-link" href="#" onclick="startEditCompany('${{esc(c.id)}}'); return false;">Edit</a>
              ${{c.email ? `<a class="action-link" href="#" onclick="draftEmailForCompany('${{esc(c.id)}}'); return false;">Draft</a>` : ''}}
              <a class="action-link" href="#" onclick="deleteCompany('${{esc(c.id)}}'); return false;">Delete</a>
            </td>
          </tr>
        `;
      }}

      document.getElementById('companiesWrap').innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Company</th>
              <th>Website</th>
              <th>Location</th>
              <th>Phone</th>
              <th>Email</th>
              <th>Score</th><th>Priority</th>
              <th>Status</th>
              <th>Notes</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>${{body}}</tbody>
        </table>
      `;

      // Pagination controls
      const pagEl = document.getElementById('companiesPagination');
      if (totalPages <= 1) {{
        pagEl.innerHTML = '';
      }} else {{
        pagEl.innerHTML = `
          <button onclick="currentPage = 1; renderCompanies();" ${{currentPage === 1 ? 'disabled' : ''}}>First</button>
          <button onclick="currentPage--; renderCompanies();" ${{currentPage === 1 ? 'disabled' : ''}}>Previous</button>
          <span class="page-info">Page ${{currentPage}} of ${{totalPages}}</span>
          <button onclick="currentPage++; renderCompanies();" ${{currentPage === totalPages ? 'disabled' : ''}}>Next</button>
          <button onclick="currentPage = ${{totalPages}}; renderCompanies();" ${{currentPage === totalPages ? 'disabled' : ''}}>Last</button>
        `;
      }}
    }}

    function renderEmails() {{
      const rows = filteredEmails();
      const current = selectedResearch();
      document.getElementById('emailsTitle').textContent = current ? `Email Queue - ${{current.name}}` : 'Email Queue';
      document.getElementById('emailCount').textContent = `${{rows.length}} rows`;

      if (!rows.length) {{
        document.getElementById('emailsWrap').innerHTML = '<div class="empty-state"><div class="empty-icon"><svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" stroke-linecap="round" stroke-linejoin="round"/></svg></div><p>No emails for this project.</p></div>';
        return;
      }}

      let body = '';
      for (const e of rows) {{
        const status = e.status || 'pending';

        // Date of the most recent status event
        const statusDate = status === 'replied' ? e.replied_at
          : status === 'bounced' ? e.bounced_at
          : status === 'opened' ? e.opened_at
          : status === 'sent' ? e.sent_at
          : null;
        const dateLabel = statusDate ? statusDate.slice(0, 10) : '';

        // Preview column: show reply content when replied, otherwise sent body
        let previewCell = '';
        if (status === 'replied' && e.reply_snippet) {{
          previewCell = `<div class="reply-label">↩ Their reply</div><div class="reply-content">${{esc(e.reply_snippet)}}</div>`;
        }} else {{
          previewCell = `<div class="sent-content">${{esc(e.body || '-')}}</div>`;
        }}

        // Per-row actions: only for pending drafts (anything sent/replied/bounced
        // is either live on someone's inbox or settled — don't let the user touch it)
        let actionsCell = '-';
        if (status === 'pending') {{
          actionsCell = `
            <a class="action-link" href="#" onclick="approveEmail('${{esc(e.id)}}'); return false;">Approve</a>
            <a class="action-link" href="#" onclick="editEmail('${{esc(e.id)}}'); return false;">Edit</a>
            <a class="action-link" href="#" onclick="deleteEmail('${{esc(e.id)}}'); return false;">Reject</a>
          `;
        }}

        body += `
          <tr>
            <td>
              <strong>${{esc(e.subject || '-')}}</strong>
              ${{e.company_name ? `<div class="muted" style="font-size:12px;margin-top:2px">${{esc(e.company_name)}}</div>` : ''}}
            </td>
            <td style="white-space:nowrap"><a href="mailto:${{esc(e.to_email || '')}}">${{esc(e.to_email || '-')}}</a></td>
            <td>
              <span class="badge ${{statusClass(status)}}">${{esc(status)}}</span>
              ${{dateLabel ? `<div class="status-date">${{dateLabel}}</div>` : ''}}
            </td>
            <td class="preview-cell">${{previewCell}}</td>
            <td style="white-space:nowrap">${{actionsCell}}</td>
          </tr>
        `;
      }}

      document.getElementById('emailsWrap').innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Subject</th>
              <th>To</th>
              <th>Status</th>
              <th>Content</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>${{body}}</tbody>
        </table>
      `;
    }}

    async function apiPost(path, payload) {{
      const res = await fetch(path, {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload || {{}}),
      }});
      return res.json();
    }}

    function renderKpis() {{
      // KPI cards react to the current research filter — if a single project
      // is selected, show its counts; otherwise show global totals.
      const companies = filteredCompanies();
      const emails = filteredEmails();
      const has = (v) => v !== null && v !== undefined && String(v).trim() !== '';
      const qualified = companies.filter(c => (c.status || '').toLowerCase() === 'qualified').length;
      const withPhone = companies.filter(c => has(c.phone)).length;
      const withEmail = companies.filter(c => has(c.email)).length;
      const pending = emails.filter(e => (e.status || 'pending') === 'pending').length;
      const sent = emails.filter(e => e.status === 'sent' || e.status === 'opened' || e.status === 'clicked' || e.status === 'replied').length;
      const replied = emails.filter(e => e.status === 'replied').length;

      document.getElementById('kpiProjects').textContent = selectedResearchId ? '1' : String(DATA.researches.length);
      document.getElementById('kpiCompanies').textContent = String(companies.length);
      document.getElementById('kpiQualified').textContent = String(qualified);
      document.getElementById('kpiPhone').textContent = String(withPhone);
      document.getElementById('kpiEmail').textContent = String(withEmail);
      document.getElementById('kpiPending').textContent = String(pending);
      document.getElementById('kpiSent').textContent = String(sent);
      document.getElementById('kpiReplied').textContent = String(replied);
    }}

    function setResearch(id) {{
      selectedResearchId = id || '';
      currentPage = 1;
      console.log('[dashboard] setResearch ->', selectedResearchId || '(all)');
      const next = new URL(window.location.href);
      if (selectedResearchId) next.searchParams.set('research_id', selectedResearchId);
      else next.searchParams.delete('research_id');
      window.history.replaceState({{}}, '', next.toString());
      setResearchLabel();
      renderKpis();
      renderCompanies();
      renderEmails();
      renderValidation();
    }}

    function runCommandPrompt(cmd) {{
      prompt('Run this command in terminal:', cmd);
    }}

    async function refreshDataFromServer() {{
      if (!INTERACTIVE) return;
      // Fetch a fresh data payload and swap into DATA in-place, then re-render.
      // Avoids the full-page reload that used to happen here.
      try {{
        const res = await fetch('/api/data', {{ cache: 'no-store' }});
        const payload = await res.json();
        if (payload.result === 'ok' && payload.data) {{
          DATA.researches = payload.data.researches || [];
          DATA.companies = payload.data.companies || [];
          DATA.emails = payload.data.emails || [];
          DATA.validations = payload.data.validations || {{}};
          // Guard: selected research may have been deleted
          if (selectedResearchId && !DATA.researches.some(r => r.id === selectedResearchId)) {{
            selectedResearchId = '';
          }}
          renderKpis();
          renderCompanies();
          renderEmails();
          renderValidation();
          return;
        }}
      }} catch (e) {{
        console.warn('fast refresh failed, falling back to full reload', e);
      }}
      // Fallback: old full-page reload
      window.location.reload();
    }}

    let editingCompanyModalId = null;

    function startEditCompany(companyId) {{
      // Open the company-edit modal pre-filled with the current company row.
      const c = companyById(companyId);
      if (!c) return;
      editingCompanyModalId = companyId;
      document.getElementById('company-modal-title').textContent = `Edit → ${{c.company_name || 'company'}}`;
      document.getElementById('company-modal-meta').textContent = c.research_name || '';
      document.getElementById('cm-company_name').value = c.company_name || '';
      document.getElementById('cm-website').value = c.website || '';
      document.getElementById('cm-location').value = c.location || '';
      document.getElementById('cm-phone').value = c.phone || '';
      document.getElementById('cm-email').value = c.email || '';
      document.getElementById('cm-priority_tier').value = (c.priority_tier || 'low').toLowerCase();
      document.getElementById('cm-status').value = (c.status || 'new').toLowerCase();
      document.getElementById('cm-notes').value = c.notes || '';
      document.getElementById('company-modal-status').textContent = '';
      document.getElementById('company-modal-overlay').classList.add('open');
    }}

    function closeCompanyModal() {{
      editingCompanyModalId = null;
      document.getElementById('company-modal-overlay').classList.remove('open');
    }}

    async function saveCompanyModal() {{
      if (!editingCompanyModalId) return;
      const c = companyById(editingCompanyModalId);
      if (!c) return;
      const fields = {{
        company_name: document.getElementById('cm-company_name').value.trim(),
        website: document.getElementById('cm-website').value,
        location: document.getElementById('cm-location').value,
        phone: document.getElementById('cm-phone').value,
        email: document.getElementById('cm-email').value,
        priority_tier: document.getElementById('cm-priority_tier').value || 'low',
        status: document.getElementById('cm-status').value || 'new',
        notes: document.getElementById('cm-notes').value,
      }};
      if (!fields.company_name) {{
        document.getElementById('company-modal-status').textContent = 'Company name is required.';
        return;
      }}
      const btn = document.getElementById('company-modal-save');
      btn.disabled = true; btn.textContent = 'Saving…';
      const res = await apiPost('/api/company/update', {{
        company_id: c.id,
        research_id: c.research_id,
        fields,
      }});
      btn.disabled = false; btn.textContent = 'Save';
      if (res.result === 'ok' || res.ok) {{
        closeCompanyModal();
        refreshDataFromServer();
      }} else {{
        document.getElementById('company-modal-status').textContent = `Error: ${{res.error || 'save failed'}}`;
      }}
    }}

    // Legacy handlers used by non-interactive mode / old code paths. Safe no-ops.
    function cancelEditCompany() {{
      closeCompanyModal();
    }}

    function showNoteModal(companyId) {{
      const c = companyById(companyId);
      if (!c) return;
      document.getElementById('note-modal-company').textContent = c.company_name || '';
      const body = document.getElementById('note-modal-body');
      body.innerHTML = renderNotes(c.notes || '');
      document.getElementById('note-modal-overlay').classList.add('open');
    }}

    function renderNotes(raw) {{
      // Split on pipe separators, trim each chunk
      const chunks = raw.split('|').map(s => s.trim()).filter(Boolean);
      if (!chunks.length) return '';
      let html = '';
      for (const chunk of chunks) {{
        // Detect "Label: content" pattern
        const m = chunk.match(/^([^:]{{2,40}}):\\s+(.+)$/s);
        if (m) {{
          const label = esc(m[1].trim());
          const content = m[2].trim();
          // Split content by semicolons into list items if there are multiple
          const items = content.split(';').map(s => s.trim()).filter(Boolean);
          let contentHtml;
          if (items.length > 1) {{
            contentHtml = '<ul style="margin:0;padding-left:16px">' + items.map(i => `<li>${{esc(i)}}</li>`).join('') + '</ul>';
          }} else {{
            contentHtml = `<span>${{esc(content)}}</span>`;
          }}
          html += `<div class="note-section"><div class="note-section-label">${{label}}</div><div class="note-section-text">${{contentHtml}}</div></div>`;
        }} else {{
          // Plain paragraph (usually the opening summary)
          html += `<div class="note-section"><div class="note-section-text">${{esc(chunk)}}</div></div>`;
        }}
      }}
      return html;
    }}
    function closeNoteModal() {{
      document.getElementById('note-modal-overlay').classList.remove('open');
    }}
    document.addEventListener('keydown', (e) => {{
      if (e.key === 'Escape') {{
        closeNoteModal();
        closeEmailModal();
        closeCompanyModal();
      }}
    }});

    async function saveEditCompany(companyId) {{
      const c = companyById(companyId);
      if (!c) return;

      const getVal = (key) => {{
        const el = document.getElementById(`edit-${{key}}-${{companyId}}`);
        return el ? el.value : '';
      }};

      const fields = {{
        company_name: getVal('company_name').trim(),
        website: getVal('website'),
        location: getVal('location'),
        phone: getVal('phone'),
        email: getVal('email'),
        status: getVal('status') || 'new',
        priority_tier: getVal('priority_tier') || 'low',
        notes: getVal('notes'),
      }};

      if (!fields.company_name) {{
        alert('Company name is required.');
        return;
      }}

      if (INTERACTIVE) {{
        await apiPost('/api/company/update', {{
          company_id: c.id,
          research_id: c.research_id,
          fields,
        }});
        editingCompanyId = null;
        return refreshDataFromServer();
      }}

      const cmd = 'python3 -c "from market_validation.research import update_company; print(update_company(\\'' + escSingle(c.id) + '\\',\\'' + escSingle(c.research_id) + '\\', {{\\'company_name\\':\\'' + escSingle(fields.company_name) + '\\',\\'website\\':\\'' + escSingle(fields.website) + '\\',\\'location\\':\\'' + escSingle(fields.location) + '\\',\\'phone\\':\\'' + escSingle(fields.phone) + '\\',\\'email\\':\\'' + escSingle(fields.email) + '\\',\\'status\\':\\'' + escSingle(fields.status) + '\\',\\'priority_tier\\':\\'' + escSingle(fields.priority_tier) + '\\',\\'notes\\':\\'' + escSingle(fields.notes) + '\\'}}))"';
      runCommandPrompt(cmd);
      editingCompanyId = null;
      renderCompanies();
    }}

    async function deleteCompany(companyId) {{
      const c = companyById(companyId);
      if (!c) return;
      if (!confirm('Delete this company?')) return;

      if (INTERACTIVE) {{
        await apiPost('/api/company/delete', {{ company_id: c.id, research_id: c.research_id }});
        return refreshDataFromServer();
      }}

      const cmd = 'python3 -c "from market_validation.research import delete_company; print(delete_company(\\'' + escSingle(c.id) + '\\',\\'' + escSingle(c.research_id) + '\\'))"';
      runCommandPrompt(cmd);
    }}

    async function approveEmail(id) {{
      if (!confirm('Send this queued email now?')) return;
      if (INTERACTIVE) {{
        await apiPost('/api/email/approve', {{ email_id: id }});
        return refreshDataFromServer();
      }}
      const cmd = 'python3 -c "from market_validation.email_sender import approve_email; print(approve_email(\\'' + id + '\\'))"';
      runCommandPrompt(cmd);
    }}

    function editEmail(id) {{
      // Open the draft modal pre-populated with the existing email so the user
      // can refine subject/body in-place (nicer than prompt() dialogs).
      const em = DATA.emails.find(e => e.id === id);
      if (!em) return;
      if (!INTERACTIVE) {{
        const cmd = 'python3 -c "from market_validation.email_sender import update_queued_email; print(update_queued_email(\\'' + escSingle(id) + '\\'))"';
        runCommandPrompt(cmd);
        return;
      }}
      openEmailModal(
        {{
          mode: 'edit',
          email_id: id,
          to_email: em.to_email,
          company_name: em.company_name,
          company_id: em.company_id,
          research_id: em.research_id,
        }},
        {{ company_name: em.company_name, to_email: em.to_email, subject: em.subject, body: em.body }},
      );
    }}

    async function deleteEmail(id) {{
      if (!confirm('Delete this pending email file?')) return;
      if (INTERACTIVE) {{
        await apiPost('/api/email/delete', {{ email_id: id }});
        return refreshDataFromServer();
      }}
      const cmd = 'rm output/email-queue/' + id + '.json';
      runCommandPrompt(cmd);
    }}

    async function addCompanyRow() {{
      let rid = selectedResearchId;
      if (!rid) {{
        rid = prompt('Research ID for new company:', '');
        if (!rid) return;
      }}

      const ref = DATA.researches.find((r) => r.id === rid);
      const marketDefault = ref ? (ref.market || '') : '';
      const company_name = prompt('Company name:', '');
      if (!company_name) return;
      const market = prompt('Market:', marketDefault) || marketDefault;
      const website = prompt('Website (optional):', '') || '';
      const location = prompt('Location (optional):', '') || '';
      const phone = prompt('Phone (optional):', '') || '';
      const email = prompt('Email (optional):', '') || '';
      const notes = prompt('Notes (optional):', '') || '';

      if (INTERACTIVE) {{
        await apiPost('/api/company/add', {{ research_id: rid, company_name, market: market || 'general', website, location, phone, email, notes }});
        return refreshDataFromServer();
      }}

      const cmd = 'python3 -c "from market_validation.research import add_company; print(add_company(research_id=\\'' + escSingle(rid) + '\\', company_name=\\'' + escSingle(company_name) + '\\', market=\\'' + escSingle(market || 'general') + '\\', website=\\'' + escSingle(website) + '\\', location=\\'' + escSingle(location) + '\\', phone=\\'' + escSingle(phone) + '\\', email=\\'' + escSingle(email) + '\\', notes=\\'' + escSingle(notes) + '\\'))"';
      runCommandPrompt(cmd);
    }}

    function exportCSV() {{
      const rows = filteredCompanies();
      if (!rows.length) {{ alert('No companies to export.'); return; }}
      const cols = ['company_name','website','location','phone','email','priority_score','priority_tier','status','volume_estimate','volume_unit','notes','research_name'];
      const headers = ['Company','Website','Location','Phone','Email','Score','Priority','Status','Volume','Volume Unit','Notes','Research'];
      const csvEsc = (v) => {{
        const s = String(v == null ? '' : v);
        return s.includes(',') || s.includes('"') || s.includes('\\n') ? '"' + s.replace(/"/g, '""') + '"' : s;
      }};
      let csv = headers.map(csvEsc).join(',') + '\\n';
      for (const r of rows) {{
        csv += cols.map(c => csvEsc(r[c])).join(',') + '\\n';
      }}
      const blob = new Blob([csv], {{ type: 'text/csv;charset=utf-8;' }});
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      const ref = selectedResearch();
      a.href = url;
      a.download = (ref ? ref.name.replace(/[^a-zA-Z0-9]/g, '_') : 'companies') + '.csv';
      a.click();
      URL.revokeObjectURL(url);
    }}

    function wire() {{
      const select = document.getElementById('researchSelect');
      select.addEventListener('change', (e) => setResearch(e.target.value));

      if (selectedResearchId) {{
        const exists = DATA.researches.some((r) => r.id === selectedResearchId);
        if (!exists) selectedResearchId = '';
      }}

      setResearchLabel();
      renderKpis();
      renderValidation();
      renderCompanies();
      renderEmails();
    }}

    wire();

    // Auto-sync Gmail on load then every 60 seconds
    if (INTERACTIVE) {{
      syncGmail();
      setInterval(syncGmail, 60000);
    }}
  </script>
</body>
</html>
"""


def generate_html(
    output_path: str | Path = "output/dashboard.html",
    open_browser: bool = True,
    interactive: bool = False,
) -> str:
    data = _load_data()
    researches = data["researches"]
    companies = data["companies"]
    emails = data["emails"]

    pending_count = sum(1 for e in emails if e.get("status") == "pending")
    sent_count = sum(1 for e in emails if e.get("status") in ("sent", "opened", "replied", "bounced"))
    replied_count = sum(1 for e in emails if e.get("replied_at"))
    qualified_count = sum(1 for c in companies if c.get("status") == "qualified")
    phone_count = sum(1 for c in companies if c.get("phone"))
    email_count = sum(1 for c in companies if c.get("email"))

    payload_json = json.dumps(data, ensure_ascii=True).replace("</", "<\\/")
    html = _html_template(interactive=interactive)
    html = (
        html.replace("__GENERATED_AT__", _escape_html(_iso_now()))
        .replace("__RESEARCH_COUNT__", str(len(researches)))
        .replace("__COMPANY_COUNT__", str(len(companies)))
        .replace("__QUALIFIED_COUNT__", str(qualified_count))
        .replace("__PHONE_COUNT__", str(phone_count))
        .replace("__EMAIL_COUNT__", str(email_count))
        .replace("__PENDING_COUNT__", str(pending_count))
        .replace("__SENT_COUNT__", str(sent_count))
        .replace("__REPLIED_COUNT__", str(replied_count))
        .replace("__RESEARCH_OPTIONS__", _render_research_options(researches))
        .replace("__PAYLOAD_JSON__", payload_json)
        .replace("__INTERACTIVE__", "true" if interactive else "false")
    )

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

    class Handler(BaseHTTPRequestHandler):
        def _json(self, payload: dict[str, Any], status: int = 200) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            from urllib.parse import parse_qs, unquote, urlparse as _up

            parsed = _up(self.path)
            path = parsed.path

            # Open-tracking pixel
            if path.startswith("/api/email/track/open/"):
                email_id = path.split("/api/email/track/open/", 1)[1].strip("/")
                record_open(email_id)
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

            return self._json({"result": "error", "error": "not found"}, 404)

        def do_POST(self):
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

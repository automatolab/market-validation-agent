"""
Dashboard HTML generator and optional local server.

Modes:
- Static HTML: generate_html()
- Interactive server with direct row CRUD: main --serve
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from market_validation.environment import load_project_env

load_project_env()


def _iso_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
                    SUM(CASE WHEN email IS NOT NULL AND TRIM(email) <> '' THEN 1 ELSE 0 END) as with_email
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
                r.name as research_name,
                r.last_source_health as last_source_health
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
                    "last_source_health": row[15],
                }
            )

    emails: list[dict[str, Any]] = []
    EMAIL_QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    for file in sorted(EMAIL_QUEUE_DIR.glob("*.json")):
        emails.append(json.loads(file.read_text()))

    return {
        "researches": researches,
        "companies": companies,
        "emails": emails,
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
  <title>Market Research Dashboard</title>
  <style>
    :root {{
      --bg: #f2f5f9;
      --surface: #ffffff;
      --text: #182433;
      --muted: #5e7083;
      --line: #d9e2eb;
      --brand: #0b5ca8;
      --ok: #1d7b3a;
      --warn: #996900;
      --radius: 14px;
      --shadow: 0 10px 30px rgba(24, 36, 51, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; color: var(--text); background: radial-gradient(1200px 500px at 15% -10%, #e6eef8, transparent), var(--bg); font: 15px/1.45 "Segoe UI", "Helvetica Neue", Arial, sans-serif; }}
    .app {{ max-width: 1520px; margin: 0 auto; padding: 24px; }}
    .header {{ display: flex; gap: 16px; justify-content: space-between; align-items: flex-end; margin-bottom: 18px; }}
    .title h1 {{ margin: 0; font-size: clamp(22px, 3vw, 34px); }}
    .title p {{ margin: 6px 0 0; color: var(--muted); }}
    .kpis {{ display: grid; grid-template-columns: repeat(4, minmax(90px, 1fr)); gap: 10px; width: min(520px, 100%); }}
    .kpi {{ background: var(--surface); border: 1px solid var(--line); border-radius: 12px; padding: 12px; box-shadow: var(--shadow); }}
    .kpi .v {{ font-size: 24px; font-weight: 700; }}
    .kpi .l {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; }}
    .panel {{ background: var(--surface); border: 1px solid var(--line); border-radius: var(--radius); box-shadow: var(--shadow); margin-bottom: 16px; overflow: hidden; }}
    .panel-head {{ padding: 14px 16px; border-bottom: 1px solid var(--line); display: flex; justify-content: space-between; align-items: center; }}
    .panel-head h2 {{ margin: 0; font-size: 17px; }}
    .panel-body {{ padding: 14px; }}
    .toolbar {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 10px; }}
    .select {{ min-width: 280px; border: 1px solid var(--line); border-radius: 10px; padding: 10px 12px; font: inherit; background: #fff; }}
    .count-pill {{ border: 1px solid var(--line); border-radius: 999px; padding: 8px 12px; font-size: 13px; color: #324457; background: #fff; }}
    .btn-link {{ display: inline-block; border: 1px solid var(--line); border-radius: 10px; padding: 8px 12px; background: #fff; color: var(--brand); text-decoration: none; font-size: 13px; }}
    .btn-link:hover {{ background: #f7fbff; }}
    .table-wrap {{ width: 100%; overflow: auto; border: 1px solid var(--line); border-radius: 12px; }}
    table {{ width: 100%; min-width: 860px; border-collapse: collapse; }}
    th, td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid #edf2f7; vertical-align: top; }}
    th {{ background: #f8fafc; font-size: 12px; color: #4b5f73; text-transform: uppercase; letter-spacing: 0.06em; position: sticky; top: 0; }}
    tr:hover td {{ background: #fbfdff; }}
    td a {{ color: var(--brand); text-decoration: none; }}
    td a:hover {{ text-decoration: underline; }}
    .muted {{ color: var(--muted); }}
    .empty {{ color: var(--muted); margin: 0; padding: 10px; }}
    .badge {{ display: inline-block; padding: 4px 8px; border-radius: 999px; font-size: 12px; border: 1px solid transparent; }}
    .priority-high {{ background: #e8f7ed; color: var(--ok); border-color: #c2e5cd; }}
    .priority-medium {{ background: #fff4de; color: var(--warn); border-color: #ecd6a7; }}
    .priority-low {{ background: #edf2f7; color: #4f6071; border-color: #dfe7ef; }}
    .status-pending {{ background: #fff4de; color: var(--warn); border-color: #ecd6a7; }}
    .status-sent {{ background: #e8f7ed; color: var(--ok); border-color: #c2e5cd; }}
    .status-opened {{ background: #e8f0fb; color: #1a56b0; border-color: #b3cfee; }}
    .status-clicked {{ background: #f0e8fb; color: #6b21a8; border-color: #d0b3ee; }}
    .status-replied {{ background: #e8f7ed; color: #1d7b3a; border-color: #c2e5cd; font-weight:600; }}
    .status-bounced {{ background: #fde8e8; color: #b91c1c; border-color: #f5c2c2; }}
    .status-date {{ margin-top: 3px; font-size: 12px; color: var(--muted); }}
    .preview-cell {{ font-size: 13px; min-width: 260px; }}
    .sent-content {{ color: var(--muted); white-space: pre-wrap; line-height: 1.5; }}
    .reply-label {{ font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: #1d7b3a; margin-bottom: 4px; }}
    .reply-content {{ white-space: pre-wrap; line-height: 1.5; color: var(--text); }}
    .action-link {{ margin-right: 10px; white-space: nowrap; }}
    .editing-row td {{ background: #fffdf4; }}
    .cell-input {{ width: 100%; border: 1px solid var(--line); border-radius: 8px; padding: 6px 8px; font: inherit; background: #fff; }}
    .cell-split {{ display: flex; gap: 6px; }}
    @media (max-width: 980px) {{
      .app {{ padding: 14px; }}
      .header {{ flex-direction: column; align-items: stretch; }}
      .kpis {{ grid-template-columns: repeat(3, minmax(100px, 1fr)); width: 100%; }}
      .select {{ min-width: 100%; }}
    }}
  </style>
</head>
<body>
  <div class='app'>
    <div class='header'>
      <div class='title'>
        <h1>Market Research Dashboard</h1>
        <p>Generated __GENERATED_AT__</p>
      </div>
      <div class='kpis'>
        <div class='kpi'><div class='v'>__RESEARCH_COUNT__</div><div class='l'>Research Projects</div></div>
        <div class='kpi'><div class='v'>__COMPANY_COUNT__</div><div class='l'>Companies</div></div>
        <div class='kpi'><div class='v'>__PENDING_COUNT__</div><div class='l'>Pending Emails</div></div>
        <div class='kpi'><div class='v'>__SENT_COUNT__</div><div class='l'>Sent Emails</div></div>
        <div class='kpi'><div class='v'>__OPENED_COUNT__</div><div class='l'>Opened</div></div>
        <div class='kpi'><div class='v'>__REPLIED_COUNT__</div><div class='l'>Replied</div></div>
        <div class='kpi'><div class='v'>__BOUNCED_COUNT__</div><div class='l'>Bounced</div></div>
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
    <section class='panel'>
      <div class='panel-head'>
        <h2 id='emailsTitle'>Email Queue</h2>
        <span id='syncStatus' class='count-pill' style='font-size:12px;color:var(--muted)'></span>
      </div>
      <div class='panel-body'>
        <div class='toolbar'><span id='emailCount' class='count-pill'>0 rows</span></div>
        <div id='emailsWrap' class='table-wrap'></div>
      </div>
    </section>
    <section class='panel'>
      <div class='panel-head'><h2 id='companiesTitle'>Companies</h2></div>
      <div class='panel-body'>
        <div class='toolbar'>
          <span id='companyCount' class='count-pill'>0 rows</span>
          <a class='btn-link' href='#' onclick='addCompanyRow(); return false;'>Add Company</a>
        </div>
        <div id='companiesWrap' class='table-wrap'></div>
      </div>
    </section>
  </div>

  <script id='dashboard-data' type='application/json'>__PAYLOAD_JSON__</script>
  <script>
    const DATA = JSON.parse(document.getElementById('dashboard-data').textContent);
    const params = new URLSearchParams(window.location.search);
    let selectedResearchId = params.get('research_id') || '';
    const INTERACTIVE = __INTERACTIVE__;
    let editingCompanyId = null;

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
      const rows = filteredCompanies();
      const current = selectedResearch();
      document.getElementById('companiesTitle').textContent = current ? `Companies - ${{current.name}}` : 'Companies';
      document.getElementById('companyCount').textContent = `${{rows.length}} rows`;

      if (!rows.length) {{
        document.getElementById('companiesWrap').innerHTML = '<p class="empty">No companies for this project/filter.</p>';
        return;
      }}

      let body = '';
      for (const c of rows) {{
        const isEditing = editingCompanyId === c.id;
        const email = c.email ? `<a href="mailto:${{esc(c.email)}}">${{esc(c.email)}}</a>` : '-';
        const phoneHref = c.phone ? String(c.phone).replace(/\\s+/g, '') : '';
        const phone = c.phone ? `<a href="tel:${{esc(phoneHref)}}">${{esc(c.phone)}}</a>` : '-';
        const volume = c.volume_estimate ? `${{esc(c.volume_estimate)}} ${{esc(c.volume_unit || '')}}` : '-';
        const notes = esc(c.notes || '').slice(0, 120);

        if (isEditing) {{
          const pri = (c.priority_tier || 'low').toLowerCase();
          const st = (c.status || 'new').toLowerCase();
          body += `
            <tr class="editing-row">
              <td><input class="cell-input" id="edit-company_name-${{esc(c.id)}}" value="${{esc(c.company_name || '')}}" /></td>
              <td><input class="cell-input" id="edit-location-${{esc(c.id)}}" value="${{esc(c.location || '')}}" /></td>
              <td><input class="cell-input" id="edit-phone-${{esc(c.id)}}" value="${{esc(c.phone || '')}}" /></td>
              <td><input class="cell-input" id="edit-email-${{esc(c.id)}}" value="${{esc(c.email || '')}}" /></td>
              <td>
                <div class="cell-split">
                  <input class="cell-input" id="edit-volume_estimate-${{esc(c.id)}}" value="${{esc(c.volume_estimate || '')}}" />
                  <input class="cell-input" id="edit-volume_unit-${{esc(c.id)}}" value="${{esc(c.volume_unit || '')}}" />
                </div>
              </td>
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

        body += `
          <tr>
            <td><strong>${{esc(c.company_name)}}</strong><div class="muted">${{esc(c.research_name || '')}}</div></td>
            <td>${{esc(c.location || '-')}}</td>
            <td>${{phone}}</td>
            <td>${{email}}</td>
            <td>${{volume}}</td>
            <td><span class="badge ${{priorityClass(c.priority_tier)}}">${{esc(c.priority_tier || 'low')}}</span></td>
            <td>${{esc(c.status || '-')}}</td>
            <td>
              ${{c.last_source_health ? `<a href="#" onclick="viewSourceHealth('${{esc(c.id)}}'); return false;">View</a>` : '<span class="muted">-</span>'}}
            </td>
            <td class="muted">${{notes || '-'}}</td>
            <td>
              <a class="action-link" href="#" onclick="startEditCompany('${{esc(c.id)}}'); return false;">Edit Row</a>
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
              <th>Location</th>
              <th>Phone</th>
              <th>Email</th>
              <th>Volume</th>
              <th>Priority</th>
              <th>Status</th>
              <th>Sources</th>
              <th>Notes</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>${{body}}</tbody>
        </table>
      `;
    }}

    function renderEmails() {{
      const rows = filteredEmails();
      const current = selectedResearch();
      document.getElementById('emailsTitle').textContent = current ? `Email Queue - ${{current.name}}` : 'Email Queue';
      document.getElementById('emailCount').textContent = `${{rows.length}} rows`;

      if (!rows.length) {{
        document.getElementById('emailsWrap').innerHTML = '<p class="empty">No emails for this project/filter.</p>';
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

    function viewSourceHealth(companyId) {{
      const c = companyById(companyId);
      if (!c) return;
      try {{
        const raw = c.last_source_health || null;
        const parsed = raw ? JSON.parse(raw) : null;
        const pretty = parsed ? JSON.stringify(parsed, null, 2) : 'No source health available';
        alert('Source health for ' + (c.company_name || '') + '\\n\\n' + pretty);
      }} catch (err) {{
        alert('Failed to parse source health: ' + err);
      }}
    }}

    function setResearch(id) {{
      selectedResearchId = id || '';
      const next = new URL(window.location.href);
      if (selectedResearchId) next.searchParams.set('research_id', selectedResearchId);
      else next.searchParams.delete('research_id');
      window.history.replaceState({{}}, '', next.toString());
      setResearchLabel();
      renderCompanies();
      renderEmails();
    }}

    function runCommandPrompt(cmd) {{
      prompt('Run this command in terminal:', cmd);
    }}

    async function refreshDataFromServer() {{
      if (!INTERACTIVE) return;
      const res = await fetch('/api/refresh');
      await res.json();
      window.location.reload();
    }}

    function startEditCompany(companyId) {{
      editingCompanyId = companyId;
      renderCompanies();
    }}

    function cancelEditCompany() {{
      editingCompanyId = null;
      renderCompanies();
    }}

    async function saveEditCompany(companyId) {{
      const c = companyById(companyId);
      if (!c) return;

      const getVal = (key) => {{
        const el = document.getElementById(`edit-${{key}}-${{companyId}}`);
        return el ? el.value : '';
      }};

      const fields = {{
        company_name: getVal('company_name').trim(),
        location: getVal('location'),
        phone: getVal('phone'),
        email: getVal('email'),
        status: getVal('status') || 'new',
        priority_tier: getVal('priority_tier') || 'low',
        notes: getVal('notes'),
        volume_estimate: getVal('volume_estimate'),
        volume_unit: getVal('volume_unit'),
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

      const cmd = 'python3 -c "from market_validation.research import update_company; print(update_company(\\'' + escSingle(c.id) + '\\',\\'' + escSingle(c.research_id) + '\\', {{\\'company_name\\':\\'' + escSingle(fields.company_name) + '\\',\\'location\\':\\'' + escSingle(fields.location) + '\\',\\'phone\\':\\'' + escSingle(fields.phone) + '\\',\\'email\\':\\'' + escSingle(fields.email) + '\\',\\'status\\':\\'' + escSingle(fields.status) + '\\',\\'priority_tier\\':\\'' + escSingle(fields.priority_tier) + '\\',\\'notes\\':\\'' + escSingle(fields.notes) + '\\',\\'volume_estimate\\':\\'' + escSingle(fields.volume_estimate) + '\\',\\'volume_unit\\':\\'' + escSingle(fields.volume_unit) + '\\'}}))"';
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

    async function editEmail(id) {{
      const body = prompt('Enter new email body:');
      if (body === null) return;
      const subject = prompt('Enter new subject (optional):', '');
      if (subject === null) return;

      if (INTERACTIVE) {{
        await apiPost('/api/email/update', {{ email_id: id, subject, body }});
        return refreshDataFromServer();
      }}

      const cmd = 'python3 -c "from market_validation.email_sender import update_queued_email; print(update_queued_email(\\'' + escSingle(id) + '\\', subject=\\'' + escSingle(subject) + '\\', body=\\'' + escSingle(body) + '\\'))"';
      runCommandPrompt(cmd);
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

    function setResearch(id) {{
      selectedResearchId = id || '';
      const next = new URL(window.location.href);
      if (selectedResearchId) next.searchParams.set('research_id', selectedResearchId);
      else next.searchParams.delete('research_id');
      window.history.replaceState({{}}, '', next.toString());
      setResearchLabel();
      renderCompanies();
      renderEmails();
    }}

    function wire() {{
      const select = document.getElementById('researchSelect');
      select.addEventListener('change', (e) => setResearch(e.target.value));

      if (selectedResearchId) {{
        const exists = DATA.researches.some((r) => r.id === selectedResearchId);
        if (!exists) selectedResearchId = '';
      }}

      setResearchLabel();
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
    opened_count = sum(1 for e in emails if e.get("opened_at"))
    replied_count = sum(1 for e in emails if e.get("replied_at"))
    bounced_count = sum(1 for e in emails if e.get("bounced_at"))

    payload_json = json.dumps(data, ensure_ascii=True).replace("</", "<\\/")
    html = _html_template(interactive=interactive)
    html = (
        html.replace("__GENERATED_AT__", _escape_html(_iso_now()))
        .replace("__RESEARCH_COUNT__", str(len(researches)))
        .replace("__COMPANY_COUNT__", str(len(companies)))
        .replace("__PENDING_COUNT__", str(pending_count))
        .replace("__SENT_COUNT__", str(sent_count))
        .replace("__OPENED_COUNT__", str(opened_count))
        .replace("__REPLIED_COUNT__", str(replied_count))
        .replace("__BOUNCED_COUNT__", str(bounced_count))
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

    from market_validation.email_sender import approve_email, delete_email, update_queued_email
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
                self.end_headers()
                self.wfile.write(data)
                return

            if path == "/api/refresh":
                generate_html(open_browser=False, interactive=True)
                return self._json({"result": "ok"})

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

            except Exception as exc:
                return self._json({"result": "error", "error": str(exc)}, 400)

            return self._json({"result": "error", "error": "not found"}, 404)

        def log_message(self, format, *args):
            return

    return Handler


def serve_dashboard(host: str = "127.0.0.1", port: int = 8787, open_browser: bool = True) -> str:
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
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--no-open", action="store_true")
    args = parser.parse_args()

    if args.static:
        path = generate_html(open_browser=not args.no_open, interactive=False)
        print(f"Dashboard generated: {path}")
        return

    serve_dashboard(host=args.host, port=args.port, open_browser=not args.no_open)


if __name__ == "__main__":
    main()

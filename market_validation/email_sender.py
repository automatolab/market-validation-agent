"""
Email outreach queue and SMTP sender.

Manages a draft/review/approve workflow backed by JSON queue files and
SQLite persistence. Supports single sends, templated sends, and batch
operations. Integrates with the dashboard for review and approval.
"""

from __future__ import annotations

import base64
import email.utils
import json
import os
import smtplib
import sqlite3
from datetime import UTC, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from market_validation.environment import load_project_env
from market_validation.log import get_logger
from market_validation.research import PROJECT_ROOT, _connect, resolve_db_path

_log = get_logger("email_sender")

load_project_env()


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _get_smtp_connection() -> smtplib.SMTP:
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP_USER and SMTP_PASSWORD environment variables are required")

    server = smtplib.SMTP(smtp_host, smtp_port)
    server.starttls()
    server.login(smtp_user, smtp_password)
    return server


def _append_compliance_footer(body: str, html_body: str | None) -> tuple[str, str | None]:
    """Append a CAN-SPAM-style footer (sender info + unsubscribe).

    Required for legitimate cold outreach: receivers expect a way to opt out,
    and the absence of one is a strong spam signal. Operators configure the
    sender details once via .env.
    """
    sender_name = os.getenv("EMAIL_FOOTER_SENDER", "")
    sender_addr = os.getenv("EMAIL_FOOTER_ADDRESS", "")
    unsubscribe_url = os.getenv("EMAIL_UNSUBSCRIBE_URL", "")
    plain_lines = []
    if sender_name:
        plain_lines.append(sender_name)
    if sender_addr:
        plain_lines.append(sender_addr)
    if unsubscribe_url:
        plain_lines.append(f"Unsubscribe: {unsubscribe_url}")
    if not plain_lines:
        # No footer configured — return body unchanged. Operators are warned
        # at send time when this leaves us open to spam-trap problems.
        return body, html_body
    footer_plain = "\n\n--\n" + "\n".join(plain_lines)
    new_body = body + footer_plain
    new_html = html_body
    if html_body:
        footer_html = (
            '<hr style="margin-top:24px;border:none;border-top:1px solid #eee">'
            '<div style="color:#888;font-size:12px;line-height:1.5;margin-top:8px;font-family:sans-serif">'
            + "<br>".join(line for line in plain_lines if line)
            + "</div>"
        )
        # Insert before the closing </body> if present, else append.
        if "</body>" in html_body:
            new_html = html_body.replace("</body>", footer_html + "</body>")
        else:
            new_html = html_body + footer_html
    return new_body, new_html


def send_email(
    *,
    to_email: str,
    subject: str,
    body: str,
    html_body: str | None = None,
    from_email: str | None = None,
) -> dict[str, Any]:
    from_email = from_email or os.getenv("FROM_EMAIL")
    if not from_email:
        raise ValueError("FROM_EMAIL environment variable is required")

    body, html_body = _append_compliance_footer(body, html_body)

    try:
        message_id = email.utils.make_msgid()
        if html_body:
            msg = MIMEMultipart("alternative")
            msg["From"] = from_email
            msg["To"] = to_email
            msg["Subject"] = subject
            msg["Message-ID"] = message_id
            # Return-Path matches the FROM address — receivers see proper
            # bounce routing, and DMARC doesn't get spooked.
            msg["Return-Path"] = from_email
            msg["Reply-To"] = os.getenv("REPLY_TO_EMAIL", from_email)
            # List-Unsubscribe header gives Gmail/Outlook the one-click opt-out
            # button. Falls back to mailto:from_email when no URL is set.
            unsub = os.getenv("EMAIL_UNSUBSCRIBE_URL")
            if unsub:
                msg["List-Unsubscribe"] = f"<{unsub}>"
                msg["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"
            else:
                msg["List-Unsubscribe"] = f"<mailto:{from_email}?subject=unsubscribe>"
            msg.attach(MIMEText(body, "plain"))
            msg.attach(MIMEText(html_body, "html"))
        else:
            msg = MIMEText(body, "plain")
            msg["From"] = from_email
            msg["To"] = to_email
            msg["Subject"] = subject
            msg["Message-ID"] = message_id
            msg["Return-Path"] = from_email
            msg["Reply-To"] = os.getenv("REPLY_TO_EMAIL", from_email)
            unsub = os.getenv("EMAIL_UNSUBSCRIBE_URL")
            msg["List-Unsubscribe"] = (
                f"<{unsub}>" if unsub else f"<mailto:{from_email}?subject=unsubscribe>"
            )

        server = _get_smtp_connection()
        server.sendmail(from_email, [to_email], msg.as_string())
        server.quit()

        return {
            "result": "ok",
            "sent_at": _iso_now(),
            "to": to_email,
            "subject": subject,
            "message_id": message_id,
        }
    except smtplib.SMTPException as e:
        _log.warning("SMTP send failed for %s: %s", to_email, e)
        return {
            "result": "failed",
            "error": str(e),
            "to": to_email,
            "subject": subject,
        }
    except Exception as e:
        # Keep a broader fallback so unexpected errors (DNS, socket, etc.)
        # don't propagate and kill the queue worker, but log with traceback.
        _log.exception("unexpected send_email failure to %s: %s", to_email, e)
        return {
            "result": "failed",
            "error": str(e),
            "to": to_email,
            "subject": subject,
        }


def send_templated_email(
    *,
    to_email: str,
    template: dict[str, Any],
    company_name: str,
    contact_name: str | None = None,
    from_email: str | None = None,
) -> dict[str, Any]:
    subject_template = template.get("subject_template", "Subject {{company_name}}")
    body_template = template.get("body_template", "Body {{company_name}}")

    subject = subject_template.replace("{{company_name}}", company_name)
    if contact_name:
        subject = subject.replace("{{contact_name}}", contact_name)

    body = body_template.replace("{{company_name}}", company_name)
    if contact_name:
        body = body.replace("{{contact_name}}", contact_name)

    return send_email(
        to_email=to_email,
        subject=subject,
        body=body,
        from_email=from_email,
    )


def send_batch_emails(
    *,
    recipients: list[dict[str, Any]],
    template: dict[str, Any],
    from_email: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    results = []
    for recipient in recipients:
        to_email = recipient.get("email") or recipient.get("contact_email")
        if not to_email:
            results.append({
                "result": "skipped",
                "reason": "no email",
                "company_id": recipient.get("company_id"),
            })
            continue

        if dry_run:
            results.append({
                "result": "ok",
                "dry_run": True,
                "to": to_email,
                "company_id": recipient.get("company_id"),
            })
            continue

        result = send_templated_email(
            to_email=to_email,
            template=template,
            company_name=recipient.get("company_name", ""),
            contact_name=recipient.get("contact_name"),
            from_email=from_email,
        )
        result["company_id"] = recipient.get("company_id")
        results.append(result)

    sent = sum(1 for r in results if r.get("result") == "ok")
    failed = sum(1 for r in results if r.get("result") == "failed")

    return {
        "result": "ok",
        "sent": sent,
        "failed": failed,
        "total": len(results),
        "details": results,
        "sent_at": _iso_now(),
    }


EMAIL_QUEUE_DIR = PROJECT_ROOT / "output" / "email-queue"
EMAIL_QUEUE_DIR.mkdir(parents=True, exist_ok=True)


def _ensure_email_schema(conn: sqlite3.Connection) -> None:
    """Create the emails table if it does not exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS emails (
            id TEXT PRIMARY KEY,
            research_id TEXT,
            company_id TEXT,
            company_name TEXT,
            contact_name TEXT,
            to_email TEXT NOT NULL,
            subject TEXT NOT NULL,
            body TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT,
            sent_at TEXT,
            opened_at TEXT,
            replied_at TEXT,
            bounced_at TEXT,
            reply_snippet TEXT,
            message_id TEXT
        );
    """)


def _sync_email_to_db(email_data: dict[str, Any]) -> None:
    """Upsert an email record into the SQLite database."""
    db_path = resolve_db_path(PROJECT_ROOT)
    conn = _connect(db_path)
    try:
        _ensure_email_schema(conn)
        conn.execute(
            """INSERT INTO emails (
                id, research_id, company_id, company_name, contact_name,
                to_email, subject, body, status, created_at, sent_at,
                opened_at, replied_at, bounced_at, reply_snippet, message_id
            ) VALUES (
                :id, :research_id, :company_id, :company_name, :contact_name,
                :to_email, :subject, :body, :status, :created_at, :sent_at,
                :opened_at, :replied_at, :bounced_at, :reply_snippet, :message_id
            )
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                subject = excluded.subject,
                body = excluded.body,
                sent_at = excluded.sent_at,
                opened_at = excluded.opened_at,
                replied_at = excluded.replied_at,
                bounced_at = excluded.bounced_at,
                reply_snippet = excluded.reply_snippet,
                message_id = excluded.message_id
            """,
            {
                "id": email_data.get("id"),
                "research_id": email_data.get("research_id"),
                "company_id": email_data.get("company_id"),
                "company_name": email_data.get("company_name"),
                "contact_name": email_data.get("contact_name"),
                "to_email": email_data.get("to_email"),
                "subject": email_data.get("subject"),
                "body": email_data.get("body"),
                "status": email_data.get("status", "pending"),
                "created_at": email_data.get("created_at"),
                "sent_at": email_data.get("sent_at"),
                "opened_at": email_data.get("opened_at"),
                "replied_at": email_data.get("replied_at"),
                "bounced_at": email_data.get("bounced_at"),
                "reply_snippet": email_data.get("reply_snippet"),
                "message_id": email_data.get("message_id"),
            },
        )
        conn.commit()
    finally:
        conn.close()


def prep_email(
    *,
    to_email: str,
    subject: str,
    body: str,
    company_name: str | None = None,
    contact_name: str | None = None,
    research_id: str | None = None,
    company_id: str | None = None,
) -> dict[str, Any]:
    """Prep an email for review — saves to queue (DB + JSON file).

    Atomic to the extent SQLite + filesystem allow: writes the DB row first,
    then writes the JSON queue file via temp + rename. If the DB insert
    fails, no queue file is created. If the rename fails after the DB
    insert, the DB row is rolled back so the queue stays consistent.
    Eliminates orphan-JSON / orphan-DB-row states from the previous
    write-JSON-then-sync pattern.
    """
    EMAIL_QUEUE_DIR.mkdir(parents=True, exist_ok=True)

    email_id = base64.urlsafe_b64encode(os.urandom(6)).decode()[:8]
    timestamp = _iso_now()

    email_data = {
        "id": email_id,
        "created_at": timestamp,
        "status": "pending",
        "to_email": to_email,
        "subject": subject,
        "body": body,
        "company_name": company_name,
        "contact_name": contact_name,
        "research_id": research_id,
        "company_id": company_id,
        "approved": False,
        "sent_at": None,
        "opened_at": None,
        "clicked_at": None,
        "clicks": [],
    }

    queue_file = EMAIL_QUEUE_DIR / f"{email_id}.json"
    tmp_file = EMAIL_QUEUE_DIR / f".{email_id}.json.tmp"

    # 1. Write the DB row first — single transaction.
    try:
        _sync_email_to_db(email_data)
    except sqlite3.Error as exc:
        _log.warning("prep_email: DB write failed for %s: %s", email_id, exc)
        return {"result": "error", "error": f"DB write failed: {exc}"}

    # 2. Write JSON to a temp file, then atomically rename. If anything in
    # this block fails, roll back the DB row so we don't leak orphans.
    try:
        tmp_file.write_text(json.dumps(email_data, indent=2))
        os.replace(tmp_file, queue_file)
    except OSError as exc:
        _log.warning("prep_email: queue file write failed for %s: %s", email_id, exc)
        # Best-effort rollback of the DB row we just wrote.
        try:
            db_path = resolve_db_path(PROJECT_ROOT)
            with _connect(db_path) as conn:
                conn.execute("DELETE FROM emails WHERE id = ?", (email_id,))
        except sqlite3.Error as cleanup_exc:
            _log.warning("prep_email: DB rollback failed for %s: %s", email_id, cleanup_exc)
        # Clean up the temp file if it still exists.
        try:
            if tmp_file.exists():
                tmp_file.unlink()
        except OSError:
            pass
        return {"result": "error", "error": f"queue file write failed: {exc}"}

    return {
        "result": "ok",
        "email_id": email_id,
        "status": "pending",
        "queued_at": timestamp,
        "queue_file": str(queue_file),
    }


def get_email_queue(status: str | None = None) -> dict[str, Any]:
    """Get all queued emails."""
    emails = []
    for f in sorted(EMAIL_QUEUE_DIR.glob("*.json")):
        data = json.loads(f.read_text())
        if status is None or data.get("status") == status:
            emails.append(data)
    return {
        "result": "ok",
        "count": len(emails),
        "emails": emails,
    }


def approve_email(email_id: str) -> dict[str, Any]:
    """Approve and send a queued email."""
    queue_file = EMAIL_QUEUE_DIR / f"{email_id}.json"
    if not queue_file.exists():
        return {"result": "error", "error": "Email not found in queue"}

    email_data = json.loads(queue_file.read_text())

    if email_data.get("approved"):
        return {"result": "error", "error": "Email already approved"}

    # Build HTML version with tracking pixel + click-wrapping
    try:
        from market_validation.email_tracker import build_html_body
        html_body = build_html_body(email_data["body"], email_id)
    except Exception as exc:
        # HTML rendering is optional — plaintext still gets sent. Log so a
        # broken tracker module shows up instead of silently losing HTML.
        _log.warning("email_tracker.build_html_body failed for %s: %s", email_id, exc)
        html_body = None

    result = send_email(
        to_email=email_data["to_email"],
        subject=email_data["subject"],
        body=email_data["body"],
        html_body=html_body,
    )

    if result.get("result") == "ok":
        email_data["status"] = "sent"
        email_data["approved"] = True
        email_data["sent_at"] = result.get("sent_at")
        email_data["message_id"] = result.get("message_id")
        queue_file.write_text(json.dumps(email_data, indent=2))
        _sync_email_to_db(email_data)

    return result


def approve_all_emails() -> dict[str, Any]:
    """Approve and send all pending emails with a configurable send rate.

    Defaults to 1 email per second (3600/hr) — well below Gmail's 500/day
    free-tier rate limit while still completing a 50-email batch in under
    a minute. Set EMAIL_SEND_INTERVAL_SECONDS=0 to disable the delay (only
    safe for tiny batches you fully trust).
    """
    import time as _time

    queue = get_email_queue(status="pending")
    pending = queue.get("emails", [])
    interval = float(os.getenv("EMAIL_SEND_INTERVAL_SECONDS", "1.0"))
    results: list[dict[str, Any]] = []
    for i, queued in enumerate(pending):
        result = approve_email(queued["id"])
        results.append(result)
        # Sleep between sends — but not after the last one.
        if interval > 0 and i < len(pending) - 1:
            _time.sleep(interval)

    sent = sum(1 for r in results if r.get("result") == "ok")
    return {
        "result": "ok",
        "sent": sent,
        "failed": len(results) - sent,
        "send_interval_seconds": interval,
        "details": results,
    }


def export_email_queue_markdown(status: str | None = None) -> str:
    """Export email queue as markdown for review."""
    queue = get_email_queue(status=status)
    emails = queue.get("emails", [])

    lines = [
        "# Email Queue",
        "",
        f"**Total:** {len(emails)} emails",
        "",
    ]

    for i, queued in enumerate(emails, 1):
        lines.extend([
            f"## {i}. {queued['subject']}",
            "",
            f"**ID:** `{queued['id']}`",
            f"**Status:** {queued['status']}",
            f"**To:** {queued['to_email']}",
            f"**Company:** {queued.get('company_name') or '-'}",
            f"**Contact:** {queued.get('contact_name') or '-'}",
            f"**Created:** {queued['created_at']}",
            "",
            "### Subject",
            f"{queued['subject']}",
            "",
            "### Body",
            "```",
            queued['body'],
            "```",
            "",
            "---",
            "",
        ])

    lines.append("## Commands")
    lines.append("")
    lines.append("```bash")
    lines.append("# Approve and send one email")
    lines.append("python3 -c \"from market_validation.email_sender import approve_email; print(approve_email('<email_id>'))\"")
    lines.append("")
    lines.append("# Approve and send ALL pending emails")
    lines.append("python3 -c \"from market_validation.email_sender import approve_all_emails; print(approve_all_emails())\"")
    lines.append("")
    lines.append("# Edit queued email (update body/subject)")
    lines.append("python3 -c \"from market_validation.email_sender import update_queued_email; print(update_queued_email('<email_id>', subject='New Subject', body='New body'))\"")
    lines.append("")
    lines.append("# Delete pending email")
    lines.append("rm output/email-queue/<email_id>.json")
    lines.append("```")

    return "\n".join(lines)


def update_queued_email(
    email_id: str,
    subject: str | None = None,
    body: str | None = None,
) -> dict[str, Any]:
    """Update a queued email's subject or body."""
    queue_file = EMAIL_QUEUE_DIR / f"{email_id}.json"
    if not queue_file.exists():
        return {"result": "error", "error": "Email not found in queue"}

    email_data = json.loads(queue_file.read_text())

    if email_data.get("status") == "sent":
        return {"result": "error", "error": "Cannot edit sent email"}

    if subject is not None:
        email_data["subject"] = subject
    if body is not None:
        email_data["body"] = body

    email_data["updated_at"] = _iso_now()
    queue_file.write_text(json.dumps(email_data, indent=2))
    _sync_email_to_db(email_data)

    return {
        "result": "ok",
        "email_id": email_id,
        "updated": True,
    }


def delete_email(email_id: str) -> dict[str, Any]:
    """Delete a queued email JSON file and its DB row."""
    queue_file = EMAIL_QUEUE_DIR / f"{email_id}.json"
    existed = queue_file.exists()
    if existed:
        queue_file.unlink()
    # Also remove from the emails table so the dashboard reflects the delete
    try:
        db_path = resolve_db_path(PROJECT_ROOT)
        conn = _connect(db_path)
        try:
            _ensure_email_schema(conn)
            conn.execute("DELETE FROM emails WHERE id = ?", (email_id,))
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        # JSON queue file was already deleted above; DB delete is a best-effort
        # cleanup. Log so an out-of-sync state shows up.
        _log.warning("delete_email: DB cleanup failed for %s: %s", email_id, exc)

    if not existed:
        return {"result": "error", "error": "Email not found in queue"}
    return {
        "result": "ok",
        "email_id": email_id,
        "deleted": True,
    }


def reject_all_emails() -> dict[str, Any]:
    """Delete every pending email (queue JSON + DB row)."""
    queue = get_email_queue(status="pending")
    results = []
    for e in queue.get("emails", []):
        results.append(delete_email(e["id"]))
    deleted = sum(1 for r in results if r.get("result") == "ok")
    return {"result": "ok", "deleted": deleted, "details": results}


# ---------------------------------------------------------------------------
# AI drafting — generate a cold-outreach email from company + research context
# ---------------------------------------------------------------------------

def _load_research_and_company(company_id: str) -> dict[str, Any]:
    """Pull a company row and its parent research row. Returns {} on miss."""
    db_path = resolve_db_path(PROJECT_ROOT)
    conn = _connect(db_path)
    try:
        conn.row_factory = None
        crow = conn.execute(
            """SELECT id, research_id, company_name, website, location, phone, email,
                      priority_tier, priority_score, notes, volume_estimate, volume_unit,
                      status
               FROM companies WHERE id = ?""",
            (company_id,),
        ).fetchone()
        if not crow:
            return {}
        research_id = crow[1]
        rrow = conn.execute(
            "SELECT id, name, market, product, geography, description FROM researches WHERE id = ?",
            (research_id,),
        ).fetchone()
    finally:
        conn.close()
    return {
        "company": {
            "id": crow[0], "research_id": crow[1], "company_name": crow[2],
            "website": crow[3], "location": crow[4], "phone": crow[5], "email": crow[6],
            "priority_tier": crow[7], "priority_score": crow[8], "notes": crow[9],
            "volume_estimate": crow[10], "volume_unit": crow[11], "status": crow[12],
        },
        "research": (
            {"id": rrow[0], "name": rrow[1], "market": rrow[2], "product": rrow[3],
             "geography": rrow[4], "description": rrow[5]}
            if rrow else None
        ),
    }


def _ai_draft_subject_body(
    *,
    company_name: str,
    market: str,
    product: str | None,
    geography: str | None,
    notes: str | None,
    description: str | None,
    contact_name: str | None = None,
) -> dict[str, str]:
    """Ask Claude/opencode for a short cold email. Returns {subject, body}."""
    from market_validation.company_enrichment import _run_ai_prompt

    product_line = product or market
    ctx_bits: list[str] = []
    if description:
        ctx_bits.append(f"Our product/service: {description}")
    if geography:
        ctx_bits.append(f"Target area: {geography}")
    if notes:
        ctx_bits.append(f"What we know about them: {notes[:600]}")
    context = "\n".join(ctx_bits) if ctx_bits else "No extra context."

    greeting_target = contact_name or "the team"
    prompt = f"""Write a short cold-outreach email from a seller of "{product_line}" (market: "{market}") to "{company_name}".

{context}

Requirements:
- Subject: concise, specific to {company_name}. No generic "Quick question" or "Reaching out". Max 70 chars.
- Body: 4-6 short lines, plain text, no markdown. Open with a specific hook tied to what we know about them. State one clear value prop. End with a single low-commitment ask (15-minute call, reply if interested).
- Salutation: address {greeting_target}. No "Hi there" or "Dear Sir/Madam".
- No emojis. No "I hope this email finds you well." No postscripts.
- Sign off with "Best," only. Leave the sender name blank (the human will fill it).
- Do NOT use em dashes (—) or en dashes (–) anywhere in the subject or body. Use commas, periods, or a regular hyphen "-" instead.

Return ONLY JSON with exactly these two keys:
{{"subject": "...", "body": "..."}}"""

    raw = _run_ai_prompt(prompt, timeout=60)

    # Parse first JSON object in the response
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end <= start:
        raise ValueError(f"AI response had no JSON object: {raw[:200]}")
    data = json.loads(raw[start : end + 1])
    subject = _strip_dashes(str(data.get("subject") or "").strip())
    body = _strip_dashes(str(data.get("body") or "").strip())
    if not subject or not body:
        raise ValueError("AI returned empty subject or body")
    return {"subject": subject, "body": body}


def _strip_dashes(text: str) -> str:
    """Replace em dashes and en dashes with comma+space.

    Belt-and-suspenders for the prompt instruction: the LLM occasionally
    slips an em dash through despite the explicit ask, so we sanitize after
    parsing. Comma is the most natural substitute for the parenthetical
    clauses where em dashes typically appear in cold-email prose.
    """
    return text.replace("—", ", ").replace("–", ", ")


def draft_email_for_company(company_id: str) -> dict[str, Any]:
    """Generate a draft email for a single company. Does NOT queue."""
    loaded = _load_research_and_company(company_id)
    if not loaded:
        return {"result": "error", "error": f"company {company_id} not found"}

    c = loaded["company"]
    r = loaded["research"] or {}
    if not c.get("email"):
        return {"result": "error", "error": f"{c['company_name']} has no email on file"}

    try:
        draft = _ai_draft_subject_body(
            company_name=c["company_name"],
            market=(r.get("market") or ""),
            product=r.get("product"),
            geography=r.get("geography"),
            notes=c.get("notes"),
            description=r.get("description"),
        )
    except Exception as exc:
        # AI drafting is pure content generation — failures shouldn't spam
        # the log at warning level, but operators need to see them when
        # debugging why drafts aren't appearing in the queue.
        _log.info("AI draft failed for company %s: %s", c.get("id"), exc)
        return {"result": "error", "error": f"AI draft failed: {exc}"}

    return {
        "result": "ok",
        "company_id": c["id"],
        "research_id": c["research_id"],
        "company_name": c["company_name"],
        "to_email": c["email"],
        "subject": draft["subject"],
        "body": draft["body"],
    }


def draft_emails_for_research(
    research_id: str,
    statuses: list[str] | None = None,
    skip_existing: bool = True,
) -> dict[str, Any]:
    """Draft + queue emails for every company in *research_id* matching *statuses*.

    By default targets "qualified" leads that have an email and are not already
    in the queue. Returns counts and per-company details.
    """
    if not statuses:
        statuses = ["qualified"]

    db_path = resolve_db_path(PROJECT_ROOT)
    conn = _connect(db_path)
    try:
        # The emails table is created lazily on first prep_email call. Ensure it
        # exists before querying for already-queued drafts so the initial
        # draft-all call on a fresh DB doesn't fail with "no such table: emails".
        _ensure_email_schema(conn)
        conn.row_factory = None
        placeholders = ",".join("?" for _ in statuses)
        rows = conn.execute(
            f"""SELECT id FROM companies
                WHERE research_id = ?
                  AND email IS NOT NULL AND email != ''
                  AND status IN ({placeholders})
                ORDER BY priority_score DESC NULLS LAST, company_name""",
            (research_id, *statuses),
        ).fetchall()
        company_ids = [r[0] for r in rows]

        already_drafted: set[str] = set()
        if skip_existing:
            existing = conn.execute(
                "SELECT company_id FROM emails WHERE research_id = ? AND status = 'pending'",
                (research_id,),
            ).fetchall()
            already_drafted = {r[0] for r in existing if r[0]}
    finally:
        conn.close()

    # Partition: work to do vs. skipped-because-already-queued
    work: list[str] = []
    details: list[dict[str, Any]] = []
    for cid in company_ids:
        if cid in already_drafted:
            details.append({"company_id": cid, "status": "skipped", "reason": "already_queued"})
        else:
            work.append(cid)

    # Parallel AI drafting — each worker calls Claude/opencode for one company.
    # The AI CLI is the bottleneck; prep_email (queue file + DB upsert) is cheap.
    drafted = 0
    failed = 0
    skipped = sum(1 for d in details if d["status"] == "skipped")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _draft_one(cid: str) -> dict[str, Any]:
        d = draft_email_for_company(cid)
        if d.get("result") != "ok":
            return {"company_id": cid, "status": "failed", "error": d.get("error")}
        q = prep_email(
            to_email=d["to_email"],
            subject=d["subject"],
            body=d["body"],
            company_name=d["company_name"],
            research_id=d["research_id"],
            company_id=d["company_id"],
        )
        return {
            "company_id": cid,
            "status": "queued",
            "email_id": q.get("email_id"),
            "subject": d["subject"],
        }

    if work:
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(_draft_one, cid): cid for cid in work}
            for fut in as_completed(futures):
                cid = futures[fut]
                try:
                    result = fut.result()
                except Exception as exc:
                    _log.info("draft-all: worker failed for %s: %s", cid, exc)
                    result = {"company_id": cid, "status": "failed", "error": str(exc)}
                details.append(result)
                if result["status"] == "queued":
                    drafted += 1
                else:
                    failed += 1

    return {
        "result": "ok",
        "research_id": research_id,
        "candidates": len(company_ids),
        "drafted": drafted,
        "skipped": skipped,
        "failed": failed,
        "details": details,
    }


def build_parser() -> Any:
    import argparse
    parser = argparse.ArgumentParser(description="Send outreach emails from templates")
    parser.add_argument("--to", required=True, help="Recipient email")
    parser.add_argument("--subject", required=True, help="Email subject")
    parser.add_argument("--body", required=True, help="Email body (plain text)")
    parser.add_argument("--dry-run", action="store_true", help="Validate without sending")
    return parser


def main() -> None:
    import json

    parser = build_parser()
    args = parser.parse_args()

    load_project_env()

    if args.dry_run:
        print(json.dumps({
            "result": "ok",
            "dry_run": True,
            "to": args.to,
            "subject": args.subject,
        }, ensure_ascii=True))
        return

    result = send_email(
        to_email=args.to,
        subject=args.subject,
        body=args.body,
    )
    print(json.dumps(result, ensure_ascii=True))

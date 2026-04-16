from __future__ import annotations

import base64
import email.utils
import json
import os
import smtplib
import sqlite3
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

from market_validation.environment import load_project_env
from market_validation.research import PROJECT_ROOT, _connect, resolve_db_path

load_project_env()


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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

    try:
        message_id = email.utils.make_msgid()
        if html_body:
            msg = MIMEMultipart("alternative")
            msg["From"] = from_email
            msg["To"] = to_email
            msg["Subject"] = subject
            msg["Message-ID"] = message_id
            msg.attach(MIMEText(body, "plain"))
            msg.attach(MIMEText(html_body, "html"))
        else:
            msg = MIMEText(body, "plain")
            msg["From"] = from_email
            msg["To"] = to_email
            msg["Subject"] = subject
            msg["Message-ID"] = message_id

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
    except Exception as e:
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
    """Prep an email for review - saves to queue instead of sending."""
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
    queue_file.write_text(json.dumps(email_data, indent=2))
    _sync_email_to_db(email_data)

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
    except Exception:
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
    """Approve and send all pending emails."""
    queue = get_email_queue(status="pending")
    results = []
    for email in queue.get("emails", []):
        result = approve_email(email["id"])
        results.append(result)

    sent = sum(1 for r in results if r.get("result") == "ok")
    return {
        "result": "ok",
        "sent": sent,
        "failed": len(results) - sent,
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

    for i, email in enumerate(emails, 1):
        lines.extend([
            f"## {i}. {email['subject']}",
            "",
            f"**ID:** `{email['id']}`",
            f"**Status:** {email['status']}",
            f"**To:** {email['to_email']}",
            f"**Company:** {email.get('company_name') or '-'}",
            f"**Contact:** {email.get('contact_name') or '-'}",
            f"**Created:** {email['created_at']}",
            "",
            "### Subject",
            f"{email['subject']}",
            "",
            "### Body",
            "```",
            email['body'],
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
    """Delete a queued email JSON file."""
    queue_file = EMAIL_QUEUE_DIR / f"{email_id}.json"
    if not queue_file.exists():
        return {"result": "error", "error": "Email not found in queue"}

    queue_file.unlink()
    return {
        "result": "ok",
        "email_id": email_id,
        "deleted": True,
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

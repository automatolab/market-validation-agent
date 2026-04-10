from __future__ import annotations

import base64
import json
import os
import smtplib
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

from market_validation.environment import load_project_env


def _iso_now() -> str:
    from datetime import datetime, timezone
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
    from_email: str | None = None,
) -> dict[str, Any]:
    from_email = from_email or os.getenv("FROM_EMAIL")
    if not from_email:
        raise ValueError("FROM_EMAIL environment variable is required")

    try:
        msg = MIMEText(body, "plain")
        msg["From"] = from_email
        msg["To"] = to_email
        msg["Subject"] = subject

        server = _get_smtp_connection()
        server.sendmail(from_email, [to_email], msg.as_string())
        server.quit()

        return {
            "result": "ok",
            "sent_at": _iso_now(),
            "to": to_email,
            "subject": subject,
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

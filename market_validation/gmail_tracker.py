"""
Gmail API tracking: reply detection + bounce detection.

Uses the actual Gmail thread of each sent message — no subject guessing.
Requires config/gmail_credentials.json (OAuth2 Desktop app from Google Cloud Console).
First-time setup: python3 -m market_validation.gmail_tracker --auth
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from market_validation.email_sender import EMAIL_QUEUE_DIR
from market_validation.environment import load_project_env

load_project_env()

_PROJECT_ROOT = Path(__file__).parent.parent
CREDENTIALS_FILE = _PROJECT_ROOT / "config" / "gmail_credentials.json"
TOKEN_FILE = _PROJECT_ROOT / "config" / "gmail_token.json"
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_credentials():
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow

    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CREDENTIALS_FILE.exists():
                raise FileNotFoundError(
                    f"Gmail credentials not found at {CREDENTIALS_FILE}\n"
                    "Download OAuth2 credentials from Google Cloud Console and save there."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), SCOPES)
            creds = flow.run_local_server(port=8080)
        TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
        TOKEN_FILE.write_text(creds.to_json())

    return creds


def get_service():
    from googleapiclient.discovery import build
    return build("gmail", "v1", credentials=get_credentials())


def _load_sent_emails() -> list[dict[str, Any]]:
    emails = []
    for f in sorted(EMAIL_QUEUE_DIR.glob("*.json")):
        data = json.loads(f.read_text())
        if data.get("approved") and data.get("message_id"):
            emails.append(data)
    return emails


def _save_email(data: dict[str, Any]) -> None:
    (EMAIL_QUEUE_DIR / f"{data['id']}.json").write_text(json.dumps(data, indent=2))


def _find_gmail_thread(service, message_id: str) -> str | None:
    """Find the Gmail threadId for a sent message using its RFC822 Message-ID header."""
    try:
        results = service.users().messages().list(
            userId="me",
            q=f"rfc822msgid:{message_id}",
            maxResults=1,
        ).execute()
        msgs = results.get("messages", [])
        if msgs:
            return msgs[0].get("threadId")
    except Exception:
        pass
    return None


def _thread_has_reply(service, thread_id: str, sent_message_id: str) -> dict[str, Any] | None:
    """
    Check if a Gmail thread has more messages than just the original sent one.
    Returns the reply message metadata if found, else None.
    """
    try:
        thread = service.users().threads().get(
            userId="me",
            id=thread_id,
            format="metadata",
            metadataHeaders=["From", "Date", "Subject"],
        ).execute()
        messages = thread.get("messages", [])
        # More than 1 message in thread = reply exists
        if len(messages) > 1:
            # Return the latest non-sent message info
            reply_msg = messages[-1]
            headers = {
                h["name"].lower(): h["value"]
                for h in reply_msg.get("payload", {}).get("headers", [])
            }
            return {
                "from": headers.get("from", ""),
                "date": headers.get("date", ""),
                "subject": headers.get("subject", ""),
                "gmail_msg_id": reply_msg.get("id"),
            }
    except Exception:
        pass
    return None


def check_replies(service) -> list[str]:
    """
    For each sent email with a message_id, find its Gmail thread and check for replies.
    Returns list of queue email IDs that were marked replied.
    """
    sent = _load_sent_emails()
    replied_ids: list[str] = []

    for email_data in sent:
        if email_data.get("replied_at"):
            continue

        message_id = email_data.get("message_id", "").strip("<>")
        if not message_id:
            continue

        thread_id = _find_gmail_thread(service, message_id)
        if not thread_id:
            continue

        reply_info = _thread_has_reply(service, thread_id, message_id)
        if reply_info:
            email_data["replied_at"] = _iso_now()
            email_data["status"] = "replied"
            email_data["reply_from"] = reply_info.get("from", "")
            email_data["reply_subject"] = reply_info.get("subject", "")
            _save_email(email_data)
            replied_ids.append(email_data["id"])

    return replied_ids


def check_bounces(service) -> list[str]:
    """
    Search inbox for delivery failure NDRs, match to sent emails by recipient address.
    Returns list of queue email IDs that were marked bounced.
    """
    sent = _load_sent_emails()
    if not sent:
        return []

    query = (
        "from:(mailer-daemon OR postmaster) "
        "subject:(\"Delivery Status Notification\" OR \"Mail Delivery Failure\" "
        "OR \"Undeliverable\" OR \"delivery failed\") "
        "newer_than:30d"
    )
    try:
        results = service.users().messages().list(userId="me", q=query, maxResults=50).execute()
    except Exception:
        return []

    messages = results.get("messages", [])
    recipient_map = {e["to_email"].lower(): e for e in sent if not e.get("bounced_at")}
    bounced_ids: list[str] = []

    for msg_ref in messages:
        try:
            full = service.users().messages().get(
                userId="me", id=msg_ref["id"], format="full"
            ).execute()
            snippet = full.get("snippet", "").lower()
            for recipient, email_data in list(recipient_map.items()):
                if recipient in snippet:
                    email_data["bounced_at"] = _iso_now()
                    email_data["status"] = "bounced"
                    _save_email(email_data)
                    bounced_ids.append(email_data["id"])
                    del recipient_map[recipient]
        except Exception:
            continue

    return bounced_ids


def sync_all() -> dict[str, Any]:
    """Run reply + bounce checks via Gmail API. Returns summary."""
    try:
        service = get_service()
    except FileNotFoundError as e:
        return {"result": "error", "error": str(e)}
    except Exception as e:
        return {"result": "error", "error": f"Gmail auth failed: {e}"}

    replied = check_replies(service)
    bounced = check_bounces(service)

    return {
        "result": "ok",
        "synced_at": _iso_now(),
        "replied": replied,
        "bounced": bounced,
        "replied_count": len(replied),
        "bounced_count": len(bounced),
    }


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Gmail tracking sync")
    parser.add_argument("--auth", action="store_true", help="Run OAuth2 auth flow (one-time setup)")
    parser.add_argument("--sync", action="store_true", help="Check replies + bounces")
    args = parser.parse_args()

    if args.auth:
        get_credentials()
        print(f"Auth complete. Token saved to {TOKEN_FILE}")
        return

    if args.sync:
        result = sync_all()
        print(json.dumps(result, indent=2))
        return

    parser.print_help()


if __name__ == "__main__":
    main()

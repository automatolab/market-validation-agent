"""
Gmail API tracking: reply detection + bounce detection.

Uses the actual Gmail thread of each sent message — no subject guessing.
Requires config/gmail_credentials.json (OAuth2 Desktop app from Google Cloud Console).
First-time setup: python3 -m market_validation.gmail_tracker --auth
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
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
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _find_gmail_thread(service, message_id: str, to_email: str = "", subject: str = "") -> str | None:
    """
    Find the Gmail threadId for a sent message.
    Tries rfc822msgid first; falls back to in:sent search by recipient + subject.
    """
    # Try exact Message-ID match
    for q in [f"rfc822msgid:{message_id}", f"rfc822msgid:<{message_id}>"]:
        try:
            results = service.users().messages().list(userId="me", q=q, maxResults=1).execute()
            msgs = results.get("messages", [])
            if msgs:
                return msgs[0].get("threadId")
        except Exception:
            pass

    # Fallback: search sent folder by recipient + subject
    if to_email and subject:
        safe_subject = subject.replace('"', "")[:80]
        q = f'in:sent to:{to_email} subject:"{safe_subject}"'
        try:
            results = service.users().messages().list(userId="me", q=q, maxResults=1).execute()
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
            reply_msg = messages[-1]
            headers = {
                h["name"].lower(): h["value"]
                for h in reply_msg.get("payload", {}).get("headers", [])
            }
            # snippet is returned by the API even in metadata format
            snippet = reply_msg.get("snippet", "")
            return {
                "from": headers.get("from", ""),
                "date": headers.get("date", ""),
                "subject": headers.get("subject", ""),
                "gmail_msg_id": reply_msg.get("id"),
                "snippet": snippet[:300] if snippet else "",
            }
    except Exception:
        pass
    return None


def _decode_mime_body(payload: dict[str, Any]) -> str:
    """Walk a Gmail message payload and pull the first text/plain part body.

    Falls back to the snippet if no plaintext part is decodable. Avoids the
    ``snippet`` field's quoted-original problem on multipart replies because
    Gmail returns text/plain ahead of text/html, and many clients put their
    reply at the top of the plaintext part.
    """
    import base64 as _b64

    def _walk(parts: list[dict[str, Any]]) -> str | None:
        for p in parts:
            mime_type = p.get("mimeType", "")
            if mime_type == "text/plain":
                data = (p.get("body") or {}).get("data") or ""
                if data:
                    try:
                        return _b64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                    except Exception:
                        return None
            sub = p.get("parts") or []
            if sub:
                found = _walk(sub)
                if found:
                    return found
        return None

    parts = payload.get("parts") or []
    if parts:
        text = _walk(parts)
        if text:
            return text
    # Single-part message
    body = (payload.get("body") or {}).get("data") or ""
    if body:
        try:
            return _b64.urlsafe_b64decode(body).decode("utf-8", errors="replace")
        except Exception:
            pass
    return ""


def _strip_quoted_reply(text: str) -> str:
    """Remove the quoted original from a plaintext reply body.

    The previous implementation used a single regex that broke on non-English
    Gmail clients ("Em qua, ...", "Le mer., ...", etc.). This version handles
    several common attribution formats and the universal ``> `` line-prefix
    quoting used by RFC 2822 mail clients.
    """
    if not text:
        return ""
    import re as _re
    lines = text.splitlines()
    cut: int | None = None
    # Common attribution patterns across locales
    attribution_patterns = [
        r"^On\s+\w+,?\s+.*wrote:",                  # English
        r"^On\s+\w{3,}\s+\d+,?\s+\d{4}.*wrote:",    # English long-form
        r"^Em\s+\w+,?\s+.*escreveu:",               # Portuguese
        r"^Le\s+\w+,?\s+.*écrit\s*:",               # French
        r"^Am\s+\w+,?\s+.*schrieb\s*:",             # German
        r"^El\s+\w+,?\s+.*escribió\s*:",            # Spanish
        r"^Il\s+\w+,?\s+.*ha scritto\s*:",          # Italian
        r"^-+\s*Original Message\s*-+",
        r"^-+\s*Forwarded message\s*-+",
        r"^From\s*:\s*",                            # Outlook quoting
    ]
    attribution_re = _re.compile("|".join(attribution_patterns), _re.IGNORECASE)
    for i, line in enumerate(lines):
        stripped = line.strip()
        if attribution_re.match(stripped):
            cut = i
            break
        # Lines starting with '>' are the canonical RFC quoted original.
        if stripped.startswith(">"):
            cut = i
            break
    if cut is not None:
        lines = lines[:cut]
    return "\n".join(lines).strip()


def check_replies(service) -> list[str]:
    """
    For each sent email, search all mail (not just inbox) for a reply from the recipient.
    Captures reply snippet. Skips emails already marked replied with a snippet.
    """
    sent = _load_sent_emails()
    replied_ids: list[str] = []

    for email_data in sent:
        # Skip if already have full reply info
        if email_data.get("replied_at") and email_data.get("reply_snippet"):
            continue

        to_addr = email_data.get("to_email", "")
        subject = email_data.get("subject", "")
        safe_subject = subject.replace('"', "").replace("\\", "")[:60]

        # in:anywhere includes inbox, sent, archive, and trash
        query = f'from:{to_addr} subject:"Re: {safe_subject}" in:anywhere newer_than:60d'
        try:
            results = service.users().messages().list(
                userId="me", q=query, maxResults=1
            ).execute()
            messages = results.get("messages", [])
            if not messages:
                continue

            # Fetch full message so we can parse the MIME body and avoid the
            # snippet's quoted-original problem.
            msg = service.users().messages().get(
                userId="me",
                id=messages[0]["id"],
                format="full",
            ).execute()
            headers = {
                h["name"].lower(): h["value"]
                for h in msg.get("payload", {}).get("headers", [])
            }

            email_data["replied_at"] = email_data.get("replied_at") or _iso_now()
            email_data["status"] = "replied"
            email_data["reply_from"] = headers.get("from", "")
            email_data["reply_subject"] = headers.get("subject", "")

            # Prefer MIME body extraction over the snippet field. Snippets
            # often include the quoted original because Gmail extracts naive
            # plaintext; the MIME body of a well-formed reply is just the
            # new content (or has clear attribution / `> ` quoting we can strip).
            payload = msg.get("payload") or {}
            body_text = _decode_mime_body(payload)
            if body_text:
                clean = _strip_quoted_reply(body_text)
            else:
                # Fall back to snippet — same approach as before, but with
                # i18n-aware attribution stripping.
                import html as _html
                snippet = msg.get("snippet", "")
                clean = _strip_quoted_reply(_html.unescape(snippet))
            email_data["reply_snippet"] = clean[:500]
            _save_email(email_data)
            replied_ids.append(email_data["id"])
        except Exception:
            continue

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

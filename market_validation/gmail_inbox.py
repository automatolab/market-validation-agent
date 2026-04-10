from __future__ import annotations

import base64
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from market_validation.environment import load_project_env


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _get_gmail_service():
    from googleapiclient.discovery import build
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow

    creds = None
    token_path = Path("token.json")
    creds_path = Path("credentials.json")

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), ["https://www.googleapis.com/auth/gmail.modify"])
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        elif creds_path.exists():
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), ["https://www.googleapis.com/auth/gmail.modify"])
            creds = flow.run_local_server(port=0)
            with open(token_path, "w") as token:
                token.write(creds.to_json())
        else:
            raise FileNotFoundError("credentials.json not found. Download from Google Cloud Console.")

    return build("gmail", "v1", credentials=creds)


def _decode_email_body(payload: dict[str, Any]) -> str:
    try:
        from bs4 import BeautifulSoup
        has_bs4 = True
    except ImportError:
        has_bs4 = False

    body = ""
    data = payload.get("body", {}).get("data")
    if data:
        try:
            body = base64.urlsafe_b64decode(data).decode("utf-8")
        except Exception:
            pass

    if not body and payload.get("parts"):
        for part in payload["parts"]:
            mime_type = part.get("mimeType", "")
            if mime_type == "text/plain":
                data = part.get("body", {}).get("data")
                if data:
                    try:
                        body = base64.urlsafe_b64decode(data).decode("utf-8")
                    except Exception:
                        pass
            elif mime_type == "text/html" and has_bs4:
                data = part.get("body", {}).get("data")
                if data:
                    try:
                        html = base64.urlsafe_b64decode(data).decode("utf-8")
                        soup = BeautifulSoup(html, "html.parser")
                        body = soup.get_text(separator="\n", strip=True)
                    except Exception:
                        pass
            if body:
                break

    return re.sub(r"\s+", " ", body).strip()


def _extract_headers(headers: list[dict[str, Any]], name: str) -> str:
    for header in headers:
        if header.get("name", "").lower() == name.lower():
            return header.get("value", "")
    return ""


def fetch_email_replies(
    *,
    sender_email: str | None = None,
    subject_prefix: str | None = None,
    hours_lookback: int = 72,
    max_results: int = 50,
) -> list[dict[str, Any]]:
    try:
        service = _get_gmail_service()
    except FileNotFoundError:
        return [{"error": "Gmail credentials not found. Run setup at https://developers.google.com/gmail/api/quickstart/python"}]

    my_email = sender_email or os.getenv("MY_EMAIL")
    if not my_email:
        return [{"error": "MY_EMAIL not set in environment"}]

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours_lookback)
    query_parts = [f"to:{my_email}", f"after:{int(since.timestamp())}"]

    if subject_prefix:
        query_parts.append(f"subject:{subject_prefix}")

    query = " ".join(query_parts)

    try:
        results = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=max_results,
        ).execute()

        messages = results.get("messages", [])
        replies = []

        for msg in messages:
            msg_data = service.users().messages().get(userId="me", id=msg["id"], format="full").execute()
            payload = msg_data.get("payload", {})
            headers = payload.get("headers", [])

            subject = _extract_headers(headers, "Subject")
            from_addr = _extract_headers(headers, "From")
            date = _extract_headers(headers, "Date")
            message_id = _extract_headers(headers, "Message-ID")
            thread_id = msg_data.get("threadId")

            body = _decode_email_body(payload)

            if my_email.lower() in from_addr.lower():
                continue

            replies.append({
                "message_id": message_id,
                "thread_id": thread_id,
                "subject": subject,
                "from": from_addr,
                "date": date,
                "body": body[:5000],
                "fetched_at": _iso_now(),
            })

        return replies

    except Exception as e:
        return [{"error": str(e), "fetched_at": _iso_now()}]


def build_reply_payload(
    replies: list[dict[str, Any]],
    company_match_patterns: dict[str, str] | None = None,
) -> dict[str, Any]:
    if not replies:
        return {
            "result": "ok",
            "messages": [],
            "warnings": ["No replies found"],
        }

    company_match_patterns = company_match_patterns or {}
    processed_messages = []

    for reply in replies:
        if "error" in reply:
            continue

        body = reply.get("body", "")
        from_addr = reply.get("from", "")
        subject = reply.get("subject", "")

        company_id = None
        for pattern, cid in company_match_patterns.items():
            if pattern.lower() in from_addr.lower() or pattern.lower() in subject.lower():
                company_id = cid
                break

        intent = "unknown"
        body_lower = body.lower()

        interest_signals = ["interested", "yes", "great", "send", "quote", "pricing", "call me", "reach out", "details"]
        decline_signals = ["not interested", "no thanks", "not now", "remove", "unsubscribe", "don't want"]

        for sig in interest_signals:
            if sig in body_lower:
                intent = "interested"
                break
        for sig in decline_signals:
            if sig in body_lower:
                intent = "not_interested"
                break

        processed_messages.append({
            "message_id": reply.get("message_id"),
            "thread_id": reply.get("thread_id"),
            "company_id": company_id,
            "from": from_addr,
            "subject": subject,
            "body": body,
            "intent": intent,
            "fetched_at": reply.get("fetched_at"),
        })

    return {
        "result": "ok",
        "messages": processed_messages,
        "count": len(processed_messages),
    }


def fetch_and_build_replies(
    *,
    sender_email: str | None = None,
    subject_prefix: str | None = None,
    hours_lookback: int = 72,
    max_results: int = 50,
    company_match_patterns: dict[str, str] | None = None,
) -> dict[str, Any]:
    replies = fetch_email_replies(
        sender_email=sender_email,
        subject_prefix=subject_prefix,
        hours_lookback=hours_lookback,
        max_results=max_results,
    )
    return build_reply_payload(replies, company_match_patterns)


def build_parser() -> Any:
    import argparse
    parser = argparse.ArgumentParser(description="Fetch email replies from Gmail for reply_parse stage")
    parser.add_argument("--sender-email", default=None, help="Your email address (or use MY_EMAIL env var)")
    parser.add_argument("--subject-prefix", default=None, help="Filter by subject prefix (e.g., 'Brisket')")
    parser.add_argument("--hours-lookback", type=int, default=72, help="Hours to look back for replies")
    parser.add_argument("--max-results", type=int, default=50, help="Max messages to fetch")
    parser.add_argument("--output-json", action="store_true", help="Output as JSON for pipeline")
    return parser


def main() -> None:
    import json

    parser = build_parser()
    args = parser.parse_args()

    result = fetch_and_build_replies(
        sender_email=args.sender_email,
        subject_prefix=args.subject_prefix,
        hours_lookback=args.hours_lookback,
        max_results=args.max_results,
    )

    if args.output_json:
        print(json.dumps(result, ensure_ascii=True))
    else:
        print(json.dumps(result, ensure_ascii=True, indent=2))
        if result.get("count", 0) > 0:
            print(f"\nFetched {result['count']} replies. Use --output-json for pipeline ingestion.")
        else:
            print(f"\nNo replies found in last {args.hours_lookback} hours.")

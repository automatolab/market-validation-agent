from __future__ import annotations

import email as email_lib
import imaplib
import os
import smtplib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any


# Custom header used to link a reply back to the originating lead.
_LEAD_ID_HEADER = "X-MV-Lead-ID"


@dataclass(frozen=True)
class InboundReply:
    imap_uid: int
    lead_id: str | None
    in_reply_to: str | None
    sender_email: str
    sender_name: str
    subject: str
    body: str
    received_at: str


class EmailSender:
    """SMTP sender for outreach emails. Config via env vars:
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM
    """

    def __init__(self) -> None:
        self._host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self._port = int(os.getenv("SMTP_PORT", "587"))
        self._user = os.getenv("SMTP_USER", "")
        self._password = os.getenv("SMTP_PASSWORD", "")
        self._from = os.getenv("SMTP_FROM", "") or self._user

    @property
    def configured(self) -> bool:
        return bool(self._user and self._password)

    def send(
        self,
        *,
        to: str,
        subject: str,
        body: str,
        lead_id: str | None = None,
    ) -> tuple[bool, str | None]:
        """Send email. Returns (success, message_id)."""
        if not self.configured:
            return False, None

        message_id = f"<mv-{uuid.uuid4()}@mv-agent.local>"

        msg = MIMEMultipart("alternative")
        msg["Message-ID"] = message_id
        msg["Subject"] = subject
        msg["From"] = self._from
        msg["To"] = to
        if lead_id:
            msg[_LEAD_ID_HEADER] = lead_id
        msg.attach(MIMEText(body, "plain", "utf-8"))

        try:
            with smtplib.SMTP(self._host, self._port, timeout=15) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(self._user, self._password)
                smtp.sendmail(self._from, [to], msg.as_string())
            return True, message_id
        except Exception:
            return False, None


class InboxPoller:
    """IMAP poller that finds replies to outreach emails. Config via env vars:
    IMAP_HOST, IMAP_PORT, IMAP_USER, IMAP_PASSWORD
    Falls back to SMTP_USER/SMTP_PASSWORD if IMAP_* not set.
    """

    def __init__(self) -> None:
        self._host = os.getenv("IMAP_HOST", "imap.gmail.com")
        self._port = int(os.getenv("IMAP_PORT", "993"))
        self._user = os.getenv("IMAP_USER", "") or os.getenv("SMTP_USER", "")
        self._password = os.getenv("IMAP_PASSWORD", "") or os.getenv("SMTP_PASSWORD", "")

    @property
    def configured(self) -> bool:
        return bool(self._user and self._password)

    def poll(self, since_uid: int = 0, sent_message_ids: set[str] | None = None) -> list[InboundReply]:
        """Poll INBOX for replies. Only returns UIDs > since_uid.

        Matching strategy (in order):
        1. X-MV-Lead-ID header present in the reply (rare — clients strip custom headers)
        2. In-Reply-To header matches a known sent message ID
        Returns all replies that match either condition. Caller matches lead_id from In-Reply-To.
        """
        if not self.configured:
            return []

        replies: list[InboundReply] = []
        try:
            with imaplib.IMAP4_SSL(self._host, self._port) as mail:
                mail.login(self._user, self._password)
                mail.select("INBOX", readonly=True)

                # Search for all emails with "Re:" in subject (broad filter)
                _, data = mail.search(None, 'SUBJECT "Re:"')
                if not data or not data[0]:
                    return []

                uids = [int(u) for u in data[0].split()]
                new_uids = [u for u in uids if u > since_uid]

                for uid in new_uids:
                    _, msg_data = mail.fetch(str(uid).encode(), "(RFC822)")
                    if not msg_data or not msg_data[0] or not isinstance(msg_data[0], tuple):
                        continue
                    raw = msg_data[0][1]
                    if not isinstance(raw, bytes):
                        continue
                    parsed = self._parse_message(raw, uid, sent_message_ids or set())
                    if parsed is not None:
                        replies.append(parsed)
        except Exception:
            pass

        return replies

    def _parse_message(
        self,
        raw: bytes,
        uid: int,
        sent_message_ids: set[str],
    ) -> InboundReply | None:
        try:
            msg = email_lib.message_from_bytes(raw)

            lead_id: str | None = msg.get(_LEAD_ID_HEADER) or msg.get("X-Mv-Lead-Id")
            in_reply_to: str | None = msg.get("In-Reply-To", "").strip() or None
            subject: str = msg.get("Subject", "")
            sender: str = msg.get("From", "")

            # Only process if we can link to a lead
            is_our_reply = (
                lead_id is not None
                or (in_reply_to is not None and in_reply_to in sent_message_ids)
            )
            if not is_our_reply:
                return None

            sender_email, sender_name = _parse_sender(sender)
            body = _extract_body(msg)
            received_at = datetime.now(timezone.utc).isoformat()

            return InboundReply(
                imap_uid=uid,
                lead_id=lead_id,
                in_reply_to=in_reply_to,
                sender_email=sender_email,
                sender_name=sender_name,
                subject=subject,
                body=body,
                received_at=received_at,
            )
        except Exception:
            return None


def _parse_sender(raw: str) -> tuple[str, str]:
    """Return (email, display_name) from a raw From header value."""
    raw = raw.strip()
    if "<" in raw and ">" in raw:
        name = raw.split("<")[0].strip().strip('"\'')
        addr = raw.split("<")[1].split(">")[0].strip()
        return addr, name
    return raw, ""


def _extract_body(msg: Any) -> str:
    """Extract plaintext body from an email.Message."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                if isinstance(payload, bytes):
                    return payload.decode("utf-8", errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if isinstance(payload, bytes):
            return payload.decode("utf-8", errors="replace")
    return ""

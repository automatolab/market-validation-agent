"""
Open-pixel tracking for sent emails.

The dashboard server handles GET /api/email/track/open/{id} → stamps opened_at.
Works for self-testing (sending to your own inbox on the same machine).
Reply/bounce tracking is handled by gmail_tracker.py via Gmail API.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from market_validation.email_sender import EMAIL_QUEUE_DIR


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load(email_id: str) -> dict[str, Any] | None:
    path = EMAIL_QUEUE_DIR / f"{email_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _save(email_id: str, data: dict[str, Any]) -> None:
    (EMAIL_QUEUE_DIR / f"{email_id}.json").write_text(json.dumps(data, indent=2))


def record_open(email_id: str) -> bool:
    """Stamp first open. Idempotent after that."""
    data = _load(email_id)
    if data is None:
        return False
    if not data.get("opened_at"):
        data["opened_at"] = _iso_now()
        data["status"] = "opened"
        _save(email_id, data)
    return True


def tracking_base_url() -> str:
    import os
    return os.getenv("TRACKING_BASE_URL", "http://localhost:8787").rstrip("/")


def pixel_url(email_id: str) -> str:
    return f"{tracking_base_url()}/api/email/track/open/{email_id}"


def build_html_body(plain_body: str, email_id: str) -> str:
    """Wrap plain text in minimal HTML and append open-tracking pixel."""
    html = (
        plain_body
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br>\n")
    )
    pixel = (
        f'<img src="{pixel_url(email_id)}" '
        'width="1" height="1" style="display:none;border:0" alt="">'
    )
    return (
        "<!doctype html><html><body>"
        f"<div style='font-family:sans-serif;line-height:1.6'>{html}</div>"
        f"{pixel}"
        "</body></html>"
    )


# 1×1 transparent GIF served as the pixel response
TRANSPARENT_GIF = bytes([
    0x47, 0x49, 0x46, 0x38, 0x39, 0x61,
    0x01, 0x00, 0x01, 0x00, 0x80, 0x00, 0x00,
    0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
    0x21, 0xf9, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x2c, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00,
    0x02, 0x02, 0x44, 0x01, 0x00,
    0x3b,
])

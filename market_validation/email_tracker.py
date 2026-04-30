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


_GOOGLE_IP_RANGES = (
    # Google image proxy ranges — opens from these IPs are pre-fetched by
    # Gmail itself, not the actual recipient. Treat as "indirect" so the
    # dashboard doesn't claim a real open.
    "66.249.",   # googlebot / image proxy
    "64.233.",
    "72.14.",
    "74.125.",
    "209.85.",
    "216.239.",
    "172.217.",
    "173.194.",
    "108.177.",
)


def _is_google_proxy_open(client_ip: str | None) -> bool:
    if not client_ip:
        return False
    return any(client_ip.startswith(prefix) for prefix in _GOOGLE_IP_RANGES)


def record_open(email_id: str, client_ip: str | None = None) -> bool:
    """Stamp first open. Idempotent after that.

    When the open hits Google's image proxy (Gmail pre-fetches all images so
    the recipient may not have actually opened the message), we record a
    weaker ``opened_indirect`` status. Real opens from non-Google IPs get
    the original ``opened`` status. Eliminates the 60-90% false-positive
    rate on Gmail-delivered campaigns.
    """
    data = _load(email_id)
    if data is None:
        return False
    is_google_proxy = _is_google_proxy_open(client_ip)
    if not data.get("opened_at"):
        data["opened_at"] = _iso_now()
        data["opened_via_proxy"] = is_google_proxy
        data["status"] = "opened_indirect" if is_google_proxy else "opened"
        if client_ip:
            data["opened_ip"] = client_ip
        _save(email_id, data)
    elif is_google_proxy and not data.get("opened_direct_at"):
        # First Google-proxy open already recorded; if a *non*-proxy open
        # comes later, upgrade. Otherwise leave alone.
        pass
    elif not is_google_proxy and data.get("opened_via_proxy"):
        # Upgrade an indirect open to a direct one.
        data["opened_direct_at"] = _iso_now()
        data["status"] = "opened"
        data["opened_ip"] = client_ip
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

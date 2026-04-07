from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


class OllamaClient:
    """Small JSON-first client for Ollama chat completions."""

    def __init__(
        self,
        base_url: str | None = None,
        model: str | None = None,
        timeout_seconds: float = 20.0,
        temperature: float = 0.2,
    ) -> None:
        self._base_url = (base_url or os.getenv("OLLAMA_API_BASE", "")).strip()
        self._model = (model or os.getenv("OLLAMA_MODEL", "gpt-oss:120b")).strip()
        self._timeout_seconds = timeout_seconds
        self._temperature = temperature

    @property
    def enabled(self) -> bool:
        return bool(self._base_url and self._model)

    def chat_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any] | None:
        if not self.enabled:
            return None

        body = {
            "model": self._model,
            "stream": False,
            "format": "json",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "options": {
                "temperature": self._temperature,
            },
        }

        request = Request(
            url=urljoin(self._base_url.rstrip("/") + "/", "api/chat"),
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8", errors="ignore"))
        except (HTTPError, URLError, OSError, TimeoutError, ValueError, json.JSONDecodeError):
            return None

        message = payload.get("message") if isinstance(payload, dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, str) or not content.strip():
            return None

        parsed = _extract_json(content)
        if isinstance(parsed, dict):
            return parsed
        return None


def _extract_json(content: str) -> Any | None:
    """Extract JSON payload from raw model text, with fence and wrapper tolerance."""

    raw = content.strip()
    if not raw:
        return None

    direct = _try_parse_json(raw)
    if direct is not None:
        return direct

    fenced = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.I | re.S).strip()
    if fenced != raw:
        direct = _try_parse_json(fenced)
        if direct is not None:
            return direct

    for opener, closer in (("{", "}"), ("[", "]")):
        start = raw.find(opener)
        end = raw.rfind(closer)
        if start == -1 or end == -1 or end <= start:
            continue
        candidate = raw[start : end + 1]
        direct = _try_parse_json(candidate)
        if direct is not None:
            return direct

    return None


def _try_parse_json(raw: str) -> Any | None:
    try:
        return json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


def load_project_env(root: str | Path | None = None) -> Path | None:
    candidates: list[Path] = []

    if root is not None:
        candidates.append(Path(root).resolve() / ".env")

    candidates.append(Path.cwd().resolve() / ".env")

    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if not resolved.exists():
            continue
        load_dotenv(dotenv_path=resolved, override=False)
        return resolved

    load_dotenv(override=False)
    return None

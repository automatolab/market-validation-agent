from __future__ import annotations

import os
from pathlib import Path

from market_validation.environment import load_project_env


def test_load_project_env_reads_root_dotenv(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("TEST_ENV_VALUE=from-dotenv\n", encoding="utf-8")

    os.environ.pop("TEST_ENV_VALUE", None)

    loaded = load_project_env(root=tmp_path)
    assert loaded == env_path.resolve()
    assert os.getenv("TEST_ENV_VALUE") == "from-dotenv"

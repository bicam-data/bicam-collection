from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from urllib.parse import quote


def parse_dotenv(dotenv_path: Path | None) -> dict[str, str]:
    if dotenv_path is None or not dotenv_path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def load_dotenv(dotenv_path: Path | None) -> None:
    for key, value in parse_dotenv(dotenv_path).items():
        os.environ.setdefault(key, value)


@dataclass(slots=True)
class Settings:
    database_url: str
    env_path: Path
    congress_api_base_url: str = "https://api.congress.gov/v3"
    govinfo_api_base_url: str = "https://api.govinfo.gov"

    @classmethod
    def from_env(cls, project_root: Path) -> "Settings":
        env_path = (project_root / ".env").resolve()
        load_dotenv(env_path)
        database_url = os.environ.get("DATABASE_URL") or _build_database_url_from_parts()
        if not database_url:
            raise ValueError("DATABASE_URL or DB_PASSWORD is required")
        return cls(
            database_url=database_url,
            env_path=env_path,
        )

    def api_base_url_for_source(self, source: str) -> str:
        if source == "congress":
            return self.congress_api_base_url
        if source == "govinfo":
            return self.govinfo_api_base_url
        raise ValueError(f"Unsupported source '{source}'")


def _build_database_url_from_parts() -> str | None:
    password = os.environ.get("DB_PASSWORD")
    if not password:
        return None
    user = os.environ.get("DB_USER", "admin")
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5433")
    dbname = os.environ.get("DB_NAME", "bicam_collection")
    quoted_user = quote(user, safe="")
    quoted_password = quote(password, safe="")
    return f"postgresql://{quoted_user}:{quoted_password}@{host}:{port}/{dbname}"

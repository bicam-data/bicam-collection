from __future__ import annotations

from pathlib import Path

from bicam_collection.config import Settings
from bicam_collection.migrations import apply_migrations


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    applied = apply_migrations(
        database_url=settings.database_url,
        migrations_dir=project_root / "migrations",
    )
    if applied:
        print(f"applied {len(applied)} migration(s): {', '.join(applied)}")
    else:
        print("database schema is current")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

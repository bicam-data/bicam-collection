from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import psycopg


@dataclass(frozen=True, slots=True)
class Migration:
    name: str
    sql: str


def discover_migrations(migrations_dir: Path) -> list[Migration]:
    if not migrations_dir.exists():
        return []
    migrations = [
        Migration(name=path.name, sql=path.read_text(encoding="utf-8"))
        for path in migrations_dir.glob("[0-9][0-9][0-9]_*.sql")
    ]
    return sorted(migrations, key=lambda migration: migration.name)


def apply_migrations(*, database_url: str, migrations_dir: Path) -> list[str]:
    applied: list[str] = []
    with psycopg.connect(database_url) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                name TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        existing = {
            row[0]
            for row in conn.execute("SELECT name FROM schema_migrations").fetchall()
        }
        for migration in discover_migrations(migrations_dir):
            if migration.name in existing:
                continue
            with conn.transaction():
                conn.execute(migration.sql)
                conn.execute(
                    "INSERT INTO schema_migrations (name) VALUES (%s)",
                    (migration.name,),
                )
            applied.append(migration.name)
    return applied


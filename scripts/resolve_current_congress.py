"""Print the newest Congress in the archive (BICAM_CURRENT_CONGRESS overrides it)."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re

import psycopg

from bicam_collection.config import Settings


CURRENT_CONGRESS_SQL = """
    select max((regexp_match(endpoint_url, '/congress/([0-9]+)(?:[?/]|$)'))[1]::integer)
      from raw_responses
     where source = 'congress'
       and category = 'congresses'
       and node_type = 'congress_detail'
"""

_POSITIVE_INTEGER = re.compile(r"^[1-9][0-9]*$")


def parse_congress(value: object | None) -> int | None:
    """Return a positive Congress number, or ``None`` for an invalid value."""

    text = str(value).strip() if value is not None else ""
    if not _POSITIVE_INTEGER.fullmatch(text):
        return None
    return int(text)


def resolve_current_congress(database_url: str, *, override: object | None = None) -> int:
    """Resolve the newest Congress from the archive or an explicit override."""

    explicit = parse_congress(override)
    if explicit is not None:
        return explicit
    with psycopg.connect(database_url) as connection:
        value = connection.execute(CURRENT_CONGRESS_SQL).fetchone()[0]
    resolved = parse_congress(value)
    if resolved is None:
        raise RuntimeError("no Congress.gov congress_detail rows are available")
    return resolved


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--override",
        help="explicit positive Congress number for a controlled recovery/test run",
    )
    args = parser.parse_args(argv)
    project_root = Path(__file__).resolve().parents[1]
    settings = Settings.from_env(project_root)
    override = args.override or os.environ.get("BICAM_CURRENT_CONGRESS")
    print(resolve_current_congress(settings.database_url, override=override))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

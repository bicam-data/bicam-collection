from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bicam_collection.config import Settings


class ConfigTests(unittest.TestCase):
    def test_settings_builds_database_url_from_db_parts_when_database_url_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with tempfile.TemporaryDirectory() as tmpdir:
                project_root = Path(tmpdir)
                (project_root / ".env").write_text(
                    "\n".join(
                        [
                            "DB_PASSWORD=secret value",
                            "DB_USER=admin",
                            "DB_HOST=localhost",
                            "DB_PORT=5433",
                            "DB_NAME=bicam_collection",
                        ]
                    ),
                    encoding="utf-8",
                )

                settings = Settings.from_env(project_root)

        self.assertEqual(
            settings.database_url,
            "postgresql://admin:secret%20value@localhost:5433/bicam_collection",
        )


if __name__ == "__main__":
    unittest.main()

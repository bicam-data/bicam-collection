import os
import subprocess
from collections.abc import Sequence
from pathlib import Path

from ..plugins.registry import get_registry


def run_dbt(
    project_dir: str | os.PathLike | None = None,
    profiles_dir: str | os.PathLike | None = None,
    select: Sequence[str] | None = None,
    full_build: bool = False,
) -> int:
    """
    Run dbt against the bundled project under ./dbt.

    Args:
        project_dir: Path to dbt project (defaults to repo_root/dbt)
        profiles_dir: Path to dbt profiles directory (defaults to ~/.dbt)
        select: A list of selection strings (e.g., tags) to build
        full_build: If True, run `dbt build`; otherwise `dbt run`

    Returns:
        The dbt process return code.
    """
    repo_root = Path(__file__).resolve().parents[2]
    project_dir = Path(project_dir) if project_dir else repo_root / "dbt"

    cmd: list[str] = ["dbt", "build" if full_build else "run"]
    cmd += ["--project-dir", str(project_dir)]

    if profiles_dir:
        cmd += ["--profiles-dir", str(profiles_dir)]

    if select:
        # Allow tags or model paths, e.g. ["tag:congressional"]
        cmd += ["--select", ",".join(select)]

    env = os.environ.copy()
    proc = subprocess.run(cmd, env=env)
    return proc.returncode


def get_data_types_with_cleaners() -> list[str]:
    """
    Get list of data types that have cleaning plugins available.

    This helps identify which dbt models should have cleaning transformations applied.
    """
    registry = get_registry()
    data_types_with_cleaners = []

    # Get all registered data types
    try:
        all_data_types = registry.get_all_data_types()
        for data_type in all_data_types:
            # Check if there's a custom logic plugin with cleaning methods
            custom_logic = registry.get_custom_logic_plugin(data_type, "cleaning")
            if custom_logic:
                # Check if it has any _clean_*_singular methods

                methods = [
                    m
                    for m in dir(custom_logic)
                    if m.startswith("_clean_") and m.endswith("_singular")
                ]
                if methods:
                    data_types_with_cleaners.append(data_type)
    except Exception as e:
        import logging

        logger = logging.getLogger(__name__)
        logger.warning(f"Error getting data types with cleaners: {e}")

    return data_types_with_cleaners


def run_congressional_pipeline(full_build: bool = True) -> int:
    """
    Convenience: run the congressional stack (stg -> int -> core -> mart) via tags.
    """
    tags = ["tag:congressional"] if full_build else ["tag:stg,tag:congressional"]
    return run_dbt(select=tags, full_build=full_build)

"""Path helper functions for data extraction."""

from pathlib import Path
from typing import Optional


def ensure_dir(path: Path | str) -> Path:
    """Ensure directory exists."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_latest_file(directory: Path | str, pattern: str = "*.json") -> Optional[Path]:
    """Get the most recent file matching the pattern."""
    directory = Path(directory)
    if not directory.exists():
        return None

    files = list(directory.glob(pattern))
    if not files:
        return None

    return max(files, key=lambda p: p.stat().st_mtime)


def get_bronze_dir(bronze_path: Path, source: str) -> Path:
    """Get bronze directory for a source."""
    return bronze_path / source


def get_silver_dir(silver_path: Path, source: str) -> Path:
    """Get silver directory for a source."""
    return silver_path / source
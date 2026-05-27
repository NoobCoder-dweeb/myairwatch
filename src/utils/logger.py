"""Logging configuration for air quality data extraction."""

import logging
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE_PATH = LOG_DIR / "app.log"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def _has_file_handler(logger: logging.Logger, log_file: Path) -> bool:
    """Return True when logger already writes to the requested file."""
    target = log_file.resolve()
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler_path = Path(handler.baseFilename).resolve()
            if handler_path == target:
                return True
    return False


def _has_console_handler(logger: logging.Logger) -> bool:
    """Return True when logger already has a non-file stream handler."""
    return any(
        isinstance(handler, logging.StreamHandler)
        and not isinstance(handler, logging.FileHandler)
        for handler in logger.handlers
    )


def get_logger(
    name: str,
    level: int = logging.INFO,
    log_file: str | Path = LOG_FILE_PATH,
    console: bool = False,
) -> logging.Logger:
    """Get a logger that always writes to logs/app.log.

    Console logging is opt-in. File logging is mandatory so pipeline runs leave
    an auditable record even when stdout is not captured.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    formatter = logging.Formatter(LOG_FORMAT)
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    if not _has_file_handler(logger, log_path):
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if console and not _has_console_handler(logger):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


DEFAULT_LOGGER = get_logger("air_quality")

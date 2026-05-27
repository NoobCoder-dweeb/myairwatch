"""Date helper functions for data extraction."""

from datetime import datetime


def get_timestamp(format: str = "%Y%m%d_%H%M%S") -> str:
    """Get current timestamp as formatted string."""
    return datetime.now().strftime(format)


def get_partition_date() -> tuple[int, int, int]:
    """Get current year, month, day for partitioning."""
    now = datetime.now()
    return now.year, now.month, now.day

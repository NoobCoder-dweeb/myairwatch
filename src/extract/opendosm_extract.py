"""Extract air quality data from OpenDOSM API for Malaysia."""

import json
from datetime import datetime, timedelta
from pathlib import Path

import requests

from ..utils.config import get_data_bronze_path, get_opendosm_base_url
from ..utils.constants import JSON, OPENDOSM_DATASET_ID, DEFAULT_PAGE_SIZE, REQUEST_TIMEOUT
from ..utils.date_helpers import get_timestamp
from ..utils.path_helpers import ensure_dir
from ..utils.logger import DEFAULT_LOGGER as logger

# Default to last 6 months
DEFAULT_MONTHS_BACK = 6


def fetch_opendosm_data(
    base_url: str = get_opendosm_base_url(),
    limit: int = DEFAULT_PAGE_SIZE,
    offset: int = 0,
    date_from: str | None = None,
    date_to: str | None = None,
) -> JSON:
    """Fetch air pollution data from OpenDOSM API with pagination."""
    url = base_url
    params = {
        "id": OPENDOSM_DATASET_ID,
        "limit": limit,
        "offset": offset,
    }
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to

    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def fetch_all_opendosm_data(
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[JSON]:
    """Fetch all air pollution data from OpenDOSM using pagination."""
    all_data = []
    limit = DEFAULT_PAGE_SIZE
    offset = 0

    while True:
        data = fetch_opendosm_data(
            limit=limit,
            offset=offset,
            date_from=date_from,
            date_to=date_to,
        )

        if isinstance(data, dict) and "data" in data:
            records = data["data"]
        elif isinstance(data, list):
            records = data
        else:
            records = [data] if data else []

        if not records:
            break

        all_data.extend(records)
        logger.info(f"Fetched {len(records)} records (total: {len(all_data)})")

        if len(records) < limit:
            break

        offset += limit

    return all_data


def save_to_bronze(data: list[JSON], output_dir: Path) -> Path:
    """Save extracted data as JSON to bronze layer."""
    ensure_dir(output_dir)

    timestamp = get_timestamp()
    filename = f"opendosm_air_pollution_{timestamp}.json"
    filepath = output_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(data)} records to {filepath}")
    return filepath


def get_date_range(months_back: int = DEFAULT_MONTHS_BACK) -> tuple[str, str]:
    """Calculate date range for the last N months."""
    date_to = datetime.now().strftime("%Y-%m-%dT%H:%M:%S+00:00")
    date_from = (datetime.now() - timedelta(days=months_back * 30)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    return date_from, date_to


def extract_opendosm(months_back: int = DEFAULT_MONTHS_BACK) -> Path:
    """Main extraction function for OpenDOSM data.

    Args:
        months_back: Number of months of historical data to fetch (default: 6)
    """
    bronze_path = get_data_bronze_path()
    opendosm_dir = bronze_path / "opendosm"

    # Calculate date range for last N months
    date_from, date_to = get_date_range(months_back)
    logger.info(f"Fetching data from {date_from} to {date_to}")

    data = fetch_all_opendosm_data(date_from=date_from, date_to=date_to)
    filepath = save_to_bronze(data, opendosm_dir)

    return filepath


if __name__ == "__main__":
    extract_opendosm()

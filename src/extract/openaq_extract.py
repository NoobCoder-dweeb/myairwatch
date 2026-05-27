"""Extract air quality data from OpenAQ API for Malaysia."""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from ..utils.config import get_data_bronze_path, get_openaq_api_key, get_openaq_base_url
from ..utils.constants import JSON, DEFAULT_PAGE_SIZE, REQUEST_TIMEOUT
from ..utils.logger import DEFAULT_LOGGER as logger

# Malaysia location IDs from explore.openaq.org
MALAYSIA_LOCATION_IDS = [
    6289999,  # Sejati Residences Cyberjaya
    3400978,  # Setia Eco Park
    3331918,  # Taman Tun Dr. Ismail
    5893160,  # KLCC
    5894144,  # Bukit Bintang
]

# Default to last 6 months
DEFAULT_MONTHS_BACK = 6


def fetch_location_measurements(
    location_id: int,
    base_url: str = get_openaq_base_url(),
    api_key: str = "",
    limit: int = 1000,
    date_from: str | None = None,
    date_to: str | None = None,
) -> JSON:
    """Fetch measurements for a specific location."""
    url = f"{base_url}/locations/{location_id}"
    params: JSON = {"limit": limit}
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to
    headers = {"X-API-Key": api_key} if api_key else {}
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_all_measurements(
    location_ids: list[int],
    base_url: str = get_openaq_base_url(),
    api_key: str = "",
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[JSON]:
    """Fetch measurements for all specified location IDs."""
    all_measurements = []

    for location_id in location_ids:
        try:
            data = fetch_location_measurements(
                location_id,
                base_url=base_url,
                api_key=api_key,
                limit=1000,
                date_from=date_from,
                date_to=date_to,
            )
            if data:
                data["location_id"] = location_id
                all_measurements.append(data)
                logger.info("Fetched OpenAQ measurements: location_id=%s", location_id)
        except Exception as e:
            logger.warning(
                "OpenAQ measurement fetch failed: location_id=%s error=%s",
                location_id,
                e,
            )
            continue

    return all_measurements


def save_to_bronze(
    measurements: list[JSON],
    output_dir: Path,
) -> Path:
    """Save extracted data as JSON to bronze layer."""
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    measurements_file = output_dir / f"openaq_measurements_{timestamp}.json"
    with open(measurements_file, "w", encoding="utf-8") as f:
        json.dump(measurements, f, ensure_ascii=False, indent=2)
    logger.info(
        "Saved OpenAQ bronze measurements: record_count=%s output_path=%s",
        len(measurements),
        measurements_file,
    )

    return measurements_file


def get_date_range(months_back: int = DEFAULT_MONTHS_BACK) -> tuple[str, str]:
    """Calculate date range for the last N months."""
    date_to = datetime.now().strftime("%Y-%m-%dT%H:%M:%S+00:00")
    date_from = (datetime.now() - timedelta(days=months_back * 30)).strftime(
        "%Y-%m-%dT%H:%M:%S+00:00"
    )
    return date_from, date_to


def extract_openaq(months_back: int = DEFAULT_MONTHS_BACK) -> tuple[Path | None, Path]:
    """Main extraction function for OpenAQ data.

    Args:
        months_back: Number of months of historical data to fetch (default: 6)

    Returns:
        Tuple of (locations_file, measurements_file). locations_file is None since
        we fetch directly from location IDs without a separate locations file.
    """
    bronze_path = get_data_bronze_path()
    openaq_dir = bronze_path / "openaq"

    base_url = get_openaq_base_url()
    api_key = get_openaq_api_key()

    # Calculate date range for last N months
    date_from, date_to = get_date_range(months_back)
    logger.info(
        "Fetching OpenAQ measurements: date_from=%s date_to=%s location_count=%s",
        date_from,
        date_to,
        len(MALAYSIA_LOCATION_IDS),
    )

    # Fetch measurements for all Malaysia location IDs
    measurements = fetch_all_measurements(
        MALAYSIA_LOCATION_IDS,
        base_url=base_url,
        api_key=api_key,
        date_from=date_from,
        date_to=date_to,
    )

    measurements_file = save_to_bronze(measurements, openaq_dir)

    # Return tuple to match pipeline expectations (locations_file is None)
    return None, measurements_file


if __name__ == "__main__":
    extract_openaq()

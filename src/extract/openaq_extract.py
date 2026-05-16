"""Extract air quality data from OpenAQ API for Malaysia."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from ..utils.config import get_data_bronze_path, get_openaq_api_key, get_openaq_base_url
from ..utils.constants import JSON, OPENAQ_COUNTRY_CODE, DEFAULT_PAGE_SIZE, REQUEST_TIMEOUT
from ..utils.date_helpers import get_timestamp
from ..utils.path_helpers import ensure_dir
from ..utils.logger import DEFAULT_LOGGER as logger


def fetch_openaq_locations(
    base_url: str = get_openaq_base_url(),
    api_key: str = "",
    country: str = "MY",
    limit: int = 1000,
    offset: int = 0,
) -> dict[str, Any]:
    """Fetch monitoring locations in Malaysia from OpenAQ API."""
    url = f"{base_url}/locations"
    params = {
        "country": country,
        "limit": limit,
        "offset": offset,
    }
    headers = {"X-API-Key": api_key} if api_key else {}
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_location_measurements(
    location_id: int,
    base_url: str = get_openaq_base_url(),
    api_key: str = "",
    limit: int = 1000,
) -> dict[str, Any]:
    """Fetch measurements for a specific location."""
    url = f"{base_url}/locations/{location_id}"
    params = {"limit": limit}
    headers = {"X-API-Key": api_key} if api_key else {}
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_all_openaq_locations(
    base_url: str = get_openaq_base_url(),
    api_key: str = "",
) -> list[dict[str, Any]]:
    """Fetch all monitoring locations in Malaysia using pagination."""
    all_locations = []
    limit = 1000
    offset = 0

    while True:
        data = fetch_openaq_locations(
            base_url=base_url,
            api_key=api_key,
            limit=limit,
            offset=offset,
        )

        if isinstance(data, dict) and "results" in data:
            records = data["results"]
        elif isinstance(data, list):
            records = data
        else:
            records = [data] if data else []

        if not records:
            break

        all_locations.extend(records)
        print(f"Fetched {len(records)} locations (total: {len(all_locations)})")

        if len(records) < limit:
            break

        offset += limit

    return all_locations


def fetch_all_measurements(
    locations: list[dict[str, Any]],
    base_url: str = get_openaq_base_url(),
    api_key: str = "",
) -> list[dict[str, Any]]:
    """Fetch measurements for all locations."""
    all_measurements = []

    for location in locations:
        location_id = location.get("id")
        if not location_id:
            continue

        try:
            data = fetch_location_measurements(
                location_id,
                base_url=base_url,
                api_key=api_key,
                limit=1000,
            )
            if data:
                data["location_id"] = location_id
                data["location_name"] = location.get("location", "unknown")
                all_measurements.append(data)
                print(f"Fetched measurements for location {location_id}")
        except Exception as e:
            print(f"Error fetching measurements for location {location_id}: {e}")
            continue

    return all_measurements


def save_to_bronze(
    locations: list[dict[str, Any]],
    measurements: list[dict[str, Any]],
    output_dir: Path,
) -> tuple[Path, Path]:
    """Save extracted data as JSON to bronze layer."""
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    locations_file = output_dir / f"openaq_locations_{timestamp}.json"
    with open(locations_file, "w", encoding="utf-8") as f:
        json.dump(locations, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(locations)} locations to {locations_file}")

    measurements_file = output_dir / f"openaq_measurements_{timestamp}.json"
    with open(measurements_file, "w", encoding="utf-8") as f:
        json.dump(measurements, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(measurements)} measurements to {measurements_file}")

    return locations_file, measurements_file


def extract_openaq() -> tuple[Path, Path]:
    """Main extraction function for OpenAQ data."""
    bronze_path = get_data_bronze_path()
    openaq_dir = bronze_path / "openaq"

    base_url = get_openaq_base_url()
    api_key = get_openaq_api_key()

    locations = fetch_all_openaq_locations(base_url=base_url, api_key=api_key)
    measurements = fetch_all_measurements(locations, base_url=base_url, api_key=api_key)

    locations_file, measurements_file = save_to_bronze(locations, measurements, openaq_dir)

    return locations_file, measurements_file


if __name__ == "__main__":
    extract_openaq()
"""
OpenAQ Air Quality Data Extraction

Extracts global air quality data from OpenAQ API v3.
Supports querying by country, parameter, and date range.

API Documentation: https://docs.openaq.org/
API Key: https://explore.openaq.org (free registration required)
"""

import os
from pathlib import Path
from dotenv import load_dotenv

import requests
import pandas as pd

# Load .env file if present
load_dotenv()

from datetime import datetime
from typing import Optional


OPENAQ_BASE_URL = "https://api.openaq.org/v3"
OUTPUT_DIR = Path("data/bronze")


def get_openaq_client(api_key: Optional[str] = None) -> dict:
    """Create headers for OpenAQ API requests."""
    key = api_key or os.environ.get("OPENAQ_API_KEY")
    if not key:
        raise ValueError(
            "OpenAQ API key required. Get one at https://explore.openaq.org "
            "and set via OPENAQ_API_KEY env var or pass as parameter."
        )
    return {"X-API-Key": key}


def fetch_locations(country: str = "MY", limit: int = 1000, api_key: Optional[str] = None) -> list:
    """Fetch monitoring locations for a country."""
    headers = get_openaq_client(api_key)
    params = {"country": country, "limit": limit}

    print(f"Fetching OpenAQ locations for {country}...")
    response = requests.get(f"{OPENAQ_BASE_URL}/locations", headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    print(f"Found {len(data.get('results', []))} locations")
    return data.get("results", [])


def fetch_measurements(
    location_id: int,
    parameter: str = "pm25",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 1000,
    api_key: Optional[str] = None
) -> list:
    """Fetch measurements for a specific location."""
    headers = get_openaq_client(api_key)
    params = {
        "parameter": parameter,
        "limit": limit,
    }
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to

    response = requests.get(
        f"{OPENAQ_BASE_URL}/locations/{location_id}/measurements",
        headers=headers,
        params=params
    )
    response.raise_for_status()

    data = response.json()
    return data.get("results", [])


def fetch_country_measurements(
    country: str = "MY",
    parameter: str = "pm25",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 1000,
    api_key: Optional[str] = None
) -> pd.DataFrame:
    """Fetch all measurements for a country."""
    headers = get_openaq_client(api_key)
    params = {
        "country": country,
        "parameter": parameter,
        "limit": limit,
    }
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to

    print(f"Fetching {parameter} measurements for {country}...")
    response = requests.get(f"{OPENAQ_BASE_URL}/measurements", headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    results = data.get("results", [])

    if not results:
        print("No results returned")
        return pd.DataFrame()

    # Flatten nested structure into DataFrame
    records = []
    for item in results:
        record = {
            "location_id": item.get("locationId"),
            "location_name": item.get("location"),
            "country": item.get("country"),
            "parameter": item.get("parameter"),
            "value": item.get("value"),
            "unit": item.get("unit"),
            "datetime_utc": item.get("datetime").get("utc") if item.get("datetime") else None,
            "datetime_local": item.get("datetime").get("local") if item.get("datetime") else None,
            "date_updated": item.get("date").get("updated") if item.get("date") else None,
        }
        records.append(record)

    df = pd.DataFrame(records)
    print(f"Fetched {len(df)} measurements")
    return df


def save_bronze(df: pd.DataFrame, output_dir: Path = OUTPUT_DIR) -> Path:
    """Save raw data to bronze layer as Parquet."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "openaq_air_quality.parquet"
    df.to_parquet(output_path, index=False)
    print(f"Saved to {output_path}")
    return output_path


def run(
    country: str = "MY",
    parameter: str = "pm25",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    api_key: Optional[str] = None
):
    """Main extraction workflow."""
    df = fetch_country_measurements(
        country=country,
        parameter=parameter,
        date_from=date_from,
        date_to=date_to,
        api_key=api_key
    )

    if df.empty:
        print("No data fetched")
        return None

    print("\nData Preview:")
    print(df.head())
    print(f"\nColumns: {list(df.columns)}")

    save_bronze(df)
    return df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract OpenAQ air quality data")
    parser.add_argument("--country", default="MY", help="Country code (default: MY)")
    parser.add_argument("--parameter", default="pm25", help="Parameter to fetch (default: pm25)")
    parser.add_argument("--date-from", help="Start date (ISO format)")
    parser.add_argument("--date-to", help="End date (ISO format)")
    parser.add_argument("--api-key", help="OpenAQ API key (or set OPENAQ_API_KEY env var)")

    args = parser.parse_args()

    run(
        country=args.country,
        parameter=args.parameter,
        date_from=args.date_from,
        date_to=args.date_to,
        api_key=args.api_key
    )
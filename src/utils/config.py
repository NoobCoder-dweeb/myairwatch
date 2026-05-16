"""Configuration loader for air quality data extraction."""

import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")


def get_openaq_api_key() -> str:
    """Get OpenAQ API key from environment variables."""
    key = os.getenv("OPENAQ_API_KEY", "")
    if not key or key == "your_api_key_here":
        raise ValueError("OPENAQ_API_KEY not set in .env file")
    return key


def get_data_bronze_path() -> Path:
    """Get bronze layer path from environment variables."""
    path = os.getenv("DATA_BRONZE_PATH")
    if not path:
        raise ValueError("DATA_BRONZE_PATH not set in .env file")
    return Path(path)


def get_data_silver_path() -> Path:
    """Get silver layer path from environment variables."""
    path = os.getenv("DATA_SILVER_PATH")
    if not path:
        raise ValueError("DATA_SILVER_PATH not set in .env file")
    return Path(path)


def get_openaq_base_url() -> str:
    """Get OpenAQ base URL from environment variables."""
    return os.getenv("OPENAQ_BASE_URL", "https://api.openaq.org/v3")


def get_opendosm_base_url() -> str:
    """Get OpenDOSM base URL from environment variables."""
    return os.getenv("OPENDOSM_BASE_URL", "https://api.data.gov.my/data-catalogue")


def get_airflow_uid() -> int:
    """Get Airflow UID from environment variables."""
    uid = os.getenv("AIRFLOW_UID")
    if uid:
        return int(uid)
    return 1000
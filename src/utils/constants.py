"""Constants for air quality data extraction."""

from typing import Any

JSON = dict[str, Any]

OPENDOSM_DATASET_ID = "air_pollution"
OPENAQ_COUNTRY_CODE = "MY"

DEFAULT_PAGE_SIZE = 1000
REQUEST_TIMEOUT = 30
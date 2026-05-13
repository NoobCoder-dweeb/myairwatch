"""
OpenDOSM Air Quality Data Extraction

Extracts Malaysia air quality data from OpenDOSM API.
Data includes monthly averages of: CO, NO², O³, PM₁₀, PM₂.₅, SO²

API Documentation: https://developer.data.gov.my/
Data Catalogue: https://open.dosm.gov.my/ms-MY/data-catalogue/air_pollution
"""

import io
from pathlib import Path
from dotenv import load_dotenv

import pandas as pd
import pyarrow.parquet as pq
import requests

# Load .env file if present
load_dotenv()

OPENDOSM_AIR_POLLUTION_URL = (
    "https://storage.data.gov.my/environment/air_pollution.parquet"
)
OUTPUT_DIR = Path("data/bronze/opendosm")


def extract_opendosm() -> pd.DataFrame:
    """Extract air quality data from OpenDOSM parquet file."""
    print("Downloading OpenDOSM air quality data...")
    response = requests.get(OPENDOSM_AIR_POLLUTION_URL)
    response.raise_for_status()

    df = pq.read_table(io.BytesIO(response.content)).to_pandas()
    print(f"Downloaded {len(df)} rows")
    return df


def save_bronze(df: pd.DataFrame, output_dir: Path = OUTPUT_DIR) -> Path:
    """Save raw data to bronze layer as Parquet."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "opendosm_air_quality.parquet"
    df.to_parquet(output_path, index=False)
    print(f"Saved to {output_path}")
    return output_path


def run():
    """Main extraction workflow."""
    df = extract_opendosm()

    print("\nData Preview:")
    print(df.head())
    print(f"\nColumns: {list(df.columns)}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")

    save_bronze(df)
    return df


if __name__ == "__main__":
    run()

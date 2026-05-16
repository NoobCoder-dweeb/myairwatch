#!/usr/bin/env python
"""Main entry point for running air quality data extraction."""

import argparse
import sys

from .spark_ingest import (
    ingest_openaq_bronze_to_silver,
    ingest_opendosm_bronze_to_silver,
)
from ..utils.config import get_data_bronze_path, get_data_silver_path
from .opendosm_extract import extract_opendosm


def run_opendosm():
    """Run OpenDOSM extraction."""

    print("=" * 50)
    print("Extracting OpenDOSM data...")
    print("=" * 50)
    filepath = extract_opendosm()
    print(f"Done! Data saved to: {filepath}")
    return filepath


def run_openaq():
    """Run OpenAQ extraction."""
    from .openaq_extract import extract_openaq

    print("=" * 50)
    print("Extracting OpenAQ data...")
    print("=" * 50)
    locations_file, measurements_file = extract_openaq()
    print(f"Done! Data saved to:")
    print(f"  - Locations: {locations_file}")
    print(f"  - Measurements: {measurements_file}")
    return locations_file, measurements_file


def run_spark_ingest(source: str):
    """Run PySpark ingestion from bronze to silver."""

    bronze = get_data_bronze_path()
    silver = get_data_silver_path()

    if source == "opendosm":
        opendosm_dir = bronze / "opendosm"
        if not opendosm_dir.exists():
            print(f"Error: OpenDOSM bronze directory not found: {opendosm_dir}")
            sys.exit(1)

        json_files = list(opendosm_dir.glob("*.json"))
        if not json_files:
            print(f"Error: No JSON files found in {opendosm_dir}")
            sys.exit(1)

        latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
        print(f"Ingesting OpenDOSM from {latest_file}")
        ingest_opendosm_bronze_to_silver(latest_file, silver)

    elif source == "openaq":
        openaq_dir = bronze / "openaq"
        if not openaq_dir.exists():
            print(f"Error: OpenAQ bronze directory not found: {openaq_dir}")
            sys.exit(1)

        json_files = list(openaq_dir.glob("*_measurements_*.json"))
        if not json_files:
            print(f"Error: No measurement JSON files found in {openaq_dir}")
            sys.exit(1)

        latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
        print(f"Ingesting OpenAQ from {latest_file}")
        ingest_openaq_bronze_to_silver(latest_file, silver)

    else:
        print(f"Error: Unknown source '{source}'. Use 'opendosm' or 'openaq'")
        sys.exit(1)

    print("Done!")


def run_all():
    """Run all extractions and ingestion."""
    print("=" * 50)
    print("Running full pipeline...")
    print("=" * 50)

    run_opendosm()
    print()

    run_openaq()
    print()

    print("=" * 50)
    print("Running PySpark ingestion...")
    print("=" * 50)
    run_spark_ingest("opendosm")
    print()
    run_spark_ingest("openaq")

    print("=" * 50)
    print("Pipeline complete!")
    print("=" * 50)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Air Quality Data Extraction Pipeline")
    parser.add_argument(
        "--source",
        choices=["opendosm", "openaq", "spark-opendosm", "spark-openaq", "all"],
        default="all",
        help="Source to extract or ingest",
    )
    parser.add_argument(
        "--no-ingest",
        action="store_true",
        help="Skip PySpark ingestion step",
    )

    args = parser.parse_args()

    if args.source == "all":
        run_all()
    elif args.source == "opendosm":
        run_opendosm()
    elif args.source == "openaq":
        run_openaq()
    elif args.source == "spark-opendosm":
        run_spark_ingest("opendosm")
    elif args.source == "spark-openaq":
        run_spark_ingest("openaq")


if __name__ == "__main__":
    main()

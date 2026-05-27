#!/usr/bin/env python
"""Main entry point for running air quality data extraction."""

import argparse
import sys

from .spark_ingest import (
    ingest_openaq_bronze_to_silver,
    ingest_opendosm_bronze_to_silver,
)
from ..utils.config import get_data_bronze_path, get_data_silver_path
from ..utils.logger import DEFAULT_LOGGER as logger
from .opendosm_extract import extract_opendosm


def run_opendosm():
    """Run OpenDOSM extraction."""

    logger.info("Starting OpenDOSM extraction")
    filepath = extract_opendosm()
    logger.info("OpenDOSM extraction complete: output_path=%s", filepath)
    return filepath


def run_openaq():
    """Run OpenAQ extraction."""
    from .openaq_extract import extract_openaq

    logger.info("Starting OpenAQ extraction")
    locations_file, measurements_file = extract_openaq()
    logger.info(
        "OpenAQ extraction complete: locations_path=%s measurements_path=%s",
        locations_file,
        measurements_file,
    )
    return locations_file, measurements_file


def run_spark_ingest(source: str):
    """Run PySpark ingestion from bronze to silver."""

    bronze = get_data_bronze_path()
    silver = get_data_silver_path()

    if source == "opendosm":
        opendosm_dir = bronze / "opendosm"
        if not opendosm_dir.exists():
            logger.error("OpenDOSM bronze directory not found: path=%s", opendosm_dir)
            sys.exit(1)

        json_files = list(opendosm_dir.glob("*.json"))
        if not json_files:
            logger.error("No OpenDOSM JSON files found: path=%s", opendosm_dir)
            sys.exit(1)

        latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
        logger.info("Starting OpenDOSM ingestion: input_path=%s", latest_file)
        ingest_opendosm_bronze_to_silver(latest_file, silver)

    elif source == "openaq":
        openaq_dir = bronze / "openaq"
        if not openaq_dir.exists():
            logger.error("OpenAQ bronze directory not found: path=%s", openaq_dir)
            sys.exit(1)

        json_files = list(openaq_dir.glob("*_measurements_*.json"))
        if not json_files:
            logger.error("No OpenAQ measurement JSON files found: path=%s", openaq_dir)
            sys.exit(1)

        latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
        logger.info("Starting OpenAQ ingestion: input_path=%s", latest_file)
        ingest_openaq_bronze_to_silver(latest_file, silver)

    else:
        logger.error("Unsupported ingestion source: source=%s", source)
        sys.exit(1)

    logger.info("Spark ingestion complete: source=%s", source)


def run_all():
    """Run all extractions and ingestion."""
    logger.info("Starting full extraction and ingestion run")

    run_opendosm()

    run_openaq()

    logger.info("Starting Spark ingestion phase")
    run_spark_ingest("opendosm")
    run_spark_ingest("openaq")

    logger.info("Full extraction and ingestion run complete")


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

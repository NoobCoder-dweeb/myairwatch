#!/usr/bin/env python
# Usage: uv run pipeline.py
"""Universal ETL pipeline for Malaysia air quality data.

This pipeline orchestrates the full ETL/ELT flow:
1. Extract: Fetch data from OpenAQ and OpenDOSM APIs
2. Load (Bronze): Save raw JSON to bronze layer
3. Transform (Silver): PySpark ingestion to Parquet

Usage:
    uv run pipeline.py                    # Run full pipeline
    uv run pipeline.py --source openaq   # Run only OpenAQ extraction
    uv run pipeline.py --source opendosm # Run only OpenDOSM extraction
    uv run pipeline.py --skip-extract    # Run only ingestion (silver layer)
    uv run pipeline.py --dry-run         # Show what would run without executing
"""

import argparse
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from src.extract.openaq_extract import extract_openaq
from src.extract.opendosm_extract import extract_opendosm
from src.extract.spark_ingest import (
    ingest_openaq_bronze_to_silver,
    ingest_opendosm_bronze_to_silver,
)
from src.utils.config import get_data_bronze_path, get_data_silver_path
from src.utils.logger import DEFAULT_LOGGER as logger


def run_opendosm_extract() -> Path:
    """Extract data from OpenDOSM API."""
    logger.info("Starting OpenDOSM extraction")
    filepath = extract_opendosm()
    logger.info("OpenDOSM extraction complete: output_path=%s", filepath)
    return filepath


def run_openaq_extract() -> tuple[Path, Path]:
    """Extract data from OpenAQ API."""
    logger.info("Starting OpenAQ extraction")
    locations_file, measurements_file = extract_openaq()
    logger.info(
        "OpenAQ extraction complete: locations_path=%s measurements_path=%s",
        locations_file,
        measurements_file,
    )
    return locations_file, measurements_file


def run_opendosm_ingest():
    """Ingest OpenDOSM bronze data to silver layer."""
    bronze_path = get_data_bronze_path()
    silver_path = get_data_silver_path()
    opendosm_dir = bronze_path / "opendosm"

    if not opendosm_dir.exists():
        raise FileNotFoundError(f"OpenDOSM bronze directory not found: {opendosm_dir}")

    json_files = list(opendosm_dir.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {opendosm_dir}")

    latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
    logger.info("Starting OpenDOSM ingestion: input_path=%s", latest_file)

    df = ingest_opendosm_bronze_to_silver(latest_file, silver_path)
    logger.info("OpenDOSM ingestion complete")
    return df


def run_openaq_ingest():
    """Ingest OpenAQ bronze data to silver layer."""
    bronze_path = get_data_bronze_path()
    silver_path = get_data_silver_path()
    openaq_dir = bronze_path / "openaq"

    if not openaq_dir.exists():
        raise FileNotFoundError(f"OpenAQ bronze directory not found: {openaq_dir}")

    json_files = list(openaq_dir.glob("*_measurements_*.json"))
    if not json_files:
        raise FileNotFoundError(f"No measurement JSON files found in {openaq_dir}")

    latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
    logger.info("Starting OpenAQ ingestion: input_path=%s", latest_file)

    df = ingest_openaq_bronze_to_silver(latest_file, silver_path)
    logger.info("OpenAQ ingestion complete")
    return df


def run_full_pipeline(args: argparse.Namespace):
    """Execute the full ETL pipeline."""
    logger.info(
        "Starting pipeline execution: source=%s skip_extract=%s skip_ingest=%s",
        args.source,
        args.skip_extract,
        args.skip_ingest,
    )

    # Phase 1: Extraction
    if not args.skip_extract:
        logger.info("Starting extraction phase")

        if args.source in ("all", "opendosm"):
            run_opendosm_extract()

        if args.source in ("all", "openaq"):
            run_openaq_extract()

        logger.info("Extraction phase complete")
    else:
        logger.info("Skipping extraction phase")

    # Phase 2: Ingestion (bronze -> silver)
    if not args.skip_ingest:
        logger.info("Starting ingestion phase")

        if args.source in ("all", "opendosm"):
            run_opendosm_ingest()

        if args.source in ("all", "openaq"):
            run_openaq_ingest()

        logger.info("Ingestion phase complete")
    else:
        logger.info("Skipping ingestion phase")

    logger.info("Pipeline execution complete")


def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(
        description="Malaysia Air Quality ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--source",
        choices=["all", "openaq", "opendosm"],
        default="all",
        help="Data source to process (default: all)",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip extraction phase (use existing bronze data)",
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip ingestion phase (bronze -> silver)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would run without executing",
    )

    args = parser.parse_args()

    if args.dry_run:
        logger.info(
            "Dry run complete: source=%s skip_extract=%s skip_ingest=%s",
            args.source,
            args.skip_extract,
            args.skip_ingest,
        )
        return

    try:
        run_full_pipeline(args)
    except Exception as e:
        logger.error("Pipeline execution failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()

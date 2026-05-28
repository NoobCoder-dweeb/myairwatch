"""Spark entry points for bronze-to-silver air-quality transforms."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from ..utils.config import get_data_bronze_path, get_data_silver_path
from ..utils.logger import DEFAULT_LOGGER as logger
from .clean_air_quality import transform_air_quality_df
from .partition_writer import write_silver_partitioned


def read_bronze_json(
    spark: SparkSession,
    bronze_file: str | Path,
) -> DataFrame:
    """Read pretty-printed bronze JSON arrays into Spark."""
    return spark.read.option("multiLine", "true").json(str(bronze_file))


def transform_bronze_file_to_silver(
    spark: SparkSession,
    bronze_file: str | Path,
    silver_base_path: str | Path,
    source: str,
    mode: str = "overwrite",
) -> DataFrame:
    """Transform one bronze JSON file and write cleaned silver Parquet data."""
    bronze_df = read_bronze_json(spark, bronze_file)
    silver_df = transform_air_quality_df(bronze_df, source=source)
    output_path = Path(silver_base_path) / "air_quality"

    record_count = silver_df.count()
    logger.info(
        "Transformed bronze records to silver: source=%s input_path=%s record_count=%s",
        source,
        bronze_file,
        record_count,
    )

    write_silver_partitioned(silver_df, output_path, mode=mode)
    logger.info(
        "Silver partition write complete: source=%s output_path=%s",
        source,
        output_path,
    )
    return silver_df


def latest_bronze_file(source: str, bronze_base_path: str | Path | None = None) -> Path:
    """Return the newest bronze file for a supported source."""
    bronze_path = Path(bronze_base_path) if bronze_base_path else get_data_bronze_path()
    if source == "opendosm":
        source_dir = bronze_path / "opendosm"
        pattern = "*.json"
    elif source == "openaq":
        source_dir = bronze_path / "openaq"
        pattern = "*_measurements_*.json"
    else:
        raise ValueError(f"Unsupported air-quality source: {source}")

    files = list(source_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(
            f"No bronze files found for source '{source}' in {source_dir}"
        )
    return max(files, key=lambda path: path.stat().st_mtime)


def transform_latest_bronze_to_silver(
    spark: SparkSession,
    source: str,
    bronze_base_path: str | Path | None = None,
    silver_base_path: str | Path | None = None,
    mode: str = "overwrite",
) -> DataFrame:
    """Transform the newest bronze file for a source into silver data."""
    bronze_file = latest_bronze_file(source, bronze_base_path)
    silver_path = Path(silver_base_path) if silver_base_path else get_data_silver_path()
    return transform_bronze_file_to_silver(
        spark,
        bronze_file=bronze_file,
        silver_base_path=silver_path,
        source=source,
        mode=mode,
    )

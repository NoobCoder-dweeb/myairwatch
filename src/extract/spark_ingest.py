"""PySpark ingestion utilities for air quality data."""

from pathlib import Path
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from ..utils.config import get_data_bronze_path, get_data_silver_path
from ..utils.logger import DEFAULT_LOGGER as logger
from ..transform.clean_air_quality import transform_air_quality_df
from ..transform.partition_writer import write_silver_partitioned


def create_spark_session(app_name: str = "AirQualityIngestion") -> SparkSession:
    """Create and configure SparkSession."""
    # Create spark session
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # set log level
    return spark


def read_json_to_df(
    spark: SparkSession,
    path: str | Path,
    schema: Optional[Any] = None,
) -> DataFrame:
    """Read JSON files into Spark DataFrame.

    Bronze extraction writes pretty-printed JSON arrays, so Spark needs
    multiLine mode instead of its default newline-delimited JSON mode.
    """
    path_str = str(path)
    reader = spark.read.option("multiLine", "true")
    if schema:
        reader = reader.schema(schema)
    return reader.json(path_str)


def write_partitioned_df(
    df: DataFrame,
    output_path: str | Path,
    partition_cols: list[str] = [],
    mode: str = "overwrite",  # overwrite old data
    format: str = "parquet",
) -> None:
    """Write DataFrame with partitioning."""
    writer = df.write.mode(mode).format(format)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(str(output_path))


def ingest_bronze_to_silver(
    bronze_path: str | Path,
    silver_path: str | Path,
    source: str = "opendosm",
) -> DataFrame:
    """Clean and ingest bronze data to the silver layer."""
    spark = create_spark_session(f"Ingest_{source}")

    try:
        bronze_df = read_json_to_df(spark, bronze_path)
        df = transform_air_quality_df(bronze_df, source=source)
        record_count = df.count()

        logger.info(
            "Cleaned bronze records for silver: source=%s input_path=%s record_count=%s",
            source,
            bronze_path,
            record_count,
        )

        write_silver_partitioned(df, silver_path)

        logger.info(
            "Silver layer write complete: source=%s output_path=%s",
            source,
            silver_path,
        )
        return df

    finally:
        spark.stop()


def ingest_opendosm_bronze_to_silver(
    bronze_file: str | Path,
    silver_base_path: str | Path,
) -> DataFrame:
    """Ingest OpenDOSM data from bronze to silver."""
    silver_path = Path(silver_base_path) / "air_quality"
    return ingest_bronze_to_silver(bronze_file, silver_path, source="opendosm")


def ingest_openaq_bronze_to_silver(
    bronze_file: str | Path,
    silver_base_path: str | Path,
) -> DataFrame:
    """Ingest OpenAQ data from bronze to silver."""
    silver_path = Path(silver_base_path) / "air_quality"
    return ingest_bronze_to_silver(bronze_file, silver_path, source="openaq")


if __name__ == "__main__":
    # retrieve path to directory
    bronze = get_data_bronze_path()
    silver = get_data_silver_path()

    opendosm_dir = bronze / "opendosm"
    openaq_dir = bronze / "openaq"

    # ingest each directory
    if opendosm_dir.exists():
        json_files = list(opendosm_dir.glob("*.json"))
        if json_files:
            latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
            logger.info("Starting OpenDOSM ingestion: input_path=%s", latest_file)
            ingest_opendosm_bronze_to_silver(latest_file, silver)

    if openaq_dir.exists():
        json_files = list(openaq_dir.glob("*_measurements_*.json"))
        if json_files:
            latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
            logger.info("Starting OpenAQ ingestion: input_path=%s", latest_file)
            ingest_openaq_bronze_to_silver(latest_file, silver)

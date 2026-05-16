"""PySpark ingestion utilities for air quality data."""

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..utils.config import get_data_bronze_path, get_data_silver_path


def create_spark_session(app_name: str = "AirQualityIngestion") -> SparkSession:
    """Create and configure SparkSession."""
    # Create spark session
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # set log level
    return spark


def read_json_to_df(
    spark: SparkSession,
    path: str | Path,
    schema: Optional[Any] = None,
) -> DataFrame:
    """Read JSON files into Spark DataFrame."""
    path_str = str(path)
    if schema:
        return spark.read.schema(schema).json(path_str)
    return spark.read.json(path_str)


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
    partition_by_date: bool = True,
) -> DataFrame:
    """Ingest data from bronze to silver layer with optional partitioning."""
    spark = create_spark_session(f"Ingest_{source}")

    try:
        df = read_json_to_df(spark, bronze_path)

        print(f"Loaded {df.count()} records from bronze layer")

        if partition_by_date:
            if "date" in df.columns:
                df = df.withColumn("year", F.year(F.col("date")))
                df = df.withColumn("month", F.month(F.col("date")))
                df = df.withColumn("day", F.dayofmonth(F.col("date")))

                output = str(silver_path)
                write_partitioned_df(
                    df,
                    output,
                    partition_cols=["year", "month", "day"],
                )
            else:
                df = df.withColumn("ingestion_year", F.year(F.current_timestamp()))
                df = df.withColumn("ingestion_month", F.month(F.current_timestamp()))
                df = df.withColumn("ingestion_day", F.dayofmonth(F.current_timestamp()))

                output = str(silver_path)
                write_partitioned_df(
                    df,
                    output,
                    partition_cols=[
                        "ingestion_year",
                        "ingestion_month",
                        "ingestion_day",
                    ],
                )
        else:
            df.write.mode("overwrite").json(str(silver_path))

        print(f"Silver layer written to {silver_path}")
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
            print(f"Ingesting OpenDOSM from {latest_file}")
            ingest_opendosm_bronze_to_silver(latest_file, silver)

    if openaq_dir.exists():
        json_files = list(openaq_dir.glob("*_measurements_*.json"))
        if json_files:
            latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
            print(f"Ingesting OpenAQ from {latest_file}")
            ingest_openaq_bronze_to_silver(latest_file, silver)

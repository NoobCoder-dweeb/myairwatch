"""Silver-layer partition writing helpers."""

from pathlib import Path

from pyspark.sql import DataFrame

SILVER_PARTITION_COLUMNS = ["year", "month", "day", "source"]


def write_silver_partitioned(
    df: DataFrame,
    output_path: str | Path,
    mode: str = "overwrite",
) -> None:
    """Write silver data as Parquet partitioned by date and source."""
    missing = [
        column for column in SILVER_PARTITION_COLUMNS if column not in df.columns
    ]
    if missing:
        raise ValueError(f"Missing required silver partition columns: {missing}")

    (
        df.write.mode(mode)
        .format("parquet")
        .partitionBy(*SILVER_PARTITION_COLUMNS)
        .save(str(output_path))
    )

"""Load silver air-quality parquet into BigQuery and run warehouse models."""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable
from uuid import uuid4

import pyarrow.dataset as ds
import pyarrow.parquet as pq
from google.cloud import bigquery

from src.quality.duplicate_checks import (
    assert_bigquery_no_duplicates,
    assert_no_duplicate_rows,
)
from src.quality.freshness_checks import (
    assert_bigquery_has_rows,
    assert_table_has_rows,
)
from src.quality.null_checks import assert_bigquery_no_nulls, assert_no_nulls
from src.quality.pollutant_range_checks import (
    assert_bigquery_pollutant_ranges,
    assert_pollutant_ranges,
)
from src.quality.schema_validation import assert_columns_present
from src.transform.clean_air_quality import SILVER_COLUMNS
from src.utils.config import BASE_DIR
from src.utils.logger import DEFAULT_LOGGER as logger


AIR_QUALITY_TABLE = "air_quality_readings"
DEFAULT_DATASET = "myairwatch_staging"

BIGQUERY_SCHEMA = [
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("source_location_id", "STRING"),
    bigquery.SchemaField("location_name", "STRING"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("latitude", "FLOAT"),
    bigquery.SchemaField("longitude", "FLOAT"),
    bigquery.SchemaField("pollutant", "STRING"),
    bigquery.SchemaField("unit", "STRING"),
    bigquery.SchemaField("reading_value", "FLOAT"),
    bigquery.SchemaField("observed_at", "TIMESTAMP"),
    bigquery.SchemaField("observed_date", "DATE"),
    bigquery.SchemaField("year", "INTEGER"),
    bigquery.SchemaField("month", "INTEGER"),
    bigquery.SchemaField("day", "INTEGER"),
    bigquery.SchemaField("health_risk_category", "STRING"),
]

REQUIRED_COLUMNS = [
    "source",
    "pollutant",
    "observed_at",
    "observed_date",
    "year",
    "month",
    "day",
]

MERGE_KEY_COLUMNS = [
    "source",
    "source_location_id",
    "location_name",
    "pollutant",
    "observed_at",
]


@dataclass(frozen=True)
class LoadResult:
    """Result returned by the BigQuery load pipeline."""

    target_table_id: str
    temp_table_id: str
    local_row_count: int


@dataclass(frozen=True)
class BigQueryLoadConfig:
    """Configuration for the silver-to-BigQuery load workflow."""

    project_id: str
    dataset_id: str = DEFAULT_DATASET
    target_table: str = AIR_QUALITY_TABLE
    dbt_project_dir: Path = BASE_DIR / "dbt_myairwatch"
    dbt_profiles_dir: Path | None = None
    location: str | None = None
    temp_table_prefix: str = "tmp_air_quality_readings"
    required_columns: list[str] = field(default_factory=lambda: REQUIRED_COLUMNS.copy())
    merge_key_columns: list[str] = field(default_factory=lambda: MERGE_KEY_COLUMNS.copy())

    @property
    def target_table_id(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.target_table}"

    def new_temp_table_id(self) -> str:
        suffix = uuid4().hex[:12]
        return f"{self.project_id}.{self.dataset_id}.{self.temp_table_prefix}_{suffix}"


class AirQualityLoadPipeline:
    """Pipeline for loading local silver parquet into BigQuery and dbt."""

    def __init__(
        self,
        config: BigQueryLoadConfig,
        client: bigquery.Client | None = None,
        runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    ):
        self.config = config
        self.client = client or bigquery.Client(
            project=config.project_id,
            location=config.location,
        )
        self.runner = runner

    def run(self, parquet_path: str | Path) -> LoadResult:
        """Run validation, BigQuery merge, cleanup, and dbt models/tests."""
        local_row_count = validate_silver_parquet(
            parquet_path,
            required_columns=self.config.required_columns,
            merge_key_columns=self.config.merge_key_columns,
        )
        temp_table_id = self.config.new_temp_table_id()

        try:
            load_parquet_to_temp_table(parquet_path, temp_table_id, self.client)
            merge_temp_table(self.client, temp_table_id, self.config)
            run_post_load_checks(self.client, self.config)
        finally:
            drop_table(self.client, temp_table_id)

        run_dbt_models_and_tests(self.config, runner=self.runner)
        return LoadResult(
            target_table_id=self.config.target_table_id,
            temp_table_id=temp_table_id,
            local_row_count=local_row_count,
        )


def validate_silver_parquet(
    parquet_path: str | Path,
    required_columns: list[str] | None = None,
    merge_key_columns: list[str] | None = None,
) -> int:
    """Run local quality checks against silver parquet and return row count."""
    table = _read_silver_table(parquet_path)
    required_columns = required_columns or REQUIRED_COLUMNS
    merge_key_columns = merge_key_columns or MERGE_KEY_COLUMNS

    assert_columns_present(table, SILVER_COLUMNS)
    assert_table_has_rows(table)
    assert_no_nulls(table, required_columns)
    assert_no_duplicate_rows(table, merge_key_columns)
    assert_pollutant_ranges(table)

    return table.num_rows


def load_silver_to_bigquery(
    parquet_path: str | Path,
    config: BigQueryLoadConfig,
    client: bigquery.Client | None = None,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> LoadResult:
    """Run the complete load pipeline."""
    return AirQualityLoadPipeline(config, client=client, runner=runner).run(parquet_path)


def load_parquet_to_temp_table(
    parquet_path: str | Path,
    temp_table_id: str,
    client: bigquery.Client,
) -> None:
    """Load local silver parquet into a temporary BigQuery table."""
    table = _read_silver_table(parquet_path).select(SILVER_COLUMNS)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=BIGQUERY_SCHEMA,
    )

    with tempfile.NamedTemporaryFile(suffix=".parquet") as local_file:
        pq.write_table(table, local_file.name)
        with open(local_file.name, "rb") as parquet_file:
            load_job = client.load_table_from_file(
                parquet_file,
                temp_table_id,
                job_config=job_config,
            )
        load_job.result()

    logger.info("Loaded silver parquet into temporary BigQuery table: %s", temp_table_id)


def merge_temp_table(
    client: bigquery.Client,
    temp_table_id: str,
    config: BigQueryLoadConfig,
) -> None:
    """Merge the temp table into the configured target table."""
    merge_condition = " AND ".join(
        [
            f"target.{column} IS NOT DISTINCT FROM source.{column}"
            for column in config.merge_key_columns
        ]
    )
    update_columns = [
        column for column in SILVER_COLUMNS if column not in config.merge_key_columns
    ]
    update_clause = ",\n        ".join(
        [f"{column} = source.{column}" for column in update_columns]
    )
    insert_columns = ", ".join(SILVER_COLUMNS)
    insert_values = ", ".join([f"source.{column}" for column in SILVER_COLUMNS])

    query = f"""
MERGE `{config.target_table_id}` AS target
USING `{temp_table_id}` AS source
ON {merge_condition}
WHEN MATCHED THEN
  UPDATE SET
        {update_clause}
WHEN NOT MATCHED THEN
  INSERT ({insert_columns})
  VALUES ({insert_values})
"""
    client.query(query).result()
    logger.info("Merged temporary table into BigQuery target: %s", config.target_table_id)


def run_post_load_checks(client: bigquery.Client, config: BigQueryLoadConfig) -> None:
    """Run post-load checks through the quality package."""
    assert_bigquery_has_rows(client, config.target_table_id)
    assert_bigquery_no_nulls(client, config.target_table_id, config.required_columns)
    assert_bigquery_no_duplicates(
        client,
        config.target_table_id,
        config.merge_key_columns,
    )
    assert_bigquery_pollutant_ranges(client, config.target_table_id)


def drop_table(client: bigquery.Client, table_id: str) -> None:
    """Drop a BigQuery table if it exists."""
    client.delete_table(table_id, not_found_ok=True)
    logger.info("Dropped temporary BigQuery table: %s", table_id)


def run_dbt_models_and_tests(
    config: BigQueryLoadConfig,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> None:
    """Run dbt models and tests after the warehouse load succeeds."""
    base_command = ["dbt", "--project-dir", str(config.dbt_project_dir)]
    if config.dbt_profiles_dir:
        base_command.extend(["--profiles-dir", str(config.dbt_profiles_dir)])

    runner([*base_command, "run"], check=True)
    runner([*base_command, "test"], check=True)


def _read_silver_table(parquet_path: str | Path):
    path = Path(parquet_path)
    if not path.exists():
        raise FileNotFoundError(f"Silver parquet path does not exist: {path}")

    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    return dataset.to_table()


def main() -> None:
    """CLI entrypoint for loading local silver parquet into BigQuery."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--silver-path",
        required=True,
        help="Local silver parquet file or directory",
    )
    parser.add_argument("--project-id", required=True, help="Google Cloud project id")
    parser.add_argument("--dataset-id", default=DEFAULT_DATASET)
    parser.add_argument("--target-table", default=AIR_QUALITY_TABLE)
    parser.add_argument("--location", default=None)
    parser.add_argument(
        "--dbt-project-dir",
        type=Path,
        default=BASE_DIR / "dbt_myairwatch",
    )
    parser.add_argument("--dbt-profiles-dir", type=Path, default=None)
    args = parser.parse_args()

    config = BigQueryLoadConfig(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        target_table=args.target_table,
        location=args.location,
        dbt_project_dir=args.dbt_project_dir,
        dbt_profiles_dir=args.dbt_profiles_dir,
    )
    AirQualityLoadPipeline(config).run(args.silver_path)


if __name__ == "__main__":
    main()

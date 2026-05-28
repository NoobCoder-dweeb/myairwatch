from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.load.load_to_bigquery import (
    BigQueryLoadConfig,
    load_silver_to_bigquery,
    validate_silver_parquet,
)
from src.transform.clean_air_quality import SILVER_COLUMNS

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class FakeJob:
    def __init__(self, rows=None):
        self.rows = rows or []

    def result(self):
        return self.rows


class FakeBigQueryClient:
    def __init__(self, scalars=None):
        self.scalars = list(scalars or [1, 0, 0, 0])
        self.loaded_tables = []
        self.deleted_tables = []
        self.queries = []
        self.events = []

    def load_table_from_file(self, parquet_file, table_id, job_config):
        parquet_file.read()
        self.loaded_tables.append((table_id, job_config))
        self.events.append(("load", table_id))
        return FakeJob()

    def query(self, query):
        self.queries.append(query)
        if query.lstrip().upper().startswith("SELECT"):
            self.events.append(("check", query))
            return FakeJob([(self.scalars.pop(0),)])

        self.events.append(("merge", query))
        return FakeJob()

    def delete_table(self, table_id, not_found_ok=False):
        self.deleted_tables.append((table_id, not_found_ok))
        self.events.append(("drop", table_id))


def write_silver_parquet(path):
    values = {
        "source": ["opendosm"],
        "source_location_id": [None],
        "location_name": [None],
        "state": ["Kuala Lumpur"],
        "country": ["Malaysia"],
        "latitude": [3.1],
        "longitude": [101.7],
        "pollutant": ["PM2.5"],
        "unit": ["ug/m3"],
        "reading_value": [12.0],
        "observed_at": [datetime(2026, 5, 27, 0, 0, 0)],
        "observed_date": [date(2026, 5, 27)],
        "year": [2026],
        "month": [5],
        "day": [27],
        "health_risk_category": ["good"],
    }
    table = pa.table({column: values[column] for column in SILVER_COLUMNS})
    pq.write_table(table, path)


def test_validate_silver_parquet_accepts_expected_schema(tmp_path):
    parquet_path = tmp_path / "air_quality.parquet"
    write_silver_parquet(parquet_path)

    assert validate_silver_parquet(parquet_path) == 1


def test_validate_silver_parquet_rejects_missing_columns(tmp_path):
    parquet_path = tmp_path / "bad.parquet"
    pq.write_table(pa.table({"source": ["opendosm"]}), parquet_path)

    with pytest.raises(ValueError, match="missing columns"):
        validate_silver_parquet(parquet_path)


def test_load_silver_to_bigquery_runs_merge_checks_cleanup_and_dbt(tmp_path):
    parquet_path = tmp_path / "air_quality.parquet"
    write_silver_parquet(parquet_path)
    client = FakeBigQueryClient()
    commands = []

    def fake_runner(command, check):
        commands.append((command, check))

    config = BigQueryLoadConfig(
        project_id="test-project",
        dbt_project_dir=tmp_path / "dbt_myairwatch",
    )

    result = load_silver_to_bigquery(
        parquet_path,
        config,
        client=client,
        runner=fake_runner,
    )

    assert result.target_table_id == "test-project.myairwatch_staging.air_quality_readings"
    assert result.local_row_count == 1
    assert [event[0] for event in client.events] == [
        "load",
        "merge",
        "check",
        "check",
        "check",
        "check",
        "drop",
    ]

    temp_table_id = client.loaded_tables[0][0]
    merge_query = client.queries[0]
    assert f"`{temp_table_id}`" in merge_query
    assert "`test-project.myairwatch_staging.air_quality_readings`" in merge_query
    assert "IS NOT DISTINCT FROM" in merge_query
    assert client.deleted_tables == [(temp_table_id, True)]
    assert commands == [
        (["dbt", "--project-dir", str(tmp_path / "dbt_myairwatch"), "run"], True),
        (["dbt", "--project-dir", str(tmp_path / "dbt_myairwatch"), "test"], True),
    ]


def test_temp_table_is_dropped_when_post_load_check_fails(tmp_path):
    parquet_path = tmp_path / "air_quality.parquet"
    write_silver_parquet(parquet_path)
    client = FakeBigQueryClient(scalars=[0])
    config = BigQueryLoadConfig(project_id="test-project")

    with pytest.raises(ValueError, match="is empty"):
        load_silver_to_bigquery(parquet_path, config, client=client)

    assert client.loaded_tables
    assert client.deleted_tables == [(client.loaded_tables[0][0], True)]


def test_validate_silver_parquet_rejects_duplicate_keys(tmp_path):
    parquet_path = tmp_path / "duplicates.parquet"
    values = {
        "source": ["opendosm", "opendosm"],
        "source_location_id": [None, None],
        "location_name": [None, None],
        "state": ["Kuala Lumpur", "Kuala Lumpur"],
        "country": ["Malaysia", "Malaysia"],
        "latitude": [3.1, 3.1],
        "longitude": [101.7, 101.7],
        "pollutant": ["PM2.5", "PM2.5"],
        "unit": ["ug/m3", "ug/m3"],
        "reading_value": [12.0, 12.0],
        "observed_at": [datetime(2026, 5, 27, 0, 0, 0)] * 2,
        "observed_date": [date(2026, 5, 27)] * 2,
        "year": [2026, 2026],
        "month": [5, 5],
        "day": [27, 27],
        "health_risk_category": ["good", "good"],
    }
    table = pa.table({column: values[column] for column in SILVER_COLUMNS})
    pq.write_table(table, parquet_path)

    with pytest.raises(ValueError, match="Duplicate silver rows"):
        validate_silver_parquet(parquet_path)


def test_dbt_project_contains_models_and_tests():
    dbt_dir = PROJECT_ROOT / "dbt_myairwatch"
    required_files = [
        dbt_dir / "dbt_project.yml",
        dbt_dir / "models" / "staging" / "stg_opendosm.sql",
        dbt_dir / "models" / "staging" / "stg_openaq.sql",
        dbt_dir / "models" / "staging" / "schema.yml",
        dbt_dir / "models" / "intermediate" / "int_air_quality_enriched.sql",
        dbt_dir / "models" / "marts" / "fact_air_quality.sql",
        dbt_dir / "models" / "marts" / "schema.yml",
    ]

    for path in required_files:
        assert path.read_text().strip(), f"{path} should not be empty"

    staging_schema = (dbt_dir / "models" / "staging" / "schema.yml").read_text()
    assert "sources:" in staging_schema
    assert "air_quality_readings" in staging_schema
    assert "not_null" in staging_schema
    assert "{{ source('myairwatch_staging', 'air_quality_readings') }}" in (
        dbt_dir / "models" / "staging" / "stg_opendosm.sql"
    ).read_text()

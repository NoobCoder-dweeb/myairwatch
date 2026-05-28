"""Freshness and row-count checks for air-quality data."""

from pyarrow import Table


def assert_table_has_rows(table: Table) -> None:
    """Raise when local data has no rows."""
    if table.num_rows == 0:
        raise ValueError("Silver parquet has no rows to load")


def assert_bigquery_has_rows(client, table_id: str) -> None:
    """Raise when a BigQuery table has no rows."""
    row_count = _query_scalar(client, f"SELECT COUNT(*) FROM `{table_id}`")
    if row_count == 0:
        raise ValueError(f"Post-load check failed: {table_id} is empty")


def _query_scalar(client, query: str):
    rows = client.query(query).result()
    return next(iter(rows))[0]

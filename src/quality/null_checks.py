"""Null checks for local silver data and BigQuery tables."""

from pyarrow import Table


def assert_no_nulls(table: Table, columns: list[str]) -> None:
    """Raise when required local columns contain null values."""
    null_columns = [
        column
        for column in columns
        if column in table.column_names and table.column(column).null_count > 0
    ]
    if null_columns:
        raise ValueError(f"Required columns contain null values: {null_columns}")


def assert_bigquery_no_nulls(client, table_id: str, columns: list[str]) -> None:
    """Raise when required BigQuery columns contain null values."""
    null_condition = " OR ".join([f"{column} IS NULL" for column in columns])
    query = f"""
SELECT COUNT(*)
FROM `{table_id}`
WHERE {null_condition}
"""
    row_count = _query_scalar(client, query)
    if row_count:
        raise ValueError("Post-load check failed: required columns contain null values")


def _query_scalar(client, query: str):
    rows = client.query(query).result()
    return next(iter(rows))[0]

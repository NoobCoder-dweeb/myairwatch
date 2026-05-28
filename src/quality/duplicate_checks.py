"""Duplicate checks for local silver data and BigQuery tables."""

from pyarrow import Table


def assert_no_duplicate_rows(table: Table, key_columns: list[str]) -> None:
    """Raise when local rows duplicate the configured identity key."""
    frame = table.select(key_columns).to_pandas()
    if frame.duplicated().any():
        raise ValueError(f"Duplicate silver rows found for key columns: {key_columns}")


def assert_bigquery_no_duplicates(client, table_id: str, key_columns: list[str]) -> None:
    """Raise when BigQuery rows duplicate the configured identity key."""
    group_columns = ", ".join(key_columns)
    query = f"""
SELECT COUNT(*)
FROM (
  SELECT {group_columns}, COUNT(*) AS row_count
  FROM `{table_id}`
  GROUP BY {group_columns}
  HAVING row_count > 1
)
"""
    duplicate_count = _query_scalar(client, query)
    if duplicate_count:
        raise ValueError("Post-load check failed: duplicate readings found")


def _query_scalar(client, query: str):
    rows = client.query(query).result()
    return next(iter(rows))[0]

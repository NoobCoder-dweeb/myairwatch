"""Schema validation checks for silver air-quality data."""

from pyarrow import Table


def assert_columns_present(table: Table, expected_columns: list[str]) -> None:
    """Raise when a table does not contain the expected columns."""
    missing = [column for column in expected_columns if column not in table.column_names]
    if missing:
        raise ValueError(f"Silver parquet is missing columns: {missing}")

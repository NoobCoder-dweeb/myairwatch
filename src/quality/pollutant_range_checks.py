"""Basic pollutant range checks for silver air-quality data."""

from pyarrow import Table


def assert_pollutant_ranges(table: Table) -> None:
    """Raise when local pollutant readings are outside simple expected bounds."""
    frame = table.select(["pollutant", "reading_value"]).to_pandas()
    values = frame.dropna(subset=["reading_value"])
    bad_rows = values[
        (values["reading_value"] < 0)
        | (
            (values["pollutant"] == "RELATIVE_HUMIDITY")
            & (values["reading_value"] > 100)
        )
        | (
            (values["pollutant"] == "TEMPERATURE")
            & (
                (values["reading_value"] < -50)
                | (values["reading_value"] > 60)
            )
        )
    ]
    if not bad_rows.empty:
        raise ValueError("Pollutant readings are outside expected ranges")


def assert_bigquery_pollutant_ranges(client, table_id: str) -> None:
    """Raise when BigQuery pollutant readings are outside simple expected bounds."""
    query = f"""
SELECT COUNT(*)
FROM `{table_id}`
WHERE reading_value < 0
   OR (pollutant = 'RELATIVE_HUMIDITY' AND reading_value > 100)
   OR (pollutant = 'TEMPERATURE' AND (reading_value < -50 OR reading_value > 60))
"""
    bad_count = _query_scalar(client, query)
    if bad_count:
        raise ValueError("Post-load check failed: pollutant readings outside ranges")


def _query_scalar(client, query: str):
    rows = client.query(query).result()
    return next(iter(rows))[0]

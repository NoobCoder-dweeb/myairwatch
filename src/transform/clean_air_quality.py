"""Air-quality silver-layer transformations."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .clean_pollutants import standardize_pollutants
from .normalise_states import normalize_state_column

SILVER_COLUMNS = [
    "source",
    "source_location_id",
    "location_name",
    "state",
    "country",
    "latitude",
    "longitude",
    "pollutant",
    "unit",
    "reading_value",
    "observed_at",
    "observed_date",
    "year",
    "month",
    "day",
    "health_risk_category",
]

HEALTH_RISK_THRESHOLDS = {
    "PM2.5": [12.0, 35.4, 55.4, 150.4, 250.4],
    "PM10": [54.0, 154.0, 254.0, 354.0, 424.0],
    "CO": [4.4, 9.4, 12.4, 15.4, 30.4],
    "O3": [0.054, 0.070, 0.085, 0.105, 0.200],
    "NO2": [0.053, 0.100, 0.360, 0.649, 1.249],
    "SO2": [0.035, 0.075, 0.185, 0.304, 0.604],
}

HEALTH_RISK_LABELS = [
    "good",
    "moderate",
    "unhealthy_sensitive",
    "unhealthy",
    "very_unhealthy",
    "hazardous",
]


def add_timestamp_columns(df: DataFrame, raw_timestamp_col: str) -> DataFrame:
    """Parse a raw timestamp column and add derived observed_at/date/year/month/day."""
    raw = F.col(raw_timestamp_col)
    observed_at = F.try_to_timestamp(raw)
    observed_date = F.to_date(observed_at)

    return (
        df.withColumn("observed_at", observed_at)
        .withColumn("observed_date", observed_date)
        .withColumn("year", F.year(observed_date))
        .withColumn("month", F.month(observed_date))
        .withColumn("day", F.dayofmonth(observed_date))
    )


def _risk_for_thresholds(value: Column, thresholds: list[float]) -> Column:
    """Build a simple risk expression from ordered threshold values."""
    risk = F.lit(HEALTH_RISK_LABELS[-1])
    for limit, label in reversed(list(zip(thresholds, HEALTH_RISK_LABELS))):
        risk = F.when(value <= limit, label).otherwise(risk)
    return risk


def add_health_risk_category(df: DataFrame) -> DataFrame:
    """Add a pollutant-specific health-risk category."""
    pollutant = F.col("pollutant")
    value = F.col("reading_value")

    risk = F.when(value.isNull(), "unknown")
    for pollutant_name, thresholds in HEALTH_RISK_THRESHOLDS.items():
        risk = risk.when(
            pollutant == pollutant_name,
            _risk_for_thresholds(value, thresholds),
        )
    risk = risk.otherwise("unknown")
    return df.withColumn("health_risk_category", risk)


def deduplicate_readings(df: DataFrame) -> DataFrame:
    """Remove duplicate readings using the stable silver identity columns."""
    dedupe_columns = [
        "source",
        "source_location_id",
        "location_name",
        "pollutant",
        "observed_at",
    ]
    return df.dropDuplicates(dedupe_columns)


def clean_opendosm_air_quality(df: DataFrame) -> DataFrame:
    """Transform OpenDOSM bronze records into the silver air-quality schema."""
    cleaned = df.select(
        F.lit("opendosm").alias("source"),
        F.lit(None).cast("string").alias("source_location_id"),
        F.lit(None).cast("string").alias("location_name"),
        F.lit(None).cast("string").alias("state"),
        F.lit("Malaysia").alias("country"),
        F.lit(None).cast("double").alias("latitude"),
        F.lit(None).cast("double").alias("longitude"),
        F.col("pollutant").cast("string").alias("pollutant"),
        F.lit(None).cast("string").alias("unit"),
        F.col("concentration").cast("double").alias("reading_value"),
        F.col("date").cast("string").alias("date"),
    )
    return clean_air_quality_frame(cleaned, source="opendosm", raw_timestamp_col="date")


def clean_openaq_air_quality(df: DataFrame) -> DataFrame:
    """Transform OpenAQ bronze records into the silver air-quality schema.

    Current OpenAQ bronze files contain location/sensor metadata. Sensor rows
    are retained with null `reading_value` until measurement values are fetched.
    """
    exploded = df.withColumn("_result", F.explode_outer("results")).withColumn(
        "_sensor", F.explode_outer("_result.sensors")
    )
    cleaned = exploded.select(
        F.lit("openaq").alias("source"),
        F.coalesce(F.col("location_id"), F.col("_result.id"))
        .cast("string")
        .alias("source_location_id"),
        F.col("_result.name").cast("string").alias("location_name"),
        F.col("_result.locality").cast("string").alias("state"),
        F.col("_result.country.name").cast("string").alias("country"),
        F.col("_result.coordinates.latitude").cast("double").alias("latitude"),
        F.col("_result.coordinates.longitude").cast("double").alias("longitude"),
        F.col("_sensor.parameter.name").cast("string").alias("pollutant"),
        F.col("_sensor.parameter.units").cast("string").alias("unit"),
        F.lit(None).cast("double").alias("reading_value"),
        F.coalesce(
            F.col("_result.datetimeLast.utc"), F.col("_result.datetimeFirst.utc")
        )
        .cast("string")
        .alias("timestamp"),
    )
    return clean_air_quality_frame(
        cleaned, source="openaq", raw_timestamp_col="timestamp"
    )


def clean_air_quality_frame(
    df: DataFrame, source: str, raw_timestamp_col: str
) -> DataFrame:
    """Apply shared silver-layer cleaning rules to a normalized raw frame.

    Args:
        df: Input DataFrame with raw timestamp column
        source: Data source identifier ("opendosm" or "openaq")
        raw_timestamp_col: Name of the raw timestamp column to parse
    """
    cleaned = standardize_pollutants(df)
    cleaned = normalize_state_column(cleaned)
    cleaned = add_timestamp_columns(cleaned, raw_timestamp_col)
    cleaned = add_health_risk_category(cleaned)
    cleaned = deduplicate_readings(cleaned)
    cleaned = cleaned.filter(
        F.col("observed_date").isNotNull() & F.col("pollutant").isNotNull()
    )
    return cleaned.select(*SILVER_COLUMNS)


def transform_air_quality_df(df: DataFrame, source: str) -> DataFrame:
    """Transform a source-specific bronze DataFrame into silver air-quality data."""
    if source == "opendosm":
        return clean_opendosm_air_quality(df)
    if source == "openaq":
        return clean_openaq_air_quality(df)
    raise ValueError(f"Unsupported air-quality source: {source}")

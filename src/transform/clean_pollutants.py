"""Pollutant and unit standardization helpers."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..utils.mappings import mapping_expr

POLLUTANT_NAME_MAP = {
    "co": "CO",
    "carbon monoxide": "CO",
    "no": "NO",
    "nitric oxide": "NO",
    "no2": "NO2",
    "nitrogen dioxide": "NO2",
    "o3": "O3",
    "ozone": "O3",
    "so2": "SO2",
    "sulfur dioxide": "SO2",
    "sulphur dioxide": "SO2",
    "pm1": "PM1",
    "pm10": "PM10",
    "pm 10": "PM10",
    "pm2.5": "PM2.5",
    "pm25": "PM2.5",
    "pm 2.5": "PM2.5",
    "bc": "BC",
    "black carbon": "BC",
    "temperature": "TEMPERATURE",
    "relativehumidity": "RELATIVE_HUMIDITY",
    "relative humidity": "RELATIVE_HUMIDITY",
    "humidity": "RELATIVE_HUMIDITY",
    "um003": "PARTICLE_COUNT_0_3UM",
}

DEFAULT_UNIT_BY_POLLUTANT = {
    "CO": "ppm",
    "NO": "ppm",
    "NO2": "ppm",
    "O3": "ppm",
    "SO2": "ppm",
    "PM1": "ug/m3",
    "PM2.5": "ug/m3",
    "PM10": "ug/m3",
    "BC": "ug/m3",
    "TEMPERATURE": "celsius",
    "RELATIVE_HUMIDITY": "percent",
    "PARTICLE_COUNT_0_3UM": "particles/cm3",
}


# Build module-level mapping expressions once for reuse. This avoids
# reconstructing the same `create_map` expression on every call.
from ..utils.mappings import LazyMap

# Build module-level lazy map objects for reuse. These are cheap to
# construct and will produce Spark expressions only when indexed.
POLLUTANT_MAP = LazyMap(POLLUTANT_NAME_MAP)
DEFAULT_UNIT_MAP = LazyMap(DEFAULT_UNIT_BY_POLLUTANT)


def standardize_pollutant_name(pollutant_col: Column) -> Column:
    """Return canonical pollutant names such as PM2.5, PM10, CO, NO2."""
    normalized = F.lower(F.trim(pollutant_col.cast("string")))
    normalized = F.regexp_replace(normalized, r"[_-]+", " ")
    normalized = F.regexp_replace(normalized, r"\s+", " ")
    mapped = POLLUTANT_MAP[normalized]
    return F.coalesce(mapped, F.upper(F.trim(pollutant_col.cast("string"))))


def standardize_unit(unit_col: Column | None, pollutant_col: Column) -> Column:
    """Return canonical units, using pollutant defaults when the input is blank."""
    if unit_col is None:
        normalized_unit = F.lit(None).cast("string")
    else:
        normalized_unit = F.lower(F.trim(unit_col.cast("string")))
        normalized_unit = F.regexp_replace(normalized_unit, "µ", "u")
        normalized_unit = F.regexp_replace(normalized_unit, "μ", "u")
        normalized_unit = F.regexp_replace(normalized_unit, "m³", "m3")
        normalized_unit = F.regexp_replace(normalized_unit, r"\s+", "")

    cleaned_unit = (
        F.when(normalized_unit.isin("ug/m3", "ugm-3", "ugm3"), F.lit("ug/m3"))
        .when(normalized_unit.isin("ppm", "ppmv"), F.lit("ppm"))
        .when(normalized_unit.isin("ppb", "ppbv"), F.lit("ppb"))
        .when(normalized_unit.isin("%", "percent", "percentage"), F.lit("percent"))
        .when(normalized_unit.isin("c", "celsius", "degc"), F.lit("celsius"))
        .when(normalized_unit == "particles/cm3", F.lit("particles/cm3"))
        .otherwise(F.when(normalized_unit == "", None).otherwise(normalized_unit))
    )

    default_unit = DEFAULT_UNIT_MAP[pollutant_col]
    return F.coalesce(cleaned_unit, default_unit)


def standardize_pollutants(
    df: DataFrame,
    pollutant_col: str = "pollutant",
    unit_col: str | None = "unit",
) -> DataFrame:
    """Standardize pollutant and unit columns on an air-quality DataFrame."""
    unit_expr = F.col(unit_col) if unit_col and unit_col in df.columns else None
    df = df.withColumn("pollutant", standardize_pollutant_name(F.col(pollutant_col)))
    return df.withColumn("unit", standardize_unit(unit_expr, F.col("pollutant")))

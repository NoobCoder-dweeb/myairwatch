"""Malaysia state normalization helpers."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..utils.mappings import mapping_expr

STATE_ALIASES = {
    "johor": "Johor",
    "kedah": "Kedah",
    "kelantan": "Kelantan",
    "melaka": "Melaka",
    "malacca": "Melaka",
    "negeri sembilan": "Negeri Sembilan",
    "n sembilan": "Negeri Sembilan",
    "pahang": "Pahang",
    "penang": "Pulau Pinang",
    "pulau pinang": "Pulau Pinang",
    "perak": "Perak",
    "perlis": "Perlis",
    "sabah": "Sabah",
    "sarawak": "Sarawak",
    "selangor": "Selangor",
    "terengganu": "Terengganu",
    "wp kuala lumpur": "Kuala Lumpur",
    "w.p. kuala lumpur": "Kuala Lumpur",
    "wilayah persekutuan kuala lumpur": "Kuala Lumpur",
    "kuala lumpur": "Kuala Lumpur",
    "kl": "Kuala Lumpur",
    "wp labuan": "Labuan",
    "w.p. labuan": "Labuan",
    "wilayah persekutuan labuan": "Labuan",
    "labuan": "Labuan",
    "wp putrajaya": "Putrajaya",
    "w.p. putrajaya": "Putrajaya",
    "wilayah persekutuan putrajaya": "Putrajaya",
    "putrajaya": "Putrajaya",
}

LOCATION_STATE_HINTS = {
    "cyberjaya": "Selangor",
    "setia eco park": "Selangor",
    "taman tun dr. ismail": "Kuala Lumpur",
    "taman tun dr ismail": "Kuala Lumpur",
    "ttdi": "Kuala Lumpur",
    "klcc": "Kuala Lumpur",
    "bukit bintang": "Kuala Lumpur",
}


from ..utils.mappings import LazyMap

# Build module-level lazy map for state aliases; expression is created
# only when accessed to avoid requiring Spark during module import.
STATE_ALIASES_MAP = LazyMap(STATE_ALIASES)


def normalize_state_name(state_col: Column) -> Column:
    """Normalize Malaysia state and federal territory names."""
    normalized = F.lower(F.trim(state_col.cast("string")))
    normalized = F.regexp_replace(normalized, r"[_-]+", " ")
    normalized = F.regexp_replace(normalized, r"\s+", " ")
    normalized = F.when(normalized == "", None).otherwise(normalized)
    mapped = STATE_ALIASES_MAP[normalized]
    return F.coalesce(mapped, F.when(normalized.isNotNull(), F.initcap(normalized)))


def infer_state_from_location(location_col: Column) -> Column:
    """Infer state from known Malaysia monitoring location names."""
    normalized = F.lower(F.trim(location_col.cast("string")))
    normalized = F.regexp_replace(normalized, r"\s+", " ")

    expr = F.lit(None).cast("string")
    for location_hint, state in LOCATION_STATE_HINTS.items():
        expr = F.when(normalized.contains(location_hint), F.lit(state)).otherwise(expr)
    return expr


def normalize_state_column(
    df: DataFrame,
    state_col: str = "state",
    location_col: str = "location_name",
) -> DataFrame:
    """Add a normalized `state` column, optionally inferred from location name."""
    state_expr = (
        normalize_state_name(F.col(state_col))
        if state_col in df.columns
        else F.lit(None).cast("string")
    )
    location_state_expr = (
        infer_state_from_location(F.col(location_col))
        if location_col in df.columns
        else F.lit(None).cast("string")
    )
    return df.withColumn("state", F.coalesce(state_expr, location_state_expr))

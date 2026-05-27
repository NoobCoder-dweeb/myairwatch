"""Utility helpers for creating Spark mapping expressions.

This module centralises small helpers used to build `create_map` Column
expressions from plain Python dictionaries. Keeping these in `src.utils`
makes them available across the codebase.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


def mapping_expr(mapping: dict[str, str]) -> Column:
    """Return a Spark `create_map` Column expression for `mapping`.

    Example:
        MAP_EXPR = mapping_expr({"a": "A", "b": "B"})
        df.select(MAP_EXPR[F.col("key_col")])
    """
    pairs: list[Column] = []

    for key, value in mapping.items():
        key_lit = F.lit(key)
        value_lit = F.lit(value)
        pairs.extend([key_lit, value_lit])

    return F.create_map(*pairs)


class LazyMap:
    """Lazily build a `create_map` Column expression on access.

    Constructing `lit()` expressions requires an active Spark context, so
    `LazyMap` defers building the `create_map` expression until the map is
    indexed with a Column. Example:

        POLLUTANT_MAP = LazyMap(POLLUTANT_NAME_MAP)
        mapped = POLLUTANT_MAP[normalized_col]
    """

    def __init__(self, mapping: dict[str, str]):
        self._mapping = mapping

    def __getitem__(self, key: Column | str) -> Column:
        # Build the create_map expression at access time (Spark must be active)
        expr = mapping_expr(self._mapping)
        return expr[key]

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from src.transform.clean_air_quality import (
    SILVER_COLUMNS,
    add_health_risk_category,
    transform_air_quality_df,
)
from src.transform.clean_pollutants import standardize_pollutants
from src.transform.normalise_states import normalize_state_column
from src.transform.partition_writer import write_silver_partitioned


OPENAQ_SCHEMA = T.StructType(
    [
        T.StructField("location_id", T.LongType()),
        T.StructField(
            "results",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("id", T.LongType()),
                        T.StructField("name", T.StringType()),
                        T.StructField("locality", T.StringType()),
                        T.StructField(
                            "country",
                            T.StructType([T.StructField("name", T.StringType())]),
                        ),
                        T.StructField(
                            "coordinates",
                            T.StructType(
                                [
                                    T.StructField("latitude", T.DoubleType()),
                                    T.StructField("longitude", T.DoubleType()),
                                ]
                            ),
                        ),
                        T.StructField(
                            "datetimeFirst",
                            T.StructType([T.StructField("utc", T.StringType())]),
                        ),
                        T.StructField(
                            "datetimeLast",
                            T.StructType([T.StructField("utc", T.StringType())]),
                        ),
                        T.StructField(
                            "sensors",
                            T.ArrayType(
                                T.StructType(
                                    [
                                        T.StructField(
                                            "parameter",
                                            T.StructType(
                                                [
                                                    T.StructField(
                                                        "name", T.StringType()
                                                    ),
                                                    T.StructField(
                                                        "units", T.StringType()
                                                    ),
                                                ]
                                            ),
                                        )
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
    ]
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("myairwatch_transform_tests")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def rows_by_key(df, key):
    return {row[key]: row.asDict() for row in df.collect()}


def openaq_df(spark, records):
    return spark.createDataFrame(records, schema=OPENAQ_SCHEMA)


def test_standardize_pollutants_handles_aliases_units_and_defaults(spark):
    df = spark.createDataFrame(
        [
            ("pm25", "\u00b5g/m\u00b3"),
            ("PM 10", None),
            ("relativehumidity", "%"),
            ("temperature", "c"),
            ("mystery", ""),
        ],
        ["pollutant", "unit"],
    )

    rows = rows_by_key(standardize_pollutants(df), "pollutant")

    assert rows["PM2.5"]["unit"] == "ug/m3"
    assert rows["PM10"]["unit"] == "ug/m3"
    assert rows["RELATIVE_HUMIDITY"]["unit"] == "percent"
    assert rows["TEMPERATURE"]["unit"] == "celsius"
    assert rows["MYSTERY"]["unit"] is None


def test_normalize_state_uses_aliases_and_location_fallback(spark):
    df = spark.createDataFrame(
        [
            ("wp kuala lumpur", None),
            ("penang", None),
            (None, "KLCC"),
            ("", "Cyberjaya Sensor"),
            (None, None),
        ],
        ["state", "location_name"],
    )

    states = [row.state for row in normalize_state_column(df).collect()]

    assert states == [
        "Kuala Lumpur",
        "Pulau Pinang",
        "Kuala Lumpur",
        "Selangor",
        None,
    ]


def test_health_risk_categories_cover_boundaries_and_unknowns(spark):
    df = spark.createDataFrame(
        [
            ("PM2.5", 12.0),
            ("PM2.5", 12.1),
            ("PM2.5", 55.5),
            ("PM10", 425.0),
            ("CO", None),
            ("TEMPERATURE", 30.0),
        ],
        ["pollutant", "reading_value"],
    )

    risks = [row.health_risk_category for row in add_health_risk_category(df).collect()]

    assert risks == [
        "good",
        "moderate",
        "unhealthy",
        "hazardous",
        "unknown",
        "unknown",
    ]


def test_opendosm_transform_creates_silver_schema_and_filters_bad_rows(spark):
    bronze_df = spark.createDataFrame(
        [
            ("2026-05-27", "PM 2.5", 10.0),
            ("2026-05-27", "PM 2.5", 10.0),
            ("2026-05-28T01:02:03Z", "PM 10", 160.0),
            ("bad-date", "CO", 1.0),
            ("2026-05-29", None, 1.0),
            ("2026-05-30", "CO", None),
        ],
        ["date", "pollutant", "concentration"],
    )

    silver_df = transform_air_quality_df(bronze_df, "opendosm")
    rows = silver_df.orderBy("observed_date", "pollutant").collect()

    assert silver_df.columns == SILVER_COLUMNS
    assert len(rows) == 3
    assert rows[0].source == "opendosm"
    assert rows[0].pollutant == "PM2.5"
    assert rows[0].unit == "ug/m3"
    assert rows[0].year == 2026
    assert rows[0].month == 5
    assert rows[0].day == 27
    assert rows[0].health_risk_category == "good"
    assert rows[1].pollutant == "PM10"
    assert rows[1].health_risk_category == "unhealthy_sensitive"
    assert rows[2].pollutant == "CO"
    assert rows[2].health_risk_category == "unknown"


def test_openaq_transform_explodes_sensors_and_infers_state(spark):
    bronze_df = openaq_df(
        spark,
        [
            {
                "location_id": 123,
                "results": [
                    {
                        "id": 123,
                        "name": "KLCC",
                        "locality": None,
                        "country": {"name": "Malaysia"},
                        "coordinates": {"latitude": 3.1579, "longitude": 101.7123},
                        "datetimeFirst": {"utc": "2026-05-01T00:00:00Z"},
                        "datetimeLast": {"utc": "2026-05-27T12:00:00Z"},
                        "sensors": [
                            {
                                "parameter": {
                                    "name": "pm25",
                                    "units": "\u00b5g/m\u00b3",
                                }
                            },
                            {"parameter": {"name": "temperature", "units": "c"}},
                            {
                                "parameter": {
                                    "name": "relativehumidity",
                                    "units": "%",
                                }
                            },
                        ],
                    }
                ],
            }
        ]
    )

    rows = transform_air_quality_df(bronze_df, "openaq").orderBy("pollutant").collect()

    assert len(rows) == 3
    assert {row.pollutant for row in rows} == {
        "PM2.5",
        "RELATIVE_HUMIDITY",
        "TEMPERATURE",
    }
    assert {row.unit for row in rows} == {"ug/m3", "percent", "celsius"}
    assert {row.state for row in rows} == {"Kuala Lumpur"}
    assert {row.source_location_id for row in rows} == {"123"}
    assert {row.health_risk_category for row in rows} == {"unknown"}
    assert {row.observed_date.isoformat() for row in rows} == {"2026-05-27"}


def test_transform_rejects_unknown_source(spark):
    df = spark.createDataFrame([("2026-05-27", "CO", 1.0)], ["date", "pollutant", "concentration"])

    with pytest.raises(ValueError, match="Unsupported air-quality source"):
        transform_air_quality_df(df, "unknown")


def test_partition_writer_requires_date_and_source_columns(spark, tmp_path):
    df = spark.createDataFrame([("opendosm", 2026, 5, 27, "CO")], ["source", "year", "month", "day", "pollutant"])
    output_path = Path(tmp_path) / "silver" / "air_quality"

    write_silver_partitioned(df, output_path)

    assert (output_path / "year=2026" / "month=5" / "day=27" / "source=opendosm").exists()

    with pytest.raises(ValueError, match="Missing required silver partition columns"):
        write_silver_partitioned(df.drop("source"), output_path)


def test_openaq_rows_without_sensor_or_timestamp_are_filtered(spark):
    bronze_df = openaq_df(
        spark,
        [
            {
                "location_id": 1,
                "results": [
                    {
                        "id": 2,
                        "name": "No Sensors",
                        "locality": None,
                        "country": {"name": "Malaysia"},
                        "coordinates": {"latitude": 3.0, "longitude": 101.0},
                        "datetimeFirst": {"utc": "2026-05-01T00:00:00Z"},
                        "datetimeLast": {"utc": "2026-05-27T00:00:00Z"},
                        "sensors": [],
                    },
                    {
                        "id": 3,
                        "name": "No Timestamp",
                        "locality": None,
                        "country": {"name": "Malaysia"},
                        "coordinates": {"latitude": 3.0, "longitude": 101.0},
                        "datetimeFirst": {"utc": None},
                        "datetimeLast": {"utc": None},
                        "sensors": [{"parameter": {"name": "pm25", "units": "ug/m3"}}],
                    },
                    {
                        "id": 1,
                        "name": "KLCC",
                        "locality": None,
                        "country": {"name": "Malaysia"},
                        "coordinates": {"latitude": 3.0, "longitude": 101.0},
                        "datetimeFirst": {"utc": "2026-05-01T00:00:00Z"},
                        "datetimeLast": {"utc": "2026-05-27T00:00:00Z"},
                        "sensors": [{"parameter": {"name": "pm25", "units": "ug/m3"}}],
                    },
                ],
            }
        ]
    )

    rows = transform_air_quality_df(bronze_df, "openaq").collect()

    assert len(rows) == 1
    assert rows[0].source_location_id == "1"
    assert rows[0].pollutant == "PM2.5"

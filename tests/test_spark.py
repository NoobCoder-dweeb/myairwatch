from pyspark.sql import SparkSession


def test_spark_dataframe_creation():
    # create spark session
    spark = (
        SparkSession.builder.master("local[*]").appName("myairwatch_test").getOrCreate()
    )

    # create test dataframe
    test_df = spark.createDataFrame(
        [
            ("Selangor", "PM2.5", 35.2),
            ("Johor", "PM10", 42.1),
        ],
        ["state", "pollutant", "value"],
    )

    # assertions
    assert test_df.count() == 2

    columns = test_df.columns
    assert columns == ["state", "pollutant", "value"]

    first_row = test_df.collect()[0]

    assert first_row["state"] == "Selangor"
    assert first_row["pollutant"] == "PM2.5"
    assert first_row["value"] == 35.2

    # close spark session
    spark.stop()

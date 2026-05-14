from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder \
        .master("local[*]") \
        .appName("myairwatch") \
        .getOrCreate()

# create a test dataframe
test_df = spark.createDataFrame(
    [("Selangor", "PM2.5", 35.2), ("Johor", "PM10", 42.1)],
    ["state", "pollutant", "value"],
)

# run the action
test_df.show()

# close the session
spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .appName("Date and Time Functions") \
    .getOrCreate()


data = [(1, "2024-05-18 12:30:00")]
df = spark.createDataFrame(data, ["id", "timestamp_col"])

print("Original DataFrame:")
df.show(truncate=False)

#
df = df.withColumn("current_date", current_date()) \
    .withColumn("current_timestamp", current_timestamp()) \
    .withColumn("formatted_date", date_format("timestamp_col", "yyyy-MM-dd")) \
    .withColumn("parsed_date", to_date("timestamp_col")) \
    .withColumn("parsed_timestamp", to_timestamp("timestamp_col")) \
    .withColumn("date_diff", datediff(current_date(), "parsed_date")) \
    .withColumn("months_between", months_between(current_date(), "parsed_date")) \
    .withColumn("add_months", add_months("parsed_date", 1)) \
    .withColumn("date_add", date_add("parsed_date", 7)) \
    .withColumn("date_sub", date_sub("parsed_date", 7)) \
    .withColumn("day_of_month", dayofmonth("parsed_timestamp")) \
    .withColumn("month", month("parsed_timestamp")) \
    .withColumn("year", year("parsed_timestamp")) \
    .withColumn("hour", hour("parsed_timestamp")) \
    .withColumn("minute", minute("parsed_timestamp")) \
    .withColumn("second", second("parsed_timestamp"))


print("\nDataFrame after transformations:")
df.show(truncate=False)



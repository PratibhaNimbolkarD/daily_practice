from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, to_timestamp, date_format, datediff, months_between, current_date, current_timestamp, year, month, dayofmonth, hour, minute, second, weekofyear, date_add, date_sub, date_trunc, unix_timestamp, from_unixtime

spark = SparkSession.builder \
    .appName("Date and Time Functions") \
    .getOrCreate()

data = [("2023-07-01", "2023-07-10 12:30:00"),
        ("2023-08-01", "2023-08-15 16:45:30")]

df = spark.createDataFrame(data, ["date_str", "timestamp_str"])

df = df.withColumn("date", to_date(col("date_str"), "yyyy-MM-dd"))
df = df.withColumn("timestamp", to_timestamp(col("timestamp_str"), "yyyy-MM-dd HH:mm:ss"))

df = df.withColumn("formatted_date", date_format(col("date"), "dd-MM-yyyy"))
df = df.withColumn("formatted_timestamp", date_format(col("timestamp"), "dd-MM-yyyy HH:mm:ss"))

df = df.withColumn("date_diff", datediff(col("date"), lit("2023-06-01")))
df = df.withColumn("months_diff", months_between(col("date"), lit("2023-06-01")))

df = df.withColumn("year", year(col("date")))
df = df.withColumn("month", month(col("date")))
df = df.withColumn("day", dayofmonth(col("date")))
df = df.withColumn("hour", hour(col("timestamp")))
df = df.withColumn("minute", minute(col("timestamp")))
df = df.withColumn("second", second(col("timestamp")))
df = df.withColumn("week_of_year", weekofyear(col("date")))

df = df.withColumn("date_add_10", date_add(col("date"), 10))
df = df.withColumn("date_sub_10", date_sub(col("date"), 10))

df = df.withColumn("truncated_date", date_trunc("month", col("timestamp")))

df = df.withColumn("current_date", current_date())
df = df.withColumn("current_timestamp", current_timestamp())

df = df.withColumn("unix_timestamp", unix_timestamp(col("timestamp")))
df = df.withColumn("from_unix_timestamp", from_unixtime(col("unix_timestamp"), "yyyy-MM-dd HH:mm:ss"))

df.show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Pivot Example").getOrCreate()


data = [("Alice", "2019", 100),
        ("Alice", "2020", 200),
        ("Bob", "2019", 150),
        ("Bob", "2020", 250)]
df = spark.createDataFrame(data, ["name", "year", "amount"])
pivot_df = df.groupBy("name").pivot("year").sum("amount")
pivot_df.show()



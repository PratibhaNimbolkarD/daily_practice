from pyspark.sql import SparkSession
from pyspark.sql.functions import posexplode

spark = SparkSession.builder.appName("PosExplode Example").getOrCreate()

data = [("Alice", ["Math", "Physics", "Chemistry"]),
        ("Bob", ["Biology", "Geography"])]

df = spark.createDataFrame(data, ["name", "subjects"])

pos_exploded_df = df.select("name", posexplode("subjects").alias("pos", "subject"))

pos_exploded_df.show()



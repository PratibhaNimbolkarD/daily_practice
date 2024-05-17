from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

# Create a SparkSession
spark = SparkSession.builder.appName("Explode Example").getOrCreate()

data = [("Alice", ["Math", "Physics", "Chemistry"]),
        ("Bob", ["Biology", "Geography"])]

df = spark.createDataFrame(data, ["name", "subjects"])

exploded_df = df.select("name", explode("subjects").alias("subject"))
exploded_df.show()



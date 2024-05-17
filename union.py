from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Union Example").getOrCreate()


data1 = [("Pratibha", 34), ("Himani", 45), ("Neha", 27)]
df1 = spark.createDataFrame(data1, ["name", "age"])

data2 = [("Naina", 40), ("Anjali", 29), ("Sakshi", 32)]
df2 = spark.createDataFrame(data2, ["name", "age"])


combined_df = df1.union(df2)

combined_df.show()
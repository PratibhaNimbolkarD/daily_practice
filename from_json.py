from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, StructType

spark = SparkSession.builder.appName("from_json_parameters").getOrCreate()

data = [
    ('{"employee_name": "Aarav Kumar", "age": 28, "address": {"street": "123 Main St", "city": "Metropolis", "zip": "12345"}}',),
    ('{"employee_name": "Vani Sharma", "age": 32, "address": {"street": "456 Elm St", "city": "Gotham", "zip": "67890"}}',)
]

schema = StructType([
    StructField("employee_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True)
    ]), True)
])

df = spark.createDataFrame(data, ["json"])

parsed_df = df.withColumn("parsed_json", from_json(col("json"), schema))

result_df = parsed_df.select(
    col("parsed_json.employee_name").alias("employee_name"),
    col("parsed_json.age").alias("age"),
    col("parsed_json.address.street").alias("street"),
    col("parsed_json.address.city").alias("city"),
    col("parsed_json.address.zip").alias("zip")
)

result_df.show(truncate=False)

# Handling Missing Data:
#
# You have a DataFrame with product information containing columns: product_id, product_name, category, price, and quantity. Some of the rows have missing values in the price and quantity columns. Write a PySpark script to:
# Identify the rows with missing values.
# Fill the missing price values with the average price of the respective category.
# Fill the missing quantity values with zero.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Create a Spark session
spark = SparkSession.builder.appName("HandleMissingData").getOrCreate()

# Sample data for products DataFrame
products_data = [
    (1, "Product A", "Category 1", 10.0, 100),
    (2, "Product B", "Category 2", None, 200),
    (3, "Product C", "Category 1", 15.0, None),
    (4, "Product D", "Category 3", 20.0, 300),
    (5, "Product E", "Category 2", None, None),
    (6, "Product F", "Category 1", 10.0, 150)
]

# Create DataFrame
products_df = spark.createDataFrame(products_data, ["product_id", "product_name", "category", "price", "quantity"])
products_df.show()

avg_price = products_df.groupby('category').agg(avg('price').alias('avg_price'))

joined_df = products_df.join(avg_price , on = 'category' , how= 'left')
price_filled_df = joined_df.withColumn('price' ,col('price').fillna(col("avg_price")))
price_filled_df.show()

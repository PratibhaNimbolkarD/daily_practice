# You have two DataFrames: orders and customers. The orders DataFrame has columns: order_id, customer_id, order_date, and amount. The customers DataFrame has columns: customer_id, name, and email. Write a PySpark script to:
# Perform an inner join between orders and customers on customer_id.
# Calculate the total amount spent by each customer.
# List the top 5 customers based on the total amount spent.

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum , col

# Create a Spark session
spark = SparkSession.builder.appName("JoiningDataFrames").getOrCreate()

# Sample data for orders DataFrame
orders_data = [
    (101, 1, "2023-01-01", 150.0),
    (102, 2, "2023-01-02", 200.0),
    (103, 3, "2023-01-03", 250.0),
    (104, 1, "2023-01-04", 300.0),
    (105, 4, "2023-01-05", 350.0)
]

# Sample data for customers DataFrame
customers_data = [
    (1, "Alice Johnson", "alice@example.com"),
    (2, "Bob Smith", "bob.smith@domain.com"),
    (3, "Charlie Brown", "charlie@brown.com"),
    (4, "David Wilson", "david.wilson@domain.com"),
    (5, "Eve Adams", "eve@adams.com")
]

# Create DataFrames
orders_df = spark.createDataFrame(orders_data, ["order_id", "customer_id", "order_date", "amount"])
customers_df = spark.createDataFrame(customers_data, ["customer_id", "name", "email"])

joined_df = orders_df.join(customers_df , on='customer_id' , how='inner')
joined_df.show()




amount_spent_df = orders_df.groupby('customer_id').agg(sum('amount').alias('total_spent'))
top_two_customer = amount_spent_df.orderBy(col('total_spent').desc()).limit(2)
top_two_customer.show()
# amount_spent_df.show()
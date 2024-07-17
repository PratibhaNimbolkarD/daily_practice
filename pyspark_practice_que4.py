# Working with JSON Data:
#
# You have a JSON file containing nested data about orders. Each order includes fields like order_id, customer, items, and total_amount. The customer field itself is a JSON object containing customer_id, name, and email. The items field is an array of JSON objects containing item_id, product_name, and quantity. Write a PySpark script to:
# Read the JSON file into a DataFrame.
# Flatten the nested JSON structure.
# Calculate the total quantity of items ordered by each customer.

from  pyspark.sql import  SparkSession
from  pyspark.sql.types import StructType,StructField,StringType,IntegerType, DoubleType, ArrayType
from  pyspark.sql.functions import explode, col , count

spark = SparkSession.builder.appName('read_json').getOrCreate()

customer_schema = StructType([
    StructField("customer_id" , IntegerType() , True),
    StructField("name" , StringType() , True),
    StructField("email" , StringType() , True)
])

items_schema = StructType([
    StructField("item_id", IntegerType() , True),
    StructField("product_name" , StringType() , True),
    StructField("quantity" , IntegerType() , True)
])

json_custom_schema = StructType([
    StructField("order_id" , IntegerType() , True),
    StructField("customer" , customer_schema , True),
    StructField("items" , ArrayType(items_schema) , True ),
    StructField("total_amount", DoubleType() , True)
])

read_df = spark.read.json('file.json' , multiLine=True , schema= json_custom_schema)

read_df = read_df.withColumn('items',explode(read_df['items']))
read_df.printSchema()
exploded_df = read_df.select('*', col('customer.customer_id').alias('customer_id'), col('customer.name').alias('customer_name') ,col('customer.email').alias('customer_email') , col('items.item_id').alias('item_id') ,col( 'items.product_name').alias('product_name'), col('items.quantity').alias('item_quantity')).drop('customer' , 'items')
exploded_df.show()

total_quantity = exploded_df.groupby('customer_id').agg(sum('item_quantity'))
total_quantity.show()
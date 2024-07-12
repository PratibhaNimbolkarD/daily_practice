from pyspark.sql import SparkSession
from  pyspark.sql.functions import *
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrame Merge").getOrCreate()

# Creating the first DataFrame with four columns
data1 = [
    (1, "Pratibha", "9876543210", "123 Street A"),
    (2, "Aarav", "9876543211", "456 Street B"),
    (3, "Vivaan", "9876543212", "789 Street C"),
    (4, "Diya", "9876543213", "101 Street D"),
    (5, "Ananya", "9876543214", "102 Street E")
]

schema1 = ["ID" , "Name" , "mobno" , "address"]

data2 = [
    (6, "Aadhya", "5155546546"),
    (7, "Krishna", "2156656566"),
    (8, "Meera", "6565656565"),
    (9, "Kabir", "56564564654"),
    (10, "Rohan", "4546484888")
]

schema2 = ["Id", "Name" , "m=bno"]

df1 = spark.createDataFrame(data1 , schema1)
df2 = spark.createDataFrame(data2 , schema2)
df1 = df1.select("Name", "Id", "address", "mobno")
df2 = df2.withColumn("address" , lit("null"))
df = df1.union(df2)
df.show()

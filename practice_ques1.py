from pyspark.sql.functions import *
from pyspark.sql import  SparkSession


spark = SparkSession.builder.appName('practice').getOrCreate()

data = [(1,"Pratibha","Cricket,Tennis"),
        (2, "Aarav", "Football,Swimming"),
        (3, "Vivaan", "Basketball,Running"),
        (4, "Diya", "Badminton,Chess")]

schema = ["Id" , "Name" , "Games"]

df = spark.createDataFrame(data, schema)
df = df.withColumn("Games" , explode(split(df["Games"] , ",")))
df.show(truncate = False)
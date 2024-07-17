# Window Functions:
#
# You have a DataFrame containing employee data with columns: employee_id, name, department, salary, and date_of_joining. Write a PySpark script to:
# Rank the employees within each department based on their salary in descending order.
# Calculate the cumulative salary within each department.
# Identify the top 3 highest-paid employees in each department.

from pyspark.sql import SparkSession, WindowSpec
from pyspark.sql import Row
from pyspark.sql import Window
from pyspark.sql.functions import rank, sum as _sum, col , desc

# Create a Spark session
spark = SparkSession.builder.appName("EmployeeWindowFunctions").getOrCreate()

# Create DataFrame with employee data
data = [
    (1, "Alice", "HR", 70000, "2022-01-15"),
    (2, "Bob", "IT", 80000, "2021-03-10"),
    (3, "Charlie", "IT", 60000, "2021-05-20"),
    (4, "David", "HR", 75000, "2021-08-30"),
    (5, "Eve", "Finance", 90000, "2020-12-05"),
    (6, "Frank", "Finance", 95000, "2019-11-25"),
    (7, "Grace", "HR", 72000, "2022-06-10"),
    (8, "Hank", "IT", 85000, "2020-02-15")
]

# Create DataFrame
columns = ["employee_id", "name", "department", "salary", "date_of_joining"]
employees_df = spark.createDataFrame(data, schema=columns)
employees_df.show(truncate=False)

WindowSpec = Window.partitionBy('department').orderBy(desc('salary'))
salary_desc = employees_df.withColumn("Rank" , rank().over(WindowSpec)).where(col("Rank")<=3)
salary_desc.show()

culumative_salary = employees_df.withColumn('Avg_salary' , _sum("salary").over(WindowSpec))
culumative_salary.show()


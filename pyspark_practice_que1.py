# You have a DataFrame containing customer information with columns: customer_id, name, email, signup_date, and last_login_date. Some of the email addresses are invalid, and there are duplicate records. Write a PySpark script to:
# Remove duplicate records based on customer_id.
# Filter out records with invalid email addresses.
# Calculate the number of days between signup_date and last_login_date.



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, lit, to_date

# Create a Spark session
spark = SparkSession.builder.appName("CustomerDataCleaning").getOrCreate()

# Sample data
data = [
    (1, "Alice Johnson", "alice@example.com", "2023-01-01", "2023-06-30"),
    (2, "Bob Smith", "bob.smith@domain.com", "2022-12-15", "2023-07-01"),
    (3, "Charlie Brown", "invalid_email", "2023-03-10", "2023-06-25"),
    (4, "David Wilson", "david.wilson@domain.com", "2022-11-01", "2023-07-10"),
    (5, "Eve Adams", "eve@adams.com", "2023-01-20", "2023-06-20"),
    (1, "Alice Johnson", "alice@example.com", "2023-01-01", "2023-06-30"),
    (3, "Charlie Brown", "charlie@brown.com", "2023-03-10", "2023-06-25")
]

# Create a DataFrame
df = spark.createDataFrame(data, ["customer_id", "name", "email", "signup_date", "last_login_date"])
# df.show()
drop_duplicate_df = df.drop_duplicates(["customer_id"])
# drop_duplicate_df.show()

filer_records = df.filter(col("email").rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'))
filer_records.show()



df = df.withColumn('signup_date' , to_date('signup_date' , 'yyyy-MM-dd'))
datediff_df = df.withColumn('date_diff' , datediff(col('last_login_date') , col('signup_date')))
datediff_df.show()

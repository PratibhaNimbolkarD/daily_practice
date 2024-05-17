from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col , trim , ltrim,rtrim , translate ,substring_index , substring,split,repeat, length,lower,regexp_extract,regexp_replace,rpad,lpad

spark = SparkSession.builder.appName("pratibha").getOrCreate()
data = [
    {"email_id": 1, "sender": "  user1@example.com  ", "body": "Thank you for your inquiry. we will respond shortly."},
    {"email_id": 2, "sender": "  user2@example.com  ", "body": "We received your request and will get back to you soon."},
]

emails_df = spark.createDataFrame(data)
emails_df.show(truncate=False)

processed_emails_df = emails_df.withColumn("processed_body", upper(col("body")))
processed_emails_df.show(truncate=False)

trim_df = emails_df.withColumn("sender" , trim(col("sender")))
trim_df.show(truncate=False)

trim_left_space = emails_df.withColumn("sender" , ltrim(col("sender")))
trim_left_space.show(truncate=False)

trim_right_spaces = emails_df.withColumn("sender",rtrim(col("sender")))
trim_right_spaces.show(truncate=False)

translate_df = emails_df.withColumn("sender" , translate("sender" , "@" , "#") )
translate_df.show(truncate=False)

df_substring_index = emails_df.withColumn("sender_name", substring_index(col("sender"), "@", -1))
df_substring_index.show(truncate=False)

df_substring = emails_df.withColumn("substring_body" , substring(col("body") , 1 , 8))
df_substring.show(truncate=False)

df_split = emails_df.withColumn("split_array", split(col("sender"), "@"))
df_split.show(truncate=False)

df_repeat = emails_df.withColumn("sender" , repeat(col("sender") , 2))
df_repeat.show(truncate=False)


df_lower = emails_df.withColumn("body_lower", lower(col("body")))
df_lower.show(truncate=False)


regexp_extract_df = emails_df.withColumn("domain_name", regexp_extract(col("sender"), "@(.+)$", 1))
regexp_extract_df.show(truncate=False)

regexp_replace_df = emails_df.withColumn("body_replaced", regexp_replace(col("body"), "shortly", "soon"))
regexp_replace_df.show(truncate=False)

rpad_df = emails_df.withColumn("sender_padded", rpad(col("sender"), 25, "*"))
rpad_df.show(truncate=False)


lpad_df = emails_df.withColumn("email_id_padded", lpad(col("email_id").cast("string"), 5, "0"))
lpad_df.show(truncate=False)

df_length = emails_df.withColumn("length_body" ,length(col("body")))
df_length.show(truncate=False)

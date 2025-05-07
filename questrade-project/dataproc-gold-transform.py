from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, date_trunc, current_timestamp
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GoldTransform").getOrCreate()

# Load Silver layer data
df_silver = spark.read.parquet("gs://questrade-silver-layer/cleaned/")

# Aggregate the data by accountId, month, and year
df_gold = (
    df_silver.groupBy("accountId", "year", "month")
    .agg(
        sum("marketValueUSD").alias("total_market_value"),
        sum("marketValueUSD").over().alias("total_market_value_usd_all_accounts")  # Sum of all accounts for comparison
    )
    .withColumn("processed_at", current_timestamp())  # Add a timestamp for processing
    .orderBy("year", "month", "accountId") 
)

# Write to the Gold layer (BigQuery or Parquet)
df_gold.write.mode("overwrite").parquet("gs://questrade-gold-layer/aggregated/")

# # Write the aggregated data to BigQuery
# df_gold.write \
#     .format("bigquery") \
#     .option("table", "your-project-id.your_dataset.your_table_name") \
#     .option("writeMethod", "direct") \
#     .mode("overwrite") \
#     .save()

# # Close the Spark session
# spark.stop()

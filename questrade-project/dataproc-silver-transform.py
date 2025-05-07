from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, upper, expr,
    current_timestamp, year, month, to_date
)

spark = SparkSession.builder.appName("SilverTransform").getOrCreate()

df = spark.read.json("gs://questrade-raw-layer/bronze/*.json")

df_clean = (
    df.selectExpr("accounts")  # Assuming the JSON has an 'accounts' field
      .selectExpr("explode(accounts) as account")
      .select("account.*")
      .dropna(subset=["number", "type", "status"])
      .withColumn("processed_at", current_timestamp())
      .withColumn("date", to_date(current_timestamp()))
      .withColumn("year", year(current_timestamp()))
      .withColumn("month", month(current_timestamp()))
)

df_clean.write.mode("overwrite").parquet("gs://questrade-silver-layer/cleaned/")

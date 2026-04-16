# Databricks notebook source
# MAGIC %md
# MAGIC # 🌊 Step 6: Simulated Streaming → Bronze
# MAGIC ## Real-time ingestion using Structured Streaming (rate source simulation)

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog = "retail_intelligence"
bronze  = f"{catalog}.bronze"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Streaming Architecture
# MAGIC In production this would be: **Kafka / Event Hubs / Kinesis → Bronze**
# MAGIC Here we simulate with Spark's built-in `rate` source — identical code, different source.

# COMMAND ----------
# MAGIC %md ## 1. Define Streaming Schema (simulates Kafka JSON payload)

# COMMAND ----------
event_schema = StructType([
    StructField("event_id",    StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id",  StringType()),
    StructField("event_type",  StringType()),  # view, add_to_cart, purchase
    StructField("amount",      DoubleType()),
    StructField("timestamp",   TimestampType()),
    StructField("region",      StringType()),
])

# COMMAND ----------
# MAGIC %md ## 2. Simulate Streaming Events (rate source)

# COMMAND ----------
# Simulate 10 events per second
streaming_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("event_id",    F.concat(F.lit("EVT_"), F.col("value").cast("string")))
    .withColumn("customer_id", F.concat(F.lit("C"), (F.col("value") % 5000 + 1).cast("string")))
    .withColumn("product_id",  F.concat(F.lit("P"), (F.col("value") % 480 + 1).cast("string")))
    .withColumn("event_type",  F.element_at(
        F.array(F.lit("view"), F.lit("add_to_cart"), F.lit("purchase")),
        (F.col("value") % 3 + 1).cast("int")
    ))
    .withColumn("amount",      F.round(F.rand() * 500 + 10, 2))
    .withColumn("region",      F.element_at(
        F.array(F.lit("North"), F.lit("South"), F.lit("East"), F.lit("West"), F.lit("Central")),
        (F.col("value") % 5 + 1).cast("int")
    ))
    .withColumn("_ingested_at", F.current_timestamp())
    .drop("value")
)

# COMMAND ----------
# MAGIC %md ## 3. Write Stream to Bronze (append mode)

# COMMAND ----------
stream_query = (
    streaming_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/FileStore/retail_intelligence/raw/_checkpoint/events")
    .trigger(processingTime="10 seconds")   # micro-batch every 10s
    .toTable(f"{bronze}.bronze_events")
)
print("✅ Streaming pipeline started — writing to bronze_events every 10s")

# COMMAND ----------
# MAGIC %md ## 4. Windowed Aggregations on the Stream (Stateful)

# COMMAND ----------
# Aggregate revenue per region per 5-minute window — real-time KPI
windowed_agg = (
    streaming_df
    .filter(F.col("event_type") == "purchase")
    .withWatermark("_ingested_at", "10 minutes")
    .groupBy(
        F.window("_ingested_at", "5 minutes", "1 minute"),
        F.col("region")
    )
    .agg(
        F.count("*").alias("purchase_count"),
        F.sum("amount").alias("revenue"),
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/FileStore/retail_intelligence/raw/_checkpoint/windowed_agg")
    .toTable(f"{bronze}.bronze_realtime_agg")
)
print("✅ Windowed aggregation stream started — real-time revenue by region")

# COMMAND ----------
# MAGIC %md ## 5. Stop streams (run when done testing)
# MAGIC ```python
# MAGIC stream_query.stop()
# MAGIC windowed_agg.stop()
# MAGIC ```

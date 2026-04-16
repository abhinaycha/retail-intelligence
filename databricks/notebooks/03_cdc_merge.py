# Databricks notebook source
# MAGIC %md
# MAGIC # 🔄 Step 3: CDC — Change Data Capture with MERGE INTO
# MAGIC ## Handles upserts (inserts + updates) on Delta tables — the core of incremental DE

# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql import functions as F

catalog = "retail_intelligence"
silver  = f"{catalog}.silver"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Why CDC?
# MAGIC Real source systems don't just append — they update and delete records.
# MAGIC MERGE INTO on Delta handles this with ACID guarantees:
# MAGIC - **MATCHED** → UPDATE existing row
# MAGIC - **NOT MATCHED** → INSERT new row
# MAGIC - **NOT MATCHED BY SOURCE** → DELETE removed rows (Type 1 SCD)

# COMMAND ----------
# MAGIC %md ## 1. Simulate incoming CDC batch (new + updated orders)

# COMMAND ----------
# Simulate: 500 updated orders + 200 new orders
from pyspark.sql.types import *
import random

# Read existing bronze orders
bronze_orders = spark.table(f"{catalog}.bronze.bronze_orders")

# Simulate updates: change status on some completed orders
updated_orders = (bronze_orders
    .sample(fraction=0.01, seed=42)
    .withColumn("status", F.lit("Returned"))
    .withColumn("_cdc_operation", F.lit("UPDATE"))
    .withColumn("_cdc_timestamp", F.current_timestamp())
)

# Simulate inserts: new orders
new_orders = (bronze_orders
    .sample(fraction=0.004, seed=99)
    .withColumn("order_id", F.concat(F.lit("NEW_"), F.col("order_id")))
    .withColumn("order_date", F.lit("2024-09-01"))
    .withColumn("_cdc_operation", F.lit("INSERT"))
    .withColumn("_cdc_timestamp", F.current_timestamp())
)

cdc_batch = updated_orders.union(new_orders)
print(f"CDC batch: {cdc_batch.count()} records ({updated_orders.count()} updates, {new_orders.count()} inserts)")

# COMMAND ----------
# MAGIC %md ## 2. MERGE INTO Silver Orders (Upsert)

# COMMAND ----------
if spark.catalog.tableExists(f"{silver}.silver_orders"):
    target = DeltaTable.forName(spark, f"{silver}.silver_orders")
    
    (target.alias("target")
        .merge(cdc_batch.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("✅ MERGE INTO silver_orders completed")
else:
    print("ℹ️ silver_orders not yet created — run dbt first, then re-run CDC")

# COMMAND ----------
# MAGIC %md ## 3. Handle Deletes (Soft Delete Pattern)

# COMMAND ----------
# Best practice: never hard-delete — use a 'is_deleted' flag
delete_sql = f"""
MERGE INTO {silver}.silver_orders AS target
USING (SELECT 'O000001' AS order_id) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET target.is_deleted = true, target._updated_at = current_timestamp()
"""
print("📌 Soft-delete pattern (run when needed):")
print(delete_sql)

# COMMAND ----------
# MAGIC %md ## 4. Delta Time Travel — View History

# COMMAND ----------
if spark.catalog.tableExists(f"{silver}.silver_orders"):
    history = spark.sql(f"DESCRIBE HISTORY {silver}.silver_orders LIMIT 5")
    display(history.select("version","timestamp","operation","operationParameters"))

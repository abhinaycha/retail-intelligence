# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Step 2: Auto Loader → Bronze Layer
# MAGIC ## Incrementally ingests CSV files from DBFS into Delta Bronze tables

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *

catalog = "retail_intelligence"
bronze  = f"{catalog}.bronze"

# Source paths (upload CSVs here first via DBFS)
base_path = "dbfs:/FileStore/retail_intelligence/raw"

# COMMAND ----------
# MAGIC %md ## 1. Ingest Orders → Bronze

# COMMAND ----------
orders_schema = StructType([
    StructField("order_id",     StringType()),
    StructField("customer_id",  StringType()),
    StructField("product_id",   StringType()),
    StructField("quantity",     IntegerType()),
    StructField("unit_price",   DoubleType()),
    StructField("discount_pct", DoubleType()),
    StructField("gross_amount", DoubleType()),
    StructField("net_amount",   DoubleType()),
    StructField("order_date",   StringType()),
    StructField("channel",      StringType()),
    StructField("region",       StringType()),
    StructField("status",       StringType()),
    StructField("ship_days",    IntegerType()),
])

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{base_path}/_schema/orders")
    .option("header", "true")
    .schema(orders_schema)
    .load(f"{base_path}/orders/")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{base_path}/_checkpoint/orders")
    .option("mergeSchema", "true")
    .toTable(f"{bronze}.bronze_orders")
)
print("✅ bronze_orders stream started")

# COMMAND ----------
# MAGIC %md ## 2. Ingest Products → Bronze

# COMMAND ----------
products_schema = StructType([
    StructField("product_id",   StringType()),
    StructField("product_name", StringType()),
    StructField("category",     StringType()),
    StructField("subcategory",  StringType()),
    StructField("cost_price",   DoubleType()),
    StructField("retail_price", DoubleType()),
    StructField("supplier_id",  StringType()),
    StructField("is_active",    StringType()),
])

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{base_path}/_schema/products")
    .option("header", "true")
    .schema(products_schema)
    .load(f"{base_path}/products/")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{base_path}/_checkpoint/products")
    .toTable(f"{bronze}.bronze_products")
)
print("✅ bronze_products stream started")

# COMMAND ----------
# MAGIC %md ## 3. Ingest Customers → Bronze

# COMMAND ----------
customers_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("first_name",  StringType()),
    StructField("last_name",   StringType()),
    StructField("email",       StringType()),
    StructField("age",         IntegerType()),
    StructField("city",        StringType()),
    StructField("state",       StringType()),
    StructField("segment",     StringType()),
    StructField("join_date",   StringType()),
    StructField("is_active",   StringType()),
])

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{base_path}/_schema/customers")
    .option("header", "true")
    .schema(customers_schema)
    .load(f"{base_path}/customers/")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{base_path}/_checkpoint/customers")
    .toTable(f"{bronze}.bronze_customers")
)
print("✅ bronze_customers stream started")

# COMMAND ----------
# MAGIC %md ## 4. Ingest Inventory → Bronze

# COMMAND ----------
inventory_schema = StructType([
    StructField("inventory_id",     StringType()),
    StructField("product_id",       StringType()),
    StructField("warehouse_id",     StringType()),
    StructField("snapshot_date",    StringType()),
    StructField("quantity_on_hand", IntegerType()),
    StructField("reorder_level",    IntegerType()),
    StructField("is_below_reorder", StringType()),
])

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{base_path}/_schema/inventory")
    .option("header", "true")
    .schema(inventory_schema)
    .load(f"{base_path}/inventory/")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{base_path}/_checkpoint/inventory")
    .toTable(f"{bronze}.bronze_inventory")
)
print("✅ bronze_inventory stream started")

# COMMAND ----------
# MAGIC %md ## 5. Verify Bronze Tables

# COMMAND ----------
for tbl in ["bronze_orders","bronze_products","bronze_customers","bronze_inventory"]:
    count = spark.table(f"{bronze}.{tbl}").count()
    print(f"  {tbl}: {count:,} rows")

# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ Delta Live Tables Pipeline — Bronze → Silver
# MAGIC ## Declarative data pipeline with built-in quality expectations

# COMMAND ----------
import dlt
from pyspark.sql import functions as F

catalog = "retail_intelligence"

# COMMAND ----------
# MAGIC %md ## BRONZE LAYER — Raw ingestion with DLT

# COMMAND ----------
@dlt.table(
    name="bronze_orders",
    comment="Raw orders ingested from CSV files via Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", "dbfs:/FileStore/retail_intelligence/raw/_schema/orders_dlt")
        .load("dbfs:/FileStore/retail_intelligence/raw/orders/")
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------
@dlt.table(name="bronze_products",  comment="Raw products", table_properties={"quality": "bronze"})
def bronze_products():
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.schemaLocation","dbfs:/FileStore/retail_intelligence/raw/_schema/products_dlt")
        .load("dbfs:/FileStore/retail_intelligence/raw/products/")
        .withColumn("_ingested_at", F.current_timestamp())
    )

@dlt.table(name="bronze_customers", comment="Raw customers", table_properties={"quality": "bronze"})
def bronze_customers():
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.schemaLocation","dbfs:/FileStore/retail_intelligence/raw/_schema/customers_dlt")
        .load("dbfs:/FileStore/retail_intelligence/raw/customers/")
        .withColumn("_ingested_at", F.current_timestamp())
    )

@dlt.table(name="bronze_inventory", comment="Raw inventory", table_properties={"quality": "bronze"})
def bronze_inventory():
    return (spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.schemaLocation","dbfs:/FileStore/retail_intelligence/raw/_schema/inventory_dlt")
        .load("dbfs:/FileStore/retail_intelligence/raw/inventory/")
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------
# MAGIC %md ## SILVER LAYER — Cleaned + validated with expectations

# COMMAND ----------
@dlt.table(
    name="silver_orders",
    comment="Cleaned, validated orders — Silver layer",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
)
@dlt.expect_or_drop("valid_order_id",    "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_quantity",    "quantity > 0")
@dlt.expect_or_drop("valid_amount",      "net_amount > 0")
@dlt.expect("valid_order_date",          "order_date IS NOT NULL")
@dlt.expect("valid_discount",            "discount_pct BETWEEN 0 AND 1")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .withColumn("order_date",  F.to_date("order_date"))
        .withColumn("discount_pct",F.col("discount_pct").cast("double"))
        .withColumn("margin_pct",  F.round((F.col("net_amount") - F.col("gross_amount")*0.6) / F.col("net_amount"), 4))
        .withColumn("is_deleted",  F.lit(False))
        .withColumn("_updated_at", F.current_timestamp())
        .dropDuplicates(["order_id"])
    )

# COMMAND ----------
@dlt.table(
    name="silver_customers",
    comment="Cleaned customers with PII-masked view",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_age",                 "age BETWEEN 18 AND 100")
@dlt.expect("valid_email",               "email LIKE '%@%.%'")
def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
        .withColumn("join_date",   F.to_date("join_date"))
        .withColumn("is_active",   F.col("is_active").cast("boolean"))
        .withColumn("full_name",   F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
        .withColumn("email_masked",F.regexp_replace("email", r"(?<=.{2}).(?=.*@)", "*"))
        .dropDuplicates(["customer_id"])
    )

# COMMAND ----------
@dlt.table(name="silver_products",  comment="Cleaned products", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.expect("positive_cost",            "cost_price > 0")
@dlt.expect("positive_retail",          "retail_price > 0")
def silver_products():
    return (
        dlt.read_stream("bronze_products")
        .withColumn("is_active",   F.col("is_active").cast("boolean"))
        .withColumn("margin_pct",  F.round((F.col("retail_price")-F.col("cost_price"))/F.col("retail_price"),4))
        .dropDuplicates(["product_id"])
    )

# COMMAND ----------
@dlt.table(name="silver_inventory", comment="Cleaned inventory snapshots", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_inventory_id",   "inventory_id IS NOT NULL")
@dlt.expect("non_negative_stock",           "quantity_on_hand >= 0")
def silver_inventory():
    return (
        dlt.read_stream("bronze_inventory")
        .withColumn("snapshot_date",   F.to_date("snapshot_date"))
        .withColumn("is_below_reorder",F.col("is_below_reorder").cast("boolean"))
        .dropDuplicates(["inventory_id"])
    )

# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ Step 4: Performance Optimization
# MAGIC ## OPTIMIZE, Z-ORDER, Liquid Clustering, VACUUM

# COMMAND ----------
catalog = "retail_intelligence"
silver  = f"{catalog}.silver"
gold    = f"{catalog}.gold"

# COMMAND ----------
# MAGIC %md ## 1. OPTIMIZE — Compact small files into large Parquet files

# COMMAND ----------
# Small files are the #1 performance killer in Delta Lake
# OPTIMIZE merges them into ~1GB files for efficient reads

spark.sql(f"OPTIMIZE {silver}.silver_orders")
print("✅ silver_orders optimized")

spark.sql(f"OPTIMIZE {silver}.silver_customers")
print("✅ silver_customers optimized")

# COMMAND ----------
# MAGIC %md ## 2. Z-ORDER — Multi-dimensional clustering for fast filtering

# COMMAND ----------
# Z-ORDER co-locates related data — dramatically speeds up WHERE filters
# Best columns: those you filter most often

spark.sql(f"""
    OPTIMIZE {silver}.silver_orders
    ZORDER BY (order_date, region, product_id)
""")
print("✅ silver_orders Z-ORDERED on order_date, region, product_id")

spark.sql(f"""
    OPTIMIZE {gold}.gold_daily_sales
    ZORDER BY (order_date, region)
""")
print("✅ gold_daily_sales Z-ORDERED on order_date, region")

# COMMAND ----------
# MAGIC %md ## 3. Liquid Clustering (DBR 13.3+) — Auto-adaptive clustering

# COMMAND ----------
# Liquid Clustering is the modern replacement for Z-ORDER
# It self-tunes without needing OPTIMIZE to run on a schedule

liquid_sql = f"""
-- Enable Liquid Clustering on a table (set at CREATE time):
-- CREATE TABLE {silver}.silver_orders
--   CLUSTER BY (order_date, region, product_id)
--   AS SELECT * FROM {catalog}.bronze.bronze_orders;

-- Check clustering info:
-- DESCRIBE DETAIL {silver}.silver_orders;
"""
print("📌 Liquid Clustering DDL (for new tables):")
print(liquid_sql)

# COMMAND ----------
# MAGIC %md ## 4. VACUUM — Remove old file versions to save storage

# COMMAND ----------
# Delta keeps old file versions for time travel
# VACUUM removes files older than retention period (default 7 days)

# Safety: always check what will be deleted first
spark.sql(f"SET spark.databricks.delta.retentionDurationCheck.enabled = false")

for table in [f"{silver}.silver_orders", f"{silver}.silver_customers", f"{gold}.gold_daily_sales"]:
    try:
        spark.sql(f"VACUUM {table} RETAIN 168 HOURS DRY RUN")  # 168h = 7 days
        print(f"✅ VACUUM DRY RUN on {table}")
    except Exception as e:
        print(f"  ℹ️ {table}: {e}")

# COMMAND ----------
# MAGIC %md ## 5. Table Statistics — Help query optimizer

# COMMAND ----------
for table in [f"{silver}.silver_orders", f"{silver}.silver_products"]:
    try:
        spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS")
        print(f"✅ Statistics computed for {table}")
    except Exception as e:
        print(f"  ℹ️ {table}: {e}")

# COMMAND ----------
# MAGIC %md ## 6. Caching hot tables

# COMMAND ----------
# Cache Gold tables that get queried repeatedly by dashboards
try:
    spark.sql(f"CACHE SELECT * FROM {gold}.gold_daily_sales")
    print("✅ gold_daily_sales cached in memory")
except Exception as e:
    print(f"ℹ️ Cache note: {e}")

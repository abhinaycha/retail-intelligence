# Databricks notebook source
# MAGIC %md
# MAGIC # 🏗️ Step 1: Unity Catalog Setup
# MAGIC ## Creates catalog → schemas (bronze, silver, gold) + PII tags

# COMMAND ----------
# MAGIC %md ## 1. Create Catalog

# COMMAND ----------
catalog_name = "retail_intelligence"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")
print(f"✅ Catalog '{catalog_name}' ready")

# COMMAND ----------
# MAGIC %md ## 2. Create Schemas (Bronze / Silver / Gold)

# COMMAND ----------
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema} COMMENT 'Medallion layer: {schema}'")
    print(f"✅ Schema '{schema}' created")

# COMMAND ----------
# MAGIC %md ## 3. Tag PII Columns (column-level security)

# COMMAND ----------
# PII tagging will be applied after tables are created in later steps
pii_policy = """
-- Run after silver tables exist:
-- ALTER TABLE retail_intelligence.silver.silver_customers
--   ALTER COLUMN email SET TAGS ('pii' = 'true', 'pii_type' = 'email');
-- ALTER TABLE retail_intelligence.silver.silver_customers
--   ALTER COLUMN first_name SET TAGS ('pii' = 'true', 'pii_type' = 'name');
"""
print("📌 PII tagging SQL ready — run after silver tables are created")
print(pii_policy)

# COMMAND ----------
# MAGIC %md ## 4. Set Default Permissions

# COMMAND ----------
spark.sql(f"GRANT USAGE ON CATALOG {catalog_name} TO `account users`")
spark.sql(f"GRANT USE SCHEMA ON DATABASE {catalog_name}.bronze TO `account users`")
spark.sql(f"GRANT SELECT ON DATABASE {catalog_name}.silver TO `account users`")
spark.sql(f"GRANT SELECT ON DATABASE {catalog_name}.gold TO `account users`")
print("✅ Default permissions granted")

# COMMAND ----------
display(spark.sql(f"SHOW SCHEMAS IN {catalog_name}"))

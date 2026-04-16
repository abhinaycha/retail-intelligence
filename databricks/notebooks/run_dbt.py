# Databricks notebook source
# MAGIC %md # dbt Runner — Execute dbt commands from Databricks

# COMMAND ----------
# Install dbt-databricks
%pip install dbt-databricks dbt-utils dbt-expectations --quiet
dbutils.library.restartPython()

# COMMAND ----------
import subprocess, os

# Get dbt command from workflow parameter (default: full run)
dbt_command = dbutils.widgets.get("dbt_command") if dbutils.widgets.getArgument("dbt_command", "") else "dbt run"

# Set Databricks token for dbt profile
os.environ["DATABRICKS_TOKEN"] = dbutils.secrets.get(scope="retail_intelligence", key="databricks_token")

# Navigate to dbt project directory (within Repos)
dbt_project_path = "/Workspace/Repos/er.abhinay2011@gmail.com/retail-intelligence/dbt"

# Run dbt command
result = subprocess.run(
    f"cd {dbt_project_path} && {dbt_command} --profiles-dir .",
    shell=True, capture_output=True, text=True
)

print("=== STDOUT ===")
print(result.stdout)
print("=== STDERR ===")
print(result.stderr)

if result.returncode != 0:
    raise Exception(f"dbt command failed: {dbt_command}")
print(f"✅ {dbt_command} completed successfully")

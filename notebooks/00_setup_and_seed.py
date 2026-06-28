# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup and seed source CSV files

# COMMAND ----------

from datetime import date
import os
import sys

repo_root = os.getcwd()
if repo_root.endswith("/notebooks"):
    repo_root = os.path.dirname(repo_root)
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.append(src_path)

from theatre_ops.sample_data import source_batches  # noqa: E402

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("run_date", str(date.today()))
dbutils.widgets.dropdown("full_refresh", "true", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
run_date = dbutils.widgets.get("run_date")
full_refresh = dbutils.widgets.get("full_refresh").lower() == "true"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.landing")

landing_root = f"/Volumes/{catalog}/bronze/landing"

if full_refresh:
    for child in ["_schemas", "_checkpoints"]:
        dbutils.fs.rm(f"{landing_root}/{child}", True)

# COMMAND ----------

batches = source_batches(run_date)

for source_name, batch in batches.items():
    target_path = f"{landing_root}/{source_name}/{batch.partition_column}={run_date}"
    if full_refresh:
        dbutils.fs.rm(f"{landing_root}/{source_name}", True)
    df = spark.createDataFrame(batch.rows)
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(target_path)
    )
    print(f"Wrote {df.count()} rows to {target_path}")

# COMMAND ----------

display(dbutils.fs.ls(landing_root))

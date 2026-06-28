# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze ingestion with Auto Loader

# COMMAND ----------

from datetime import date

from pyspark.sql import functions as F

SOURCES = [
    "theatre_master",
    "screen_master",
    "movie_master",
    "showtime_schedule",
    "booking_snapshot",
    "screen_availability",
    "schedule_policy",
]

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("run_date", str(date.today()))
dbutils.widgets.dropdown("full_refresh", "true", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
run_date = dbutils.widgets.get("run_date")
full_refresh = dbutils.widgets.get("full_refresh").lower() == "true"

landing_root = f"/Volumes/{catalog}/bronze/landing"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")

# COMMAND ----------

for source in SOURCES:
    table_name = f"{catalog}.bronze.bronze_{source}"
    source_path = f"{landing_root}/{source}"
    schema_path = f"{landing_root}/_schemas/{source}"
    checkpoint_path = f"{landing_root}/_checkpoints/{source}"

    if full_refresh:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        dbutils.fs.rm(schema_path, True)
        dbutils.fs.rm(checkpoint_path, True)

    raw_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .load(source_path)
    )

    if "_rescued_data" not in raw_df.columns:
        raw_df = raw_df.withColumn("_rescued_data", F.lit(None).cast("string"))

    bronze_df = (
        raw_df.withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(run_date))
        .withColumn(
            "_source_snapshot_date",
            F.regexp_extract(F.input_file_name(), r"(snapshot_date|load_date)=([^/]+)", 2),
        )
    )

    query = (
        bronze_df.writeStream.option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)
    )
    query.awaitTermination()

    row_count = spark.table(table_name).count()
    print(f"Auto Loader wrote {row_count} rows to {table_name}")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.bronze"))

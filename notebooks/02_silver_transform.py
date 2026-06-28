# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver current-state tables

# COMMAND ----------

from datetime import date

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("run_date", str(date.today()))

catalog = dbutils.widgets.get("catalog")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.silver.dim_theatre AS
    SELECT DISTINCT
      theatre_id,
      theatre_name,
      city,
      state,
      region,
      timezone,
      theatre_status,
      to_timestamp(source_updated_ts) AS source_updated_ts
    FROM {catalog}.bronze.bronze_theatre_master
    WHERE theatre_id IS NOT NULL
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.silver.dim_screen AS
    SELECT DISTINCT
      screen_id,
      theatre_id,
      screen_name,
      CAST(capacity AS INT) AS capacity,
      screen_format,
      CAST(is_active AS BOOLEAN) AS is_active,
      to_timestamp(source_updated_ts) AS source_updated_ts
    FROM {catalog}.bronze.bronze_screen_master
    WHERE screen_id IS NOT NULL
      AND CAST(capacity AS INT) > 0
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.silver.dim_movie AS
    SELECT DISTINCT
      movie_id,
      movie_title,
      genre,
      language,
      CAST(runtime_minutes AS INT) AS runtime_minutes,
      movie_format,
      CAST(release_date AS DATE) AS release_date,
      distributor,
      CAST(average_ticket_price AS DOUBLE) AS average_ticket_price,
      CAST(seeded_baseline_demand AS DOUBLE) AS seeded_baseline_demand,
      to_timestamp(source_updated_ts) AS source_updated_ts
    FROM {catalog}.bronze.bronze_movie_master
    WHERE movie_id IS NOT NULL
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.silver.fact_showtime_schedule_current AS
    WITH ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY showtime_id
          ORDER BY to_timestamp(source_updated_ts) DESC
        ) AS rn
      FROM {catalog}.bronze.bronze_showtime_schedule
    )
    SELECT
      showtime_id,
      theatre_id,
      screen_id,
      movie_id,
      to_timestamp(show_start_ts) AS show_start_ts,
      to_timestamp(show_end_ts) AS show_end_ts,
      schedule_status,
      CAST(is_locked AS BOOLEAN) AS is_locked,
      to_timestamp(published_ts) AS published_ts,
      to_timestamp(source_updated_ts) AS source_updated_ts
    FROM ranked
    WHERE rn = 1
      AND to_timestamp(show_end_ts) > to_timestamp(show_start_ts)
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.silver.fact_booking_snapshot AS
    WITH ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY showtime_id, snapshot_ts
          ORDER BY to_timestamp(source_updated_ts) DESC
        ) AS rn
      FROM {catalog}.bronze.bronze_booking_snapshot
    )
    SELECT
      snapshot_id,
      showtime_id,
      to_timestamp(snapshot_ts) AS snapshot_ts,
      CAST(booked_seats AS INT) AS booked_seats,
      CAST(booking_count AS INT) AS booking_count,
      CAST(gross_booking_revenue AS DOUBLE) AS gross_booking_revenue,
      CAST(hours_to_show AS DOUBLE) AS hours_to_show,
      to_timestamp(source_updated_ts) AS source_updated_ts
    FROM ranked
    WHERE rn = 1
      AND CAST(booked_seats AS INT) >= 0
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.silver.fact_screen_availability AS
    SELECT
      availability_id,
      theatre_id,
      screen_id,
      to_timestamp(available_from_ts) AS available_from_ts,
      to_timestamp(available_to_ts) AS available_to_ts,
      CAST(is_available AS BOOLEAN) AS is_available,
      blackout_reason,
      CAST(minimum_turnaround_minutes AS INT) AS minimum_turnaround_minutes,
      to_timestamp(source_updated_ts) AS source_updated_ts
    FROM {catalog}.bronze.bronze_screen_availability
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.silver.fact_schedule_policy AS
    SELECT
      policy_id,
      theatre_id,
      CAST(freeze_window_hours AS DOUBLE) AS freeze_window_hours,
      CAST(minimum_revenue_uplift AS DOUBLE) AS minimum_revenue_uplift,
      CAST(minimum_confidence_score AS DOUBLE) AS minimum_confidence_score,
      CAST(maximum_recommendations_per_day AS INT) AS maximum_recommendations_per_day,
      to_timestamp(source_updated_ts) AS source_updated_ts
    FROM {catalog}.bronze.bronze_schedule_policy
    """
)

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.silver"))

# Databricks notebook source
# MAGIC %md
# MAGIC # 99 - Screen allocation recommendation demo

# COMMAND ----------

from datetime import date

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("run_date", str(date.today()))

catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current schedule and capacity issue

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          s.theatre_id,
          s.showtime_id,
          m.movie_title,
          sc.screen_name,
          sc.capacity,
          s.show_start_ts,
          s.is_locked
        FROM {catalog}.silver.fact_showtime_schedule_current s
        JOIN {catalog}.silver.dim_movie m ON s.movie_id = m.movie_id
        JOIN {catalog}.silver.dim_screen sc ON s.screen_id = sc.screen_id
        ORDER BY s.theatre_id, s.show_start_ts, sc.capacity
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demand forecast

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          f.theatre_id,
          f.showtime_id,
          m.movie_title,
          f.screen_capacity,
          f.current_booked_seats,
          fc.booking_curve_forecast,
          fc.historical_demand_baseline,
          fc.unconstrained_demand_forecast,
          fc.forecast_confidence_score,
          fc.top_forecast_drivers
        FROM {catalog}.gold.gold_showtime_demand_features f
        JOIN {catalog}.gold.gold_showtime_demand_forecast fc
          ON f.showtime_id = fc.showtime_id
        JOIN {catalog}.silver.dim_movie m
          ON f.movie_id = m.movie_id
        ORDER BY fc.unconstrained_demand_forecast DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Candidate validation

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
          theatre_id,
          showtime_a_id,
          showtime_b_id,
          current_capacity_a,
          current_capacity_b,
          forecast_a,
          forecast_b,
          estimated_ticket_revenue_uplift,
          constraint_validation_status,
          constraint_failure_reason
        FROM {catalog}.gold.gold_screen_allocation_candidates
        ORDER BY constraint_validation_status DESC, estimated_ticket_revenue_uplift DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final recommendations

# COMMAND ----------

recommendations = spark.table(f"{catalog}.gold.gold_screen_allocation_recommendations")
recommendation_count = recommendations.count()
if recommendation_count < 3:
    raise ValueError(f"Expected at least 3 recommendations, found {recommendation_count}")

display(
    recommendations.select(
        "theatre_id",
        "show_date",
        "movie_a_title",
        "current_screen_a",
        "recommended_screen_a",
        "movie_b_title",
        "current_screen_b",
        "recommended_screen_b",
        "forecast_a",
        "forecast_b",
        "estimated_ticket_revenue_uplift",
        "forecast_confidence_score",
        "recommendation_reason",
    ).orderBy("theatre_id", "show_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MVP data quality audit

# COMMAND ----------

display(spark.table(f"{catalog}.ops.ops_data_quality_audit"))

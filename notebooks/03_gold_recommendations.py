# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold forecasts, candidates, and recommendations

# COMMAND ----------

from datetime import date

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("run_date", str(date.today()))

catalog = dbutils.widgets.get("catalog")
run_date = dbutils.widgets.get("run_date")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.ops")

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.gold.gold_showtime_demand_features AS
    WITH latest_booking AS (
      SELECT *
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY showtime_id ORDER BY snapshot_ts DESC) AS rn
        FROM {catalog}.silver.fact_booking_snapshot
      )
      WHERE rn = 1
    )
    SELECT
      s.showtime_id,
      s.theatre_id,
      s.screen_id,
      s.movie_id,
      CAST(s.show_start_ts AS DATE) AS show_date,
      date_format(s.show_start_ts, 'E') AS day_of_week,
      date_format(s.show_start_ts, 'HH:mm') AS time_slot,
      sc.capacity AS screen_capacity,
      COALESCE(b.booked_seats, 0) AS current_booked_seats,
      COALESCE(b.hours_to_show, 999) AS hours_to_show,
      CASE
        WHEN COALESCE(b.hours_to_show, 999) <= 24 THEN 0.55
        WHEN COALESCE(b.hours_to_show, 999) <= 72 THEN 0.42
        ELSE 0.30
      END AS expected_booking_completion_pct,
      ROUND(COALESCE(b.booked_seats, 0) / sc.capacity, 4) AS booking_pace_ratio,
      m.seeded_baseline_demand AS historical_avg_attendance_movie,
      ROUND(m.seeded_baseline_demand / sc.capacity, 4) AS historical_avg_occupancy_movie,
      m.average_ticket_price,
      m.movie_format,
      sc.screen_format,
      s.show_start_ts,
      s.show_end_ts,
      s.is_locked,
      CASE WHEN dayofweek(s.show_start_ts) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
      CASE WHEN b.showtime_id IS NULL THEN 'NO_BOOKING_SNAPSHOT' ELSE 'OK' END
        AS forecast_input_quality_status,
      current_timestamp() AS feature_run_ts
    FROM {catalog}.silver.fact_showtime_schedule_current s
    JOIN {catalog}.silver.dim_screen sc
      ON s.screen_id = sc.screen_id
    JOIN {catalog}.silver.dim_movie m
      ON s.movie_id = m.movie_id
    LEFT JOIN latest_booking b
      ON s.showtime_id = b.showtime_id
    WHERE s.schedule_status = 'PUBLISHED'
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.gold.gold_showtime_demand_forecast AS
    SELECT
      showtime_id,
      concat('forecast-', '{run_date}') AS forecast_run_id,
      ROUND(
        GREATEST(
          current_booked_seats / expected_booking_completion_pct,
          historical_avg_attendance_movie
        ),
        2
      ) AS unconstrained_demand_forecast,
      CASE
        WHEN forecast_input_quality_status = 'OK' AND hours_to_show <= 96 THEN 0.86
        WHEN forecast_input_quality_status = 'OK' THEN 0.72
        ELSE 0.50
      END AS forecast_confidence_score,
      ROUND(current_booked_seats / expected_booking_completion_pct, 2)
        AS booking_curve_forecast,
      historical_avg_attendance_movie AS historical_demand_baseline,
      concat(
        'booked_seats=', current_booked_seats,
        '; baseline=', historical_avg_attendance_movie,
        '; completion_pct=', expected_booking_completion_pct
      ) AS top_forecast_drivers,
      current_timestamp() AS forecast_run_ts
    FROM {catalog}.gold.gold_showtime_demand_features
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.gold.gold_screen_allocation_candidates AS
    WITH enriched AS (
      SELECT
        f.*,
        fc.unconstrained_demand_forecast,
        fc.forecast_confidence_score,
        p.freeze_window_hours,
        p.minimum_revenue_uplift,
        p.minimum_confidence_score,
        p.maximum_recommendations_per_day
      FROM {catalog}.gold.gold_showtime_demand_features f
      JOIN {catalog}.gold.gold_showtime_demand_forecast fc
        ON f.showtime_id = fc.showtime_id
      JOIN {catalog}.silver.fact_schedule_policy p
        ON f.theatre_id = p.theatre_id
    ),
    paired AS (
      SELECT
        sha2(concat_ws('|', a.showtime_id, b.showtime_id, '{run_date}'), 256) AS candidate_id,
        a.theatre_id,
        a.showtime_id AS showtime_a_id,
        b.showtime_id AS showtime_b_id,
        a.movie_id AS movie_a_id,
        b.movie_id AS movie_b_id,
        a.screen_id AS current_screen_a_id,
        b.screen_id AS current_screen_b_id,
        b.screen_id AS proposed_screen_a_id,
        a.screen_id AS proposed_screen_b_id,
        a.unconstrained_demand_forecast AS forecast_a,
        b.unconstrained_demand_forecast AS forecast_b,
        a.screen_capacity AS current_capacity_a,
        b.screen_capacity AS current_capacity_b,
        b.screen_capacity AS proposed_capacity_a,
        a.screen_capacity AS proposed_capacity_b,
        a.average_ticket_price AS price_a,
        b.average_ticket_price AS price_b,
        a.movie_format AS movie_a_format,
        b.movie_format AS movie_b_format,
        b.screen_format AS proposed_screen_a_format,
        a.screen_format AS proposed_screen_b_format,
        a.show_start_ts,
        LEAST(a.forecast_confidence_score, b.forecast_confidence_score)
          AS forecast_confidence_score,
        a.minimum_revenue_uplift,
        a.minimum_confidence_score,
        a.maximum_recommendations_per_day,
        a.is_locked AS showtime_a_locked,
        b.is_locked AS showtime_b_locked,
        LEAST(a.hours_to_show, b.hours_to_show) AS min_hours_to_show,
        a.freeze_window_hours,
        CASE
          WHEN a.movie_format = 'STANDARD'
            THEN b.screen_format IN ('STANDARD', 'PREMIUM')
          ELSE a.movie_format = b.screen_format
        END AS movie_a_target_compatible,
        CASE
          WHEN b.movie_format = 'STANDARD'
            THEN a.screen_format IN ('STANDARD', 'PREMIUM')
          ELSE b.movie_format = a.screen_format
        END AS movie_b_target_compatible,
        ROUND(
          LEAST(a.unconstrained_demand_forecast, a.screen_capacity) * a.average_ticket_price
          + LEAST(b.unconstrained_demand_forecast, b.screen_capacity) * b.average_ticket_price,
          2
        ) AS current_expected_ticket_revenue,
        ROUND(
          LEAST(a.unconstrained_demand_forecast, b.screen_capacity) * a.average_ticket_price
          + LEAST(b.unconstrained_demand_forecast, a.screen_capacity) * b.average_ticket_price,
          2
        ) AS proposed_expected_ticket_revenue
      FROM enriched a
      JOIN enriched b
        ON a.theatre_id = b.theatre_id
       AND a.showtime_id <> b.showtime_id
       AND ABS(unix_timestamp(a.show_start_ts) - unix_timestamp(b.show_start_ts)) <= 900
       AND a.unconstrained_demand_forecast > a.screen_capacity
       AND b.unconstrained_demand_forecast < b.screen_capacity
       AND b.screen_capacity > a.screen_capacity
    )
    SELECT
      *,
      ROUND(proposed_expected_ticket_revenue - current_expected_ticket_revenue, 2)
        AS estimated_ticket_revenue_uplift,
      CASE
        WHEN showtime_a_locked OR showtime_b_locked THEN 'INVALID'
        WHEN min_hours_to_show <= freeze_window_hours THEN 'INVALID'
        WHEN NOT movie_a_target_compatible OR NOT movie_b_target_compatible THEN 'INVALID'
        WHEN proposed_expected_ticket_revenue <= current_expected_ticket_revenue THEN 'INVALID'
        ELSE 'VALID'
      END AS constraint_validation_status,
      CASE
        WHEN showtime_a_locked OR showtime_b_locked THEN 'LOCKED_SHOWTIME'
        WHEN min_hours_to_show <= freeze_window_hours THEN 'FREEZE_WINDOW'
        WHEN NOT movie_a_target_compatible OR NOT movie_b_target_compatible
          THEN 'FORMAT_INCOMPATIBLE'
        WHEN proposed_expected_ticket_revenue <= current_expected_ticket_revenue
          THEN 'NON_POSITIVE_UPLIFT'
        ELSE ''
      END AS constraint_failure_reason,
      current_timestamp() AS candidate_run_ts
    FROM paired
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.gold.gold_screen_allocation_recommendations AS
    WITH candidate_details AS (
      SELECT
        c.*,
        ma.movie_title AS movie_a_title,
        mb.movie_title AS movie_b_title,
        sa.screen_name AS current_screen_a,
        sb.screen_name AS current_screen_b,
        sb.screen_name AS recommended_screen_a,
        sa.screen_name AS recommended_screen_b,
        CAST(c.show_start_ts AS DATE) AS show_date,
        ROW_NUMBER() OVER (
          PARTITION BY c.theatre_id, CAST(c.show_start_ts AS DATE)
          ORDER BY c.estimated_ticket_revenue_uplift DESC
        ) AS daily_rank
      FROM {catalog}.gold.gold_screen_allocation_candidates c
      JOIN {catalog}.silver.dim_movie ma ON c.movie_a_id = ma.movie_id
      JOIN {catalog}.silver.dim_movie mb ON c.movie_b_id = mb.movie_id
      JOIN {catalog}.silver.dim_screen sa ON c.current_screen_a_id = sa.screen_id
      JOIN {catalog}.silver.dim_screen sb ON c.current_screen_b_id = sb.screen_id
      WHERE c.constraint_validation_status = 'VALID'
        AND c.estimated_ticket_revenue_uplift >= c.minimum_revenue_uplift
        AND c.forecast_confidence_score >= c.minimum_confidence_score
    )
    SELECT
      sha2(concat_ws('|', candidate_id, 'recommendation'), 256) AS recommendation_id,
      concat('recommendation-', '{run_date}') AS recommendation_run_id,
      'PROPOSED' AS recommendation_status,
      theatre_id,
      show_date,
      show_start_ts,
      showtime_a_id,
      showtime_b_id,
      movie_a_title,
      movie_b_title,
      current_screen_a,
      current_screen_b,
      recommended_screen_a,
      recommended_screen_b,
      forecast_a,
      forecast_b,
      current_capacity_a,
      current_capacity_b,
      estimated_ticket_revenue_uplift,
      forecast_confidence_score,
      concat(
        'Move ', movie_a_title, ' from ', current_screen_a, ' to ', recommended_screen_a,
        ' because predicted demand is ', CAST(ROUND(forecast_a, 0) AS INT),
        ' seats against current capacity of ', current_capacity_a,
        ', while ', movie_b_title, ' is predicted at ',
        CAST(ROUND(forecast_b, 0) AS INT),
        ' seats against capacity of ', current_capacity_b,
        '. Estimated ticket revenue uplift: $',
        CAST(ROUND(estimated_ticket_revenue_uplift, 0) AS INT),
        '.'
      ) AS recommendation_reason,
      'mvp_rules_v1' AS rules_version,
      'baseline_forecast_v1' AS forecast_version,
      current_timestamp() AS data_freshness_ts,
      current_timestamp() AS created_ts
    FROM candidate_details
    WHERE daily_rank <= maximum_recommendations_per_day
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {catalog}.ops.ops_data_quality_audit AS
    SELECT
      current_timestamp() AS check_ts,
      'gold_recommendations_min_count' AS check_name,
      CASE WHEN COUNT(*) >= 3 THEN 'PASS' ELSE 'FAIL' END AS status,
      concat('recommendation_count=', COUNT(*)) AS detail
    FROM {catalog}.gold.gold_screen_allocation_recommendations
    UNION ALL
    SELECT
      current_timestamp(),
      'gold_candidates_invalid_present',
      CASE
        WHEN SUM(CASE WHEN constraint_validation_status = 'INVALID' THEN 1 ELSE 0 END) >= 1
          THEN 'PASS'
        ELSE 'FAIL'
      END,
      concat(
        'invalid_candidate_count=',
        SUM(CASE WHEN constraint_validation_status = 'INVALID' THEN 1 ELSE 0 END)
      )
    FROM {catalog}.gold.gold_screen_allocation_candidates
    """
)

recommendation_count = spark.table(
    f"{catalog}.gold.gold_screen_allocation_recommendations"
).count()
if recommendation_count < 3:
    raise ValueError(f"Expected at least 3 recommendations, found {recommendation_count}")

print(f"Generated {recommendation_count} recommendations")

# COMMAND ----------

display(spark.table(f"{catalog}.gold.gold_screen_allocation_recommendations"))

"""Local Spark E2E pipeline for the Databricks MVP."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from theatre_ops.sample_data import source_batches


def run_spark_pipeline(spark: SparkSession, run_date: str, output_root: Path) -> dict[str, int]:
    """Run the MVP with local Spark temp views.

    Auto Loader's ``cloudFiles`` source exists in Databricks Runtime, not local open-source
    Spark. This runner uses normal CSV reads for the Bronze substitute, then runs the same
    Silver and Gold Spark SQL shape used by the Databricks notebooks.
    """
    spark.sparkContext.setLogLevel("WARN")
    output_root.mkdir(parents=True, exist_ok=True)
    spark.sparkContext.setCheckpointDir(str(output_root / "_spark_checkpoints"))

    _write_source_csvs_with_spark(spark, run_date, output_root)
    _create_bronze_views(spark, run_date, output_root)
    _create_silver_views(spark)
    _create_gold_views(spark, run_date)

    counts = {
        "bronze_tables": len(source_batches(run_date)),
        "silver_tables": 7,
        "gold_features": spark.table("gold_showtime_demand_features").count(),
        "gold_forecasts": spark.table("gold_showtime_demand_forecast").count(),
        "gold_candidates": spark.table("gold_screen_allocation_candidates").count(),
        "gold_recommendations": spark.table("gold_screen_allocation_recommendations").count(),
    }
    _validate_counts(spark, counts)
    return counts


def _write_source_csvs_with_spark(spark: SparkSession, run_date: str, output_root: Path) -> None:
    for source_name, batch in source_batches(run_date).items():
        target_path = output_root / source_name / f"{batch.partition_column}={run_date}"
        df = spark.createDataFrame(batch.rows)
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(target_path))


def _create_bronze_views(spark: SparkSession, run_date: str, output_root: Path) -> None:
    for source_name, batch in source_batches(run_date).items():
        source_path = output_root / source_name
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(source_path))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_batch_id", F.lit(run_date))
            .withColumn("_source_snapshot_date", F.lit(run_date))
            .withColumn("_rescued_data", F.lit(None).cast("string"))
        )
        df.createOrReplaceTempView(f"bronze_{source_name}")
        _materialize_view(spark, f"bronze_{source_name}")


def _create_silver_views(spark: SparkSession) -> None:
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW dim_theatre AS
        SELECT DISTINCT
          theatre_id,
          theatre_name,
          city,
          state,
          region,
          timezone,
          theatre_status,
          to_timestamp(source_updated_ts) AS source_updated_ts
        FROM bronze_theatre_master
        WHERE theatre_id IS NOT NULL
        """
    )
    _materialize_view(spark, "dim_theatre")

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW dim_screen AS
        SELECT DISTINCT
          screen_id,
          theatre_id,
          screen_name,
          CAST(capacity AS INT) AS capacity,
          screen_format,
          CAST(is_active AS BOOLEAN) AS is_active,
          to_timestamp(source_updated_ts) AS source_updated_ts
        FROM bronze_screen_master
        WHERE screen_id IS NOT NULL
          AND CAST(capacity AS INT) > 0
        """
    )
    _materialize_view(spark, "dim_screen")

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW dim_movie AS
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
        FROM bronze_movie_master
        WHERE movie_id IS NOT NULL
        """
    )
    _materialize_view(spark, "dim_movie")

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW fact_showtime_schedule_current AS
        WITH ranked AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY showtime_id
              ORDER BY to_timestamp(source_updated_ts) DESC
            ) AS rn
          FROM bronze_showtime_schedule
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
    _materialize_view(spark, "fact_showtime_schedule_current")

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW fact_booking_snapshot AS
        WITH ranked AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY showtime_id, snapshot_ts
              ORDER BY to_timestamp(source_updated_ts) DESC
            ) AS rn
          FROM bronze_booking_snapshot
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
    _materialize_view(spark, "fact_booking_snapshot")

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW fact_screen_availability AS
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
        FROM bronze_screen_availability
        """
    )
    _materialize_view(spark, "fact_screen_availability")

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW fact_schedule_policy AS
        SELECT
          policy_id,
          theatre_id,
          CAST(freeze_window_hours AS DOUBLE) AS freeze_window_hours,
          CAST(minimum_revenue_uplift AS DOUBLE) AS minimum_revenue_uplift,
          CAST(minimum_confidence_score AS DOUBLE) AS minimum_confidence_score,
          CAST(maximum_recommendations_per_day AS INT) AS maximum_recommendations_per_day,
          to_timestamp(source_updated_ts) AS source_updated_ts
        FROM bronze_schedule_policy
        """
    )
    _materialize_view(spark, "fact_schedule_policy")


def _create_gold_views(spark: SparkSession, run_date: str) -> None:
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW gold_showtime_demand_features AS
        WITH latest_booking AS (
          SELECT *
          FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY showtime_id ORDER BY snapshot_ts DESC) AS rn
            FROM fact_booking_snapshot
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
          sc.screen_name,
          m.movie_title,
          s.show_start_ts,
          s.show_end_ts,
          s.is_locked,
          CASE WHEN dayofweek(s.show_start_ts) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
          CASE WHEN b.showtime_id IS NULL THEN 'NO_BOOKING_SNAPSHOT' ELSE 'OK' END
            AS forecast_input_quality_status,
          current_timestamp() AS feature_run_ts
        FROM fact_showtime_schedule_current s
        JOIN dim_screen sc
          ON s.screen_id = sc.screen_id
        JOIN dim_movie m
          ON s.movie_id = m.movie_id
        LEFT JOIN latest_booking b
          ON s.showtime_id = b.showtime_id
        WHERE s.schedule_status = 'PUBLISHED'
        """
    )
    _materialize_view(spark, "gold_showtime_demand_features")

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW gold_showtime_demand_forecast AS
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
          current_timestamp() AS forecast_run_ts
        FROM gold_showtime_demand_features
        """
    )
    _materialize_view(spark, "gold_showtime_demand_forecast")

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW gold_screen_allocation_candidates AS
        WITH enriched AS (
          SELECT
            f.*,
            fc.unconstrained_demand_forecast,
            fc.forecast_confidence_score,
            p.freeze_window_hours,
            p.minimum_revenue_uplift,
            p.minimum_confidence_score,
            p.maximum_recommendations_per_day
          FROM gold_showtime_demand_features f
          JOIN gold_showtime_demand_forecast fc
            ON f.showtime_id = fc.showtime_id
          JOIN fact_schedule_policy p
            ON f.theatre_id = p.theatre_id
        ),
        paired AS (
          SELECT
            sha2(concat_ws('|', a.showtime_id, b.showtime_id, '{run_date}'), 256)
              AS candidate_id,
            a.theatre_id,
            a.showtime_id AS showtime_a_id,
            b.showtime_id AS showtime_b_id,
            a.movie_title AS movie_a_title,
            b.movie_title AS movie_b_title,
            a.screen_name AS current_screen_a,
            b.screen_name AS current_screen_b,
            b.screen_name AS recommended_screen_a,
            a.screen_name AS recommended_screen_b,
            a.unconstrained_demand_forecast AS forecast_a,
            b.unconstrained_demand_forecast AS forecast_b,
            a.screen_capacity AS current_capacity_a,
            b.screen_capacity AS current_capacity_b,
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
            ROUND(
              LEAST(a.unconstrained_demand_forecast, a.screen_capacity) * a.average_ticket_price
              + LEAST(b.unconstrained_demand_forecast, b.screen_capacity) * b.average_ticket_price,
              2
            ) AS current_expected_ticket_revenue,
            ROUND(
              LEAST(a.unconstrained_demand_forecast, b.screen_capacity) * a.average_ticket_price
              + LEAST(b.unconstrained_demand_forecast, a.screen_capacity) * b.average_ticket_price,
              2
            ) AS proposed_expected_ticket_revenue,
            CASE
              WHEN a.movie_format = 'STANDARD'
                THEN b.screen_format IN ('STANDARD', 'PREMIUM')
              ELSE a.movie_format = b.screen_format
            END AS movie_a_target_compatible,
            CASE
              WHEN b.movie_format = 'STANDARD'
                THEN a.screen_format IN ('STANDARD', 'PREMIUM')
              ELSE b.movie_format = a.screen_format
            END AS movie_b_target_compatible
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
    _materialize_view(spark, "gold_screen_allocation_candidates")

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW gold_screen_allocation_recommendations AS
        WITH ranked AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY theatre_id, CAST(show_start_ts AS DATE)
              ORDER BY estimated_ticket_revenue_uplift DESC
            ) AS daily_rank
          FROM gold_screen_allocation_candidates
          WHERE constraint_validation_status = 'VALID'
            AND estimated_ticket_revenue_uplift >= minimum_revenue_uplift
            AND forecast_confidence_score >= minimum_confidence_score
        )
        SELECT
          sha2(concat_ws('|', candidate_id, 'recommendation'), 256) AS recommendation_id,
          concat('recommendation-', '{run_date}') AS recommendation_run_id,
          'PROPOSED' AS recommendation_status,
          theatre_id,
          CAST(show_start_ts AS DATE) AS show_date,
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
        FROM ranked
        WHERE daily_rank <= maximum_recommendations_per_day
        """
    )
    _materialize_view(spark, "gold_screen_allocation_recommendations")


def _validate_counts(spark: SparkSession, counts: dict[str, int]) -> None:
    if counts["gold_recommendations"] < 3:
        raise AssertionError(
            f"Expected at least 3 recommendations, found {counts['gold_recommendations']}"
        )
    invalid_candidates = spark.sql(
        """
        SELECT COUNT(*) AS invalid_count
        FROM gold_screen_allocation_candidates
        WHERE constraint_validation_status = 'INVALID'
        """
    ).collect()[0]["invalid_count"]
    if invalid_candidates < 1:
        raise AssertionError("Expected at least one invalid candidate")
    bad_recommendations = spark.sql(
        """
        SELECT COUNT(*) AS bad_count
        FROM gold_screen_allocation_recommendations
        WHERE estimated_ticket_revenue_uplift <= 0
           OR recommendation_reason IS NULL
           OR recommendation_reason = ''
        """
    ).collect()[0]["bad_count"]
    if bad_recommendations:
        raise AssertionError(f"Found {bad_recommendations} invalid recommendation rows")


def recommendation_reasons(spark: SparkSession) -> list[str]:
    rows = spark.sql(
        """
        SELECT recommendation_reason
        FROM gold_screen_allocation_recommendations
        ORDER BY theatre_id, show_date, showtime_a_id
        """
    ).collect()
    return [row["recommendation_reason"] for row in rows]


def spark_session(app_name: str = "theatre-screen-allocation-local-e2e") -> SparkSession:
    builder: Any = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return builder.getOrCreate()


def _materialize_view(spark: SparkSession, view_name: str) -> None:
    materialized = spark.table(view_name).localCheckpoint(eager=True)
    materialized.createOrReplaceTempView(view_name)

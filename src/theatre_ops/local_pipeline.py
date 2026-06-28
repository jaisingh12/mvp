"""Dependency-free local E2E pipeline for the Databricks MVP logic.

This mirrors the notebook stages closely enough to catch data and business-rule regressions
before running the Databricks-only Auto Loader path.
"""

from __future__ import annotations

import csv
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from theatre_ops.sample_data import SourceBatch, source_batches


def write_source_csvs(run_date: str, output_root: Path) -> dict[str, Path]:
    output_root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    for source_name, batch in source_batches(run_date).items():
        source_dir = output_root / source_name / f"{batch.partition_column}={run_date}"
        source_dir.mkdir(parents=True, exist_ok=True)
        csv_path = source_dir / f"{source_name}.csv"
        _write_csv(csv_path, batch)
        paths[source_name] = csv_path

    return paths


def run_local_pipeline(run_date: str, output_root: Path) -> dict[str, list[dict[str, Any]]]:
    source_paths = write_source_csvs(run_date, output_root)
    bronze = {source: _read_bronze_csv(source, path, run_date) for source, path in source_paths.items()}
    silver = build_silver(bronze)
    gold = build_gold(silver, run_date)
    return {"bronze": _flatten_stage(bronze), "silver": _flatten_stage(silver), "gold": gold}


def build_silver(bronze: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    return {
        "dim_theatre": [
            _project(
                row,
                [
                    "theatre_id",
                    "theatre_name",
                    "city",
                    "state",
                    "region",
                    "timezone",
                    "theatre_status",
                ],
            )
            for row in bronze["theatre_master"]
        ],
        "dim_screen": [
            {
                **_project(row, ["screen_id", "theatre_id", "screen_name", "screen_format"]),
                "capacity": int(row["capacity"]),
                "is_active": _as_bool(row["is_active"]),
            }
            for row in bronze["screen_master"]
            if int(row["capacity"]) > 0
        ],
        "dim_movie": [
            {
                **_project(
                    row,
                    ["movie_id", "movie_title", "genre", "language", "movie_format", "distributor"],
                ),
                "runtime_minutes": int(row["runtime_minutes"]),
                "average_ticket_price": float(row["average_ticket_price"]),
                "seeded_baseline_demand": float(row["seeded_baseline_demand"]),
            }
            for row in bronze["movie_master"]
        ],
        "fact_showtime_schedule_current": [
            {
                **_project(
                    row,
                    ["showtime_id", "theatre_id", "screen_id", "movie_id", "schedule_status"],
                ),
                "show_start_ts": _parse_ts(row["show_start_ts"]),
                "show_end_ts": _parse_ts(row["show_end_ts"]),
                "is_locked": _as_bool(row["is_locked"]),
            }
            for row in bronze["showtime_schedule"]
            if _parse_ts(row["show_end_ts"]) > _parse_ts(row["show_start_ts"])
        ],
        "fact_booking_snapshot": [
            {
                **_project(row, ["snapshot_id", "showtime_id"]),
                "snapshot_ts": _parse_ts(row["snapshot_ts"]),
                "booked_seats": int(row["booked_seats"]),
                "booking_count": int(row["booking_count"]),
                "gross_booking_revenue": float(row["gross_booking_revenue"]),
                "hours_to_show": float(row["hours_to_show"]),
            }
            for row in bronze["booking_snapshot"]
            if int(row["booked_seats"]) >= 0
        ],
        "fact_screen_availability": [
            {
                **_project(row, ["availability_id", "theatre_id", "screen_id", "blackout_reason"]),
                "available_from_ts": _parse_ts(row["available_from_ts"]),
                "available_to_ts": _parse_ts(row["available_to_ts"]),
                "is_available": _as_bool(row["is_available"]),
                "minimum_turnaround_minutes": int(row["minimum_turnaround_minutes"]),
            }
            for row in bronze["screen_availability"]
        ],
        "fact_schedule_policy": [
            {
                **_project(row, ["policy_id", "theatre_id"]),
                "freeze_window_hours": float(row["freeze_window_hours"]),
                "minimum_revenue_uplift": float(row["minimum_revenue_uplift"]),
                "minimum_confidence_score": float(row["minimum_confidence_score"]),
                "maximum_recommendations_per_day": int(row["maximum_recommendations_per_day"]),
            }
            for row in bronze["schedule_policy"]
        ],
    }


def build_gold(silver: dict[str, list[dict[str, Any]]], run_date: str) -> list[dict[str, Any]]:
    screens = _by_key(silver["dim_screen"], "screen_id")
    movies = _by_key(silver["dim_movie"], "movie_id")
    policies = _by_key(silver["fact_schedule_policy"], "theatre_id")
    bookings = _by_key(silver["fact_booking_snapshot"], "showtime_id")

    features: list[dict[str, Any]] = []
    for show in silver["fact_showtime_schedule_current"]:
        screen = screens[show["screen_id"]]
        movie = movies[show["movie_id"]]
        booking = bookings.get(show["showtime_id"])
        hours_to_show = booking["hours_to_show"] if booking else 999.0
        expected_completion = _expected_completion_pct(hours_to_show)
        booked_seats = booking["booked_seats"] if booking else 0
        features.append(
            {
                **show,
                "screen_capacity": screen["capacity"],
                "screen_name": screen["screen_name"],
                "screen_format": screen["screen_format"],
                "movie_title": movie["movie_title"],
                "movie_format": movie["movie_format"],
                "average_ticket_price": movie["average_ticket_price"],
                "current_booked_seats": booked_seats,
                "hours_to_show": hours_to_show,
                "expected_booking_completion_pct": expected_completion,
                "historical_avg_attendance_movie": movie["seeded_baseline_demand"],
            }
        )

    forecasts = [_forecast_feature(row, run_date) for row in features]
    forecast_by_showtime = _by_key(forecasts, "showtime_id")

    candidates: list[dict[str, Any]] = []
    for a in features:
        for b in features:
            if a["showtime_id"] == b["showtime_id"]:
                continue
            if a["theatre_id"] != b["theatre_id"]:
                continue
            if abs((a["show_start_ts"] - b["show_start_ts"]).total_seconds()) > 900:
                continue
            fa = forecast_by_showtime[a["showtime_id"]]
            fb = forecast_by_showtime[b["showtime_id"]]
            if fa["unconstrained_demand_forecast"] <= a["screen_capacity"]:
                continue
            if fb["unconstrained_demand_forecast"] >= b["screen_capacity"]:
                continue
            if b["screen_capacity"] <= a["screen_capacity"]:
                continue
            candidates.append(_candidate(a, b, fa, fb, policies[a["theatre_id"]], run_date))

    recommendations = [_recommendation(row, run_date) for row in candidates if _is_publishable(row)]
    return [
        {"table": "gold_showtime_demand_features", "rows": features},
        {"table": "gold_showtime_demand_forecast", "rows": forecasts},
        {"table": "gold_screen_allocation_candidates", "rows": candidates},
        {"table": "gold_screen_allocation_recommendations", "rows": recommendations},
    ]


def validate_local_outputs(gold: list[dict[str, Any]]) -> None:
    tables = {table["table"]: table["rows"] for table in gold}
    candidates = tables["gold_screen_allocation_candidates"]
    recommendations = tables["gold_screen_allocation_recommendations"]

    if len(recommendations) < 3:
        raise AssertionError(f"Expected at least 3 recommendations, found {len(recommendations)}")
    if not any(row["constraint_validation_status"] == "INVALID" for row in candidates):
        raise AssertionError("Expected at least one invalid candidate")
    for row in recommendations:
        if row["estimated_ticket_revenue_uplift"] <= 0:
            raise AssertionError(f"Recommendation {row['recommendation_id']} has non-positive uplift")
        if not row["recommendation_reason"]:
            raise AssertionError(f"Recommendation {row['recommendation_id']} has no reason")


def _candidate(
    a: dict[str, Any],
    b: dict[str, Any],
    fa: dict[str, Any],
    fb: dict[str, Any],
    policy: dict[str, Any],
    run_date: str,
) -> dict[str, Any]:
    current_revenue = round(
        min(fa["unconstrained_demand_forecast"], a["screen_capacity"]) * a["average_ticket_price"]
        + min(fb["unconstrained_demand_forecast"], b["screen_capacity"]) * b["average_ticket_price"],
        2,
    )
    proposed_revenue = round(
        min(fa["unconstrained_demand_forecast"], b["screen_capacity"]) * a["average_ticket_price"]
        + min(fb["unconstrained_demand_forecast"], a["screen_capacity"]) * b["average_ticket_price"],
        2,
    )
    status = "VALID"
    failure = ""
    if a["is_locked"] or b["is_locked"]:
        status, failure = "INVALID", "LOCKED_SHOWTIME"
    elif min(a["hours_to_show"], b["hours_to_show"]) <= policy["freeze_window_hours"]:
        status, failure = "INVALID", "FREEZE_WINDOW"
    elif not _format_compatible(a["movie_format"], b["screen_format"]) or not _format_compatible(
        b["movie_format"], a["screen_format"]
    ):
        status, failure = "INVALID", "FORMAT_INCOMPATIBLE"
    elif proposed_revenue <= current_revenue:
        status, failure = "INVALID", "NON_POSITIVE_UPLIFT"

    return {
        "candidate_id": _hash(a["showtime_id"], b["showtime_id"], run_date),
        "theatre_id": a["theatre_id"],
        "showtime_a_id": a["showtime_id"],
        "showtime_b_id": b["showtime_id"],
        "movie_a_title": a["movie_title"],
        "movie_b_title": b["movie_title"],
        "current_screen_a": a["screen_name"],
        "current_screen_b": b["screen_name"],
        "recommended_screen_a": b["screen_name"],
        "recommended_screen_b": a["screen_name"],
        "forecast_a": fa["unconstrained_demand_forecast"],
        "forecast_b": fb["unconstrained_demand_forecast"],
        "current_capacity_a": a["screen_capacity"],
        "current_capacity_b": b["screen_capacity"],
        "estimated_ticket_revenue_uplift": round(proposed_revenue - current_revenue, 2),
        "forecast_confidence_score": min(
            fa["forecast_confidence_score"], fb["forecast_confidence_score"]
        ),
        "minimum_revenue_uplift": policy["minimum_revenue_uplift"],
        "minimum_confidence_score": policy["minimum_confidence_score"],
        "show_start_ts": a["show_start_ts"],
        "constraint_validation_status": status,
        "constraint_failure_reason": failure,
    }


def _recommendation(candidate: dict[str, Any], run_date: str) -> dict[str, Any]:
    uplift = round(candidate["estimated_ticket_revenue_uplift"])
    return {
        "recommendation_id": _hash(candidate["candidate_id"], "recommendation"),
        "recommendation_run_id": f"recommendation-{run_date}",
        "recommendation_status": "PROPOSED",
        **candidate,
        "recommendation_reason": (
            f"Move {candidate['movie_a_title']} from {candidate['current_screen_a']} "
            f"to {candidate['recommended_screen_a']} because predicted demand is "
            f"{round(candidate['forecast_a'])} seats against current capacity of "
            f"{candidate['current_capacity_a']}, while {candidate['movie_b_title']} is "
            f"predicted at {round(candidate['forecast_b'])} seats against capacity of "
            f"{candidate['current_capacity_b']}. Estimated ticket revenue uplift: ${uplift}."
        ),
    }


def _forecast_feature(row: dict[str, Any], run_date: str) -> dict[str, Any]:
    booking_curve_forecast = round(
        row["current_booked_seats"] / row["expected_booking_completion_pct"], 2
    )
    demand = round(max(booking_curve_forecast, row["historical_avg_attendance_movie"]), 2)
    return {
        "showtime_id": row["showtime_id"],
        "forecast_run_id": f"forecast-{run_date}",
        "unconstrained_demand_forecast": demand,
        "forecast_confidence_score": 0.86 if row["hours_to_show"] <= 96 else 0.72,
        "booking_curve_forecast": booking_curve_forecast,
        "historical_demand_baseline": row["historical_avg_attendance_movie"],
    }


def _is_publishable(candidate: dict[str, Any]) -> bool:
    return (
        candidate["constraint_validation_status"] == "VALID"
        and candidate["estimated_ticket_revenue_uplift"] >= candidate["minimum_revenue_uplift"]
        and candidate["forecast_confidence_score"] >= candidate["minimum_confidence_score"]
    )


def _read_bronze_csv(source: str, path: Path, run_date: str) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        row["_ingest_ts"] = datetime.now(UTC).isoformat()
        row["_source_file"] = str(path)
        row["_batch_id"] = run_date
        row["_source_snapshot_date"] = run_date
        row["_rescued_data"] = ""
        row["_source_name"] = source
    return rows


def _write_csv(path: Path, batch: SourceBatch) -> None:
    fieldnames = list(batch.rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(batch.rows)


def _flatten_stage(stage: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [{"table": name, "rows": rows} for name, rows in stage.items()]


def _project(row: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    return {key: row[key] for key in keys}


def _by_key(rows: list[dict[str, Any]], key: str) -> dict[Any, dict[str, Any]]:
    return {row[key]: row for row in rows}


def _expected_completion_pct(hours_to_show: float) -> float:
    if hours_to_show <= 24:
        return 0.55
    if hours_to_show <= 72:
        return 0.42
    return 0.30


def _as_bool(value: Any) -> bool:
    return str(value).lower() == "true"


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _format_compatible(movie_format: str, screen_format: str) -> bool:
    if movie_format == "STANDARD":
        return screen_format in {"STANDARD", "PREMIUM"}
    return movie_format == screen_format


def _hash(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

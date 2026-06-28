"""Deterministic synthetic source data for the Databricks MVP."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any

RANDOM_SEED = 42
SOURCES = [
    "theatre_master",
    "screen_master",
    "movie_master",
    "showtime_schedule",
    "booking_snapshot",
    "screen_availability",
    "schedule_policy",
]


@dataclass(frozen=True)
class SourceBatch:
    source_name: str
    partition_column: str
    rows: list[dict[str, Any]]


def parse_run_date(run_date: str | date) -> date:
    if isinstance(run_date, date):
        return run_date
    return datetime.strptime(run_date, "%Y-%m-%d").date()


def source_batches(run_date: str | date) -> dict[str, SourceBatch]:
    """Return small, stable source tables with 3 valid swaps and 1 blocked candidate."""
    base_date = parse_run_date(run_date)
    updated_ts = datetime.combine(base_date, time(8, 0)).isoformat()

    theatres = [
        {
            "theatre_id": "T001",
            "theatre_name": "North Star Cinemas",
            "city": "Austin",
            "state": "TX",
            "region": "South",
            "timezone": "America/Chicago",
            "theatre_status": "ACTIVE",
            "source_updated_ts": updated_ts,
        },
        {
            "theatre_id": "T002",
            "theatre_name": "Lakeview Megaplex",
            "city": "Denver",
            "state": "CO",
            "region": "Mountain",
            "timezone": "America/Denver",
            "theatre_status": "ACTIVE",
            "source_updated_ts": updated_ts,
        },
    ]

    screens: list[dict[str, Any]] = []
    for theatre_id in ["T001", "T002"]:
        for idx, capacity, screen_format in [
            (1, 80, "STANDARD"),
            (2, 120, "STANDARD"),
            (3, 200, "STANDARD"),
            (4, 280, "PREMIUM"),
        ]:
            screens.append(
                {
                    "screen_id": f"{theatre_id}-S{idx}",
                    "theatre_id": theatre_id,
                    "screen_name": f"Screen {idx}",
                    "capacity": capacity,
                    "screen_format": screen_format,
                    "is_active": True,
                    "source_updated_ts": updated_ts,
                }
            )

    movies = [
        ("M001", "Dragon Galaxy", "Action", "STANDARD", 14.0, 225),
        ("M002", "Laugh Riot", "Comedy", "STANDARD", 12.0, 205),
        ("M003", "Midnight Orbit", "Sci-Fi", "STANDARD", 13.5, 210),
        ("M004", "Quiet Garden", "Drama", "STANDARD", 10.0, 70),
        ("M005", "Tiny Detectives", "Family", "STANDARD", 9.5, 75),
        ("M006", "Chef's Table Live", "Documentary", "STANDARD", 11.0, 60),
        ("M007", "Premium Heist", "Action", "PREMIUM", 16.0, 165),
        ("M008", "Festival Shorts", "Drama", "STANDARD", 8.0, 55),
    ]
    movie_rows = [
        {
            "movie_id": movie_id,
            "movie_title": title,
            "genre": genre,
            "language": "English",
            "runtime_minutes": 115,
            "movie_format": movie_format,
            "release_date": str(base_date - timedelta(days=14 + idx)),
            "distributor": "MVP Pictures",
            "average_ticket_price": price,
            "seeded_baseline_demand": baseline,
            "source_updated_ts": updated_ts,
        }
        for idx, (movie_id, title, genre, movie_format, price, baseline) in enumerate(movies)
    ]

    schedule_rows: list[dict[str, Any]] = []
    booking_rows: list[dict[str, Any]] = []

    def add_show(
        showtime_id: str,
        theatre_id: str,
        screen_id: str,
        movie_id: str,
        show_day: date,
        start_hour: int,
        start_minute: int,
        booked_seats: int,
        locked: bool = False,
    ) -> None:
        movie = next(row for row in movie_rows if row["movie_id"] == movie_id)
        start_dt = datetime.combine(show_day, time(start_hour, start_minute))
        end_dt = start_dt + timedelta(minutes=int(movie["runtime_minutes"]) + 20)
        schedule_rows.append(
            {
                "showtime_id": showtime_id,
                "theatre_id": theatre_id,
                "screen_id": screen_id,
                "movie_id": movie_id,
                "show_start_ts": start_dt.isoformat(),
                "show_end_ts": end_dt.isoformat(),
                "schedule_status": "PUBLISHED",
                "is_locked": locked,
                "published_ts": datetime.combine(base_date, time(9, 0)).isoformat(),
                "source_updated_ts": updated_ts,
            }
        )
        snapshot_ts = datetime.combine(base_date, time(10, 0)).isoformat()
        booking_rows.append(
            {
                "snapshot_id": f"BS-{showtime_id}",
                "showtime_id": showtime_id,
                "snapshot_ts": snapshot_ts,
                "booked_seats": booked_seats,
                "booking_count": max(1, booked_seats // 2),
                "gross_booking_revenue": round(booked_seats * float(movie["average_ticket_price"]), 2),
                "hours_to_show": 30 + ((show_day - base_date).days * 24),
                "source_updated_ts": updated_ts,
            }
        )

    opportunities = [
        ("T001", 1, "M001", "M004", False),
        ("T001", 2, "M002", "M005", False),
        ("T002", 1, "M003", "M006", False),
        ("T002", 3, "M001", "M008", True),
    ]
    for idx, (theatre_id, day_offset, high_movie, low_movie, locked) in enumerate(opportunities, 1):
        day = base_date + timedelta(days=day_offset)
        add_show(
            f"ST-{idx:02d}-A",
            theatre_id,
            f"{theatre_id}-S2",
            high_movie,
            day,
            19,
            0,
            booked_seats=94,
            locked=locked,
        )
        add_show(
            f"ST-{idx:02d}-B",
            theatre_id,
            f"{theatre_id}-S4",
            low_movie,
            day,
            19,
            5,
            booked_seats=28,
        )

    filler_idx = 20
    for theatre_id in ["T001", "T002"]:
        for day_offset in range(7):
            day = base_date + timedelta(days=day_offset)
            add_show(
                f"ST-{filler_idx:02d}",
                theatre_id,
                f"{theatre_id}-S1",
                "M007",
                day,
                16,
                0,
                booked_seats=45,
            )
            filler_idx += 1

    availability_rows = []
    for screen in screens:
        availability_rows.append(
            {
                "availability_id": f"AV-{screen['screen_id']}",
                "theatre_id": screen["theatre_id"],
                "screen_id": screen["screen_id"],
                "available_from_ts": datetime.combine(base_date, time(0, 0)).isoformat(),
                "available_to_ts": datetime.combine(base_date + timedelta(days=8), time(23, 59)).isoformat(),
                "is_available": True,
                "blackout_reason": "",
                "minimum_turnaround_minutes": 20,
                "source_updated_ts": updated_ts,
            }
        )

    policy_rows = [
        {
            "policy_id": f"POL-{theatre['theatre_id']}",
            "theatre_id": theatre["theatre_id"],
            "freeze_window_hours": 6,
            "minimum_revenue_uplift": 100,
            "minimum_confidence_score": 0.6,
            "maximum_recommendations_per_day": 5,
            "source_updated_ts": updated_ts,
        }
        for theatre in theatres
    ]

    return {
        "theatre_master": SourceBatch("theatre_master", "snapshot_date", theatres),
        "screen_master": SourceBatch("screen_master", "snapshot_date", screens),
        "movie_master": SourceBatch("movie_master", "snapshot_date", movie_rows),
        "showtime_schedule": SourceBatch("showtime_schedule", "snapshot_date", schedule_rows),
        "booking_snapshot": SourceBatch("booking_snapshot", "load_date", booking_rows),
        "screen_availability": SourceBatch("screen_availability", "snapshot_date", availability_rows),
        "schedule_policy": SourceBatch("schedule_policy", "snapshot_date", policy_rows),
    }

from theatre_ops.sample_data import source_batches


def test_sample_data_contains_required_sources_and_scenarios() -> None:
    batches = source_batches("2026-07-01")

    assert set(batches) == {
        "theatre_master",
        "screen_master",
        "movie_master",
        "showtime_schedule",
        "booking_snapshot",
        "screen_availability",
        "schedule_policy",
    }
    assert len(batches["theatre_master"].rows) == 2
    assert len(batches["screen_master"].rows) == 8

    schedule = batches["showtime_schedule"].rows
    locked = [row for row in schedule if row["is_locked"]]
    assert len(locked) == 1

    paired_prime_time = [
        row for row in schedule if row["showtime_id"].startswith(("ST-01", "ST-02", "ST-03"))
    ]
    assert len(paired_prime_time) == 6

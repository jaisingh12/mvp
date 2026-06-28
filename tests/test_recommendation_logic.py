from theatre_ops.recommendation_logic import is_format_compatible, screen_swap_uplift


def test_screen_swap_uplift_is_positive_when_high_demand_gets_large_screen() -> None:
    uplift = screen_swap_uplift(
        forecast_a=225,
        current_capacity_a=120,
        price_a=14,
        forecast_b=70,
        current_capacity_b=280,
        price_b=10,
    )

    assert uplift == 1470


def test_format_compatibility_rules_are_simple_and_explicit() -> None:
    assert is_format_compatible("STANDARD", "PREMIUM")
    assert is_format_compatible("PREMIUM", "PREMIUM")
    assert not is_format_compatible("PREMIUM", "STANDARD")

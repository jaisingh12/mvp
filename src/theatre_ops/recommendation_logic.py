"""Pure recommendation math shared by notebooks and local tests."""

from __future__ import annotations


def capped_revenue(forecast: float, capacity: int, average_ticket_price: float) -> float:
    return min(float(forecast), int(capacity)) * float(average_ticket_price)


def pair_revenue(
    forecast_a: float,
    capacity_a: int,
    price_a: float,
    forecast_b: float,
    capacity_b: int,
    price_b: float,
) -> float:
    return capped_revenue(forecast_a, capacity_a, price_a) + capped_revenue(
        forecast_b, capacity_b, price_b
    )


def screen_swap_uplift(
    forecast_a: float,
    current_capacity_a: int,
    price_a: float,
    forecast_b: float,
    current_capacity_b: int,
    price_b: float,
) -> float:
    current_revenue = pair_revenue(
        forecast_a, current_capacity_a, price_a, forecast_b, current_capacity_b, price_b
    )
    proposed_revenue = pair_revenue(
        forecast_a, current_capacity_b, price_a, forecast_b, current_capacity_a, price_b
    )
    return round(proposed_revenue - current_revenue, 2)


def is_format_compatible(movie_format: str, screen_format: str) -> bool:
    movie_format = movie_format.upper()
    screen_format = screen_format.upper()
    if movie_format == "STANDARD":
        return screen_format in {"STANDARD", "PREMIUM"}
    return movie_format == screen_format

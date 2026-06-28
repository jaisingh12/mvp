#!/usr/bin/env python
"""Run the MVP locally without Spark or Databricks dependencies."""

from __future__ import annotations

import argparse
from pathlib import Path

from theatre_ops.local_pipeline import run_local_pipeline, validate_local_outputs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default="2026-07-01")
    parser.add_argument("--output-root", default="/tmp/theatre_screen_allocation_mvp")
    args = parser.parse_args()

    result = run_local_pipeline(args.run_date, Path(args.output_root))
    gold_tables = {table["table"]: table["rows"] for table in result["gold"]}
    validate_local_outputs(result["gold"])

    print("Local E2E passed")
    print(f"CSV output root: {args.output_root}")
    print(f"Bronze tables: {len(result['bronze'])}")
    print(f"Silver tables: {len(result['silver'])}")
    print(f"Gold features: {len(gold_tables['gold_showtime_demand_features'])}")
    print(f"Gold forecasts: {len(gold_tables['gold_showtime_demand_forecast'])}")
    print(f"Gold candidates: {len(gold_tables['gold_screen_allocation_candidates'])}")
    print(f"Gold recommendations: {len(gold_tables['gold_screen_allocation_recommendations'])}")

    for row in gold_tables["gold_screen_allocation_recommendations"]:
        print(f"- {row['recommendation_reason']}")


if __name__ == "__main__":
    main()

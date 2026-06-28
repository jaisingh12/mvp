#!/usr/bin/env python
"""Run a local Spark E2E test for the MVP transforms."""

from __future__ import annotations

import argparse
from pathlib import Path

from theatre_ops.spark_pipeline import recommendation_reasons, run_spark_pipeline, spark_session


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default="2026-07-01")
    parser.add_argument("--output-root", default="/tmp/theatre_screen_allocation_spark_mvp")
    args = parser.parse_args()

    spark = spark_session()
    try:
        counts = run_spark_pipeline(spark, args.run_date, Path(args.output_root))
        print("Spark local E2E passed")
        print(f"CSV output root: {args.output_root}")
        for name, count in counts.items():
            print(f"{name}: {count}")
        for reason in recommendation_reasons(spark):
            print(f"- {reason}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

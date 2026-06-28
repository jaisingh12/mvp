from pathlib import Path

from theatre_ops.spark_pipeline import run_spark_pipeline, spark_session


def test_spark_pipeline_validates_mvp_contract(tmp_path: Path) -> None:
    spark = spark_session("theatre-screen-allocation-pytest")
    try:
        counts = run_spark_pipeline(spark, "2026-07-01", tmp_path)
    finally:
        spark.stop()

    assert counts["gold_candidates"] == 4
    assert counts["gold_recommendations"] == 3

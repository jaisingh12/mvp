from pathlib import Path

from theatre_ops.local_pipeline import run_local_pipeline, validate_local_outputs


def test_local_pipeline_validates_mvp_contract(tmp_path: Path) -> None:
    result = run_local_pipeline("2026-07-01", tmp_path)
    validate_local_outputs(result["gold"])

    gold_tables = {table["table"]: table["rows"] for table in result["gold"]}
    assert len(gold_tables["gold_screen_allocation_candidates"]) == 4
    assert len(gold_tables["gold_screen_allocation_recommendations"]) == 3

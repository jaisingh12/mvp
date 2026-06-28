# Theatre Screen Allocation MVP on Databricks

This repository contains a compact Databricks MVP for recommending advisory screen swaps
for a theatre chain. It generates deterministic synthetic source CSVs, ingests them into
Bronze Delta tables with Auto Loader, builds Silver current-state tables, and writes Gold
forecast, candidate, and recommendation tables.

The MVP is intentionally small so it can be pulled into Databricks Free Edition and tested
from the UI.

## What The Demo Proves

- A high-demand movie can be detected when it is scheduled in a small screen.
- A lower-demand movie in a larger comparable time slot can be identified.
- Hard constraints such as locked showtimes and format compatibility are checked.
- Recommendations are advisory only; the current schedule is never updated automatically.
- Each recommendation includes forecast demand, uplift, confidence, freshness, and a plain
  English reason.

## Databricks Free Edition Notes

Use serverless notebook compute. Free Edition is quota-limited and serverless-only, so this
repo avoids custom clusters, custom external storage locations, and long-running workflows.

If `CREATE CATALOG` is not allowed in your workspace, set the notebook `catalog` widget to an
existing catalog such as `workspace`. The notebooks create schemas and a managed Unity Catalog
volume under that catalog.

## Pull Into Databricks

1. Open Databricks Free Edition.
2. Go to **Workspace** or **Git folders**.
3. Add Git folder from:

   ```text
   https://github.com/jaisingh12/mvp.git
   ```

4. Pull the latest branch.
5. Open the notebooks in `notebooks/`.

## Run Order

Run these notebooks manually first:

1. `00_setup_and_seed.py`
2. `01_bronze_autoloader.py`
3. `02_silver_transform.py`
4. `03_gold_recommendations.py`
5. `99_demo.py`

Default widgets:

- `catalog`: `workspace`
- `run_date`: a `YYYY-MM-DD` date
- `full_refresh`: `true`

## Tables Created

Bronze:

- `bronze.bronze_theatre_master`
- `bronze.bronze_screen_master`
- `bronze.bronze_movie_master`
- `bronze.bronze_showtime_schedule`
- `bronze.bronze_booking_snapshot`
- `bronze.bronze_screen_availability`
- `bronze.bronze_schedule_policy`

Silver:

- `silver.dim_theatre`
- `silver.dim_screen`
- `silver.dim_movie`
- `silver.fact_showtime_schedule_current`
- `silver.fact_booking_snapshot`
- `silver.fact_screen_availability`
- `silver.fact_schedule_policy`

Gold:

- `gold.gold_showtime_demand_features`
- `gold.gold_showtime_demand_forecast`
- `gold.gold_screen_allocation_candidates`
- `gold.gold_screen_allocation_recommendations`

## Auto Loader Requirement

Bronze ingestion uses:

- `spark.readStream.format("cloudFiles")`
- `cloudFiles.format = csv`
- one schema location per source
- one checkpoint per source
- `trigger(availableNow=True)`

Bronze rows include `_ingest_ts`, `_source_file`, `_batch_id`, `_source_snapshot_date`, and
`_rescued_data`.

## Local Checks

The Databricks Auto Loader flow must be tested in Databricks. The pure Python pieces can be
checked locally:

```bash
python -m pytest
```

Run the dependency-free local E2E smoke test:

```bash
PYTHONPATH=src python scripts/run_local_e2e.py --run-date 2026-07-01
```

This writes CSVs locally, reads them back as Bronze-like rows with ingestion metadata, builds Silver/Gold in Python, and validates the MVP output contract. Auto Loader itself remains a Databricks runtime test.

## Phase 2

- Add SCD Type 2 dimensions for theatre and screen.
- Add full Databricks Asset Bundle deployment.
- Add attendance, feedback, KPI, and data-quality audit tables.
- Add a scheduled Databricks Workflow after manual UI validation succeeds.

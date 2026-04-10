# HIFLD Dagster pipelines (first 10 assets)

Dagster code location that defines **10 dataset assets** with download URLs **defined directly in the asset modules**. The pipeline does **not** read inventory, hints, a registry, or a separate specs layer at runtime.

## Run locally

From the repo root (`hifld-next-datasets`):

```bash
uv sync

dagster dev
# or: dg dev
```

- **Staging:** By default writes to `data/staging/`. Set `HIFLD_STAGING_BUCKET` to use GCS.
- **Versioning:** Each run writes to `{agency_slug}/{dataset_slug}/{run_id}/`.

## Layout

- `assets/` — One module per agency. Each file defines explicit `@asset` functions and the dataset URL constants those assets use.
- `checks/` — One module per agency. Each file defines explicit `@asset_check` functions and imports the dataset constants from the matching asset module.
- `download.py` — Shared download-to-staging helper.
- `validation.py` — Shared data quality checks (columns match original, geometries valid).

## Adding datasets

Add the dataset URL definition directly in the right `assets/<agency>.py`, then add one explicit asset in that file and one explicit check in `checks/<agency>.py`.
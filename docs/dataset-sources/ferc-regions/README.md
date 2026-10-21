# FERC Regions — unapproved historical preservation

Explicit pinned PASDA historical source: five CONUS regions, original IDs/DBF literals/native4326 geometry retained, including two invalid polygons. This is not a maintained agency refresh feed.
Pipeline: `assets/ferc_regions.py` and `sources/ferc_regions.py`. Integrity schedule remains STOPPED and requires explicit complete pinned local inputs.

Run: `uv run --frozen python -m unittest discover -s tests -p 'test_ferc_regions*.py'`.
Set `HIFLD_FERC_CAPTURE_DIR` for the registered source job; use isolated local staging/output and DAGSTER_HOME. Native GeoParquet only; incompatible derivatives stay disabled.

Full captures, baseline audits, logs and reports remain checksummed artifacts on issue85/PR460. Removed Git research/audit tools: `original-research-and-audit-tools-48a9d631.tar.gz`, SHA256 `2124cf82d8bba9de9e891fdab82b1dafddc9cbe73ecefc092b0ebb4cec995496`.

Source/rights, original invalid geometry, decimal/schema/ID/coverage/format compatibility, baseline, contract and release require independent decisions. Quality and promotion gates remain enforced; nothing is accepted or published by local draft runs.

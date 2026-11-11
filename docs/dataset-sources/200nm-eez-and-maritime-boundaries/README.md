# NOAA 200NM EEZ — unapproved draft

Source: https://nauticalcharts.noaa.gov/data/us-maritime-limits-and-boundaries.html (official service layer3).
Extraction, raw retention, schema/ID/coverage guards and proposed encodings are implemented in `src/dagster_hifld/noaa_extract.py`; the explicit asset and stopped weekly refresh are in `assets/noaa.py`.

Run focused checks: `uv run --frozen python -m unittest discover -s tests -p 'test_noaa*.py'`. Set `HIFLD_NOAA_EEZ_EVIDENCE_DIR` to verified external captures for full96-row replay. `scripts/verification/noaa_eez_local.py --help` runs a durable isolated graph.

Preserved captures: `issue-3-full-evidence-06a989e0ace3.tar.zst`, SHA256 `e7391f8d8d2a0836ad316a8efc291557435f957b1bbef8346a2b1e61577605ae`.
Removed research/reports/manifests/audit tools: `issue-3-removed-research-docs-dbc43c17f060.tar.gz`, SHA256 `f04807e97a341d9c2fb63353b4a3497289fa926b050831ed6ce400ef2d07d20d`.
Full execution evidence and exact reviews are recorded on issue3/PR420; artifact member inventories remain outside Git.

Source/release/baseline/ID/type/schema/geometry/coverage/rights acceptance remains required. Declared Integer preserves32bits; unresolved OID width, epoch-ms date encoding and string-width mappings remain unapproved. No promotion without the independent approval registry.

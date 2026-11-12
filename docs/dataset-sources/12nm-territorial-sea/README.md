# NOAA 12NM Territorial Sea — unapproved draft

Source: https://nauticalcharts.noaa.gov/data/us-maritime-limits-and-boundaries.html (official service layer 1).
Pipeline: `assets/noaa.py`, sharing extraction with PR420 and release gates with PR421. Weekly refresh remains STOPPED.

Run: `uv run --frozen python -m unittest discover -s tests -p 'test_noaa*.py'`.
For complete 87-row checks, set `HIFLD_NOAA_TERRITORIAL_CAPTURE_DIRECTORY` to retained raw bodies plus `receipts.json`, or `HIFLD_NOAA_TERRITORIAL_CAPTURE_EVIDENCE_DIR` to the original compressed evidence layout.

Full original captures, reports and research tools: `issue-2-removed-research-docs-6a6902c09907.tar.gz`, SHA256 `df52a0c074fc115d8fdd63735adbcff5655c9fa985e300ea87f3c57de0165b4f`.
Exact runs, full artifact inventories and acceptance decisions remain outside Git and on issue2/PR418.

Source/release continuity, baseline, IDs/types/schema/geometry/coverage/formats/rights require maintainer acceptance. OID width, epoch-ms dates, string width constraints and terminal geometry/bbox mapping remain unapproved. Unsupported Shapefile is disabled; independent release approval gates publication.

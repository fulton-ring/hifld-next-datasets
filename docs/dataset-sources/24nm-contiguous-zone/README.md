# NOAA 24NM Contiguous Zone — unapproved draft

Source: https://nauticalcharts.noaa.gov/data/us-maritime-limits-and-boundaries.html (official service layer2).
Pipeline: `assets/noaa_contiguous.py`, using the shared extraction code from PR420 and verification gates from PR421. Weekly refresh is stopped by default.

Run: `uv run --frozen python -m unittest discover -s tests -p 'test_noaa*.py'`.
For full63-row checks, set `HIFLD_NOAA_CONTIGUOUS_CAPTURE_EVIDENCE_DIR` to verified external captures, or `HIFLD_NOAA_CONTIGUOUS_LIVE_RECEIPT` to a complete retained live receipt. The separate `HIFLD_NOAA_CONTIGUOUS_EVIDENCE_DIR` optionally names a fresh isolated output directory.

Captures/reports: `issue-4-full-evidence-a63935e4d0fe.tar.zst`, SHA256 `d8604e1ceae48af75e9eebd9a85fa78ebae944f1142f494fa5f58aa8df2338aa`.
Removed research/manifests/audit tools: `issue-4-removed-research-docs-1581f3ff8cbb.tar.gz`, SHA256 `44b92c25af79dc119c0f6154265ce708921b80d94e948e3ef11de86d2fb94d18`.
Evidence and exact reviews remain on issue4/PR419; full artifact inventories stay outside Git.

Source continuity/vintage, baseline, raw/published IDs/types/schema/geometry/coverage/formats/rights require maintainer acceptance. Unsupported Shapefile is disabled; publication remains blocked by the independent approval gate.

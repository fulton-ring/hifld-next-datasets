# AIANNH Areas draft

Source: Census TIGER/Line 2026 AIANNH ZIP, https://www2.census.gov/geo/tiger/TIGER2026/AIANNH/tl_2026_us_aiannh.zip.
**Vintage: TIGER2026** — issue #777 migrated this draft from the superseded TIGER2023 pin (`tl_2023_us_aiannh.zip`, 9,086,993 bytes, SHA-256 `4155f58dfefcc40cfe61d741f760f882ebe6c6008537c0c7bc58075871af2f40`) to the latest maintained vintage. The pinned TIGER2026 archive is 9,175,653 bytes with SHA-256 `9bc1b69e2007450436e30b802c4e2db8e1e4d37266b9380816d1e32fec1e460c` (Last-Modified `2026-10-07T01:45:56Z` observed on 2026-11-15; the 2026 TIGER/Line Shapefiles were released October 14, 2026). It contains 867 AIANNH area features at EPSG:4269 (319 MultiPolygon, 548 Polygon; no invalid or empty geometry), five more than the 2023 vintage's 862.
The draft job is `aiannh_source_integrity_refresh`; its monthly schedule remains STOPPED.
Source, baseline, edition/grain, and output acceptance decisions remain open; no release is approved.
Tracked research and generated evidence are preserved in `/workspace/work/hifld-coordinator/artifacts/issue-8-removed-research-docs-19061d06de01.tar.gz` (SHA-256 `b606dbc4698b167dc207a7c453033414e8d3201e707dc5bb619eb21bc04c4320`).
Restore the archived `docs/dataset-sources/aiannh-areas-1/` tree from that tarball when needed.

The 10 synthetic/default source tests run with `python -m unittest tests.test_aiannh_source`.
The two complete-capture tests are opt-in and retain their original assertions; they replay a preserved exact TIGER2026 ZIP:
`AIANNH_TEST_ARCHIVE=/absolute/path/to/tl_2026_us_aiannh.zip python -m unittest tests.test_aiannh_source`.
Three standalone investigation scripts are archived at `/workspace/work/hifld-coordinator/artifacts/issue-8-unused-verification-tools-19061d06de01.tar.gz` (SHA-256 `c2d5c7df72dccbf8f01529ae768b8bcb61f7970e78ce5ef2e28af93fb6eb2ebc`).

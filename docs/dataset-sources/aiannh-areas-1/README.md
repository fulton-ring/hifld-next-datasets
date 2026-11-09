# AIANNH Areas draft

Source: Census TIGER/Line 2023 AIANNH ZIP, https://www2.census.gov/geo/tiger/TIGER2023/AIANNH/tl_2023_us_aiannh.zip.
The draft job is `aiannh_source_integrity_refresh`; its monthly schedule remains STOPPED.
Source, baseline, edition/grain, and output acceptance decisions remain open; no release is approved.
Tracked research and generated evidence are preserved in `/workspace/work/hifld-coordinator/artifacts/issue-8-removed-research-docs-19061d06de01.tar.gz` (SHA-256 `b606dbc4698b167dc207a7c453033414e8d3201e707dc5bb619eb21bc04c4320`).
Restore the archived `docs/dataset-sources/aiannh-areas-1/` tree from that tarball when needed.

The 10 synthetic/default source tests run with `python -m unittest tests.test_aiannh_source`.
The two complete-capture tests are opt-in and retain their original assertions:
`AIANNH_TEST_ARCHIVE=/workspace/work/hifld-worker-runtime/8/zip2023.body python -m unittest tests.test_aiannh_source`.
Three standalone investigation scripts are archived at `/workspace/work/hifld-coordinator/artifacts/issue-8-unused-verification-tools-19061d06de01.tar.gz` (SHA-256 `c2d5c7df72dccbf8f01529ae768b8bcb61f7970e78ce5ef2e28af93fb6eb2ebc`).

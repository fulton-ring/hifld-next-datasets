# Colleges and Universities

Complete NCES EDGE 2025–2026 source: https://nces.ed.gov/programs/edge/Geographic/SchoolLocations . The adapter preserves all 6,438 UNITIDs, original file FIDs, 21 DBF attributes and native EPSG:4326 Points. It is an **unapproved changed successor** to the enriched DOE/ORNL product.

With fresh isolated `DAGSTER_HOME`, `HIFLD_STAGING_DIR` and `HIFLD_DATASETS_DIR`, and cloud bucket variables unset, run `uv run dagster job execute -m dagster_hifld.definitions -a defs -j colleges_source_integrity`. Optionally set `HIFLD_COLLEGES_CAPTURE_DIR` to the complete restored capture. The monthly schedule defaults STOPPED. Focused tests: `uv run python -m unittest discover -s tests -p 'test_colleges*.py'`; set `COLLEGES_TEST_CAPTURE_DIR` for complete-real cases.

Checksummed artifacts under the coordinator's `/workspace/work/hifld-coordinator/38/cleanup/` preserve full inputs, baselines, reports, replay scripts, native outputs and construction contexts:

- `original-evidence-929eceb.tar.gz`: SHA256 `0308ca2c2b6dabeb6aa279970573403e0935cdcb104f0162d02a172515b20fab`.
- `colleges-audit-native-artifact.tar.gz`: SHA256 `314214d79fa0bce37f04bf4d518e907b58e4460985ef5acb6cf71c1d0eada713`.
- `review-repair-artifact-6efc19d.tar.gz`: SHA256 `87769aa56703bc0e4b2acfee0112c8a7978ea8f6bbe52cb2d9ceb70f2beb9c7e` (current lifecycle/error tests and registered replay).

Source/successor scope, crosswalk, rights, contract, baseline and all ID/schema/type/coverage/year/CRS/geometry/format changes still require acceptance. Quality and independent release approval block promotion. Current results and decisions belong to issue #38 / PR #444; no source or release approval is granted here.

# Ferry Routes draft (#88)

BTS NTAD delegates the Ferry Routes item `bc0f7dffb12d4ce9a47bf890d6c09804` to the official NTAD_Ferry_Routes layer 0. The adapter preserves all 964 original OBJECTIDs, 55 attributes and 714,082 native EPSG:4326 coordinate pairs. Actual rows say census 2020/data 2019 while item text claims 2022; that producer conflict remains unresolved.

Run `ferry_routes_source_integrity` with isolated local `DAGSTER_HOME`, `HIFLD_STAGING_DIR`, `HIFLD_DATASETS_DIR` and `HIFLD_FERRY_ROUTES_CAPTURE_DIR` pointing to the complete 19-response capture. Default tests: `python -m unittest discover -s tests -p 'test_ferry_routes*.py'`. Set `FERRY_ROUTES_TEST_CAPTURE_DIR` for the complete-input opt-in tests. The monthly schedule stays STOPPED; quality and independent release approval block promotion.

Complete captures, historical baselines, original IDs, reports and replay scripts are preserved in `/workspace/work/hifld-coordinator/88/compact-cleanup/original-tracked-2c5e2b6.tar.gz` (SHA256 `240e1722f22af6b87adc82f5dfca283686a7aa16658f6645c106a4100f6dcf78`). Its `MANIFEST.json` pins every original Git blob, Git mode, physical permission and member SHA. The original complete capture remains `/workspace/work/hifld-coordinator/88/capture/`; generated verification artifacts remain outside Git under that coordinator directory.

Source/crosswalk, vintage, rights, contract, baseline and ID/schema/type/nullability/coverage/geometry/format compatibility still require acceptance. Local pipeline tests grant none of these approvals. Current results and reviews belong to issue #88 / PR #447.

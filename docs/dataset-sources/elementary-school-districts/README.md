# Elementary School Districts

The draft adapter reconstructs all 25 official Census TIGER/Line 2026 elementary-district publications: 2,030 original records, 15 DBF attributes, file-local FIDs and native EPSG:4269 polygons. Complete ZIP/member/GEOID/FID lineage stays in a separate metadata sidecar. Source/edition, rights, baseline, ID/schema/type/coverage/CRS/geometry/format and release acceptance remain pending; the monthly schedule stays STOPPED.

Use fresh isolated `DAGSTER_HOME`, `HIFLD_STAGING_DIR` and `HIFLD_DATASETS_DIR`, with cloud bucket variables unset. Set `HIFLD_ELSD_CAPTURE_DIR` to the complete restored capture, then run `uv run dagster job execute -m dagster_hifld.definitions -a defs -j elsd_source_integrity`. Source success does not authorize promotion: normal catalog quality and independent exact release decisions still apply.

Run focused defaults with `uv run python -m unittest discover -s tests -p 'test_elementary_districts*.py'`. Set `ELSD_TEST_CAPTURE_DIR` to the complete external capture to include every full-population/native/state regression. Tiny synthetic defaults exercise transport, DBF/schema/native-format/lineage and refusal-journal behavior; they do not substitute for the complete audit.

Full original captures/reports/logs/replay scripts and pre-cleanup documents are losslessly retained in `/workspace/work/hifld-coordinator/64/cleanup/original-tracked-evidence-0d0575a.tar.gz`, SHA256 `c6055497685839c63ded3b561a1fd8d55adbc8fc1c9acdc901bd2d37be6d5050`. Its manifest binds all 140 original Git blobs, physical permissions and SHA256 values. Other complete captures/baselines remain under the coordinator's `/workspace/work/hifld-coordinator/64/`; current run results, artifact locations and decisions belong to issue #64 / PR #455.

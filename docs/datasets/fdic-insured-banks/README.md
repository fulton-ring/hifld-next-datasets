# FDIC locations — draft reconstruction (#82)

Source: [official FDIC locations API](https://api.fdic.gov/banks/locations), pinned index `locations_20261016090006`. The complete 78,028-record branch/headquarters population is checked against before/after ID censuses. Raw responses, native IDs, JSON primitive types, absent keys, explicit nulls, leading-zero ZIP strings and historical releases remain intact. This is observed source coherence, not a transactional snapshot.

The output is lossless-property GeoJSON with a **proposed**, unapproved `[LONGITUDE,LATITUDE]` interpretation; the producer does not declare a CRS. All 803 integer zero pairs remain. Two out-of-range latitudes retain literal properties with null proposed geometries. The full-population quality check blocks all 805 rows. Homogeneous derivatives remain unsupported until an accepted lossless variant/presence contract exists. Source/baseline selection, IDs/schema/date/CRS/rights and release decisions remain pending.

The explicit `fdic_locations_draft_reconstruction` Dagster job proves preservation only and excludes the registered blocking quality check. Configure its ingest op's `capture_directory` to the matching preserved artifact directory, with isolated `DAGSTER_HOME`, `HIFLD_STAGING_DIR` and `HIFLD_DATASETS_DIR`. The weekly schedule is STOPPED; changed source/index/schema/coverage is quarantined. No production write or release approval is implied.

The historical candidate references `fdic-82-original-captures.tar.gz` (64,717,414 bytes; SHA-256 `05b9fbf0650f403bcc95feb8b3e853780d05750277106d672a36c2c23c7f48f1`, 74 members). That archive was not present in the clean-main workspace and its historical pin is retained unchanged. A new bounded, complete 2026-10-21 capture was taken against all 37 URLs in the existing pin set; every body matched its pinned status, URL, byte count and SHA-256. The archive is `fdic-82-capture-v20261021T214500Z.tar.gz` (65,777,696 bytes; SHA-256 `a93045ddaf0c9928bc01558ae679d638d2c5e9acb44aa50428734d7fcc9ada00`) with 76 members and a complete 37-object manifest. It and the raw replay evidence are kept outside Git under `/workspace/82-evidence/`.

The fresh registered source run at code head `7a20b311ccc5fdcfa1ef0f27ea3a8dbd0d9cc2b2` is `998d3e5a-94ae-459e-964f-7908779b1235`; the catalog run is `d6c69f0e-cec2-4934-9ca6-7aea55689faf` (catalog materialized, blocking quality check correctly failed), and post-catalog cache replay is `29a55d48-2a0f-438c-8e52-03c014bed4c9` (`reused=True`). The isolated false-quality and valid-empty-approval refusals are `be8f6754-c1aa-44c7-baa9-f41b27204a96` and `6c1fa128-f4a4-45fa-b845-a151cfb7aa1e`; both kept the three-object synthetic published fixture and input candidate unchanged. Full run reports and exact byte receipts remain in the external evidence directory.

```sh
HIFLD_FDIC_LOCATIONS_TEST_CAPTURE_DIR=/operator/preserved-captures \
  PYTHONPATH=src python -m unittest discover -s tests -p 'test_fdic_locations*.py'
```

The six-record fixture tests structure only; it does not establish national completeness or approval.

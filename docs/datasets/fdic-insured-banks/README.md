# FDIC locations — draft reconstruction (#82)

Source: [official FDIC locations API](https://api.fdic.gov/banks/locations), pinned index `locations_20261016090006`. The complete 78,028-record branch/headquarters population is checked against before/after ID censuses. Raw responses, native IDs, JSON primitive types, absent keys, explicit nulls, leading-zero ZIP strings and historical releases remain intact. This is observed source coherence, not a transactional snapshot.

The output is lossless-property GeoJSON with a **proposed**, unapproved `[LONGITUDE,LATITUDE]` interpretation; the producer does not declare a CRS. All 803 integer zero pairs remain. Two out-of-range latitudes retain literal properties with null proposed geometries. The full-population quality check blocks all 805 rows. Homogeneous derivatives remain unsupported until an accepted lossless variant/presence contract exists. Source/baseline selection, IDs/schema/date/CRS/rights and release decisions remain pending.

The explicit `fdic_locations_draft_reconstruction` Dagster job proves preservation only and excludes the registered blocking quality check. Configure its ingest op's `capture_directory` to the matching preserved artifact directory, with isolated `DAGSTER_HOME`, `HIFLD_STAGING_DIR` and `HIFLD_DATASETS_DIR`. The weekly schedule is STOPPED; changed source/index/schema/coverage is quarantined. No production write or release approval is implied.

Required complete input artifact: `fdic-82-original-captures.tar.gz`, 64,717,414 bytes, SHA-256 `05b9fbf0650f403bcc95feb8b3e853780d05750277106d672a36c2c23c7f48f1`. Its 74 members have been fully hash-replayed. Original bodies/receipts, full comparisons and actual-run replay evidence remain checksummed artifacts, outside Git; historical replay requires these originals, not live refetch.

```sh
HIFLD_FDIC_LOCATIONS_TEST_CAPTURE_DIR=/operator/preserved-captures \
  PYTHONPATH=src python -m unittest discover -s tests -p 'test_fdic_locations*.py'
```

The six-record fixture tests structure only; it does not establish national completeness or approval.

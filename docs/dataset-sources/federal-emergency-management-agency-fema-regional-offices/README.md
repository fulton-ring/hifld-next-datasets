# FEMA Regional Offices local draft

The adapter captures the producer-hosted FEMA `RegHQs` FeatureServer layer 0 office points. It retains the complete 14-office roster (10 regional, 3 area, 1 national), original fields and IDs, native EPSG:4326 point coordinates, strings, nulls and source responses. Layer 2 regional polygons are a different product. No geocoding or reduction to ten records is performed.

The refresh job is `fema_regional_offices_draft_refresh`; its weekly schedule remains **STOPPED**. Every candidate is unapproved. FEMA metadata creation time `20201016` does not establish record release or currency. Published IDs, field names/types, string whitespace, baseline compatibility, rights, contract and release acceptance still require human decisions.

The complete capture fixture is preserved outside Git at `/workspace/work/83-evidence/source/fema-regional-offices-complete.json.gz` (58,546 bytes; SHA256 `e8f4c330cbd3c666d77721425770066c837e0283c92ddc340a89c19950aed525`). Test commands:

```bash
env -u HIFLD_FEMA_OFFICES_TEST_CAPTURE_ARCHIVE PYTHONPATH=src:tests:. python -m unittest tests.test_fema_regional_offices tests.test_fema_regional_offices_registration
HIFLD_FEMA_OFFICES_TEST_CAPTURE_ARCHIVE=/workspace/work/83-evidence/source/fema-regional-offices-complete.json.gz PYTHONPATH=src:tests:. python -m unittest tests.test_fema_regional_offices tests.test_fema_regional_offices_registration
```

Complete-source lifecycle and gate evidence stays in `/workspace/work/83-evidence`; it is not a source, baseline, rights, compatibility, contract or release approval.

# Dialysis Centers working draft

Healthcare Ready's [Rx Open page](https://healthcareready.org/rxopen/) explicitly delegates its HealthcareReady-owned dashboard → webmap → `RxOpen_Master_Inactive/FeatureServer/0`. The complete `Facility_Type = 'Dialysis Center'` category has 7,717 records. All15 source fields, original `ObjectId`/`UniqueID`, int32 ZIP/FIPS, float64 Phone, literal strings/nulls and native3857 Points remain unchanged. Monthly refresh is STOPPED.

The producer says inactive: `Open` is not verified operating status. All15 literal Point(0,0) records remain; a blocking location check prevents normal promotion. No geocoding, filtering, replacement IDs or partial national substitute. Rights/release day unspecified. Historical7,772-row candidates have different fields, repeated IDs and no UniqueID; source/continuation/contract/baseline/schema/ID/type/nullability/coverage/geometry/rights decisions remain external. No release accepted.

Run managed Python with `PYTHONPATH=src`: `python -m unittest discover -s tests -p test_dialysis_centers.py`. Full real-source test opt-in: `DIALYSIS_TEST_CAPTURE_DIR=/path/to/captures`. Materialize `dialysis_source_integrity` with isolated `DAGSTER_HOME`, `HIFLD_STAGING_DIR`, `HIFLD_DATASETS_DIR`; optional `HIFLD_DIALYSIS_CAPTURE_DIR` replays complete dated captures. Register the resulting publish partition before cataloging. Shared Git/CAS provenance and independent release approval remain mandatory.

Full source/baseline inputs and replay inventory are external: `/workspace/work/hifld-coordinator/55/dialysis-inputs.tar.gz`, SHA256 `af2cabc127cd329cd186f63b8d2ef5822cdd7b3bb78615bd5252e09befd7a6e9`. Issue55/PR carry execution/review decisions; Git contains code, focused synthetic tests and this README.

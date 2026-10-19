# FEMA Regions — draft pipeline (#84)

Source: [official FEMA RegHQs region polygons](https://gis.fema.gov/arcgis/rest/services/FEMA/RegHQs/FeatureServer/2). This explicit Dagster adapter preserves all 10 regions, their source IDs/physical attributes and 2,640,166 ordered vertices in native EPSG:4326 GeoPackage/Parquet. Complete before/after IDs, counts, fields and edit metadata establish observed coherence, not a transactional snapshot or an exact release date.

Run `fema_regions_draft_refresh` with its ingest op’s `capture_directory` set to the matching preserved complete inputs, and isolated `DAGSTER_HOME`, `HIFLD_STAGING_DIR` and `HIFLD_DATASETS_DIR`. The monthly schedule is STOPPED. Source drift is quarantined. Full-population quality is registered; release additionally requires independent baseline/contract/ID/schema/rights approval and the promotion registry. Historical format-specific names, integer widths, IDs and geometry wrapping/precision differences remain unapproved; no source or release acceptance is granted here.

Complete captures and replay evidence remain external artifacts, pinned by runtime source configuration. Removed Git research/receipts/reports and replay scripts are fully preserved in `fema84-removed-git-evidence-a1cf.tar.gz` (69,464 bytes; SHA-256 `faf7f390c956c5d9fd6f5b6b714f01b492fd072c58e8c5f7b472ff47c6a66469`), with all 82 original file hashes replayed. Matching original source/baseline captures are required for historical reconstruction; live refetch is not a substitute.

```sh
HIFLD_FEMA_REGIONS_TEST_CAPTURE_DIR=/operator/preserved-captures \
  PYTHONPATH=src python -m unittest discover -s tests -p 'test_fema_regions*.py'
```

Bounded fixtures establish structural behavior only. Prior complete local materializations, failure gates and independent review remain applicable to the unchanged source code; this documentation trim grants no production publication.

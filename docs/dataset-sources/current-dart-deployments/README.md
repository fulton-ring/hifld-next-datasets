# Current DART Deployments draft

- Source: NOAA NCEI hazards map delegates to ArcGIS item `764d3a99a4f8440ea05522815b5f5e19`, layer 0; dataset page: https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/tsunamis/dart-ocean-bottom-pressure
- Local job: `noaa_dart_draft_refresh`; weekly schedule `noaa_dart_draft_refresh_schedule` remains STOPPED.
- Captured layer has 62 global sites, including planned and international sites; it is not 39 currently operational US-owned buoys. Portal timestamps are not a declared release date.
- Source authority, historical baseline, identifier/schema/physical-type mappings, output formats, and release decisions remain unresolved; no production write or acceptance is implied.
- The exact twelve original response bodies and their receipts remain in the historical source commit `a253db50361cb1c23b415d16b3d3b0a3d587a6a1` under `docs/dataset-sources/current-dart-deployments/evidence/captures`.
- Restore those files outside the repository with `git archive a253db50361cb1c23b415d16b3d3b0a3d587a6a1 docs/dataset-sources/current-dart-deployments/evidence/captures | tar -x -C TARGET`.
- The 16 full-source tests require `HIFLD_CURRENT_DART_TEST_CAPTURE_DIR` pointing to the restored `captures` directory; two capture-independent mechanics tests run without it.
- Two capture-independent mechanics tests remain in the default run; the full-source test class skips until the complete capture directory is explicitly supplied.

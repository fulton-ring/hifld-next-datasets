# Current DART Deployments draft

- Source: NOAA NCEI hazards map delegates to ArcGIS item `764d3a99a4f8440ea05522815b5f5e19`, layer 0; dataset page: https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/tsunamis/dart-ocean-bottom-pressure
- Local job: `noaa_dart_draft_refresh`; weekly schedule `noaa_dart_draft_refresh_schedule` remains STOPPED.
- Captured layer has 62 global sites, including planned and international sites; it is not 39 currently operational US-owned buoys. Portal timestamps are not a declared release date.
- Source authority, historical baseline, identifier/schema/physical-type mappings, output formats, and release decisions remain unresolved; no production write or acceptance is implied.
- Archived tracked research, captures, reports, and candidate outputs: `/workspace/work/hifld-coordinator/artifacts/issue-49-removed-research-docs-a253db50361c.tar.gz` (SHA256 `7c222aead62007be134c2b9d861cbd78706bee236ed448d319bdf571b7eabf8b`).
- Restore the archive outside Git with `tar -xzf issue-49-removed-research-docs-a253db50361c.tar.gz -C TARGET`.
- The exact 24 original capture inputs are separately retained at `/workspace/work/hifld-coordinator/49/artifact-cleanup-49/original-complete-captures`.
- The 16 full-source tests require `HIFLD_CURRENT_DART_TEST_CAPTURE_DIR`; example: `HIFLD_CURRENT_DART_TEST_CAPTURE_DIR=/workspace/work/hifld-coordinator/49/artifact-cleanup-49/original-complete-captures PYTHONPATH=src:tests:. python -m unittest tests.test_current_dart`.
- Two capture-independent mechanics tests remain in the default run; the full-source test class skips until the complete capture directory is explicitly supplied.

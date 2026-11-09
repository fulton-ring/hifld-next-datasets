# American Red Cross Regions — unaccepted draft

- Candidate publisher account: `NHQRedCrossGIS`; service https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Master_ARC_Geography_2022/FeatureServer/2 . Independent source/crosswalk/rights acceptance remains pending.
- Both FY26 January2026 banner and FY27 July2026 title/prose are retained; effective edition/release date is unknown.
- Code: `sources/arc_regions.py`, `assets/arc_regions.py`. Complete47-region extraction preserves all17attributes/OBJECTIDs and native3857rings/XY; coverage/schema/geometry drift quarantines.
- Native GeoPackage/GeoParquet are drafts. OID producer bit width is unspecified; proposedint64 encoding, output nullability/text widths and historical48→47compatibility require acceptance. Separate generated `__gpkg_fid` is storage only.
- Producer polygons19(NewJersey) and42(SouthFlorida) are invalid and retained without repair. Default source and normal catalog quality gates block release; missing independent approval also blocks promotion.
- Job `arc_regions_draft_refresh`; monthly09:00 America/New_York schedule remains STOPPED. Config `capture_directory` replays complete retained inputs; empty config performs bounded live extraction.
- Tests: `python -m unittest discover -s tests -p 'test_arc_regions*.py'`; real inputs, five complete generation-pinned baselines, audits/logs and run evidence stay outside Git under `/workspace/work/hifld-coordinator/16/`.
- GPKG `last_change=2000-01-01T00:00:00.000Z` is deterministic local export metadata, never a producer release/capture date. All source/baseline/contracts/ID/schema/geometry/coverage/rights/release decisions remain unapproved.
- Processing provenance binds the installed pipeline with its explicit Git-blob or packaged-source SHA-256 scheme, so production containers without `.git` do not claim an invented Git revision.

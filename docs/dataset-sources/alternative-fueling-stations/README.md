# Alternative Fueling Stations

- Source: https://geodata.bts.gov/datasets/usdot::alternative-fueling-stations/about
- Service: https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/Alternative_Fueling_Stations/FeatureServer/0
- Local draft job: `fuel_stations_draft_refresh_job`; schedule is stopped, promotion is disabled.
- Tests build synthetic receipts and rows in `tests/test_fuel_stations.py`; no evidence files are runtime fixtures.
- Blockers: source/lineage and DHS crosswalk decision; baseline and ID/schema migration; 21,474 added and 13,010 removed IDs; station 390105 location anomaly; refresh policy.
- Full historical research, captures, audit scripts, and validation evidence: `/workspace/work/hifld-coordinator/artifacts/issue-11-removed-research-docs-fbab57205d95.tar.gz`.
- Restore inventory and member SHA256 pins: `/workspace/work/hifld-coordinator/artifacts/issue-11-removed-research-docs-fbab57205d95.tar.manifest.json`.

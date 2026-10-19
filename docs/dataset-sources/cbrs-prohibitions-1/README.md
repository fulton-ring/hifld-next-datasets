# CBRS Prohibitions — unapproved working draft

Source: FWS 2024-12-19 publication, https://www.fws.gov/program/coastal-barrier-resources-act/maps-and-data .
The explicit asset retains all 1,693 native records, original shapefile FIDs, NAD83 coordinates, DATE/date32 values and raw inputs. Monthly refresh remains STOPPED.
Run focused tests: `uv run --frozen python -m unittest discover -s tests -p 'test_cbrs_prohibitions_source.py'`.
Set `HIFLD_CBRS_PROHIBITIONS_CAPTURE_DIR` to complete original bodies/receipts for full real-input tests; default tests use clearly synthetic structural examples.
Research/captures/logs/replay tools are preserved in local artifact `issue-30-reviewed-91889f5-research-artifact.tar.gz`.
Artifact SHA256: `0f9293f03671e1b1a2edde55f359f672218a9b57f6e7fcb6b9fcd10f2c2a81ad` (157 members including complete path/mode/size/SHA inventory).
Verify the archive hash and each `ARTIFACT-MANIFEST.json` member before replay; retain the original captured ZIP and receipts.
Native prebuilt GeoParquet is preserved; generic regeneration, Shapefile and PMTiles mappings remain blocked.
Source edition/rights, baseline, original/published IDs, Date/float/schema/geometry compatibility and release require external decisions.
H00P/1497 and NY-31A/2022 geometry differences remain unapproved; inaccessible crosswalk is unverified.
No merge, production publication, accepted baseline/contract or activated schedule is implied.

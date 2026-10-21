# CBRS Units — unapproved working draft

Source: FWS 2024-12-19 full boundaries publication, https://www.fws.gov/media/digital-coastal-barrier-resources-system-boundaries .
Reconstructs all 942 native Units records; preserves eight physical DBF fields, original FIDs, leading text and NAD83 geometry.
Source, rights, baseline, FID/OBJECTID, schema/type/geometry/coverage and release decisions remain external; no in/out determination.
Registered job `fws_cbrs_units_draft_refresh`; monthly `fws_cbrs_units_draft_refresh_schedule` schedule stays STOPPED.
Set the ingest op `capture_dir` to the complete original four-capture directory; local staging/Dagster storage only.
Default focused checks: `PYTHONPATH=.:src:tests python -m unittest test_cbrs_units_source`.
Complete real checks additionally require `HIFLD_CBRS_UNITS_CAPTURE_DIR=/path/to/preserved/captures`.
All captures/research/reports/replay tools remain private artifacts, outside Git; issue #31 / PR #440 record exact results and decisions.
Original artifact: `coordinator31:artifact-cleanup/issue-31-original-evidence-4f7e359.tar.zst`.
SHA256 `71869b9f70f41ec29f132f02aa0d3efc49ca8787924b6ee653f41dfd26b322fc`; full member/Git-mode/permission inventory retained alongside it.

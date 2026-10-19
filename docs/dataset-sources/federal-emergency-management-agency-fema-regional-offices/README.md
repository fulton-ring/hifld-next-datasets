# FEMA Regional Offices local draft

- Source: FEMA ArcGIS `FEMA/RegHQs`, layer 0 office points; layer 2 regional boundaries are separate.
- Job: `fema_regional_offices_draft_refresh`; weekly schedule remains **STOPPED**.
- Decision state: FEMA producer service identified; record currency/release date, DataLumos crosswalk (403/unresolved), baseline, field/type/ID compatibility, rights, contract and release acceptance remain unapproved.
- Restore outside the repository: `RESEARCH_RESTORE_DIR=/tmp/fema83-research-restore; mkdir -p "$RESEARCH_RESTORE_DIR"; tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-83-removed-research-docs-d5babdee99d2.tar.gz -C "$RESEARCH_RESTORE_DIR"`.
- Archived tracked research/evidence: `/workspace/work/hifld-coordinator/artifacts/issue-83-removed-research-docs-d5babdee99d2.tar.gz` (100850 bytes; SHA256 `e665b6bc4a00220c870209e82447e73fd4c45039d896b723c7b29bd12356ba42`). Extract outside the repository to restore original files.
- Archived standalone verification scripts: `/workspace/work/hifld-coordinator/artifacts/issue-83-unused-fema-verification-tools-d5babdee99d2.tar.gz` (5521 bytes; SHA256 `6fb189c0f58fee2dd8bca94dc6a4e5c88c304a9f37f8f4598b20add0b7001272`). Restore outside the repository with the research archive using `tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-83-removed-research-docs-d5babdee99d2.tar.gz -C "$RESEARCH_RESTORE_DIR"` and `tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-83-unused-fema-verification-tools-d5babdee99d2.tar.gz -C "$RESEARCH_RESTORE_DIR"`.
- Complete focused-test captures: `/workspace/work/hifld-coordinator/83/artifact-cleanup-83/fema-regional-offices-complete.json.gz` (58546 bytes; SHA256 `e8f4c330cbd3c666d77721425770066c837e0283c92ddc340a89c19950aed525`).
- Default focused tests: `env -u HIFLD_FEMA_OFFICES_TEST_CAPTURE_ARCHIVE PYTHONPATH=src:tests:. python -m unittest tests.test_fema_regional_offices tests.test_fema_regional_offices_registration` (capture-dependent methods skip).
- Full-capture opt-in: `HIFLD_FEMA_OFFICES_TEST_CAPTURE_ARCHIVE=/workspace/work/hifld-coordinator/83/artifact-cleanup-83/fema-regional-offices-complete.json.gz PYTHONPATH=src:tests:. python -m unittest tests.test_fema_regional_offices tests.test_fema_regional_offices_registration`.
- No source, baseline, compatibility, contract, or release approval is implied.

# Amtrak Stations

- Source: https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Amtrak_Stations/FeatureServer/0
- Local Dagster job: `amtrak_draft_refresh_job`; scheduled refresh remains stopped.
- Release blockers: baseline/crosswalk decision, ID migration, and output contract and release approval.
- Original complete 1,036-row input: `/workspace/work/hifld-coordinator/artifacts/issue-17-complete-source-20261019.zip`
- Original input SHA256: `b207098b37863d006b383c36ff6c0c4ddac379c0c705ab46d6c7ff3f05d8cf04`
- Archived research/evidence: `/workspace/work/hifld-coordinator/artifacts/issue-17-removed-research-docs-d75533f423de.tar.gz` (SHA256 `ac4061c458ceb53735fc48c7a7a55f4dc22c5c49b2db226cc266a8101274c8e4`).
- Opt-in replay: `HIFLD_AMTRAK_TEST_CAPTURE_ZIP=/workspace/work/hifld-coordinator/artifacts/issue-17-complete-source-20261019.zip uv run python -m unittest tests.test_amtrak.AmtrakActualReplayReviewRegression`

Audit tools: `issue-17-audit-tools-d75533f.tar.gz`, SHA256 `e1696708983760acafd1a75d883eae77bc5d0ab82e49b3bbc2dd9e6da62611b4`.

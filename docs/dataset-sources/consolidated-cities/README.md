# Consolidated Cities — unapproved Census 2026 draft

The `census_consolidated_cities` source feeds the local-only `concity_source_integrity` job. Its monthly `concity_source_integrity_monthly` schedule is STOPPED. This is a TIGER2026 successor candidate for an inventory entry naming the 2024 substate edition; the DOI crosswalk remains inaccessible and unverified.

Release is blocked on accepting the source edition and lineage, rights, an approved versioned baseline/schema/ID contract, and compatibility decisions for native CRS, geometry and output formats. No source, mapping, baseline or release is approved.

Original captured bodies and receipts, research, full logs/reports and the provenance replay are retained in `issue-41-removed-research-docs-26032a01c516.tar.gz` (664,388 bytes; SHA-256 `f385bd6c2ec10f6bace3d8335df90fbec54c940b7838a15d0a1f26fc7966428e`). The unused full Dagster replay runner is separately retained in `issue-41-unused-concity-replay-runner-26032a01c516.tar.gz` (3,312 bytes; SHA-256 `8cf447fb485ac6b855faf358b258ff947e592efbc7b86be63277291a11103b2a`). Restore the full evidence outside the Git worktree and the runner in the repository root:

```sh
mkdir -p /workspace/work/hifld-coordinator/issue41-restored
tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-41-removed-research-docs-26032a01c516.tar.gz -C /workspace/work/hifld-coordinator/issue41-restored
tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-41-unused-concity-replay-runner-26032a01c516.tar.gz -C .
```

Capture-independent tests stay enabled by default:

```sh
env -u HIFLD_CONCITY_CAPTURE_DIR uv run python -m unittest discover -s tests -p test_consolidated_cities_source.py
```

To opt into the captured-input tests, set `HIFLD_CONCITY_CAPTURE_DIR=/workspace/work/hifld-coordinator/issue41-restored/docs/dataset-sources/consolidated-cities/evidence/inputs` for that command. This explicit path stays outside the Git worktree. The independent full Dagster replay runner requires the restored originals and an empty isolated runtime directory.

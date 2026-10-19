# Colleges and Universities Campuses — local preservation draft

This is an unapproved historical preservation candidate, not a maintained current-source feed. Source staging is `ornl_campus_historical_reconstruction`; it performs no checks. The monthly `ornl_campus_historical_integrity_schedule` is STOPPED.

Release blockers remain the authority and exact release of the PASDA mirrors, rights, source/published ID and type compatibility (including duplicate published IDs and precision differences), and the fifteen original invalid polygons. No source, contract, geometry exception or release is approved.

Complete original captures, research, execution receipts, JSON reports and one-off audits are retained outside Git in `issue-39-removed-research-docs-cb98e912ad6f.tar.gz`, SHA-256 `4eae08ef934fa5bd4b37bf5fde01456eff24ab12fd49fc07c0ffdb0f70243cb0`, 6,273,614 bytes. The unused Dagster replay runner is separately archived as `issue-39-unused-audit-runner-cb98e912ad6f.tar.gz`, SHA-256 `b2166dde03b3b513c780f09044a17f6aebe8b5a55254f924a8d637a4140148fe`. Restore both from the repository root with:

```sh
tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-39-removed-research-docs-cb98e912ad6f.tar.gz -C .
tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-39-unused-audit-runner-cb98e912ad6f.tar.gz -C .
```

The default focused test command keeps capture-independent mechanics enabled:

```sh
uv run python -m unittest discover -s tests -p test_campus_preservation.py
```

To opt into the preserved-input cases after restoring the archive, set `HIFLD_CAMPUS_CAPTURE_DIR="$PWD/docs/dataset-sources/colleges-and-universities-campuses/evidence/captures"` for that command. All original source and test assertions remain intact.

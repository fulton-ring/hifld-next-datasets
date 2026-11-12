# Consolidated Cities — unapproved Census 2026 draft

The `census_consolidated_cities` source feeds the local-only `concity_source_integrity` job. Its monthly `concity_source_integrity_monthly` schedule is STOPPED. This is a TIGER2026 successor candidate for an inventory entry naming the 2024 substate edition; the DOI crosswalk remains inaccessible and unverified.

Release is blocked on accepting the source edition and lineage, rights, an approved versioned baseline/schema/ID contract, and compatibility decisions for native CRS, geometry and output formats. No source, mapping, baseline or release is approved.

The previous draft referenced an archived evidence bundle at `issue-41-removed-research-docs-26032a01c516.tar.gz` and a separate runner archive. Those files were not present in the execution workspace, so this branch does not claim to have recovered that historical evidence. A new bounded retrieval from the official Census TIGER2026 CONCITY directory was observed on 2026-10-21. Its eight response bodies and receipts, isolated Dagster instance, staging candidate, and run logs are retained outside the Git tree under `/workspace/work/41-evidence/`. This later retrieval is a new observation and does not establish edition equivalence, history before its recorded receipt times, or release acceptance.

Capture-independent tests stay enabled by default:

```sh
env -u HIFLD_CONCITY_CAPTURE_DIR uv run python -m unittest discover -s tests -p test_consolidated_cities_source.py
```

For replay tests against the current retained capture, set `HIFLD_CONCITY_CAPTURE_DIR=/workspace/work/41-evidence/capture`. The capture remains outside the Git worktree. Dagster replay evidence uses the isolated instance at `/workspace/work/41-evidence/dagster-home` and local staging at `/workspace/work/41-evidence/staging`.

# Ferry Terminals source record

Source: BTS NTAD Ferry Terminals item `4f22ac75a7b741499d2f98741e7ff548`, owner `USDOT_BTS`, delegated layer `NTAD_Ferry_Terminals/FeatureServer/0`. The captured layer has 845 rows and 22 native fields. The exact source capture pins and original source record are retained in the external archive below.

Job: `bts_ferry_terminals_draft_refresh`; proposed monthly source-integrity schedule **STOPPED**. This is a local draft and is not approved for publication.

Release blockers: the portal's NCFO 2022 description conflicts with the selected layer's NCFO 2020 / `DATA_YEAR=2019` content; source release date and archive/crosswalk records are unverified; the six historical published candidates do not establish an accepted source contract (including missing/widened IDs and type differences); historical baseline, identifier, schema/nullability, geometry, coverage, format, rights/attribution and release approvals remain unresolved. No replacement IDs, tolerant coordinate matching or source acceptance is claimed.

Archived original research, captures, logs, reports and candidate outputs: `/workspace/work/hifld-coordinator/artifacts/issue-89-removed-research-docs-9253529b5587.tar.gz` (618,009 bytes; SHA-256 `ddc73d7c71d847b1f7f4b1f85ebaf42ac5e805211ffdf3a81891ad9f787e0a7b`; all 71 members verified). Member manifest: `/workspace/work/hifld-coordinator/artifacts/issue-89-removed-research-docs-9253529b5587.tar.manifest.json`.

Restore the complete originals outside the Git worktree:

```sh
mkdir -p /workspace/work/hifld-coordinator/issue89-restored
tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-89-removed-research-docs-9253529b5587.tar.gz -C /workspace/work/hifld-coordinator/issue89-restored
```

Default focused tests do not load captures:

```sh
env -u HIFLD_FERRY_TERMINALS_CAPTURE_DIR uv run python -m unittest discover -s tests -p test_ferry_terminals.py
```

To opt into full-capture tests, set `HIFLD_FERRY_TERMINALS_CAPTURE_DIR=/workspace/work/hifld-coordinator/issue89-restored/docs/dataset-sources/ferry-terminals/evidence/captures` for that command. The external-only test class checks that this directory exists outside the Git worktree.

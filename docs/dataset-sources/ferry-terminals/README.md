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

## 2026-10-21 isolated-runner live check (reviewed head `e6022b4`; this docs-only section added after)

- Focused default tests: 19 discovered, 2 passed, 17 skipped (capture class; the external archive is not present on that runner).
- Real Dagster materialization at that head (`dagster asset materialize --module-name dagster_hifld.definitions --select ingest/ferry-terminals/ferry-terminals`, isolated `DAGSTER_HOME`/`HIFLD_STAGING_DIR`, `KUBERNETES_SERVICE_HOST` unset to avoid the k8s executor): run `28df426a-1759-45b0-a7f7-4306c5c41424`. The ingest step failed fast after 3 bounded attempts and quarantined the response; no candidate was built and nothing was substituted.
- Live `https://www.bts.gov/ntad` returned HTTP 403 (Akamai Access Denied) to every tested User-Agent from that runner egress, so the delegation-evidence capture cannot be refreshed there; full live materialization stays blocked.
- The pinned data layer itself was unchanged at check time: live `FeatureServer/0/query...f=json` returned 482,691 bytes, SHA-256 `2f351a2e26994c0d1b580e2af9a892a4e081376255b4475eaf010958a614be70` (exact pin), `returnCountOnly` 845, and layer `serviceItemId 4f22ac75a7b741499d2f98741e7ff548`.

## 2026-10-21 second isolated-runner session: capture replay and schedule repair

The external capture archive is not present on runner storage and its recorded workflow run is not retrievable, so the nine capture bodies and receipts were restored from this branch's own Git history at the pre-cleanup reviewed head `9253529b5587aa38926edd07b0e1273c38d5e71e` (the cleanup review verified the archived 71-document tar matches those exact Git blobs, so blob restore is byte-equivalent to the tar restore). They were extracted outside the Git worktree and verified before use: all nine receipts match their bodies on URL, status 200, byte count and SHA-256 with aware ISO timestamps fetched 2026-10-19; `features.body` matches the exact pin (482,691 bytes, 845 features, no `exceededTransferLimit`) and `bts_ntad.body` contains the `https://geodata.bts.gov/` delegation. Live `https://www.bts.gov/ntad` still returns 403 from this egress (default and browser User-Agents), so live refresh remains blocked and no live receipt was fabricated.

With `HIFLD_FERRY_TERMINALS_CAPTURE_DIR` pointing at the restored captures, the focused file now runs 20 tests (2 mechanics, 17 capture, 1 new schedule regression) — all pass. Real Dagster materializations used an isolated `DAGSTER_HOME`, local `HIFLD_STAGING_DIR` and `KUBERNETES_SERVICE_HOST` unset. A first live attempt (`018ca1cb-d945-4e62-9ff9-7ea0d5cfd0c4`) reproduced the fail-fast quarantine (three retained 403 receipts, nothing substituted). Capture replay then succeeded: run `97b61e68-8d36-4da7-8a84-edf9544111a8` staged the complete candidate `v0.0.0-draft-ferry-e908b641bba3235b-aee31dd7e22a4019` with 845 records, `approved: false`, 9 inputs, construction bundle, evidence and raw-capture depot; an immediate re-run (`8d64ff96-5f84-414a-8936-dfb865a9599d`) reconciled the cached candidate with the output tree byte-identical.

That session also found and fixed a runnable-pipeline defect in this draft's schedule wiring: Dagster 1.12 asset jobs name nodes by asset key (`ingest__ferry_terminals__ferry_terminals`), so the previous hardcoded `{'ops': {'bts_ferry_terminals': ...}}` job/RunRequest config was rejected at submission, and `required_resource_keys` injects `staging_storage` as a keyword argument the schedule function did not accept. The job now passes no op config (the `capture_dir` default applies) and the schedule receives the resource kwarg directly; a focused regression test evaluates the schedule and validates the emitted RunRequest against the job config schema.

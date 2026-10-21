# County Subdivisions source record

Source: Census TIGER/Line 2024 `County_Subdivision` layer in `tlgdb_2024_a_us_substategeo.gdb.zip`, 36,492 rows, 11 native attributes and EPSG:4269 MultiPolygon. The source implementation retains the edition and exact ZIP pin; the complete original source manifest is preserved in the external research archive. The 736,048,475-byte ZIP remains in its existing read-only external capture depot.

Job: `county_subdivisions_draft_refresh`; monthly schedule **STOPPED**. This is a working draft, not accepted for publication. Release blockers are source and complete historical baseline acceptance; unresolved crosswalk and release-day evidence; rights and attribution (the configured locator returned 404, not a grant); approval of identifier, schema, type, geometry, coverage and format correspondence; and independent artifact-bound release approval.

Archived original research/evidence: `/workspace/work/hifld-coordinator/artifacts/issue-45-removed-research-docs-818b901ca65e.tar.gz` (1,580,232 bytes, SHA-256 `d2a109a47170938bfd2182bf8e3d7247ab2f4b6f7bfb2a430a3cde5ea9b51044`; 91 original files verified byte-for-byte). The member manifest is `/workspace/work/hifld-coordinator/artifacts/issue-45-removed-research-docs-818b901ca65e.tar.manifest.json`.

Restore the full original directory outside the worktree with:

```sh
mkdir -p /workspace/work/hifld-coordinator/issue45-restored
 tar -xzf /workspace/work/hifld-coordinator/artifacts/issue-45-removed-research-docs-818b901ca65e.tar.gz -C /workspace/work/hifld-coordinator/issue45-restored
```

The default suite command remains `uv run python -m unittest discover tests`. The one complete-source audit is isolated in its opt-in test class; to run it, provide both existing external inputs explicitly:

```sh
HIFLD_COUSUB_TEST_ARCHIVE=<external original.gdb.zip> HIFLD_COUSUB_TEST_RECEIPT=<external original receipt.json> uv run python -m unittest tests.test_county_subdivisions.CountySubdivisionCaptureTests
```

All independent tests remain in the default `CountySubdivisionTests` class. This cleanup did not run tests or replay any source, native output, baseline, or capture.

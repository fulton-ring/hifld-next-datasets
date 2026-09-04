# Compact GeoParquet Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write approximately 2 GiB compressed GeoParquet files containing multiple spatially ordered row groups no larger than 128 MiB uncompressed, with semantic partitions preferred over S2 and a 128 MiB dataset footer ceiling.

**Architecture:** Replace the one-buffer/one-file behavior in the streaming writer with per-partition Parquet writer state that appends validated row groups and rolls files near the compressed target. Simplify preflight so S2 is selected only for large unpartitioned data, and extend layout/publish/audit validation with exact footer byte accounting.

**Tech Stack:** Python 3.12, GeoPandas, PyArrow ParquetWriter, Fiona, Dagster, unittest, GCS, Helm, DuckDB.

---

### Task 1: Multi-row-group streaming files and fallback selection

**Files:**
- Modify: `src/dagster_hifld/conversion.py`
- Test: `tests/test_conversion.py`

- [x] **Step 1: Write failing compact-layout tests**

Add tests proving that repeated write-buffer flushes for one logical partition produce one
file with multiple row groups until the compressed file target is reached, that target
rollover produces numbered sibling files, and that semantic/admin partitioning never gains
an S2 suffix solely because the full dataset is large. Update the default-policy assertion
to require `DEFAULT_GEOPARQUET_TARGET_FILE_BYTES == 2 * 1024**3` while retaining the 128 MiB
uncompressed row-group maximum.

- [x] **Step 2: Verify the tests fail for the current one-file-per-buffer behavior**

Run the exact new tests with `uv run python -m unittest ... -v`; expect the compact-layout
test to observe multiple files and the semantic preflight test to observe `admin_s2`.

- [x] **Step 3: Implement per-partition Parquet writer state**

Append each spatially sorted buffer as one row group to an open `pyarrow.parquet.ParquetWriter`.
Canonicalize batch schema metadata so changing per-batch extents do not make schemas
incompatible, preserve the bbox covering column, close and record a file only at rollover or
end-of-layer, and keep the existing exact post-encoding row-group validation and singleton
failure. Rotate based on compressed bytes already written with one row-group reserve so the
physical file remains near the 2 GiB target.

- [x] **Step 4: Restrict S2 to the unpartitioned fallback**

Select S2 only for `force_s2` or a large `single_file` dataset for which no candidate admin
column was selected. Semantic/admin/HUC/prefix layouts remain semantic and rely on file
rollover for oversized partitions. Avoid computing semantic-plus-S2 histograms when they
cannot be selected.

- [x] **Step 5: Run focused conversion tests**

Run `uv run python -m unittest tests.test_conversion -v`; expect zero failures.

### Task 2: Dataset-wide footer budget and maintenance audit

**Files:**
- Modify: `src/dagster_hifld/conversion.py`
- Modify: `src/dagster_hifld/assets/publish.py`
- Modify: `src/dagster_hifld/maintenance.py`
- Modify: `docs/geoparquet-maintenance.md`
- Test: `tests/test_conversion.py`
- Test: `tests/test_publish.py`
- Test: `tests/test_geoparquet_maintenance_task3b.py`

- [x] **Step 1: Write failing metadata-budget tests**

Require every output layout to carry `footer_size_bytes`; require publication to reject
layers whose footer total exceeds 128 MiB; require the audit to compare declared and actual
footer sizes, enforce the combined ceiling, accept large semantic layouts without S2, and
reject a large unpartitioned non-S2 layout.

- [x] **Step 2: Verify the tests fail before implementation**

Run the exact new publish and maintenance test methods with `uv run python -m unittest ...
-v`; expect missing footer fields and the legacy S2 rule to fail.

- [x] **Step 3: Add exact footer accounting**

Read `ParquetFile.metadata.serialized_size` after closing each file, record it in the output
layout, sum it across all layers in `_write_geoparquet_layout_manifest_set`, and raise before
publishing an authoritative manifest when the sum exceeds `128 * 1024**2`.

- [x] **Step 4: Align the read-only audit**

Measure actual serialized footer sizes, validate the manifest declarations and dataset
total, and base the fallback rule on actual compressed file/layout size and partition
strategy rather than total uncompressed bytes. Document the thresholds and semantics.

- [x] **Step 5: Run focused tests**

Run `uv run python -m unittest tests.test_publish tests.test_geoparquet_maintenance_task3b
-v`; expect zero failures.

### Task 3: Verification, image deployment, and real-data rerun

**Files:**
- Verify all changed files; no additional source files are expected.

- [ ] **Step 1: Run the full repository quality gates**

Run `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright`,
`uv run basedpyright`, and `uv run pytest`; expect zero failures.

- [ ] **Step 2: Commit and push the feature branch**

Review the full diff, commit only the planned files, and push
`codex/geoparquet-source-publishing`.

- [ ] **Step 3: Build, push, and deploy**

Run `hifld-next-iac/scripts/deploy-dagster.sh hifld-next <immutable-tag>` from the IAC
repository. Confirm the daemon, webserver, and user-code deployments all use the new image
and are ready.

- [ ] **Step 4: Materialize canary and NFHL assets**

Trigger the restored Census Block Groups version first, verify its semantic layout, then
trigger the NFHL asset and monitor it through publication. Do not delete or replace source
formats.

- [ ] **Step 5: Verify storage and DuckDB performance**

Check production object sizes and version generations, aggregate footer size, every row
group's uncompressed size, feature counts, schemas, CRS, and bbox coverage. Run DuckDB
selective spatial and aggregate queries against the GCS GeoParquet glob and report timings
and query plans.

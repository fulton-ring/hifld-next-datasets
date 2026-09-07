# Row-group packing implementation plan

**Goal:** Merge consecutive undersized batches into approximately 128 MiB uncompressed GeoParquet row groups.

**Approved design:** Keep a pending Arrow row group per currently processed semantic partition; accumulate consecutive sorted batches using measured Arrow size as a cheap estimate, split incoming batches at target/row-count boundaries, and retain temporary-Parquet validation of actual encoded uncompressed bytes. Flush at partition end. Keep the 160 MiB hard limit, bounded memory, spatial ordering, all rows, and existing 2 GiB compressed file rollover. Do not change storage paths, partition naming, source selection, or deployment.

**Evidence:** Iowa NFHL West part-001 contains three row groups of 67,368,260, 44,310,480, and 3,778,212 uncompressed bytes. Their combined size is below the 128 MiB target. The old writer emits each estimated-size input batch immediately and only splits oversized batches.

**Architecture:** The sorted spool still owns ordering. The writer decouples incoming feature batches from output row-group boundaries. Consecutive Arrow pieces may be combined, but never across semantic partitions. Final validated row groups are passed to the existing compressed-size rollover path. A final remainder and safety-driven early flushes remain permitted; exact 128 MiB is not required.

**Tech stack:** Python, Arrow/Parquet, GeoPandas, unittest.

## Tasks

- [x] Establish clean baseline with `uv run --no-sync python -m unittest discover tests`: 328 tests, one skipped.
- [x] Add a regression to `tests/test_conversion.py` (or a focused companion) that feeds multiple tiny input batches and expects one final row group, identical rows/order, and unchanged partition paths. Run targeted unittest and observe the old writer failing that expectation.
- [x] Update `src/dagster_hifld/conversion.py` to buffer Arrow pieces by measured size, split at target/row-count boundaries, flush at partition end, and validate actual Parquet hard limits before final writes. Avoid repeated compression of the entire growing pending group.
- [x] Cover target-size splitting, row caps, semantic partition isolation, row conservation/spatial order, oversized features, final remainder, and compressed file rollover. Update old tests that use tiny input buffers to deliberately force output groups to use explicit row caps instead.
- [x] Run focused tests, full suite, changed-file formatting and lint with existing findings separated from new findings. Independently review specification compliance and code quality.
- [ ] Integrate the verified commit into `codex/geoparquet-source-publishing`; report tests and deployment status. No deployment or rerun is authorized in this request.

## Verification evidence

- RED: merging regression expected `[3]`, old writer returned `[1, 1, 1]`; target-packing regression expected `[2, 2, 1]`, old writer returned five singleton groups.
- Independent final full suite after implementation and review follow-ups: 334 tests passed, one skipped (4.541s).
- Independent local geometry-heavy probe: 400 polygons with varying vertex counts, split into two semantic partitions, 256 KiB row-group target. Before: 40 groups per partition, approximately 10–31 KiB each. After: 3 groups per partition; measured Parquet uncompressed sizes `[261371, 257760, 201628]` and `[260891, 264960, 206908]` bytes. All IDs preserved and no partition mixing. Timing was 0.60s before and 0.69s after; this tiny fixture is not a production performance benchmark.
- Existing writer lint baseline: 49 findings (I001: 1, UP035: 1, BLE001: 19, UP045: 27, PYI034: 1). Unrelated legacy lint cleanup is out of scope.
- Writer lint after implementation: same 49 findings, no increase. Conversion module and conversion test formatting checks pass. The unrelated existing formatting in `tests/test_publish.py` is preserved; its only change is an explicit row cap in a rollover fixture.
- Independent spec and quality reviews passed. Follow-up tests assert exact incoming-batch splits, combined row caps, and skewed row sizes. Nonpositive row caps now fail explicitly instead of potentially stalling.
- Implementation commit: `c6e37ad`. No deployment, data rewrite, promotion, or production mutation performed.

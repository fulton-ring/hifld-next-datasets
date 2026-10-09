# Preserve source ID implementation plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task-by-task, with test-first and verification-before-completion discipline. Steps use checkbox syntax for tracking.

**Goal:** Preserve source ID attributes and reject new conversions with missing catalog-described fields, without rewriting history.

**Architecture:** Keep the existing chunked converter and authored dictionary semantics. Separate the GeoPackage internal FID from source ID attributes, and gate converted records against actual GeoParquet facts before catalog activation. Catalog-only refreshes remain compatible with historical data.

**Tech Stack:** Python, unittest, Fiona/GeoPandas, PyArrow, tippecanoe, local storage resources.

Approved design: `docs/superpowers/specs/2026-10-08-preserve-source-id-design.md`.
Execute inline in the existing `codex/preserve-source-id` worktree; no new production publication.

## Task 1: ID preservation

Files: `src/dagster_hifld/conversion.py`, `tests/test_conversion.py`.

- [x] Add a real-driver regression with 24 string IDs (including leading zeros),
  uppercase/lowercase ID cases and a distinct existing `source_ID`. Write source
  GeoJSON, call `write_geopackage_chunked`, and inspect both Fiona attributes and
  SQLite `fid`. Force one-feature append batches with a zero test chunk target;
  retain the production default unchanged. Core test assertions:

  ```python
  ids = [f"{value:010d}" for value in range(24)]
  self.assertEqual(result[id_name].tolist(), ids)
  self.assertEqual(result["source_ID"].tolist(), [f"original-{v}" for v in ids])
  self.assertNotIn("source_ID_2", result.columns)
  self.assertNotIn("index", result.columns)
  ```

- [x] Run `UV_CACHE_DIR=/private/tmp/hifld-id-repair-uv-cache uv run python -m unittest discover -s tests -p test_conversion.py`; expect failure because ID is renamed.
- [x] Remove only `"id"` from the reserved set. Add `index=False, FID="fid"`
  to the sample write and all four output writes in `write_geopackage_chunked`.
  Preserve the remaining FID collision logic.
- [x] Re-run targeted tests; verify leading zeros, separate internal FID and
  all append batches. Exercise both installed writer engines without changing
  global engine configuration outside a test context.
- [x] Commit the tested converter repair together with the publication gate after final verification.

## Task 2: Converted-record schema gate

Files: `src/dagster_hifld/portolan/workflow.py`, `tests/test_portolan_workflow.py`.

- [x] Add tests calling a typed `validate_converted_columns` helper with
  dictionary `ID` and actual `source_ID` to require rejection. Accept matching
  attributes plus extra derived columns. Accept renamed geometry only when the
  declared primary geometry is physically present and marked as geometry.
  Core failure test:

  ```python
  described = columns_from_dictionary({"columns": [
      {"name": "ID", "type": "string", "nullable": False}
  ]})
  with self.assertRaisesRegex(ValueError, "missing.*ID"):
      validate_converted_columns(described, facts_with_source_id)
  ```

- [x] Run the workflow tests and observe the missing helper failure. Implement:

  ```python
  def validate_converted_columns(
      columns: tuple[ColumnRecord, ...], facts: GeoParquetFacts
  ) -> None:
      physical = {column.name for column in facts.columns}
      missing = sorted({
          column.name for column in columns
          if not column.is_geometry and column.name not in physical
      })
      if missing:
          raise ValueError(
              "Generated GeoParquet is missing catalog-described fields: "
              + ", ".join(missing)
          )
      if any(column.is_geometry for column in columns) and not any(
          column.name == facts.geometry_column and column.is_geometry
          for column in facts.columns
      ):
          raise ValueError("Generated GeoParquet is missing its primary geometry field.")
  ```

- [x] Add a publication regression using local storage and a real Parquet
  footer. Pin an initial pointer; arrange mismatched dictionary/Parquet plus
  quality/collection/source metadata; replace only the expensive conversion
  stage with a prepared output. Call `publish_portolan_record` and require
  rejection and byte-identical active pointer. Separately verify `convert=False`
  can still prepare the same historical record. Observe rejection test fail
  before wiring the helper into `_prepare_portolan_record`.
- [x] Parse dictionary columns once after source-dictionary validation, call
  `validate_converted_columns(columns, facts)` only when `convert` is true,
  and pass those parsed columns to `CatalogRecord` instead of parsing again.
- [x] Run workflow tests, then full tests and compile checks.

## Task 3: Cross-format and handoff verification

- [x] Exercise the repaired chunked GeoPackage through actual GeoParquet,
  Shapefile ZIP and PMTiles converters. Compare ID values and PMTiles metadata;
  report any missing native tools explicitly. Add reusable regression coverage
  in `tests/test_conversion.py` instead of temporary test files.
- [x] If previewing Hospitals, download only its archived GeoPackage into a
  temporary local directory, explicitly rename `source_ID` to `ID` in a new
  copy, compare all 8,340 values keyed by OBJECTID, and run local converters.
  No production bucket writes, pointer changes or publication occur.
- [x] Run targeted tests, `uv run python -m unittest discover tests`,
  `uv run python -m compileall -q src/dagster_hifld`, and `git diff --check`.
  The publisher has no configured Ruff/Pyright gates; do not pretend the
  application repository's checks are configured here.
- [x] Review the diff against the approved scope and record exact outcomes.
  Update the existing production report if necessary; it already records the
  completed rollback and historical mismatch. Hand off a reviewed branch/PR,
  explicitly distinguishing implementation from deployment and publication.

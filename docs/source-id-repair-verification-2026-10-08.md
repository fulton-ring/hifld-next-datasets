# Source ID repair verification — 2026-10-08

## Scope and status

Implemented on `codex/preserve-source-id`, based on publisher main `23bb5dc`.
This is a code repair and local data preview, not a production deployment or
dataset publication. Production remains on the complete rollback release
`cf809c93-c5c0-4ec3-968a-d3ecb8ec6696`, with Hospitals v1.1.0 latest. Historical
objects have not been rewritten.

## Implementation

- The chunked GeoPackage writer preserves source `ID`/`id`, explicitly uses a
  separate internal `fid`, and does not export the pandas index. Existing FID
  collision handling is otherwise unchanged; legitimate `source_ID` fields are
  not heuristically renamed.
- Converted publication checks every described non-geometry field by exact name
  against actual GeoParquet facts. A declared geometry may be normalized to the
  actual primary geometry field. Extra derived physical fields remain allowed.
  Missing fields reject catalog activation. This gate checks names/presence,
  not complete cross-format type/value equivalence.
- Catalog-only refreshes remain compatible with historical mismatches. Existing
  schemas are not automatically corrected or certified by this change. Immutable
  data/metadata can already have been promoted before this gate; rejected output
  remains unreferenced rather than being deleted or selected by the pointer.

## Regression evidence

Before the fix, real-driver tests reproduced uppercase `ID` being renamed to
`source_ID_2` in the presence of a distinct `source_ID`. Lowercase `id` also
exposed the old sanitizer's case-insensitive SQLite name collision. Both Fiona
and Pyogrio now preserve names, values and leading zeros through 24 rows and
multiple append batches, with internal FIDs 1–24 kept separate.

The workflow regression first failed with `ValueError not raised`: a dictionary
described `ID`, actual Parquet contained `source_ID`, and publication proceeded.
It now rejects that mismatch and verifies the previous active pointer is
byte-identical. Separate tests cover case-sensitive attribute matching, normalized
geometry, extra derived columns, absent primary geometry and historical refresh.

## Local Hospitals preview

Downloaded only the archived v1.1.0 GeoPackage. SHA-256 before and after:
`0872793940e4984b7873f4ee559b4f4661ab237f84d71585996d5aa0b26a3858`.
An explicit `source_ID` → `ID` change was made in a separate local input copy;
the archived download stayed unchanged. No GCS writes or pointer changes occurred.

The repaired chunked writer and real derived-format converters produced:

- GeoPackage, GeoParquet and Shapefile ZIP: all 8,340 `ID` values exactly match
  the archived `source_ID`, keyed by OBJECTID; no `source_ID` remains in outputs.
- PMTiles: `ID` is a string field. Decoding zoom 6 yielded 8,949 tile features
  covering all 8,340 distinct source OBJECTIDs; every ID value matches its source.
  Repeated tile features are not extra source records.
- Leading-zero examples remain strings: `0196496796`, `0002996766`, `0002596746`.
- GeoParquet schema inspection and the new name-validation helper accept the
  corrected ID/geometry dictionary.

Local artifacts are retained under `/private/tmp/hifld-id-preview.giAPZg/`,
including the unchanged download, explicit corrected input, converted GeoPackage,
derived GeoParquet/PMTiles, Shapefile ZIP, verification script, result JSON and
conversion log. These temporary artifacts are not committed or uploaded.

## Checks

From the publisher worktree with its managed uv environment:

- `uv run python -m unittest discover -s tests -p test_conversion.py`: 108 tests passed.
- `uv run python -m unittest discover -s tests -p test_portolan_workflow.py`: 42 tests passed.
- `uv run python -m unittest discover tests`: 451 tests run, zero failures/errors,
  one skipped (450 passed).
- `uv run python -m compileall -q src/dagster_hifld`: passed.
- Native Ruff check for `portolan/workflow.py` and `test_portolan_workflow.py`:
  passed. Added/changed test regions were Ruff-formatted; `git diff --check` passed.
- Optional native Ruff check for `conversion.py` and `test_conversion.py` still
  fails on 57 pre-existing findings: 27 UP045, 18 BLE001, 5 SIM117, 2 I001 and
  one each UP035, ISC004, RUF012, PYI034 and C408. Running the same native check
  against the original `23bb5dc` files reports 58 findings (48 + 10); the touched
  import block removes one I001. No broad ignores or unrelated cleanup were added.
  This publisher repository has no configured Ruff/Pyright CI gates; this is not
  a claim that full-file lint/type checking is clean.

The existing requests dependency warning and sandbox-restricted PyArrow CPU
introspection warnings remain. Negative-path tests intentionally log rejected
conversion cases. They did not cause test failures.

## Production follow-up

Review and merge/deploy the publisher repair separately from any data publication.
A corrected Hospitals release needs an explicitly reviewed new version from the
corrected input, with unchanged source-data dates and processing provenance.
Do not overwrite v1.1.0 or v1.1.1, guess at other `source_ID` fields, or present
the processing-only change as newly fetched agency data.

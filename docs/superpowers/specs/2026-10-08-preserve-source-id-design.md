# Preserve source ID fields during conversion

## Scope and approved production action

Restore Hospitals v1.1.0 as latest and correct the publisher without rewriting
historical data. The processing-only v1.1.1 publication was a pipeline test, not
a source-data fix or an update from the agency.

The production pointer has been rolled back to the complete permanent-bucket
release `cf809c93-c5c0-4ec3-968a-d3ecb8ec6696`. Both the webapp and feature server
adopted it without restarting, serve v1.0.0 and v1.1.0 with 8,340 features each,
and no longer advertise v1.1.1. No historical objects were changed or deleted.

## Root cause and historical limitation

`conversion._sanitize_geopackage_columns` unnecessarily treats `id` as reserved
and renames `ID` to `source_ID`. A separate internal GeoPackage `fid` can coexist
with the source's string `ID`, including its leading zeros.

The archived v1.1.0 GeoPackage and GeoParquet already contain `source_ID`, while
its PMTiles and Shapefile contain `ID`. Reprocessing that archived GeoPackage
propagated `source_ID` into the v1.1.1 PMTiles and Shapefile. Both versions'
authored data dictionary still describes `ID`; publication did not check that
the described source fields exist in the actual GeoParquet.

Rolling back restores the previous map/download behavior. It does not repair
the older GeoPackage/GeoParquet or their existing schema mismatch.

## Chosen repair

1. Preserve source `ID` and `id` as ordinary attributes in every sample and chunk
   written by `write_geopackage_chunked`. Keep a separate internal `fid` and do
   not export the pandas index. Retain existing collision handling for actual
   FID-reserved names; do not broaden this repair into a naming-policy redesign.
2. Before a newly converted record can enter a catalog release, check that every
   non-geometry field from the authored dictionary exists by exact name in the
   actual GeoParquet schema. Validate geometry through the explicit primary
   geometry metadata, since source geometry names can differ from the converted
   geometry name. Allow additional physical/derived GeoParquet columns. Reject
   missing or renamed described fields with a useful error listing their names;
   leave the active release pointer unchanged on rejection.
3. Keep this new gate on conversion/publication (`convert=True`), not historical
   catalog-only refreshes. This prevents new inconsistent output while preserving
   the ability to refresh or roll back existing immutable releases. It does not
   certify existing historical schemas or automatically correct them.
4. Do not guess that arbitrary `source_ID` attributes should become `ID`.
   To preview a corrected Hospitals release, use a separate local working copy
   with the explicitly reviewed `source_ID` → `ID` change, verify all 8,340 values
   and leading zeros, and regenerate derived outputs from that copy. Never
   overwrite the archived input or publish another production version without
   a separate review of the corrected outputs and publication decision.

This is narrower than replacing all catalog schemas with physical Parquet
schemas, which would expose internal columns and change authored metadata
semantics. Merely removing `ID` from the rename list without a publication gate
would leave the metadata inconsistency undetected.

## Verification and acceptance

- Real GeoPackage driver regression tests preserve uppercase/lowercase source
  IDs, leading zeros, and distinct existing `source_ID` attributes across
  multiple chunks; the internal `fid` remains separate.
- Schema-gate tests reject `ID` versus `source_ID`, accept exact source names
  plus derived columns, and cover geometry-name normalization. Workflow tests
  demonstrate rejection before pointer activation and catalog-only compatibility.
- Exercise corrected local data through GeoPackage, GeoParquet and Shapefile
  output, comparing field names and source values. Verify PMTiles field names
  using the production converter toolchain when available; explicitly report
  any toolchain limitation rather than substituting a mocked claim.
- Run targeted conversion/workflow tests first, then the publisher's full
  `uv run python -m unittest discover tests` gate and import/compile checks.
  Run configured lint/type checks if present; do not introduce new broad dynamic
  typing or project-wide ignores.
- Update the production-cutover report to record the rollback and historical
  schema limitation. No bucket cleanup or application API change is included.

## Implementation boundary

Expected application changes are limited to `src/dagster_hifld/conversion.py`
and `src/dagster_hifld/portolan/workflow.py`, with regression coverage in
`tests/test_conversion.py` and `tests/test_portolan_workflow.py`. No dependency,
environment-variable, storage-path or public API change is required.

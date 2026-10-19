# Verifying an agency ingest against HIFLD Next

## Acceptance reference

Use the currently accepted **published HIFLD Next dataset/file version**, not the candidate download, as the independent baseline. Resolve the inventory-to-catalog mapping explicitly: inventory IDs can differ from published slugs. Pin the published dataset slug, file slug, version, actual source object URLs and checksums at the start of verification. Preserve that reference through implementation and review; do not follow a changing "latest" pointer or reset the baseline to make a comparison pass. Keep the original archive version available as historical context when the accepted dataset has since been updated.

The approved schema/ID contract must be derived from that accepted data and its dictionary. Do not invent a replacement schema. If the baseline cannot be retrieved or confidently matched, verification is **blocked**, never passed.

## Three comparisons

1. **Agency source → staged raw data:** prove a complete download. For ArcGIS, reconcile object IDs/counts and pagination/transfer limits, not just HTTP success. Record the service/layer identity, request time, upstream data release date, filters, counts and checksums. If the upstream changes during extraction, obtain a consistent snapshot or retry/report the inconsistency.
2. **Staged raw data → candidate published output:** verify that conversion preserves every expected record, ID and approved attribute, as well as geometry. Any documented filtering, deduplication or splitting must have explicit accounting; it cannot silently lose rows. Validate the actual generated output, not only the input or an asserted schema JSON.
3. **Accepted HIFLD Next version → candidate:** perform a full record comparison and quality regression analysis. New source releases are expected to differ; report and explain those differences instead of demanding byte-for-byte equality or assuming any change is an error.

## Required comparison report

Write a reproducible, machine-readable `metadata/baseline_comparison.json` and a readable summary. Record baseline/candidate versions, URLs/checksums, source identity, matching rules, comparison code/command and results.

- **Schema:** added/removed/renamed fields, types, nullability, meanings, geometry type and CRS. Preserve source and published identifier columns and leading zeros/precision. No unexplained additional published columns. Preserve raw source fields, but flag unexpected raw fields rather than silently dropping them.
- **Rows:** baseline, upstream, staged and output counts; unique/null/duplicate key counts; full ID sets for retained, added and removed records. On retained IDs, compare approved attributes and geometry, report changed-field counts and representative examples. Use a reviewed composite key only when a stable producer key is unavailable; otherwise matching is unresolved. Never compare by row order, use regenerated row numbers, or invent ID continuity.
- **Coverage:** compare counts and missingness by meaningful geography/category (for example state, county, facility type), plus extents. Similar total counts can conceal a missing state or a changed population definition.
- **Quality:** null/invalid/empty geometries, out-of-range coordinates, duplicate records/keys, required-field nulls, attribute domains and source-date distributions. Distinguish real producer updates from conversion corruption or an accidentally clipped/mirrored/different layer.
- **Currency:** record the source's release/observation dates separately from portal edits and download time. A newer upload of an old mirror is not a fresh producer release.

Full counts, key sets, required-field and geometry checks must cover the whole dataset. Samples illustrate differences; they are not proof of completeness. Use the project's existing Polars/geospatial stack with bounded-memory/disk-backed comparisons for large datasets. Reuse shared helpers rather than writing 415 comparison engines.

## Decision policy

No universal ±10% row-count rule is sufficient. Use dataset-specific, documented acceptance criteria grounded in the pinned HIFLD Next baseline, source publication practice and approved transformations.

ID loss/churn, schema changes, unexplained extra columns, population/coverage changes, substantial unexplained removals or quality regressions route to **Potential breaking change** and block auto-merge and publication. Missing evidence/baseline fails closed. Legitimate additions, removals or updated attribute values can pass only when their cause is supported by source evidence and accepted criteria; equal row counts alone never constitute approval.

Retain the staged candidate, report and last accepted published version on failure. Never promote a candidate first and compare afterward.

## Code changes

For each ordinary dataset:

1. Add/update the explicit ingest in `src/dagster_hifld/assets/<agency>.py`, reusing `download_convert_and_stage` from `download.py` when the source is a straightforward file download. Add source-specific extraction/pagination only when needed.
2. Import/register it in `assets/ingest_registry.py`. `assets/supported_assets.py` derives downstream support. Existing `assets/catalog.py`, `assets/publish.py` and shared publish partitions provide catalog/publish handling; do not duplicate those per dataset or restore an obsolete per-dataset partition registry.
3. Supply producer provenance, refresh policy, approved mapping and dataset-specific comparison criteria. For releases, retain the existing semantic-version publish contract rather than treating an ingest timestamp as an accepted published version.
4. Add focused extraction/ID-preservation/comparison tests and include an end-to-end local verification report against the real pinned HIFLD Next baseline.

Shared prerequisite work, implemented once:

- Put reusable row/quality comparison logic in `src/dagster_hifld/validation.py` (currently empty), reusing `catalog.py` quality/dictionary generation.
- Extend `assets/catalog.py` to write the comparison report for the candidate against the pinned published baseline. Load published baseline data using the existing storage resources; a staging-only hard-coded historical version is not a substitute.
- Update `checks/common.py` and `checks/__init__.py` to consume the report, fail on missing/unverified baselines and make the promotion-critical checks blocking.
- Enforce the same shared gate before immutable promotion in `assets/publish.py`, including `run_local_version_pipeline`, so direct/local calls cannot bypass a Dagster check.
- Test missing baseline, same counts with different IDs, a missing region masked by growth elsewhere, legitimate producer additions, removed IDs, extra columns, value/type loss, conversion truncation and quality regressions.

## Implementation status

This document and the worker/reviewer instructions define the required acceptance plan. **The full row-level comparison and blocking publish integration are not implemented yet.** Existing catalog checks provide useful metrics but currently use a legacy `v20260214` baseline, allow added columns, tolerate ±10% count changes and pass when that baseline is absent. They must not be presented as adequate end-to-end validation.

Complete the shared prerequisite before enabling automatic implementation acceptance. A worker must not claim success from source availability, a copied contract JSON, tests alone, or schema/count similarity without this data comparison.

## Flexible dataset construction

The download helper is optional. Pipelines can assemble many source files, page services, combine state/county releases, select layers, join agency reference tables, derive geometries, parse tables, normalize values or perform other source-specific transformations. Use ordinary Python plus Dagster assets/ops/graphs; the shared contract concerns verifiable final dataset outputs and provenance, not identical ingestion implementations.

Separate supporting raw/intermediate assets from publishable terminal dataset/file outputs. Include supporting assets in Definitions, but do not automatically turn every source file or intermediate table into a published dataset. A graph-backed asset can expose one terminal ingest output. If using a multi-output asset definition, update/test the current one-key-per-definition support derivation in assets/supported_assets.py; do not silently use only its first key.

Define approved source schemas/mappings per input. They can differ from the historic HIFLD format. The accepted published schema/ID contract is the output reference; source differences must be explained and mapped, not hidden or blindly copied as new published columns.

Make verification transformation-aware. Concatenation accounts for every expected partition and record; joins check cardinality, unmatched keys and accidental multiplication; filters/deduplication/aggregation record their rules and count/identity effects; reprojection/geometry construction checks spatial validity and coverage. Retain raw inputs and source IDs plus appropriate output-to-input lineage. An intentional change in row grain or published identity is a compatibility decision, not permission to discard IDs.

Do not require raw and final counts to be identical when the documented transformation legitimately changes them. Require complete accounting, tested transformation semantics and comparison of the final constructed dataset to its pinned accepted HIFLD Next version.

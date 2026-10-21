# Courthouses source audit

The inventory name maps to the current USGS National Structures Courthouses layer, not to an
unverified HIFLD mirror. The official producer service is the [USGS National Map Structures
MapServer](https://carto.nationalmap.gov/arcgis/rest/services/structures/MapServer), layer 40,
“Courthouses.” USGS describes the layer as “A building in which courts of law are regularly
held.” The current service copyright says “Data Refreshed October, 2026.” That is a service
metadata observation; the producer does not state a release date or edition, so the pipeline
records both as unknown.

The historical reference is the DataLumos / ICPSR [HIFLD OPEN Courthouses V1
project](https://www.datalumos.org/datalumos/project/239030/version/V1/view), whose page describes
a 2024 data period and a January 2025 refresh. The anonymous archive request returned HTTP 403,
so its downloadable GeoJSON and shapefile were not retrieved or compared. The DHS-to-USGS
crosswalk and historical equivalence remain unverified. No equivalence or migration is claimed.

## Complete current-layer candidate

On 2026-10-21, before and after witnesses agreed on 3,713 features and the complete OBJECTID
roster. Sixteen bounded service/query responses preserve the service and layer metadata, counts,
ID rosters, eight pages of 500 or fewer features, decoded bodies, original wire bodies, and
hash-pinned receipts. The current response JSON lowercases attribute member names even though
the layer declaration uses uppercase field names; the raw responses retain those exact spellings,
and the candidate maps those keys to the declared uppercase schema without altering values.

All 19 declared attributes are retained, including OBJECTID, PERMANENT_IDENTIFIER, and GLOBALID.
All 3,713 native EPSG:3857 points are present. The candidate does not geocode, reproject, repair,
or drop records. The only processing is typed GeoParquet construction with native WKB points.
Parquet readback compares every attribute value and every point WKB against the retained service
responses. The complete ID, schema, record, and geometry digests are pinned in the audit file.
Nulls are retained, including ADMINTYPE on every row, SOURCE_FEATUREID on 3,711 rows,
ADDRESSBUILDINGNAME on 3,691 rows, and GNIS_ID on 3,712 rows.

The current-layer candidate is `v20261021T181055Z` under `courthouses-3/courthouses-3/` in the
isolated evidence staging directory. Its typed GeoParquet is 409,338 bytes with SHA-256
`805cdc8e26265caa6c905bc10b6134555698a4ab3acb5f4ed1c88d100e6dd8f5`. Replaying the retained
capture after cataloging produced a separate timestamped candidate with byte-identical source
outputs and no additional network requests. This replay uses immutable writes; it is not a
cross-run cache claim.

## Runtime and gates

The registered source and catalog were run from an isolated package copy with no `.git` metadata.
The manifest records `sha256-packaged-dagster-hifld-v1`, a `packaged-sha256:` digest, and
`working_tree_status: unknown`; it does not fabricate a Git commit. The registered candidate
quality check passed with 3,713 points, zero null geometries, and zero invalid geometries. A
post-catalog no-Git replay returned the same raw, audit, source-manifest, and GeoParquet hashes.

Registered release checks also exercised both refusal paths against a populated 14-object
synthetic prior-release fixture. The false-quality run stopped at the blocking quality check
before verify or promote. The empty-approval run passed quality, failed at verify, and never
started promote. The published fixture inventory remained byte-identical across both runs.
Those gate runs used candidate `v20261021T180808Z`, before the final adapter added more strict
receipt-scope and Parquet-readback assertions. The source values, GeoParquet bytes, catalog,
quality logic, verifier, and promotion controls are unchanged; the final candidate adds those
fail-closed assertions and records the same source and geometry digests.

The ingest is registered with a monthly refresh schedule that remains stopped. The source is an
unapproved candidate only. HIFLD continuity, producer edition/release date, baseline and ID
contract, coverage, rights, attribution, and release approval remain pending. No production data
was published. Run IDs, object hashes, and retained local evidence paths are in
[`local-materialization.json`](local-materialization.json); machine-readable source observations
are in [`source.json`](source.json).

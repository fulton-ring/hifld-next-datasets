# Coastlines (USACE IENC)

The archive inventory maps this entry to DataLumos project 238945, version V1.
That archive page is historical evidence and its 2025-04-08 inventory edit is
not a source release date. The DHS crosswalk workbook and archived feature data
were not available for this audit, so the exact crosswalk row and continuity
with the old HIFLD extraction remain unresolved.

The current candidate is USACE IENCCloud `COASTLINE_LINE`, MapServer layer 33.
The public USDOT BTS IENC item (owner `USDOT_BTS`, ID
`3607d485a9854d6fa458bf507058ceae`) explicitly links the master file
geodatabase and feature-class service, says USACE/Army Geospatial Center
updates the charts bi-monthly, and warns that chart features and attributes
can be significantly inaccurate. This supports official distribution and
publisher cadence. The item’s portal-modified timestamp is not a release date
for this layer.

Anonymous metadata checks on 2026-10-20 found an ArcGIS polyline layer in
EPSG:4326 with eight declared fields, a 2,000-record maximum, 22,819 records,
and 22,819 unique OBJECTIDs from 1 through 22,819. The exact IDs-only response
and its request receipt are retained with the source record. A ten-feature
sample returned 52,122 bytes; a rough extrapolation suggests about 119 MB of
JSON before overhead. The proposed 450 MB acquisition limit leaves headroom,
but this estimate is not a full-transfer measurement. No feature-wide capture
or row-level comparison has been performed.

The layer covers inland chart waterways (observed extent about 29.68–45.14°N,
79.47–96.67°W); it does not represent the national coastline. OBJECTID must
not be assumed stable between releases. `Feature_ID` is present, but its
nullability, uniqueness and cross-release continuity have not been measured.
The service exposes no dated release field. These facts leave source freshness,
historical continuity, identifiers, coverage approval, rights and the
published schema contract open.

The source-specific Dagster draft captures every paginated response with exact
wire/body receipts, validates the full before/after source ID roster and schema,
preserves all native attributes and line coordinates, and stages a GeoParquet
candidate. The registered catalog and quality check run on this candidate.
Historical baseline, freshness, schema, ID, coverage, rights and release gates
remain false; the stopped schedule and exact-approval verifier prevent release.
`Feature_ID` contains 22,571 distinct non-null values across 22,819 records, so
the source field is retained without treating it as a unique key.

Full local evidence, output comparisons, run IDs and the fail-closed verification
result are recorded in [runtime-evidence.md](runtime-evidence.md). The exact
metadata/capture checksums are in [source.json](source.json) and
[capture_manifest.json](capture_manifest.json). No accepted release or
production publication is implied.

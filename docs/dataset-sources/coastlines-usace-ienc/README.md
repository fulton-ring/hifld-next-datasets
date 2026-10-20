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

The complete registered source capture on 2026-10-20 retained 22,819 records
in 12 offset pages. All 23 requests had complete HTTP 200 responses; before
and after counts and ID rosters matched exactly. The full transfer was
135,330,690 decoded/wire bytes across metadata, rosters and feature pages,
within the 450 MB streaming capture ceiling. Raw responses, receipts, page
limits and CRS metadata are retained in the capture manifest. The final
corrected-head source run replayed the retained capture without a live request.

The layer covers inland chart waterways (observed extent about 29.68–45.14°N,
79.47–96.67°W); it does not represent the national coastline. The complete
candidate preserves all seven attributes and every line coordinate exactly
from captured responses. OBJECTID must not be assumed stable between releases.
`Feature_ID` has 22,571 distinct non-null values across 22,819 records and is
not treated as a unique key. The service exposes no dated release field.
Source freshness, historical continuity, identifiers, coverage approval,
rights and the published schema contract remain open.

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

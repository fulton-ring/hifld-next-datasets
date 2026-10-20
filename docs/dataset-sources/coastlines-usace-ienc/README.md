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

No ingest implementation is included because issue #37 requires an approved
raw/published contract and a source-to-archive compatibility decision before
implementation. The source record preserves the exact URLs, source declarations,
metadata/ID census receipts, limitations and outstanding gates. The card should
remain in Source research until maintainers provide or approve the contract and
the archive baseline can be compared.

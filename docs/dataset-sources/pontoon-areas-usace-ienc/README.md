# Pontoon Areas (USACE IENC)

The HIFLD inventory row points to DataLumos / ICPSR project 239792, Version V1. The archived item is historical evidence. Its source date (2018-04-16) and inventory portal edit date (2025-04-08) are not current source release dates. The DataLumos V1 page returned HTTP 403 during this reconstruction, so current-to-historical ID and geometry comparison remains pending.

The official USDOT BTS Inland Electronic Navigational Charts item is owned by `USDOT_BTS`. It describes USACE/AGC bi-monthly IENC updates, directly links the USACE IENC master geodatabase and official Feature Classes service directory, and warns that chart-derived features may contain inaccuracies. The official directory lists `PONTOON_AREA`; its service is USACE IENC / AGC, layer 73.

At the observed source endpoint, PONTOON_AREA contains 607 polygons with native OBJECTID values 1–607, six non-geometry source attributes and EPSG:4326 coordinates. This is the complete observed IENC chart feature class, not a nationwide inventory of pontoon structures. The layer provides no edition or release date. The BTS update cadence is separate from retrieval date.

The draft pipeline captures all raw ESRI responses and request receipts, brackets the complete ID roster with before/after reads, checks schema, source geometry and layer extent, and stages source-faithful typed GeoParquet. All source IDs/attributes and polygon rings are preserved without reprojection or geometry repair. Refresh is STOPPED and candidates remain unapproved pending baseline/continuity, release/currentness, schema/ID/coverage, rights, attribution and release decisions.

See [source.json](source.json) for exact field declarations, provenance and open decisions.

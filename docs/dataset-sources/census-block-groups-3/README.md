# Census 2024 Block Groups

The draft ingest streams every feature from the `Block_Group` layer of the Census
Bureau's 2024 national substate FileGDB archive into native GeoParquet. It retains
the driver-emitted `OBJECTID`, seven source attributes, and WKB without generating
a business identifier or normalizing geometries. The source layer definition says
`esriGeometryPolygon` with 4-byte OID; GDAL's Arrow interface represents those as
MultiPolygon WKB and int64, respectively. That encoding difference is recorded,
not treated as an accepted compatibility decision.

The pinned archive contains 242,748 features in EPSG:4269. A refresh requires
explicit paths to a retained archive and its receipt through
`CENSUS_BLOCK_GROUPS_ARCHIVE_PATH` and `CENSUS_BLOCK_GROUPS_RECEIPT_PATH`; the
registered schedule is stopped. The candidate and its catalog quality check do
not approve source authority, baseline continuity, rights, schema/ID contracts,
coverage, freshness, or release. Output is limited to the verified native
GeoParquet object until those decisions are made.

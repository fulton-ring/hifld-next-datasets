# Generalized Units — unapproved working draft

Source: FWS official CBRS maps-and-data page links the exact USGS CBRS Mapper
MapServer `https://gis1.wim.usgs.gov/server/rest/services/CBRSMapper/CoastalBarrierResourcesSystem/MapServer`
via ArcGIS item `a60c25b4b2e44969b3164742901058a2` (owner
`richard_easterbrook@fws.gov_fws`). Draft captures layer 4 `Generalized Units`
only: 942 native NAD83 (EPSG:4269) Polygon/MultiPolygon units (592 System Unit,
350 Otherwise Protected Area), nine source attributes plus OBJECTID, declared
`esriFieldTypeSingle` acreage fields serialized by the endpoint as integral JSON
numbers (adapted to float64 per the declared type; exact producer bytes retained
in raw captures). Esri clockwise-shell/counter-clockwise-hole rings are
classified by signed area into smallest containing shells without repair;
reconstruction yields 0 invalid geometries.

Registered ingest asset `ingest/generalized-units-1/generalized-units-1`
(`fws_generalized_units_draft_refresh_job`; monthly schedule STOPPED). Set the
op `capture_dir` to a complete original eight-capture directory
(`fws_data`, `official_item`, `layer`, `count`, `ids`, `page0000`,
`count_after`, `ids_after` bodies + receipts); local staging/Dagster storage
only. Default focused checks:
`PYTHONPATH=.:src:tests python -m unittest test_generalized_units_source`.
Complete real checks additionally require
`HIFLD_GENERALIZED_UNITS_TEST_CAPTURE_DIR=/path/to/preserved/captures`.

Real verified run 2026-10-21, page SHA256
`19869fa685f62e9fee7a606138485ce602f0ece578431489aa56aa2b5f3298ea` (19,137,445
bytes), Dagster runs `6bee41c5-8977-40b5-b006-db59290fc25c` (build) and
`b29a7893-76a1-47a1-80df-110c534c8cd5` (cached replay reuse), isolated staging
under `/tmp/genunits-stage`. Candidate version
`v0.0.0-draft-generalizedunits-29a7a8fadd72de3f-…`; 32 candidate objects
hash-verified; registered catalog metadata written (942 features, Mixed
Polygon/MultiPolygon, 0 invalid, 10 columns).

Unresolved (blockers, requires maintainer decision): DHS crosswalk match
(https://doi.org/10.3886/E241367V1 is Cloudflare-blocked for anonymous access),
historical HIFLD baseline equivalence (DataLumos V1 page is also
Cloudflare-blocked for anonymous fetch), producer release date (the mapper
page's "last modified December 19, 2024" note is not a feed release date),
rights acceptance and publication approval. Draft status only; never a legal
CBRS in/out determination.

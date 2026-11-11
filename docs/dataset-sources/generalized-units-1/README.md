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

Complete lifecycle re-verified 2026-11-11 at head `5860cf1a` in a retained
factory task sandbox, after the shared comparator end guard
(`src/dagster_hifld/validation.py`) was made read-time-insensitive: inputs on
filesystems that update atime/ctime on read previously failed closed the
full-record fidelity gate (`geopackage_complete_fidelity_failure`) despite
byte-identical data. Page re-captured byte-identical to the pin (same SHA256,
19,137,445 bytes). Dagster runs (isolated `DAGSTER_HOME`/staging):
live-fetch ingests `1dfed889-95ce-4365-8742-7e0442c082ff` and
`6799a56b-1ca2-4659-b0a0-fdb8799b0c62` (each observation set stages its own
version; the pinned page held every time), capture-dir ingest
`fed26c02-d0b1-42fd-97da-e88467836324` and identical replay
`0b4acc40-b6c8-41b1-9dd8-e51ed01e5c7e` — candidate
`v0.0.0-draft-generalizedunits-1cb4e67b96a93376-aaac93cfff6b0cb3`, 32 objects
hash-verified, full-record source-fidelity comparison `pass` (942/942 rows,
zero differences); catalog `473d8d8a-595c-4a87-9b8f-bde9847a2871` with the
blocking `quality_manifest` check passed. Publication refused fail-closed:
direct exact-object promotion with a valid empty approval registry raised
`ReleaseApprovalError: missing_or_ambiguous_exact_object_approval`, and the
Dagster publish/verify run `8fe118b7-1e6b-4271-9079-bb268f3aea69` failed with
no published writes; a populated three-object synthetic published fixture and
the staged candidate were preserved byte-for-byte, and a false quality
manifest blocks the same downstream check. Raw captures, receipts, staging,
Dagster home and proof records are retained on the factory task sandbox disk
(`/workspace/evidence`).

Unresolved (blockers, requires maintainer decision): DHS crosswalk match
(https://doi.org/10.3886/E241367V1 is Cloudflare-blocked for anonymous access),
historical HIFLD baseline equivalence (DataLumos V1 page is also
Cloudflare-blocked for anonymous fetch), producer release date (the mapper
page's "last modified December 19, 2024" note is not a feed release date),
rights acceptance and publication approval. Draft status only; never a legal
CBRS in/out determination.

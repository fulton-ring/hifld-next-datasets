# State Capitols source audit

**Candidate status: draft; not approved for release.**

The HIFLD inventory row `state-capitols-2` (agency USGS) matches the official USGS National Map
Structures service. The producer endpoint is
https://carto.nationalmap.gov/arcgis/rest/services/structures/MapServer — layer 6, "State
Capitols" (a byte-identical duplicate exposure exists at layer 41; layer 6 is pinned). The
DataLumos archive (https://www.datalumos.org/datalumos/project/240632/version/V2/view) and the DHS
crosswalk DOI 10.3886/E241367V1 are historical HIFLD copies; exact equivalence is unresolved.

## Complete native capture (2026-10-21)

Live metadata resolved the layer: `esriGeometryPoint`, `maxRecordCount 2000`, service
`currentVersion 11.3`, `mapName structures`, EPSG:3857. The layer declares its horizontal CRS in
`sourceSpatialReference` (wkid 102100/latestWkid 3857); it omits `hasZ` and `hasM`. All feature
requests explicitly asked for Z and M, while the layer and page responses returned only x/y
coordinates. An anonymous complete capture observed
**55 features** (50 states plus AS, GU, MP, PR, VI; no DC), unique OBJECTID 6638576–6873433, ID
digest `f760211171a6ee10347ae7a6e612a6d42a19c104217fbdf959223abcc8963770`, record digest
`fe5b62591cf0364d4f9b4c5fc42ac9d07f209178c52432a1ca4dd7ac230e2322`, geometry digest
`f8351597c3cac78a4c6456eaee5fbc4034eb4a9999764cb9061875fcb239a0d3`. Declared fields are
OBJECTID, SHAPE, PERMANENT_IDENTIFIER, SOURCE_FEATUREID, SOURCE_DATASETID, SOURCE_DATADESC,
SOURCE_ORIGINATOR, LOADDATE (epoch ms), FTYPE, FCODE, NAME, POINTLOCATIONTYPE, ADMINTYPE,
ADDRESSBUILDINGNAME, ADDRESS, CITY, STATE, ZIPCODE, GNIS_ID, GLOBALID. The JSON query response
lowercases attribute keys; the transform restores the declared names without changing values.
`source_featureid`, `admintype`, and `addressbuildingname` are null on all 55 rows. One Alaska row
(ORNL TNM Corps update, loaddate 2014-03-17) differs from the uniform 2012-12-21 USGS load; the
record digest pins both. The service declares no data-release date; LOADDATE is a per-feature load
time, not an edition date.

## Draft pipeline

`src/dagster_hifld/sources/state_capitols.py` captures service/layer metadata, count, and the
complete ID roster before and after paging by explicit object IDs, retaining original wire bytes,
decoded bodies, and hash-pinned receipts for every request. It refuses to stage on any drift:
service identity, declared schema, response schema/aliases, ID roster, per-page population, value
types, null counts, point bounds, state roster, and pinned record/geometry/schema digests all fail
closed. Incomplete captures are quarantined. Output is a typed GeoParquet candidate (native
EPSG:3857 WKB points, declared field names/types, OBJECTID/GLOBALID/PERMANENT_IDENTIFIER/GNIS_ID
retained). Content-derived versioning plus the first-construction witness bind code, source bytes,
and output objects; cache reuse verifies retained receipts and candidate bytes. The per-feature
LOADDATE values are source attributes, not an edition date. `publish.verify` enforces exact
independent approval before any publication.
The refresh job `state_capitols_draft_refresh` and its schedule are defined and stopped.

## Local materialization and release refusal

Isolated no-Git Dagster evidence is recorded in [`local-materialization.json`](local-materialization.json)
and retained under `/workspace/work/613-evidence`. The original failed-validation manifest and
its complete request receipts were preserved. After correcting the CRS-field check, the pipeline
replayed those same receipts without another HTTP capture. Registered source run
`f1b64d4e-959c-46ca-acbd-03aafdaad178` staged all 55 features; catalog run
`c7ae8632-f161-4bea-b854-28df03932b6b` passed full-population quality; post-catalog cache run
`483f1f91-3e71-423e-b812-2add4beba425` reused the candidate. Native parity compared all 19 source
attribute fields and ordered object IDs, and confirmed each output WKB point matches the source
XY without coordinate transformation.

The registered false-quality graph (`d0c05432-7f6a-40bd-837c-d8c2e57f7df6`) stopped at the
quality check before verification or promotion. With a schema-valid empty approval registry,
quality passed but verification refused the exact missing approval and promotion did not start
(`c8a92915-0c3d-4439-8e38-dc64984fefb7`). Both runs preserved the populated synthetic published
fixture. Candidate readiness and release flags remain false; no production publication occurred.

## Gates

Schema/ID contract, HIFLD archive equivalence, baseline, coverage/source-vintage, rights,
attribution, and release approval remain maintainer decisions. The schedule remains stopped and
no production data is published.

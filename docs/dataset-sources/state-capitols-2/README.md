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
`currentVersion 11.3`, `mapName structures`, EPSG:3857. An anonymous complete capture observed
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
and output objects; a same-content recapture verifies against the sealed custody and reuses the
original receipts. `publish.verify` enforces exact independent approval before any publication.
The refresh job `state_capitols_draft_refresh` and its schedule are defined and stopped.

## Local materialization and release refusal

Isolated local Dagster evidence is recorded in [`local-materialization.json`](local-materialization.json).
Source run `56a0a316-bb53-48ee-ab16-302fe555df73` completed a fresh complete capture (55 features,
`reused: false`); repeat run `af9da180-6926-4e85-aa6f-c3c23668cbd9` re-captured and reused the
sealed first construction (`reused: true`, same version); registered `publish_catalog` run
`8b747b53-62f9-42b9-a6d5-1068bb3a4937` passed full-row quality (55 rows, Point, native EPSG:3857,
zero null/empty/invalid geometries). The exact `publish.verify` run
`b3ee6151-9dda-402e-8a88-9681daaa8de3` refused release with
`ReleaseApprovalError: missing_independent_release_approval_registry`, and a populated
published-inventory sentinel was byte-identical before and after the refusal. No publication
occurred.

## Gates

Schema/ID contract, HIFLD archive equivalence, baseline, coverage/source-vintage, rights,
attribution, and release approval remain maintainer decisions. No production data is published.
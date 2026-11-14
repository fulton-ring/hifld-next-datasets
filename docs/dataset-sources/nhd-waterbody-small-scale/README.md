# nhd-waterbody-small-scale — Waterbody - Small Scale (retired USGS NHD medium-resolution layer 10)

HIFLD Open inventory entry `nhd/waterbody-small-scale-2` ("Waterbody - Small
Scale", agency US Geological Survey, issued 2024-03-20; the inventory's
"modified 2025-07-03" is the rescue-archive edit date, not a data release).
Researched 2026-11-13. No pipeline is implemented on this dataset: the only
exact-matching producer publication is the retired, frozen USGS National
Hydrography Dataset (NHD) service layer, and no currently maintained agency
product can recreate the medium-resolution waterbody layer. This note records
the checked evidence for the project's "no maintained source: park or retire"
decision, per the sibling precedent on issue #216 (`nhd-area-small-scale`,
draft PR #806).

Sibling entries, for disambiguation (same USGS service roster, different
layers): `nhd/waterbody` = the maintained 3DHP Waterbody layer (issue #229);
`nhd/waterbody-large-scale-2` = legacy NHD high-resolution layer 12
(issue #230); `nhd/waterbody-small-scale-hi-pr-vi-pacific-territories-2` =
legacy NHD layer 11 (issue #232). None of those is in scope here.

## Crosswalk and source match

- The official DHS "HIFLD Open Crosswalk Spreadsheet" GeoPlatform item
  `41b4ff18bb094ba996dec75916de3bf3` returns `CONT_0001` "Item does not exist"
  (checked 2026-11-13), and DOI 10.3886/E241367V1 resolves only to
  DataLumos/ICPSR hosts the source policy excludes from fetching, so no
  crosswalk byte was re-fetched for this note. The row content is taken from
  the sha256-verified public copies already documented on this repository
  (byte copy sha256 `7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc`,
  cited on #255 and on PR #806): the DHS crosswalk consolidates the NHD-family
  rows into one "3D Hydrography Program (3DHP) Layers" row (Open ID
  `650c07e542fa43b6a327c7f683681712`, status Deprecate) whose external landing
  is the USGS `usgs_3dhp_all` FeatureServer.
- The exact-matching producer publication of this inventory entry is layer 10
  "Waterbody - Small Scale" of USGS's own NHD map service
  (`https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer`): the
  HIFLD Open NHD collection carried this service's layer names verbatim, the
  inventory title equals the layer name, and the inventory description is the
  service description verbatim plus the added note "Please use 3DHP for
  updated data ... Data Refreshed January, 2024." The inventory keyword list
  equals the service `documentInfo` keywords. Layer 10 is the
  medium-resolution (`RESOLUTION` = `Medium`) 1:100,000-scale NHDWaterbody
  with NHDPlus-style `COMID` identifiers; the territories variant is layer 11
  (issue #232) and the high-resolution variant is layer 12 (issue #230).
- The DataLumos project 240402 copy named by the issue is the Data Rescue
  Project's HIFLD Open rescue copy — identification and comparison context
  only, never a pipeline input. No mirror, archive or third-party host was or
  will be fetched as a source.

## Dated anonymous extraction evidence (2026-11-13, producer REST endpoints)

- Feature count: **448,512** (`layer/10/query?where=1=1&returnCountOnly=true`).
- `RESOLUTION` = `Medium` for all 448,512 rows (group-by count) — exactly the
  1:100,000-scale medium-resolution waterbody layer; no high-resolution or
  local-resolution features.
- `FTYPE` breakdown: LakePond 373,871; SwampMarsh 66,506; Reservoir 5,226;
  Playa 1,684; Ice Mass 1,220; Estuary 4; Inundation Area 1 (sums 448,512).
- Newest `FDATE` on the layer is **2011-02-24** (ordered descending,
  `resultRecordCount=3`) — no feature-level edit in roughly 15 years; the
  layer content is frozen.
- Schema (17 fields, as served): `OBJECTID` (OID), `Shape` (geometry),
  `FDATE` (date), `GNIS_ID` (str 10), `GNIS_NAME` (str 65), `AREASQKM`
  (double), `ELEVATION` (double), `REACHCODE` (str 14), `FCODE` (int),
  `Shape_Length` / `Shape_Area` (double), `COMID` (int), `RESOLUTION`
  (str 7), `FTYPE` (str 24), `ONOFFNET` (smallint), `PurpCode` (str 2),
  `PurpDesc` (str 254).
- Transfer limits: `maxRecordCount` 2000 with JSON/geoJSON/PBF query formats,
  so a complete extraction needs at least 225 paged requests;
  `esriGeometryPolygon` in the service's Web Mercator CRS (wkid 102100 /
  3857); CONUS-only extent (≈24.57–49.41°N, 124.71–67.03°W; Alaska, HI, PR,
  VI and the Pacific territories live in the sibling layers).

## Retirement and no-maintained-source finding (evidence dated 2026-11-13)

- USGS states the NHD was retired on 2023-10-01: "NHD data will continue to
  be available, but no longer maintained. The most current data will be
  available through the 3D Hydrography Program (3DHP)."
  (https://www.usgs.gov/national-hydrography/national-hydrography-dataset)
- USGS's 3DHP access page states that in 2024 USGS shifted from maintaining
  the NHD, WBD and NHDPlus High-Resolution to 3DHP production and that these
  legacy products are "no longer updated and maintained", kept only as
  reference services/downloads
  (https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products).
- The service root still carries the copyright stamp "USGS TNM – National
  Hydrography Dataset.  Data Refreshed October, 2026." — a service
  re-publication stamp, not a data update: the newest feature `FDATE` on the
  layer is 2011-02-24. Republishing retired data does not make it maintained.

## Maintained products cannot recreate this layer

- **3DHP Waterbody (issue #229, maintained successor):** the crosswalk-directed
  `3DHP_all` service (`3dhp.nationalmap.gov` ... `usgs_3dhp_all`,
  `hydro.nationalmap.gov/.../3DHP_all` alias; service copyright "Data
  refreshed September 4, 2026") publishes a Waterbody layer (id 60) with
  7,158,943 features in the 3DHP data model (`id3dhp`, `featuredate`,
  `mainstemid`, `gnisid`, `featuretype`, `workunitid`). Per USGS's own
  crosswalk tables (NHD→3DHP), 3DHP waterbodies are lake/reservoir/river
  polygons built from elevation-derived hydrography where collected, with NHD
  content only as a temporary supplement that EDH replaces over time. It is
  the successor of the high-resolution waterbody theme, not a
  medium-resolution generalization, and cannot recreate the 448,512-row
  1:100,000 `COMID`/`REACHCODE` layer.
- **EPA NHDPlus V2 (medium-resolution custodian):** USGS's "Legacy NHD
  (1:100,000)" section names EPA's NHDPlus Version 2 suite as the
  medium-resolution custodian. NHDPlus V2 is a 2012 product built from 2012
  static snapshots; the USGS Water Mission Area catalog entry for NHDPlus v2
  (https://water.usgs.gov/catalog/datasets/8a60b6b4-d785-4265-af99-cd1870ea7928/)
  classifies it "Update Type: Static" with no refresh frequency or temporal
  coverage, and EPA's NHDPlus versioning page describes only occasional
  error-fix component updates. A 2012 snapshot is itself a historical
  one-time release and cannot recreate the final NHD medium-resolution
  publication this inventory entry carried.
- **NHDPlus High Resolution** is 1:24,000-scale only — not a substitute.
- **Precedent:** the sibling medium-resolution entry #216 ("Area - Small
  Scale", NHD service layer 7) was investigated on 2026-11-13 with the same
  checks and closed as not planned under the same policy ("no maintained
  source: park or retire"), with its draft PR #806 retained as decision
  evidence.

## Decision required (maintainer)

Per the project source policy this dataset needs one of:

1. **Park or retire** `nhd/waterbody-small-scale-2` (the policy default given
   the evidence above), or
2. An explicit maintainer decision to rebuild from the retired-but-published
   USGS NHD legacy publication (frozen service layer 10, or the equivalent
   final national NHD file geodatabase download) as a static historical
   release — accepting that it can never refresh, and pinning freshness and
   baseline equivalence to the January 2024 HIFLD capture from that frozen
   release.

No source, schema/ID contract, baseline, rights or release is approved by
this note; no pipeline was built on the retired release; no data was
published.

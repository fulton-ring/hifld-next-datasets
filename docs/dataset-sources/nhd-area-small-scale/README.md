# Area - Small Scale (USGS NHD)

Inventory `nhd/area-small-scale-2`, agency U.S. Geological Survey (USGS). This draft captures exactly
layer 7 "Area - Small Scale" of USGS's own National Hydrography Dataset (NHD) map service; no mirror,
archive or third-party host is fetched as a pipeline input.

## Producer, publication and source relationship

- The HIFLD Open NHD collection carried the layer names of USGS's NHD service verbatim ("Area - Small
  Scale" is exactly service layer 7), and the layer's content matches the HIFLD entry: medium-resolution
  ("`RESOLUTION` = `Medium`") 1:100,000-scale NHD area polygons with NHDPlus-style `COMID` identifiers.
  The territories variant is service layer 8 (inventory
  `nhd/area-small-scale-hi-pr-vi-pacific-territories-2`, tracked as a separate issue).
- The producing agency is USGS; the service is served from USGS's own The National Map host
  (`hydro.nationalmap.gov`). The DHS HIFLD deprecation crosswalk (public copy of DOI 10.3886/E241367V1,
  `hifld_open_crosswalk_geoplatform.xlsx`, sha256 `7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc`,
  row "3D Hydrography Program (3DHP) Layers") marks the hydrography collection "Deprecate" and directs
  users to the USGS `usgs_3dhp_all` FeatureServer.

## Retirement, successor checks and no-maintained-source finding (evidence dated 2026-11-12/13)

- USGS states the NHD was retired on 2023-10-01 ("no longer maintained") and that USGS now produces the
  3D Hydrography Program; the retired NHD remains "available, but no longer maintained"
  (https://www.usgs.gov/national-hydrography/national-hydrography-dataset,
  https://www.usgs.gov/national-hydrography/access-national-hydrography-products — captured as
  `product_doc` evidence). The agency still republishes the final NHD as reference products (services
  refreshed quarterly, national download regenerated monthly, captured service JSON "Data Refreshed
  October, 2026"), but a refresh/repackaging of retired data does not make the underlying data
  maintained.
- Medium-resolution maintenance path (checked 2026-11-13): USGS's NHD page ("Legacy NHD (1:100,000)")
  names EPA's NHDPlus Version 2 suite as the medium-resolution custodian ("the US EPA manages the
  maintenance and distribution of NHD Medium Resolution as part of the NHDPlus Version 2 suite of
  products", https://www.usgs.gov/national-hydrography/national-hydrography-dataset). Investigated:
  NHDPlus V2 is a 2012 product built from 2012 snapshots of NHD Medium Resolution, NED and WBD (USGS
  "Abridged History of Hydrography Datasets"); the USGS Water Mission Area catalog entry for
  "NHDPlus v2" (https://water.usgs.gov/catalog/datasets/8a60b6b4-d785-4265-af99-cd1870ea7928/)
  classifies it `Update Type: Static` with no temporal coverage or refresh frequency, and EPA's
  NHDPlus versioning page (https://www.epa.gov/waterdata/nhdplus-versioning) describes only occasional
  error-fix component updates, not ongoing NHD stewardship. A 2012 NHD Medium Resolution snapshot
  cannot recreate the final NHD medium-resolution layer this inventory entry carried (which includes
  stewardship updates through the 2023-10-01 retirement), so NHDPlus V2 is itself a historical
  one-time release, not a maintained reconstructible source.
- High-resolution alternatives are not substitutes: NHDPlus High Resolution is 1:24,000-scale, and
  USGS's 3DHP page states that in 2024 USGS shifted from maintaining the NHD, WBD and NHDPlus HR to
  3DHP production, with these legacy products "no longer updated and maintained"
  (https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products, checked 2026-11-13). The
  crosswalk-directed 3DHP_all service
  (`https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all/FeatureServer`) publishes
  HydroLocation, Flowline, Waterbody, Catchment and Spatial Metadata layers only — no "Area" layer and
  no medium-resolution (1:100,000) generalized product.
- No maintained source: no currently maintained agency product can recreate the medium-resolution
  (1:100,000) NHD area layer. Under the project source policy this dataset therefore routes to the
  "no maintained source: park or retire" decision (needs_decision) rather than pending release
  acceptance. The retired-but-published USGS NHD reference service remains the only exact-matching
  agency publication; this draft adapter against it is retained as evidence for that decision, with
  the refresh schedule stopped and every approval flag false.
- The old DHS/HIFLD-hosted ArcGIS items and the DataLumos project 239954 archive copy are identification
  and comparison context only; the DataLumos archive modification date (2025-07-03) is not a source
  release date.

## Fetched inputs and who publishes them

- `https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer` (root `?f=pjson`, layer 7 `?f=pjson`,
  `/query` count/ids/page requests) — USGS The National Map hydrography service. Layer 7 "Area - Small
  Scale" is an `esriGeometryPolygon` feature layer, native service spatial reference Web Mercator
  (wkid 102100 / EPSG:3857), `maxRecordCount` 2000 with `supportsPagination`. Observed snapshot
  (2026-11-12): 17,020 features, `OBJECTID` roster exactly 1..17020, sorted-roster capture tiled as
  segments of the observed working width (the producer refused 2000-record native-SR segments with
  transient complete-200 error documents while <=1000-record segments served).
- `https://www.usgs.gov/news/technical-announcement/nhd-and-wbd-map-services-moving-cloud` — USGS's own
  technical announcement documenting the exact NHD service endpoint (captured as `service_doc`).
- `https://www.usgs.gov/national-hydrography/access-national-hydrography-products` — USGS product page
  documenting NHD retirement (2023-10-01), the reference republication, and download cadence (captured as
  `product_doc`).

## Capture contract

The adapter pins the before/after OBJECTID rosters, declared field names/types/aliases/lengths, the
declared layer extent, the service layer roster, the observed enumeration value sets (`RESOLUTION`,
`FTYPE`, `FCODE`, `ONOFFNET`, `PurpCode`), `GNIS_ID` value lengths, `COMID` uniqueness, and the
complete-record/geometry digests, and fails closed on any drift, unexpected column, Z/M ordinate, or
incomplete page tiling. Producer rings are kept verbatim (including their Web Mercator coordinates);
null, string-"NaN", or unassemblable rings are retained with null candidate geometry and gated in the
audit, never repaired. The candidate remains an unapproved draft: with no maintained source available
this dataset is routed to the "no maintained source: park or retire" decision, the refresh schedule is
stopped, and source acceptance, baseline equivalence, schema/ID/coverage contracts, rights and release
all stay false.

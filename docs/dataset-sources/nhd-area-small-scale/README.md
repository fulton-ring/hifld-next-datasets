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

## Retirement, successor check and freshness (evidence dated 2026-11-12)

- USGS states the NHD was retired on 2023-10-01 ("no longer maintained") and that USGS now produces the
  3D Hydrography Program; the retired NHD remains "available, but no longer maintained"
  (https://www.usgs.gov/national-hydrography/national-hydrography-dataset,
  https://www.usgs.gov/national-hydrography/access-national-hydrography-products — captured as
  `product_doc` evidence).
- The agency still republishes the final NHD as maintained reference products: the USGS services
  documentation describes the NHD dynamic service as refreshed quarterly, the NHD national download is
  regenerated monthly, and the captured service JSON declares "Data Refreshed October, 2026". No
  per-feature or per-snapshot release date is published; the medium-resolution layer has been static
  since stewardship ended.
- Successor check: the crosswalk-directed 3DHP_all service
  (`https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all/FeatureServer`) publishes
  HydroLocation, Flowline, Waterbody, Catchment and Spatial Metadata layers only — no "Area" layer and
  no medium-resolution (1:100,000) generalized product — so the current 3DHP publication cannot recreate
  this layer. The retired-but-published USGS NHD service is therefore the exact-matching agency
  publication; accepting a retired dataset's reference publication (and any future 3DHP successor
  migration) remains a maintainer decision.
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
audit, never repaired. The candidate is an unapproved faithful draft: source acceptance of a retired
dataset's reference publication, baseline equivalence with the archived HIFLD copy, schema/ID/coverage
contracts, rights, and release all remain pending acceptance, and the refresh schedule is stopped.

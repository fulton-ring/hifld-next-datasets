# Flowline - Small Scale (HI, PR, VI, Pacific Territories) (USGS NHD)

Inventory `nhd/flowline-small-scale-hi-pr-vi-pacific-territories-2`, agency U.S. Geological Survey
(USGS). This draft captures exactly layer 5 "Flowline - Small Scale (HI, PR, VI, Pacific
Territories)" of USGS's own National Hydrography Dataset (NHD) map service; no mirror, archive or
third-party host is fetched as a pipeline input.

## Producer, publication and source relationship

- The HIFLD Open NHD collection carried the layer names of USGS's NHD service verbatim
  ("Flowline - Small Scale (HI, PR, VI, Pacific Territories)" is exactly service layer 5), and the
  layer's content matches the HIFLD entry: the flowline features the NHDPlus-based small-scale
  product ships for Hawaii, Puerto Rico, the U.S. Virgin Islands and the Pacific territories, with
  NHDPlus-style `ComID` identifiers and NHD flowline attributes (`Permanent_`, `FDate`, `ReachCode`,
  `GNIS_ID`/`GNIS_Name`, `FTYPE`/`FCode`, stream-order/hydrosequence fields). Unlike the
  conterminous-United States small-scale layers (`RESOLUTION` = `Medium`), this territories variant
  serves `RESOLUTION` = `High` on every row (the territories component of the small-scale product is
  built on the high-resolution NHD). The conterminous-United States sibling is service layer 4
  (inventory `nhd/flowline-small-scale-2`); the conterminous-United States "Area - Small Scale"
  sibling (layer 7) is implemented separately.
- The producing agency is USGS; the service is served from USGS's own The National Map host
  (`hydro.nationalmap.gov`). The DHS HIFLD deprecation crosswalk (public copy of DOI 10.3886/E241367V1,
  `hifld_open_crosswalk_geoplatform.xlsx`, sha256 `7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc`,
  row "3D Hydrography Program (3DHP) Layers") marks the hydrography collection "Deprecate" and directs
  users to the USGS `usgs_3dhp_all` FeatureServer.

## Retirement, successor check and freshness (evidence dated 2026-11-13)

- USGS states the NHD was retired on 2023-10-01 ("no longer maintained") and that USGS now produces
  the 3D Hydrography Program; the retired NHD remains "available, but no longer maintained"
  (https://www.usgs.gov/national-hydrography/national-hydrography-dataset,
  https://www.usgs.gov/national-hydrography/access-national-hydrography-products — captured as
  `product_doc` evidence).
- The agency still republishes the final NHD as maintained reference products: the USGS services
  documentation describes the NHD dynamic service as refreshed quarterly, the NHD national download is
  regenerated monthly, and the captured service JSON declares "Data Refreshed October, 2026". No
  per-feature or per-snapshot release date is published; the territories small-scale component has
  been static since stewardship ended.
- Successor check: the crosswalk-directed 3DHP_all service
  (`https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all/FeatureServer`, captured
  2026-11-13, copyright "Data refreshed September 4, 2026") publishes HydroLocation, Flowline,
  Waterbody, Catchment and Spatial Metadata layers only — no 1:100,000-scale small-scale product and
  no HI/PR/VI/Pacific-territories variant — so the current 3DHP publication cannot recreate this
  layer. The retired-but-published USGS NHD service is therefore the exact-matching agency
  publication; accepting a retired dataset's reference publication (and any future 3DHP successor
  migration) remains a maintainer decision.
- The old DHS/HIFLD-hosted ArcGIS items and the DataLumos project 240276 archive copy are
  identification and comparison context only; the DataLumos archive modification date (2025-07-03)
  is not a source release date.

## Fetched inputs and who publishes them

- `https://hydro.nationalmap.gov/arcgis/rest/services/nhd/MapServer` (root `?f=pjson`, layer 5
  `?f=pjson`, `/query` count/ids/page requests) — USGS The National Map hydrography service. Layer 5
  "Flowline - Small Scale (HI, PR, VI, Pacific Territories)" is an `esriGeometryPolyline` feature
  layer, native service spatial reference Web Mercator (wkid 102100 / EPSG:3857), `maxRecordCount`
  2000 with `supportsPagination`; the layer metadata declares Z/M-enabled geometry while the served
  ESRI JSON vertices are plain 2-value [x, y] ordinates. Observed snapshot (2026-11-13): 33,273
  features, `OBJECTID` roster exactly 1..33273, sorted-roster capture tiled as segments of the
  observed working width (2000-record native-SR segments served during probing), with adaptive
  bisection if the producer's transient transfer cap reappears.
- `https://www.usgs.gov/news/technical-announcement/nhd-and-wbd-map-services-moving-cloud` — USGS's
  own technical announcement documenting the exact NHD service endpoint (captured as `service_doc`).
- `https://www.usgs.gov/national-hydrography/access-national-hydrography-products` — USGS product
  page documenting NHD retirement (2023-10-01), the reference republication, and download cadence
  (captured as `product_doc`).

## Capture contract

The adapter pins the before/after OBJECTID rosters, declared field names/types/aliases/lengths, the
declared layer extent, the service layer roster, the observed enumeration value sets (`Resolution`,
`FlowDir`, `FTYPE`, `FCode`), `GNIS_ID` value lengths, `ComID` uniqueness (unique in this layer) and
`Permanent_` duplication, and the complete-record/geometry digests, and fails closed on any drift,
unexpected column, served Z or M ordinate, or incomplete page tiling. Producer paths are kept
verbatim (including their Web Mercator coordinates); each path becomes one LineString part in
producer order and multi-path features become MultiLineString; null, string-"NaN", or structurally
degenerate paths are retained with null candidate geometry and gated in the audit, never repaired.
The candidate is an unapproved faithful draft: source acceptance of a retired dataset's reference
publication, baseline equivalence with the archived HIFLD copy, schema/ID/coverage contracts,
rights, and release all remain pending acceptance, and the refresh schedule is stopped.

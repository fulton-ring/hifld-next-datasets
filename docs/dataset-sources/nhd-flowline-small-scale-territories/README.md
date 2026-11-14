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

## Retirement, successor checks and no-maintained-source finding (evidence dated 2026-11-13)

- USGS states the NHD was retired on 2023-10-01 ("no longer maintained") and that USGS now produces
  the 3D Hydrography Program; the retired NHD remains "available, but no longer maintained"
  (https://www.usgs.gov/national-hydrography/national-hydrography-dataset,
  https://www.usgs.gov/national-hydrography/access-national-hydrography-products — captured as
  `product_doc` evidence). The agency still republishes the final NHD as reference products (services
  refreshed quarterly, national download regenerated monthly, captured service JSON "Data Refreshed
  October, 2026"), but a refresh/repackaging of retired data does not make the underlying data
  maintained.
- Maintenance status of the underlying data (checked 2026-11-13): USGS's 3DHP access page states that
  "In 2024, the USGS shifted from maintaining the National Hydrography Dataset (NHD), the Watershed
  Boundary Dataset (WBD), and the NHDPlus High-Resolution to production of 3D Hydrography Program
  products. While these legacy products are no longer updated and maintained, they are provided here
  as both downloadable products and as services for reference purposes"
  (https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products). This layer is the
  territories component of the small-scale product built on the high-resolution NHD (served rows
  carry `RESOLUTION` = `High`), so its underlying data is exactly that legacy, unmaintained NHD.
- Medium-resolution custodianship path is not a substitute (checked 2026-11-13): USGS's NHD page names
  EPA's NHDPlus Version 2 suite as the custodian of NHD Medium Resolution, but that path covers
  1:100,000-scale conterminous-U.S. production ("medium resolution National Hydrography Dataset at
  1:100,000 scale for the conterminous U.S.", same page), so it neither serves Hawaii, Puerto Rico,
  the U.S. Virgin Islands or the Pacific territories nor matches this layer's High-resolution source
  data.
- Successor check: the crosswalk-directed 3DHP_all service
  (`https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all/FeatureServer`, checked
  2026-11-13, copyright "Data refreshed September 4, 2026") publishes only HydroLocation (layers
  20/30/40), Flowline (50), Waterbody (60), Catchment (80) and Spatial Metadata (90/92/94) layers —
  no 1:100,000-scale small-scale product and no HI/PR/VI/Pacific-territories variant — and its
  Flowline layer carries the 3DHP identity model (`id3dhp`, `mainstemid`, `flowpathid3dhp`,
  `featuretype`, `streamlevel`) without the NHDPlus-style `ComID`, `ReachCode` and `Permanent_`
  schema this dataset serves, so the current maintained 3DHP publication cannot recreate this layer.
- No maintained source: no currently maintained agency product can recreate this layer. Under the
  project source policy this dataset therefore routes to the "no maintained source: park or retire"
  decision (needs_decision) rather than pending release acceptance. The retired-but-published USGS
  NHD reference service remains the only exact-matching agency publication; this draft adapter
  against it is retained as evidence for that decision, with the refresh schedule stopped and every
  approval flag false.
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
The candidate remains an unapproved draft: with no maintained source available this dataset is routed
to the "no maintained source: park or retire" decision, the refresh schedule is stopped, and source
acceptance, baseline equivalence with the archived HIFLD copy, schema/ID/coverage contracts, rights
and release all stay false.

# USCG Captain of the Port Zones: source blocked

Status: no source adapter or refresh pipeline is implemented; source authority and delegation remain unresolved. This record consolidates the 2026-10-19 evidence (superseded draft PR #479) with the 2026-11-15 re-verification on this issue.

The inventory entry (`us-coast-guard-uscg-captain-of-the-port-zones`, issue #360) identifies USCG Operations Systems Center (OSC) as publisher and describes 41 nationwide Captain of the Port zones developed by "USCG - OSC Enterprise GIS" based on Title 33, Code of Federal Regulations Part 3, with an inventory time period of 2014-04-09. The regulation is the legal boundary authority, but it does not itself supply a maintained spatial production feed.

## Producer enumeration (2026-11-15)

The producing agency's own ArcGIS Online organizations were enumerated in full this time (the 2026-10-19 record checked only a group):

- **USCG FSLTP Portal**, org `Fr5UUTVwNRJlXAkf`, urlKey `uscoastguard` (hosted under `services9.arcgis.com/Fr5UUTVwNRJlXAkf`; owner accounts `USCG_GIS_Data`, `Ian.S.Hanna_uscoastguard`, `AAdams_USCG`, `TBoone_USCG`, `SWagner_USCG`, `AAdams_USCG`, etc.):
  - Org-scoped item search for `cotp` OR `"captain of the port"` returned 5 items — Port Status web map, a port-status dashboard, an MSIB safety zone, and two SAR layers; no COTP zones product. 175 public items total were listed.
  - The full hosted-service directory (74 FeatureServers, fetched anonymously 2026-11-15) contains no COTP/captain service. The only maintained USCG boundary products there are `USCG_Sectors_view` (36 sector polygons; layer `dataLastEditDate` 2026-01-30, item modified 2026-10-08 — actively maintained), `Regional_Response_Team_Boundary`, and local VTS/PARS layers.
  - The NAVCEN "Port Status - Public" web map (`62edf98af2f845f3bf3d3bf834c9f5cb`) references `USCG_Sectors_view`, port-status views, and notice layers — no COTP zone polygons.
- **USCG public org** `6ldl6K67FkYzPtEE` (hosted under `services8.arcgis.com/6ldl6K67FkYzPtEE`; owner accounts `AAdams_USCGPublic`, `JHsieh_USCGPublic`, `FCantu_USCGPublic`, `ihanna_uscgpublic`, `RMiller_public_uscg`): 84 public items and the full 43-service directory contain no COTP zones service (vessel statistics, casualties, spills, boating-safety layers only).
- The [US Coast Guard Enterprise GIS group](https://www.arcgis.com/home/group.html?id=befd4332cf43489499661606dd1267d6) (re-checked 2026-11-15) still holds exactly 10 items, all 2011–2016-era Explorer web-app bookmarks, none a COTP zones layer — confirming the 2026-10-19 finding.
- A global ArcGIS Online item search for `"captain of the port"` OR `cotp` (25 results, fetched 2026-11-15) shows every public COTP-zones feature service is owned by a non-USCG account: `sarah.idczak_DEQ` (Oregon DEQ, Oregon subset), `TxGLOGIS` (Texas GLO, Gulf subset), `danielgao` (personal), `clibby007` (personal, "543rd Engineer Detachment GPC" org), `adecadmin` (Alaska DEC re-upload of the 2017 HIFLD item). None is a USCG publication or documented delegation.

## USCG's in-flight replacement explicitly excludes this product

USCG Navigation Center solicitation `70Z02326QNAV00002`, "Captain of the Port (COTP) Boundary Geospatial Dataset Production Support Services" (posted 2026-09-15, quotations due 2026-09-22, six-month performance period), has NAVCEN contracting for a polygon dataset of **approximately 525 ports and waterways** from the Port Status application inventory. Its Performance Work Statement states the requirement "does not establish, amend, or replace legal COTP jurisdictional boundaries." Even after award and delivery, that product would not recreate the 41-zone COTP jurisdictional release this issue tracks. No NAVCEN COTP/port polygon publication exists yet (checked 2026-11-15; `navcen.uscg.gov` blocks anonymous fetching, the NAVCEN apps are the AGOL products enumerated above).

## Historical/archive copies (not maintained, and never pipeline inputs)

- The closest complete ArcGIS candidate remains [item `ea4ca1379e534a84a6cd90e897849120`](https://www.arcgis.com/home/item.html?id=ea4ca1379e534a84a6cd90e897849120): 41 polygons, OBJECTIDs 1–41, EPSG:4326, 14 fields (`OBJECTID, FID_1, ZoneName, Office, SubZone, DistrictNa, Atu, Opfac, AreaName, SHAPE_STAr, SHAPE_STLe, SHAPE_Leng, Shape__Area, Shape__Length`). Owned by personal account `clibby007`, item modified 2024-10-05, service/layer metadata last modified 2021-02-25; no USCG delegation link. Not a producer feed.
- [HIFLD ArcGIS item `c7e042e171144923b216828948de24bb`](https://www.arcgis.com/home/item.html?id=c7e042e171144923b216828948de24bb) is owned by Alaska DEC's `adecadmin`, last modified 2017-11-22; its referenced feature endpoint returned an ArcGIS "Invalid URL" payload (2026-10-19 check).
- The HIFLD Open portal's own copy, [item `729d008770244f96b12fa5f6f339e62e`](https://www.arcgis.com/home/item.html?id=729d008770244f96b12fa5f6f339e62e) (item updated 2025-05-09 in search-engine caches), now answers `CONT_0001` "Item does not exist or is inaccessible" (checked 2026-11-15).
- DataLumos project 240941 (DOI 10.3886/E240941V1) and the PASDA HIFLD_FEMA MapServer layer 201 are banned-host archive copies; identification context only, never fetched. The FWC/FWRI metadata page (`gis.myfwc.com`) documents the original producer baseline: "USCG - OSC Enterprise GIS Baseline Inventory Dataset 'Captain of the Port Zones source data, Geographic NAD83, USCG - OSC Enterprise GIS (2007) [cotp_zn]'", current as of 2006-03-29, maintenance "as needed" — a historical release, not a maintained feed.
- The public HIFLD Next v1.0.0 collection is reachable (41 features; DataLumos lineage; one invalid geometry reported) but supplies no maintained producer URL or delegation proof; 11,039-byte metadata capture SHA-256 `3a06ec0c814a76ef29296e21e8646a6b58e217432dc14252a490c6023e82b240`.

## Decision needed

No maintained USCG producer feed or documented USCG delegation exists for the complete 41-zone release. Under the project source policy, no archive, mirror or third-party copy may be substituted. Options require an explicit maintainer decision:

1. **Park or retire** the issue as "no maintained source" (current recommendation), or
2. Accept a materially different substitute — e.g., rebuilding from the maintained USCG `USCG_Sectors_view` product (36 sector polygons delineated per 33 CFR, actively maintained) — which changes the feature set (36 vs 41) and the schema/ID contract, or
3. Wait for NAVCEN's contracted port/waterway product, which by its own statement will not cover legal COTP jurisdictional boundaries.

Until a decision, no pipeline is built and no production publication is implied.

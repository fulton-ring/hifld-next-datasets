# US Coal Fields — USGS NCRDS CoalByRank maintained service capture

- Assigned dataset: HIFLD Open inventory `us-coal-fields-1` ("US Coal Fields"), the retired
  HIFLD Open polygon layer of the coal fields of the conterminous United States (1:5,000,000).
- Producer (only pipeline input, verified 2026-11-14): the **USGS Energy Resources Program NCRDS
  "CoalByRank" MapServer, layer 0 "CoalFieldsbyRank"** on USGS's own ArcGIS Server
  (`https://energy.usgs.gov/arcgis/rest/services/NCRDS/CoalByRank/MapServer`, ArcGIS Server
  currentVersion 12.1). This is the agency's current, maintained publication of the national GIS
  coal-fields mapping, served alongside the sibling `CoalByAge` and `CoalProvinces` services in the
  producer's `NCRDS` service directory. USGS's active *Evolving Utilization of Solid Energy Fuels
  (EUSEF)* project (https://www.usgs.gov/centers/geology-energy-and-minerals-science-center/science/evolving-utilization-solid-energy-fuels,
  page updated 2024-08-28) states under its Data Management task that "Products, including the
  National Coal Resources Data System (NCRDS), the World Coal Quality Inventory (WoCQI), and
  Geographic Information System coal fields mapping are available to all those seeking information
  on our Nation's vast energy resources."
- URLs the pipeline fetches as input (all `energy.usgs.gov`, published by the U.S. Geological
  Survey Energy Resources Program; the ArcGIS REST API publishes no per-snapshot release date):
  - `https://energy.usgs.gov/arcgis/rest/services/NCRDS?f=pjson` (producer service directory)
  - `https://energy.usgs.gov/arcgis/rest/services/NCRDS/CoalByRank/MapServer?f=pjson` (service
    document; before and after the feature pages)
  - `https://energy.usgs.gov/arcgis/rest/services/NCRDS/CoalByRank/MapServer/0?f=pjson` (layer
    document; before and after the feature pages)
  - `.../MapServer/0/query?where=1=1&returnCountOnly=true&f=json` and
    `.../query?where=1=1&returnIdsOnly=true&f=json` (completeness brackets; before and after)
  - `.../MapServer/0/query?where=1=1&outFields=*&returnGeometry=true&orderByFields=OBJECTID
    ASC&resultOffset=0&resultRecordCount=2000&f=json` (paged features; 602 features, one page at
    the producer's `maxRecordCount` 2000)
- Content origin and release date: the service description itself cites the origin publication —
  East, J.A., 2013, *Coal fields of the conterminous United States—National Coal Resource
  Assessment updated version*, USGS Open-File Report 2012-1205
  (https://pubs.usgs.gov/of/2012/1205/, first posted 2013-04-02). That report is a completed
  one-time publication (its FGDC metadata declares Progress Complete, maintenance and update
  frequency None planned) that remains permanently available but is **not maintained**. The report
  and its Downloads package are **never fetched** by this pipeline. An earlier draft of this
  dataset (head 99e592f8) captured the OFR 2012-1205 Downloads `GIS.zip` directly and labelled it
  the "current maintained USGS release"; that one-time release is retired as a pipeline input per
  the project source policy and that provenance claim is corrected: the maintained producer
  publication is the NCRDS coal-fields map service captured here.
- Fidelity of the maintained service to the origin publication (verified 2026-11-14 in an isolated
  factory task sandbox against the pinned OFR `GIS.zip`, sha256
  `e491215d0a937cae48cc5e9a2763ea43c85a4d11c183ec7c5b1ac559d2a585a5`): all 602 served records have
  AREA/PERIMETER/PROVINCE/NAME/AGE/RANK values exactly equal to the OFR `Coal_Fields` DBF rows in
  record order (OBJECTID 1..602 = source record order), and the served Web Mercator rings are
  vertex-identical to the NAD83 source after projection (602/602 identical ring structure and
  vertex counts, 298,530/298,530 vertices, maximum offset 1.95 m under the standard
  EPSG:4269→EPSG:3857 transform — the NAD83→WGS84 datum shift at
  1:5,000,000 scale). The served layer's declared extent equals its observed bounds
  ([-13844079.967, 3003294.840300001, -7920327.6513, 6275165.573600002], EPSG:3857).
- Served schema (pinned): `OBJECTID` (OID), `AREA`/`PERIMETER` (doubles), `PROVINCE` C(31),
  `NAME` C(36), `AGE` C(30), `RANK` C(50), service-generated `Shape_Length`/`Shape_Area` (doubles),
  Esri `esriGeometryPolygon` in wkid 102100 / EPSG:3857. Attribute populations are pinned:
  PROVINCE Eastern 247 / Gulf 4 / Interior 56 / Northern Great Plains 53 / Pacific Coast 70 /
  Rocky Mountain 172; AGE Cretaceous 187, Cretaceous-Jurassic 2, Mississippian 9,
  Pennsylvanian-Permian 266, Tertiary 123, Tertiary-Cretaceous 8, Triassic 7; RANK Anthracite 29,
  Lignite 57, Low Volatile Bituminous 29, Medium and High Volatile Bituminous 381, Subbituminous
  106; 110 distinct NAME values. The one source-published ring self-intersection (record 95,
  "Great Falls Field") is served unchanged by the service and is retained unchanged and pinned,
  never repaired.
- Capture flow: bounded paged capture of the URLs above with wire/body/receipt evidence,
  before/after service/layer/count/id-roster stability brackets, pinned canonical digests (count
  payload, id roster, record values, served geometry, served schema), served-rings polygon
  reconstruction without repair or reprojection (candidate GeoParquet in the served Web Mercator
  EPSG:3857), and quarantine on any drift. The producer's ArcGIS Server rotates the layer
  document's `drawingInfo.renderer.authoringInfo.colorRamp` on every request (renderer
  presentation metadata, not dataset content, observed 2026-11-14); that exact path alone is
  excluded from the before/after layer bracket, and every data-bearing layer anchor is validated
  separately. The candidate is staged under `geoparquet/` with the manifest's
  `prebuilt_output_policy`; publication stays blocked.
- Retired copies never fetched: DataLumos archive project 240310 (the inventory's Download
  Location), the deprecated `services1.arcgis.com/Hp6G80Pky0om7QvQ` HIFLD FeatureServer, GeoPlatform
  item `13ad01f317f74e5989c3ee199496fa32`, and the Esri Living Atlas "USA Coal Fields" item
  `5dffaad2b2074d0dad20e9d5f52f6641` (item owner `esri_landscape2`, not the producing agency).
  The DHS crosswalk row for "US Coal Fields" (DOI 10.3886/E241367V1 copy sha256 7f3394fe…) records
  Status "Deprecate", New Hosting N/A, justification "No longer supported as part of the HIFLD
  mission", and names the USGS Downloads directory as the external landing page. (The crosswalk
  row's Agency cell reads "Federal Judicial Center"; the inventory row and the publication are
  unambiguously U.S. Geological Survey, so the producer is recorded as USGS.)
- Local job: `us_coal_fields_draft_refresh`; its weekly schedule is STOPPED.
- Capture-backed tests run opt-in with `HIFLD_US_COAL_FIELDS_TEST_CAPTURE_ARCHIVE=<tar.gz of the
  captured raw response bodies>` (the ten `raw/<name>.body` files of one real capture).
- No source authority, schema/ID contract, baseline, coverage, rights or release acceptance is
  granted by this implementation; all acceptance flags stay false and the shared verify/promote
  gates remain unchanged.

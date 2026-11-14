# US Coal Fields — USGS OFR 2012-1205 producer publication capture

- Assigned dataset: HIFLD Open inventory `us-coal-fields-1` ("US Coal Fields"), the retired
  HIFLD Open polygon layer of the coal fields of the conterminous United States (1:5,000,000).
- Producer publication (only candidate input, byte-pinned): U.S. Geological Survey
  Open-File Report 2012-1205, *Coal Fields of the Conterminous United States: National Coal
  Resource Assessment Updated Version* (Joseph A. East), first posted April 2, 2013
  (https://doi.org/10.3133/ofr20121205). The pipeline fetches only:
  - https://pubs.usgs.gov/of/2012/1205/ (landing page; publication evidence)
  - https://pubs.usgs.gov/of/2012/1205/Downloads/ (directory index; distribution evidence)
  - https://pubs.usgs.gov/of/2012/1205/Downloads/Readme.txt (package readme)
  - https://pubs.usgs.gov/of/2012/1205/Downloads/Metadata/Coal_Fields.xml (FGDC metadata)
  - https://pubs.usgs.gov/of/2012/1205/Downloads/GIS.zip (the official 220 MB GIS package;
    the assigned dataset is its member `GIS/Updated Coal Fields/Coal_Fields.*`)
- Last data update: the publication itself (first posted 2013-04-02; FGDC pubdate 2012,
  dataset caldate 2013; GIS.zip server file date 2013-04-02). USGS Pubs Warehouse keeps the
  publication permanently available; update frequency is not planned and the 2021
  "Simplified Presentation Format" explicitly revised no data. No newer USGS successor of
  the national coal-fields layer was found (checked 2026-11-14 via the Pubs Warehouse page
  and its linked downloads).
- Retired copies never fetched: DataLumos archive project 240310 (the inventory's Download
  Location), the deprecated `services1.arcgis.com/Hp6G80Pky0om7QvQ` HIFLD FeatureServer, and
  GeoPlatform item `13ad01f317f74e5989c3ee199496fa32`. The DHS crosswalk row for
  "US Coal Fields" (DOI 10.3886/E241367V1 copy sha256 7f3394fe…) records Status "Deprecate",
  New Hosting N/A, justification "No longer supported as part of the HIFLD mission", and
  names the USGS Downloads directory as the external landing page. (The crosswalk row's
  Agency cell reads "Federal Judicial Center"; the inventory row and the publication are
  unambiguously U.S. Geological Survey, so the producer is recorded as USGS.)
- Source dataset (pinned member digests, observed 2026-11-14): ESRI polygon shapefile,
  EPSG:4269 (NAD83), 602 records, DBF fields `AREA N(12,3)`, `PERIMETER N(12,3)`,
  `PROVINCE C(31)`, `NAME C(36)`, `AGE C(30)`, `RANK C(50)`; bounds
  [-124.36347198486328, 26.034013748169002, -71.1495132446289, 49.00178527832048].
  The shapefile declares no feature-ID column; the ingest retains the original DBF/SHP
  record positions (`source_fid`, `source_shp_record_number`) and generates no substitute IDs.
  One source-published ring self-intersection (record 95, "Great Falls Field") is retained
  unchanged and pinned.
- HIFLD continuity (comparison context only, never an input): the retired HIFLD export
  published at `hifld-next-datasets-prod/hifld/us-coal-fields-1/.../v1.0.0/geojson/…` has the
  same 602 features with identical attribute values (FID = source record order) and a
  vertex-faithful NAD83→CRS84 reprojection (298,530/298,530 vertices matched, max offset
  1.7e-05 degrees), plus service-generated `Shape__Area`/`Shape__Length`.
- Capture flow: bounded HTTP capture of the five URLs above with wire/body/receipt
  evidence, pinned byte digests, pinned package roster (914 members; the full
  "GIS/Updated Coal Fields" family with size+CRC), native DBF/SHP decoding (exact base-10
  decimal values, clockwise-shell/counterclockwise-hole ring grouping without repair),
  and lossless staging of the seven exact source member bytes under `shapefile/`.
- Local job: `us_coal_fields_draft_refresh`; its weekly schedule is STOPPED.
- Capture-backed tests run opt-in with
  `HIFLD_US_COAL_FIELDS_TEST_CAPTURE_ARCHIVE=<tar.gz of raw capture bodies>`.
- No source authority, schema/ID contract, baseline, coverage, rights or release acceptance
  is granted by this implementation; all acceptance flags stay false and the shared
  verify/promote gates remain unchanged.

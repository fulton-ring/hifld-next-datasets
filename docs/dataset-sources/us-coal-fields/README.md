# US Coal Fields — parked: no maintained USGS producer publication

Assigned dataset: HIFLD Open inventory `us-coal-fields-1` ("US Coal Fields"), the retired HIFLD
Open polygon layer of the coal fields of the conterminous United States (1:5,000,000). The DHS
HIFLD Open crosswalk (DOI 10.3886/E241367V1) marks the row **Deprecate** (New Hosting N/A,
justification "No longer supported as part of the HIFLD mission") and the inventory Download
Location is the DataLumos archive project 240310 — a banned archive source that was never fetched.

## Why the dataset is parked (dated evidence, checked 2026-11-14)

Two USGS producer publications were investigated and both fail the source policy's
maintained-publication requirement:

- **The origin publication is a completed one-time release.** USGS Open-File Report 2012-1205,
  *Coal Fields of the Conterminous United States: National Coal Resource Assessment Updated
  Version* (East, J.A., https://pubs.usgs.gov/of/2012/1205/, first posted 2013-04-02). Its
  producer FGDC metadata `https://pubs.usgs.gov/of/2012/1205/Downloads/Metadata/Coal_Fields.xml`
  (43,681 bytes, SHA-256 `17299068e32ab1bffce6391fd803ce70a6dd08fea91de49772e432fbb2833ba5`,
  observed 2026-11-14, matching the superseded draft's capture and the independent reviewer's
  check earlier the same day) declares `<progress>Complete</progress>` and
  `<update>None planned</update>`. The official landing page describes the 2021 revision as a
  Simplified Presentation Format that changed no data. The Downloads package `GIS.zip`
  (230,322,811 bytes, SHA-256 `e491215d0a937cae48cc5e9a2763ea43c85a4d11c183ec7c5b1ac559d2a585a5`,
  observed 2026-11-14) was fully captured during the superseded draft and validated, but a
  one-time release with no planned maintenance is not a pipeline input under the current source
  policy, so the draft capture was removed.
- **The live producer service serves that unchanged release with no maintenance evidence.** The
  USGS Energy Resources Program NCRDS "CoalByRank" MapServer, layer 0 "CoalFieldsbyRank"
  (`https://energy.usgs.gov/arcgis/rest/services/NCRDS/CoalByRank/MapServer/0`, USGS's own ArcGIS
  Server, currentVersion 12.1, listed in the producer's NCRDS service directory) serves 602
  records equal to the OFR release (verified 2026-11-14: attributes row-for-row with the OFR
  `Coal_Fields` shapefile, OBJECTID 1..602 = record order; served Web Mercator rings
  vertex-identical to the NAD83 source after projection, 298,530/298,530 vertices, max offset
  1.95 m). But maintenance evidence for this exact layer was checked and is absent:
  - the layer document exposes no `editingInfo`, `lastEditDate` or `editFieldsInfo` (checked
    2026-11-14);
  - the service iteminfo carries no modified/created dates (checked 2026-11-14);
  - the service `info/metadata` CreaDate (2024-03-29) is metadata creation, not a data update;
  - the EUSEF project's Data Management task (page updated 2024-08-28,
    https://www.usgs.gov/centers/geology-energy-and-minerals-science-center/science/evolving-utilization-solid-energy-fuels)
    states it provides "a central administrative repository for existing solid energy fuel data,
    ensuring that these valuable USGS database products will continue to be available" —
    availability preservation, not refresh;
  - the National Coal Resource Assessment science topic is marked "Active Completed".
  A live service serving an unchanged one-time release without producer maintenance evidence is
  not a maintained publication under the source policy. This replaces the earlier claim (head
  ff40599e) that the service is the "maintained producer publication"; that claim is withdrawn
  per the independent review.
- **Current producer alternatives checked (2026-11-14):**
  - *NCRDS CoalByAge and CoalProvinces MapServices*: sibling services of the same origin
    publication with the same absent maintenance metadata; renderings of the one-time release,
    not separate maintained products.
  - *Maps of Energy Occurrence task* (page last updated 2018-11-13): publications are coking-coal
    sample data releases and a Mongolia GIS compilation; no national US coal-fields layer product.
  - *U.S. Coal Resources and Reserves Assessments (EUSEF task)*: regional-scale basin assessments;
    no national coal-fields polygon layer publication.
- **Retired/mirrored copies never fetched:** DataLumos project 240310 (the inventory Download
  Location), the deprecated `services1.arcgis.com/Hp6G80Pky0om7QvQ` HIFLD FeatureServer,
  GeoPlatform item `13ad01f317f74e5989c3ee199496fa32`, the Esri Living Atlas "USA Coal Fields"
  item `5dffaad2b2074d0dad20e9d5f52f6641` (owner `esri_landscape2`, not the producing agency),
  and the OFR 2012-1205 Downloads package itself (one-time release; comparison context only).

## What the parked code does

`src/dagster_hifld/sources/us_coal_fields.py` keeps only the pinned producer-contract research
record and the fail-closed refusal (`no_maintained_producer_publication_park_or_retire`); it
defines no fetching, decoding, staging or candidate output, and every materialization attempt
fails closed. The registered ingest asset `ingest/us-coal-fields-1/us-coal-fields-1` fails
closed, job `us_coal_fields_parked_refusal` exists for an explicit refusal run, and the monthly
schedule `us_coal_fields_parked_monthly` is STOPPED. `parse_ofr_release_status` fails closed if
USGS ever changes the OFR release status or declares a planned maintenance/update frequency, and
`parse_layer_maintenance` fails closed if the CoalByRank layer document ever publishes
`editingInfo`, `lastEditDate` or `editFieldsInfo` maintenance metadata — either event re-opens
the source decision explicitly instead of being absorbed silently.

## Resuming requires an explicit maintainer decision

One of: park permanently; retire the dataset; or name a maintained producer source (for example,
a USGS product that republishes or updates the national coal-fields mapping with producer
maintenance evidence and a stated refresh cadence, which would be re-evaluated against the
source policy). `approved: false` everywhere, no default fetching, no release schedule started,
no promotion path introduced.

No production write, schedule start, or release acceptance is implied.

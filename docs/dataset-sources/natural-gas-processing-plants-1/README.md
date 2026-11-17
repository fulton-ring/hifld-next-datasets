# Natural Gas Processing Plants (natural-gas-processing-plants-1)

## Parked refresh (issue #172, checked 2026-11-16 UTC)

The public `capture_natural_gas_processing_plants` entrypoint now refuses
acquisition and cache replay before network or storage effects. The registered
Dagster asset/job remain addressable, with their monthly schedule stopped.
Historical paths, identifiers, captures and artifacts are preserved. Approval
flags remain false; code review does not authorize publication.

The bounded original-source investigation did not establish an accessible,
maintained plant-location input. This is not a claim that no recoverable source
exists. Restoring ingest requires a maintained EIA publication with verifiable
producer, dates, cadence, native population, attributes and geometry. Access to
the token-gated service would permit investigation, not establish currentness
or source/product acceptance. Addresses alone would require a separately scoped
geocoding product and cannot silently replace native points.

## Original EIA survey publication

Live HTTP 200 probes on 2026-11-16 established:

- <https://www.eia.gov/survey/#eia-757> marks EIA-757 Schedule A
  **Suspended**. It describes a nominal collection every three years; Schedule B
  collects operational status during emergencies and is not a maintained full
  plant-location roster.
- <https://www.eia.gov/naturalgas/ngqs/data/report> identifies `RP9`,
  "757 Processing Capacity", with `last_updated: Jan 31, 2019 at 3:48 PM`,
  available years 2012, 2014 and 2017, and maximum year 2017. Other report families
  have 2026 update dates; those do not date the processing-plant data.
- <https://www.eia.gov/naturalgas/ngqs/release_dates.json> gives EIA-757 a
  January 2019 release and next release `TBD`.

The public query application <https://www.eia.gov/naturalgas/ngqs/> and its
`app.min.js` were inspected to identify those metadata routes. No newer plant
payload was captured or full national parity asserted. These are research
metadata URLs, not new runtime inputs.

## Energy Atlas access

On 2026-11-16,
<https://services7.arcgis.com/FGr1D95XCGALKXqM/arcgis/rest/services/NaturalGas_ProcessingPlants_US_EIA/FeatureServer?f=pjson>
returned HTTP 200 containing ArcGIS error `499 Token Required`,
`GWM_0003`. The item metadata endpoint
<https://www.arcgis.com/sharing/rest/content/items/1603bffa90024b9eb280215af7edc3bd?f=pjson>
returned HTTP 200 containing error `403`, `GWM_0003`. Neither response exposes
current data dates or native payload. Agency organization hosting alone does
not verify the owning item, maintenance, rights or redistribution permission.
No nonproducer ArcGIS item or mirror was fetched as a substitute.

## Retained historical package

The former runtime input was EIA's
<https://www.eia.gov/maps/map_data/NaturalGas_ProcessingPlants_US_EIA.zip>:
78,776 bytes, server `Last-Modified: 2020-01-10T15:23:59Z` (previously verified
2026-11-15), layer `NaturalGas_ProcessingPlants_US_2017_v2`, 478 points,
EPSG:4326, 2017 survey period, embedded GIS publication 2020-01-13.
It is a historical snapshot, not a maintained feed. EIA's
<https://www.eia.gov/analysis/naturalgas/> describes 510 active Lower 48 plants
in the 2017 survey, so the GIS package is not asserted to equal the complete
survey population. It was not downloaded again during this investigation.

Historical decoding and exact native field/ID/geometry regression checks remain.
The former acquisition/seal/catalog/cache success test is replaced with refusal
checks because refresh and replay are intentionally unavailable. Offline partial
capture quarantine behavior remains tested through the retained private helper.
Existing publication approval refusal remains tested with populated inventory.

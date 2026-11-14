# NOAA OCS U.S. Maritime Limits & Boundaries Overview

This is an isolated draft implementation for HIFLD inventory row `overview-20`. It targets NOAA MapServer layer 0, “Overview”, as its own composite line layer. Existing 12 NM Territorial Sea, 24 NM Contiguous Zone, and 200 NM EEZ assets target layers 1, 2, and 3; they are not substitutes for this layer.

## Source and observed release

- Producer: NOAA National Ocean Service, Office of Coast Survey.
- Live layer: [MapServer layer 0 query](https://maritimeboundaries.noaa.gov/arcgis/rest/services/MaritimeBoundaries/US_Maritime_Limits_Boundaries/MapServer/0/query?where=1%3D1&outFields=%2A&returnGeometry=true&f=pjson); [service metadata](https://maritimeboundaries.noaa.gov/arcgis/rest/services/MaritimeBoundaries/US_Maritime_Limits_Boundaries/MapServer?f=pjson).
- [NOAA product page](https://nauticalcharts.noaa.gov/data/us-maritime-limits-and-boundaries.html) describes static downloads and dynamic services that reflect source updates.
- [NOAA InPort record 39963](https://www.fisheries.noaa.gov/inport/item/39963) identifies service version 4.1, revised 2013-09-13, gives a 2002–2015 data timeframe, and says an update is forthcoming. The MapServer response itself is pinned to `documentInfo.Title = "U.S. Maritime Limits and Boundaries (version 4.1)"` and service-description prefix `(Version 4.1, 9/13/2013)`. The implementation records 2013-09-13 as the advertised service release date; it records each retrieval timestamp separately. Currentness and continued use of this source version need maintainer review.
- Layer 0 REST count was 260 immediately before and after extraction on 2026-10-20. Its independent ID responses contained the same 260 unique `OBJECTID`s, roster SHA-256 `ff1a9250b4f0b618e83644fc3f7057fccb141fc0e95725d6cec2ee149fad8006`. The terminal page was empty and the complete response fit below the producer’s 1,000-record page limit.
- The query schema is pinned at 12 fields and SHA-256 `b1ba8a90183c60d0895e176f6af4f1946ce73f27fe656573ac41e6b71a5e0ae2`. Native CRS is EPSG:4326; geometry is polyline. Query fields, nulls, whitespace, dates, numeric precision, source identifiers, and coordinate sequences are retained.
- NOAA’s official [static shapefile ZIP](https://maritimeboundaries.noaa.gov/downloads/USMaritimeLimitsAndBoundariesSHP.zip) was independently captured and compared: all 260 IDs and line coordinates match, but its 16-column representation differs from the 12-field live REST response. The ZIP is comparative evidence, not an input substituted into this layer.
- DataLumos project [240571](https://www.icpsr.umich.edu/web/pages/ICPSR/studies/240571) lists Overview GeoJSON and shapefile archives and DOI `10.3886/E240571V1`; its page returned HTTP 403 in this runtime, so archive bodies and an exact historical HIFLD-to-NOAA crosswalk were not independently inspected.

## Candidate behavior and gates

The Dagster source asset validates count, before/after IDs, exact complete ID roster, schema, terminal-page emptiness, CRS, geometry, requested/final URLs, and raw response receipts. It builds an immutable, content-derived `v0.0.0-draft.*` candidate in local staging only. The candidate contains a GeoPackage with all 12 native attributes, an Arrow/GeoParquet candidate, raw response bodies and receipts, and provenance metadata. The GeoPackage writer's wall-clock metadata is normalized so retries of the same capture produce identical bytes; actual retrieval time stays in provenance. The manifest states `promotion_allowed: false`, `source_accepted: false`, and leaves the accepted contract and baseline unset.

PMTiles is withheld pending review of global wrap and rendering behavior. Shapefile ZIP is withheld pending review of DBF field-width truncation, especially `NOTE` (1,000 characters) and `SUPP_INFO` (500 characters). No output is promoted and the proposed monthly 06:00 America/New_York refresh schedule is stopped by default; it refuses non-local or bucket-backed storage.

NOAA InPort lists no access constraints but does not establish a reuse license. License, exact HIFLD crosswalk, historical baseline, identifier and physical-schema mapping, present-day source acceptance, and output contract remain unapproved. These are review decisions, not grounds to silently replace the layer with the three existing component assets.

The reproducible offline capture and static ZIP comparison used during implementation are retained outside the repository at `/workspace/work/hifld-coordinator/262/`. The repository records hashes and decisions above; no large source dumps are committed.

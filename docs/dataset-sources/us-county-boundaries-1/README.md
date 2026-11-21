# US County Boundaries (Census TIGER/Line)

This draft acquires the official U.S. Census Bureau Geography Division TIGER/Line `COUNTY` ZIP and preserves its body, wire response, and receipt under the candidate's `raw/` prefix. The registered native-output route writes a GeoParquet candidate at EPSG:4269.

**Vintage: TIGER2026.** Issue #777 moved this pipeline from the superseded TIGER2023 pin to the latest maintained vintage. The pinned source URL is `https://www2.census.gov/geo/tiger/TIGER2026/COUNTY/tl_2026_us_county.zip` (Last-Modified `2026-10-07T01:45:57Z` observed on 2026-11-15; the 2026 TIGER/Line Shapefiles were released October 14, 2026). The pinned archive is 83,378,454 bytes with SHA-256 `b527c9ba7aeb8f17ef118f026eae5e5a938aa1d00b873bc32342d0dce59321c0`.

The isolated archive inspection found 3,235 county and county-equivalent features, 18 declared attributes, GEOID values retained as five-character strings, EPSG:4269, and no null, empty, or invalid geometries. Full-population readback compared every typed attribute value, GEOID in source order, and each feature's WKB against the source Shapefile; 3,182 geometries are Polygon and 53 are MultiPolygon. The source extent is recorded as observed. This is source integrity evidence; it does not establish compatibility with a historical HIFLD/FEMA baseline, a published schema or ID contract, or accepted coverage.

The TIGER/Line vintage year is not treated as the source data release date. The capture packet records the exact request and complete wire/body receipts. The archived HIFLD copy was not used. Source release/currentness, schema and ID contracts, coverage, rights, mapping, and release acceptance remain false. The refresh schedule is STOPPED.

History: the candidate was first captured at the superseded TIGER2023 vintage on 2026-10-21 (`tl_2023_us_county.zip`, 83,451,409 bytes, SHA-256 `692e12c30c83adcaabdbac0d3954fafa55e1c89a24b36d95e72e02dff938652e`). Those dated receipts remain in `source.json` and `local-materialization.json` as historical evidence only; the pipeline pins TIGER2026 above.

Retained local evidence is under `/workspace/work/363-clean-evidence/`, including the raw body and wire response, request receipt, and archive inspection report. These local evidence files are not committed as data assets.

An isolated packaged-runtime lifecycle also ran with no `.git` directory and no `git` executable: registered source and catalog materializations passed, native GeoParquet discovery selected the full population, and the resolved provenance retained the packaged identity. A false-quality candidate stopped before verify/promote, and an empty approval registry stopped at verify without changing the populated synthetic published fixture. The exact run IDs, package identity, output hash, and full parity report are recorded in `local-materialization.json`; raw capture and Dagster run database remain in the local evidence packet.

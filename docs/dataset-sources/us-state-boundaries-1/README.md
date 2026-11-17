# US State Boundaries (Census TIGER/Line)

This draft acquires the official U.S. Census Bureau Geography Division TIGER/Line `STATE` ZIP and preserves its body, wire response, and receipt under the candidate's `raw/` prefix. The registered native-output route writes a GeoParquet candidate at EPSG:4269.

**Vintage: TIGER2026.** Issue #777 moved this pipeline from the superseded TIGER2023 pin to the latest maintained vintage. The pinned source URL is `https://www2.census.gov/geo/tiger/TIGER2026/STATE/tl_2026_us_state.zip` (Last-Modified `2026-10-07T01:45:57Z` observed on 2026-11-15; the 2026 TIGER/Line Shapefiles were released October 14, 2026). The pinned archive is 9,957,057 bytes with SHA-256 `2e17ea40dff3e00163eec87a26efa0b0ff7e8fe364a191bf1a7754dcb49ea326`.

The isolated archive inspection found the same 56 state and state-equivalent features as the 2023 vintage (GEOID roster and digest unchanged), 15 declared attributes, EPSG:4269, and no null, empty, or invalid geometries. The source extent is recorded as observed. This is source integrity evidence; it does not establish compatibility with a historical HIFLD/FEMA baseline, a published schema or ID contract, or accepted coverage.

The TIGER/Line vintage year is not treated as the source data release date. Source release/currentness, schema and ID contracts, coverage, rights, mapping, and release acceptance remain false. The refresh schedule is STOPPED.

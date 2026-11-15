# Puerto Rico Subbarrios (Census TIGER/Line)

This draft acquires the official U.S. Census Bureau Geography Division TIGER/Line `SUBBARRIO` ZIP for Puerto Rico (FIPS 72) and preserves the complete source ZIP and every sidecar under the candidate's `raw/` prefix. The registered route catalogs the native Shapefile; GeoPackage and PMTiles output are withheld (see the source manifest's `unsupported_output_formats`).

**Vintage: TIGER2026.** Issue #777 moved this pipeline from the superseded TIGER2023 pin to the latest maintained vintage. The pinned source URL is `https://www2.census.gov/geo/tiger/TIGER2026/SUBBARRIO/tl_2026_72_subbarrio.zip` (Last-Modified `2026-10-06T23:34:54Z` observed on 2026-11-15; the 2026 TIGER/Line Shapefiles were released October 14, 2026). The pinned archive is 137,565 bytes with SHA-256 `a85af8bea84e763c6a9150950a3aca40193dfa8bc46ece8ea97122e03cd80b64`.

The isolated archive inspection found 143 subbarrio features (two fewer than the 2023 vintage's 145), 17 declared attributes, GEOID values retained as 15-character strings, EPSG:4269, and no null, empty, or invalid geometries; 141 geometries are Polygon and 2 are MultiPolygon. The observed extent is [-67.192456, 17.910811, -65.624604, 18.48101]. This is source integrity evidence; it does not establish a published schema or ID contract, accepted coverage, or release.

The TIGER/Line vintage year is not treated as the source data release date. Source release/currentness, schema and ID contracts, coverage, rights, mapping, and release acceptance remain false. The refresh schedule is STOPPED.

History: the candidate was first captured at the superseded TIGER2023 vintage on 2026-10-21 (`tl_2023_72_subbarrio.zip`, 133,211 bytes, SHA-256 `5358e982d49bc6a5c099e12d32220e756e05aff848f24ba89d862dd89d7b8d8a`). Those dated receipts remain in `source.json` and `local-materialization.json` as historical evidence only; the pipeline pins TIGER2026 above. Offline archive checks replay a retained local copy of the pinned 2026 ZIP via `HIFLD_PUERTO_RICO_SUBBARRIOS_FIXTURE` (see `tests/fixtures/puerto_rico_subbarrios/README.md`).

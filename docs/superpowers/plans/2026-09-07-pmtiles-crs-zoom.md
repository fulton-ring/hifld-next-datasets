# PMTiles CRS and Degenerate Zoom Fixes

**Goal:** Generate valid PMTiles for National Forests and Isolated Danger Buoy without changing zoom behavior for ordinary datasets or guessing missing projections.

**Approved scope:** Implement, deploy Dagster, and materialize only the two PMTiles assets. Do not promote or interrupt the running NHD/NFHL West assets.

## Design

Keep automatic zoom selection. Only when Tippecanoe exits with its specific insufficient-distinct-locations error, retry once without `-zg`, retaining explicit maximum zoom 14. Preserve bounded diagnostics and output validation for both attempts. Do not retry unrelated failures.

For CRS, preserve normal EPSG:4326 and valid projected/custom CRS reprojection, but fail on failed transformations or coordinates outside valid longitude/latitude bounds. Do not silently pass projected coordinates to Tippecanoe. Correct National Forests' staged GeoPackage CRS metadata only after matching its data to authoritative USGS metadata. Preserve feature coordinates and attributes; record provenance. The source CRS assignment and later EPSG:4326 transformation are distinct operations.

## Implementation and verification

- [ ] Add failing zoom regression with the real one-point error, plus coincident points and unrelated-failure checks; implement one bounded retry in `_create_and_upload_pmtiles`.
- [ ] Add failing `_to_wgs84` regressions for invalid degree coordinates and failed transformations, retain projected-to-geographic and valid-custom-CRS coverage; implement explicit validation and raised errors.
- [ ] Confirm Forests' source CRS against USGS National Forest layer 31 and matching feature identity/coordinates. Correct local GeoPackage metadata; validate counts/coordinate equality and transformed geographic bounds.
- [ ] Run focused tests and full `uv run python -m unittest discover tests` in normal repository layout; compare Ruff with baseline and review diff.
- [ ] Run real Tippecanoe smoke tests for one point and Forests using the new code.
- [ ] Integrate into existing feature branch, build immutable linux/amd64 Dagster image, push, deploy chart 1.13.21 preserving Helm values except image tags; verify digest/rollout.
- [ ] Upload corrected Forests source with generation guard and provenance, launch only the two PMTiles assets, verify successful outputs/PMTiles bounds. No production promotion.
- [ ] Retain audit and remove task-only worktree/local data.

## Completion audit — 2026-09-07 UTC

- Source commit `1f8feed`, integrated into `codex/geoparquet-source-publishing`.
- Full normal-layout unittest suite: 328 tests, OK, one skipped. New test files pass Ruff check/format; existing conversion lint findings decreased from 50 to 49; no broad lint refactor.
- Real Tippecanoe tests exercised single-point and coincident-point fallback. Actual buoy source also passed in the deployment image with Tippecanoe 2.49.0.
- National Forests source CRS verified against [USGS National Forest layer 31](https://carto.nationalmap.gov/arcgis/rest/services/govunits/MapServer/31). The current Prescott feature contains a boundary vertex matching the staged coordinate within 0.000058 metres, with explicit WKID 102100/latestWKID 3857. Historical IDs changed in the current service.
- Corrected only the staged GeoPackage's source CRS to EPSG:3857, with all 154 feature geometry payloads and all attributes unchanged. GDAL normalized LOADDATE by adding Z; original strings were restored and all attributes compared exactly before upload.
- Source object: `gs://hifld-next-staging-prod/national-forests/national-forests/v1.0.0/geopackage/national-forests-geopackage.gpkg`. Original generation `1788533055475892`; corrected generation `1788745195975293`; CRC32C `1Ms5tg==`; 63,074,304 bytes. Upload used generation precondition. Bucket versioning and seven-day soft delete confirmed.
- CRS provenance retained at `national-forests/national-forests/v1.0.0/metadata/crs_correction.json` in staging.
- Image `gcr.io/hifld-next/dagster-user:pmtiles-crs-zoom-1f8feed-20260907`; digest `sha256:d4342b90b8d56599e8195ece7ce3799f3b10edbcc703f84e6f36a4def2d77e63`.
- Helm release `dagster`, namespace `hifld-next-datasets`, revision 55, chart 1.13.21. Only four image tags changed; all services ready.
- Forests PMTiles-only run `48ecca71-c868-4c72-b122-a1b1e5c35dcf`: SUCCESS. Staged `pmtiles/national-forests.pmtiles`, 15,939,646 bytes, zooms 0–11, 12,707 addressed tiles, bounds `[-150.0076937,18.2312385,-65.6996681,61.5189922]`.
- Buoy PMTiles-only run `539ebfe1-15e2-48ea-a0b1-ff48f67145ab`: SUCCESS. Staged `pmtiles/isolated-danger-buoy-point-usace-ienc.pmtiles`, 7,243 bytes, zooms 0–14, 17 addressed tiles, point `[-91.3768654,40.5754858]`.
- Both remote archives passed PMTiles v3 header and JSON metadata checks, including nonzero tile counts and valid geographic bounds.
- No promotion or discovery triggered. Existing production and other staged Forests formats were not regenerated; their CRS issue is not corrected by this PMTiles-only run. NHD and NFHL West runs were not interrupted.

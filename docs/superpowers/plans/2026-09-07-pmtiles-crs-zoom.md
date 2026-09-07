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

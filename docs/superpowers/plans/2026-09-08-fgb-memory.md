# FGB memory accounting fix and NFHL East retry

**Goal:** Correct Fiona feature size accounting and bound PMTiles preparation batches before deploying and retrying NFHL East only.

**Approved design:** Serialize complete Fiona feature geometry, not its abbreviated string representation. Bound full serialized bytes per FGB input batch independently of compression estimates, with a conservative 100 MiB ceiling and row cap. Reject oversized individual features clearly rather than silently dropping them. Log each chunk's counts, estimated bytes and process memory. Serialized bytes are not an absolute RSS bound: Python/GEOS/GDAL copies require memory headroom.

**Evidence:** Real Fiona polygon reproduction: 10 vertices estimated141B, actual493B; 100000 vertices estimated145B, actual4155659B. NFHL East run c6af00cf-3077-45c5-981c-e9e65430cde6 was OOMKilled, exit137, at2026-09-07T16:10:39Z with64GiB limit. Kernel recorded main dagster process anonRSS53278884KiB. The exact operation at death was not persisted.

## Execution

- [ ] Add regression tests using actual Fiona feature models, demonstrate the abbreviated-serialization failure, and implement complete serialization.
- [ ] Bound FGB batches independently of the GeoParquet compression multiplier, enforce a row cap and an explicit oversized-feature error, add chunk diagnostics, and test row conservation and nonspatial/error behavior.
- [ ] Run targeted/full tests and lint/format comparisons; independently review the fix.
- [ ] Merge into the existing feature branch; build amd64 image and push to the existing gcr.io/hifld-next/dagster-user registry.
- [ ] Upgrade only Dagster with the existing chart/version/values and new image tag, verify deployment and launcher images.
- [ ] Launch only publish/formats/pmtiles for nfhl/national-flood-hazard-layer-area-nfhl-1-east/v1.0.0; verify running worker and early bounded-batch evidence. Do not promote or regenerate other formats.

The image also includes the previously verified row-group packing change already merged into the feature branch. This retry does not select GeoParquet, so that writer is not exercised by this run.

## Verification and deployment

- Code commits `31460ad`, `38ddc33`; full suite338 tests passed, one skipped. New focused memory tests4 passed. Independent review's retained sample GeoDataFrame finding fixed in38ddc33.
- Packaged amd64 runtime smoke test: 100000-vertex Fiona geometry measured1388977 bytes; FGB caps104857600 serialized bytes and10000 rows.
- Image `gcr.io/hifld-next/dagster-user:fgb-memory-38ddc33-20260908`; registry digest `sha256:c7f5e58825259ebebd5e8685b9058757e8341e982daa097aaa30e29aa3dc6dc9`.
- Helm revision56, chart1.13.21, all three services ready on new image. Existing resource limits retained. GCS partial logs upload every30seconds; conversion logger managed atINFO. IaC overlay commit6dd5f7d records upload interval.
- Initial partial deployment-array CLI override was rejected by chart validation before rollout; reapplied with complete existing Helm values, changing only four image tags and diagnostic configuration.
- East PMTiles-only retry launched: `04ed31e3-c551-4257-9917-12da53df09f0`. No other assets selected; no production promotion.

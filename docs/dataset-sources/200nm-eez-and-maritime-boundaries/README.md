# 200NM EEZ and Maritime Boundaries — research comparison

Outcome: **Blocked / Potential breaking change / Needs decision**. [DECISION.md](DECISION.md) gives concrete baseline, contract, geometry representation and continuity decisions. This research branch does not grant approval or change ingestion, dependencies, catalog/publish behavior or production data.

## Newly reachable evidence, 2026-10-19 UTC

Runtime policy was observed current, unrestricted and enforced at revision 6. NOAA InPort39963 now responds HTTP 200 at `https://www.fisheries.noaa.gov/inport/item/39963`; the former `inport.nmfs.noaa.gov` HTML URL redirects there. Its producer/distributor Office of Coast Survey explicitly links the exact NOAA MapServer already linked by the OCS landing page. This verifies a producer publication, not a third-party portal account. InPort's visible publication/revision values are year 2013, while schema.org renders2013-01-01. Do not invent day precision. The service states version 4.1 dated 2013-09-13. InPort says “Update to dataset forthcoming” and maintenance “Continually.” Its 2026-08-20 metadata modification is not a 2026 data release. The legacy XML endpoint still returns503. All complete response captures and exact URLs/checksums are retained in `evidence/network-retry.json`.

DataLumos archive project238491 and the DOI's crosswalk project241367 now respond with provider HTTP 403 challenge pages, with no CONNECT denial. Thus exact DHS XLSX/hyperlink continuity remains unverified. Their role remains historical archive. Earlier tunnel failures in `evidence/requests.json` describe earlier observations only. `evidence/current-requests.json` records fresh count 96, unchanged layer metadata, and full pjson/GeoJSON bodies byte-identical to the complete retained captures. The full `f=json` query variant returns HTTP 200 with ArcGIS error 500 and is rejected; HTTP success alone never means valid extraction.

## Complete pinned published candidate comparison

The complete `v1.0.0` GeoParquet baseline **candidate** is preserved read-only outside Git at `/workspace/work/hifld-network-retry/data/200nm-eez-and-maritime-boundaries.parquet`. Its 416471 bytes, SHA256 `3de5e54cbdcf80a70b0c085fef69598b0e24f2ef9aa17dfa43f641113fdbfd33`, generation `1791506335103859`, ETag and generation-pinned URL are in `evidence/published-candidate.json`. The corresponding full catalog is committed and checksummed. This pin is not acceptance; all seven original published captures remain untouched.

`evidence/published-candidate-comparison.json` reproduces the complete physical Arrow schema/nullability and GeoParquet CRS, every source/baseline OBJECTID, full identifier values, all 13 source attribute comparisons, region/category coverage, and per-feature geometry diagnostics. Both sides have 96 unique nonnull OBJECTIDs and BOUND_ID strings. All 13 attribute values match under Python numeric equality; that deliberately does **not** establish physical type compatibility. In particular source Esri `UNILATERAL` is float while candidate is Arrow int64, date attributes are epoch-millisecond integers, and source metadata omits nullability declarations. Candidate adds `id` and `bbox` and represents source `SHAPE` as `geometry`. Every candidate `id` equals the source OBJECTID and GeoJSON feature-level id; no ID generation is authorized.

All 96 ordered geometry part/vertex coordinate sequences equal both Esri JSON and GeoJSON, without normalization, rounding, repairs or transformation. All 96 geometries are valid/nonempty and topologically equal, and every candidate bbox exactly matches its geometry. **80 physical geometry types differ**: source GeoJSON 90 LineString/6 MultiLineString versus candidate 10 LineString/86 MultiLineString. Only 16 have exact geometry type-and-coordinate equality. Representation changes require a contract decision even where topology and coordinates match. Coverage distributions agree across all 12 regions and Maritime Boundary/Maritime Limit categories. This is a complete bounded comparison, not a release stability claim.

Original extraction reconciles 96 complete rows with independent ordered OBJECTID projection and count below maxRecordCount 1000; no transfer-limit flag. `returnIdsOnly` error 500 captures remain rejected. Sequential requests are not an atomic immutable snapshot. Retained full bodies make the dated observations replayable; neither OBJECTID nor BOUND_ID is proven stable across releases. NOAA warns NOT FOR LEGAL USE.

## Replay and local readiness

Use the frozen worktree environment; `UV_CACHE_DIR=/workspace/.cache/uv uv sync --frozen` succeeded. From this worktree:

```bash
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/200nm-eez-and-maritime-boundaries/audit.py
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/200nm-eez-and-maritime-boundaries/audit.py --quality
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/200nm-eez-and-maritime-boundaries/compare_candidate.py --baseline /workspace/work/hifld-network-retry/data/200nm-eez-and-maritime-boundaries.parquet
```

The comparison verifies all committed input fingerprints, the whole candidate checksum/length and catalog checksum before parsing, then prints a deterministic report. Compare it byte-for-byte with the committed comparison JSON. The baseline is intentionally outside Git; preserve it and use the manifest's generation pin if fetching again. Existing `audit.py` remains the source extraction replay. Both scripts perform no network or output data mutation.

`evidence/readiness-20261019.json` records fresh successful `Definitions.validate_loadable(defs)`, actual repository asset graph loading and durable `DagsterInstance.get()` creation with worker-specific DAGSTER_HOME, staging and published directories, production bucket variables removed. Only local isolated state was created. Earlier webserver/GraphQL evidence remains historical. No dataset materialization ran because no approved issue3 contract/ingestion exists; shared verifier implementation/integration belongs to #417. No full-suite or CI pass is claimed, and missing sibling `hifld-next-iac/environments/prod/dagster.tf` remains an explicit limitation.

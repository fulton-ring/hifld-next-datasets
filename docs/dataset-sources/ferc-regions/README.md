# FERC Regions: explicit historical preservation draft

Issue #85. Worker `/root/verifier_417`; coordinator https://chatgpt.com/local/01a15273-e90b-7211-b493-3ad4668ed0e8.

The proposed pipeline reconstructs the **complete pinned PASDA historical original as provided**, not a current FERC feed. Its five regions cover CONUS. It does not claim Alaska/Hawaii or national completeness. Source selection, edition, rights, baseline, schema/ID/decimal/geometry interpretations and release remain unapproved. No production write, materialization acceptance, merge or closure is authorized by this evidence.

The official FERC dam-safety page and its regional-office PDF return200. DHS/Data.gov's exact HIFLD product record `MGMT-GMO-HIFLD-180608` returns200. PASDA's original ZIP includes HIFLD/HSIP ORNL/LANL/INL/NGA credits and links a historical HSEMA service; the actual service currently returns HTTP200 with ArcGIS400 Invalid URL. DataLumos239062 and the E241367 crosswalk return provider403; their complete challenge bodies remain preserved locally, not published. These are specific access/authority gaps. No crosswalk mismatch or permanent source absence is inferred.

## Complete original and physical audit

The explicit input directory must contain `pasda_original.body`, `ferc_boundary_map.body`, `dhs_raw.body` and each corresponding `.receipt.json`. The complete original archive is43,569,780bytes, SHA256 `d95afa79a4827b2e18d21acc9c5d9ad8f201a5f5f498e7b5a55ef47df1655ced`. Matching originals/receipts remain under `/workspace/work/hifld-coordinator/85/source`; no live refetch is required or performed by the source asset.

All11 original members are pinned. Every original DBF value, original zero-based file-local `OGC_FID`, `OBJECTID`, literal `ID`, all3,727,592 XY coordinates and17,526 ordered rings were independently audited. Original native EPSG4326 WKB is retained without repair, reprojection or simplification. ATLANTA and NEW YORK contain two invalid polygons. The full-population source check must fail on those two originals; preservation success is not quality success.

The original two shape fields are DBF `N24,15`. Actual literal area values include `2586408727577.0898437500`, which cannot fit Arrow Decimal(24,15)'s nine integral digits. The proposed candidate stores **exact literal values as Decimal128(38,15)**, keeps every original literal and physical width/scale declaration in audit/raw evidence, and fails on rounding/overflow. This is an explicitly unapproved interpretation, not a claim that the physical DBF width is unchanged. GDAL's double interpretation is diagnostic only. GeoPackage is wholly unsupported because SQLite REAL would silently lose literal decimal precision. The proposed terminal is native-geometry GeoParquet with original field names, int32 OBJECTID, Date32 dates and all source IDs; no generic regeneration/schema expansion is accepted.

All five complete advertised baseline candidates are captured and their decoded body sizes/MD5s checked against the collection; SHA256 and lossless retained-file pins are recorded in the baseline artifact receipt. GeoJSON and GeoPackage bodies are losslessly gzip-retained. These remain unapproved candidates. Published Parquet has `source_ID` although collection metadata says `ID`; original `ID`, int32 OBJECTID, Date32, `SHAPE__Are/SHAPE__Len` and all five WKB differ physically from the historical publication. Diagnostic joins/mappings are unapproved. Complete historical IDs remain independently retained, not overwritten by new source IDs. No exact cross-format geometry continuation is asserted.

## Implementation under review

- `sources/ferc_regions.py` and `ferc_regions_spec.py`: explicit local pinned acquisition, raw-before-receipt retention, full native audit, exact-decimal draft, immutable first-bundle/CAS/marker and actual reachable Git/blob identity.
- `assets/ferc_regions.py`: `ferc_historical_preservation`, blocking `ferc_historical_full_quality`, source-only `ferc_historical_reconstruction_job`, STOPPED manual-input integrity schedule.
- The source-only job explicitly excludes checks and proves preservation only. Actual full-quality and normal catalog/release paths remain blocking. No autonomous PASDA polling or archive refresh is implemented.
- A permanent observed-trust-loss journal is persisted **before** an observed untrusted cache is detached. Partial journals refuse repeated retries and retain the candidate if persistence fails. Exact trusted operator restoration remains explicit. Loss of every witness before any observation is not detectable without an independently protected inventory.
- Raw originals are retained once outside candidate discovery; checked candidate references and the immutable first bundle bind their original receipts. Later successful retrieval contexts do not rewrite first construction.

Run bounded plus complete-original parser tests using the preserved archive:

```sh
PYTHONPATH=src HIFLD_FERC_TEST_CAPTURE_DIR=/workspace/work/hifld-coordinator/85/source \
  python -m unittest discover -s tests -p test_ferc_regions.py -v
```

Without the opt-in directory, the43MB original tests are explicitly skipped; bounded real DBF tests still run. Original large captures are not duplicated in Git. Public processing C0 `c62ab714b6911bfccc9300fe13a12b2be180a5d5` has now completed 16 actual isolated registered runs with all expected outcomes. The source succeeds for preservation; full source and normal catalog checks block on both original invalid polygons. Missing independent registry is separately demonstrated without a quality waiver. Whole-witness loss, two retries, exact trusted restoration, interrupted incident persistence, two repeated refusals and exact restoration preserve original receipts/raw/native/prior outputs. The final source-only cold rebuild was followed by a genuine normal catalog run to reconstruct derived metadata; that catalog again blocked on both invalids. The current suite passed 561 tests (560 passed, one intentional production skip) in88.671s with real sibling IaC and Tippecanoe. No CI or independent final-head pass is inferred. The initial schedule test harness used a nonexistent public `run_config` property; the corrected test evaluates the actual schedule tick. Both logs are retained locally.

The additional complete numeric/coordinate diagnostic is retained as a full artifact, pinned in [artifact-manifest.json](evidence/artifact-manifest.json): eight of ten numeric literals differ from their exact GDAL binary-double expansions; all five geometries differ even after zero-tolerance diagnostic normalization. Sorted full-coordinate multiset comparisons have maximum paired difference9.947598300641403e-14, which is **not an approved matching/tolerance policy**. Unsorted positional differences are ring/order-sensitive and are not reported as vertex movement.

The independently reviewed shared native-GeoParquet reader/discovery checkpoint `230ead9f8607017bd6b2323518e7d8879dc18e69` is explicitly merged. First construction still waits for the publicly reachable FERC processing commit. No disguised legacy Shapefile layout or lossy GeoPackage fallback is used. The portable actual-Dagster runner executed against the publicly reachable processing C0 after exact remote/native tree verification. See the concise [validation summary](evidence/validation-summary.json) for run IDs and full artifact pins. No later documentation commit replaces the original processing context.

## Artifact storage

Complete capture bodies, receipts, large comparisons and runtime logs are preserved as checksummed artifacts, outside Git. The concise [artifact manifest](evidence/artifact-manifest.json) binds the losslessly retained originals. Source tests keep only the 1,084-byte original DBF and small projection/encoding fixtures. `scripts/verification/ferc_baseline_audit.py` replays the five complete candidates and reports every physical schema/ID/value/geometry diagnostic without accepting a mapping.

The full [baseline comparison summary](evidence/full-comparison-summary.json) keeps physical schemas and all five IDs per full-record format. Full GeoJSON readback initially hit GDAL default/160MB object-complexity caps; those failed diagnostic logs remain retained. The final replay uses the exact pinned writer layout, preserves top-level Feature members separately and parses every geometry through GEOS without a Python coordinate-list expansion. PMTiles is completely byte-pinned and its vector-layer metadata inspected; no lossless native row/geometry claim is made for generalized tiles.

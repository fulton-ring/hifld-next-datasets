# Alternative Fueling Stations — issue #11

This branch implements a working **local draft pipeline**, preserving the complete official public feed and every raw identifier. It does not accept a source, baseline, schema, compatibility exception or release. It is stacked on the #417 verifier branch because final publication must fail closed without an independent exact-release decision.

## Evidence and decisions

The official `USDOT_BTS` account, U.S. DOT organization, item `9010c04bcdf84c36abb74e98778af579` and its XML metadata identify DOE/NLR AFDC as producer and BTS as distributor. This matches the inventory's stated NTAD lineage. Current official pages use **NLR** while the inventory retains **NREL**. The anonymous BTS FeatureServer works without the NLR API key. The AFDC registered bulk form was not submitted and no API key was invented. Legacy `developer.nrel.gov` still gives CONNECT403; new `developer.nlr.gov` documentation and AFDC/BTS work. DataLumos gives a provider-side Cloudflare403; the DHS crosswalk match is explicitly unresolved.

The complete native EPSG:3857 source extraction has **103,502 points across 52 pages**, all 80 attributes, unique `OBJECTID` and `id`, unchanged pre/post count, ID census, schema and source edit metadata. HTTP receipts and SHA256 pins cover every original response. Metadata publication date 2026-10-07, source data edit 2026-10-18 and extraction time are distinct; this daily feed has no separately established immutable release/vintage date.

A complete generation-pinned HIFLD v1.0.0 GeoParquet candidate has **95,038 rows**. It remains unapproved. Across all records the comparison finds 82,028 shared station IDs, **21,474 added / 13,010 removed IDs**, three source-only attributes (`OBJECTID`, `related_stations`, `ev_charging_units`), and numerous physical type differences. It compares every shared attribute and point, with no approved tolerance applied.

Station `id=390105` is a concrete unresolved quality/migration case: the historical candidate says Durham, NC, while current source attributes and native geometry say Adak, AK; the station name changes only by an `(@East)` suffix. Exact raw coordinates and both sets of latitude/longitude/location attributes are in [full-comparison.json](evidence/full-comparison.json). Nothing was corrected, filtered or approved. The 9,137 differing raw coordinate pairs include possible binary representation effects as well as substantive changes, so the raw difference count is not an approved migration rule.

The maintainer must decide the crosswalk/lineage evidence, exact accepted baseline, raw/published schema and ID contract, ID/geometry migration (including that anomaly), and daily snapshot version/freshness policy. The complete unapproved proposal is [source.json](source.json); [DECISION.md](DECISION.md) lists concrete choices and exact pins.

## Working implementation

`src/dagster_hifld/assets/fuel_stations.py` contains:

- A bounded, keyless live capture function with pre/post completeness guards, native geometry, raw byte receipts and explicit ArcGIS error/truncation detection.
- A registered source-specific `bts_alternative_fueling_stations` asset with explicit capture directory and immutable semantic version. An empty capture path fetches the live official BTS feed. It permits isolated local staging only during review.
- Lossless raw staging, deterministic typed GeoParquet candidate generation, a streaming Arrow-to-GeoPackage bridge preserving all raw types/nullability/values, and identity-attribute native-CRS GeoJSON for the existing catalog/format pipeline. Epoch-ms dates stay int64, source integer IDs stay integer, strings/leading zeros remain strings. No published projection is applied.
- A pinned unapproved draft source schema signature; changed names/types/nullability stop canonical staging for review. Incomplete/changed source snapshots, duplicate/null identifiers, malformed geometry and immutable-version collisions fail.
- A separately materializable raw → typed-candidate graph with dynamic partitions.

All original JSON bytes remain available. The full typed GeoPackage bridge also passed all 103,502 rows/80 fields/native WKB checks. An earlier generic GeoJSON→GeoPackage experiment coerced nullable integers/ID values into floats or strings; its complete diagnostic is retained, and the source-specific Arrow bridge replaces that path. Six longitude values use integer-looking JSON tokens despite source-declared Double; candidate physical double representation follows that declaration without changing numeric value. This is explicit and is not an ID/string conversion. Dates are not converted to timestamps or historical published strings.

**Shapefile is unsupported and quarantined.** The 80-column schema includes long identifier names and embedded JSON strings that cannot fit the DBF naming/width model without a reviewed mapping. No schema-renaming/truncation/null conversion is approved. The shared #417 source-version exclusion prevents advertising that format as faithful. Generic downstream outputs remain review drafts, with no promotion authority.

## Reproduction

Preserve `/workspace/work/hifld-coordinator/11/`: it contains every original page, complete published candidate, source/provenance pages, checksums, isolated Dagster state and staged products. Large raw files are deliberately outside Git; committed metadata and audits are bounded. Full historical replay requires those exact preserved inputs; a later live refresh is a new snapshot.

From this worktree with the frozen environment:

```bash
UV_CACHE_DIR=/workspace/.cache/uv PYTHONPATH=src uv run --frozen --offline python docs/dataset-sources/alternative-fueling-stations/audit.py /workspace/work/hifld-coordinator/11 work/fuel-audit
UV_CACHE_DIR=/workspace/.cache/uv PYTHONPATH=src uv run --frozen --offline python -m unittest tests.test_fuel_stations tests.test_dynamic_partitions tests.test_checks tests.test_full_verification
```

For a new anonymous capture, run `capture.py NEW_EMPTY_DIRECTORY` with the same environment; it requests live public pages through the normal proxy, including generation pinning. Never overwrite the preserved historical capture. Configuring the normal asset requires `capture_dir` and a new semantic `version`; its actual op name is available from `bts_alternative_fueling_stations.op.name`.

## Validation scope

The actual complete raw → typed candidate graph passed (run `f978cc06-123d-4480-9919-1a88079d2933`). The normal explicit ingest and standard catalog passed (runs `cab3f800-825d-426c-b21a-78ae2df9c47e` and `8a7fadee-89df-49d8-9496-3d8f3fa71f89`). All 103,502 native point geometries, 8,280,160 attribute values and declared physical types were checked against original source pages, including both ID columns. The real shared promotion helper rejected missing independent release approval, retained candidate evidence and preserved the previous local accepted fixture's exact SHA256. No synthetic registry was used for real data.

Focused tests exercise missing/renamed/null/duplicate IDs, ID precision, leading-zero text, schema drift, typed values, missing/truncated/error responses, source edits, invalid geometry/CRS, immutable candidate preservation, real synthetic Dagster ingestion and refusal of remote review staging. Shared synthetic approval fixtures are distinct from real dataset acceptance. See the committed validation receipt for exact format/gate results. No CI or full-suite success is inferred; missing sibling `hifld-next-iac/environments/prod/dagster.tf` remains an explicit limitation.

The final full terminal Parquet audit retains all 103,502 rows, 80 original attribute columns/physical types/values and native WKB. It explicitly records derived `bbox` and the terminal `OBJECTID` nullability declaration changing from false to true; no actual null IDs exist. These metadata differences remain unapproved. The complete repository suite ran 480 tests:478 passed,1 production opt-in skip and the sole known missing-sibling-IaC error. No full-suite success is claimed.

The complete normal format/gate graph ran as `4b2bb620-c893-4583-907a-220aee00a5e5`: catalog/quality check, typed GeoPackage reuse, terminal GeoParquet and PMTiles generation passed; Shapefile explicitly skipped with zero files. Verification then rejected missing independent release approval. The blocking baseline check and promotion did not execute, the previous local fixture retained its exact hash, and all raw/candidate evidence survived. PMTiles is a derived visualization, with no raw-format equality claim.

The exported `fuel_stations_draft_refresh_job` selects source ingest only. Its proposed daily 09:00 America/New_York schedule is explicitly STOPPED; timestamped immutable draft versions are generated from scheduled time. It is an integrity-poll proposal, not an inferred official release timetable. Remote/cloud staging is rejected before any live request. No schedule was activated. Ten source-specific tests pass, including these guards.

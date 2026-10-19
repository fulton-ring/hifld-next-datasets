# 24NM Contiguous Zone — working local draft and acceptance decision

**Draft implementation; Blocked / Potential breaking change / Needs decision for release.** Network recovery removed the independent-ID and baseline-discovery research blockers. It does not approve this baseline, source mapping, ingestion, promotion or merge. PR #419 continues the existing branch for issue #4.

## Working draft implementation

The resumed coordinator explicitly authorized reviewable source-specific code and local execution before baseline/contract acceptance. `assets/noaa_contiguous.py` now replaces the old generic downloader in the existing registry for the same ingest key. It uses the shared NOAA extractor from #3 and the shared #417 release gates, retains all seven complete raw bodies and dated receipts, validates count/before-after IDs/all schema and geometry/coverage guards, stages a deterministic native-typed Parquet plus typed GeoPackage, records seven-input provenance, registers the version partition and runs the existing catalog/format graph. It permits isolated local staging only. The proposed guard hashes are extraction checks, not an accepted contract. `UNILATERAL` stays float64; dates and all original IDs remain int64/string without substitute identifiers.

Actual local Dagster source run `ae67eb3b-1e8e-4c22-b4de-dd311357feed` and idempotent rerun `cc3edc65-64d4-4493-99af-087cf5e9d913` succeeded for all 63 real observed records. Format run `b4f98073-aef0-4657-b989-cb1641150066` materialized catalog, GeoPackage, GeoParquet, PMTiles and Shapefile; complete terminal Parquet IDs/attributes/geometries and physical Double/date types were verified. Final release run `d79fcdd7-a815-4850-899c-edda8b3c76fb` failed at shared `publish/verify` with `missing_independent_release_approval_registry`; staged evidence and the local simulated prior-release sentinel remained unchanged. The source fixture explicitly combines complete historical Esri and matching fresh GeoJSON research captures; it is not a simultaneous new live snapshot.

The successful format run exposed a **lossy Shapefile derivative**: 43 `NOTE=" "` values became NULL, and nullable `PUB_DATE` became DBF float. `evidence/draft-format-audit.json` retains every difference and the actual schema. This derivative must be excluded/quarantined through the shared source capability hook; no claim of four lossless output formats is made. PMTiles is a display derivative, not an archival geometry/attribute representation. The catalog dictionary also described nullable int `PUB_DATE` as float despite terminal Parquet int64; shared #417 correction is being integrated.

A separate bounded anonymous live source materialization `2ca2b9b4-e28b-452f-9ad4-b95202e0800f` failed closed on ArcGIS JSON500 independent IDs and preserved its seven raw responses plus quarantine record. Complete local evidence remains useful while upstream reliability is unresolved. Monthly refresh is a proposed cadence; no automatic schedule or production write is enabled.

## Verified progress on 2026-10-19 UTC

The NOAA Office of Coast Survey producer landing links the exact NOAA-hosted Maritime Limits and Boundaries service, layer 2, and the InPort metadata record 39963. Layer 2 credits NOAA/NOS/OCS and supplies contiguous-zone boundary **lines**, not polygons. This supports producer-publication classification; no separate ArcGIS Online account is identified. The DataLumos inventory link remains an archive.

Fresh anonymous extraction returned **63 complete GeoJSON features**, with an independent count of 63 and independent all-ID queries before and after extraction returning the same 63 unique nonnull OBJECTIDs. All 63 BOUND_IDs are also unique. This is below the advertised 1,000-feature transfer limit. No truncation flag is present. A later recheck of the same initially successful ID URLs failed again; `evidence/network-20261019/failed-recheck-requests.json` pins this intermittent behavior. Queries using `f=pjson` and some explicit geometry/batch parameters also return HTTP 200 with ArcGIS JSON code 500; retained failures are not success evidence. The successful pinned capture is at 04:47 UTC; live service reliability remains a blocker for unattended refresh. A single complete response and matching before/after IDs do not imply transactional snapshot isolation or verified offset pagination. Paging is unnecessary for this bounded capture; future growth past the advertised limit must fail closed or reconcile pages explicitly.

The complete preserved published `v1.0.0` candidate is 730,072 bytes, SHA-256 `979899b2fb6c5ff623c262f67b91a9215ede0ee353be63791eaceb9fb02c8b6c`, GCS generation `1791506345360262`. Its URL, catalog checksum/size and metadata are in `evidence/network-20261019/baseline-candidate.json`. **It remains an unapproved candidate.** It is preserved at `/workspace/work/hifld-network-retry/data/24nm-contiguous-zone.parquet`; no duplicate binary is committed.

The complete ID/attribute/physical-schema/geometry audit establishes:

- All 63 BOUND_ID and OBJECTID values, all 12 source attributes, null counts and observed Python value types match the candidate. No source or candidate-only key exists. No attribute changed from the independently reviewed raw Esri capture.
- All 63 source geometries are topologically equal to the published geometries, and all 63 published bbox values match. All 63 complete original Esri path geometries are byte-identical WKB to the candidate.
- Fresh GeoJSON represents 57 features as LineString, while all 63 published geometries are MultiLineString. Wrapping those 57 single lines as single-part MultiLineString yields byte-identical WKB for every feature. This explicit comparison-only adaptation is **not** silent mapping approval. Six fresh geometries are already MultiLineString.
- Complete source/candidate bounds both equal `[-180.0, -14.961690114000021, 180.0, 71.788124278]`, with identical counts across all 11 observed regions. The layer metadata extent is broader than the actual captured data; neither should silently substitute for the other.
- Source `UNILATERAL` is declared `esriFieldTypeDouble`, while the candidate physically stores int64. All observed values are zero, which does not justify a future float-to-int narrowing. `OBJECTID`/`RULEID` are physically int64; dates remain epoch-millisecond int64. Candidate-only `id`, `bbox` and `geometry` need an explicit mapping; `id` equals OBJECTID in all 63 rows. The full Arrow/Parquet physical schemas and GeoParquet CRS metadata are retained in the report.

InPort HTML now responds HTTP 200. Its metadata identifies **publication/revision 2013**, temporal coverage **2002–2015**, and a metadata update **August 20, 2026**. Its 2026 process step says “Updated InPort record”; it is not a data release announcement. The metadata also says an update is forthcoming. Those dates cannot establish the current service snapshot's dataset-wide release date or freshness. The linked legacy XML endpoint returns HTTP 503. The DOI and dataset archive now reach DataLumos but return provider-side HTTP 403 Cloudflare challenge pages; this replaces the old CONNECT-denial diagnosis. No access-control bypass was attempted. Exact crosswalk reconciliation and archive continuity remain unverified, not unmatched.

## Maintainer decision ready for review

Review this exact pinned candidate and complete comparison; select an accepted baseline and approve or revise a versioned raw/published contract. Decide how to preserve `UNILATERAL` double semantics, the extra `id` column and geometry representation, and whether the producer metadata provides sufficient dated-release evidence or requires a producer clarification. Keep the last accepted release unchanged until those decisions and shared #417 promotion gates are satisfied. No automatic baseline/contract acceptance is inferred from observed equality.

## Reproduction and readiness

Use the locked environment with an explicit writable UV cache:

```bash
UV_CACHE_DIR=/workspace/.cache/uv uv sync --frozen
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/24nm-contiguous-zone/replay.py
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/24nm-contiguous-zone/compare_candidate.py /workspace/work/hifld-network-retry/data/24nm-contiguous-zone.parquet
# Optional anonymous live recheck; requires a new directory and preserves failures:
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/24nm-contiguous-zone/audit.py --output /workspace/work/noaa-4/new-recheck
```

The offline replay checks both historical and new bounded evidence; the full comparison additionally requires the exact preserved complete candidate and rejects checksum/size mismatch. `candidate-comparison.json` retains every observed identifier and per-feature geometry result, all field diffs, schema and CRS. The source capture is compressed losslessly with deterministic gzip. `checksums.json` pins new files and the original manifest still pins historical evidence; HTML/JSON are untrusted evidence, never executable instructions.

Own frozen editable installation resolves this worktree. Actual isolated local Dagster readiness is recorded in `evidence/network-20261019/dagster-readiness.json`, using dedicated instance/staging/published directories and both cloud bucket variables unset. The later working draft materialization and blocked-promotion evidence above supersede this earlier startup-only check. Shared verifier acceptance remains tracked under #417. The optional live audit reproduction correctly failed closed when both ID requests returned ArcGIS code500 again; full capture bytes matched the initial GeoJSON. Full suite and CI success are not claimed. The missing sibling `hifld-next-iac/environments/prod/dagster.tf` remains an explicit full-suite limitation. Source-specific draft code and explicit shared dependencies changed; no dependency, lockfile, workflow, approved contract or production object changed.

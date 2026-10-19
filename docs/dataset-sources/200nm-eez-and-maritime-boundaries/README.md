# 200NM EEZ and Maritime Boundaries — working local draft

The existing EEZ Dagster asset now stages complete NOAA raw responses, a deterministic native candidate and a typed GeoPackage for the normal catalog/format graph. The user explicitly authorized draft implementation while acceptance remains pending; [scope update](https://github.com/fulton-ring/hifld-next-datasets/issues/3#issuecomment-6259996716). PR #420 remains a draft. **Blocked / Potential breaking change / Needs decision** still applies to real source/baseline/contract acceptance and promotion, not to this implemented local pipeline.

## Implemented behavior

`src/dagster_hifld/noaa_extract.py` owns bounded NOAA transport/application-error retries, exact raw body/failed-attempt retention, independent before/after count and ID reconciliation, complete schema and transfer-limit validation, strict identifier/types and geometry/coverage drift quarantine. A layer exceeding the bounded 1000-row capability fails closed rather than pretending pagination succeeded. Duplicate JSON members and HTTP 200 ArcGIS errors are rejected. Proposed source snapshot hashes are review boundaries, not accepted contracts.

The explicit `noaa_200nm_eez_and_maritime_boundaries` asset requires `allow_unapproved_local_draft=true` and local staging; it rejects published/remote storage. Complete source field declarations and every ID remain intact. Native Arrow candidates retain all 14 source fields including SHAPE WKB, source Double UNILATERAL as float64, dates as epoch-ms int64, source strings/whitespace/nulls and exact vertices. No id/bbox is generated in that native candidate. A typed GeoPackage retains all 13 source attributes and geometry for the existing processing graph; SQLite's internal feature key never substitutes for source identifiers.

Draft version namespaces bind complete raw hashes, full installed pipeline-code fingerprint, proposed schema/ID/geometry/coverage/count policy and retrieval URLs. Identical source/code is idempotent; changed processing gets a separate immutable namespace. Reentry validates complete provenance semantics, preserving original retrieval/code attribution only for identical installed code and appending refresh receipts separately. Seven raw inputs, checksums, source roles, exact retrieval URLs, retrieval times, release uncertainty and transformations flow into the source manifest and data dictionary.

The shared #417 implementation is an explicit dependency, through `62977c91ebaf2740f654f3be7cc98490b607b224`. Generated formats run before final `publish/verify`; no independent approval registry means the actual graph fails there and skips promotion. No real registry, source approval, baseline acceptance or contract decision was authored.

## Full real-input materialization evidence

`evidence/draft-integration-result.json` pins tested code, correct worktree module path, actual ingest/graph run IDs, fresh seven-request receipts, isolated state/storage, output hashes, exact final gate error and previous-release fixture preservation. `evidence/draft-terminal-audit.json` compares every real row, field, physical Python attribute type and ordered geometry vertex across native candidate, GeoPackage and terminal GeoParquet. All 96 IDs and all 13 attribute values/types and all vertices match. Catalog quality passes, PUB_DATE is correctly documented as integer and all seven source inputs remain in the dictionary. Terminal GeoParquet explicitly represents SHAPE as geometry and adds derived bbox; this proposed transformation still requires acceptance. PMTiles is a visualization with tiled/quantized geometry, not a lossless source replacement.

The initial full graph exposed a genuinely lossy Shapefile export despite conversion STEP_SUCCESS: NOTE changed on 20 rows, SUPP_INFO on 1 row (OBJECTID 1097, 500-character source string), SHAPE.LEN rounded on 26 rows and nullable date integers became DBF floats. Complete findings are in `evidence/initial-derived-loss-audit.json`. The version manifest now excludes Shapefile with a concrete full-data reason. The shared format hook records a skip before producing a ZIP, and the release boundary rejects stale excluded artifacts. Final fresh staging has **no Shapefile ZIP**. Strings and IDs were never shortened to make the format pass.

The previous public v1 candidate is copied into isolated local published storage solely as a preservation fixture, not accepted as a baseline. Failed promotion leaves its exact bytes intact, produces no new published files and preserves all draft/raw evidence. Real acceptance remains the decisions in [DECISION.md](DECISION.md).

## Reproduce

Use the frozen worktree environment and correct module resolution:

```bash
UV_CACHE_DIR=/workspace/.cache/uv uv sync --frozen
PYTHONPATH=$PWD/src UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python -m unittest discover -s tests -p 'test_noaa*.py'
PATH=/workspace/.tools/tippecanoe/usr/bin:$PATH PYTHONPATH=$PWD/src UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python scripts/verification/noaa_eez_local.py --runtime-root /workspace/work/hifld-worker-3/new-isolated-run --capture-directory /workspace/work/hifld-worker-3/live-capture --previous-candidate /workspace/work/hifld-network-retry/data/200nm-eez-and-maritime-boundaries.parquet
```

The runner requires an empty runtime directory, registers the actual dynamic partition and executes catalog, format, checks and verification assets. Omit `--capture-directory` for bounded live NOAA fetches; its successful complete bodies and failed HTTP 200 application-error attempts are retained. The recorded current capture independently contains both count and ordered-ID requests before and after extraction. Historical unit replay intentionally reuses count/ID fixtures across before/after slots and is labeled accordingly; it is not presented as independent live evidence. Historical complete raw captures already committed remain replayable with `audit.py` and `audit.py --quality`.

`evidence/draft-validation.json` records focused/full-suite outcomes. Full-suite success and CI success are not claimed: missing sibling `hifld-next-iac/environments/prod/dagster.tf` remains an explicit limitation, and the production test is skipped. Earlier startup-only readiness reports are historical and do not substitute for the materialization evidence above.

## Producer and baseline research retained

NOAA InPort39963 now responds HTTP 200 at `https://www.fisheries.noaa.gov/inport/item/39963`, explicitly identifies Office of Coast Survey producer/distributor and links the exact MapServer. Visible publication/revision precision is year 2013; schema.org emits 2013-01-01 without establishing day precision. Service version 4.1 states 2013-09-13. “Update to dataset forthcoming,” continually maintained status and 2026-08-20 metadata modification do not establish a 2026 data release. Legacy XML still returns 503. DataLumos archive 238491 and crosswalk 241367 now return provider 403 challenges without CONNECT denial; exact DHS XLSX/embedded-link continuity remains unresolved. See network-retry/current-requests manifests. NOAA warns NOT FOR LEGAL USE.

The complete v1.0.0 GeoParquet candidate remains preserved unchanged outside Git at `/workspace/work/hifld-network-retry/data/200nm-eez-and-maritime-boundaries.parquet`: 416471 bytes, generation 1791506335103859, SHA256 `3de5e54cbdcf80a70b0c085fef69598b0e24f2ef9aa17dfa43f641113fdbfd33`. `evidence/published-candidate.json` pins its generation URL/catalog/checksums; this is not acceptance. `compare_candidate.py --baseline <preserved-file>` reproduces the full baseline-candidate comparison after checking input hashes. All 96 IDs and 13 attribute values and ordered vertex sequences agree, while physical differences include UNILATERAL int64 versus source Double, extra id/bbox, SHAPE→geometry and 80 geometry-type differences (source 90 LineString/6 MultiLineString versus candidate 10/86). These remain explicit contract/migration choices.

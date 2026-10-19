# Consolidated Cities: complete Census 2026 draft

This branch implements a complete source pipeline. It does not accept the source edition, published baseline, schema, ID mapping or a release. The user's corrected scope permits reviewable draft code and isolated materialization before those decisions; the original issue checklist does not authorize acceptance by this worker.

The exact inventory description names the 2024 **substate** geographic geodatabase, `Consolidated_City`. The smaller national GDB used by issue #40 has no such layer. The official 2024 directory lists the predecessor substate ZIP at 702 MiB; that full predecessor was not downloaded. This implementation uses the separately verified official TIGER 2026 CONCITY successor. All seven state ZIPs from the official directory are required; together they contain all eight consolidated-city records published in that edition. This is an unapproved edition change, not a claim of exact continuation.

Every ZIP, original receipt and all original shapefile members are retained losslessly. The original embedded ISO metadata identifies Census Geography Division, publication year 2026, creation 2026-09 and lastUpdate 2026-10. It does not give a publication day. Directory/HTTP modification times are packaging evidence. Embedded rights say the products are not copyrighted, require Census acknowledgement, and are statistical boundaries rather than legal land descriptions. The DOI crosswalk returned 403: its match is **inaccessible/unverified**, not unmatched. The historical agent-content-demos issue #40 and all three comments were read as historical leads.

The source has eight unique GEOIDs, fifteen DBF fields, seven Polygon and one MultiPolygon, all in native EPSG:4269. The DBF physically declares ALAND/AWATER as N(14,0) and preserves identifier strings and widths, including leading zeros. A separate physical DBF decoder compares every value against the GDAL result. The original file-local `OGC_FID` driver index is retained along with literal `source_archive`; it is not a producer DBF OID or a replacement for published IDs. Original mixed WKB is preserved without wrapping, repair, reprojection, rounding, filtering or deduplication. The GPKG `last_change` is a deterministic format sentinel, not a producer publication date.

The complete published candidate is retained unchanged, including every `OBJECTID`, `id`, `OID` and other field. Both sides contain eight records and the same eight GEOIDs, but **zero** native WKB, exact-coordinate or topological matches in their declared native CRS. Names, field sets, IDs and values differ. The complete per-GEOID report is [comparison.json](evidence/comparison.json). GEOID is only a diagnostic join. No rename/crosswalk or coordinate tolerance is approved or applied.

The explicit source asset, registry entry and STOPPED monthly refresh job use an isolated local staging resource. Every invocation validates receipts and shared standard provenance before canonical writes, validates the complete seven-file roster, archive CRC/member sets, physical schemas, IDs and geometry, and preserves partial/error inputs in quarantine. A hidden build is installed atomically only after complete output roundtrips. Reuse freshly re-reads all originals and compares complete rows, Arrow metadata and an independently regenerated deterministic whole GPKG, including auxiliary SRS, headers, envelopes and spatial indexes. Repinning a cache receipt does not establish fidelity. Missing/invalid ready markers and corrupt source evidence move the entire candidate intact outside discovery. First-construction provenance has a separate immutable capture so cache labels cannot rewrite its commit or original receipts.

Original shapefile ZIPs remain in raw staging. New consolidated shapefile and PMTiles outputs are explicitly excluded pending schema/visualization approval. Typed source Parquet, GPKG and normal terminal GeoParquet remain unapproved candidates; format-added geometry/bbox columns are reported separately.

Run the focused tests and actual isolated Dagster replay with an environment containing the repository's managed dependencies:

```bash
PYTHONPATH=src python -m unittest discover -s tests -p test_consolidated_cities_source.py
PYTHONPATH=src python scripts/verify_consolidated_cities_draft.py --runtime-dir /tmp/concity-isolated-run
```

The runner requires an empty local runtime directory and uses committed complete original inputs by default. It registers the actual dynamic version partition, runs the normal repository source job, two full catalog/format cycles and source refreshes, verifies all produced rows/types/IDs/native geometry/provenance, and tests failed quality and missing approval preserve a synthetic prior accepted fixture. No real baseline is marked accepted. Actual run IDs and suite results are added in the final evidence checkpoint; loading assets alone is not pipeline verification.

Release remains blocked on crosswalk/source-edition acceptance, an approved versioned raw/published contract and ID mapping, geometry/schema/format compatibility, and independent approval of the exact candidate. No merge, closure, production output, environment change, schedule activation or contract/baseline approval is authorized by these local results.

## Actual local verification

Frozen code `9ca31d9a9f06c85449d11bb591fea5982881ec5e` completed the actual registered Dagster graph in isolated local state, using all complete retained original inputs. Source and refresh succeeded; each normal catalog/format run stopped only at `missing_independent_release_approval_registry`. Source refresh succeeded after both catalog cycles, including hashed original-manifest snapshots and resolved provenance. The synthetic failed-quality run blocked promotion, and every raw/source object and the prior synthetic accepted fixture remained unchanged. All eight terminal rows preserve every source ID/attribute/type/native WKB and coverage.

- source: `34e01692-1d28-4124-9d91-6cd3e37012fe`, success.
- refresh: `9b2beda9-0a07-4697-af79-8dd6dd60f0cb`, success.
- normal_catalog_formats_cycle_1_unapproved_promotion: `5d2faa46-f4b4-4a1d-b46d-c40356f5910c`, expected gate failure.
- post_catalog_cycle_1_source_refresh: `c503d89c-3853-4931-98b5-f6192677e50b`, success.
- normal_catalog_formats_cycle_2_unapproved_promotion: `76b291db-c204-48a9-b132-b04d2cf7eb8c`, expected gate failure.
- post_catalog_cycle_2_source_refresh: `91630590-5f6b-4c32-a471-c94cae831ce4`, success.
- synthetic_failed_quality_blocks_promote: `73c5b9e8-ed01-4d5b-a866-c0b9798982fa`, expected gate failure.
- post_gate_source_refresh: `5306be9a-a1b8-43ca-86f1-a6abb44c02e2`, success.

The committed [materialization report](evidence/local-verification/materialization.json) and complete log contain assertions and run statuses. Focused tests: **8 passed**. Full-suite outcome will be recorded below when the actual run finishes. No GitHub CI success is inferred.

Actual full repository suite on the same frozen code commit: **514 tests, 513 passed, 1 skipped, zero failures/errors**, 80.046 seconds. The real unmodified sibling IaC snapshot at `56aa10757129c457c530d707d9a40bae13e0907f` was present; its former absence is a historical limitation, not suppressed by a fixture. Tippecanoe was available. Complete output is [full-suite.log](evidence/local-verification/full-suite.log). These results do not assert CI success or real dataset acceptance.

## Live HTTP transport followup

Code `c6f9532edfd2056bba5b330ab6d58de0dc2e8185` corrects an HTTPX transport bug: `Content-Length` counts the encoded wire body, whereas `iter_bytes` returns decoded bytes. Requests now ask for `Accept-Encoding: identity` and still compare any declared length against HTTPX's actual `num_bytes_downloaded` if a server compresses anyway. Receipts separately record encoding, declared wire length, actual downloaded wire bytes and decoded body length/SHA. Stream errors retain yielded decoded partial bytes and each failed attempt's receipt; missing `Content-Length` is supported.

Twelve focused tests pass, including complete gzip responses whose decoded length differs, compressed length mismatch, premature gzip stream errors retaining partial bytes/three attempts, and an existing no-length/no-counter response test double. These regressions use real HTTPX decoders and downloaded-byte counters.

Actual LIVE acquisition fetched all eight official Census inputs with 200 responses into isolated raw-only storage. The directory was chunked with no declared length (15,684 wire/decoded bytes); all seven ZIP wire/decoded lengths match their declared lengths. No Content-Encoding was observed on those identity requests; gzip handling is a separately tested case, not a claim about those live responses. Every live ZIP remains byte-identical to the original retained publication. Fresh complete decoding again produces all eight records and exact original typed values, IDs, field metadata and native WKB. Freshly regenerated whole GPKG and typed Parquet are byte-identical to the previously materialized payloads. AST comparison confirms only `acquire` changed; no source writer, schema, identifier, geometry, cache or gate function changed.

See [validation.json](evidence/transport-followup/validation.json), the complete live receipts and focused-test log. The earlier full 514-test suite and eight actual Dagster runs remain evidence for their explicitly recorded code commit; they were not broadly rerun for this transport-only followup. No new terminal materialization, dataset acceptance or production write is claimed by the raw-only live probe.


## Authenticated original construction repair

Actual isolated audit on prior0721head found co-forged external/canonical nonexistent code_commit accepted in registered run `dde3f065-808d-4266-a8b3-5c403846dcae`; raw/prior data stayed intact. Source/test checkpoint `f300cb5646042079696f28b5ec9c5043f6afb628` integrates independently reviewed #417 reachable-Git/blob verification. Fresh and reused original manifests must match all actual processing blobs. Missing/malformed original bundles cannot bootstrap from a cached candidate and quarantine the entire candidate. First bundle installation occurs only after complete native private outputs and ready inventory pass, under the same installation lock; interrupted private builds retain raw evidence and establish no bundle.

Fourteen focused tests pass (23.782s). The full local suite passes536/537 with one opt-in production skip (120.556s), using the actual unmodified siblingIaC snapshot56aa107. Sixteen actual registered Dagster runs cover all8native rows, two normal catalog cycles stopping only at missing independent approval, failedquality preservation, four provenance/anchor denials and four original-input rebuilds. Complete native GPKG/Parquet bytes and four extraction/writer/fidelity ASTs are unchanged from the independently audited earlier source. Full original capture receipts and manifest survive each recovery. Genuine documentation-only later-HEAD reuse/rebuild and independent final-head review follow this checkpoint. Reproduce the lifecycle with the existing normal runner and then `evidence/authenticated-construction/replay_provenance.py --runtime-dir <same-runtime> --repo <checkout>`. Exact run IDs/logs/pins are in that directory. All real source, baseline, contract, geometry/schema/format and release acceptance remains external.

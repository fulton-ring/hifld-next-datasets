# Administrative Areas (USACE IENC): research for #6

Research updated 2026-10-19 UTC. **Blocked / Potential breaking change / Needs decision.** Baseline retrieval and completed geometry research advance PR #423; no dataset acceptance, schema approval, ingestion change or production write is made.

## Network and provenance

Runtime policy is unrestricted and enforced, current observed revision 6. New requests traverse the inherited proxy successfully. DataLumos crosswalk redirects return provider-side HTTP 403/Cloudflare, and official AGC navigation/root/factsheet requests return CDN HTTP 403. These are distinct from the earlier CONNECT denials, which remain historical evidence only. ArcGIS organization metadata still returns HTTP 200 with JSON error 403. See `retry-pages.json`, `retry-additional-pages.json` and retained response bodies. The exact XLSX crosswalk/hyperlink match, independent USACE delegation to the actual service/account, source terms, and real layer release date remain unresolved. The master item’s `(16-Sep-26)` snippet is an assertion, not an independently verified release; portal edits and retrieval dates are not substituted.

The historical inventory/portal URL ending `/ADMINISTRATIVE_AREA/MapServer/0` now responds with ArcGIS JSON 404. Inspecting its reachable **service root** found the administrative layer at **56**, with the original USACE IENC/S-57-to-99-class-geodatabase description. Its complete **2,966 attributes** were retrieved in two ordered pages (2,000 + 966), reconciled against all IDs/count. Across all nine shared non-OBJECTID attributes its row multiset equals the master candidate’s. However, an OBJECTID join produces **532 differing values across 302 rows**. Identical counts and ID domains therefore do not establish stable row identity. No ID remapping is accepted. Historical-service geometry was not independently compared in full; shared attribution is not proof of maintained official delegation. `historical_compare.py` replays this finding offline.

## Complete pinned baseline and source comparison

The complete published `v1.0.0` GeoParquet is now preserved as an **unapproved baseline candidate**, with exact URL, GCS generation, byte length and SHA-256 in `evidence/published-candidate.json`. This read does not accept the baseline. Complete source geometry is preserved in `/workspace/hifld-worker-runtime/6/complete-source`: **119 exact-ID GeoJSON pages**, at most 25 features per request, no precision/simplification parameters, no transfer-limit truncation. Count/all-ID sets are equal before and after extraction. Every returned property exactly matches the prior complete 2,966-row attribute capture. All pages are hashed in `evidence/geometry-pages.json`; compressed full pages remain outside Git and must be preserved for offline replay/acceptance review. Git retains bounded per-feature ID/geometry hashes and complete comparison results.

| Observation | Source candidate | Published candidate |
| --- | --- | --- |
| Complete rows | 2,966 | 3,131 |
| Geometry | 2,966 valid, nonnull, nonempty Polygons | 3,131 valid, nonnull, nonempty Polygons |
| `OBJECTID` | 2,966 distinct integers | 3,131 distinct int64 values |
| `Feature_ID` | 2,781 distinct strings, no nulls | Absent |
| Name field | `Object_Name` | `Object_Nam` |
| Additional published identifier | No verified counterpart | `id`, 3,131 distinct int64 values |

All column names/types/null counts/value-domain differences and raw categorical counts are recorded in `complete-comparison.json`. Published truncated field names remain unmapped; apparent shapefile truncation does not authorize a mapping. Preserve all source/published identifiers, embedded spaces, leading-zero segments and precision. Even `(Source_Dataset, Feature_ID)` is nonunique: 2,937 distinct combinations in 2,966 rows. No deduplication or generated replacement IDs are proposed.

Full geometry bounds are nearly identical, approximately [-94.44324,29.66978,-79.46556,42.78222], which alone does not establish equivalent coverage. Exact normalized coordinate hashes have zero matches; the service-generated GeoJSON uses different decimal formatting from GeoParquet, so diagnostic comparisons are reported separately. Explicit **diagnostic** precision grids of both `1e-9` and `1e-7` degrees each find **2,757 matching geometry occurrences**, leaving **209 source and 374 published occurrences unmatched**. These derived diagnostic copies neither replace originals nor establish an approved tolerance. Hash inequality does not prove topological inequality; no geometry repair, topology-changing conversion, or tolerance is approved. The difference is substantive enough to require a concrete migration decision rather than claiming continuation from counts/extents.

## Required decisions

`source.json` retains an explicitly unapproved proposed source schema and a concrete migration decision checklist. A maintainer must first accept or reject the exact pinned published candidate, decide stable/nonunique ID semantics, approve a versioned full column mapping (including the absent `Feature_ID` and unmapped published `id`), and review the 2,966-vs-3,131 geometry/coverage change. Independent producer delegation, the original crosswalk and real release evidence remain source prerequisites. PASDA and DataLumos remain separately labeled archives; this research does not implement archive refresh. Shared verifier/promotion work is tracked in #417 and does not require accepting this dataset in advance.

## Replay and local readiness

From this worktree, dependencies were installed with `UV_CACHE_DIR=/workspace/.cache/uv uv sync --frozen`. No manifest/lockfile change was needed. Offline commands:

```bash
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/administrative-areas-usace-ienc/extract.py --offline
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/administrative-areas-usace-ienc/historical_compare.py
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/administrative-areas-usace-ienc/complete_audit.py --runtime /workspace/hifld-worker-runtime/6/complete-source --baseline /workspace/work/hifld-network-retry/data/administrative-areas-usace-ienc.parquet
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/administrative-areas-usace-ienc/validate.py
```

`complete_audit.py --capture` re-downloads the current candidate; a changed source can intentionally fail the pinned prior-attribute equality check. It does not overwrite or approve a historical baseline. Preserve the runtime source pages and pinned baseline, because Git intentionally contains bounded evidence rather than the complete large objects. SHA256SUMS covers all committed research artifacts.

Actual Dagster webserver and code-server readiness was rerun from this worktree’s frozen environment, with isolated `DAGSTER_HOME=/workspace/hifld-worker-runtime/6/dagster`, local staging/published directories, both bucket variables unset, own Crawl4AI state and tippecanoe PATH. `/server_info` returned HTTP 200 (Dagster 1.12.19), and GraphQL returned `RepositoryConnection` with this ingest asset loaded. Exact new responses are retained in `readiness-retry*`; the owned server was stopped. The first shell attempt lacked local socket permission; repeating with the supported command network grant loaded successfully. This is research loading readiness only. No source ingest/catalog/publish materialization is claimed; implementation/promotion integration and failed-promotion preservation are owned by #417. The full repository suite and CI were not run for this documentation/evidence change. The missing sibling `hifld-next-iac/environments/prod/dagster.tf` remains an explicit limitation.

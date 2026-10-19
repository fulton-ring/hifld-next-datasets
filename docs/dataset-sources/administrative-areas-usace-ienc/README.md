# Administrative Areas (USACE IENC): research for #6

Research only, checked 2026-10-19 UTC. **Blocked / Potential breaking change / Needs decision.** No ingestion, approved contract, published schema, workflow, or production change is made.

The inventory row is pinned by full-file SHA-256 in `evidence/inventory-row.json`. The exact DHS crosswalk match remains **unverified**, not unmatched: DOI 10.3886/E241367V1 redirects to www.datalumos.org, where the inherited proxy denies CONNECT with 403. The original XLSX could not be retrieved, so displayed values and hyperlink targets could not be parsed. Historical agent-content-demos#40 findings are captured as leads only; their title matching does not establish this join.

## Candidate and authority limits

ArcGIS search independently found item `6ee858a220a143a48c8d40a6cf0d3680`, `USACE_IENC_Master_Service`, owned by `usace_agc_dkd` (public profile Dale Dodson). Its service contains the exact named `ADMINISTRATIVE_AREA` layer at **56**. The historical exact-title HIFLD item `3d053589d0ad4e1a9df46d33382e39cb` and `ADMINISTRATIVE_AREA` item `c04e3977dbae4f949425faa22cca0cf1` reference the old ienccloud service. These are candidate lineage clues, not an accepted crosswalk/source replacement.

Attribution credits USACE, Army Geospatial Center, US Coast Guard and IIC Technologies. Authority remains unresolved: organization metadata returned **HTTP 200 with ArcGIS JSON error 403**; the AGC landing and old ienccloud host were policy blocked. No independent producer link/delegation to this specific service was obtained. A plausible account name, attribution, or portal title does not establish authority. PASDA's current ingest URL and DataLumos remain separately labeled archives, never maintained agency feeds.

The candidate item snippet states `(16-Sep-26)`. This is an asserted service vintage, not an independently verified layer release. `release_date` remains null. Portal modification and layer data-edit timestamps are retained separately; neither the 2024-02-05 inventory issue date nor the 2025-04-08 inventory edit is treated as a current release. Terms remain unresolved (candidate licenseInfo empty).

## Extraction and identity

Anonymous requests on 2026-10-19 returned count **2,966**, all 2,966 distinct non-null integer OBJECTIDs, and 30 ordered attribute pages of 100 rows (last 66), with exact count/ID reconciliation. The ID set was rechecked after extraction and was unchanged. Service limit 2,000 and pagination support are captured. Pages preserve all 12 original fields and raw JSON types/values; no schema normalization, whitespace trimming, deduplication, or replacement IDs occurs. Final page reports no exceeded transfer limit. This is a bounded, complete **attribute** extraction, not a geometry-complete download.

`Feature_ID` is a string with embedded spaces and leading-zero segments (for example `U3 0000005407 00011`). It has **2,781 distinct values**: 185 repeated rows beyond first. Even `(Source_Dataset, Feature_ID)` has only **2,937 distinct pairs**: 29 repeated rows beyond first. Neither is accepted as a unique published key; OBJECTID is service-assigned and cross-release stability is unverified. See `identity-audit.json` and `extraction-summary.json`. Empty strings remain distinct from nulls. No unexpected fields were observed; raw unknown fields must be retained and trigger review on future releases.

The first 100 unsimplified geometries exceeded the 3 MB per-request capture bound; the initial 2,000-feature attempt exceeded 2 MB. These failures are not counted as complete captures. One exact raw polygon sample for OBJECTID 2966 is retained in `geometry-sample.capture`. Layer metadata advertises polygons, EPSG:4326, and extent approximately [-94.4432,29.6698,-79.4656,42.7822]. These are advertised coverage, not a full geography/geometry audit. Full geometry null/validity/topology/category and state/chart coverage comparison remains unverified. Attributes report 81 Source_Dataset values and nationality US, without proving all expected charts or states are included.

## Contract and decision

`source.json` proposes an **explicitly unapproved** identity mapping for all 12 candidate columns, their source types/nullability/meaning/ID role, plus geometry/CRS/coverage. No accepted HIFLD Next baseline was available. No schema equivalence, exact continuation, or row/geometry continuity claim is made. Do not substitute this candidate for the existing archive ingest until the crosswalk/authority/release/complete-geometry gaps, nonunique IDs, approved versioned baseline and #417 comparison/promotion prerequisite are resolved. A maintainer must approve any source/schema/ID migration; research approval cannot approve ingestion. Refresh cadence is unknown rather than inferred from portal edits.

## Reproduction and local readiness

Run from this assigned worktree. Existing `uv.lock` was installed with `UV_CACHE_DIR=/workspace/.cache/uv uv sync --frozen`; editable module resolution points to this worktree. No full repository suite was run for this documentation-only change. The known sibling IaC fixture failure is not suppressed and no new full-suite pass is claimed.

Offline replay of retained gzip raw JSON pages (no network):

```bash
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/administrative-areas-usace-ienc/extract.py --offline
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/administrative-areas-usace-ienc/validate.py
```

`research.py` records dated anonymous request URLs, parameters, statuses, JSON errors, failures and hashes in `evidence/requests.json`. `extract.py` without `--offline` repeats live candidate attribute extraction; this is optional and does not replace pinned replay evidence. The committed captures are bounded historical evidence, with SHA-256 in `SHA256SUMS`. No huge datasets are committed.

Actual local readiness used an isolated worktree environment, `DAGSTER_HOME=/workspace/hifld-worker-runtime/6/dagster`, local `staging`/`published` directories, both bucket variables unset, `CRAWL4_AI_BASE_DIRECTORY=/workspace/hifld-next-datasets/.local`, shared UV cache, and tippecanoe PATH. The command was:

```bash
env -u HIFLD_STAGING_BUCKET -u HIFLD_DATASETS_BUCKET UV_CACHE_DIR=/workspace/.cache/uv CRAWL4_AI_BASE_DIRECTORY=/workspace/hifld-next-datasets/.local DAGSTER_HOME=/workspace/hifld-worker-runtime/6/dagster HIFLD_STAGING_DIR=/workspace/hifld-worker-runtime/6/staging HIFLD_DATASETS_DIR=/workspace/hifld-worker-runtime/6/published PATH=/workspace/.tools/tippecanoe/usr/bin:$PATH uv run --frozen dagster-webserver -m dagster_hifld.definitions -h 127.0.0.1 -p 3406
```

`GET /server_info` returned HTTP 200 with Dagster 1.12.19; a GraphQL `repositoriesOrError` query returned `RepositoryConnection` and loaded assets including this dataset's ingest asset. Exact bounded responses are retained. No daemon, source asset materialization, catalog/publish run, production settings, or deployment was requested. Readiness proves only loading, not ingestion correctness. The owned webserver was stopped after verification.

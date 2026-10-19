# Airport Area (USACE IENC): research only

Issue #9 remains **Blocked / Needs decision**. Potential breaking change review is required before replacing an accepted dataset. No ingestion, contract approval, or materialization is included.

The inventory identifies `airport-area-usace-ienc`, US Army Corps of Engineers, publisher unspecified, DataLumos project 238564 V1. The original DHS crosswalk XLSX could not be obtained: its DOI and archive requests failed with `Tunnel connection failed: 403 Forbidden`. Matching is **UNVERIFIED**, not unmatched. Hyperlink targets and original row identity have not been reconciled. Historical agent-content-demos#40 was read as leads only; its body/comments do not establish this dataset's current endpoint.

Anonymous ArcGIS discovery found master item `6ee858a220a143a48c8d40a6cf0d3680`, owner `usace_agc_dkd` (public profile Dale Dodson). Service metadata identifies polygon layer **57 `AIRPORT_AREA`**, distinct from layer 4 `AIRPORT_AREA_POINT`. The layer's description defines an airport/airfield area containing a runway. This is named layer identity, not evidence of exact archive continuity. Item attribution names USACE, Army Geospatial Center, US Coast Guard and IIC Technologies. Independent official publication/delegation evidence remains missing: organization metadata returned HTTP 200 containing ArcGIS error 403; the agency page was blocked by CONNECT403. Consequently `source_role` remains `unresolved`. No mirror was promoted to official status.

The item snippet states `(16-Sep-26)`; this is an unverified edition claim, not an accepted release date. Item modified/layer lastEditDate are portal timestamps. `release_date` remains null. This layer represents charted inland navigation coverage, not all US airports.

## Captured extraction and replay

On 2026-10-19 UTC, anonymous layer metadata, count, complete ID list and a full attribute/geometry response were retained under `evidence/`. `requests.json` records URLs, UTC times, response status, byte counts and SHA256. Failed CONNECT captures contain the exception string, not an HTTP response body. No credentials were sent or retained. Requests used Python urllib with inherited proxy/TLS and 40–45 second timeouts.

Run from repository root:

```bash
python docs/dataset-sources/airport-area-usace-ienc/replay.py
```

Replay checks capture hashes, rejects mismatch, reconciles 72 features against count and all 72 unique nonnull OBJECTIDs, compares raw field sets and verifies no exceeded transfer limit. The 72 rows fit below the advertised record limit; no additional page is required. Raw feature capture is about 227KB and retains all rings/attributes. Reconciliation reports every field's null/distinct counts, chart dataset/category/source-date distributions, full coordinate bounds, empty geometries, unclosed rings and coordinate-range checks. OGC topology has not been evaluated. OBJECTID is service-maintained; no cross-release stability is claimed. Feature_ID is preserved as text; it is not promoted to an accepted matching key. This is a captured-snapshot completeness result; no accepted-baseline comparison or future service stability is implied.

`source.json` proposes all 11 raw columns plus geometry, with exact source types, schema nullability, provisional meanings and ID roles. It is explicitly **PROPOSED_UNAPPROVED**. Unknown dictionary semantics, the unavailable accepted HIFLD Next version/dictionary, crosswalk match and archive attributes prevent a published compatibility decision. Retain all raw identifiers/unknown fields; quarantine drift and require maintainer decisions rather than silently changing the public schema. #417 and approved versioned contracts remain prerequisites.

## Local readiness

`uv sync --frozen` succeeded in this worktree, with `UV_CACHE_DIR=/workspace/.cache/uv`. `uv run --frozen dagster dev -m dagster_hifld.definitions -p 3409` used `DAGSTER_HOME=/workspace/hifld-worker-runtime/9/dagster`, separate local staging/published directories and unset bucket variables. On 2026-10-19 UTC, `/server_info` returned HTTP200 (Dagster 1.12.19); GraphQL `repositoriesOrError` returned `RepositoryConnection`, `__repository__`, and 73 loaded asset nodes. Exact response captures are retained. No dataset was materialized. This verifies local startup/loading only. No full suite was run for this documentation-only change; it does not claim pipeline correctness or resolution of the known missing sibling Terraform fixture.

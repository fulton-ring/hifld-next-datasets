# AIANNH Areas: working draft pipeline implemented; release unapproved

## Current implementation scope — 2026-10-19

The user authorized faithful draft implementation while baseline/grain/contract acceptance remains pending. This supersedes the earlier research-only scope below. Existing PR #426 is a **draft**, stacked on shared verifier PR #421 (`62977c91ebaf2740f654f3be7cc98490b607b224`). Worker `/root/aiannh_8` continues the existing issue #8 branch.

`src/dagster_hifld/sources/aiannh.py` implements the exact inventory-linked **TIGER2023** source. Default anonymous HTTP requires complete HTTP200, bounded size, exact9,086,993 bytes and reviewed SHA256. It validates the exact7archive members/CRC, DBF/SHP count, all862rows, original physical schema, complete attributes/IDs, native geometry/coverage and independent retained reference digests.2025/2026, changed bytes, incomplete/error payloads, schema/type/ID/geometry/coverage drift and failed derivative checks go to a separate quarantine namespace. Draft ingestion rejects remote and published resources before network or writes.

The original ZIP and every sidecar/member remain unchanged under `raw/`. Sorted typed source records preserve every identifier, leading zero, integer, string and native EPSG:4269 coordinate. A source-specific GeoPackage keeps the original561Polygon/301MultiPolygon mix: the generic writer had changed all features to MultiPolygon. GeoPackage's internal fid is a storage primary key and never becomes a source attribute. Storage-only `gpkg_contents.last_change` is fixed to the source release date for deterministic bytes; original producer metadata remains raw. Existing shared GeoParquet conversion preserves all source fields/types and original mixed geometry; its declared covering `bbox` is separately retained and checked for every record. No combined M polygons, column aliases, surrogate source IDs, projection or simplification are introduced.

Regenerated Shapefile is explicitly excluded through the shared format hook. A real local regeneration probe changes **all16 physical DBF field type/width declarations** (for example `GEOID str:5→str:80`, `NAME str:100→str:80`, `ALAND int:14→int:18`) and omits both producer ISO XML sidecars. The complete generated artifact is retained outside the candidate in `/workspace/work/hifld-coordinator/aiannh-8/regenerated-shapefile-quarantine/`; its report is committed in `evidence/implementation/regenerated-shapefile.json`. This does not assert current observed text-value truncation: the physical schema differs and the narrower NAME declaration changes future representable values. Original producer Shapefile ZIP stays intact. PMTiles is an unsupported visualization derivative pending a separate fidelity/precision decision.

The registered `aiannh_source_integrity_refresh` job selects only this ingest. Its `aiannh_tiger2023_monthly_integrity_refresh` schedule is **STOPPED** by default, with cron `0 3 1 * *` in `America/New_York` (03:00 on the first day of each month). It retries fixed2023 integrity into isolated local draft staging. It schedules no conversion, annual source replacement or publication. Operators must explicitly configure isolated local storage and activate the schedule; this code does not activate it or approve deployment/release.

Reproduce actual local materialization from preserved producer bytes without funding any model/API worker:

```bash
uv run --frozen python scripts/verification/aiannh_draft_integration.py \
  --archive /workspace/work/hifld-coordinator/aiannh-8/zip2023.body \
  --workdir /workspace/work/hifld-coordinator/aiannh-8/local-replay
AIANNH_TEST_ARCHIVE=/workspace/work/hifld-coordinator/aiannh-8/zip2023.body \
  uv run --frozen python -m unittest discover -s tests -p test_aiannh_source.py
```

The integration script uses durable isolated Dagster state, explicit registered publish partitions and local staging/published resources, then decodes and verifies every862feature in each generated supported format with the shared full-row comparator. It creates **no Census release approval**. Missing approval fails promotion without changing any staged/published bytes; an injected failed quality check also remains false and publication stays denied. Because the missing registry is checked first, that real-data second denial does not establish a quality-specific approval scenario; shared verifier tests cover quality-specific failure using synthetic test-only acceptance.

The concrete remaining decisions block **release**, not ongoing draft implementation: accept/reject the pinned legacy published baseline; select2023 components vs2025 legacy grouped lineage vs separately versioned successors; approve the complete source/legacy column and ID mapping, M versus R/T feature grain, geometry/CRS/precision/invalid policy and exact release inventory. DataLumos crosswalk/archive remains an independent research gap. No contracts/baselines have been approved and no production data, issue closure or GitHub merge is authorized by these tests.

Exact implementation validation (`3aed282`, with all pipeline source hashes recorded in `evidence/implementation/execution.json`): all862source features passed native GeoPackage and GeoParquet comparison with **zero added, removed, attribute, geometry or coverage differences**. Raw SHA256/sidecars and both failure-preservation scenarios passed.11 targeted tests passed, including source/CRC/ID/type/coverage/geometry/incomplete-response quarantine, exact rerun, local-only resource guard and inactive schedule evaluation. The full suite was attempted at preceding implementation commit `bc929f6`:476tests,3errors,2skipped. Errors were missing sibling IaC and two missing `tippecanoe` executable failures. No full-suite or CI pass is claimed. Logs and exact run IDs are committed.

The physical Shapefile probe can be repeated separately with `scripts/verification/aiannh_shapefile_probe.py --geopackage <native-candidate.gpkg> --workdir <separate-quarantine-directory>`; it never publishes the derivative.

## Research and baseline comparison record

## Network retry and complete published comparison — 2026-10-19

Worker `/root/aiannh_8`, issue #8 / existing PR #426. Coordinator: https://chatgpt.com/local/01a15273-e90b-7211-b493-3ad4668ed0e8.

Runtime status reports current observations, enforced unrestricted policy at revision 6 and ready repository credential. No environment change was made. All three original Census ZIPs were downloaded again anonymously and **match the historical SHA256 bytes exactly**, restoring full geometry re-audit. They are preserved in `/workspace/work/hifld-coordinator/aiannh-8/zip{2023,2025,2026}.body`; existing captures remain intact. Official Census landing-page retries now return HTTP 403, while the `www2.census.gov` national ZIPs return HTTP 200. Original successful landing captures remain release-date evidence; no fresh release-page verification is inferred. DataLumos DOI/archive retries return HTTP 403 Cloudflare challenges, with no CONNECT tunnel denial. See `evidence/retry/refetch.json` and `retry-requests.json`.

The complete published `v1.0.0` Parquet candidate is 8,263,475 bytes, SHA256 `878e40fea860e5dcd2979e391c5b70fad0d5290e7f8b3785b5831b6eae40e1c4`, GCS generation `1791506346721167`. It is a **candidate, not an accepted baseline**. Full decoded rows, every published ID/attribute and geometry digests are retained in `published-rows.json.gz`; the unchanged complete Parquet is preserved at `/workspace/work/hifld-network-retry/data/aiannh-areas-1.parquet`. The pinned collection and both source manifests are retained. Those source manifests repeat the inventory's 2023 URL but do not establish actual acquisition lineage.

| Complete comparison | Published candidate | Census 2023 | Census 2025 | Census 2026 |
| --- | ---: | ---: | ---: | ---: |
| Rows | 704 | 862 | 867 | 867 |
| Unique area codes (`AIANNH` / `AIANNHCE`, diagnostic only) | 704 | 703 | 704 | 704 |
| Shared area codes with published | — | 702 | 704 | 703 |
| Shared exact `GEOID` | — | 540 | 541 | 540 |
| Invalid geometries | 38 | 0 | 0 | 0 |

The row-count difference is substantially a **different feature grain**: 163 published `AIANNHCOMP=M` rows have combined reservation-and-trust-land names; source2025 instead has separate R/T components. All 704 source2025 area codes, GNIS identifier sets, and **summed** `ALAND`/`AWATER` values equal the published `AIANNH` / `AIANNHNS` / string `AREALAND`/`AREAWATER` values. This is a strong grouped-2025 lineage signal, not proof of exact geometry provenance or an approved join/normalization. Source2023-only code is `0990`, published-only codes are `3015`, `3695`; source2026-only is `3075`, published-only is `8700`. Codes and leading zeros are preserved verbatim.

The candidate has 28 physical columns including geometry and `bbox`; source has 16 attributes plus geometry. Candidate IDs `AIANNHFP1/2/3`, `OID`, `OBJECTID`, `id`, etc., and all other legacy columns remain visible in the complete schema/diff. Source has distinct names/types such as `AIANNHCE`, `CLASSFP`, `COMPTYP`, `ALAND`, `AWATER`, `GEOIDFQ`, `NAMELSAD`; no aliases or generated IDs have been approved. Full null distributions and exact shared-field differences are in `evidence/retry/comparison.json`.

Native geometry CRS differs: EPSG:3857 published versus EPSG:4269 source. Diagnostic reprojection of the complete candidate to EPSG:4269 finds zero exact topological matches among the 527/528/527 valid exact-GEOID pairs for2023/2025/2026; 13 invalid published pairs per vintage are excluded without repair. Exact equality is sensitive to projection/coordinate precision; this **does not measure the magnitude or significance** of boundary changes. No tolerance, repair, reprojection or simplification is approved for publication. No automatic change of grain or dissolved output is authorized.

**Concrete maintainer decision:** identify whether this pinned `v1.0.0` object is the accepted reference; choose fixed2023 components, the2025 lineage/grain candidate, or separately versioned annual successors; approve an explicit all-column/ID mapping, geometry CRS/precision/invalid policy and migration of combined M features versus R/T components. The current source-derived proposal is unapproved and cannot silently replace legacy columns. Retain **Blocked / Potential breaking change / Needs decision** while independent #417 verifier work advances. The DataLumos/crosswalk block remains separate.

Reproduce the complete comparison without live fetch:

```bash
uv run --frozen python docs/dataset-sources/aiannh-areas-1/compare_published.py \
  --baseline /workspace/work/hifld-network-retry/data/aiannh-areas-1.parquet \
  --archive-dir /workspace/work/hifld-coordinator/aiannh-8 \
  --output /tmp/aiannh-comparison.json \
  --rows-output /tmp/aiannh-published-rows.json.gz
```

`uv sync --frozen` succeeded without dependency/lockfile changes. Historical `audit.py` re-audited all matching raw archives with no retained-evidence diff, and `replay.py` passed. Actual isolated local Dagster repository loading succeeded: 73 assets and the existing AIANNH ingest loaded, with local staging/published storage and both production bucket variables unset. `evidence/retry/readiness.json` records this research readiness; no extraction/conversion/materialization or failed-promotion integration is claimed on this documentation/comparison branch. Full suite was not rerun; missing sibling IaC remains an explicit limitation. No pipeline/source/contract acceptance, production write, merge or issue closure occurred.

## Earlier independent research record

The following record and its files are retained as historical evidence. Current retry results and located candidate above supersede the earlier network/baseline availability statements.

Checked 2026-10-19 UTC. References #8. The exact inventory row explicitly links Census TIGER2023 `tl_2023_us_aiannh.zip`; the existing ingest uses that same vintage. This is a direct inventory-to-producer lead, **not** a verified DHS crosswalk join. The original XLSX was unavailable; DOI redirects to DataLumos project 241367 and fails with CONNECT tunnel 403. Hyperlink targets could not be parsed. Match status is **UNVERIFIED**, not unmatched. DataLumos project 238558 is a historical archive with unverified depositor/account and contents; its 2025-04-08 modification date is not a source release date.

Census landing pages link its TIGER archive, and downloaded embedded ISO metadata names Census Geography Division as producer/publisher, with Boundary and Annexation Survey lineage. Anonymous national-file downloads, CRC validation and full feature scans succeeded for 2023, 2025 and 2026. The obsolete inventory metadata URL returned 404, while embedded metadata is retained. Official page release statements are November 22, 2023; September 23, 2025; and October 14, 2026. These dates differ from metadata creation, portal review, directory modification and retrieval timestamps. The current landing page explicitly describes legal boundaries/names as of January 1, 2026. No delegated distributor or mirror was substituted.

| Vintage | Full rows | Unique GEOID / GEOIDFQ | Geometry |
| --- | ---: | ---: | --- |
| 2023 inventory-linked | 862 | 862 / 862 | 561 Polygon, 301 MultiPolygon |
| 2025 separate candidate | 867 | 867 / 867 | 552 Polygon, 315 MultiPolygon |
| 2026 current producer candidate | 867 | 867 / 867 | 548 Polygon, 319 MultiPolygon |

All scans found zero attribute nulls, invalid or empty geometries. CRS is NAD83 EPSG:4269. `AIANNHCE` and `AIANNHNS` repeat across components and must not be treated as row-unique. `GEOID` includes the component discriminator; leading zeros and strings remain intact. All 16 attributes and geometry are explicitly covered by the **unapproved** proposal in `source.json`. Observed zero nulls are not a producer non-null guarantee. Embedded 2023 GEOIDFQ definition misleadingly says “tribal census tract”; preserve its actual text and resolve that dictionary inconsistency before approval.

2023→2026, matched by observed unique GEOID only: 859 retained, eight added, three removed; 244 retained GEOIDFQ values changed, five CLASSFP values changed, and 378 geometry WKB digests changed. Geometry hashes do not establish topological differences. Complete ID sets, retained attributes, category distributions and bounds are retained in `evidence/rows*.json` and `audit.json`. Same counts/bounds do not imply equivalent population. Geographic/category coverage requires maintainer comparison against the accepted published baseline; broad national coverage alone cannot prove archive equivalence.

Publication remains blocked by unavailable original crosswalk/archive comparison, unknown accepted HIFLD Next dataset/file/version/checksums and approved contract, and shared #417 comparison/promotion prerequisites. Request **Blocked**, **Potential breaking change**, and **Needs decision**: decide fixed 2023 semantics versus separately versioned annual successors and approve all identifiers/columns after full baseline comparison. No code/contract/controller changes, data promotion or materialization occurred.

## Reproduction and evidence bounds

All commands used cwd `/workspace/hifld-worker-worktrees/8`. `uv sync --frozen` created this worktree's editable installation. Network policy was read; managed status had current observations but policy/credential states unknown. Inherited proxy and CA settings were preserved, with no network or credential changes.

- `python docs/dataset-sources/aiannh-areas-1/capture.py` performs bounded anonymous curl requests with 30-second timeout and 20 MB limit. `requests.json` retains exact commands, timestamps, redirects, statuses/errors, byte counts and SHA256 values. Fresh requests overwrite the capture; use a disposable copy to avoid changing historical evidence.
- `uv run --frozen python docs/dataset-sources/aiannh-areas-1/audit.py --archive-dir /workspace/hifld-worker-runtime/8` checks pinned ZIP SHA256 values, ZIP CRCs, all rows, schema, IDs, geometry/CRS and category counts. Raw ZIPs remain at that runtime path (about 27 MB total), outside Git. URLs include explicit vintage but are not assumed immutable: changed hashes must fail. Complete raw geometry recomputation requires those matching ZIPs; Git retains bounded two-feature geometry samples, full raw DBFs, ISO dictionaries, PRJs and full per-row attributes/geometry digests, rather than full geometry archives.
- `python docs/dataset-sources/aiannh-areas-1/replay.py` verifies retained evidence **offline**, without live refetch. It validates checksums, full DBF/row counts, complete IDs/attributes/categories, proposal coverage and captured repository/asset loading. It does not claim a full offline geometry re-audit from hashes alone.

Local readiness used `DAGSTER_HOME=/workspace/hifld-worker-runtime/8/dagster`, local staging/published directories, unset both bucket variables, shared UV cache, `CRAWL4_AI_BASE_DIRECTORY`, and tippecanoe PATH from batch instructions. `uv run --frozen dagster-webserver -h 127.0.0.1 -p 3408 -w workspace.yaml` served `/server_info` and GraphQL `repositoriesOrError` / `assetNodes`: RepositoryConnection, one loaded repository and 73 assets, including this existing ingest. Initial probe preceded startup and failed to connect; the retained successful probe followed startup. Readiness is not extraction/conversion/materialization correctness. Full suite was not repeated for documentation-only work; no new full-suite pass is claimed.

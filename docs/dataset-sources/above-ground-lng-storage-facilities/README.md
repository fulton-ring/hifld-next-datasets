# Above Ground LNG Storage Facilities — research only

Outcome: **Blocked**. No ingestion or schema approval is requested by this evidence package. No data asset, controller or approved contract changed. The issue remains open.

The exact inventory entry is pinned in `evidence/inventory-row.json`. Its national scope explicitly includes CONUS, Alaska and Hawaii, excludes import/export terminals and temporary/mobile LNG storage, and attributes DOE/EIA. Original producer/delegation for this exact compilation remains unverified. The original DHS crosswalk could not be retrieved: both the DOI URL and direct DataLumos archive request returned `Tunnel connection failed: 403 Forbidden` through the inherited proxy. This is an environment access blocker, not proof of dead URLs or an unmatched crosswalk row. No workbook checksum, cell or hyperlink match is fabricated.

The current ingest's PASDA URL returns a complete 8,569-byte ZIP, retained here for offline replay. It contains **13 Pennsylvania points**, not the inventory's national coverage. Its embedded XML describes conversion and `DeleteIdentical` on Shape on August 15, 2025. That is processing evidence, not a producer release date. `AGLNGID` contains 13 distinct non-null string identifiers; `FAC_GOVID` contains compound strings and repeated `NOT AVAILABLE` sentinels. Every raw identifier cell, including DBF padding, is retained as ASCII and hex in `archive-analysis.json`; original bytes remain in the ZIP. The snapshot has 36 physical attribute columns, Point geometry and EPSG:3857. Full schema, widths/precision, null counts, identifier sets, geographic/category counts, extents and geometry quality are recorded. All 13 geometries are valid/nonempty. Record SOURCEDATE values range from 2020-05-26 to 2022-03-15; none establishes a national dataset release.

A separately labeled HIFLD mirror is owned by `HSEMA_PPD`, not the producing EIA account. Its item metadata is retrievable, but the layer, count and all-ID requests return HTTP 200 with **ArcGIS error 400 Invalid URL**. These responses are rejected as extraction failures. Item size 122 is not treated as a count. Pagination, transfer limits, live schema/CRS, IDs and national coverage cannot be reconciled from errors.

EIA's own maps page links its Energy Atlas. That atlas and the EIA ArcGIS portal independently identify organization `FGr1D95XCGALKXqM`. Anonymous bounded searches of that organization returned three LNG image items, plus storage regions, pumped storage and underground storage images. Import/export terminals are explicitly excluded from the target inventory and underground storage/regions are different subjects. No image is a facility feed. Public global searches additionally returned regional or third-party copies, which are discovery leads only. These searches do not prove the absence of every agency alternative. They do not establish a maintained national continuation.

`source.json` records unknowns explicitly and proposes all 36 observed attributes plus geometry for review. This **unapproved, permissive proposal** is derived only from the clipped snapshot; meanings inferred from names and unknown units are labeled. No accepted published baseline/dictionary was pinned. Consequently no accepted schema diff, national completeness result, raw-to-generated-output reconciliation or full published-baseline row comparison is claimed. XML FID/Shape entries are distinguished from physical DBF fields; no substitute row IDs are proposed. Maintainers must resolve the actual national source, accepted baseline and approved contract, with shared prerequisite #417, before implementation or publication. The PA-only existing mirror requires a Potential breaking change decision before any replacement or national continuity claim.

## Reproduce and inspect

All commands below run from this worktree root with inherited proxy/TLS configuration; no credentials or network policy were changed.

```sh
UV_CACHE_DIR=/workspace/.cache/uv uv sync --frozen
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/above-ground-lng-storage-facilities/analyze_archive.py
UV_CACHE_DIR=/workspace/.cache/uv uv run --frozen python docs/dataset-sources/above-ground-lng-storage-facilities/validate_evidence.py
```

The archive analyzer runs entirely offline against the committed bounded historical ZIP and verifies exact equality with the recorded report. `evidence/requests.json` records every researched request URL, UTC time, status or precise transport error, body filename, length and SHA-256. Response bodies are retained (HTML, text and extracted XML use lossless gzip to preserve exact whitespace), so review does not require a live refetch. The manifest records `arcgis_error` for later captures; the first five initial captures predate that additional manifest key. `capture.py NAME URL ...` can perform bounded fresh requests separately, but replay uses retained bytes. Live URLs may change and are not immutable snapshots.

## Local Dagster readiness

An isolated environment was synced to this worktree. `dagster-webserver` was actually started on **127.0.0.1:3405**, with buckets unset and private local staging/published directories:

```sh
env -u HIFLD_STAGING_BUCKET -u HIFLD_DATASETS_BUCKET \
  UV_CACHE_DIR=/workspace/.cache/uv \
  CRAWL4_AI_BASE_DIRECTORY=/workspace/hifld-next-datasets/.local \
  PATH=/workspace/.tools/tippecanoe/usr/bin:$PATH \
  DAGSTER_HOME=/workspace/hifld-worker-runtime/5/dagster \
  HIFLD_STAGING_DIR=/workspace/hifld-worker-runtime/5/staging \
  HIFLD_DATASETS_DIR=/workspace/hifld-worker-runtime/5/published \
  uv run --frozen dagster-webserver -m dagster_hifld.definitions -h 127.0.0.1 -p 3405
```

`GET /server_info` returned HTTP 200 with Dagster 1.12.19. GraphQL `repositoriesOrError` returned HTTP 200 and `RepositoryConnection`, including asset keys for the existing graph and this dataset. The exact JSON responses are retained. No source was materialized; readiness is not ingestion correctness. Only the worker's own server was stopped afterward. The full repository suite was not rerun for these research-only documents; no fresh suite pass is claimed.

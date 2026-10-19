# Agricultural Minerals Operations: blocked source research

Checked 2026-10-19 UTC for issue #7. This research does not approve ingestion or a published contract. Requested lane: **Potential breaking change / Needs decision** because no accepted baseline, source contract or verified replacement is available. The issue remains open.

The exact inventory entry (captured in `evidence/inventory-row.json`) describes USGS Minerals Information Team plants/mines active in **2003**, replacing a June 2004 layer, and explicitly says the data are legacy and not expected to be updated. The inventory's 2017 dates do not establish a current source release. A modern active-operations compilation would change the population and requires a compatibility decision.

The DHS crosswalk DOI redirects to DataLumos; the runner rejects the www.datalumos.org tunnel with **403 Forbidden**. The original XLSX was not obtained and hyperlink targets could not be parsed. Match status is **unverified due to network blocking**, not unmatched. A permitted ICPSR alternative returned 404. No exact match is inferred from the title. Prior issue agent-content-demos#40 and comments were retrieved as historical leads only; its title matching and aggregate endpoint counts are not dataset-level proof.

## Candidate and authority

The anonymous ArcGIS search returned a public feature service owned by **clibby007**, organization **HDRa0B57OVrv2E1q**, item **1c9f0c8899bb4f47be90fd6723b926e1**. Its snippet repeats the active-in-2003 USGS attribution. Its description, license and access attribution are empty. Neither the item nor the search establishes USGS ownership, delegation, exact HIFLD continuity, rights or currency. It is retained strictly as a **third-party mirror fallback**. Other search results (Data Basin, jofraley_swcs and MLepasepp) are unverified leads, not selected sources.

The USGS National Minerals Information Center landing page responds, but it does not establish this dataset's machine source. The exploratory mrdata agricultural endpoint returned HTTP 403 with a short body; it is unavailable from this runner, not declared dead. No maintained producer publication or official distributor was verified. The selected-source fields in `source.json` consequently remain unresolved/null. No claim of exhaustive agency absence is made.

## Extraction evidence and replay

`audit.py` records anonymous requests, rejects HTTP errors and ArcGIS JSON error bodies, gets the count and full object-ID set, extracts in batches of 100 IDs, checks transfer-limit flags, reconciles every returned ID, and rechecks the full ID set after extraction. The captured run reconciled **236 records in three batches**, below the advertised 2,000 transfer limit. All 12 attributes were retained. All four identifier-like fields (`OBJECTID_1`, `FID_1`, `OBJECTID`, `MINOP1X020`) were unique/non-null within this mirror; their cross-release stability and original semantics remain unverified. Never substitute row IDs or discard inherited identifiers.

Captured points use EPSG:4326; all 236 have non-null, in-range coordinates. State, commodity and plant/mine distributions and field null counts are in `evidence/extraction.json`. These are mirror quality observations, not proof of full historical national coverage, geometry validity beyond coordinate checks, producer completeness or equivalence to HIFLD Next. Item modification (2022) and layer edit metadata (2021) are portal events, not producer release dates. Source release remains unknown; 2003 is observation vintage.

Raw JSON pages, complete IDs, metadata, request URLs/status/timestamps and SHA-256 hashes are committed as bounded historical evidence. Offline replay uses these bytes and needs no live refetch:

```sh
export UV_CACHE_DIR=/workspace/.cache/uv CRAWL4_AI_BASE_DIRECTORY=/workspace/hifld-next-datasets/.local
uv run --frozen python docs/dataset-sources/agricultural-minerals-operations/verify.py
```

For a separate fresh snapshot, run `audit.py` in a copied evidence directory; it overwrites capture filenames and does not approve a contract. Unknown fields are retained in raw responses and fail the captured schema check. No ID normalization, geometry conversion, publication or materialization occurred.

## Local readiness and remaining gates

`uv sync --frozen` installed this worktree as the editable package. A real local `dagster-webserver -m dagster_hifld.definitions -a defs -h 127.0.0.1 -p 3407` started with isolated `DAGSTER_HOME=/workspace/hifld-worker-runtime/7/dagster`, local staging/published directories and both cloud bucket variables unset. GET `/server_info` and GraphQL repositories/assets returned HTTP 200; raw responses are retained. This verifies repository loading only. No unapproved source was materialized. A redundant startup attempt hit the already-running own port; the original server provided the recorded successful checks.

No full suite rerun was warranted by docs-only changes. The batch's known full-suite missing sibling `hifld-next-iac/environments/prod/dagster.tf` error is not claimed fixed. Offline evidence validation is the focused check.

Before implementation: obtain the workbook and verify the exact inventory crosswalk including hyperlink targets; establish producer lineage and a real release date; retrieve and pin accepted published HIFLD Next objects/dictionary/checksums; compare full IDs/attributes/geometry/coverage; obtain maintainer approval of a versioned source/output contract and any migration; complete #417 shared comparison/promotion gates. The complete mirror-derived proposal in `source.json` is explicitly **unapproved**, including every observed attribute plus geometry. It must not become the accepted baseline by default. Historical preservation versus a different modern successor needs an explicit maintainer decision.

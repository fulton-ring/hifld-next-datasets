# Agency source recovery: initial inventory and execution plan

## Scope and evidence

This is a planning baseline, not a completed endpoint audit. It enumerates all 415 unique entries in `HIFLD_Open_Inventory_12112025.csv` at commit `6969d05028352eefd20348a738a829034af9882f`. Every entry has a stable `dataset_id` derived from its inventory path. Source status remains unverified until dataset-specific evidence is recorded. Inventory modification dates are archive metadata, not present-day agency release dates.

The DHS crosswalk has a different scope from the archive inventory. Reconcile its rows explicitly; never assume a positional or one-to-one join. Preserve unmatched entries and all one-to-many relationships. A title similarity score is a candidate match, not accepted provenance.

Starting evidence:

- DHS crosswalk and DataLumos index: https://doi.org/10.3886/E241367V1 . Obtain the original XLSX files, retain their checksums, and parse hyperlink targets as well as displayed cell values.
- My Community Data audit: https://www.mycommunitydata.org/federal-data-implementation . Historical evidence; does not establish today's endpoint health or absence of agency alternatives.
- Existing research: https://github.com/fulton-ring/agent-content-demos/issues/40 . Its October 18, 2026 run reports 305 crosswalk links: 205 responding, 72 dead, 18 token-required, 10 failures/timeouts. These are attributed prior results, not independently reproduced in this baseline. They are not comparable to the March audit without reconciling URL lists and methods.
- USGS Structures service: https://carto.nationalmap.gov/arcgis/rest/services/structures/MapServer?f=pjson . The fetched service metadata describes an October 2026 refresh and feature layers 49 (hospitals), 51 (fire/EMS), and 53 (police). A web fetch is not a complete download or record-level validation.

## What already exists

The ingest registry contains ten simple dataset ingests plus the multi-file 2020 Census Blocks ingestion. Eleven inventory entries have been mapped to these modules. This records code presence, not successful runs or source health.

- NOAA maritime limits: three explicit assets.
- Census AIANNH: pinned to TIGER2023; establish a release policy before calling this current.
- Census 2020 blocks: maintain historical-vintage semantics; do not silently replace 2020 geography with another vintage.
- FAA airspace and aviation, BTS Amtrak, Red Cross divisions.
- Above-ground LNG and USACE administrative areas: currently download PASDA archive files; replacing these with maintained agency feeds is separate work.

Follow the current registry in `assets/ingest_registry.py`. Derived support is assembled by `assets/supported_assets.py`; do not copy obsolete instructions about manually maintaining an old registry. Confirm wiring against the checked-out code and tests.

## Dataset work contract

Each issue starts in Source research. It may enter Ready for implementation only after recording:

1. Inventory ID, exact crosswalk match or explicit unmatched result, original producer and current publisher.
2. Official landing page and machine-download URL, stable service/item/layer identity, access terms, geographic coverage, geometry type, schema and persistent IDs.
3. Anonymous request evidence with UTC timestamp, redirect target, status and parsed response. ArcGIS JSON errors (including token-required responses) must not count as success even with HTTP 200.
4. A small valid feature sample plus a count/completeness check; pagination and transfer limits; source release date distinct from fetch time and portal edit time.
5. Relationship to the archive: exact continuation, successor with changes, partial substitute, archive only, restricted, or unresolved. Compare fields, coverage, IDs and representative records before choosing.
6. Proposed refresh cadence grounded in agency publication practice, failure/retry policy and expected resource needs.

Implementation uses explicit source-specific Dagster assets and the existing ingest → catalog → publish flow. Retain source identifiers, provenance, checksums and release dates. Make repeated ingestion of an unchanged release idempotent. Test pagination, unexpected schema/CRS, corrupt or empty responses, and incomplete downloads. Use local staging for verification; production publication is a separate deployment decision. Run `uv run python -m unittest discover tests` and focused checks required by the change.

A source being unavailable from a restricted runner is `network_blocked` or `unverified`, not `dead`. A service answering is not proof that its data is maintained. A static archive is not an updating pipeline.

## Potential breaking changes and schema contract

Add a dedicated **Potential breaking change** board lane. Enter it whenever source identity, identifiers, schema, geometry, coverage or meaning may change. An agent must report the difference rather than silently normalize it away. This lane blocks implementation-ready dispatch, publication/promotion and auto-merge until a maintainer explicitly accepts a documented compatibility plan. Independent agent review alone cannot clear this exception. Approval is tied to the reviewed schema/source contract and PR head; later changes require re-evaluation.

Before implementation, capture a versioned schema contract from the accepted published dataset and its data dictionary, alongside the raw source schema. If no accepted baseline exists, obtain maintainer approval for the initial contract. The contract names allowed published columns, types, nullability, meanings, ID roles, geometry/CRS and coverage. Keep raw downloads losslessly in staging; publish only the explicitly accepted projection.

Every candidate release produces a machine-readable diff and a readable report covering added, removed and renamed columns; type and nullability changes; ID uniqueness/null counts and continuity; geometry/CRS and coverage changes. Unexpected differences fail the compatibility check and route the issue to Potential breaking change. Never auto-update the baseline to make the check pass.

- Preserve all existing source and published ID columns, their names, types and values. Preserve leading zeros and large integer precision. Do not drop IDs, substitute row numbers, regenerate IDs or treat ArcGIS OBJECTID as a stable cross-release identifier without evidence. If upstream removes or changes IDs, flag it and propose a mapping/migration; do not invent continuity.
- New upstream columns remain in the raw snapshot, but do not automatically enter the published schema. List each addition with meaning and reason; explicitly map and approve it before publication. An allowlist must not silently discard identifiers: any unmapped identifier or suspected ID field triggers review.
- Renames, removed columns, changed field meanings, lossy type coercions, newly nullable required fields, ID churn and changed geometry/coverage require an explicit compatibility decision. Do not swap a partial substitute into an existing dataset slug just because its title resembles the original.
- Tests must exercise missing/renamed IDs, leading-zero IDs, duplicate/null keys, unexpected added columns and type changes. Verify that a flagged release can be retained in staging while the last accepted published release remains available.
- Resolution records the before/after schema, impact, mapping, migration/versioning decision, approving maintainer and commit. Only then return to Ready or Review as appropriate; all normal checks and independent review still apply.

## Authoritative sources and mirrors

Track **source role** separately from **health**, **freshness** and **relationship to the archive**. A mirror can respond and contain newer uploads without being the producer or a maintained agency release.

For every URL record the original producer, publishing organization/account, host, service/item identity, upstream lineage, source release date and evidence URL. Classify it as producer publication, delegated official distributor, third-party mirror, historical archive or unresolved. Government ownership, a .gov URL, an ArcGIS title/logo, HIFLD branding or a recent portal edit alone does not establish authority or currency. Agency ArcGIS services require ownership and lineage evidence; a delegated distributor requires an explicit producer link or documented delegation.

PASDA HIFLD/FEMA snapshots, DataLumos, SeerAI/Source Cooperative and Esri archive copies are preservation candidates for these HIFLD layers, not evidence of current producer publication. Classify each actual item; do not infer the role of all data on a platform from its domain. Follow mirrors' upstream references to the producer and verify the actual service or release.

Retain mirrors as separate fallback/archive records with their provenance and snapshot dates. Do not silently promote a mirror to the authoritative URL when the producer is unavailable. Any proposed fallback publication needs a maintainer decision and must remain visibly identified as a mirror or archive, with the original source and last verified release. Unknown lineage stays unresolved and cannot become implementation-ready.

## Initial implementation queue

Prioritize source families for investigation, but keep one dataset issue per inventory ID:

- Fire/EMS stations: USGS feature layer 51. Verify equivalence and complete extraction first.
- Hospitals and local law enforcement: USGS layers 49 and 53 are candidate substitutes, not automatic replacements for the HIFLD compilations. Require an explicit comparison and preserve archive identity.
- Public/private schools: NCES EDGE https://nces.ed.gov/programs/edge/Geographic/SchoolLocations . Use education-year releases; points do not replace campus polygons.
- BTS transportation: https://geodata.bts.gov/ . Verify existing aviation/Amtrak ingests, then add missing datasets.
- USACE navigation: rediscover named layers rather than relying on obsolete numeric IDs. Validate agency ownership and per-layer semantics.
- Energy: investigate official tabular releases where appropriate. Authentication-required Energy Atlas services are not usable public feeds. Do not assume EIA-860 reproduces pipeline geometry or the entire HIFLD energy catalog.

The accompanying issue-draft generator produces `dataset-issues.jsonl`, one research-first issue draft per inventory entry. These are drafts, not verified implementation tickets or created GitHub issues. Unknown candidates are intentionally blank. The deterministic HTML marker permits a future publisher to skip already-created issues; search open and closed issues and preserve the returned URLs in the manifest. Do not bulk-dispatch these drafts as implementation-ready work.

## GitHub project and worker implementation

The executable workflows and controller now live in `.github/workflows/pipeline-dispatcher.yml`, `.github/workflows/pipeline-worker.yml` and `tools/pipeline_workers/`. See `tools/pipeline_workers/README.md` for configuration, schema contracts, retry behavior and validation. The material below describes the intended board and worker contract; live activation still requires project access and credentials.

Proposed board states: Source research → Ready → In progress → Review → Changes requested → Done, with Potential breaking change, Blocked and Needs decision. Use a dedicated HIFLD project or a dedicated filtered view. Fields: Dataset ID, Agency, Source relationship, Source health, Last verified, Schema compatibility, Source role, Maintainer decision, Priority, Agent, Issue, PR, Worker session, Attempt count. Preserve a complete history on issues even if project access is temporarily unavailable.

Select the runner using the supported official Codex Action or CLI/SDK, not an assumed counterpart of Claude's routines API:

- https://github.com/openai/codex-action
- https://developers.openai.com/codex/sdk

The action requires configured provider credentials; the Claude worker token does not grant Codex API access. GitHub connector permissions and ChatGPT access do not establish that Actions has model credentials or Projects write access.

Dispatcher requirements:

- Query only this repository and `Agent=Codex`; an empty Agent must not dispatch to both Claude and Codex. Existing Claude dispatcher accepts empty Agent, so every shared-board Codex card must have an explicit value.
- Claim each issue atomically with a lease, run ID and expiration. Check active PR/session before launching. Use bounded concurrency and retries, and recover expired claims.
- Make issue creation, dispatch, PR creation and status transitions idempotent. Never interpolate issue text into shell code.
- One branch and PR per dataset, with source evidence, focused changes and a runnable verification record. Batch shared infrastructure separately to avoid conflicting worker edits.
- Exclude Potential breaking change cards from dispatch and auto-merge eligibility. Treat unresolved provenance and schema differences as blocking findings; require the explicit maintainer decision described above.
- Run an independent reviewer against the exact PR head commit. Review dataset identity, completeness, provenance, tests and release behavior. New commits invalidate old review results.
- Changes requested resumes a bounded revision loop on the same PR. Stop and surface unresolved findings or exhausted attempts rather than endlessly relaunching.
- Merge only according to the selected approval policy, after required checks and the independent review for the current head. The selected policy is independent review plus required checks, then auto-merge. This document does not itself install or activate the controller.
- Model execution should not itself control merge credentials. Keep the controller's permissions separate and obey repository branch rules. A review by the same GitHub identity may not satisfy GitHub's required reviewer rules; choose the bot/app identities explicitly.

Acceptance scenarios: duplicate events launch once; failed launches release the lease; stale worker completion cannot move a newer attempt; cross-repo and unassigned cards are ignored; source failure is reported; review findings trigger revision; any head change invalidates merge eligibility; failed checks and unresolved findings block merge; schema drift or unresolved mirror lineage routes to Potential breaking change and blocks publication/merge; agent approval cannot substitute for the maintainer decision; no workflow silently starts a paid worker before its credentials and scope are configured.

## Current blockers

- Issues have been enabled on this fork, and dataset tickets are being created here. The upstream is `Public-Environmental-Data-Partners/hifld-next-datasets`; do not publish tickets upstream by accident.
- The session has no GitHub Projects read/write tool. Project permissions and a supported authenticated controller still need setup; repository admin metadata alone does not establish project access.
- The execution proxy is unreachable. Its configured policy also lacks agency domains. Restore the environment proxy and configure access to the audit/crosswalk hosts and verified agency hosts before a bulk audit. No live-check failures were inferred from this infrastructure problem.
- The crosswalk workbook and row-level results from the prior audit have not been imported. DataLumos returned a web-fetch 403 and the My Community Data page did not yield readable body text in this session. Reuse the prior worker's raw artifacts if recoverable, otherwise download the original and rerun with bounded requests.

## Delivery order

1. Complete the dataset ticket/project setup, restore endpoint access and obtain crosswalk/historical audit inputs.
2. Match all inventory rows, record evidence and publish research-first tickets with stable IDs.
3. Independently verify the first source families and implement a small pilot using existing pipeline components.
4. The worker/controller CI and independent review implementation is in this PR; provision project/controller access and Codex credentials and live-validate it.
5. Exercise the worker/revision/merge state machine on the pilot, then expand the implementation queue.

## Inventory path anomaly

The inventory uses `Done` as the path for `US PGA 5Pct50Yrs BC arc` (filename `us-pga-5pct50yrs-bc-arc`). Preserve this original inventory ID for traceability, but verify its correct published catalog identity during source research; do not treat the path as evidence of completion or silently rename an existing dataset. Nested paths such as `nfhl/...` are also preserved as inventory IDs.

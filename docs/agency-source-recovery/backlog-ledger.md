# Dataset reconstruction backlog ledger

This ledger tracks all 415 dataset inventory issues in `fulton-ring/hifld-next-datasets` (#2–416), plus shared verification #417. Machine-readable evidence, exact heads, review scopes, run IDs and unresolved decisions are in `backlog-ledger.json`. Earlier PR search counts are historical snapshots, not current open-PR counts. Issue #417 remains open; no issue was closed and no production data was published.

Existing priority PRs #420/#3, #419/#4, #427/#10 and #428/#11 were recovered with their prior exact-head reviews and runtime evidence. They were not rebuilt. Shared verifier #417 was resumed at reviewed4dfbe33:22focused tests passed; synthetic verify/promote run23663675-bfd2-4cfa-8ff2-c9aa35952259 succeeded and staged-drift runa7c4751b-eec6-4bae-9f65-ab24805b817e refused promotion, preserving14fixture objects. Synthetic mechanics do not approve real dataset baselines.

The recent checkpoint below distinguishes completed reviewed drafts, review revisions, active work and evidenced external blockers. A docs-only research branch does not complete a feasible pipeline. Missing baseline/contract approval permits draft implementation and local testing; it blocks release. Other issue records still need triage or reconciliation of prior work; they are not silently classified as blocked.

| Dataset issue | Draft PR | Current state | Head |
| --- | --- | --- | --- |
| #21 | #539 | pipeline_review_pending | 61cbdd6c |
| #25 | — | in_progress |  |
| #27 | #540 | pipeline_review_pending | 9d3b1d52 |
| #37 | — | in_progress |  |
| #253 | #527 | reviewed_draft_prior_runtime | 1c6e403b |
| #257 | #529 | reviewed_locally_verified_draft | 39f84887 |
| #260 | #530 | reviewed_draft_prior_runtime | 126dae20 |
| #261 | #531 | reviewed_locally_verified_draft | 750c43c5 |
| #262 | #535 | reviewed_locally_verified_draft | 3dbecf38 |
| #263 | — | external_source_access_resource_decision |  |
| #264 | — | external_source_access_resource_decision |  |
| #265 | #532 | external_source_access_decision | 279a6596 |
| #266 | — | external_scope_geometry_decision |  |
| #267 | — | external_source_lineage_decision |  |
| #268 | #534 | reviewed_locally_verified_historical_draft | f1e6e659 |
| #269 | #537 | reviewed_locally_verified_draft | d1ddc072 |
| #270 | #538 | review_changes_in_progress | 3aa50bff |
| #271 | #536 | review_changes_in_progress | 7472fa8c |
| #272 | — | in_progress |  |
| #273 | — | in_progress |  |
| #274 | — | in_progress |  |

Current-head code review and actual runtime scope are separate fields. Documentation or narrowly scoped capture/validation fixes after a recorded graph do not fabricate new run IDs; the ledger preserves the run's real commit and the final review's exact commit. Public historical runtime claims whose raw artifacts vanished in Cloud replacement remain labeled as such.

New raw captures and packets are preserved locally. A compact checksummed evidence archive is prepared; a durable artifact-destination question is pending because the configured downloadable-output path is unavailable in this Cloud. Code and this ledger are already remote on their existing branches/PRs. No production data was published.

The configured GitHub integration cannot read organization Projects v2 or repository Actions variables (actual403/forbidden responses). This blocks Project-board access, not dataset sources. Work continues using the issue/PR ledger. No merge, issue closure, contract approval or production publication is authorized or performed.


## Board supervisor checkpoint — 2026-10-21

The user authorized the existing paid queues under the <=$25/day **inference** guard. GKE/Actions costs are separate; actual provider dollar usage is not visible here. This pass holds a soft batch of two implementations and one paid review, within the unchanged six/two controller ceilings. Direct Project reads remain forbidden; move labels and actual admissions were verified.

| Dataset | Current evidence | Next action |
| --- | --- | --- |
| #275 Primary Roads | PR #546 head `8d1dd15c0ded6d7b77a5eff1bca6fb9189683e7c`; review run39597483660 | Inspect trusted review; no repeat full suite |
| #282 Puerto Rico Subbarrios | Saved head `502a00aedcc4d664b4fa2d772d0c6dfc449f28cf`; delta review5599567509 PASS; trusted result-file failure | Needs decision for remaining approvals; no paid retry |
| #283 Pylon Areas | Implementation39597542093 admitted; official 6,440-polygon source inspected | Await actual saved pipeline/run evidence |
| #284 Pylon Locations | Implementation39599138325 admitted; official4,079-point source inspected | Await actual saved pipeline/run evidence |

Shared #417 is at `b57ff17ad49a2a8f84a29aaec12a52efbb3869c0`, independently reviewed scoped fixes (review5599552229), with actual synthetic success `d054f1c6-2fe8-46f5-a332-47924cdcb9bc` and drift refusal `cd59e479-c18b-4647-aeaf-99f45e6644ad`. Baseline/configuration and unsupported arbitrary transformation accounting remain blocked; this does not complete #417. The branch advanced only after #283 pinned its original checkout. No merges, closures or production publication.


## Latest verified checkpoint — 2026-10-21 13:22 UTC

The 415-issue inventory remains intact. Earlier PR counts are historical search snapshots and are not current open-PR totals.

The current ledger coverage fields give a conservative, non-readiness snapshot: 415 inventory issues; 110 issue records with a current or historical PR reference (including research and tracking PRs); 5 rows with a structured exact-head review plus local-run bundle (#257, #261, #262, #268 and #269), plus 6 distinct latest-checkpoint issues with exact-head review or CI/runtime evidence (#3, #4, #10, #11, #275 and #282); 13 with the external decision/source evidence blocker label; 303 explicitly awaiting triage with no recovered pipeline PR; and 84 needing prior PR evidence reconciliation. The five-row structured subset is not the overall verified-pipeline count. These ledger-based groups overlap and do not establish which remaining datasets can be automated or count current open pipeline PRs.

| Work | Verified state |
| --- | --- |
| Shared verifier #421 / issue #417 | Head `b57ff17ad49a2a8f84a29aaec12a52efbb3869c0` merged as `df0e0c3db6c63015c8f98788c4f9f2b90e8991c3`; fail-closed scope is limited to literal GeoJSON. Issue #417 remains open. |
| NOAA #420 / issue #3 | Head `0e05c81e1a2ce15e6cfdfa705ff82010971b5466`, merge `7f456f5d6d9c8d3ed2c7d0796b04f26537f38918`; tested merged tree `311609f286322807cbe11a43da3694966d9fa824`, root review 5600251607. Issues #3 and #10 remain OPEN; schedules stopped, no approvals or publication. |
| FAA #427 / issue #10 | Head `4a20188d477412d1c4ad835db4f030fd52da9e0b`, merge `7c83fc66a14b51854ebe8911ffa5ead86d2529e8`; includes `uv.lock` runtime copy. |
| NOAA #419 / issue #4 | Head `6a37f2adec173dbf24e58bd6c94c7f92b69b61ea`, merge `dd45ea532951121db7b38ee573d6b13a9052f38c`; CI build passed, merged tree `1a9ef2b5195f72813a0ecc47004154e51debc0ef`, root review 5600345800. Issue #4 remains open. |
| Fueling #428 / issue #11 | Head `eb6163b8e43933b1760fb23ee76b638734d0abe0`; packaged runtime fix, 41 tests and CI pass; It had been waiting on #419; #419 has now merged, so recheck integration against the merged tree. |
| Issue #25 | Replacement tracking PR #544. |
| Roads #546 / issue #275 | Head `eb8448f77d19e37d798032f339b1f7f8b793ec86`; packaged hash fix, 14 tests; source/cache/catalog runs and root review 5599755431 are recorded. Parent #260 chain blocks merge. |
| Subbarrios #548 / issue #282 | Head `502a00aedcc4d664b4fa2d772d0c6dfc449f28cf`; independent paid review passed despite cleanup failure run 39599895858. Do not retry. Parent #271 chain blocks merge. |
| Pylon Areas #556 / issue #283 | Saved PR #556 head `e1bcef4fa76c8a3eb4705d152482d307707f11cf` after run 39597542093 failed trusted handoff. A packaged-runtime Git defect was found; narrow fix and local raw recapture are ongoing, with review pending. Worker reports 6,440 polygons and four run IDs; raw GKE retention is unverified. |
| Pylon Locations / issue #284 | Recovery produced draft PR #557; worker reports actual runs at head `73c3ddf`. Independent verification remains pending. |

At 13:18 UTC there were no active paid jobs and new paid admissions were held. The `<= $25/day` inference guard is unchanged; actual spend is unknown and the guard excludes GKE/Actions compute. Verified code merges are authorized. Issue closure, release acceptance and production publication remain unauthorized.
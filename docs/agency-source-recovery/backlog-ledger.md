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
| #261 | #561 (merged) | code_merged_issue_open_acceptance_gated | 0535903d |
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


## Latest verified checkpoint — 2026-10-21 13:52 UTC

The 415-issue inventory remains intact. Earlier PR counts are historical search snapshots and are not current open-PR totals.

The 13:52 UTC snapshot had 131 open PRs: 128 mapped to 127 inventory issues, with two PRs on #261, and 288 issues without a mapped open PR. This was superseded by the 14:14 UTC census below. Status labels in that earlier ledger snapshot showed 270 awaiting triage and 117 needing PR evidence reconciliation. These are workflow and association counts, not feasibility, capacity, or release-readiness estimates. Separately, 5 dataset rows have structured exact-head review plus local-run bundles and 11 additional current priority records have review/CI/runtime/audit evidence; the first group is not the total verified-work count.

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

## Latest verified checkpoint — 2026-10-21 13:43 UTC

The priority code PRs #420/#3, #419/#4, #427/#10 and #428/#11 are merged, as is shared verifier PR #421. Their source issues remain separate acceptance gates; issues #3 and #10 were verified open, and issue #11 was verified open. Shared #417 remains open; its fail-closed support is limited to literal GeoJSON. Schedules are stopped. No source/contract approvals, issue closures, release acceptance or production publication are recorded.

| Work | Latest verified evidence | Remaining gate |
| --- | --- | --- |
| #428 / issue #11 | Head `43faff7f72893264f3c04c3a717b29cb567ce1a0`, merge `7b653e26c7b1b21deb87628499b7aaa9ee7cc105`, tree `32a7b5b7a7a6b53b3d85b97afc884d9565cf6b22`; root review 5600411657; Docker CI passed; 41 tests. | Issue #11 remains open; acceptance gates remain. |
| #556 / issue #283 | Head `a30e67a0117cf5398b6737336062e1a25783192c`, merge `ec5493622161dd3bc7e46769699d374b434b2b55`; root review 5600646726; current-head Docker build passed. Independent audit passed all 6,440 attributes and ring coordinates plus 37 body/wire receipt pairs. Capture/replay, quality failure and approval-refusal runs are recorded; fixture unchanged. Evidence retained at `/workspace/work/283-evidence`. Superseded paid review run 39607933649 was cancelled; no paid retry. | Issue #283 remains open; rights, continuity, delegation and release decisions remain unresolved. |
| #557 / issue #284 | Head `327a875b9fda34b1d542d27a5d69032ff742fe42`, merge `5867c5cbf15210ff9c6c93f810ddf9bc63d8a8e8`; root review 5600557965; Docker build passed. Independent audit verified 4,079 native rows/IDs/attributes/XY and 26 raw body/wire pairs (78 artifacts). Ingest/catalog/GeoPackage/GeoParquet passed. | Blocking quality run `b1697eff-1fec-4d26-b8ed-ba4069d55aee` failed and skipped verify/promote; approval refusal `88e28dab-8242-4d56-a850-0ca4cadf1824` preserved the two-file fixture unchanged. Rights, edition, continuity and coverage remain pending; refresh stopped. Evidence: `/workspace/work/evidence-packets/284-native-recovery/`. Issue #284 remains open. |
| #539 / issue #21 and #540 / issue #27 | Independent reviews found a packaged-runtime construction-validator defect requiring `Git.git` metadata. Local raw runs are valid from Git-backed checkouts. | Packaged runtime construction remains blocked. Foundation PR #559 at `83a0ee1106560b61a72f067a06cc0120c5e76ecd` merged as `c096761eacf3f621d424771d9eeaa9868d862e4d`; Docker build passed, root review 5600692808, and root verified actual merged tree `314994ebead14d02100f197e1f16f6ad27a22144` matches the reviewed preview. PR #558 at `856b9b9d1544fc9df26ae3c599dcc6246a86d577` review passed (5600645908) but retains the #273 base and is not merged. The foundation does not prove every Git-metadata blocker is fixed; EPA/IENC stack propagation is planned next. The narrow #261 packaged-identity fix is progressing on existing PR #531; do not blindly retarget it to main. Avoid duplicate fixes in #539/#540. |
| Issue #307 | PR #560 merged at head `f2a67dfb2116c9bdd2c6f6e2518e9d16acebacd3`, merge `ebe53ed3e1803c93fc74f95cf755fdfb29c711ff`; root review 5600976268. Retained replay/catalog passed 481-row quality. | Quality and approval gates refused promotion with the populated 14-object fixture unchanged; rights, baseline, archive equivalence and release remain pending. Issue #307 remains open. |

All paid runs were reported completed or cancelled by 13:43 UTC. New paid admissions are paused while malformed handoff reporting is resolved; this batch is native only. The inference guard remains `<= $25/day`; actual spend is unknown and GKE/Actions compute is excluded.

Native work checkpoint: issue #285 Quaternary Fault Lines is assigned to `recover_284_native`, with evidence claims pending. For issue #290 Recommended Tracks, `shared_gate_fixes` reports IENC `RECOMMENDED_TRACK_LINE` layer 46 with 666 features, EPSG:4326 polylines and 10 fields; this eligibility claim still needs verification, no PR exists, and BTS delegation succession is unapproved. Issue #289 is not being worked. PR #527 for issue #253 has base 181, which is not a main ancestor and carries 17 unreviewed main files; do not blindly retarget it.

Paid runs were reported completed or cancelled as of 13:43 UTC. New paid admissions are paused while malformed handoff reporting is resolved; the current work batch is native only.

## Fresh open-PR census — 2026-10-21 14:14 UTC

GitHub API pages 1–3 returned 132 currently open PRs (100, 32, 0). The census mapped 129 PRs to 129 dataset issue records using exact issue-number branches; no title-only or body-marker association was needed. Issue #261 has open PR #531 at `75f5ec9358e5f8108d3b1139c9d7812248c82624`, base `codex/cloud-dataset-260`; canonical replacement #561 is merged at `0535903d166291453f5877a77fd7727434d85a15` (merge `9a4ee165ebfbca350805ee189dc0cac5c33144f9`). Issue #64 maps to active PR #455 via branch `codex/cloud-dataset-64`.

There are 286 inventory issues with no mapped open PR in this snapshot. This does not say whether their sources are automatable or whether prior closed/merged work exists. Three open PRs were left unassociated because they are repository-level support or bookkeeping (#558 packaged-runtime support, #533 ledger, #1 board workflow). The census corrected 19 prior low-confidence row associations by exact branch; their prior issue candidates are preserved in JSON for audit. PR references were checked by metadata only; no PR code was read.

The census also records recent status changes: PR #560/#307 is merged with replay/catalog and gated refusal evidence; #561/#261 is merged with root review and CI, while #531 remains open as a noncanonical stack child and its ancestry blocker persists; #549/#285 is being repaired at head `5bb53396ee1be58c77ff1016f2e70dfd236dba11`; PR #562/#290 is at `5369d0e4c1f15d875a9f27995cbdcf74f3b5613d` with independent review pending; and PR #563/#335 is at `6c523380f087bd54114e86b26f2d834c688e96a1` with final replay/review pending. Their branch associations do not establish pipeline readiness or acceptance.

## Main integration checkpoint — 2026-10-21 14:55 UTC

PR #562 for issue #290 merged as `5014f2b705cd4c7e7d45d1ffe35cc2a145ac975a` from exact head `07ab1ec34538cd1ecd2364cd33ccb1d379ba890d`. BTS delegation succession remains unapproved; issue #290 remains open.

PR #563 for issue #335 merged as `741f54862aad46c38c8754926c95e163e88e9c2b` from exact head `6c523380f087bd54114e86b26f2d834c688e96a1`. Historical equivalence, rights, coverage and release acceptance remain unresolved; issue #335 remains open.

PR #549 for issue #285 merged as `d8ea091cdbefc389bb7894cf5c086aa5cb8c14b7` from exact head `1c2b155ff761f6190eea49729f56a774999b083d`. Earlier runtime and review evidence remains attached to its recorded run heads; root verified merged tree `4994ee7293c9a463030902ac511822fbc7f810c1`; issue #285 remains open.

PR #564 for issue #276 merged as `b2ad6f414ebd8f7e5506ff69af841f2e90e654a1` from exact head `cdffb769b262b0268596a62637082f350a15f8a1`; root review 5601503187 and CI Docker build passed; merged tree `a066656e984a068cd75ff4d56212d425df090a8d` equals the exact head tree. Older PR #547 remains open on base `codex/cloud-dataset-21` as a noncanonical stack child; it is not the canonical #276 implementation. Issue #276 remains open.

The 14:14 UTC open-PR census above is a historical snapshot, not a current open-PR count. No new full census was run in this checkpoint.

Current native work: #275 is preparing a clean-main replacement. #21 clean-main recovery is assigned to `shared_gate_fixes`. #27 capture-current contains 36 rows and 33 artifacts at `/workspace/work/27-clean-evidence/capture-current`; BTS succession 3607 was denied and newer request 7ec remains unapproved. #282 clean recovery is reported at `69bf278`, preserving PR #548 and prior review evidence.

No paid jobs are active and new paid admissions remain paused. Board preview run 39619944357 succeeded and proposed Needsdecision for #4/#10/#11/#290. Subsequent sync 39621201053 succeeded, moved those four issues to Needsdecision and removed pending labels (the #276 label-event was consumed separately); no paid dispatch occurred. Poll at 14:56 UTC found no active jobs. The inference guard remains `<= $25/day`; actual spend is unknown and compute costs are excluded.

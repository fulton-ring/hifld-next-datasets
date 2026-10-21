# Dataset reconstruction backlog ledger

This ledger tracks all415dataset issues in `fulton-ring/hifld-next-datasets` (#2–416), plus shared verification #417. Machine-readable evidence, exact heads, review scopes, run IDs and unresolved decisions are in `backlog-ledger.json`. All415issue states were fetched; none were closed by this work.

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

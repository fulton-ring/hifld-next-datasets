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

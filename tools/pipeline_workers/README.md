# Pipeline workers

`Pipeline workers` polls an organization Projects v2 board every 15 minutes and runs a bounded batch of Codex dataset workers. A manual run defaults to dry-run. Only open issues in this repository with a stable `<!-- hifld-dataset:... -->` marker, `Agent=Codex` and an eligible Status run. No empty-agent fallback. The whole dispatcher batch is serialized across scheduled/manual events; separate cards run concurrently (two by default). Jobs have timeouts, and at most three attempts are allowed per issue before Needs decision.

## Setup

1. Create the **Pipeline workers** organization project and add the dataset issues. Configure single-select **Status** with: Source research, Ready, In progress, Review, Changes requested, Potential breaking change, Blocked, Needs decision, Done. Configure single-select **Agent** with Codex and Claude worker. Assign these cards explicitly to Codex. Keep the initial dataset cards in Source research.
2. Set repository variable `PIPELINE_PROJECT_ID` to the project's GraphQL node ID (`PVT_...`), not its URL/number. Optional `PIPELINE_MAX_WORKERS` is clamped to 1–5; matrix execution is capped at two concurrent workers.
3. Set Actions secret `PIPELINE_CONTROLLER_TOKEN`: a dedicated bot/App or PAT with Projects read/write for the organization and this repo's contents, issues and pull requests read/write, plus read access to checks/statuses. For a classic PAT, `project` and appropriate repository scope are required. Allow that identity to open PRs and obey branch rules. Never use the model API key as a GitHub token.
4. Set `OPENAI_API_KEY` for the Codex Action. API billing is separate from ChatGPT access. The official Action is pinned to a commit. It uses drop-sudo and permission profiles. The model jobs do not receive the controller token or persisted checkout credentials.
5. Merge the workflow PR, run a dry-run, then run one small Source research card live. Scheduled workflows become active on the default branch; manual dispatch is not available until GitHub has registered the workflow. No model calls are made by the dry-run.

The workflow is designed for this public GitHub repository and GitHub-hosted Ubuntu runners. Jobs run Python from the runner; Codex installs its own Node-based CLI through the official action. `uv sync --frozen` provisions the candidate's pinned dependencies before the full repository tests.

## State flow

- **Source research:** worker may write only `docs/dataset-sources/<id>/`. It must identify a maintained producer/delegated distributor, not just a mirror, and write dated evidence plus `source.json`.
- **Ready:** implement the dataset against a maintainer-approved schema contract in `tools/pipeline_workers/contracts/<id>.json`. Worker patches cannot change these contracts or controller/workflow files. Missing or changed contracts route to Potential breaking change, not automatic schema acceptance.
- **In progress:** the trusted prepare job records the attempt and run URL. A fresh job applies the worker's patch to the expected commit, restricts paths, and pushes with a lease. Failure/cancellation can be retried by the next serialized dispatcher tick, using the bounded attempt budget.
- **Review:** separate fresh reviewer sees the exact PR head; tests run in a separate job with no controller/model credentials. Reviewer returns a structured verdict with authority, schema and identifier gates. Any false gate routes to Potential breaking change. Ordinary findings/test failures route to Changes requested, which resumes work on the same PR on a later tick.
- **Done:** only after full tests, independent approval and GitHub's merge rules succeed for the exact head. Research PRs return to Ready and leave the dataset issue open. Implementation PRs close their dataset issue.

A maintainer moving a card to Potential breaking change, Blocked or Needs decision stops subsequent completion/dispatch. The controller never clears those lanes automatically. A maintainer resolves a breaking change by merging an approved contract/migration decision and returning the card to Ready; changing the lane alone cannot fabricate a schema baseline. The worker's output artifacts, including patches for blocked results, remain available for 14 days.

If additional repository checks are pending or failing, the PR stays in Review and is not merged. The dispatcher rechecks a controller-authored approval for that exact head on later ticks without starting a new worker. New commits invalidate the stored approval and route to Changes requested. A GitHub merge refusal or controller failure is visible as a failed Actions run; inspect the run and adjust the card rather than bypassing a branch rule.

## Schema and source contract

`source.json` must identify `source_role`, producer, publisher, landing/download URLs, lineage evidence, source release date, check timestamp, relationship to the archived HIFLD dataset and `published_contract`. The trusted publisher rejects mirror/archive/unresolved roles. The independent reviewer checks that these assertions are supported by actual evidence; a JSON claim alone is not proof of authority.

Approved contracts contain `raw_columns`, `published_columns`, `geometry` and `coverage`. Each column spec records type, nullability, meaning and optional `id_role`/`unique`. `schema_gate.require_compatible` must run on schemas extracted from actual input/output and source-specific ID checks before promotion. It rejects added/missing/changed fields, failed identifier checks and geometry/coverage changes. Retain raw sources losslessly, never drop IDs or publish arbitrary extra columns, and keep the last accepted release on failure. The independent reviewer must verify that the ingest actually invokes this gate (or an equivalent tested check), not merely copies the approved JSON into its evidence.

PASDA/DataLumos/SeerAI/Esri HIFLD archive items are fallback evidence, not maintained agency feeds by default. Check item-level lineage, producer delegation, real release dates and extraction completeness. Different compilations or partial substitutes require an explicit maintainer decision and distinct lineage.

## Local checks

```
python -m unittest discover -s tests/pipeline_workers -v
```

Tests simulate assignments, human lanes, dry-runs, retry phase/budget, stale heads, independent review and merge decisions, and schema/identifier drift. They make no GitHub or model calls. The workflow has not been live-validated until secrets and a real project are configured; a passing local suite does not claim that setup is complete.

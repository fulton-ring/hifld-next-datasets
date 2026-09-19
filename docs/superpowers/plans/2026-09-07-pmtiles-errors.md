# PMTiles Failure Reporting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Preserve useful diagnostics and fail Dagster assets when requested PMTiles generation fails, then deploy and retry NHD Flowline Large Scale 2.

**Architecture:** Keep the current conversion pipeline and streaming subprocess. Retain only a bounded diagnostic tail in memory and attach it to raised exceptions persisted by Dagster. Preserve intentional non-spatial skips and all zoom/CRS/storage semantics.

**Tech Stack:** Python, unittest, subprocess, Fiona/GeoPandas, Tippecanoe, Dagster, Docker, Helm/GKE.

## Approved scope

No zoom-policy or CRS changes. No full regeneration/promotion. A PMTiles-only retry targets `nhd/flowline-large-scale-2/v1.0.0`. Existing NFHL West runs must not be interrupted.

## Task 1: Regression tests and minimal implementation

Files: `src/dagster_hifld/conversion.py`, `tests/test_conversion.py`; optionally one focused helper module/test if needed to avoid growing the large conversion module.

- [ ] Add failure regressions before implementation: subprocess exit 110 raises with command/layer/input count+bytes/elapsed/exit status/error tail; missing executable raises; zero exit without output raises; empty output raises; upload failure propagates; zero FGB inputs do not silently succeed for requested spatial output.
- [ ] Add bounded-output regression: a large newline-free subprocess message cannot cause unbounded buffering, and its trailing diagnostic text remains in the exception. Preserve streaming progress to process logs without inserting every progress line in Dagster's database.
- [ ] Add FGB preparation regressions: failed initial export followed by failed repair/export raises with layer, chunk, feature count and original error; preserve a successful repair and legitimate non-spatial skips.
- [ ] Run focused tests and confirm expected failures: `PYTHONPATH=src <existing-venv>/python -m unittest discover -s tests -p test_conversion.py`.
- [ ] Implement typed contextual exceptions and bounded subprocess streaming. Never use `capture_output=True` or accumulate complete output. On interruption close streams and terminate/reap the spawned process. Do not upload partial/stale output after errors.
- [ ] Run the same tests green. Run full suite: `PYTHONPATH=src <existing-venv>/python -m unittest discover tests`. Run Ruff only on modified sections/modules without unrelated whole-file rewrites; compare any existing baseline findings.
- [ ] Review spec compliance, then code quality; resolve findings and commit.

## Task 2: Integrate and deploy

- [ ] Merge reviewed worktree commit into `codex/geoparquet-source-publishing`, preserving unrelated changes. Verify full tests and `git diff --check`.
- [ ] Build linux/amd64 Dagster image with a new immutable tag, push to existing GCR repository. Reuse existing Helm values and change image tags only; verify rollout and image digest.
- [ ] Verify NHD has no active matching PMTiles run, canonical staged source exists, no output archive already exists, and adequate per-step scratch is configured.
- [ ] Launch only `publish/formats/pmtiles` for `nhd/flowline-large-scale-2/v1.0.0`; verify selected asset, deployed revision, and processing status. Do not promote.
- [ ] Remove merged worktree and task-only temporary artifacts; retain concise deployment/run audit in project docs.

## Deployment audit — 2026-09-07 UTC

- Implementation source: `04a9008`; tests-only lint cleanup: `42c3e51`.
- Integrated into `codex/geoparquet-source-publishing`.
- Fresh normal-layout verification: 318 unittest tests run, OK, one skipped; diff check clean. Ruff still reports pre-existing lint debt; new test findings were removed.
- Spec and code-quality reviews passed. Container smoke test imported the new helper and successfully streamed Tippecanoe 2.49.0 output.
- Image: `gcr.io/hifld-next/dagster-user:pmtiles-errors-04a9008-20260907`.
- Registry/deployed digest: `sha256:77626fb7e7db8807b6d871594bb9562486c5d6ba71507ba5339d1edcf6c51f3d`.
- Helm release `dagster`, namespace `hifld-next-datasets`, revision 54, chart 1.13.21. Existing values preserved, only four image tags changed. Webserver, daemon and user-code pods ready with matching digest.
- PMTiles-only NHD retry: `9d4b6201-e186-4b3a-af17-5650c07ac8a9`, partition `nhd/flowline-large-scale-2/v1.0.0`.
- No production promotion. NFHL West run `99e872e9-2815-43ce-ae36-1cdccb7b377a` left running on its original image.
- Zoom selection and CRS behavior unchanged. This retry gathers durable diagnostics; it does not yet establish the cause or resolution of NHD's historical full-dataset failure.

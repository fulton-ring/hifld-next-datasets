# Storage Safety Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make staging restoration reject unsafe namespaces, preserve immutable object snapshots through conditional promotion, retain recovery backups after incomplete rollback, and keep inventory deterministic and linear.

**Architecture:** Add a typed object-snapshot and conditional-mutation layer to `StagingStorageResource`, then make maintenance inventory carry those snapshots through candidate construction and promotion. Restore operations validate namespace isolation before listing, use one selector-scoped published listing, and surface report-level blockers for unsafe configuration or empty explicit selection.

**Tech Stack:** Python 3.12, unittest, gcsfs JSON API, local atomic filesystem operations, google-crc32c.

---

### Task 1: Namespace and selection safety

**Files:**
- Modify: `src/dagster_hifld/maintenance.py`
- Test: `tests/test_maintenance.py`

- [x] **Step 1: Write failing namespace and empty-filter tests**

Add tests that configure identical, ancestor, and descendant local/GCS namespaces and assert a blocking report before `list_prefix`; also assert same-bucket disjoint prefixes are accepted. Add explicit-filter-zero-match tests asserting `has_failures` and CLI exit 1, while an unfiltered empty inventory remains successful.

- [x] **Step 2: Run the focused tests and verify the current code fails**

Run `uv run python -m unittest tests.test_maintenance` and confirm unsafe namespaces are not rejected and filtered empty reports are successful.

- [x] **Step 3: Implement report-level blockers and segment-wise namespace comparison**

Add deterministic report errors, compare resolved local `(root/prefix)` paths or GCS `(bucket, prefix segments)`, and return before inventory listing when namespaces overlap.

- [x] **Step 4: Run the focused tests and verify they pass**

Run `uv run python -m unittest tests.test_maintenance` and expect all tests to pass.

### Task 2: Linear immutable inventory

**Files:**
- Modify: `src/dagster_hifld/resources.py`
- Modify: `src/dagster_hifld/maintenance.py`
- Test: `tests/test_resources.py`
- Test: `tests/test_maintenance.py`

- [x] **Step 1: Write failing detailed-listing and bounded-list tests**

Add tests for typed snapshots containing size, checksum, generation or local identity, selector-prefix pushdown, and one published listing for a synthetic multi-version inventory.

- [x] **Step 2: Run focused tests and verify failures**

Run `uv run python -m unittest tests.test_resources tests.test_maintenance` and confirm missing snapshot/listing behavior.

- [x] **Step 3: Implement one-pass grouping**

Add `StorageObjectSnapshot`, return detailed local/GCS listings, group version and metadata keys in one traversal, and attach selected source/metadata snapshots to each maintenance result.

- [x] **Step 4: Run focused tests and verify they pass**

Run `uv run python -m unittest tests.test_resources tests.test_maintenance` and expect all tests to pass.

### Task 3: Conditional mutation and checksum parity

**Files:**
- Modify: `src/dagster_hifld/resources.py`
- Modify: `src/dagster_hifld/maintenance.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Test: `tests/test_resources.py`
- Test: `tests/test_maintenance.py`

- [x] **Step 1: Write failing CAS and CRC32C tests**

Add mocked GCS tests proving rewrite calls include source and destination generation preconditions, local tests proving changed source/destination identities reject mutation, and a CRC32C-only local-to-GCS equality test.

- [x] **Step 2: Run tests and verify current unsafe behavior**

Run `uv run python -m unittest tests.test_resources tests.test_maintenance` and confirm unconditional copy/delete and CRC mismatch failures.

- [x] **Step 3: Implement conditional primitives and wire restoration**

Implement conditional copy/write/delete using GCS JSON generation preconditions and local temporary-file atomic create/replace with snapshot validation. Use captured inventory snapshots for published-to-candidate copies and preflight snapshots for candidate promotion, backups, deletes, and rollback.

- [x] **Step 4: Run focused tests and verify they pass**

Run `uv run python -m unittest tests.test_resources tests.test_maintenance` and expect all tests to pass.

### Task 4: Recovery backup retention

**Files:**
- Modify: `src/dagster_hifld/maintenance.py`
- Test: `tests/test_maintenance.py`

- [x] **Step 1: Write a failing promotion-plus-rollback failure test**

Inject a promotion failure followed by a restore-copy failure; assert the report is failed, includes a stable retained backup location, and the backup objects remain after candidate cleanup.

- [x] **Step 2: Run the test and verify the backup is currently deleted**

Run the exact unittest test and confirm the operation-wide cleanup removes the backup.

- [x] **Step 3: Separate candidate cleanup from backup retention**

Track incomplete rollback on a typed exception, delete only the candidate subtree in that case, and report the logical retained backup prefix without its random operation identifier.

- [x] **Step 4: Run maintenance tests and verify they pass**

Run `uv run python -m unittest tests.test_maintenance` and expect all tests to pass.

### Task 5: Full verification and commit

**Files:**
- Verify all modified files.

- [x] **Step 1: Run focused and full tests**

Run `uv run python -m unittest tests.test_maintenance tests.test_resources tests.test_lifecycle_policies`, then `uv run python -m unittest discover tests`; expect zero failures.

- [x] **Step 2: Run static and diff gates**

Run Ruff check/format for touched Python, `uv run python -m compileall -q src tests`, and `git diff --check`; expect zero failures.

- [x] **Step 3: Review and commit**

Review `git diff`, stage only planned files, and commit with `git commit -m "harden staging recovery concurrency safety"`.

### Task 6: Exact committed mutation identities

**Files:**
- Modify: `src/dagster_hifld/resources.py`
- Modify: `src/dagster_hifld/maintenance.py`
- Test: `tests/test_resources.py`
- Test: `tests/test_maintenance.py`

- [x] **Step 1: Write failing generation-addressing and mutation-result tests**

Add a GCS-to-local copy test that asserts `GCSFileSystem(version_aware=True)` opens `bucket/key#captured-generation`. Change conditional copy/write tests to require a returned `StorageObjectSnapshot` created directly from the JSON API response, and add a promotion test that injects a concurrent destination generation after the atomic result is returned.

- [x] **Step 2: Run the focused tests and verify the unsafe behavior**

Run the exact new unittest methods and confirm version-aware construction and returned committed snapshots are absent, and that promotion currently discovers the destination identity through a later lookup.

- [x] **Step 3: Return committed snapshots from atomic primitives**

Make local atomic create/replace return the snapshot captured immediately after the atomic operation. Parse the object resource returned by GCS rewrite/multipart upload into `StorageObjectSnapshot`; make conditional copy/write return that value. Instantiate generation-aware gcsfs for GCS-to-local reads. Thread returned snapshots into promotion mutations and backup bookkeeping without a post-copy identity lookup.

- [x] **Step 4: Protect rollback from the post-commit concurrency gap**

Use the mutation result snapshot as rollback's exact expected generation. If another writer replaces it before rollback, leave that generation untouched, retain the operation backup, and emit a stable failure report.

- [x] **Step 5: Run focused tests and verify they pass**

Run `uv run python -m unittest tests.test_resources tests.test_maintenance` and expect all tests to pass.

### Task 7: Preserve generic streaming writes and clear scoped static findings

**Files:**
- Modify: `src/dagster_hifld/resources.py`
- Modify: `src/dagster_hifld/maintenance.py`
- Test: `tests/test_resources.py`

- [x] **Step 1: Write a failing generic GCS write regression test**

Assert `write_key()` uses `gcsfs.GCSFileSystem().open(path, "wb")` and writes through the file object, while `write_key_if_unchanged()` remains generation-conditional and returns the upload response snapshot.

- [x] **Step 2: Restore the ingestion write path**

Restore local `Path.write_bytes` and GCS streaming `fs.open(..., "wb")` behavior in `write_key()`. Keep the buffered conditional JSON upload private to the maintenance-only `write_key_if_unchanged()` path.

- [x] **Step 3: Fix findings introduced by the maintenance changes**

Narrow optional values before calls, assign intentionally unused results, type JSON-boundary parsing with narrow unions/protocols, and remove new broad-exception/import-order findings without changing unrelated APIs.

- [x] **Step 4: Run full verification and commit**

Run focused and full unittest discovery, scoped Ruff check/format, scoped Pyright/BasedPyright with the project interpreter where available, compileall, and `git diff --check`. Commit the reviewed change as `close storage mutation generation gaps`.

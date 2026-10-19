# Initial verifier and fail-closed release boundary

Issue #417 / PR #421. This implementation is a **partial delivery**, authorized by
the resumed coordinator's explicit implementation request. It supersedes the old
design-only work restriction; the earlier design remains a proposal for the full
system. No real baseline, contract, source or release is approved by this change.

## Delivered behavior

`validation.compare_full_rows` streams one GeoJSON Feature per JSONL line into a
SQLite row/coverage index. It compares every typed key, retained attribute and
geometry and every declared coverage bucket. Integer keys are encoded as typed
bytes, decimals retain their exact JSON precision, and strings are not normalized.
Full differences are retained in a checksummed JSONL sidecar. Missing pins,
malformed/extra fields, null/duplicate keys, unrecognized CRS, unsupported Feature
members (including Feature.id), invalid geometries and incomplete comparisons do
not pass. This first adapter rejects unsupported source structures rather than
silently dropping them. No reprojection or geometry repair occurs.

The accepted baseline reference independently pins the adapter input bytes and
their reviewed derivation from original accepted data objects. The comparison
checks that input pin, not merely a caller's separate checksum argument. Input
stat identities and full checksums are checked before and after comparison.
Reports start blocked before reading so an interrupted retry cannot leave an old
passing report visible.

`release_approval.require_release_approval` is a **manual exact-release acceptance
boundary**, not automatic dataset acceptance. It reads `HIFLD_RELEASE_APPROVALS`
from deployment-owned configuration outside local staging/published roots. It
requires an exact independently authored maintainer decision, accepted baseline
identity/mapping/object checksums and normalized-input derivation, contract digest,
reviewed comparison and provenance digests, full staged and approved published
object inventories, destination identity, candidate comparison-input/output
derivation pins, and a digest of installed `dagster_hifld` source code. A report's
`pass` means identical valid rows; it does not authorize publication. A complete
`needs_decision` comparison can only publish after the exact changes are covered
by an independent registry decision. Invalid/incomplete reports cannot publish.

No registry is created by normal pipeline execution. Absent registry or approval
blocks publication, including direct/local helper calls. The gate rechecks every
staged object's hash and snapshot; immutable conditional copy pins the verified
source snapshot. Added shards, changed bytes and changed code invalidate approval.
Complete provenance/report/sidecar/dictionary/quality evidence must be included in
the downloadable approved inventory. Multi-input manifests merge by explicit
input identity and reject conflicting attribution; dictionaries retain those
inputs and transformations.

The graph now runs generated formats → `publish/verify` → a blocking ERROR
baseline check → promotion. Catalog quality checks are also blocking ERROR checks.
Publication helpers and Portolan discovery/catalog/pointer writes preflight exact
accepted bytes independently of graph selection. In-place GeoParquet maintenance
replacement is disabled; its dry run remains available and changing accepted
bytes requires a separately accepted immutable version.

## Public integration surface

- Existing source assets keep using their normal staging/catalog/format graph.
  Draft staging does not require approval; promotion does.
- Call `compare_full_rows(baseline_path, candidate_path, contract, metadata_dir,
  baseline_sha256=..., candidate_sha256=..., baseline_reference=...)` only when the
  explicit typed JSONL schema/CRS/key contract and normalized input pins exist.
  This can produce an unapproved research comparison; it cannot approve itself.
- Call the existing `run_local_version_pipeline` for local conversion or use the
  actual Dagster graph. Without acceptance, final verification/promotion must
  fail while staged candidate evidence and the previous release remain intact.
- The registry fixture in `tests/test_full_verification.py` demonstrates schema
  shape with synthetic decisions only; it is not a registry for real datasets.

## Remaining #417 requirements

No real accepted-baseline pilot exists. Direct Fiona/Arrow/partitioned readers,
upstream pagination proof, all transformation/lineage reconciliation rules,
independently recomputed normalization from original source/terminal formats,
full per-field/domain/quality accounting, all-source STAC attribution/links and
published metadata roundtrip remain unfinished. The registry currently requires
manual review of exact normalization derivations and every terminal object's
bytes. It is not a substitute for those automated verifiers.

SQLite cache/record/scratch limits and measured scaling bound this comparator's
engineering scope; they do not establish an OS-enforced RSS or disk quota. Quota
checks occur between operations and may overshoot by one record/SQLite allocation.
Conversion, catalogs and downloads have their own memory behavior. Current
Portolan random-generation bundles do not yet provide a reusable pre-review
planning/replay command for exact catalog-byte approval, so they remain blocked
without an independently prepared exact bundle decision.

Tests labeled mechanics isolate the acceptance boundary explicitly; the new
boundary and actual Dagster integration tests use no approval mocks. Synthetic
acceptance is a test fixture, not approval of any HIFLD dataset. The full suite
still cannot pass without sibling `hifld-next-iac/environments/prod/dagster.tf`.

## Executed validation

The [machine-readable record](../evidence/2026-10-19-verification-implementation.json)
pins installed implementation bytes, commands, logs and runtime evidence:

- Frozen dependencies installed without changing the lockfile. Local repository
  loading validated 74 assets with local storage and sensors disabled.
- 18 new verifier/boundary tests passed. The full suite ran 465 tests: 463 passed,
  one missing sibling IaC error, one opt-in production-write integration skipped.
  This is not a full-suite or CI success claim.
- Actual durable local synthetic Dagster run
  `998b82c1-42b5-44e3-8dfc-7e16f9661a05` ran verification, the blocking check and
  promotion successfully. A changed-byte retry
  `1533e4cc-fc97-42c5-9f8d-a3da5e74c6b8` failed before promotion; all eight prior
  published objects and staged evidence were preserved. State remains isolated at
  `/workspace/hifld-worker-runtime/417/integration-head-final`.
- The 100,000-row-per-input streaming probe read 122,355,055 combined input bytes,
  found the sole changed final-row attribute, and measured 61,348 KiB peak RSS.
  This supports the comparator's measured scaling; it does not validate the
  whole pipeline's memory use or an OS-enforced ceiling.

Independent review found and drove regression fixes for unbound normalized
baseline inputs, ignored Feature identifiers, geometry coercion/foreign members,
unknown CRS, numeric overflow, incomplete-report durability, input mutation races
and the maintenance replacement bypass. Exact-head review remains a separate
handoff requirement.

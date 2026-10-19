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

Real draft integrations subsequently exposed two shared correctness issues. The
catalog now rebuilds numeric sample columns from original Fiona values and its
declared schema using nullable numeric dtypes; it does not cast the already
rounded float sample back to integers. Nullable int64 identifiers beyond 2^53,
integer examples and integer extrema remain exact, while source-declared floats
remain floats, including all-null float columns.

An evidenced format limitation is recorded in the **version** source manifest as
`"unsupported_output_formats": {"shapefile_zip": "concrete fidelity-loss reason"}`.
This is a string map, not an acceptance decision. Shapefile generation and reuse
are skipped with that visible reason, and the release gate rejects stale staged
Shapefile artifacts under an excluded format. Canonical source/other terminal
formats remain subject to verification and independent acceptance. The shared
code contains no NOAA-specific branches and does not silently repair source data.

## Remaining #417 requirements

No real accepted-baseline pilot exists. The native-reader follow-up below delivers
pinned GeoPackage, Parquet/GeoParquet and Arrow IPC normalization, homogeneous
multi-file rosters and full retained-row field summaries. Upstream pagination
proof, all transformation/lineage reconciliation rules, additional native source
formats, full domain/quality accounting, all-source STAC attribution/links and
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


## Native comparison and normalization follow-up

`native_verification.normalize_native_input` recomputes evidence from complete
SHA-256/length-pinned native objects; `compare_native_inputs` runs the full typed
comparison and retains both physical schemas. Inputs support an explicit single
GeoPackage container/layer/geometry column, homogeneous multi-file Parquet or
GeoParquet, and Arrow IPC files. A closed suffix-specific roster detects missing,
added, corrupt or changed shards. Geometry metadata and an explicit recognized
CRS must agree; no reprojection, geometry repair, inferred Hive columns, implicit
layer selection or schema projection occurs.

GeoPackage values come from immutable read-only SQLite physical rows, joined to
the full Fiona iterator by its physical primary key. This avoids pandas/Fiona
sample float rounding of nullable int64 values. Driver feature IDs and original
physical key values are retained in the reserved evidence-only property
`$hifld.source_feature_id`. Parquet has no corresponding driver ID and this
absence remains visible. Source fields colliding with the reserved namespace are
rejected. Every retained source field is compared, including recursively typed
Arrow structs such as terminal `bbox`, list elements, maps with duplicate keys,
null distinctions and nested metadata. Integers use exact decimal strings,
floats IEEE hexadecimal values (including negative zero), decimals retain scale,
and dates/timestamps retain raw ticks plus physical unit/timezone. Physical
schema changes cannot yield a passing comparison.

A normalization policy explicitly names `id_fields`, `coverage_fields`, spatial
presence, CRS, null geometry and empty-population policy, with `status` proposed
or reviewed. Physical schemas are captured from pinned original bytes and must
be separately reviewed as part of the exact receipt/report acceptance; a status
string alone grants no permission. The normalization receipt pins parameters,
reader/library versions, implementation bytes, original objects/counts, complete
physical schema, normalized records and a full per-row lineage sidecar. No local
path or clock enters a successful receipt, so identical pinned inputs reproduce
identical receipts across separate roots. Failure first replaces active receipt
status with blocked; incomplete partial files and previous streams cannot
constitute a passing receipt. Unsafe evidence folders inside input captures are
refused before any write. Inputs are rehashed and stat identities/rosters/code
rechecked after streaming.

Native release gates additionally require independently accepted policy digest,
original object bindings, both downloadable normalization receipts and lineage
sidecars, exact accepted baseline derivation and candidate derivation, persisted
receipt/selfhash/schema/population agreement, and unchanged installed reader
versions. Proposed policy, staged self-approval and report pass alone cannot
publish. Source-owned multi-input provenance validation remains unchanged.

Callable/CLI examples (local research, not acceptance):

```sh
PYTHONPATH=src uv run python -m dagster_hifld.native_verification \
  --policy proposed-policy.json --output work/evidence normalize --spec input.json
PYTHONPATH=src uv run python -m dagster_hifld.native_verification \
  --policy proposed-policy.json --output work/comparison compare \
  --baseline-spec baseline.json --candidate-spec candidate.json \
  --baseline-reference unapproved-reference.json
```

An input manifest has `schema_version:1`, `format`, local immutable `root`,
explicit `geometry_column` (null for nonspatial inputs), and a complete `objects`
array of `{key,sha256,bytes}` with optional pinned `rows` and explicit `partition`.
GeoPackage also requires `layer`. Optional `storage_prefix` maps these relative
original keys into the staged version for promotion checks. No source data is
rewritten. The evidence JSONL is not a replacement dataset or approved contract.
Unapproved research references must explicitly say `acceptance:
"unapproved_candidate"`; accepted derivations cannot be created implicitly.

`baseline_field_changes.jsonl` reports every field's changed count and retained
row count through a disk-backed SQL table. Added/removed IDs, duplicate/null keys,
geometry and coverage changes remain in the full existing diff reports. Invalid
rows remain visible in both populations; field summaries do not turn a failed
schema comparison into a pass.

`native_synthetic_local.py` runs actual source, catalog, archival conversion,
comparison, release verification, blocking checks and promotion with an isolated
durable instance. Its decisions and original source are explicitly synthetic.
The missing-registry attempt fails with no published objects; an exact synthetic
registry succeeds; a newly injected shard fails and preserves every published
object. `native_scale_probe.py` separately checks bounded native batches and a
sole changed final row over a multi-file synthetic population.

Unsupported physical types/encodings are rejected, including Arrow dictionary
and extension types, multiple geometry columns, M/empty/curved geometry, embedded
EWKB SRIDs, generated/hidden GeoPackage columns, active SQLite journals, and
heterogeneous physical partition schemas. Other Fiona drivers are not yet a
lossless generic adapter. Geometry is compared as exact ordered coordinates and
type/dimension, not WKB byte order. No general source→terminal field mapping or
FID-loss/derived-bbox decision is invented.

Reader batch/row-group/footer/record/output budgets are checked, but native
library allocations happen before some checks. Disk-backed GeoPackage ID
tracking and roster scans are not OS-quota enforced. These bounds and measured
RSS do not establish a hard memory/disk guarantee. Cross-format schema/FID
changes need an independently reviewed policy; this implementation deliberately
preserves them as differences.


Native follow-up evidence is pinned in
[`2026-10-19-native-verification.json`](../evidence/2026-10-19-native-verification.json):
14 native tests passed; full suite 487 tests produced 485 passes, the known missing
sibling IaC error and one opt-in production-write skip. This is not CI/full-suite
success. Actual durable native full-graph synthetic success
`3e02ff39-2a41-4bc6-a281-e3643ceb4971` followed missing-approval failure
`0f216028-b5e3-4242-9987-0160b8960ec0`; extra-shard failure
`a32428e8-8a65-4640-9c66-8a8896c2865f` preserved all 19 published local objects.
The 100,000-row-per-input probe used 98 native files per input and 256-row batches,
found the sole changed final row, and measured 201,312 KiB peak process RSS. It is
synthetic and includes Python/native-library imports, with no OS quota claim.

The read-only 96-row NOAA #3 unapproved native pilot compared every physical source
attribute and ordered geometry: 13 source fields each changed 0 across 96 retained rows,
geometry 0, IDs added 0/removed 0, coverage 0, null/duplicate keys 0. Terminal Parquet's
additional bbox struct and missing GeoPackage driver IDs were retained, not
projected away: 96 candidate schema errors, 96 FID differences and 96 bbox differences
leave the overall comparison failed. The native machinery exposes the concrete
mapping/acceptance question; it does not authorize dropping those fields.

Independent native review found that a nonnullable Arrow map item could still
contain null through IPC. The follow-up enforces map key/item nullability before
recursive decoding; nested struct/list/map declarations remain enforced. Fifteen
native tests now pass, including this malformed IPC regression and the actual
isolated full-graph preservation check. Earlier native evidence above remains a
record of its original implementation bytes, not an approval of this fix.

Independent review also reproduced an original-byte preservation defect through
pre-existing evidence symlinks. `evidence_io` now refuses symlinked ancestors and
symlink/hardlink output trees before any mutation. Reports use exclusive random
temporary inodes plus atomic replacement; normalized partials and diff/field
streams use exclusive no-follow creation after removing only their old name.
The JSONL comparator also refuses evidence targets that are its own input files.
Unsafe locations raise without altering the input or linked evidence; the caller
must choose a safe evidence directory. This is not a hostile concurrent directory
replacement guarantee. Focused native and original verifier tests cover linked
reports/partials/sidecars, map nullability, original-byte preservation and actual
local success/failed-promotion behavior.

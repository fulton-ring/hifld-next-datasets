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
formats, full domain/quality accounting and general source→terminal transformation rules
remain unfinished. The direct modern publication path now delivers all-input
STAC attribution, links and metadata roundtrip below; the inventory-only builder
explicitly refuses modern inputs without exact metadata byte pins. The registry still requires manual acceptance of exact changes and terminal
object bytes. The JSONL mode explicitly retains a reviewed-normalization trust
scope; native mode now also performs original-object re-derivation and comparison
replay at the release boundary, as described below. Neither mode supplies source
acceptance or the remaining extraction/transformation rules.

SQLite cache/record/scratch limits and measured scaling bound this comparator's
engineering scope; they do not establish an OS-enforced RSS or disk quota. Quota
checks occur between operations and may overshoot by one record/SQLite allocation.
Conversion, catalogs and downloads have their own memory behavior. Persistent immutable Portolan release planning now retains exact UUID, timestamp,
SQLite, STAC and pointer bytes for an external decision. The one-shot path still
blocks without exact acceptance; the new public prepare/apply APIs avoid
regenerating bytes after review. The legacy in-place root-catalog writer and
unplanned rollback are disabled by the later draft compatibility change below.

Tests labeled mechanics isolate the acceptance boundary explicitly; the new
boundary and actual Dagster integration tests use no approval mocks. Synthetic
acceptance is a test fixture, not approval of any HIFLD dataset. The historical full-suite runs below lacked sibling
`hifld-next-iac/environments/prod/dagster.tf`. The final provenance run uses the
restored unmodified real sibling source snapshot and passes as recorded below.

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


## Deterministic native release replay

The initial native receipt gate checked exact labels, inventories and independent
registry pins. Independent review demonstrated that deliberately repinning the
registry and receipt labels could retain a false physical→normalized derivation.
This was not a staging-approval bypass, but it did not meet automatic native
verification. Native release now rereads both original populations and replays
the entire comparison before the first copy.

An independent release registry entry must provide `native_baseline_input`, a
complete native manifest with local immutable original `root`, exact object pins
and the same normalization options used in the reviewed baseline receipt.
Original baseline storage must be independent of candidate staging. The original
bytes must match the accepted baseline object's SHA/length pins. No baseline
originals are fetched automatically; missing configuration, missing files,
changed bytes and extra unpinned shards all block with explicit native-original
errors. Baseline receipts without the original captures cannot establish this
native proof. Preserved public baseline candidates can supply read-only inputs
without establishing acceptance.

Candidate originals are resolved only from the actual staged version and its
explicit storage prefix, with complete roster/hash/count checks. Both inputs
are re-normalized into hardened isolated temporary evidence; normalized records,
per-row lineage, physical schemas, reader/policy/code pins and deterministic
receipts must match. The SQLite comparison is recomputed from the pinned baseline
stream and current native candidate, including ID, value, geometry, coverage and
per-field reports. The whole deterministic result must equal the reviewed report.
Native replay accepts positive reader budgets no larger than trusted defaults;
smaller bounded batch/budget settings are preserved from the pinned receipt.
This is bounded engineering behavior, not an OS-enforced quota.

Reports explicitly identify `native_originals_recomputed` or
`reviewed_jsonl_normalization`. The latter retains the existing manual
normalization-derivation trust scope; it is not upgraded to native proof. A native
claim without native receipts, or any unmatched scope, blocks release. Independent
acceptance remains required after either technical verification scope succeeds.

Synthetic regressions deliberately repin the independent registry while leaving
stale normalized rows or a false passing comparison; both now block. The actual
local full graph additionally preserves all 19 previously published fixture
objects after this false-derivation attempt and after an extra-shard failed run.
No real dataset acceptance is supplied. Exact commands, bytes and run IDs are in
[`2026-10-19-native-replay.json`](../evidence/2026-10-19-native-replay.json).

## Strict captured-provenance validation

`provenance.validate_provenance` is a pure structural validator, re-exported by
`release_approval.validate_release_provenance` with the existing release-error API.
It requires timezone-aware ISO retrieval timestamps, actual calendar dates,
nonblank typed identity/license/attribution fields, exact byte/hash types,
credential-free HTTP URLs, a matching retrieval host, recognized source roles,
unique input identities, captured distributor delegation and named
transformations/code identity. Producer release dates remain separate: a missing
date requires the captured unknown-date explanation, and no date is synthesized.
Requested and observed final URLs are preserved and validated when supplied.
Failed diagnostic attempts are not successful source inputs and need not acquire
invented successful-input metadata. Claims of delegation/license/attribution are
retained, not certified or accepted by structural validation.

Four targeted tests cover valid offset/date variants and malformed values,
including the independently reproduced `retrieved_at="not-a-date"` case. Existing
41 verifier/native tests also pass. Seven current canonical source manifests from
the combined source graph and #11/#17/#20 were validated read-only without any
structural regression; this is not source acceptance. Full attribution/STAC and
metadata roundtrip integration remains the next implementation slice.

Independent provenance review identified offset-minute normalization by Python
and emitted request/final URL aliases. Offset hours/minutes are now explicitly
bounded before timestamp parsing, and canonical `request_url`/`final_url` aliases
are validated and retained alongside `requested_url`/`observed_final_url` without
renaming. Five pure tests pass, including malformed offset and alias credentials.
A failed HTTP attempt cannot be presented as a successful source input when its
captured `http_status` is non-2xx. Separately retained diagnostic attempts remain
untrusted evidence and do not require fabricated successful-source metadata.
The `provenance_projection` helper preserves complete selected input/code/lineage
claims for the upcoming dictionary/STAC integration; it performs no acceptance.


## Complete input metadata and reviewable immutable catalog bundles

Modern source resolution validates the complete metadata after dataset/file/
version input merging. The generated dictionary retains every input and explicit
empty transformation list. `metadata/resolved_provenance.json` is a complete
projection of processor, inputs, transformations, code identity and lineage.
Content-addressed snapshots preserve every authored manifest layer without
overwriting older receipts; `manifest_layers` records its original hash, source
key, input identities, processor/code and transformation claims. Raw layer
metadata is retained as evidence, not upgraded into source authority. Catalog
metadata writes reject linked local output trees and use conditional atomic
replacement rather than truncating aliases.

The release boundary checks the resolved manifest and dictionary against current
complete source metadata, requires all original layer snapshots to be pinned and
downloadable, and blocks missing/lost/changed input attribution. Diagnostic failed
attempts remain separate evidence. No producer/license/date/delegation decision
is inferred. Modern metadata is structurally required before conversion or
promotion in the direct Portolan workflow.

`portolan.provenance.attach_published_provenance` reads actual published metadata
and binds its checksummed byte assets to a `CatalogRecord`. STAC retains complete
input objects with original notices, request/final URL spellings, retrieval dates,
unknown release explanations, transformations and code identity. Every producer
appears separately from the HIFLD Next processor/host. Canonical URLs receive
`derived_from` links; retrieval/request/final URL aliases each receive explicit
input-scoped links. All metadata files, original layer snapshots, comparison
receipts and lineage sidecars become checksummed metadata assets and SQLite asset
rows. Declared roles, input links and complete metadata assets are validated.
The inventory-only importer explicitly fails modern multi-input records without
original metadata SHA-256 pins; it never silently projects them into legacy
single-publisher records. The existing archive-only mode stays distinct.

`prepare_portolan_release_bundle(records, request, published, plan_dir)` writes a
new local plan containing exact immutable STAC/SQLite/pointer bytes, destination,
implementation and metadata dependency snapshots. It performs no public write or
acceptance. `apply_portolan_release_bundle(plan_dir, published)` validates the
whole file roster and byte hashes, STAC semantics, actual published metadata
contents and dependency snapshots, current pointer identity and independently
accepted bytes for every object. It writes a new immutable release and changes
the pointer last. Missing approval, changed bytes, removed dependency labels,
conflicting metadata or an omitted input fail before publication. It does not
regenerate UUIDs/timestamps/SQLite after review. Partial immutable bundles from a
storage failure are retained; the accepted pointer stays unchanged unless its
final conditional update succeeds. Arbitrary concurrent filesystem directory
replacement and hard OS quotas remain outside this local trust/engineering scope.

Reproduce the actual isolated synthetic roundtrip (fixture-only decisions):

```sh
PYTHONPATH=src:. uv run python -m scripts.verification.provenance_roundtrip_local \
  --work-dir /workspace/work/new-empty-synthetic-roundtrip
```

The `b9d6b69` run retained two fixture inputs and roundtripped all 19 metadata assets
and 19 corresponding SQLite rows. Missing bundle approval, removed dependency
labels and deliberately repinned omitted-input STAC were blocked while all 40
published fixture objects remained unchanged. Its native full graph published
22 fixture objects only after an exact synthetic decision; bad provenance, false
native derivation and extra-shard attempts preserved them. The pointer selected
generation `55b31d63-dc3d-49ff-8da1-f1a411bfc3f1`. No real acceptance or production
write occurred.

Local repository suite at `b9d6b69`: **504 tests, 503 passed, one opt-in production-write
integration skipped, no errors**. The real sibling IaC snapshot at commit
`56aa10757129c457c530d707d9a40bae13e0907f` was present; no fixture replacement or
infrastructure command was used. Historical missing-IaC results remain unchanged
as historical evidence. This is local validation, not a CI assertion.

Seven current canonical source manifests pass structural compatibility. Both
provided AIANNH #8 captures retain processor `HIFLD Next draft AIANNH source
adapter`, which fails the already established exact `HIFLD Next` processor rule.
A processor-only in-memory diagnostic found no additional structural failure;
source-owned repair was requested without modifying captures or approving any
source. This actual blocked case is recorded explicitly, not counted as valid.

The bounded [evidence record](../evidence/2026-10-19-provenance-stac-roundtrip.json)
pins implementation bytes, commands, logs, exact run IDs, metadata roundtrip and
compatibility outcomes. Real accepted pilot/source/contract decisions remain
external. Additional native formats, complete pagination proof, all transformation
reconciliation and domain/quality policy still require further implementation.

## Prepared-release dependency repairs after independent review

Independent review of `b9d6b69` found three concrete integrity gaps despite exact
synthetic byte decisions: a metadata URL could name an unrelated host while its
object-key label named the real object; a prepared plan could outlive changes to
its referenced data; and SQLite metadata asset rows could be omitted while STAC
still retained them. These are repairs to technical consistency, not acceptance
decisions or claims of a missing-approval bypass.

Preparation now derives a complete dependency roster from the final rendered
STAC, including prior seeded collections, every data file and shard, styles,
thumbnails and metadata. Each asset requires a SHA-256/byte length, actual object
identity and exact `public_root/object_key` download URL. Unpinned external asset
URLs fail explicitly. The public root also matches the rendered catalog self
URL. SQLite's complete asset, canonical location and object populations and all
corresponding rendered fields are independently reconciled with STAC; changing
both the SQLite pointer hash and external exact byte decisions cannot hide
omitted rows or inconsistent object keys, URLs, roles, types or checksums.

Apply re-derives the complete asset roster and validates actual snapshots and
byte hashes before any new release write. Its local pointer commit rechecks all
dependency identities after fsync of the temporary pointer, then checks the
pointer's expected identity again. A detected late failure conditionally removes
only this attempt's own unchanged newly-created release objects if the accepted
pointer remains unchanged; the complete local plan stays available. An
interruption after pointer commit keeps its selected generation intact. No
failed verification repairs or overwrites a changed data dependency.

This local guard does not implement a cross-object cloud transaction and cannot
prevent an arbitrary external writer from altering data after publication. Data
immutability and excluding uncoordinated writers remain operational
prerequisites. **Remote prepared apply fails closed**, before any writes, until
generation-qualified dependency URLs or a transactional remote protocol is
implemented. At that repair checkpoint the legacy in-place catalog/rollback
surface retained its separate scope; the later draft change below disables both.
A future remote or legacy workflow must not be described as this local prepared proof.

The actual synthetic runner now deliberately approves repinned wrong-download
STAC/SQLite bytes and omitted SQLite rows, mutates a GeoParquet after planning,
and injects a real dependency replacement after new release-object writes but
before pointer commit. Each must block, preserve the previous pointer and
40-object roster, preserve all objects except the deliberately external-mutated
dependency itself, and retain the plan. The test, not apply, restores that
fixture's injected mutation before the next isolated probe. No producer source
capture or accepted real dataset is involved.

The repaired frozen code passed **505 local tests: 504 passed, one production-write
opt-in skipped, zero errors/failures**, using the same real pinned IaC dependency.
The durable run selected synthetic generation
`82d9d475-b6ef-42bd-a9c1-818a2790755b`, verified 21 referenced dependencies
(19 metadata assets), and blocked all four new negative cases. Its native
success was `81ff1c6b-a4bc-48ea-ad0d-60cf41b5ed30`; missing approval and extra
shard runs were `8434beea-97d2-410f-b266-1a6ebadf0d19` and
`bfa18cbf-76f3-48f1-aaa6-46527505e9a8`. Exact implementation/log pins and the
original independent reproductions are in the
[repair evidence](../evidence/2026-10-19-stac-dependency-repair.json). This is local
validation awaiting a new independent exact-head review, not a CI or real
acceptance claim.

The independent `20452e2` follow-up identified one omitted projection field:
SQLite asset-object native and CRS84 bounds could differ from the unchanged
rendered Collection after deliberate exact DB/pointer repins. Both bounds now
match `hifld:native_bbox` and the normalized Collection spatial extent, including
the existing unknown/clamped extent policy. The catalog-only replay rejects all
five negative cases while retaining its 40-object roster and pointer; no terminal
data was rebuilt for that replay. Forty-nine focused publication/workflow tests
passed; the full suite was not repeated for this narrow checkpoint.

The source-owned AIANNH correction at `bac1c53` (final docs `19061d06`) produced
new canonical metadata under `processor-normal-graph`; its single-input manifest
now passes pure structural validation with processor `HIFLD Next`. Earlier
`f066` processor-label failures and captured bytes remain historical and
unchanged. This is metadata structure compatibility, not dataset acceptance.
See the [narrow repair evidence](../evidence/2026-10-19-stac-object-bounds-repair.json).

## Actual Dagster persisted-plan integration and proposed compatibility changes

The registered partitioned asset `publish/portolan_catalog` now requires explicit
run configuration. With `mode: prepare` it reads already promoted modern metadata
and GeoParquet from the supplied physical storage prefix, validates metadata and
quality consistency, and creates a new persistent exact release plan. It does not
convert formats, generate styles/thumbnails, promote source data or write any
published object. With `mode: apply` it consumes that same persisted directory;
it never rerenders UUIDs, timestamps, SQLite or STAC. The same dependency,
provenance, SQLite, byte-approval and guarded local pointer checks still apply.

Example asset configuration (paths must be outside staging/published storage):

```yaml
ops:
  publish__portolan_catalog:
    config:
      mode: prepare # a separate reviewed run changes this to apply
      plan_dir: /workspace/work/reviewable-catalog-plan
```

The selected collection, dataset/file/version partition, staging and published
backend/location/prefix identities, public root and storage slug are bound into
the plan. Apply checks those identities against the actual selected run before
publication. Exact plan approval is additionally required using the logical key
`_catalog/reviewed-plans/{generation}.json` in the external registry's
`published_objects` byte-decision map. This key identifies an **approval-only
artifact**: no public object is written there. Its SHA-256 is computed from
`prepared_plan_approval(plan)` canonical UTF-8 JSON bytes, so review includes the
selection, dependencies, previous pointer and generated object roster, not only
the individual STAC/SQLite bytes. Preparing or parsing that plan cannot approve
it. Reusing a plan for another partition/storage/root or modifying its labels
after the decision fails closed.

These are explicit **proposed compatibility changes in this draft PR**, not
approval to deploy them: enabled deployments must configure prepare/apply and a
persistent external plan directory; implicit one-shot asset publication is
disabled; the previous in-place root STAC/SQLite writer is removed and requests
for it fail before conversion/promotion; unplanned `rollback_portolan_release`
fails before any storage access. Equivalent persistent rollback planning and
full dependency verification must be implemented and reviewed before restoring
that operation. Existing direct one-shot immutable APIs retain all strict
guards, including the exact plan decision; they cannot substitute an unreviewed
random bundle for the persisted asset workflow. Archive public-domain status
is now opt-in via `HIFLD_PORTOLAN_ARCHIVE_PUBLIC_DOMAIN=1`, rather than inferred
for a new producer dataset. Maintainers must decide migration/deployment/API
compatibility; this code grants no baseline, contract, source, license or
breaking-change acceptance. Remote apply remains explicitly unsupported.

The actual registered asset was executed in a separate durable Dagster instance
using preserved own synthetic terminal bytes (no data rebuild or approval
mocks): prepare succeeded without published writes; missing approval, wrong
partition, wrong public root, changed staging identity and modified plan values
failed for their expected audited reasons; an exact synthetic decision applied
the saved plan without changing any prepared file. A second actual prepare and
post-plan data mutation failed while preserving the selected prior generation,
all 57 object keys/snapshots present at the start of the failed attempt, and
review evidence. In-place and rollback entrypoints also failed with that same
published storage unchanged. Their deliberately injected external data mutation
was restored only by the fixture. All actual run IDs and failure-reason event
records are preserved in the evidence described below.

The frozen implementation passed **505 local tests: 504 passed, one opted-out
production-write integration, zero failures/errors**, with the unmodified pinned
real IaC dependency. Actual prepare was
`66b6b7c0-a135-4bda-a311-0d945abc4cff` and apply was
`53eef9d0-6cb5-4ffd-aeaf-3f0a2a824795`, selecting generation
`4b94cbad-9c69-4726-a4cd-d4e409abede5`; actual drift failure was
`8add275c-7ef9-4a14-af40-1f3c677cff8a`. The
[persisted-asset evidence](../evidence/2026-10-19-dagster-persisted-catalog-plan.json)
pins all code/log bytes, nine actual runs, expected failure-reason audits and the
explicit compatibility proposal. The narrow `d28dbaf` bounds repair has its own
independent PASS; that review does not cover this new Dagster/API slice. No CI,
production, source/baseline/contract or compatibility acceptance is claimed.

## Preserved native artifacts versus whole-format prohibitions

Source #30 retained faithful native Date32 GeoParquet, while #31 retained a
source-produced typed GeoParquet without generic derived columns. Both used
`unsupported_output_formats.geoparquet` to prevent lossy generic regeneration.
That field correctly prohibited *all* staged GeoParquet at the release boundary,
so even a future legitimate source/baseline decision could not promote their
preserved output. No real dataset is accepted by correcting this distinction.

New draft `prebuilt_output_policy` version 1 declares a complete native artifact
roster and forbids generic regeneration. Initial support is narrowly GeoParquet:

```json
{
  "prebuilt_output_policy": {
    "schema_version": 1,
    "formats": {
      "geoparquet": {
        "reason": "Captured evidence explains why generic regeneration is forbidden",
        "objects": [
          {"path": "geoparquet/native.parquet", "sha256": "<actual SHA256>", "bytes": 123}
        ]
      }
    }
  }
}
```

Every path is relative to the dataset/file/version directory. The entire format
roster, SHA-256, length and stable storage identity are verified before returning
prebuilt format outputs and again at release. Missing, extra, changed, malformed
or conflicting declarations raise; they never fall back to the generic converter
or delete/rebuild the prebuilt artifact, even when overwrite is enabled. Dagster
format metadata records `generation_mode=preserved_prebuilt`, disabled generic
regeneration and the original evidence reason. This capability does not validate
Parquet contents by itself: full native comparison, input derivation, provenance,
quality, exact code/object pins and independent source/baseline/contract/report
decisions are still required for promotion. The native receipt supplies the
physical proof; a generic-converter layout manifest is not fabricated for an
artifact the generic converter never generated.

`unsupported_output_formats` retains its complete prohibition. A format cannot
simultaneously be wholly banned and declared prebuilt; ambiguous overlap is
rejected. Unsupported prebuilt encodings are rejected rather than inferred.
Both policies are validated before metadata compaction, retained through source
resolution, dictionary and resolved provenance, and therefore carried into the
existing full STAC/metadata projection. Release uses the resolved dataset/file/
version policy, including inherited declarations. Current older resolved metadata
that omitted these capability declarations requires a newly reviewed canonical
metadata generation; it is not silently relabelled or rewritten in published
storage. This is a proposed draft metadata/API compatibility change requiring
maintainer review, not source/format/contract or deployment acceptance.

The synthetic Date32 fixture executes the actual registered catalog, format,
comparison, quality/verification and promotion assets with isolated durable
state. Exact synthetic-only decisions publish two rows with Date32/null dates,
large and nullable Int64 values, source FIDs and geometry retained. Instrumenting
the generic GeoParquet converter to raise proves zero calls. Missing approval,
failed quality (also checked directly after deliberate exact quality-byte repin),
staged native mutation and a whole-format ban all block and preserve the prior
21-object fixture release. Policy bytes also match published dictionary and
resolved provenance. No real source metadata or #30/#31 branch is changed here;
source owners must deliberately integrate this shared checkpoint before changing
their declarations and validating their new outputs.

Frozen local suite: **508 tests, 507 passed, one production-write opt-in skipped,
zero failures/errors**, with the real pinned IaC dependency. Actual Date32 success
was `e47d1098-2a28-4ab1-bf4a-557c703cbe4e`; missing approval,
quality failure and staged mutation were respectively
`efeb555b-7880-4282-bca1-1215b9c3c639`,
`66a1ab24-7077-454e-8cfe-e66024e8323e` and
`19edc21e-02da-47b9-a249-7d4fd6349398`. Their durable failure-event reasons were
audited. The [prebuilt evidence](../evidence/2026-10-19-prebuilt-date32-policy.json)
pins current code, original source-card observations, published schema/selected
typed values, complete runner/log hashes and all results. New checkpoint review
is pending; prior `abde85b` independent PASS covers its earlier catalog slice,
not this new capability policy. No CI or real acceptance claim is made.

Independent review of `81433b3` reproduced a cross-layer defect with real native
Date32 bytes and exact synthetic decisions: replacing the entire ban dictionary
let a version's PMTiles ban erase a dataset's GeoParquet ban and thereby admit
its prebuilt GeoParquet. The follow-up now unions whole-format bans across every
authored dataset/file/version layer. Empty or unrelated narrower maps cannot
clear an inherited ban; explicit null remains invalid. Distinct inherited
prebuilt declarations are rejected instead of silently replacing the parent's
roster/reason, and the fully merged policy is validated even for legacy metadata
without modern provenance. No allow/unban override has been invented. Original
authored files and their existing snapshots are not modified by resolution.

Three additional regression tests exercise different-format multi-layer bans,
empty/null/explicit declarations, preserved raw/snapshot bytes, and the actual
writer and release helper under exact synthetic object decisions. A second
standalone replay with preserved real typed Date32 fixture bytes confirms both
entrypoints reject the inherited ban before output/copy, leaving all 24 staged
objects and its prior published sentinel unchanged. The same registered Date32
graph still succeeds for a valid prebuilt policy with zero generic converter
calls and preserves its 21 published objects after missing approval, quality,
staged mutation and whole-format-ban failures.

Frozen validation: **511 local tests, 510 passed, one production-write opt-in
skipped, zero errors/failures**; 36 focused tests passed. Actual Date32 success
was `dc98d9b0-bcb6-402d-ad8b-5c1ed379a8d1`; missing approval,
quality and mutation runs were `40e47558-2f3b-47af-b4a7-56457ab00d1d`,
`233d2a0a-d3c9-4433-8b85-68b3495ad0ae` and
`6d42ab87-3801-4114-9516-bf1b948b5dfa`. The
[inherited-policy repair evidence](../evidence/2026-10-19-inherited-output-policy-repair.json)
pins the original independent reproductions, code and logs, new actual runs and
native replay. This narrower checkpoint awaits independent review; the parent
`81433b3` finding remains honestly recorded. Source #30/#31 integration and all
real acceptance/deployment decisions remain external.

The inherited-policy repair `4800e4f` subsequently received a separate narrow
independent PASS: 36 focused tests, original real Date32 writer/promotion positive
replays, ten inherited-ban/null-map negatives and an actual registered fixture
graph. That review does not imply acceptance of any real dataset or of the next
additive helper.

A new additive `construction_provenance.validate_construction_code_identity`
checks an original processing-code claim against the actual local Git object
store. It requires a canonical full SHA-1/SHA-256 commit reachable from HEAD or
an actual branch/remote-tracking/tag ref, then verifies SHA-256 for **every**
declared source/project/lockfile against a regular committed blob. It does not
compare to current HEAD: genuine historical processing code remains valid after
documentation-only commits and after later implementation changes. Unknown,
zero, dangling, non-commit, missing or mismatching claims fail with stable
`ConstructionCodeIdentityError(ValueError).code` reasons. Committed symlinks,
gitlinks, directories, traversal/noncanonical paths, invalid hashes and oversized
rosters/blobs reject. Reads use literal argument lists without a shell, disable
Git object replacement/lazy fetching, support linked worktrees, ignore inherited
repository-selection controls and leave working files untouched.

This is a Git content proof, not proof of execution time, signed authority,
complete dependency discovery, dataset acceptance or release permission. The
caller must independently bind the original construction manifest, complete
processing-file roster and capture bundle outside the mutable candidate cache.
Existing source adapters are deliberately not wired by this shared-only change;
source owners must integrate the reviewed helper and test their own lifecycle.
The original Git object store and qualifying refs remain trusted inputs. No
existing release gate or accepted storage behavior changes here.

The eleven focused tests use real temporary Git repositories and cover historical
code, every declared source/lock/project hash, SHA-256 repositories, unreachable
commits, linked worktrees, unavailable Git/missing objects, literal shell/pathspec
filenames, symlinks/gitlinks, replace refs and size/type/path rejection. The
reproducible `scripts/verification/construction_identity_local.py` additionally
verified all 50 actual Python/project/lockfiles at frozen code checkpoint
`0223a2ccab8cd7f10d291c90ead94956b09493c4`, reproduced a historical docs-only
positive and five false-code claims, and preserved isolated original-source and
prior-release bytes. It performs no promotion and requires no dataset decisions.
The [construction identity evidence](../evidence/2026-10-19-construction-code-identity.json)
records exact code, test, replay and validation pins. Independent review of this
new helper is separate from the earlier policy/verification scopes.

Frozen full-suite validation of code checkpoint `0223a2c`: **522 local tests,
521 passed, one production-write opt-in skipped, zero errors/failures** in
75.632 seconds, using Tippecanoe and the real pinned sibling IaC checkout. No CI
success is inferred. The focused module has eleven passing tests. This additive
content validator does not require rerunning large source-data builds; existing
registered synthetic Dagster integration tests ran as part of the full suite.

The Git/content helper `0223a2c` and replay/evidence followup `b2fd862` received
separate independent PASS (all 50 actual processing blobs, historical/current
commits, dirty-worktree/env/graft/ref adversarial probes and all five false-code
claims). That content proof does not establish execution time or acceptance.

`construction_state.py` now supplies an additive **local** durable first-construction
protocol. `canonical_construction_bundle(version, fingerprint, payload)` creates
a bounded canonical JSON envelope. `seal_first_construction(storage, record_key,
version, fingerprint, body)` stores complete original bytes under a SHA-256 CAS
key, then a write-once marker binding version/fingerprint/digest/evidence key,
then the primary alias. `load_first_construction` requires all three, with exact
CAS bytes equal to the alias. Returned original bytes never become newer refresh
receipts. Record keys live under reserved `_construction-state/`, outside normal
dataset/file/version discovery and whole-candidate quarantine. The storage
prefix, key components, version, fingerprint, JSON types, nesting and size are
validated. `ConstructionStateError(ValueError).code` exposes stable reasons.

ANY incomplete state blocks creation, including marker-only, CAS-only, retained
pending writes and alias-without-marker. Repeated refresh after quarantine cannot
rebind the same version even with new receipts. Marker-write failure after CAS
preserves the original complete bytes; a missing/corrupt referenced CAS blocks
both load and seal despite valid marker/alias labels. There is no bootstrap or
automatic trust repair. An operator may restore **exact independently trusted
original bytes**; the synthetic harness demonstrates this external action and
the API never performs it. If an actor removes **all** outside witnesses,
first-use and deleted history are indistinguishable without a separately
protected inventory: this helper does not invent that guarantee.

Writes use descriptor-relative no-follow directory/file access, bounded regular
single-link reads, durable temporary writes, atomic no-replace links and fsync.
Conflicting writes preserve the concurrent winner and the complete losing bundle
as evidence. Interrupted temporary evidence remains retained. Remote storage is
explicitly unsupported/fail-closed; no cross-object cloud transaction or global
lock is claimed. Callers must retain their common version lock, finish and
validate a private candidate first, and independently validate original capture
semantics, immutable processing identity and Git content. Payload semantics,
complete source receipts and provenance remain source-owned. This change does
not wire or rewrite any existing dataset adapter, change release gates, approve
a source/baseline/contract or authorize production.

Ten new real-filesystem lifecycle/fault tests and eleven prior Git tests passed.
The new `scripts/verification/construction_state_local.py` registers actual
isolated Dagster construction/preparation/promotion assets and durable jobs.
It verifies initial construction, newer-receipt refresh, source failure, missing
alias preparation, **two** failed refreshes, exact external restoration and the
real missing-registry release gate. The prior raw source, outside witnesses and
published sentinel remain unchanged. The initial harness attempt failed before
any construction on Dagster telemetry's unwritable default home; the isolated
home/telemetry configuration was corrected and that historical failed run is
retained. The [state evidence](../evidence/2026-10-19-first-construction-state.json)
records frozen code, full validation, actual run IDs and this limitation.

Frozen state checkpoint `a86e831` validation: **532 local tests, 531 passed,
one production-write opt-in skipped, zero errors/failures** in 111.075 seconds,
with real Tippecanoe and the pinned sibling IaC checkout. The frozen registered
fixture succeeded at `23521301-614d-4547-9e35-3fcdd67a0904`, preserved original
receipts at `05b75545-3477-4a3d-8271-360a035b3619`, and blocked five intended
failure runs; its second missing-alias refresh was
`06a9c69e-4b62-4666-97af-402b8f249585`. All four staged objects and the published
sentinel remained byte-identical. Exact code/test/harness and log pins are in
the state evidence; independent review is pending for this new slice. No CI or
real release success is inferred.

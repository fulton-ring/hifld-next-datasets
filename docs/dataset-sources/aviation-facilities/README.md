# Aviation Facilities — working local draft, release blocked

The explicit normal Dagster ingest now reconstructs the official FAA NASR-derived
BTS NTAD feed. This is draft implementation authorized by the user while source,
baseline and contract acceptance is pending. It does not grant those approvals.

The official `USDOT_BTS` item explicitly credits FAA as producer and BTS as
distributor. The BTS government site links its NTAD portal; the original item
metadata identifies FAA and the original resource metadata states publication
2026-10-02. Every complete source row has effective date `2026/10/01`. Fetch time,
portal modification, publication and effective dates are recorded separately.
The DHS crosswalk and archive still return provider403; no exact crosswalk match
is invented.

Complete anonymous extraction has **19,427 native EPSG:3857 points and91 original
attribute columns**, reconciled across10 pages against independent count/ID/schema/
edit witnesses before and after extraction. It is a bounded consistency check,
not a transactional upstream snapshot. Every receipt binds the exact official
request URL and observed final URL; changed redirects require review. Complete,
failed and partial responses remain durable quarantine. Duplicate JSON members,
wrong/missing fields, physical-type changes, invalid coordinates, precision loss,
incomplete IDs/pages and changed witnesses fail closed.

Raw `OBJECTID`, `SITE_NO` and `ARPT_ID` survive unchanged. Source `SITE_NO=23747.31`
occurs twice, as in the historical candidate; both rows remain intact. Only the
actual unique OBJECTID census establishes extraction completeness. No substitute
row ID, field deletion, projection, geometry repair or destructive type mapping
is introduced. Source-typed Arrow Parquet and the Arrow-stream GeoPackage retain
all91 column names, widths, nullability and values plus every original point.
The generic terminal Parquet is spatially reordered, so its full comparison uses
exact preserved OBJECTID correspondence rather than row position. Any nullable
metadata differences and derived bbox columns are explicitly unapproved.

The full generation-pinned historical candidate has19,952 rows. Full diagnostic
ARPT_ID correspondence finds19,001 matched,426 added and951 removed identifiers;
18,100 matched points are exactly unchanged and901 changed. The source adds
OBJECTID, absent from the historical candidate, and9 physical integer columns
are source int32 versus historical int64. Both contain the same known duplicate
SITE_NO. Every common attribute, ID, geometry and state coverage is compared in
[evidence/baseline-comparison.json](evidence/baseline-comparison.json). These are
migration decisions, not evidence for silently replacing the accepted release.

The daily10:00 New York source-only integrity poll is registered but STOPPED.
Source release cadence remains28days. Asset, schedule and staging entrypoints
refuse remote or PublishedStorageResource destinations before fetching/writing.
Candidate versions bind all raw/receipt hashes, installed pipeline fingerprint
and draft schema policy; refreshes append immutable versions and identical replay
returns the same candidate. Shared #417 quality and independent release gates
block promotion. A failed gate must preserve all raw/candidate and accepted data.

Shapefile is excluded because original field names exceed DBF's ten-character
limit. An actual PMTiles experiment assigned sequence feature IDs and thinned
dense tiles; its quantized geometry/ID/coverage mapping is unapproved, so this
rendered derivative is also excluded. Exclusion is not approval to omit published
formats. Primary typed candidates remain complete and release remains blocked.

Reproduce offline with the repository environment (no credentials or live input
needed). The bounded5.3MB evidence bundle includes the full2.4MB generation-pinned
baseline candidate and every full source page; repeated attempt bodies are stored
once and reconstructed only after SHA/byte validation:

```sh
uv sync --frozen
uv run python docs/dataset-sources/aviation-facilities/replay_capture.py /tmp/aviation-capture
PYTHONPATH=src uv run python docs/dataset-sources/aviation-facilities/run_local.py /tmp/aviation-capture /tmp/aviation-local-run
PYTHONPATH=src uv run python docs/dataset-sources/aviation-facilities/compare_baseline.py /tmp/aviation-audit /tmp/aviation-capture
PYTHONPATH=src uv run python -m unittest tests.test_aviation_extract tests.test_refresh_registration_20
```

`run_local.py` uses the normal registered asset graph, isolated DAGSTER_HOME,
local staging/published roots and registered dynamic partitions. It verifies
all produced rows/IDs/physical types/nullability/native points/provenance, runs
idempotent replay, then probes missing-approval and failed-quality preservation.
The preservation sentinel and deliberately failed quality witness establish
implementation behavior only; neither approves a real dataset.

Validation details and exact run IDs are in `evidence/local-materialization.json`.
The14 focused tests include every replayed metadata/count/ID/page HTTP200
application-error marker with valid repinned receipts; all are quarantined before
candidate construction. The real normal ingest alone is discoverable by the
normal version sensor with no manual data/manifest copies. Its17 actual input
records pass shared `validate_release_provenance` independently of an approval
registry, with original URLs/observed final URLs, retrieval times, raw hashes and
lengths, producer/distributor attribution and original license text. This
structural validation does not approve source authority or a release. The actual
clean code commit and installed pipeline fingerprint are asserted in the run.
The full suite at registration code `be93f22512056f29b1c31383f0b71061c36e84b6`
ran505tests:503passed,1production-write test skipped,1error for missing sibling
`hifld-next-iac/environments/prod/dagster.tf`. Subsequent precision/format changes
are covered by the focused source and registration tests and final real graph.
No CI or full-suite success is inferred.

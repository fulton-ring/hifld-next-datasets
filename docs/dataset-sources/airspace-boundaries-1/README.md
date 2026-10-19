# FAA Airspace Boundaries: draft pipeline and migration decision

Issue #10. This branch replaces the legacy convenience download path with a
complete-extraction adapter that retains exact Esri JSON and producer GeoJSON,
all 38 source fields and both `GLOBAL_ID` and `GlobalID`. Its typed Arrow candidate
also retains the original GeoJSON Feature.id; it never invents a row identifier.
Source physical types/nullability are explicit, and geometry is never repaired.
Outputs are immutable local quarantine objects. Remote storage is refused. This
is a working draft extraction pipeline, with promotion deliberately blocked.
The existing aviation-facilities asset remains unchanged.

The 2026-10-19 source snapshot has 477 features, independently reconciled against
count, returnIdsOnly and the terminal page. The FAA AIS item states an effective
window of 2026-09-03 09:01Z–2026-10-29 09:01Z. Its portal edit date is not used as
release evidence. The exact DHS crosswalk remains unread: DataLumos returns a
provider Cloudflare403 challenge; the runtime is unrestricted and this is not a
CONNECT denial. The FAA portal account/organization and item corroborate source
attribution, pending maintainer acceptance.

The full pinned published candidate has 375 rows. GLOBAL_ID joins find 374 common,
103 source-only and one candidate-only row. 336 common OBJECTIDs change. The
current source additionally exposes `GlobalID`, `Shape__Area`, `Shape__Length`;
the published candidate has truncated `Shape__Are`/`Shape__Len` and added `id` and
`bbox`. Current double-valued UPPER_VAL/LOWER_VAL are int64 in the candidate.
Every common geometry differs under exact normalized comparison. The old file's
declared4326 coordinates extend beyond ±180; the current native4269 service is
also unwrapped, while its4326 GeoJSON export wraps the antimeridian. The audit
cannot attribute each difference to edits versus export/reprojection. That export
contains22invalid polygons. These require a reviewed geometry migration or source
fix; they are retained unrepaired and fail the actual blocking catalog check.

`source.json` provides the complete unapproved schema proposal and concrete
maintainer decisions. No baseline, contract, migration or source is accepted.
The new pipeline is bounded to the currently verified single-page scope: if a
future count exceeds the service maximum/2000rows, it fails before staging rather
than treating a truncated response as complete. Refresh scheduling/deployment is
not enabled by this draft.

## Reproduce

Use the repository's frozen environment and set `PYTHONPATH=src`: the shared
venv's editable installation points at the original checkout otherwise.

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p test_faa_airspace.py
PYTHONPATH=src .venv/bin/python docs/dataset-sources/airspace-boundaries-1/audit.py /workspace/work/hifld-coordinator/10
PATH=/workspace/.tools/tippecanoe/usr/bin:$PATH PYTHONPATH=src .venv/bin/python docs/dataset-sources/airspace-boundaries-1/run_local.py /workspace/work/hifld-coordinator/10 /workspace/work/hifld-coordinator/10/review-replay
```

The three complete large source/candidate bodies stay in the preserved runtime
folder, with checked SHA256/byte counts in `evidence/`. Git retains bounded audit
results, metadata, source pages and replay code. Full offline reproduction needs
those exact bodies; the generation-pinned GCS URL can recover that candidate,
but live FAA refetch cannot reconstruct an earlier mutable source snapshot.
Keep the runtime files for independent acceptance review.

The real Dagster replay creates its own persistent instance and local stores,
materializes the ingest, registers a dynamic draft version, and runs the normal
catalog/format/verify/promote graph with blocking checks. It also runs verification
and direct promotion probes without any approval registry and checks that the
existing published sentinel's inventory/hashes are unchanged. It does not create
an approval fixture. Shared #417 is an explicit dependency; no duplicate gate is
implemented here. These failures are expected blockers, not a successful
end-to-end publish or CI claim.

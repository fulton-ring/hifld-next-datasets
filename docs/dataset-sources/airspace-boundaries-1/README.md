# FAA Airspace Boundaries: native draft pipeline

Issue #10. This draft replaces the convenience download with complete anonymous
extraction, lossless raw/receipt retention and a typed, immutable local candidate.
Every source field, including separate `GLOBAL_ID` and `GlobalID`, is retained
with explicit Arrow physical types/nullability. Original GeoJSON `Feature.id` is
validated as nonnull, unique, exact int64 and retained as an explicit candidate
property; fractional, boolean, overflow and duplicate identifiers are rejected.
No substitute row identifier is generated. Remote storage and every explicit
`PublishedStorageResource` (including local mode) are refused before capture/write.
All source and receipt JSON rejects duplicate object members and nonfinite
constants before identifier extraction; ambiguous raw bytes remain quarantined.

Raw bodies and receipts are archived **before validation**, including malformed,
partial, HTTP/ArcGIS error and transport-failure receipts. A blocked reason stays
beside rejected evidence after temporary capture directories are removed. Previous
candidates remain immutable. Identical snapshot/derivation reruns are no-ops.

The FAA AIS snapshot has 477features: count, independent IDs, native all-column
response, separate GeoJSON and empty terminal page reconcile. The producer states
an effective window of 2026-09-03 09:01Z–2026-10-29 09:01Z. Fetch and portal edit
times are separate. The DataLumos crosswalk/archive still return provider 403
challenges under unrestricted networking; the original DHS hyperlink match remains
unverified. The FAA account/organization and exact existing item corroborate
source attribution without granting maintainer acceptance.

## Geometry and supported output

All 477 native NAD83/EPSG4269 geometries are valid when GDAL/Fiona interprets Esri
shell/hole semantics. Every original ring/coordinate sequence is reconciled
unchanged. The separately served 4326 GeoJSON export groups 22 polygons incorrectly
(`Hole lies outside shell`). That export is retained as diagnostic evidence; it
is not used to repair, reproject or replace native geometry.

The candidate includes self-described typed GeoParquet, native GeoJSON
serialization and a native GeoPackage built directly from explicit Arrow types.
Every GeoPackage property name/type/value, row count, CRS and geometry is checked
against the typed candidate before staging. GeoPackage `last_change` is pinned to
the capture timestamp for reproducibility; it is not a source release date. The
normal isolated catalog/format graph uses this typed GeoPackage as its input.

Two derivatives are explicitly unsupported, with reasons in the version manifest:

- Shapefile cannot retain `Shape__Area`, `Shape__Length` and the original Feature.id
  property within its 10-character DBF name limit. No destructive mapping is accepted.
- PMTiles conversion rejects unwrapped native longitudes outside ±180 and the
  polar/global representation. No antimeridian splitting, wrapping or polar
  clipping mapping is invented. Full native geometry remains in the typed/GPKG
  candidates.

The full published v1.0.0 candidate has 375 rows (pinned SHA256/generation in
`source.json`). GLOBAL_ID joins find 374 common, 103 added and one removed row;
336 common OBJECTIDs change. New/full-length shape fields, `GlobalID`, numeric
physical types, added identifiers and geometry/coverage differences require a
versioned mapping decision. Native versus old declared 4326 coordinates differ
on 360 of 374 common geometries; this comparison does not establish CRS equivalence.
No baseline, source, schema, migration or release is approved by these results.

## Reproduce and proposed refresh

Use the frozen environment and `PYTHONPATH=src`: its editable installation points
at the original checkout otherwise.

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p test_faa_airspace.py
PYTHONPATH=src .venv/bin/python docs/dataset-sources/airspace-boundaries-1/audit.py /workspace/work/hifld-coordinator/10
PATH=/workspace/.tools/tippecanoe/usr/bin:$PATH PYTHONPATH=src .venv/bin/python docs/dataset-sources/airspace-boundaries-1/run_local.py /workspace/work/hifld-coordinator/10 /workspace/work/hifld-coordinator/10/review-native
```

The large complete source/candidate bodies are preserved under
`/workspace/work/hifld-coordinator/10/`; Git retains bounded audit results, source
pages, SHA256 manifests and replay code. Full offline reproduction needs those
exact bodies. A live FAA refetch cannot reconstruct an earlier mutable snapshot.

The registered source refresh itself writes a content-addressed SEMVER draft
(`v0.0.0-draft.<sha256>`) with canonical GeoPackage, exact raw bodies/receipts and
source/provenance manifests. Its namespace includes every installed pipeline Python
module, the installed dependency inventory, Python/native-library versions and
project/lockfile hashes. The asset registers its dynamic partition; normal version
discovery also finds helper-only outputs. Old quarantine evidence remains immutable.
The actual source manifest passes the shared structural provenance validator before
canonical writes; this grants no source or release acceptance.

Requested and observed final URLs must independently match the exact producer
item, layer and bounded query scopes. Missing observations, mirrors, redirects to
other layers and filtered queries stop in durable raw quarantine. Original captured
receipts are retained byte for byte; no missing redirect evidence is inferred.

The replay creates a dedicated persistent Dagster instance and local stores,
executes the registered refresh, then runs catalog/formats/verification/promotion
with actual blocking checks, without copying source data or constructing replacement
manifests. It independently probes direct promotion without
an approval registry. Staged evidence and a preexisting local historical sentinel
must survive rejection. No real acceptance fixture is created. Earlier export
quality failure is retained in `legacy-export-materialization.json`; current
native evidence is in `local-materialization.json`.

`faa_airspace_draft_refresh_job` and `faa_airspace_draft_refresh_schedule` export
a proposed daily 06:00 America/New_York poll. The schedule defaults to **STOPPED**,
requires local staging, and does not activate or deploy anything. The adapter
also independently refuses remote storage. Its verified extraction scope is
bounded to 2000 rows/service maximum: larger counts stop for pagination review
and retain captured evidence rather than accepting a truncated response.

Shared #417 supplies the release gate and format exclusions. Missing independent
baseline/contract/release acceptance must stop promotion after supported candidate
formats, preserving earlier data. Full-suite/CI success is not inferred; the
missing sibling IaC remains an explicit validation limitation.

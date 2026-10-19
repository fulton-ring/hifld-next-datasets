# Draft NOAA 12NM Territorial Sea reconstruction (#2 / PR #418)

The user explicitly authorized draft implementation and isolated local testing
before baseline/contract acceptance. This supersedes the earlier documentation-only
worker restriction. It grants no source, baseline, contract, compatibility,
merge, release, schedule activation or production approval.

`src/dagster_hifld/assets/noaa.py::noaa_12nm_territorial_sea` now extracts the
exact NOAA Office of Coast Survey **MapServer layer 1** through the shared
`noaa_extract.py` implementation from #3 and release gates from #417. This PR is
stacked on `codex/cloud-dataset-3`; it does not duplicate those shared components.
The 260-record parent ZIP and its 110 TS-flagged subset are not substitutes for
this 87-feature named layer.

## Actual behavior

- Requires explicit `allow_unapproved_local_draft` and isolated local staging.
- Retains exact seven response bodies and checksummed receipts, including failed
  retry bodies; reconciles independently retrieved before/after counts and IDs,
  Esri attributes/vertices and GeoJSON. Bounded retries cannot turn an ArcGIS
  error or HTTP error into success.
- Preserves all source fields/native physical types, `OBJECTID`, string
  `BOUND_ID`, whitespace, nulls and every geometry vertex. Raw GeoJSON retains
  its separate feature `id`; no substitute ID column is generated.
- Builds deterministic native GeoParquet and typed GeoPackage; raw content plus
  processing policy defines an immutable namespace. Repeated identical source
  captures return identical output; receipts append without overwriting history.
- Completeness/identifier/schema/geometry/coverage drift retains raw evidence,
  writes quarantine and creates no candidate. Changes require review.
- Normal Dagster repository registers the ingest asset, a source-only refresh
  job and a **STOPPED** weekly Monday 06:00 America/New_York schedule. They never
  select promotion. Nothing was activated.
- Normal catalog and format graph runs locally; regenerated Shapefile is
  explicitly unsupported because DBF truncates/trims fields and loses physical
  fidelity. PMTiles is a visualization derivative, not a lossless replacement.
- Independent real acceptance remains mandatory at `publish/verify`. No approval
  registry is fabricated for the real dataset. Failed approval skips promotion
  and preserves both the candidate and previous published preservation fixture.

## Complete real evidence

`evidence/pipeline-*.json.gz` contains all seven fresh source responses and
original retrieval receipts. These are complete real captures, not synthetic
fixtures or reused before/after projections. Count is 87, with 80 LineString and
7 MultiLineString features, 11 regions and valid EPSG:4326 geometry.

`evidence/published-candidate.json` pins the complete v1.0.0 GeoParquet candidate:
SHA-256 `81235ea80c9febddf06871facd8afa5ca3670940da7a7fa05acc17c05f0e0152`,
1,411,062 bytes, GCS generation `1791506334866309`. Complete preserved bytes are
`/workspace/work/hifld-coordinator/noaa2/published.parquet`; the manifest records
a generation-pinned public URL. This is a candidate, not an accepted baseline.

`compare_candidate.py --baseline /path/to/preserved.parquet` verifies input pins
and compares **all 87 rows**, IDs, schema/types, values, coverage and geometry.
All source IDs/shared attribute values and every ordered coordinate agree.
Pending explicit compatibility decisions are:

- Source PUB_DATE nullable epoch-ms int64 versus published double (9 nulls).
- Source UNILATERAL float64 versus published int64.
- Source SHAPE 80 LineString/7 MultiLineString versus published geometry uniformly
  MultiLineString; coordinates agree, but physical types differ.
- Published `id` agrees with raw GeoJSON feature `id`/OBJECTID in all 87 rows;
  preserve it as an independently named column only through an approved mapping.
  Native candidate does not silently insert the published schema.

The latest producer landing and InPort were retried successfully (HTTP 200).
InPort still advertises 2013 publication/revision dates; fetch time or metadata
edit time is not a current release date. Original DHS crosswalk workbook matching
and archive V1 equivalence remain unverified. See `source.json` and retained
historical research for those limits.

## Reproduce local graph verification

Use the provisioned uv environment with **this checkout's** `PYTHONPATH=src` and
installed tippecanoe in PATH. Restore the seven committed `pipeline-*.json.gz`
bodies to an empty directory, removing only the `pipeline-` prefix and `.gz`
suffix (including `receipts.json`); their checksums are verified before replay.
Then run:

```sh
PYTHONPATH=src uv run --frozen --no-sync python scripts/verification/noaa_territorial_local.py \
  --runtime-root /path/to/new-empty-isolated-directory \
  --capture-directory /path/to/verified-captures \
  --previous-candidate /path/to/preserved.parquet
```

The runner loads actual normal repository assets, checks registered stopped
refresh selection, uses a persistent isolated DAGSTER_HOME, registers the real
publish partition and executes source/catalog/format/approval boundaries. It also
runs an identical refresh and a separately labeled mutation of one real source
BOUND_ID to verify quarantine. It clears remote bucket variables and creates no
real approval. No remote staging or published data is written.

`evidence/draft-integration-result.json` records code head, actual module path,
run IDs, statuses, partition, full output schema/coverage/provenance and checks.
`evidence/draft-terminal-audit.json` checks every real row/attribute type and
vertex across native candidate, GeoPackage and terminal GeoParquet. Terminal
GeoParquet's only extra column is a derived bbox. Existing published candidate
bytes stay intact in a separate local preservation fixture.

Focused source/shared extractor tests pass. Full repository attempt ran 493 tests
with 1 error and 1 skip: missing sibling
`hifld-next-iac/environments/prod/dagster.tf`. It is not a full-suite or CI pass.
See `evidence/draft-validation.json` for commands and scope.

## Decisions required before release

A maintainer must accept the pinned baseline, versioned raw/published contract,
explicit ID/type/geometry mapping, source lineage/freshness limits and supported
output formats. External exact-head code review does not grant those decisions.
Until then this remains a locally tested draft with hard release gates.

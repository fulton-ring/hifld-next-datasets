# Aviation Facilities — working draft, release blocked

The normal Dagster ingest reconstructs the FAA NASR-derived feed explicitly
attributed to FAA and distributed by official USDOT_BTS. It preserves every
original source field and ID, exact returned API points, complete raw inputs and
historical releases. Draft implementation/local tests do not approve the source,
baseline, contract, schema migration, merge or production release.

Complete source input has **19,427 points and91 original attributes across10
pages**. Every source EFF_DATE is2026/10/01; original resource metadata separately
states publication2026-10-02. Fetch time and portal edit dates remain distinct.
Count/ID/schema/edit witnesses reconcile before/after extraction; this is not a
transactional snapshot. DataLumos crosswalk/archive provider403 prevents inventing
an exact DHS crosswalk match. Attribution and original license evidence are in
[source.json](source.json) and the complete offline capture.

All source OBJECTID/SITE_NO/ARPT_ID values and physical types remain intact,
including leading zeros and both rows with SITE_NO23747.31. Only OBJECTID is the
unique extraction census, not an approved stable key across releases. Typed Arrow
Parquet and Arrow-stream GeoPackage retain every name, width, nullability, value
and returned point; generic terminal Parquet is fully compared by original
OBJECTID correspondence because it spatially reorders rows. Terminal bbox and
OBJECTID/geometry nullable-metadata changes are explicitly unapproved.

Returned pages and `layer.extent.spatialReference` declare102100/3857. The
observed differing `layer.spatialReference={wkid:4326,latestWkid:4326}` is preserved
under its exact key and pinned before/after as an unresolved metadata discrepancy
requiring release review. Its service/source/display meaning is not established.
This draft interprets returned API coordinates from their page/extent declarations
without client reprojection; original producer storage CRS is not asserted and
4326 is not treated as equivalent to3857. Contradictory, missing or unsupported
returned authority/WKT/vertical witnesses and changing extent/geometry/CRS/
dimensions/objectID identity fail closed.

Bounded HTTP retries retain complete, failed and partial bytes before validation.
Only the exact validated official canonical receipt roster supplies successful
retrieval provenance. Optional headers/HTTPDate types, original aware ISO times
(including UTC offset component bounds), license/attribution and the complete
shared release-manifest structure are preflighted **before any canonical output**.
Unknown top-level receipts are quarantined. Failed attempts remain explicitly
byte-inventoried diagnostics rather than successful input claims. No missing time
is replaced by HTTPDate, release date or a fresh retrieval timestamp.

Immutable candidate versions bind all raw/receipt hashes, installed pipeline
source and draft schema policy, effective Python/ABI/architecture, installed
package versions, GDAL/GEOS/PROJ and dependency-manifest/lock hashes. Changed
relevant dependencies prevent reuse/misattribution, while identical replay keeps
original receipts and earlier bytes. The registered daily10:00 New York local
source-only integrity poll remains STOPPED; source cadence remains28days. All
storage entrypoints reject PublishedStorageResource and remote storage before
fetch/write. Normal version discovery needs no manual data/manifest copies.

Shared #417 quality and independent exact-release gates block promotion. The
full historical candidate remains unapproved:19,952 rows versus19,427 current;
19,001 diagnostic ARPT_ID matches,426 additions,951 removals,901 changed matched
points and9source-int32 versus published-int64 columns. Source OBJECTID is absent
from the historical candidate and retained in the draft. Full differences and
coverage are in [evidence/baseline-comparison.json](evidence/baseline-comparison.json).
Shapefile field-name/type/null loss and rendered PMTiles ID/quantization/thinning
lack approved mappings, so those derivatives are excluded. Exclusion does not
approve omitting published formats. All source/baseline/ID/type/null/coverage/CRS/
format decisions require independent acceptance before release.

Reproduce offline from the bounded5.3MB complete capture/baseline evidence bundle:

```sh
uv sync --frozen
uv run python docs/dataset-sources/aviation-facilities/replay_capture.py /tmp/aviation-capture
PYTHONPATH=src uv run python docs/dataset-sources/aviation-facilities/run_local.py /tmp/aviation-capture /tmp/aviation-local
PYTHONPATH=src uv run python -m unittest tests.test_aviation_extract tests.test_refresh_registration_20
```

For the targeted preflight check, pass `--source-catalog-only` to `run_local.py`,
then run `run_negative_provenance.py` with the same capture/runtime arguments.
The scripts use isolated DAGSTER_HOME/storage and required dynamic partitions.
Negative metadata mutations are fixtures on complete real captures, not a source
or contract approval. They preserve every previous canonical/raw/accepted-fixture
byte and create no new canonical output.

Validation scopes remain explicit:

- Independent review at359054f verified every19,427-row/91-field value/type/WKB,
  normal no-copy graph/discovery, typed GPKG/terminal output and failed-quality/
  approval preservation. Subsequent changes are validation/provenance only.
- Current21 source/registration tests and complete real source/replay/catalog/
  provenance/discovery runs pass. Four actual Dagster negatives cover an extra
  receipt, null headers, absent license and absent attribution: no canonical
  output, raw quarantine and prior bytes preserved. See
  [evidence/provenance-preflight.json](evidence/provenance-preflight.json) and
  [evidence/negative-provenance.json](evidence/negative-provenance.json).
- Unchanged terminal conversions were not rerun solely for these guards. Final
  incremental native review is pending on the exact new head.
- Historical suite atbe93f225 ran505tests:503pass,1production-write skip and1
  missing-sibling-IaC error. The coordinator subsequently supplied unmodified
  sibling snapshot56aa10757129c457c530d707d9a40bae13e0907f; the current targeted
  IaC test passes. Historical counts are preserved; the full suite has not been
  rerun after that input resolution. No current full-suite or CI pass is claimed.

# Airport Area (USACE IENC): research and migration decision

Issue #9 remains **Blocked / Potential breaking change / Needs decision**. The current evidence establishes a changed dataset even though source and published candidate each contain 72 rows and have the same overall extent. No source, baseline, contract or ingestion approval is implied.

## Complete comparison after network recovery

The captured published v1.0.0 GeoParquet is an **unapproved baseline candidate**, SHA256 `3cffcba205af2d1439ddf6a15f7cbde821bbd666e853a7c93765ef608774be59`, GCS generation `1791506346972386`, 78,850 bytes. Its exact catalog and original object are preserved in `evidence/network-recovery/`; the catalog's asset length and checksum metadata are retained alongside the measured SHA256. Catalog source dates are marked `inventory`; they are not verified producer release dates.

On 2026-10-19 UTC, unrestricted/enforced runtime revision 6 enabled fresh anonymous reads from the historical IENC service: `https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/AIRPORT_AREA/MapServer/57`. The service really exposes layer 57, not a guessed layer 0. Count, independent IDs, full Esri JSON and full GeoJSON reconcile 72 unique nonnull OBJECTIDs with no transfer-limit flag. `Feature_ID` is unique/non-null 72 in both historical and master services. The raw Esri and GeoJSON responses have identical attributes and exact coordinate multisets per OBJECTID. The complete captures are retained with request times/statuses/checksums.

The offline comparison inspects every row, column, ID, attribute and polygon:

- 71 published polygons each have one exact GEOS topological match in the historical source, without relying on row IDs or rounding coordinates.
- Published-only **Gateway Air Tours Helipad** corresponds to no source polygon; source-only **Demopolis Municipal Airport** has per-feature `Source_Date=20260415`. That date is not a dataset release date.
- 43 of the 71 exact geometry pairs have different OBJECTIDs. Published `id` equals published OBJECTID in all 72 rows. It cannot reconstruct source `Feature_ID`, which is absent from the published candidate. Do not use object row numbers as continuity identifiers.
- For the 71 geometry pairs, the proposed attribute mapping finds 43 `Informatio` null versus `Information` empty-string differences, and 10 `Source_Ind`/`Source_Indication` value differences. Other mapped attribute values agree. This is an observation mapping, never an approved publishing contract. Physical Arrow and Esri schemas are retained separately; numeric equality does not establish type compatibility.
- All 216 geometries across the three snapshots are valid and nonempty. Overall bounds agree, but membership changes. Geometry/coverage checks cannot be reduced to counts and bounding boxes.
- Historical/master services share all 72 Feature_ID values; shared attributes differ only in two OBJECTID values (38/40 swap). Their geometries have maximum Hausdorff distance `7.944109290391274e-14` degrees, with zero exact topological matches. The audit reports this numerical difference without accepting a tolerance or changing any coordinates.

The full comparison includes the 71 observed geometry pairs, every mapped difference, unmatched row attributes/bounds, schemas/null counts and coverage. All original identifiers and coordinate precision remain preserved.

## Provenance and release evidence still required

The inventory identifies `airport-area-usace-ienc`, US Army Corps of Engineers, publisher unspecified, DataLumos project 238564 V1. The original DHS crosswalk XLSX remains **UNVERIFIED**, not unmatched: the DOI now resolves to DataLumos but returns provider HTTP 403 with `cf-mitigated:challenge`. The exact archive project returns the same challenge. The AGC Echarts official landing request returns provider HTTP 403. These are recorded HTTP responses, distinct from the earlier tunnel CONNECT 403 failures. No bypass was attempted.

Historical `ienccloud.us` service metadata self-attributes `USACE IENC` and describes an S-57 database extracted to an Esri file geodatabase containing 99 feature classes. The master ArcGIS item `6ee858a220a143a48c8d40a6cf0d3680` is owned by `usace_agc_dkd` (public profile Dale Dodson) and attributes USACE, Army Geospatial Center, US Coast Guard and IIC Technologies. Neither self-attribution establishes independent agency delegation. `source_role` remains `unresolved` and release_date remains null. `(16-Sep-26)` in the master item's snippet remains an unverified edition claim, separate from portal edit times and feature Source_Date. This is charted inland-navigation airport-area coverage, not all US airports; AIRPORT_AREA_POINT is a separate layer.

`decision.json` packages the exact baseline, source and migration decisions. The source contract in `source.json` remains **PROPOSED_UNAPPROVED**. A maintainer must approve the exact baseline and versioned ID/schema/membership migration before source-specific ingestion; provenance/release evidence is also outstanding. Missing approvals do not prevent the completed research or shared #417 implementation.

## Reproduce and local readiness

From repository root, using the frozen worktree environment:

```bash
uv run --frozen python docs/dataset-sources/airport-area-usace-ienc/replay.py
uv run --frozen python docs/dataset-sources/airport-area-usace-ienc/audit-recovery.py
```

The first command replays the original bounded research capture and its hashes. The second verifies recovery request hashes, catalog/object hashes, complete IDs and coordinates, then prints the complete comparison. Its deterministic output must equal `evidence/network-recovery/comparison.json`. It performs no live reads or acceptance. To repeat live research into a **new scratch directory**, run `fetch-recovery.py <new-output-directory>`; remote endpoints may change and will not reproduce the old pinned bytes.

Fresh `uv sync --frozen` succeeded in this own worktree. A newly started local Dagster webserver/repository used private `DAGSTER_HOME`, staging and published directories under `/workspace/work/airport-9/runtime`, with both bucket variables unset. `readiness.json` records actual HTTP server and GraphQL repository/asset loading; the own process group was stopped afterward. No materialization occurred. No full-suite or CI success is claimed; the missing sibling `hifld-next-iac/environments/prod/dagster.tf` limitation is unresolved. Research changes do not establish materialization readiness or failed-promotion preservation for an unimplemented dataset asset.

The original raw `evidence/ids.json` retains CRLF bytes that trigger a historical whitespace warning; these bytes remain unchanged for checksum preservation. The new raw `network-recovery/agc.body` also contains a trailing-space line from the provider response; its original bytes are retained for the recorded hash. Authored files pass whitespace checking.

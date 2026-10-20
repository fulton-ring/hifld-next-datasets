# Plate Boundaries source evidence

**Candidate status: draft; not approved for release.** This record distinguishes the current USGS service capture from the HIFLD historical archive. The live service is a usable agency-hosted source, but the archive-to-service byte/feature relationship and the producer's current scientific edition remain unverified. No current-edition claim is made.

## Identity and lineage

The HIFLD inventory row is `plate-boundaries`, titled “Plate Boundaries,” agency USGS, description “USGS Tectonic Plate Boundaries.” The cited historical crosswalk is DHS DOI [10.3886/E241367V1](https://doi.org/10.3886/E241367V1); it points to the DataLumos archive [ICPSR 240544](https://www.datalumos.org/datalumos/project/240544/version/V1/view). That archive identifies its geographic coverage as world, its period as 2024-08-27, and states it was archived from HIFLD Open, deactivated 2025-08-26. The DataLumos page lists shapefile and GeoJSON derivatives; these are historical copies, not current feeds.

The official producer endpoint found is USGS Earthquake Hazards' [eq/map_plateboundaries MapServer](https://earthquake.usgs.gov/arcgis/rest/services/eq/map_plateboundaries/MapServer), layer 1, “Plates.” Service metadata describes “USGS Tectonic Plate Boundaries.” Its cited source lineage is USGS Seismicity of the Earth Map Series, Peter Bird's 2003 digital plate-boundary model, and DeMets, Gordon and Argus' 2010 plate-motion model. USGS hosts the endpoint, but the service does not expose a release date or a statement that this is a refreshed edition of the 2024 HIFLD snapshot. Treat the service as a direct USGS distribution of its displayed legacy/model lineage, not a demonstrated successor to the archived HIFLD bytes.

The Esri item `85210b078256486d9d9213dac8fb93f4` is a separate third-party training service. Its description says it was derived from an Esri layer package and reduced slightly for performance. It is not used as a source or fallback.

## Current extraction evidence

Anonymous HTTP extraction was performed 2026-10-20 against the USGS service. The layer declares 1,175 features and a complete OBJECTID roster. Before/after count was 1,175 and the ordered native ID roster was unchanged (1,175 unique IDs, min 1, max 1181; IDs are sparse). The service max record count is 1,000 and pagination is supported. A full 1,000-row geometry query returned only 1,000 rows, so a single-request strategy would silently truncate. A 500-ID request returned HTTP 413. Twelve batches of at most 100 explicit IDs each returned all 1,175 features; each batch contained the requested count, with no error/transfer-limit flags. Total observed query time was about 6.6 seconds. Combined response: 459,008 bytes, SHA-256 `055985e165f52052bd6e4b72808a2fc3e46c4d48acbb36532f144a67bfdf696f`.

The downloaded candidate is retained outside Git in the isolated worktree's local staging area. The merged GeoJSON is a deterministic derived candidate; all 18 raw HTTP bodies (layer metadata before and after, count/ID rosters, and 12 feature pages) are retained byte-for-byte with request receipts, sizes, and SHA-256 digests.

Native layer declarations: `OBJECTID` OID, `Shape` geometry, `NAME` string(200), `LABEL` string(50), `Shape_Length` double. Geometry is a polyline, service CRS EPSG:3857, no Z/M; extraction requested GeoJSON in EPSG:4326. The service's spatial extent is global. The current response had all declared fields, non-null unique native OBJECTIDs and complete geometry for every returned feature. `NAME` is null for 3 rows; `LABEL` and `Shape_Length` are non-null. The result is complete for the service's current layer; it does not certify model accuracy, freshness, or equivalence to the HIFLD 2024 version.

## Contract and release gate

Proposed faithful candidate mapping retains all four source attributes and the complete GeoJSON geometry without generating IDs or dropping fields. `OBJECTID` remains numeric as supplied; `NAME`, `LABEL`, and `Shape_Length` retain source values/types. Geometry and source CRS are retained in extraction evidence, with the GeoJSON response CRS explicitly recorded as EPSG:4326. The accepted published schema/ID/CRS/coverage contract, source-vintage choice, attribution/rights, freshness cadence, baseline, and release approval remain maintainer decisions. Any changed fields, ID roster, transfer count, geometry type/extent, CRS, or feature coverage must quarantine the candidate and preserve the previously accepted release.

The implementation is a stopped draft refresh. Completeness checks compare before/after counts and ID rosters and verify each response batch; drift checks bind exact field declarations, null counts, geometry type/CRS and coverage. Failed extraction or validation cannot replace existing accepted data. Complete candidates remain in `_candidates/`, outside the version-discovery path, until the maintainer approves the contract. No production publication or source acceptance is implied.

## Local validation evidence

On 2026-10-20 the Dagster source asset materialized successfully as run `081713bf-2954-4c81-8dd0-0174fceca794` to isolated local candidate prefix `_candidates/plate-boundaries/v20261020T031630Z/`. It contains 1,175 features, 18 losslessly retained response bodies, and deterministic candidate GeoJSON (459,009 bytes; SHA-256 `678863382d2e28b437016337d08136fa622a87e8f83683323e9f5bdf6c3cc9cf`). Receipt digest: `03827b10f6ad496c707c5f8752ba7cb30c1dcb49d007f134b536de3ae76d5869`. Seven focused quality tests passed. The full Dagster definitions load. The scheduled refresh is defined but stopped. The approval-gate test fails closed for the unapproved candidate, and the candidate storage path is excluded from accepted catalog discovery.

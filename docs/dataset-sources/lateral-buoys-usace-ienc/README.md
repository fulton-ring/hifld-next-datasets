# Lateral Buoys (USACE IENC)

This draft implements inventory issue #141 against the official USACE IENC `LATERAL_BUOY_POINT` MapServer layer 15. It is a complete extraction of the observed feature class, not an assertion that the layer matches a historical HIFLD release or represents all lateral buoys beyond the source chart coverage.

## Source and observed population

- The [BTS NTAD IENC item](https://www.arcgis.com/home/item.html?id=7ec34864c8804468a9cf52d437fe4b6c) (owner `USDOT_BTS`) describes the Inland Electronic Navigational Charts program, links the [official IENC Feature Classes service](https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes), and links the native master geodatabase; it is the pinned delegation/distribution evidence and is fetched live. An earlier BTS IENC map-service item that returned an anonymous-access denial is not used or fetched by this draft, and no succession or historical equivalence between items is claimed.
- The live [service directory](https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson) (probed 2026-11-09) publishes exactly one lateral buoy service, `IENC_Feature_Classes/LATERAL_BUOY_POINT`; that service exposes exactly one layer, id 15 `LATERAL_BUOY_POINT` (`esriGeometryPoint`, `maxRecordCount` 2000, EPSG:4326, `copyrightText = USACE IENC`). The port/starboard distinction is an attribute of the records (`Category_of_Lateral_Mark`), not a layer split, so the single published lateral buoy layer is the complete assigned population.
- The pinned observed snapshot (bounded count/ID probes plus one single-page complete extraction, 2026-11-09) has 10 features with a complete unique `OBJECTID` roster 1–10. Count and ID responses are checked both before and after page extraction. The 14-field layer schema, full native attributes, record content, geometry content, and coordinate extent are pinned for drift rejection.
- All 10 `Feature_ID` values are distinct non-empty strings; they are preserved unchanged and `OBJECTID` is the pinned unique source key. No surrogate or generated identifier is introduced.
- Native EPSG:4326 point XY coordinates are emitted as WKB without reprojection, repair, or simplification. Per-feature ordinates are rejected outside legal EPSG:4326 ranges and outside the pinned declared layer extent; the observed coordinate envelope must stay inside that extent. Page queries request Z/M explicitly and any returned Z/M ordinate is rejected.
- The observed extent describes only the features returned by the IENC layer. It is not independently accepted as HIFLD coverage. Per-feature `Source_Date` values are preserved as attributes and are not interpreted as a dataset release date.

## Candidate and safety gates

The registered Dagster ingest asset creates a local-only GeoParquet candidate with all 13 declared non-geometry source fields and native geometry. Raw responses, wire captures, checksummed receipts, source audit, source manifest, processing identity, and candidate-ready metadata are retained in local staging; incomplete or drifting captures are quarantined with their retained evidence instead of sealing. A complete-population blocking check confirms the observed 10-row extraction while keeping source, freshness, schema, ID, coverage, rights, mapping, and release acceptance false. The twice-monthly refresh schedule is stopped by default. The candidate is not promoted or published.

Only GeoParquet is generated. GeoPackage and Shapefile output are withheld pending approved physical schema and identifier mappings; PMTiles is withheld pending projection and display decisions.

The exact historical HIFLD baseline/crosswalk, source release date and currentness, physical schema and ID contract, coverage equivalence, rights, and release acceptance remain pending maintainer decisions. The DataLumos archive copy of the historical HIFLD Open item is retained as archive context only and was not fetched for comparison.

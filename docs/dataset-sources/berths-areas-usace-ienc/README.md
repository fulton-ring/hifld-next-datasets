# Berths Areas (USACE IENC)

This draft implements inventory issue #21 against the official USACE IENC `BERTHS_AREA` MapServer layer 98. It is a complete extraction of the observed feature class, not an assertion that the layer matches a historical HIFLD release or represents all berth areas beyond the source chart coverage.

## Source and observed population

- The [BTS IENC item](https://www.arcgis.com/home/item.html?id=3607d485a9854d6fa458bf507058ceae) describes the Inland Electronic Navigational Charts program, links the [official IENC Feature Classes service](https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes), and links the native master geodatabase. The live [service directory](https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson) publishes `IENC_Feature_Classes/BERTHS_AREA`; the layer reports `copyrightText = USACE IENC`.
- `BERTHS_AREA` is polygon geometry in EPSG:4326. The pinned observed snapshot has 1,108 features and a complete unique `OBJECTID` roster 1–1108. Count and ID responses are checked both before and after page extraction. Exact layer schema, full native attributes, IDs, geometry content, and coordinate extent are pinned for drift rejection.
- There are 1,098 distinct `Feature_ID` values among 1,108 rows. Those source values are preserved unchanged; `OBJECTID` is the pinned unique source key.
- Polygon rings are emitted as WKB in the source ring order and winding, with every finite XY vertex retained. No topology repair, coordinate reprojection, ring orientation normalization, simplification, or format conversions are performed.
- The observed extent describes only the features returned by the IENC layer. It is not independently accepted as HIFLD coverage. Per-feature `Source_Date` values are preserved as attributes and are not interpreted as a dataset release date.
- The exact layer release date is unknown. BTS describes bi-monthly updates to IENC, but currentness of this exact snapshot and the layer’s release marker have not been established. Rights, source freshness, the historical HIFLD baseline/crosswalk, physical schema and ID contract, coverage, and release approval remain pending.

## Candidate and safety gates

The registered Dagster ingest asset creates a local-only GeoParquet candidate with all declared non-geometry source fields and native geometry. Raw responses, wire captures, receipts, source audit, source manifest, processing identity, and candidate-ready metadata are retained in local staging. A complete-population blocking check confirms the observed 1,108-row extraction, while explicitly keeping source, freshness, schema, ID, coverage, rights, mapping, and release acceptance false. The twice-monthly refresh schedule is stopped by default. The candidate is not promoted or published.

Only GeoParquet is generated. GeoPackage and Shapefile output are withheld pending approved physical schema and identifier mappings; PMTiles is withheld pending projection and display decisions. No additional output format is inferred from the source data.

The exploratory source capture is retained outside Git at `/workspace/work/hifld-coordinator/21/research/`. The source asset’s final sealed live capture and Dagster run evidence are recorded outside Git after local materialization.

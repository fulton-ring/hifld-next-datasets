# Building Locations (USACE IENC)

This draft extracts the complete `BUILDING_SINGLE_POINT` layer (layer 5) from the USACE-hosted IENC Master MapServer at `https://ienccloud.us/arcgis/rest/services/IENC/USACE_IENC_Master_Service/MapServer`. It preserves the native EPSG:4326 point coordinates and all 12 declared attributes. The dated capture returned 892 features, IDs 1–892, over the inland chart service extent (approximately longitude −96.42 to −79.52, latitude 29.70 to 45.11). That extent describes chart coverage, not a national building inventory.

## Source and lineage

The service directory identifies the IENC Master MapServer as an official USACE service. The related BTS ArcGIS item (`3607d485a9854d6fa458bf507058ceae`, owner `USDOT_BTS`, “Inland Electronic Navigational Charts”) describes bi-monthly updates and links the IENC feature-class service and master geodatabase. The available item metadata does not establish that its linked endpoint is this exact MapServer route, so the producer/distributor relationship remains unverified for this service. No edition or release date was present in the source metadata. The candidate is not represented as a dated or current national release.

The historical HIFLD/DataLumos Building Locations release (DataLumos study 238828, dated 2025-04-08) is a separate lineage and is not used as an implicit crosswalk, baseline, or approval for this source. Any field crosswalk, rights determination, baseline comparison, coverage acceptance, source freshness decision, and release approval remain outstanding.

## Reproduction and evidence

The Dagster source asset accepts only isolated local staging storage. It captures request bodies, wire bytes, and receipts; verifies layer identity, CRS, schema, count, IDs and complete paging; writes typed GeoParquet plus source and quality manifests under the normal candidate layout; and stops before publication. The source schedule is stopped pending source and release decisions.

`local-materialization.json` records the actual local source extraction, cache replay, catalog quality run, and expected exact-object approval refusal. The capture and candidate are reproducible from the retained local receipts referenced there; no production publication or approval was performed.

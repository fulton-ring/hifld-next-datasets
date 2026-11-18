# EPA Regions Large Scale draft

Source: https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/US_EPA_Regions__DOC___EPA_2023_/FeatureServer/13

This dataset now reads the same single-resolution layer as EPA Regions
(`epa-regions`) and EPA Regions Small Scale. The retired
`EPA_Locations/MapServer/3` layer is not read. The layer is published by the
EPA GeoPlatform account `EPA_GEO` (item `21dfeddb55984d9c957ffad98e7da94e`),
describes 2023 DOC Census-derived boundaries enhanced by EPA, and says the
feature service and attributes are updated annually by the EPA Office of
Mission Support.

Large and Small Scale therefore produce the same output. They keep separate
dataset slugs and candidate versions so existing consumers and release records
stay addressable.

The adapter preserves all 12 original fields (`OBJECTID`, `EPA_REGION`, the six
area fields, `Shape__Area`, `Shape__Length`, `RegionString`, `RegionTitle`),
original OBJECTID values, text widths, Double values, nulls and native
EPSG:4326 ordered rings. OID physical width is unspecified; Int64 is a proposed
staging encoding. Native GeoParquet only; other formats need approved
type/geometry/ID contracts.

Run registered `epa_large_scale_source_integrity` with isolated local staging;
the monthly schedule is STOPPED. Pass a capture directory of complete
`.body`/`.receipt.json` files to replay a retained capture. Source, edition,
rights, baseline, contract and release acceptance remain unapproved.

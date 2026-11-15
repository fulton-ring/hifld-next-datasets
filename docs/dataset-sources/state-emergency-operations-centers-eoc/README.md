# State Emergency Operations Centers (EOC)

The stopped FEMA draft reads the exact [FEMA State EOC FeatureServer layer](https://gis.fema.gov/arcgis/rest/services/FEMA/STATE_EOC/FeatureServer/0), snapshots metadata, count and ID roster before and after a complete paged extract, and keeps the wire and decoded captures beside an explicitly typed native GeoParquet candidate. IDs and source attributes are retained; `eoc_id` remains `int16`, ZIP remains a string, and source coordinates remain WGS84 points.

The FEMA layer exposes no source edit timestamp or producer release date. The DHS crosswalk row, historical published baseline and maintainer schema/ID contract remain unresolved. The adapter is a local draft; source freshness, compatibility and publication remain unapproved. All capture/runtime state stays outside Git under `/tmp/hifld-native-reconstruction/326`.

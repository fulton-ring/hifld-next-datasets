# EPA Regions Small Scale working draft

Exact producer: [EPA_Locations/MapServer/2](https://geodata.epa.gov/arcgis/rest/services/OEI/EPA_Locations/MapServer/2), literal small-scale title/maxScale5,000,001. Ten regions; producer2011 TIGER/Line lineage/service2012 title establish no modern release. Ordinary regions, large-scale and EEZ layers are distinct.

All original four attributes plus `Shape`, IDs/strings/nullability/field declarations and106ordered rings/404,974 native NAD83/EPSG:4269 XY coordinates remain. Service display CRS3857 is separate. OID physical width is unspecified; Int64 is proposed staging encoding. GDAL polygon grouping is checked against every ordered ring, without projection/repair. Historical candidates use3857 and truncated field names; source/vintage/rights/ID/type/schema/CRS/geometry/baseline/contract decisions remain external. Release unapproved; monthly refresh STOPPED.

Managed Python: `PYTHONPATH=src python -m unittest discover -s tests -p test_epa_small_scale.py`. Full real-source opt-in: `EPA_SMALL_SCALE_TEST_CAPTURE_DIR=/path/to/captures`. Materialize `epa_small_scale_source_integrity` with isolated `DAGSTER_HOME`, staging/published directories and optional `HIFLD_EPA_SMALL_SCALE_CAPTURE_DIR`; register the returned publish partition. Shared Git/CAS provenance and quality/independent-approval gates apply.

Protected full inputs/replay inventory: `/tmp/hifld-native-reconstruction/79/epa-small-scale-inputs.tar.gz`, SHA256 `73b6a16a8a78074ff0aec0228d30cf83c0b49426c750e4a4f9b3634282a4230a`. This protected directory is acceptance-review storage. Durable pointers/results: issue79 and `/workspace/work/hifld-coordinator/79`; no full captures/logs/reports in Git.


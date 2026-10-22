# EPA Regions Large Scale draft

Source: https://geodata.epa.gov/arcgis/rest/services/OEI/EPA_Locations/MapServer/3
Exact large-scale layer3, minScale5000000/maxScale0; no small-scale/ordinary-region/EEZ substitution.
Run registered `epa_large_scale_source_integrity` with isolated local staging; monthly schedule is STOPPED.
`HIFLD_EPA_LARGE_SCALE_CAPTURE_DIR` selects complete original `.body`/`.receipt.json` inputs.
All four attributes, original OBJECTIDs/EPAREGION, full physical declarations and Shape rings survive.
Producer source CRS4269 is explicitly requested; display3857 remains separate, with no local projection.
A derived header-first EsriJSON buffer enables GDAL detection; all original binary64 ordered rings are checked.
Original raw body/wire, receipts and first-construction witnesses persist; new source content gets a new draft.
OID physical width is unspecified; int64 staging and GDAL Polygon/MultiPolygon grouping remain proposals.
2011TIGER/Line lineage and the2012 service title do not establish a current dated release.
Native GeoParquet only; other formats need approved type/geometry/ID contracts.
Source/baseline/coverage/rights/CRS/geometry/contract/release acceptance remains blocked.
Complete captures and execution proofs are protected checksummed external artifacts linked on issue #78.

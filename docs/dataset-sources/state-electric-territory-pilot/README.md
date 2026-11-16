# Original-state electric territory pilot (#960)

Inspected 2026-11-16. These are separately named state publications, not an
ORNL historical reconstruction or a national replacement. Electric retail areas
remain distinct from gas distribution, control/planning and holding-company
areas. The existing `electric-retail-service-territories` release paths are
unchanged. Its excluded NBAM frozen-copy acquisition entrypoint now refuses
before storage or HTTP side effects.

| Publication | Publisher / authority chain | Native population and CRS | Currency / limitations |
| --- | --- | --- | --- |
| `iowa-electric-service-boundaries/electrical-service-boundaries` | [Iowa Utilities Commission](https://iuc.iowa.gov/), official [application](https://iowa.maps.arcgis.com/apps/webappviewer/index.html?id=d595a7d431bc4c789065348a8f454dbb), webmap `a46c1e1554e743dca929264ee753b2d9`, [producer service layer 14](https://services.arcgis.com/vPD5PVLI6sfkZ5E4/arcgis/rest/services/Electrical_Service_Boundaries/FeatureServer/14); organization `vPD5PVLI6sfkZ5E4`, owner `pwilkebrown_iowa` | 1,361 source OBJECTIDs; 17 original attribute columns; ESRI:102004, source advertises Z | `editingInfo.dataLastEditDate=1777393208460` (2026-04-28); an edit timestamp, not a release date. No declared fixed refresh cadence. One native missing geometry and 29 invalid geometries retained. |
| `wisconsin-psc-electric-service-territories/{cooperative,investor-owned,municipal}` | [PSC Wisconsin Maps page](https://psc.wi.gov/Pages/ForConsumers/Maps.aspx) links its own [original ZIP](https://psc.wi.gov/SiteAssets/Maps/EL_PSCW_ServTerr.zip) | 24 cooperative, 12 investor-owned, 81 municipal records; three separate native schemas; EPSG:3071; Municipal Polygon ZM | HTTP Last-Modified 2026-08-21; DBF dates 2026-05-27 (cooperative/investor-owned), 2026-08-21 (municipal). These are container/update dates, not a common feature edition. Original per-row `DATA_DATE` retained. Two invalid polygons in each layer retained. PSC staff update maps; no fixed cadence established. |

The ZIP's embedded metadata says cooperative boundaries are approximate and may
be rough outlines around facilities; municipal boundaries may be rough polygons
around municipalities where utility submissions are absent. Investor-owned
boundaries are established for utilities that submitted data. All three disclaim
accuracy guarantees. This pilot consumes those original polygons as supplied; it
does not construct or infer replacements. No overlap, nonexclusivity, missing
utility or jurisdiction coverage claim has been resolved by the pilot.

`ESB_ID`, `REC_Number`, `PSC_ID`, `CODE`, utility names, phone/address fields and
native null/space values remain source attributes. None is asserted to be EIA
UtilityID. No verified crosswalk is available, so no name-based EIA join is made.
EIA-861 county equipment coverage is nonspatial context, not boundary geometry;
headquarters geocoding cannot recover these territories. No rights inference is
made from government hosting. The licensed DOE LEAD / Hitachi VelocitySuite
product and corporate ArcGIS copies are not inputs.

The runtime fetches only the Iowa layer metadata, count/ID queries and complete
100-ID feature batches (explicit `returnZ=true` and `returnTrueCurves=true`, no `outSR`), followed by a second
metadata/count/ID roster; Wisconsin fetches only the PSC ZIP. Capture files retain
bounded decoded and wire bytes with hash-pinned receipts. Offline replay requires
all original files and never falls through to HTTP. Raw input, original retrieval receipts, output and exact
processing identity determine immutable local draft versions. Replaying the same
retained capture reuses its version; a new live capture has its own dated receipt.
Normalized multi-input provenance validates before candidate metadata is staged. Native scalars,
field order and CRS are retained in prebuilt GeoParquet; no 2D conversion,
projection, repair or null-row removal is applied. Native true-curve responses
are refused before WKB construction rather than silently linearized. The complete
2026-11-16 Iowa capture with that parameter returned no `curveRings`. Catalog reads the native
prebuilt output. Geometry checks intentionally fail for the observed originals.

Source, freshness, schema, ID, CRS, coverage, rights, baseline and release gates
remain false; schedules are STOPPED. Independent review does not accept those
contracts or authorize publication. No complete historical ORNL baseline, national
coverage, undocumented enrichment or historical boundary parity has been tested.

## Municipal optional measures (author repair)

The original Municipal SHP has 81 PolygonZ records, all with optional M arrays:
13,343 M no-data sentinel values `-1.7976931348623157e308` and 77 actual zero
measures across 13,420 vertices. Default GDAL layer inference labels this PolygonZ
and drops measures. The adapter explicitly reads `ADJUST_GEOM_TYPE=ALL_SHAPES`
and inspects actual WKB dimensions. A bounded independent original-SHP parser
checks every record/ring/vertex XY/Z/M against WKB, allowing only cyclic ring start
and winding changes. Original no-data M values below `-1e38` map to WKB NaN;
actual zero remains zero, and array presence/no-data/actual-value counts and the
original SHP hash are saved in runtime provenance. The complete untouched ZIP
retains the exact sentinel bytes. All three Wisconsin layers undergo the raw
ordinate comparison; a dimension/value/ring/population mismatch refuses staging.

Measured Municipal output uses GeoParquet 2.0 with a native Parquet `GEOMETRY`
logical annotation and matching native EPSG:3071 CRS, derived from the actual
WKB ZM type, not GDAL's misleading PolygonZ label. The shared native catalog
requires that physical annotation and rejects M/ZM declared or served under
GeoParquet 1.x. Iowa and the two unmeasured Wisconsin layers remain GeoParquet
1.1. Existing null/invalid geometry and quality failures are preserved.

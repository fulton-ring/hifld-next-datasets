# Mexico and US Border

Inventory `mexico-and-us-border-1`, agency International Boundary and Water Commission (IBWC). The DHS HIFLD crosswalk (DOI 10.3886/E241367V1, parsed 2026-11-12 from the public crosswalk file `hifld_open_crosswalk_geoplatform.xlsx`, sha256 7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc) marks this layer "Deprecate" and directs users to IBWC's own portal item and REST service. This draft captures exactly those agency endpoints; no mirror, archive or third-party host is fetched.

Fetched inputs and who publishes them:

- `https://gisportal.ibwc.gov/agsportal/sharing/rest/content/items/f4d6ea8085d142f9b71a2860fe40bb60?f=json` — IBWC's own ArcGIS portal (owner `ibwcgis`, org `0123456789ABCDEF`); item declares NGDA ID 82 and the inventory's exact tags, and public-domain access/use constraints.
- `https://gisserver.ibwc.gov/agsserver/rest/services/USIBWCPublic/US_MexicoIB/FeatureServer` (root `?f=pjson`, layer 0 `?f=pjson`, and `/query` count/ids/page requests) — IBWC's own ArcGIS Server. Layer 0 `US_MexicoIB` is an `esriGeometryPolyline` feature layer; the observed snapshot (2026-11-12) holds 14 segments (`OBJECTID` 1..14, unique `GlobalID` roster), land/river/maritime `BORDER_TYPE` codes 1/2/3, native layer CRS Web Mercator (102100/3857), page coordinates requested and validated at EPSG:4326 via the service's own `outSR=4326` response.

The adapter pins the before/after OBJECTID and GlobalID rosters, declared field names/types/aliases/lengths/domains/nullability, pinned layer extent and declared WGS84 coverage box, coded-domain values, and page truncation/error state, and fails closed on any drift, unexpected column, type loss, null in a non-nullable identifier, or Z/M ordinate. The candidate is an unapproved faithful draft: crosswalk succession, source release date (portal item created 2024-01-31 is not a release date), historical baseline equivalence with the archived HIFLD copy, schema/ID/coverage contracts, rights, and release all remain pending acceptance, and the refresh schedule is stopped.

The archived inventory (DataLumos project 239595) is research context only; its modification date (2025-04-08) is not treated as a source release date. The old GeoPlatform/HIFLD-hosted item (`1ceab4c3babb4f058ec6c773574a6ae5`) is the crosswalk's old hosting location, not a pipeline input.

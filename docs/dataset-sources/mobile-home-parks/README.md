# Mobile Home Parks source record

Inventory entry `mobile-home-parks` (HIFLD Open Inventory 2025-12-11 row): mobile home, residential
trailer and recreational vehicle parks, agency Department of Energy, publisher Oak Ridge National
Laboratory, DataLumos archive project 239601 with inventory modification date 2024-09-20 (a
collection edit date, not a source release date). The official HIFLD description ends with "In the
latest update 2281 records were added"; the layer covers the Continental United States and Alaska.

## DHS crosswalk match (dated)

The DHS HIFLD Open crosswalk row for "Mobile Home Parks" was read on 2026-11-12 from the saved
crosswalk copy (https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx,
sha256 `7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc`, 61,689 B — the public copy
of the DHS-published crosswalk index; the official GeoPlatform crosswalk item
`41b4ff18bb094ba996dec75916de3bf3` itself is deleted/CONT_0001 per same-day sibling-layer checks in
issues #5/#122). The row states: Status `Migrated`, New Hosting Location `GII Portal`, DUA Required
`No`, GII Access Required `Yes`, Agency `Oak Ridge National Laboratory (ORNL)`, External Landing
Page empty, Open REST Service
`https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Mobile_Home_Parks/FeatureServer`,
Open ID `4cdbccc5c538452aa91ceee277c460f9`, Old/New Secure IDs "Please see GII".
The inventory row therefore matches the crosswalk exactly (layer name and DOE/ORNL lineage), and DHS
records the layer as migrated behind DHS GII access control, not to any public feed.

## Producer identified

Oak Ridge National Laboratory (DOE). ORNL's own "Geospatial Data Sets" program page
(https://www.ornl.gov/projects/geospatial-data-sets, fetched 2026-11-12, HTTP 200, 698,222 B) lists
"Mobile home parks" among the HIFLD layers developed by ORNL ("Led by the Department of Homeland
Security, HIFLD hosts more than 400 open data layers... Among the layers developed by ORNL are:
... Mobile home parks ... Hospitals ... Nursing homes/assisted living facilities"), with no download
endpoint of its own and a link that still points to the dead HIFLD Open hub
(`hifld-geoplatform.opendata.arcgis.com`). The data.gov harvest record for the former HIFLD Open
release (`MGMT-GMO-HIFLD-648667`, publisher "HIFLD", Department of Homeland Security, contact
edmo@hq.dhs.gov, metadata modified 2022-10-24) is metadata-only and carries no payload. No ORNL,
DOE or successor feed publishes a maintained national mobile-home-parks GIS layer today.

## Historical vintage facts (research context, no accepted baseline)

The published HIFLD Next v1.0.0 rescue snapshot of the same layer (hifld.publicenvirodata.org
catalog page, read 2026-11-12 as a comparison copy only, never an input) reports 56,094 features and
publishes zip/shapefile (3,999,966 B), GeoJSON (44,182,837 B) and GeoPackage (24,666,112 B) plus
parquet/PMTiles derivatives. Its field list matches the final HIFLD Open schema:
MHPID, NAME, ADDRESS, CITY, STATE, ZIP, ZIP4, TELEPHONE, TYPE, STATUS, OPEN, CLOSED, COUNTY,
COUNTYFIPS, COUNTRY, LATITUDE, LONGITUDE, NAICS_CODE, NAICS_DESC, SOURCE, IMAGERY, SOURCEDATE,
VAL_METHOD, VAL_DATE, WEBSITE, UNITS, SIZE, REVGEOFLAG (plus OBJECTID/GlobalID/SHAPE system
fields). Proposed contract for any future rebuild: preserve every column, name, type and value
verbatim (no substitute row IDs, MHPID preserved, leading zeros in ZIP/ZIP4/COUNTYFIPS and numeric
precision kept), native geometry as published, and record the producer release date separately from
portal edit/fetch dates. Completeness (56,094 features) has not been verified against any allowed
producer source; it rests on the rescue snapshot and mirrors listed below.

## Dated anonymous checks (2026-11-12, this evidence only)

- Old HIFLD Open item `4cdbccc5c538452aa91ceee277c460f9` → CONT_0001 "Item does not exist or is
  inaccessible" on both `geoplatform.maps.arcgis.com/sharing/rest/content/items/...` and
  `www.arcgis.com/sharing/rest/content/items/...`.
- Crosswalk Open REST service
  `services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Mobile_Home_Parks/FeatureServer?f=json`
  → HTTP 400 "Invalid URL" (the HIFLD org is gone).
- GeoPlatform ArcGIS org services directory (`services1.arcgis.com/Hp6G80Pky0om7QvQ/...?f=pjson`,
  532 services, fetched 2026-11-12): zero services whose name contains "mobile" or "home".
- GII: `gii.dhs.gov/HIFLD` and `gii.dhs.gov/HIFLD/api/v3/datasets?filter[slug]=mobile-home-parks`
  301-redirect to the dead `hifld-geoplatform.hub.arcgis.com` shell; the hub's
  `api/v3/datasets?filter[slug]=mobile-home-parks` returns an empty `data` array; the crosswalk's
  "GII Access Required: Yes" means the sole official continuation is credential-gated (GII account
  with layer access), consistent with sibling ORNL layers (#115, #251, #122, #61, #173, #5).
- ORNL distributes no public copy: `ornl.gov/projects/geospatial-data-sets` (200) lists the layer
  with no download; `hdds.ornl.gov`, `gist.ornl.gov`, `gis.ornl.gov` are unreachable (HTTP 000);
  ORNL/DOE Open Energy Hub (`openenergyhub.ornl.gov/api/explore/v2.1/catalog/datasets`, 183
  datasets) has no mobile-home-parks dataset; OSTI records API for "mobile home parks" + "Oak
  Ridge" returns only journal papers (e.g. ML classification studies), no dataset distribution.
- Data.gov: the harvest record `MGMT-GMO-HIFLD-648667` (found via catalog.data.gov) is metadata
  only; the CKAN `package_search` API returns 404 anonymously (as in prior sibling runs), and the
  catalog dataset search page does not serve search results anonymously from this environment.
- ArcGIS Online title search for "Mobile Home Parks" (hundreds of hits, fetched 2026-11-12): no
  item is owned by a producing-agency account (ORNL/DOE/DHS). Owners are third parties: FEMA NIC
  "Mobile Home Parks RAPT" (item `2ba2e5e2007f4a159d4dfab8534beed3`, FEMA_HQ_Preparedness_NIC,
  created 2025-08-18 — a FEMA re-upload of a HIFLD download), Pennsylvania EMA
  "HIFLD_Backups_gdb" (`18fb153120624222a96b4d7aade54e0e`), "Federal User Community"
  "Mobile Home Parks (Archive)" (`27b92e61f2fc4bc8adca966c86d560bf`, explicitly labeled archive),
  NAPSG Foundation (`ed1e173a494249a4b784d645eba49e6f`), state EM offices, local governments,
  universities and companies. All are community/mirror copies; none is claimed by its operator to
  be a producer or delegated distribution.

## Mirrors identified, banned as inputs

DataLumos project 239601 (DOI 10.3886/E239601, the inventory's archive; banned host — ICPSR/DataLumos,
anonymous requests meet a 403 challenge per sibling runs); PEDP "HIFLD Next" rescue snapshot
(hifld.publicenvirodata.org; comparison baseline only); Data Rescue Project portal
(portal.datarescueproject.org); FEMA RAPT / PEMA / NAPSG / state-EM / local-government /
university AGOL re-uploads listed above; PASDA copies (banned host). None may be used as a
pipeline input or "fallback" under the current source policy.

## Blocker (Needs decision)

No maintained public producer feed exists. DHS migrated the layer to the GII (HIFLD Secure) with
"GII Access Required: Yes" and no external landing page, so the only faithful sources are:
(a) a DHS GII/HIFLD Secure account with access to the Mobile Home Parks layer (a credentials/access
grant the factory cannot obtain itself), (b) an explicit maintainer decision accepting some other
distribution as an officially delegated one (none is currently claimed by its operator to be that),
or (c) a no-rebuild/retire decision. Until then no adapter, fetch, tests or staging candidate is
built, and the published inventory must not be repopulated from a mirror.

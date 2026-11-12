# Local Emergency Operations Center (EOC) source record

Inventory entry `local-emergency-operations-center-eoc` (HIFLD Open Inventory 2025-12-11 row): HSIP
Local Emergency Operations Centers in the United States, agency and publisher `TechniGraphics Inc.`,
inventory date issued 2017-06-07, archive DataLumos project 239489 with archive inventory
modification 2025-01-24 (a collection edit date, not a source release date). The pipeline fetches
nothing: no maintained public producer feed exists, so no adapter, fetch or tests are built, and no
mirror is substituted.

## DHS crosswalk match (dated)

The DHS HIFLD Open crosswalk row for "Local Emergency Operations Center (EOC)" was read on
2026-11-12 from the saved crosswalk copy
(https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx;
the DHS-published GeoPlatform crosswalk item is itself dead, per the Independent Systems Operators
record). Sheet 1 row 22 states exactly: Status `Migrated`; Layer Name `Local Emergency Operations
Center (EOC)`; Old Hosting Location
`https://geoplatform.maps.arcgis.com/home/item.html?id=874798faedc74358bac9bbe1867af3c7`; New
Hosting Location `GII Portal`; DUA Required `No`; GII Access Required `Yes`; Deprecation
Justification `Migrated to GII due to the deprecation of HILFD Open`; Agency `FBI Geospatial
Program`; External Landing Page empty; Open REST Service
`https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Local_Emergency_Operations_Centers_EOC/FeatureServer`;
Old Secure ID and New ID both `Please see GII`; Open ID `874798faedc74358bac9bbe1867af3c7`.
The source inventory row matches the crosswalk exactly (title and HIFLD lineage), and DHS records
the layer as migrated behind DHS GII access control, not to any public feed. For contrast, the
sibling crosswalk row 74 for "State Emergency Operations Centers (EOC)" is `Deprecate` /
"No longer supported as part of the HIFLD mission" with FEMA's own `gis.fema.gov` STATE_EOC service
as the external landing page — no agency kept a local-EOC successor outside the GII.

## Producer and publisher identities

- Inventory row: producer/publisher `TechniGraphics Inc.` — the HSIP contractor whose field notes
  the layer description carries ("At the request of NGA, text fields in this dataset have been set
  to all upper case…", GENERATOR/BASEMENT/-ALTERNATE verification narrative).
- data.gov harvest record `MGMT-GMO-HIFLD-336179`
  (`https://catalog.data.gov/dataset/local-emergency-operations-center-eoc`, live 2026-11-12):
  "Department of Homeland Security - Local Emergency Operations Center (EOC)", publisher `HIFLD`,
  identifier MGMT-GMO-HIFLD-336179, license `https://www.usa.gov/government-works`, dateModified
  2022-10-24, distribution list empty (no resources).
- DHS crosswalk Agency column: `FBI Geospatial Program` (the program office of record for the
  migrated layer). No FBI, DHS, FEMA or NGA public feed publishes the local-EOC layer.

## Historical vintage facts (research context, no accepted baseline)

The inventory's last public HIFLD Open vintage is the 2017-06-07 release; the data.gov record was
last modified 2022-10-24 before HIFLD Open's deprecation. The layer description preserved in mirror
copies states the CONTDATE-based record currentness span (oldest record 08/28/2009, newest
11/18/2009 in that description snapshot), that the GPI source contains State and Federal EOCs in
addition to local EOCs, and TechniGraphics' GENERATOR/BASEMENT/TYPE/-ALTERNATE verification rules.
No schema or row count has been verified against any allowed producer source; no baseline is
accepted. Any future rebuild must preserve every source column, name, type and value verbatim
(no substitute row IDs), native source geometry, and record the producer release date separately
from portal edit/fetch dates.

## Dated anonymous checks (2026-11-12, this evidence only)

- Crosswalk row read (saved copy above); crosswalk DOI `10.3886/E241367V1` resolves to
  `https://www.datalumos.org/datalumos/project/241367/version/V1/view` → HTTP 403 anonymous
  (DataLumos is a banned input in any case).
- Open REST service
  `https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Local_Emergency_Operations_Centers_EOC/FeatureServer?f=json`
  → HTTP 400 "Invalid URL" (the DHS HIFLD ArcGIS organization is gone).
- HIFLD Open item `874798faedc74358bac9bbe1867af3c7` on the ArcGIS sharing API → CONT_0001 "Item
  does not exist or is inaccessible"; the `geoplatform.maps.arcgis.com` item page URL is a
  JavaScript-only 200 shell with no dataset content.
- `https://gii.dhs.gov/HIFLD` redirects (HTTP 301) to `https://hifld-geoplatform.hub.arcgis.com/`;
  the hub returns 200 shells only and
  `https://hifld-geoplatform.opendata.arcgis.com/datasets/geoplatform::local-emergency-operations-center-eoc`
  serves an empty "ArcGIS Hub" shell (page text length 20). The GII portal root
  (`https://gii.dhs.gov/`, 302 to `/gii/home/`) is alive but JS-only; the crosswalk marks
  `GII Access Required: Yes`, so the migrated layer sits behind a DHS GII (HIFLD Secure) account.
- data.gov CKAN `package_search` API returns 404 anonymously (as in prior sibling runs); the
  catalog page above is live with an empty distribution list.
- FEMA publishes no local-EOC feed: the `https://gis.fema.gov/arcgis/rest/services` folder list
  (fetched 2026-11-12) contains `FEMA/STATE_EOC` but no Local EOC service, and an ArcGIS Online
  search of FEMA's org (`orgid:XG15cJAlne2vxtgt`) returns only regional static packages and
  state/regional EOC layers.
- ArcGIS Online title/slug searches for "Local Emergency Operations Center (EOC)" (32 and 137
  results) show no item owned by a producing-agency account; owners are third parties (see
  mirrors below). The DHS HIFLD org `Hp6G80Pky0om7QvQ` has zero items matching `eoc`.
- DataLumos project 239489 (the issue's archive link) → HTTP 403 anonymous; banned input
  regardless.
- Data.gov record `MGMT-GMO-HIFLD-336179` (live page, fetched 2026-11-12) — publisher HIFLD/DHS,
  empty distribution list, dateModified 2022-10-24.

## Mirrors identified, banned as inputs

- NASA NCCS HIFLD mirror `https://maps.nccs.nasa.gov/mapping/rest/services/hifld_open/emergency_services/MapServer`
  layer `local_emergency_ops_centers` (6), "Downloaded from the HILFD archives May 2019" — frozen
  2019 copy; DNS-unreachable from this environment.
- FEMA Region 9 AGOL item `fc5de936e9eb44e9bb48bf6cb432c67f`
  ("Lifeline- Safety & Security - Live - Point - Local EOCs (HIFLD)", `FEMA_Region9_GIS`, item
  modified 2024-04-12) — registers the dead `services1.arcgis.com/Hp6G80Pky0om7QvQ` service URL as
  its source; a static regional re-host, not a maintained national feed.
- University of Georgia AGOL item `b2ba402929d4493898b5af6a6b2b737d` ("HIFLD Local Emergency
  Operations Center", owner `tqd32886_USG`, created 2026-08-25) registering
  `https://services2.arcgis.com/PYn6bWCjT6bhw1z3/.../HIFLD_Local_Emergency_Operations_Center/FeatureServer`.
- Other AGOL third-party items (California Office of Emergency Services `40522b0c861a48e5a4dc59e414f3137e`,
  university, state and consultant copies), WIFIRE Commons Data Catalog, and DataLumos project
  239489 / ICPSR DOIs `10.3886/e239489v1`.
- PASDA and other mirrors banned by name under the current source policy.

None may be used as a pipeline input or "fallback" under the current source policy.

## Blocker (Needs decision)

No maintained public producer feed exists. DHS migrated the layer to the GII (HIFLD Secure) with
`GII Access Required: Yes`, DUA Required `No`, and no external landing page, so the only faithful
sources are: (a) a DHS GII/HIFLD Secure account with access to the migrated layer (credentials or
access grant the factory cannot obtain itself), (b) an explicit maintainer decision accepting some
other distribution as an officially delegated one (no operator currently claims to be that), or
(c) a no-rebuild/retire decision. Until then no adapter, fetch, tests or staging candidate is
built, and the published inventory must not be repopulated from a mirror.

# Truck Driving Schools source record

Inventory entry `truck-driving-schools` (HIFLD Open Inventory row): HIFLD "Truck Driving Schools",
agency `TechniGraphics Inc.`, publisher `Techni Graphic Systems, Inc.` — the HSIP contractor — with
the DataLumos project 240723 copy (archive inventory modification 2023-03-20, a collection edit
date, not a source release date) as identification context only. The pipeline fetches nothing: no
maintained public producer feed exists, so no adapter, fetch or tests are built, and no mirror is
substituted.

## DHS crosswalk match (dated)

The DHS HIFLD Open crosswalk row for "Truck Driving Schools" was read on 2026-11-14 (05:53Z) from
the saved crosswalk copy
(https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx,
sha256 `7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc`; the DHS-published
GeoPlatform crosswalk item itself is dead). Sheet 1 states exactly: Status `Deprecate`; Layer Name
`Truck Driving Schools`; Old Hosting Location
`https://geoplatform.maps.arcgis.com/home/item.html?id=8264664d1f53439f9d722fac2713b77b`; New
Hosting Location `N/A`; DUA Required and GII Access Required blank; Deprecation Justification
`No longer supported as part of the HIFLD mission`; Agency blank; External Landing Page blank; Open
REST Service
`https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Truck_Driving_Schools/FeatureServer`;
Old Secure ID `Please see GII`; New ID `N/A`; Open ID `8264664d1f53439f9d722fac2713b77b`.

The source inventory row matches the crosswalk lineage (the layer DHS retired from the HIFLD
mission), and DHS records no successor: not migrated to any public feed, not delegated, and the
secure (HIFLD Secure/GII) lineage pointer is `Please see GII`.

## Producer and publisher identities

- Inventory row: producer/publisher `TechniGraphics Inc.` / `Techni Graphic Systems, Inc.` — the
  NGA/DHS HSIP production contractor. No federal agency is named as the layer's program office in
  the crosswalk (Agency column blank).
- The layer's HIFLD Open publication was hosted by the DHS HIFLD ArcGIS organization
  (`services1.arcgis.com/Hp6G80Pky0om7QvQ`, the HIFLD Open hosting org), not by a TechniGraphics
  or federal-agency ArcGIS account.

## Dated anonymous checks (2026-11-14, this evidence only)

- Crosswalk row read (saved copy above); crosswalk DOI `10.3886/E241367V1` is the DataLumos record
  (banned input in any case).
- Open REST service
  `https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Truck_Driving_Schools/FeatureServer?f=pjson`
  → HTTP 400 "Invalid URL" (service deleted from the HIFLD Open org).
- HIFLD Open item `8264664d1f53439f9d722fac2713b77b` on the ArcGIS sharing API → CONT_0001 "Item
  does not exist or is inaccessible" (deleted).
- `https://gii.dhs.gov/HIFLD` → HTTP 301 to `https://hifld-geoplatform.hub.arcgis.com/` (the dead
  public hub shell route, as in prior sibling runs); the crosswalk's `Old Secure ID: Please see
  GII` marks DHS GII (HIFLD Secure) as the credential-gated continuation channel for retired-HIFLD
  layers. No anonymous GII equivalent exists.
- ArcGIS sharing search for "truck driving schools" (572 hits, 2026-11-14T05:53:42Z): only
  third-party and regional items — Oregon DEQ "Truck Driving Schools (OR)" (state subset,
  `sarah.idczak_DEQ`), personal copies (`clibby007`, 2021), Hunt Institute/SMU Dallas (2024) —
  none owned by a producing agency, none national, none maintained as a HIFLD successor.
  A `owner:"HIFLD" truck` search returns 0 items (the org holds no truck-driving item).
- data.gov catalog HTML search for "truck driving schools" (2026-11-14T05:54:29Z): no federal
  truck-driving-schools dataset (search returns unrelated results only).

## Conclusion and what a rebuild would require

The HIFLD Open "Truck Driving Schools" layer was deprecated by DHS as "No longer supported as part
of the HIFLD mission"; its open service and item are deleted; no federal or contractor successor
publication exists (data.gov and AGOL checked). The only official continuation channel is the
credential-gated DHS GII (HIFLD Secure) lineage pointer ("Please see GII"). Third-party, state-
subset and personal AGOL copies are mirrors/subsets and are never pipeline inputs under the source
policy; DataLumos project 240723 is a banned archive.

Rebuilding this dataset requires one of: a DHS GII (HIFLD Secure) access grant for this layer, an
explicit maintainer decision to accept a materially different partial product as a changed
contract, or a maintainer-supplied official archive of the last HIFLD Open vintage. Until then the
correct action is to park or retire the card; no draft adapter is built against any mirror.

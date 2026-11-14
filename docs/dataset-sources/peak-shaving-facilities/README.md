# Peak Shaving Facilities — blocked: official continuation requires DHS GII access

Inventory `peak-shaving-facilities` (agency: Department of Energy; publisher/office: Oak Ridge National
Laboratory; inventory `date_issued` 2022-10-06, `date_modified` 2024-12-12). The layer represents Peak
Shaving Plants — vaporized LNG or propane-air (LPG-air) injection points that augment natural gas pipelines
during supply shortfalls — with locational and attribute information for the continental United States.
The DHS crosswalk names the producing agency as Oak Ridge National Laboratory (ORNL), and the community
copy's own metadata credits "Oak Ridge National Laboratory (ORNL), Argonne National Laboratory (ANL),
Decision and Infrastructure Sciences Division, National Geospatial-Intelligence Agency (NGA) Homeland
Security Infrastructure Program (HSIP) Team". No public original-producer feed exists. Mirrors, HIFLD
rescue copies and archive compilations are research context only and are never pipeline inputs.

## Crosswalk match (verified 2026-11-13)

DHS published the official "HIFLD OPEN GIS Data Index and Crosswalk"
([DOI 10.3886/E241367V1](https://doi.org/10.3886/E241367V1), xlsx 60.2 KB). The official GeoPlatform item
[41b4ff18bb094ba996dec75916de3bf3](https://geoplatform.maps.arcgis.com/home/item.html?id=41b4ff18bb094ba996dec75916de3bf3)
is deleted, and [DataLumos project 241367](https://www.datalumos.org/datalumos/project/241367/version/V1/view)
holds the official copy; this verification used a byte copy of the xlsx (61,689 bytes, SHA-256
`7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc`) retrieved from a public mirror of the
DHS file. The row for `Peak Shaving Facilities` reads:

- Status: Migrated — "Migrated to GII due to the deprecation of HILFD Open"
- Old hosting location: GeoPlatform item
  [05beb7d30a6d4e44a44feb976ab5fb0c](https://geoplatform.maps.arcgis.com/home/item.html?id=05beb7d30a6d4e44a44feb976ab5fb0c)
- New hosting location: **GII Portal**; DUA Required: No; **GII Access Required: Yes**
- Agency: **Oak Ridge National Laboratory (ORNL)**; External landing page: none
- Old open REST service
  `https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Peak_Shaving_Facilities_gdb/FeatureServer`
  → HTTP 400 "Invalid URL" (2026-11-13); old HIFLD Open item `05beb7d30a6d4e44a44feb976ab5fb0c` →
  `CONT_0001` "Item does not exist or is inaccessible." (2026-11-13)

ORNL matches the inventory's DOE attribution. The sibling layer `Above Ground LNG Storage Facilities`
(carries the same GII migration, agency ORNL) is a separate inventory entry and dataset.

## No public producer feed (checked 2026-11-13)

- `gii.dhs.gov/HIFLD` redirects to the public hub shell (`hifld-geoplatform.hub.arcgis.com`); the hub API
  v3 slug search for `peak-shaving-facilities` returns 0 datasets and the dataset search returns
  `CONT_0001`. Anonymous access cannot reach the GII copy (crosswalk: GII Access Required: Yes).
- ORNL portals ([ornl.gov/projects/geospatial-data-sets](https://www.ornl.gov/projects/geospatial-data-sets/)):
  the page credits ORNL's HIFLD contributions generically and lists no layer downloads, no peak-shaving
  product and no opendata endpoint.
- [Open Energy Hub](https://openenergyhub.ornl.gov/) is a JS-only catalog; a 2026-11-11 sibling check
  (issue #255) enumerated its 183 datasets and found only EIA layers (`oil-and-natural-gas-wells`,
  `oil-refineries`) among natural-gas products — no ORNL peak-shaving dataset.
- OSTI: search results for "peak shaving" return only energy-research papers (grid/storage controls), no
  ORNL geospatial dataset or successor product.
- ArcGIS Online: searches for `title:"peak shaving"` and ORNL-owned items return no producer-owned item;
  every hit is a third-party or community re-upload (see below).
- The data.gov catalog API was unreachable from factory egress (HTTP 404 responses, 2026-11-13); the
  historical HIFLD harvest records for this layer point only to the dead HIFLD Open portal.

## Original product shape (research context, mirror-derived only)

The final HIFLD Open snapshot's schema is preserved in community rescue copies (for example
`meepbu2024_bucas` FeatureServices uploaded 2024-06 and 2025-08-19, and a 2023 `HSEMA_PPD` item whose
underlying HIFLD service is dead). It carries 34 fields: `FID`, `OBJECTID`, `PSPID`, `NAME`, `ADDRESS`,
`CITY`, `STATE`, `ZIP`, `ZIP4`, `TELEPHONE`, `TYPE`, `STATUS`, `POPULATION`, `COUNTY`, `COUNTYFIPS`,
`COUNTRY`, `LATITUDE`, `LONGITUDE`, `NAICS_CODE`, `NAICS_DESC`, `SOURCE`, `SOURCEDATE`, `VAL_METHOD`,
`VAL_DATE`, `WEBSITE`, `FAC_GOVID`, `OWNER`, `OPERATOR`, `POSREL`, `NUM_VAPE`, `NUMTANKS`, `MAXVAPCAP`,
`TOTALCAP`, `LNGLPG_SRC`. `PSPID` (pattern `PSPnnnnnn`) is the stable facility ID; capability fields
(`NUM_VAPE`, `NUMTANKS`, `MAXVAPCAP`, `TOTALCAP`, `LNGLPG_SRC`) and per-record lineage
(`SOURCE`/`SOURCEDATE`/`VAL_METHOD`/`VAL_DATE`, e.g. "DOT-PHMSA; EPA; CORPORATE WEBSITE") define the
ORNL/ANL/HSIP enrichment. A 2014 third-party copy counts 60 LNG peak shavers plus 41 satellite facilities;
a 2024 community copy counts 89 rows. These copies are mirrors — used here only to document the product's
shape, never as inputs.

## Maintained constituents cannot recreate the product

- PHMSA's maintained annual LNG bulk download
  ([landing](https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids),
  [ZIP](https://www.phmsa.dot.gov/sites/phmsa.dot.gov/files/data_statistics/pipeline/annual_liquefied_natural_gas_2010_present.zip),
  updated monthly as operator supplemental reports arrive; verified 2026-10-20 at 2,358,119 bytes,
  SHA-256 `69a8283274b89310099807781c1e22ed2c1115c4d55026962ee1da9f8788bcbf`; HTTP 403 to factory egress
  on 2026-11-13) has 309 LNG facility rows for 2025, including 70 `Peak Shaving` and 32 `Satellite`
  classifications, with `LNG_FACILITY_ID`, names, status and state/ZIP but **no latitude, longitude,
  street address or city**; some `NPMS_LNG_ID` values are blank. PHMSA's 2026 Federal Register proposal
  states it currently proposes *adding* LNG plant latitude/longitude to annual reporting; NPMS GIS access
  is restricted. It covers LNG only and misses the propane-air facilities in this layer's definition.
- FERC's jurisdictional peakshaver list
  ([page](https://www.ferc.gov/media/ferc-jurisdictional-peakshavers-10), last updated October 6, 2026)
  lists 12 existing sites, 1 under construction and 1 proposed with company and city/state text plus a
  map — no coordinate table, FERC-jurisdictional subset only.
- The EIA U.S. Energy Atlas layers (token-gated) cover LNG import/export terminals, natural gas pipelines,
  processing plants, storage and hubs — no peak-shaving facility layer.

Neither constituent publishes `PSPID`s, capability fields (tank/vaporizer counts, LNG/LPG source) or
validation lineage, and neither supplies the complete LNG + propane-air population with point locations.
A rebuild from them would be a materially different product, not a faithful reconstruction of this
dataset; geocoding from ZIP/city text or map placement is not allowed.

## Decision required

Implementing a rebuild requires either:

1. **GII/HIFLD Secure access** for the factory — the crosswalk routes this layer to the GII portal with
   GII Access Required: Yes (DUA No); a credential/access grant the factory cannot obtain itself, or
2. An explicit maintainer decision to park or retire this dataset, or
3. An explicit changed-scope decision to rebuild from the maintained LNG-only constituents without
   coordinates (not a faithful reconstruction).

Geocoding, constituent subsets, mirrors and HIFLD rescue copies (DataLumos 240568, community AGOL
uploads) are not substitutes. Shared prerequisite
[#417](https://github.com/fulton-ring/hifld-next-datasets/issues/417) remains blocking. No source,
baseline, schema/ID contract, release or publication is approved by this note.

# Oil and Natural Gas Platforms — blocked: official continuation requires DHS GII access

Inventory `oil-and-natural-gas-platforms` (agency: Department of Energy). The DHS crosswalk identifies the producing agency as Oak Ridge National Laboratory (ORNL) and routes the layer to the credential-gated DHS GII portal. No public original-producer feed exists. Mirrors, HIFLD rescue copies and archive compilations are research context only and are never pipeline inputs.

## Crosswalk match (verified 2026-11-13)

DHS published the official "HIFLD OPEN GIS Data Index and Crosswalk" ([DOI 10.3886/E241367V1](https://doi.org/10.3886/E241367V1), xlsx 60.2 KB). The original GeoPlatform item [d6403a49d5cd428ebe67a1fa7fd69a40](https://geoplatform.maps.arcgis.com/home/item.html?id=d6403a49d5cd428ebe67a1fa7fd69a40) now returns `CONT_0001 "Item does not exist"`, and [DataLumos project 241367](https://www.datalumos.org/datalumos/project/241367/version/V1/view) holds the official copy; this verification used a byte copy of the xlsx (61,689 bytes, SHA-256 `7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc`) retrieved from a public mirror of the DHS file. The row for `Oil and Natural Gas Platforms` reads:

- Status: Migrated — "Migrated to GII due to the deprecation of HILFD Open"
- New hosting location: **GII Portal**; DUA Required: No; **GII Access Required: Yes**
- Agency: **Oak Ridge National Laboratory (ORNL)**; External landing page: none
- Old open REST service `https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Oil_and_Natural_Gas_Platforms/FeatureServer` → HTTP 400 "Invalid URL" (2026-11-13); old HIFLD Open item `d6403a49d5cd428ebe67a1fa7fd69a40` → "Item does not exist" (2026-11-13)

ORNL matches the inventory's DOE attribution ("Data and details are hosted by HIFLD as received from the data provider"). The product covers offshore oil and gas platforms in Gulf of Mexico federal waters and off the US Pacific coast; the DataLumos rescue records a HIFLD Open data time period of 2011-08-24 – 2016-05-03. Mirror metadata (PASDA `HIFLD_FEMA` MapServer layer 118, context only) shows 7,294 points spanning the Pacific coast to the central Gulf and a 37-field schema whose ORNL enrichment columns define the product: `PLATFRMID`, `COMPLEXID`, `NOSTR`, `NAICS_CODE`/`NAICS_DESC`, `OWNER`, `OPERNAME`, `POSREL`, `COMMODITY`, `MANNED24HR`, `DISTLAND`, `WATERDEPTH`, `RIGCOUNT`, `PWRSOURCE`/`PWRGENFLAG`/`OILPRDFLAG`/`GASMTRFLAG`/`COMPREFLAG`/`COMPRDFLAG`/`HELPRTFLAG`, `BEDCOUNT`, `BLOCKNO`/`LEASENO`/`AREANUMBER`/`REGION`, plus per-row lineage (`SOURCE`, `SOURCEDATE`, `VAL_METHOD`, `VAL_DATE`) and `GlobalID`.

## No public producer feed (checked 2026-11-13)

- `gii.dhs.gov/HIFLD` redirects to the public hub shell (`hifld-geoplatform.hub.arcgis.com`); the hub API v3 slug search for `oil-and-natural-gas-platforms` returns no datasets. Anonymous access cannot reach the GII copy (crosswalk: GII Access Required: Yes).
- ORNL portals ([ornl.gov/projects/geospatial-data-sets](https://www.ornl.gov/projects/geospatial-data-sets/)): only a generic paragraph crediting HIFLD with a dead `hifld-geoplatform.opendata.arcgis.com` link; no platform layer downloads. [Open Energy Hub](https://openenergyhub.ornl.gov/) lists 183 datasets, none named for platforms (only the EIA layers `oil-and-natural-gas-wells` and `oil-refineries`).
- OSTI: no record publishes the layer or an ORNL successor product.
- The data.gov harvest record `MGMT-GMO-HIFLD-998631` (publisher HIFLD, modified 2022-10-24) points to HIFLD Open, which is dead.

## Maintained constituents cannot recreate the product

- BSEE's maintained platform product (`data.bsee.gov/Mapping/Files/platform.zip`, Gulf of Mexico federal waters only, 7,317 points, monthly updates) carries only `STRUCTURE_`, `COMPLEX_ID`, `STRUCTURE1`, `INSTALL_DA`, `REMOVAL_DA`. It is already tracked separately as issue #257 and carries none of the ORNL enrichment fields above.
- BOEM's Pacific platform ASCII files ([data.boem.gov/Main/PacificPlatform.aspx](https://www.data.boem.gov/Main/PacificPlatform.aspx), platform masters/structures/locations updated 2026-11-01) provide Pacific OCS platform coordinates only.

Neither BSEE nor BOEM publishes ORNL's NAICS coding, owner/operator enrichment, bed counts, manning/power/production flags, distance-to-land, rig counts or validation lineage. A rebuild trimmed to those constituents would be a materially different product, not a faithful reconstruction of this dataset; a silent substitution is not allowed.

## Decision required

Implementing a rebuild requires either:

1. **GII/HIFLD Secure access** for the factory — a credential/access grant the factory cannot obtain itself, or
2. An explicit maintainer decision to park or retire this dataset, or to rebuild from the maintained BOEM/BSEE constituents as a changed-scope product (a separate, explicit decision).

Geocoding, constituent subsets, mirrors and HIFLD rescue copies (DataLumos 240503, PASDA, Data Rescue Project) are not substitutes. Shared prerequisite [#417](https://github.com/fulton-ring/hifld-next-datasets/issues/417) remains blocking. No source, baseline, schema/ID contract, release or publication is approved by this note.

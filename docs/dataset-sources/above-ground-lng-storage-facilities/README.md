# Above Ground LNG Storage Facilities — blocked: official continuation requires DHS GII access

Inventory `above-ground-lng-storage-facilities` (agency: Department of Energy). The DHS crosswalk identifies the producing agency as Oak Ridge National Laboratory (ORNL) and routes the layer to the credential-gated DHS GII portal. No public original-producer feed exists. Mirrors, HIFLD rescue copies and archive compilations are research context only and are never pipeline inputs.

## Crosswalk match (verified 2026-11-12)

DHS published the official "HIFLD OPEN GIS Data Index and Crosswalk" ([DOI 10.3886/E241367V1](https://doi.org/10.3886/E241367V1), xlsx 60.2 KB). The original GeoPlatform item [41b4ff18bb094ba996dec75916de3bf3](https://geoplatform.maps.arcgis.com/home/item.html?id=41b4ff18bb094ba996dec75916de3bf3) now returns `CONT_0001 "Item does not exist"`, and [DataLumos project 241367](https://www.datalumos.org/datalumos/project/241367/version/V1/view) holds the official copy; this verification used a byte copy of the xlsx (61,689 bytes, SHA-256 `7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc`) retrieved from a public mirror of the DHS file. The row for `Above Ground LNG Storage Facilities` reads:

- Status: Migrated — "Migrated to GII due to the deprecation of HILFD Open"
- New hosting location: **GII Portal**; DUA Required: No; **GII Access Required: Yes**
- Agency: **Oak Ridge National Laboratory (ORNL)**; External landing page: none
- Old open REST service `https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Above_Ground_LNG_Storage_Facilities_gdb/FeatureServer` → HTTP 400 "Invalid URL" (2026-11-12); old HIFLD Open item `e97f9385ce22445a8741ecedad1ac777` → "Item does not exist" (2026-11-12)

ORNL is consistent with the inventory's DOE attribution and with the HSIP-style per-row enrichment (`SOURCE` strings such as "DOT-PHMSA; EPA; CORPORATE WEBSITE", `SOURCEDATE` 2021-09-01) observed in rescued rows.

## No public producer feed (checked 2026-11-12 plus prior dated runs)

- EIA ArcGIS org `FGr1D95XCGALKXqM`: "LNG" → 3 Image items (import/export terminal maps only); "above ground LNG" and "peak shav" → 0 results. No feature layer for above-ground storage.
- [eia.gov/maps](https://www.eia.gov/maps/) and [eia.gov/maps/oil-naturalgas.php](https://www.eia.gov/maps/oil-naturalgas.php): no above-ground LNG storage data file; the archived "U.S. LNG Peak Shaving and Import Facilities Map" (data through 2007/2008) is a JPG raster with no machine-readable payload.
- PHMSA [LNG Annual Data 2010–present](https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids): facility IDs, state and ZIP only, no geometry; Operator Safety Program workbook: no location fields (verified 2026-10-21).
- NPMS (`npms.phmsa.dot.gov`): DNS unreachable from the worker pod; not probed further this run.
- ORNL public portals ([ornl.gov/projects/geospatial-data-sets](https://www.ornl.gov/projects/geospatial-data-sets/), openenergyhub.ornl.gov): ORNL's HIFLD layer list does not include this layer and no downloads are offered.
- PASDA/PSU HIFLD copies, the DataLumos rescue [238493](https://www.datalumos.org/datalumos/project/238493/version/V1/view) (PIs: US DOE + US DHS; data period 2022-10-06), hifld.publicenvirodata.org and other surviving copies: excluded mirrors/rescues, context only. The previously staged `v1.0.0` candidate (13 Pennsylvania rows from a PASDA `conus_clipped` mirror) is invalid as a source or baseline and is not national coverage.

## Decision required

Implementing the national rebuild requires either:

1. **GII/HIFLD Secure access** for the factory — a credential/access grant the factory cannot obtain itself, or
2. An explicit maintainer decision to park or retire this dataset.

Geocoding, terminal subsets, underground storage, FERC lists, mirrors and HIFLD rescue copies are not substitutes. Shared prerequisite [#417](https://github.com/fulton-ring/hifld-next-datasets/issues/417) remains blocking. No source, baseline, schema/ID contract, release or publication is approved by this note.

## History

All 44 tracked research files, captures, reports, candidate evidence, and audit scripts are preserved in `/tmp/hifld-native-reconstruction/cleanup5/issue-5-removed-research-docs-25420f651375.tar.gz` (561,192 original bytes; archive SHA-256 `e32036ffd386039eb086dce846d6239cfbcb01f66621665f582fd92e7ac9543b`). Restore from the repository root with `tar -xzf /tmp/hifld-native-reconstruction/cleanup5/issue-5-removed-research-docs-25420f651375.tar.gz -C .`; the sidecar manifest records the verified members and hashes.

Prior evidence and decision context: [issue #5 progress](https://github.com/fulton-ring/hifld-next-datasets/issues/5#issuecomment-6259907385), [independent review](https://github.com/fulton-ring/hifld-next-datasets/issues/5#issuecomment-6259917024), [PR #424 review](https://github.com/fulton-ring/hifld-next-datasets/pull/424#issuecomment-6259917243), the 2026-10-21 source-readiness and PHMSA checks, and the 2026-11-08 research checkpoint on issue #5.

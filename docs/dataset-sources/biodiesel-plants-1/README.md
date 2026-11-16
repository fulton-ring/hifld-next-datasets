# Biodiesel Plants (biodiesel-plants-1)

## Stopped historical draft (2026-11-12)

The original producer distribution currently accessible for this inventory
entry is EIA's own static "Biodiesel Plants in the United States" Shapefile at
<https://www.eia.gov/maps/map_data/Biodiesel_Plants_US_EIA.zip> (retrieved
2026-11-12: 12,169 bytes, SHA-256
`754e171fabbeaacd1cbebe9af65c8497fd85cff7ca08de3bd87bd0d7d07b5aa5`, server
`Last-Modified: 2022-02-03`). The ZIP's eight sidecars are named
`Biodiesel_Plants_US_20210101.*`; its embedded Esri metadata credits
"U.S. Energy Information Administration", dates the plant population
"Biodiesel plants in the United States as of January 1, 2021", states the
attribute data come from EIA's [U.S. Biodiesel Plant Production
Capacity](https://www.eia.gov/biofuels/capacity/) report (Form EIA-819,
captured as relationship evidence), and warns that locations were determined
through publicly available information such as company websites and media
reports.

`src/dagster_hifld/sources/eia_biodiesel_plants.py` captures the pinned
archive and the capacity-report page, validates the complete native contract
(exact archive bytes, eight-member roster, ten DBF fields `OBJECTID`,
`Company`, `Site_Name`, `Cap_Mmgal`, `State`, `Source`, `Period`,
`Longitude`, `Latitude`, `PADD`, 75 unique points with contiguous `OBJECTID`
1..75, WGS84, embedded metadata markers, geometry-versus-attribute
coordinates, ID roster, full-record and geometry digests, and bounds), and
stages an unapproved GeoParquet candidate with raw body/wire/receipt
evidence. The native `Cap_Mmgal` field is a DBF **string** (including values
such as `less than 0.5` and `41.601085`); it is preserved as a string without
numeric coercion, and no synthetic row identifiers are generated. The ingest
asset's refresh schedule is STOPPED: a changed or removed EIA archive fails
closed into quarantine rather than refreshing.

## Current spatial source status (re-verified 2026-11-12)

The EIA Biodiesel Plants service [layer
113](https://services7.arcgis.com/FGr1D95XCGALKXqM/arcgis/rest/services/Biodiesel_Plants_US_EIA/FeatureServer/113?f=pjson)
still returns HTTP 200 with application error 499 `Token Required`
(`GWM_0003`) for layer metadata and count queries; current complete spatial
data and identifiers from the Atlas layer remain inaccessible without
authorized access. The official [capacity
report](https://www.eia.gov/biofuels/capacity/) released September 24, 2026
covers January 1, 2026 and lists 45 biodiesel plants among 253 facilities
(189 ethanol, 19 other biofuels), but supplies no coordinates or stable
source IDs. The pinned January 2021 ZIP has 75 historical points and cannot
refresh the unapproved 56-point January 2024 HIFLD rescue candidate; the
inventory's January 2023 text and crosswalk lineage remain unresolved. The
historical ZIP and that candidate share 53 exact numeric coordinate witnesses,
but 22 historical and three candidate rows remain unmatched; coordinate
matches are diagnostics, not identifiers. No ID mapping, geometry tolerance
or row continuation is approved.

## Unresolved acceptance

This is a January 2021 historical vintage, not the token-gated EIA Atlas
layer and not the retired 2024 HIFLD inventory archive (DataLumos 238710,
which is identification context only, never a pipeline input); equivalence to
the assigned inventory entry, currentness, identifier/schema/coverage
contracts, approximate-location rights and attribution (EIA reuse requires
attribution), and release approval all remain pending. A successful draft
ingest or check does not clear those decisions or permit publication; all
runtime approval flags remain false. DHS crosswalk DOI/DataLumos returned
provider HTTP 403 and remains unverified.

## Archived research files

All 51 tracked research files, full raw/current candidate bodies, reports and
replay code (1,155,933 original bytes) are preserved at
`/tmp/hifld-native-reconstruction/cleanup22/issue-22-removed-research-docs-ab4eeb7ecbf4.tar.gz`,
SHA-256 `d9e1b4cb160a114de60ad8688e32028540b6dfdf7984718bb469d7b14def2849`.
Restore from repository root with `tar -xzf
/tmp/hifld-native-reconstruction/cleanup22/issue-22-removed-research-docs-ab4eeb7ecbf4.tar.gz -C .`;
sidecars record member hashes, original Git blobs/modes and archived physical
modes. Evidence: [issue #22 progress](https://github.com/fulton-ring/hifld-next-datasets/issues/22#issuecomment-6261082836),
[independent review](https://github.com/fulton-ring/hifld-next-datasets/issues/22#issuecomment-6261936232),
and [PR #433](https://github.com/fulton-ring/hifld-next-datasets/pull/433).
Runtime originals remain untouched.

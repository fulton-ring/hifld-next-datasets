# Ethanol Plants (ethanol-plants-1)

## Stopped historical draft (2026-11-10)

The original producer distribution for this inventory entry is EIA's own static
"Ethanol Plants in the United States" Shapefile at
<https://www.eia.gov/maps/map_data/Ethanol_Plants_US_EIA.zip> (retrieved
2026-11-10: 37,644 bytes, SHA-256
`a342f4af270f48d8512ad18e393eee65394f831cc045044d85790cfae77f829c`). The ZIP's
eight sidecars are named `Ethanol_Plants_US_20210101.*`, its embedded Esri
metadata credits "U.S. Energy Information Administration", dates the
operating-plant population "as of January 1, 2021", states that attribute data
come from EIA's [U.S. Fuel Ethanol Plant Production Capacity
report](https://www.eia.gov/petroleum/ethanolcapacity/) (Form EIA-819), and
warns that the facility locations are approximate, researched from publicly
available information.

`src/dagster_hifld/sources/eia_ethanol_plants.py` captures the pinned archive
and EIA's capacity-report page, validates the complete native contract (exact
archive bytes, eight-member roster, ten DBF fields `OBJECTID`, `Company`,
`Site_Name`, `Capacity`, `State`, `PADD`, `Data_Perio`, `Source`, `Longitude`,
`Latitude`, 197 unique points, WGS84, embedded metadata markers, geometry
versus attribute coordinates, ID roster, full-record and geometry digests, and
bounds), and stages an unapproved GeoParquet candidate with raw body/wire/receipt
evidence. The ingest asset's refresh schedule is STOPPED: a changed or removed
EIA archive fails closed into quarantine rather than refreshing.

## Prior access evidence (2026-10-19, retained)

The EIA Energy Atlas layer for this dataset — ArcGIS item
`1f0f8fd4458c43bd93642a01a02959e8` at
`services7.arcgis.com/FGr1D95XCGALKXqM/…/Ethanol_Plants_US_EIA/FeatureServer/112`
— returned ArcGIS `499 Token Required` for layer metadata, count, and ID
queries; Atlas dataset routes returned 404 and the catalog query returned
`numberMatched: 0` (scoped observations). Those checks did not establish the
absence of every EIA feed, and the static `map_data` archive above supersedes
them for this draft. The capacity report workbook itself carries no geometry or
spatial IDs.

## Unresolved acceptance

This is a January 2021 historical vintage, not the retired 2024 HIFLD inventory
archive (DataLumos 241097, which is identification context only, never a
pipeline input) and not the token-gated Atlas layer; equivalence to the assigned
2024 inventory, currentness, identifier/schema/coverage contracts,
approximate-location rights and attribution, and release approval all remain
pending. A successful draft ingest or check does not clear those decisions or
permit publication.

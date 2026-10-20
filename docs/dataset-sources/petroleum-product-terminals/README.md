# Petroleum Product Terminals — source decision

Research snapshot: 2026-10-20. No maintained, authoritative public location
feed was verified for this inventory entry. This is a source blocker, so no
archive-refresh pipeline or substitute-identity asset is proposed.

The inventory CSV names the Department of Energy / EIA as agency and points
to DataLumos project `240512`, V1. Its description separates two lineages:
terminal attributes are based on EIA Form EIA-815, while locations are
approximate and researched from public federal, company, and imagery sources.
EIA describes EIA-815 as a monthly operations and storage-capacity survey; it
does not identify the HIFLD geospatial compilation or provide its facility
coordinates. The [EIA survey forms page](https://www.eia.gov/Survey/) supports
the form's scope. EIA's [2013 article](https://www.eia.gov/todayinenergy/detail.php?id=13611)
describes an older 1,400-terminal map based on EIA-815 as of March 2013; it is
not evidence of a current location feed.

The [DataLumos V1 record](https://www.datalumos.org/datalumos/project/240512/version/V1/view)
is an archival snapshot, not a live producer endpoint. It attributes the
dataset to DOE and DHS. The [Data Rescue Project archive record](https://portal.datarescueproject.org/datasets/hifld-open-petroleum-product-terminals/)
states HIFLD was archived Aug. 26, 2025; it repeats the EIA-815 attribute
lineage and says locations come from public research. DataLumos lists four
2025-11-21 derivatives (FileGDB, GeoJSON, GeoPackage, and Shapefile); these
remain archive copies. No dated current release or direct producer handoff is
established by their existence.

An older ArcGIS service titled `PetroleumProduct_Terminals_US_EIA` remains
queryable at `https://services8.arcgis.com/XLNdV9JX2tH1BT1x/arcgis/rest/services/PetroleumProduct_Terminals_US_EIA/FeatureServer/14`.
Its item is owned by `AirAllianceHouston`, has no copyright or layer
description, last data edit 2022-09-07, EPSG:4269, and reports 1,474 rows. The
name and title do not establish EIA authority; its count also differs from the
2,302-row 2024-vintage HIFLD archive. Other ArcGIS results are FEMA/regional
copies or third-party maps, not verified producer distribution. EIA's 2013
map, the 2020 Argonne/GEM metadata (2,341 rows), third-party ArcGIS services,
and the HIFLD/DataLumos archives are separately classified historical or
secondary candidates, not interchangeable current sources.

An Oregon Department of Energy 2025 plan cites an EIA Energy Atlas “Petroleum
Product Terminals” item, but the cited Atlas URL returned HTTP 404 during this
research, and its ArcGIS item API returned HTTP 403. That is a lead to
reconcile, not evidence that the layer is currently retrievable or maintained.

## Proposed contract status

The historical product is a point layer with facility attributes and
coordinates; the precise published field roster, ID stability, physical
types, nullability, coordinate/source-CRS details, geometry fidelity, and
coverage relationship have not been independently checked against the
complete archive bytes. No column/ID mapping or successor relationship is
proposed. Preserve the archive's full raw files and IDs if a maintainer later
authorizes a historical reconstruction. A new EIA, HIFLD, FEMA, DOE, or
commercial layer must not replace this identity without explicit source-owner
and compatibility decisions.

## Decision needed

Identify a maintained producer/delegated geospatial location publication for
the exact HIFLD product, or have the source owner explicitly select a
historical-reconstruction scope. The DHS crosswalk match, current source,
release date/freshness, original extraction evidence, published contract,
coverage and rights remain unverified. Until that decision, no Dagster source
asset, refresh schedule, completeness baseline, or candidate materialization
is warranted.

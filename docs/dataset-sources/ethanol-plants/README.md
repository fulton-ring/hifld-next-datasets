# Ethanol Plants (ethanol-plants)

## Historical source label (issue #775 research, 2026-11-15)

The pipeline input for this dataset is EIA's own pinned historical GIS
snapshot <https://www.eia.gov/maps/map_data/Ethanol_Plants_US_EIA.zip>
(37,644 bytes, server `Last-Modified: 2022-02-03T19:31:49Z`, re-verified
2026-11-15). The embedded layer `Ethanol_Plants_US_20210101` is the January 1,
2021 plant population (197 points, EPSG:4326). This is a historical source,
not a maintained feed: EIA stopped refreshing the `map_data` zips around
2020-2022.

## Energy Atlas successor research (2026-11-15)

No confirmed, agency-owned Energy Atlas successor exists for this layer.

- The Energy Atlas Hub site (atlas.eia.gov, EIA ArcGIS Online organization
  `FGr1D95XCGALKXqM`) publishes 74 items through its content groups
  ("U.S. Energy Atlas - Content Library (EIA)" and "(Non-EIA)"), all shale
  plays, PADDs, storage regions, census basemaps and live feeds. This layer is
  not among them, and EIA's own publishing account (`EIA.gov`) owns 56 hosted
  feature services, none of them ethanol plant layers.
- A hosted service named after this layer exists inside EIA's organization
  cluster (`services7.arcgis.com/FGr1D95XCGALKXqM/arcgis/rest/services/
  Ethanol_Plants_US_EIA/FeatureServer`), but every metadata and query request
  returns HTTP 499 `Token Required` (`GWM_0003`): it is not publicly
  accessible, and the only anonymously visible ArcGIS item pointing at it is a
  third-party proxy registration owned by a non-EIA account
  (`wygisc_info`, "WyGISC Publisher"). Ownership cannot be verified and no
  anonymous extraction is possible, so it is excluded by the source policy.
- Publicly queryable "Ethanol Plants" copies on other ArcGIS organizations
  (Minnesota state GIS, Boston University and personal accounts) are
  third-party registrations, not EIA/DOE-owned, and are research context only.

The source adapter `src/dagster_hifld/sources/eia_ethanol_plants.py` therefore
keeps capturing the pinned zip (with EIA's ethanol capacity report captured as
relationship evidence) and fails closed into quarantine on any change. All
runtime approval flags remain false and the refresh schedule stays stopped;
currentness and any future successor remain pending the maintainer decisions
recorded on issue #775.

## Maintained-refresh disposition (2026-11-16)

The public refresh entrypoint now refuses fresh acquisition and retained-capture replay before network or staging side effects. Its pinned historical payload is not a maintained current-source pipeline. Existing published versions remain unchanged and addressable; offline parser fixtures remain useful evidence. A stopped schedule alone was insufficient to prevent a manual historical capture.

Use the [current EIA combined biofuels report](https://www.eia.gov/biofuels/capacity/) for current roster attributes. The inspected January2026 workbook lacks facility coordinates, addresses and native unique facility IDs; current point reconstruction therefore needs independently verified identity/location constituents.

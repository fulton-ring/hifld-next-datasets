# Power Plants (power-plants-1)

## Historical source label (issue #775 research, 2026-11-15)

The pipeline input for this dataset is EIA's own pinned historical GIS
snapshot <https://www.eia.gov/maps/map_data/PowerPlants_US_EIA.zip> (1,539,615
bytes, SHA-256
`b65e4f0f49fdb5b9709549169c4be23fd8ad42512025b5b1df13a94a843aa4d8`, server
`Last-Modified: 2022-01-31T13:34:47Z`, re-verified 2026-11-15). The embedded
layer `PowerPlants_US_202108` is the August 2021 plant population (10,847
points, EPSG:4326, native ID `Plant_Code`). This is a historical source, not a
maintained feed: EIA stopped refreshing the `map_data` zips around 2020-2022.

## Energy Atlas successor research (2026-11-15)

No confirmed, agency-owned Energy Atlas successor exists for this layer.

- The Energy Atlas Hub site (atlas.eia.gov, EIA ArcGIS Online organization
  `FGr1D95XCGALKXqM`) publishes 74 items through its content groups
  ("U.S. Energy Atlas - Content Library (EIA)" and "(Non-EIA)"), all shale
  plays, PADDs, storage regions, census basemaps and live feeds. This layer is
  not among them, and EIA's own publishing account (`EIA.gov`) owns 56 hosted
  feature services, none of them infrastructure plant layers.
- Hosted services named after the EIA layers exist inside EIA's organization
  cluster (`services7.arcgis.com/FGr1D95XCGALKXqM/...`), but every metadata
  and query request returns HTTP 499 `Token Required` (`GWM_0003`): they are
  not publicly accessible, and the only anonymously visible ArcGIS items
  pointing at them are third-party proxy registrations owned by non-EIA
  accounts (WyGISC/University of Wyoming, FEMA Region 9, Boston University).
  Ownership cannot be verified and no anonymous extraction is possible, so
  they are excluded by the source policy.
- The publicly queryable candidate `services2.arcgis.com/FiaPA4ga0iQKduv3/
  arcgis/rest/services/Power_Plants_in_the_US/FeatureServer` (layer
  `PowerPlants_US_EIA`, Point, 33 fields, Web Mercator, layer
  `lastEditDate` 2026-08-05) is hosted in the "Esri U.S. Federal Datasets"
  portal and its service item is owned by that portal's admin account
  (`Federal_User_Community`), not by EIA/DOE. An item owned by anyone other
  than the producing agency is research context only, never a pipeline input.

The source adapter `src/dagster_hifld/sources/ornl_power_plants.py` therefore
keeps capturing the pinned zip and fails closed into quarantine on any change.
All runtime approval flags remain false and the refresh schedule stays
stopped; currentness and any future successor remain pending the maintainer
decisions recorded on issue #775.

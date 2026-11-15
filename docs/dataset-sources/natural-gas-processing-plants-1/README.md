# Natural Gas Processing Plants (natural-gas-processing-plants-1)

## Historical source label (issue #775 research, 2026-11-15)

The pipeline input for this dataset is EIA's own pinned historical GIS
snapshot
<https://www.eia.gov/maps/map_data/NaturalGas_ProcessingPlants_US_EIA.zip>
(78,776 bytes, server `Last-Modified: 2020-01-10T15:23:59Z`, re-verified
2026-11-15). The embedded layer `NaturalGas_ProcessingPlants_US_2017_v2` is
the 2017 processing-plant population (478 points, EPSG:4326). This is a
historical source, not a maintained feed: EIA stopped refreshing the
`map_data` zips around 2020-2022.

## Energy Atlas successor research (2026-11-15)

No confirmed, agency-owned Energy Atlas successor exists for this layer.

- The Energy Atlas Hub site (atlas.eia.gov, EIA ArcGIS Online organization
  `FGr1D95XCGALKXqM`) publishes 74 items through its content groups
  ("U.S. Energy Atlas - Content Library (EIA)" and "(Non-EIA)"), all shale
  plays, PADDs, storage regions, census basemaps and live feeds. This layer is
  not among them, and EIA's own publishing account (`EIA.gov`) owns 56 hosted
  feature services, none of them processing-plant layers.
- A hosted service named after this layer exists inside EIA's organization
  cluster (`services7.arcgis.com/FGr1D95XCGALKXqM/arcgis/rest/services/
  NaturalGas_ProcessingPlants_US_EIA/FeatureServer`), but every metadata and
  query request returns HTTP 499 `Token Required` (`GWM_0003`): it is not
  publicly accessible, and the only anonymously visible ArcGIS items pointing
  at it are third-party proxy registrations owned by non-EIA accounts
  (`lstone4_UWyo`). Ownership cannot be verified and no anonymous extraction
  is possible, so it is excluded by the source policy.
- The "Esri U.S. Federal Datasets" portal candidate
  `services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/
  Natural_Gas_Processing_Plants1/FeatureServer` also returns HTTP 499
  `Token Required` for every request, and no publicly shared owning item
  exists in that organization; the visible items are third-party proxy
  registrations. Not usable either way.

The source adapter
`src/dagster_hifld/sources/eia_natural_gas_processing_plants.py` therefore
keeps capturing the pinned zip and fails closed into quarantine on any change.
All runtime approval flags remain false and the refresh schedule stays
stopped; currentness and any future successor remain pending the maintainer
decisions recorded on issue #775.

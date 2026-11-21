# Natural Gas Interstate and Intrastate Pipelines
(natural-gas-interstate-and-intrastate-pipelines)

## Historical source label (issue #775 research, 2026-11-15)

The pipeline input for this dataset is EIA's own pinned historical GIS
snapshot
<https://www.eia.gov/maps/map_data/NaturalGas_InterIntrastate_Pipelines_US_EIA.zip>
(2,954,667 bytes, server `Last-Modified: 2020-04-27T19:12:02Z`, re-verified
2026-11-15). The embedded layer `NaturalGas_Pipelines_US_202001` identifies a
January 2020 pipeline edition. This is a historical source, not a maintained
feed: EIA stopped refreshing the `map_data` zips around 2020-2022.

## Energy Atlas successor research (2026-11-15)

No confirmed, agency-owned Energy Atlas successor exists for this layer.

- The Energy Atlas Hub site (atlas.eia.gov, EIA ArcGIS Online organization
  `FGr1D95XCGALKXqM`) publishes 74 items through its content groups
  ("U.S. Energy Atlas - Content Library (EIA)" and "(Non-EIA)"), all shale
  plays, PADDs, storage regions, census basemaps and live feeds. This layer is
  not among them, and EIA's own publishing account (`EIA.gov`) owns 56 hosted
  feature services, none of them pipeline layers.
- A hosted service named after this layer exists inside EIA's organization
  cluster (`services7.arcgis.com/FGr1D95XCGALKXqM/arcgis/rest/services/
  NaturalGas_InterIntrastate_Pipelines_US_EIA/FeatureServer`), but every
  metadata and query request returns HTTP 499 `Token Required` (`GWM_0003`):
  it is not publicly accessible, and the only anonymously visible ArcGIS item
  pointing at it is a FEMA Region 9 "Lifeline" registration owned by a FEMA
  account, not EIA. Ownership cannot be verified and no anonymous extraction
  is possible, so it is excluded by the source policy.
- The publicly queryable "Esri U.S. Federal Datasets" portal copy
  `services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/
  Natural_Gas_Interstate_and_Intrastate_Pipelines_1/FeatureServer` (layer
  `NaturalGas_InterIntrastate_Pipelines_US_EIA`, Polyline, 6 fields, Web
  Mercator, layer `lastEditDate` 2025-12-04) is owned by that portal's admin
  account (`Federal_User_Community`), not by EIA/DOE. An item owned by anyone
  other than the producing agency is research context only, never a pipeline
  input.

The source adapter
`src/dagster_hifld/sources/eia_natural_gas_pipelines.py` therefore keeps
capturing the pinned zip and fails closed into quarantine on any change. All
runtime approval flags remain false and the refresh schedule stays stopped;
currentness and any future successor remain pending the maintainer decisions
recorded on issue #775.

## Maintained-refresh disposition (2026-11-16)

The public refresh entrypoint now refuses fresh acquisition and retained-capture replay before network or staging side effects. Its pinned historical payload is not a maintained current-source pipeline. Existing published versions remain unchanged and addressable; offline parser fixtures remain useful evidence. A stopped schedule alone was insufficient to prevent a manual historical capture.

No complete maintained public native-geometry successor was verified in the audit. Access to an agency service and ongoing maintenance are separate questions. Investigate the producer route and redistribution rights; an old ZIP or republisher is not a maintained fallback.

# Natural Gas Trading Hubs (natural-gas-trading-hubs)

## Historical source label (issue #775 research, 2026-11-15)

The pipeline input for this dataset is EIA's own pinned historical GIS
snapshot
<https://www.eia.gov/maps/map_data/NaturalGas_TradingHubs_US_EIA.zip> (30,668
bytes, server `Last-Modified: 2020-07-09T19:51:36Z`, re-verified 2026-11-15).
The embedded layer is EIA's February 2020 natural gas hub population (32
points). This is a historical source, not a maintained feed: EIA stopped
refreshing the `map_data` zips around 2020-2022.

## Energy Atlas successor research (2026-11-15)

No confirmed, agency-owned Energy Atlas successor exists for this layer.

- The Energy Atlas Hub site (atlas.eia.gov, EIA ArcGIS Online organization
  `FGr1D95XCGALKXqM`) publishes 74 items through its content groups
  ("U.S. Energy Atlas - Content Library (EIA)" and "(Non-EIA)"), all shale
  plays, PADDs, storage regions, census basemaps and live feeds. This layer is
  not among them, and EIA's own publishing account (`EIA.gov`) owns 56 hosted
  feature services, none of them trading-hub layers.
- Hosted services named after this layer family exist inside EIA's
  organization cluster (`services7.arcgis.com/FGr1D95XCGALKXqM/arcgis/rest/
  services/Market_Hubs_Natural_Gas/FeatureServer` and `.../HGL_Market_Hubs/
  FeatureServer`), but every metadata and query request returns HTTP 499
  `Token Required` (`GWM_0003`): they are not publicly accessible, and the
  only anonymously visible ArcGIS items pointing at them are third-party proxy
  registrations owned by non-EIA accounts (`lstone4_UWyo`). Ownership cannot
  be verified and no anonymous extraction is possible, so they are excluded by
  the source policy.
- The publicly queryable `Market_Hubs_Natural_Gas` copy on
  `services.arcgis.com/kbDdtKcFi2adamXa/...` is hosted in Boston University's
  ArcGIS organization, not EIA/DOE, and is research context only.

The source adapter
`src/dagster_hifld/sources/natural_gas_trading_hubs.py` therefore keeps
capturing the pinned zip and fails closed into quarantine on any change. All
runtime approval flags remain false and the refresh schedule stays stopped;
currentness and any future successor remain pending the maintainer decisions
recorded on issue #775.

## Maintained-refresh disposition (2026-11-16)

The public refresh entrypoint now refuses fresh acquisition and retained-capture replay before network or staging side effects. Its pinned historical payload is not a maintained current-source pipeline. Existing published versions remain unchanged and addressable; offline parser fixtures remain useful evidence. A stopped schedule alone was insufficient to prevent a manual historical capture.

No complete maintained public native-geometry successor was verified in the audit. Access to an agency service and ongoing maintenance are separate questions. Investigate the producer route and redistribution rights; an old ZIP or republisher is not a maintained fallback.

# Petroleum Refineries

## Finding

No current, maintained public geospatial source for the inventory's complete point layer was established. Do not build a refresh pipeline from the historical HIFLD/EIA map archive.

EIA's public `Petroleum_Refineries_US_EIA.zip` remains reachable, but the current response is an old 2021 shapefile: 130 points, `Period_` = “As of Jan. 1 2021”, EPSG:4326, 22 source attributes and a `site_id` field. The HTTP Last-Modified header is February 3, 2022. A responding URL and old bytes do not establish a current release. The layer metadata attributes coordinates partly to public research (agency, company and satellite sources), so EIA's capacity tables alone do not establish the point locations or their source lineage.

EIA currently publishes its annual Refinery Capacity Report and per-refinery capacity workbook (2026 report released June 26, 2026), but these products are tabular and do not constitute a replacement geospatial layer. The ArcGIS “Petroleum Refineries: US_EIA” item found during research is owned by `priho011_UMN`, not EIA; its recent portal edit does not prove a maintained EIA distribution. Other similarly named ArcGIS services are third-party copies and remain separate fallback leads.

DataLumos returned HTTP 403 while the crosswalk was being inspected. The DHS crosswalk match is therefore unverified, not unmatched. No historical baseline, producer lineage, stable source ID, or HIFLD-to-EIA mapping is accepted. Record a maintainer decision before any future implementation; a future candidate must preserve every raw source attribute and ID, and coverage/schema/identifier changes must enter the breaking-change gate.

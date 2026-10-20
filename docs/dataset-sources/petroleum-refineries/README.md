# Petroleum Refineries

## Historical candidate

The official EIA archive supports a faithful historical candidate, but does not establish a current maintained point layer. The adapter pins the EIA ZIP observed at SHA-256 `08243aed0c1f3d5206435d4932fb4e94ead8fe1b8e8d4220a03f601a460f4909`, preserves the full archive and all shapefile fields and IDs, and labels its vintage `2021-01-01`. A changed archive or Last-Modified value fails closed and requires a new source release review. The scheduled refresh remains stopped.

The 2021 shapefile contains 130 EPSG:4326 points and 22 source attributes, including `OBJECTID` and `site_id`. `Period_` is “As of Jan. 1 2021”; the ZIP Last-Modified header is February 3, 2022. A responding URL and old bytes do not establish a current release. The layer metadata attributes coordinates partly to public research (agency, company and satellite sources), so EIA's capacity tables alone do not establish the point locations or their source lineage.

EIA currently publishes its annual Refinery Capacity Report and per-refinery capacity workbook (2026 report released June 26, 2026), but these products are tabular and do not constitute a replacement geospatial layer. The ArcGIS “Petroleum Refineries: US_EIA” item found during research is owned by `priho011_UMN`, not EIA; its recent portal edit does not prove a maintained EIA distribution. Other similarly named ArcGIS services are third-party copies and remain separate fallback leads.

DataLumos returned HTTP 403 while the crosswalk was being inspected. The DHS crosswalk match is therefore unverified, not unmatched. No historical baseline, producer lineage, or HIFLD-to-EIA mapping is accepted. `OBJECTID` and `site_id` are preserved as source fields; neither is accepted as a continuing identifier contract. All source release, freshness, schema, ID, coverage, rights, mapping and release approvals remain false. The 2026 EIA capacity tables are nonspatial and are not substituted or joined to the 2021 locations.

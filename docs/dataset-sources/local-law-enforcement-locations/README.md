# Local Law Enforcement Locations source blocker

The producer is Oak Ridge National Laboratory (ORNL), Department of Energy: the
inventory row credits DOE/ORNL as publisher and office, the layer's own
copyright text credits "Oak Ridge National Laboratory (ORNL), National
Geospatial-Intelligence Agency (NGA) Homeland Security Infrastructure Program
(HSIP) Team", and the official DHS HIFLD deprecation crosswalk names
"Oak Ridge National Laboratory (ORNL)" as the layer's agency. The product is a
one-time 2017 fusion (inventory date_issued 2017-09-26; last HIFLD Open portal
modification 2021-02-03) of the DOJ-BJS Census of State and Local Law
Enforcement Agencies (CSLLEA) from 2008 and the 2009 HIFLD law enforcement
feature class, excluding federal agencies, covering all 50 states, Washington
D.C. and Puerto Rico. The final public snapshot's SOURCEDATE attribute dates
23,442 of 23,486 records to 2008-2009 and the remaining 44 to 2018 (per the
2026-10-18 HIFLD Next catalog audit recorded at
fulton-ring/agent-content-demos#40), so the content was stale even before the
portal's deprecation.

On 2026-11-12 the official DHS crosswalk row for "Local Law Enforcement
Locations" was verified first-hand by parsing the dated public copy of the
crosswalk workbook (atcoordinates.info, 61,689 bytes; the official GeoPlatform
item 41b4ff18bb094ba996dec75916de3bf3 now returns "Item does not exist or is
inaccessible" and is not re-fetchable). The row records: Status Migrated; New
Hosting Location "GII Portal"; DUA Required No; **GII Access Required Yes**;
Agency "Oak Ridge National Laboratory (ORNL)"; External Landing Page None; Open
REST Service
https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Local_Law_Enforcement_Locations/FeatureServer;
and "Migrated to GII due to the deprecation of HILFD Open".

Fresh probes on 2026-11-12 confirmed every historical public channel is dead:
that crosswalk Open REST FeatureServer returns 400 "Invalid URL"; the old
GeoPlatform item 0d79b978d71b4654bddb6ca0f4b7f830 returns CONT_0001 "Item does
not exist or is inaccessible"; gii.dhs.gov/HIFLD serves only an ArcGIS Hub
shell whose dataset/search APIs redirect to
hifld-geoplatform.hub.arcgis.com, where the slug filter returns empty data and
search returns CONT_0001; hifld-dhs-gii.opendata.arcgis.com returns 404
"Domain record(s) not found"; the GeoPlatform services directory (532
services) contains no law-enforcement service; and the data.gov catalog entry
MGMT-GMO-HIFLD-415472 points only to the dead HIFLD publisher. ORNL's own
channels publish no download: ornl.gov/projects/geospatial-data-sets lists
"Law enforcement facilities" among ORNL-developed layers but has no endpoint;
hdds.ornl.gov, gis.ornl.gov and geoscience.ornl.gov do not resolve; and an
ArcGIS Online search finds no ORNL-owned item for this layer.

Accessible copies are not eligible pipeline inputs: the DHS crosswalk itself
names the GII Portal (HIFLD Secure on the DHS Geospatial Information
Infrastructure) as the sole official continuation, and GII access for non-DHS
users requires a DHS-authorized, approved Login.gov account, a profile and a
Data Use Agreement (dhs.gov/gmo/hifld). Every ArcGIS copy found is owned by
someone other than the producing agency - Esri item
e0388a21673748108c05e97e3e8da4d2 (owner jpardue@esri.com_prof_services),
services9.arcgis.com item 2257d7f6cd114a7fa2ed953269f39cf8 (owner
crcdata@ou.ad3.ucdavis.edu_ucdavis), the FEMA RAPT item
333a74c8e9c64cb6870689d31e8836af (owner FEMA_HQ_Preparedness_NIC), and
assorted university/regional re-hosts such as the
sacog.org Structure/Law_Enforcement FeatureServer. DataLumos 239493
(DOI 10.3886/E239493V1), PASDA dataset 9124, the Data Rescue Project portal
and Placekey are archives/mirrors and remain excluded under project source
policy. The USGS National Structures law-enforcement/police-station points
(already represented by the courthouses-3 USGS Structures family) are
maintained but a materially different product - structure locations, not the
ORNL agency roster with CSLLEA identity, staffing and status fields.

No anonymously accessible, maintained, full-scope source of the ORNL Local Law
Enforcement Locations product exists. Until a maintainer either provides a GII
access grant or explicitly approves a materially different product as a
changed contract, there is no defensible endpoint to capture; no adapter,
substitute or archive refresh was implemented.

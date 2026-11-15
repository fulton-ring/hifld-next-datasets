# Nursing Homes source blocker

The producer is Oak Ridge National Laboratory (ORNL), Department of Energy: the
inventory row credits DOE/ORNL, the 2016/2024 FGDC metadata credits the ORNL
Geographic Information Science and Technology (GIST) Group with the NGA Homeland
Security Infrastructure Program (HSIP) Team, and the official DHS HIFLD
deprecation crosswalk names ORNL as the layer's agency. The last HIFLD Open
release is dated 2024-06-26 (metadata reports 53,129 point features; the
inventory's "7,170 records added" belongs to that cycle).

On 2026-11-12 the official DHS crosswalk row for "Nursing Homes" was verified
first-hand by parsing the dated public copy of the crosswalk workbook
(atcoordinates.info, 61,689 bytes; the official GeoPlatform item
41b4ff18bb094ba996dec75916de3bf3 now returns "Item does not exist or is
inaccessible" and is not re-fetchable). The row records: Status Migrated; New
Hosting Location "GII Portal"; DUA Required No; **GII Access Required Yes**;
Agency "Oak Ridge National Laboratory (ORNL)"; External Landing Page None;
Open REST Service
https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/NursingHomes2024/FeatureServer;
and "Migrated to GII due to the deprecation of HILFD Open".

Fresh probes on 2026-11-12 confirmed every historical public channel is dead:
that GeoPlatform FeatureServer returns 400 "Invalid URL"; the sibling
`NursingHomes` service returns "Item does not exist or is inaccessible"; the old
portal item 426ab97fbcec4f4d872340b4d8630170 is deleted on both
geoplatform.maps.arcgis.com and www.arcgis.com; the GeoPlatform services
directory (532 services) contains no nursing/health/care service; the HIFLD Open
hub domains (hifld-geoplatform.opendata.arcgis.com,
hifld-dhs-gii.opendata.arcgis.com) return "Domain record(s) not found";
gii.dhs.gov/HIFLD redirects to a hub shell whose dataset API returns
CONT_0001. ORNL's own channels publish no download
(ornl.gov/projects/geospatial-data-sets lists the layer as ORNL-developed but
has no endpoint; hdds.ornl.gov and gis.ornl.gov fail to connect).

Accessible copies are not eligible pipeline inputs: the DHS crosswalk itself
names the GII Portal (HIFLD Secure on the DHS Geospatial Information
Infrastructure) as the sole official continuation, and GII access for non-DHS
users requires a DHS-authorized, approved Login.gov account (dhs.gov/gmo/hifld).
The CMS Provider Information dataset (4pq5-n9py, HHS) is a maintained official
source but a materially different product - Medicare/Medicaid certified
facilities only, not the broader licensed/assisted-care inventory. The FEMA
RAPT "Nursing Homes RAPT" feature service (item 47c1893618ad48ac8d0cf199593b0c9a,
owner FEMA_HQ_Preparedness_NIC, created 2025-08-18 from a documented 2025-08-18
download of the HIFLD ORNL layer, modified 2026-01-16) is an ArcGIS item owned
by someone other than the producing agency. DataLumos 239713, HSDL, the Data
Rescue Project/HIFLD Next copies and PASDA mirrors are archives and remain
excluded under project source policy.

No anonymously accessible, maintained, full-scope source of the ORNL Nursing
Home / Assisted Care product exists. Until a maintainer either provides a GII
access grant or explicitly approves a materially different partial product as a
changed contract, there is no defensible endpoint to capture; no adapter,
substitute or archive refresh was implemented.

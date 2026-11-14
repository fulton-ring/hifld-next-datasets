# Supplemental Colleges

No maintained source: this card records the source audit only. No pipeline,
adapter, schedule or acceptance is proposed or implemented.

## Product identity

The HIFLD Open `Supplemental Colleges` feature class contains additional
postsecondary education features that are **not** included in the NCES
Integrated Postsecondary Education Data System (IPEDS), related to the
`Colleges and Universities` feature class by IPEDS ID (inventory row
`supplemental-colleges` in `HIFLD_Open_Inventory_12112025.csv`; final open
edition dated **2017-09-09**, archive pointer DataLumos 240659 modified
2022-12-07). The release notes state 48 added records, TYPE/STATUS updates on
2,747 records against the then-current IPEDS release, and 13 removals.

The producing organization is Oak Ridge National Laboratory's Geographic
Information Science and Technology (GIST) Group within the DHS HSIP Freedom
program: the only federal map service still distributing this content credits
itself to "Oak Ridge National Laboratory, Geographic Information Sciences and
Technology Group/HSIP Freedom Database" for every layer, and ORNL's program
page lists ORNL-developed HIFLD education layers (public/private schools,
colleges and universities, childcare). The inventory row's agency column says
"National Center for Education Statistics" with an empty publisher column; the
archive record lists NCES and DHS as principal investigators. NCES is the
IPEDS authority the layer relates to; the enriched national point compilation
itself is ORNL-produced. The DHS crosswalk DOI 10.3886/E241367V1 returned HTTP
403 challenge pages in prior anonymous checks (2026-10-20, issue #263
evidence), so the crosswalk row for this inventory entry remains unverified.

## Only accessible official copy: frozen 2015 EPA edition

The U.S. EPA Office of Environmental Information distributes the **2015 ORNL
Freedom edition** of the ORNL education compilation as the Supplemental
Colleges layer (ID 6) of its `OEI/ORNL_Education` SEGS MapServer:
`https://geodata.epa.gov/arcgis/rest/services/OEI/ORNL_Education/MapServer/6`.
Anonymous probes on 2026-11-14 returned:

- `esriGeometryPoint`, EPSG:4326, `maxRecordCount` 1000, capabilities
  Map/Query/Data, `supportsOrderBy`/`supportsPagination` true; layer has no
  `editingInfo`, so the service publishes no last-edit or release date.
- Count **1,221**; sparse OBJECTID roster of 1,221 unique ids spanning 1..1252
  (sorted-roster SHA-256 `110df3172c1d2d93fd00b87d4912c434de0cad0dd4add6e4165b31ccb80181d6`).
- 34 declared fields: OBJECTID (OID), SHAPE (geometry), UNIQUEID(25),
  IPEDSID(25), NAME(150), ADDRESS(100), ADDRESS2(150), CITY(30), STATE(5),
  ZIP(15), ZIP4(15), TELEPHONE(30), TYPE(15), STATUS(15), POPULATION
  (Integer), COUNTY(50), COUNTYFIPS(15), COUNTRY(5), LATITUDE (Double),
  LONGITUDE (Double), NAICS_CODE(15), NAICS_DESC(50), SOURCE(150),
  SOURCE_DATE (Date), VAL_METHOD(20), VAL_DATE (Date), WEBSITE(150),
  NCESNAME(100), BASENAME(50), ENROLL (Integer), ENROLLCALC(5),
  TOT_EMPLOYEES (SmallInteger), STFIPS(5), SHELTERID(30).
- Sample rows are branch/alternate campuses of IPEDS institutions (e.g.
  OBJECTID 1, UNIQUEID "1", IPEDSID "219879", "Cleveland State Community
  College - Athens Campus", TN).
- Service identity: mapName "Education, US, 2015, ORNL, SEGS", document title
  "Education, US, 2015, ORNL Freedom, SEGS", EPA copyright text crediting the
  ORNL GIST Group/HSIP Freedom Database; the harvested data.gov record is
  titled "Educational Institutions, US, 2015, ORNL, SEGS".

## Why no pipeline is built

Project policy (maintainer commit `dc56800a`, 2026-11-12, "Unregister
pipelines built on unmaintained historical releases") removes draft pipelines
for historical releases the publisher no longer maintains unless currently
published data can recreate them. The sibling `child-care-centers` draft on
this same frozen 2015 EPA ORNL service was explicitly unregistered there as
"child care centers (frozen 2015 ORNL on EPA)"; its module is retained for
reuse but is no longer a registry pattern to copy.

The 2015 EPA edition is older than the assigned 2017-09-09 HIFLD edition (48
added, 2,747 updated, 13 removed per the release notes) and cannot recreate
it. No maintained publication of the supplemental product was found:

- ORNL/DOE: the ORNL geospatial program page (HTTP 200 on 2026-11-14) lists
  the education layers as ORNL-developed but publishes no downloads; the
  family runs' dated negatives stand (OSTI records API and DOE Data Explorer:
  no supplemental-colleges match, #38 run 2026-11-08; no ORNL campus or
  supplemental distribution, #39 run 2026-11-09; no ORNL education downloads
  from EPA OEI services folder, #115/#39 evidence).
- NCES EDGE cannot supply this dataset by definition: the supplemental class
  holds postsecondary features **not** included in IPEDS, while the maintained
  NCES EDGE successor already registered for the sibling Colleges layer
  (issue #38, PR #602) covers IPEDS institutions.
- DHS: HIFLD Open is retired; HIFLD Secure (GII) is credential-gated with a
  data use agreement.
- HIFLD rescue archives (DataLumos 240659 DOI 10.3886/E240659V1, PASDA and
  similar mirrors) are ineligible pipeline inputs under project policy and are
  identification context only.

## Decision requested

Maintainer decision: park or retire this card, or supply an access grant
(HIFLD Secure GII+DUA, or an ORNL/DOE release of the supplemental product with
checksums), or explicitly accept a different-vintage official substitute
(e.g. the frozen 2015 EPA edition as a historical preservation source, as
scoped for the campuses card #39). Implementation stays out of scope until
then.

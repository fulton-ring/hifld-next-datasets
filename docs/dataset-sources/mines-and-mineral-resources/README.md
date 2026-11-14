# Mines and Mineral Resources — blocked: no pipeline-legal source

Status as of 2026-11-12: no ingest asset, catalog or publish flow exists for
`mines-and-mineral-resources`, and nothing is staged or published. Draft
implementation is blocked because no maintained public source from the
producer, an explicitly delegated official distributor, or an agency official
historical release was found. The blocked decision belongs to a maintainer.

## Producer and lineage

The HIFLD inventory row (`mines-and-mineral-resources`, archive date
2017-06-12 = rescue copy only) names agency "TechniGraphics Inc." and
publisher "Techni Graphic Systems, Inc." Both names refer to the same private
contractor (formerly Technicolor Graphic Services; abbreviated to
TechniGraphics, Inc. in 2005). The NCTCOG HIFLD overview deck
(http://gis.nctcog.org/ris/gis/presentations/HIFLD_HSIP.pdf) lists
"Techni Graphic Systems (TGS)" as the HSIP Gold 2010 data provider for
"Industrial PinPointer", the commercial industrial-facility compilation behind
this layer's haul-road point placement, upper-cased text and
`Mines_linked_to_ManufacturingID_table.dbf` linkage (`MINEID`/`MNFID` to
`SRCLNKID`); the HIFLD Open metadata records `CONTDATE` values only from
2006-06-06 to 2006-10-12 and states the data "is legacy and not expected to be
updated. It is being provided as the best available until Mineral Resources
identifies an alternative data source."

The producer has no public distribution channel. TechniGraphics was acquired
by CACI International on 2010-11-01 and CACI closed its Wooster, Ohio
operations on 2013-05-21. A bounded 2026-11-12 search found no public
download, service or archive published by TechniGraphics, CACI, or any
explicitly delegated distributor for this layer.

## Official channel probes (2026-11-12, anonymous)

- DHS HIFLD Open crosswalk, row "Mines and Mineral Resources" (read from the
  byte-identical public copy of the official DOI 10.3886/E241367V1 workbook at
  atcoordinates.info; sha256 `7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc`,
  read as research context only): Status "Deprecate"; justification "No longer
  supported as part of the HIFLD mission"; New Hosting Location N/A; DUA
  Required and GII Access Required blank; crosswalk Agency column "U.S.
  Geological Survey"; External Landing Page
  https://www.usgs.gov/publications/mineral-resources-data-system-mrds;
  Open ID `be32bfd235b34b16b241a5043e1de880`.
- HIFLD Open item `be32bfd235b34b16b241a5043e1de880`
  (https://www.arcgis.com/sharing/rest/content/items/...?f=json):
  `CONT_0001` "Item does not exist or is inaccessible" (deleted).
- HIFLD FeatureServer
  `https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Mines_and_Mineral_Resources/FeatureServer?f=json`:
  400 "Invalid URL" (service gone).
- data.gov harvest record `MGMT-GMO-HIFLD-295022` (publisher HIFLD/DHS,
  modified 2022-10-24): distribution list empty; catalog entry only.

## Why the known copies cannot be used

Every complete copy found is on a host the project source policy bans as a
mirror or archive, and none is a producer or delegated distribution:
DataLumos/ICPSR DOI 10.3886/E239598V1 (V1 published 2025-11-03; its public
catalog page lists `metadata.xml` 33.9 KB, `mines-and-mineral-resources-geojson.zip`
524.3 KB and `mines-and-mineral-resources-shapefile.zip` 446.8 KB; catalog
metadata only — nothing was fetched, direct access is Cloudflare-challenged
and the host is banned), the Data Rescue Project portal, PASDA, and Esri or
DHS HIFLD archive items. The crosswalk "Old Secure ID" is "Please see GII":
HIFLD Secure holds a credential-gated copy requiring a GII account and
approved Data Use Agreement, which is not an anonymous machine source.

## Successor candidates (not accepted; maintainer decision required)

- USGS Mineral Resources Data System (MRDS): the crosswalk-designated external
  landing page (https://www.usgs.gov/publications/mineral-resources-data-system-mrds,
  HTTP 200 on 2026-11-12) and live downloads at https://mrdata.usgs.gov/mrds/
  (HEAD probes 2026-11-12: `mrds-csv.zip` 25,791,223 bytes last-modified
  2022-08-23; `mrds-trim.zip` 11,236,725 bytes last-modified 2018-04-06;
  WMS/WFS services). USGS states systematic updates to MRDS ceased in 2011.
  It is a materially different product: worldwide mineral-deposit reports with
  `dep_id` identifiers, no TGS schema, no manufacturing-plant linkage.
- USGS USMIN Mineral Deposit Database (https://mrdata.usgs.gov/deposit/): a
  developing critical-minerals database published as per-commodity packages;
  not a general US mine inventory.

Adopting either would be a changed-scope successor requiring a new schema/ID
contract and maintainer approval, not a reconstruction of the inventoried
TechniGraphics layer. No substitute row IDs or geocoded replacement may be
generated.

## Decision required

A maintainer must choose one: (a) park or retire the dataset as unrecoverable
from pipeline-legal sources; (b) accept a named USGS successor product (MRDS
or USMIN) as a changed-scope replacement and approve its contract; or (c)
some other policy-level path. All known complete captures of the original
layer are on hosts the source policy forbids, so no preserved-capture option
exists inside the current policy. Until decided, this dataset stays without a
pipeline, and no mirror, archive or successor is wired in.

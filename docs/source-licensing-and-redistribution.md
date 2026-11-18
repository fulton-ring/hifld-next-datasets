# Source licensing and redistribution

Status: research record reviewed 2026-11-17. It is not legal advice, and it does
not accept any source, product or release. Terms change; re-check the cited
primary pages and pin the selected release, theme and record source before use.

The archived HIFLD Open collection carries the public-domain status label
`CC-PDM-1.0`, set from the project operator's confirmation
(`src/dagster_hifld/portolan/catalog.py:29`). That label covers only the archived
snapshot. A reconstruction that copies or derives from a separately licensed
source must declare that source's own license, attribution and obligations. It
must not inherit the archive label.

`src/dagster_hifld/portolan/inventory.py:421` assigns `CC-PDM-1.0` to every
record in the `hifld` collection. A licensed product placed in that collection
would therefore be mislabelled. Put such a product in its own collection with an
explicit `license_href` (`src/dagster_hifld/portolan/workflow.py:177` resolves
any supplied href to `other`) and its own stable version path.

## Source and use matrix

"Discovery" is finding a feature to check. "Matching" is comparing a candidate
record with an independently sourced record. Neither grants rights to copy
values. Copying coordinates, names, attributes or geometry is extraction from
the source database and is governed by its license.

| Source / product | Discovery and matching | Geocoding | Tracing and geometry | Caching and retention | Database derivation | Public GeoParquet download |
| --- | --- | --- | --- | --- | --- | --- |
| OpenStreetMap database (ODbL 1.0) | Allowed; copying OSM values still creates ODbL-derived material. A visual check followed by independent government-source verification is different from copying. | Use the geocoder row. | Copying OSM geometry is extraction, not independent tracing. | ODbL sets no retention window; OSMF service policies do. | A derivative database must be ODbL; share-alike applies to public use. | Allowed under ODbL with attribution, license copy/URI and share-alike for a substantial extract or derivative database. |
| OSM-based geocoder, e.g. Nominatim | Individual results are insubstantial extracts; attribution is required when publicly used. | Individual forward/reverse results may be stored, including beside third-party data. | Indirect hits contain no raw OSM data. | Nominatim allows an absolute maximum of 1 request/second and discourages bulk geocoding; scripts running longer than a day or on a schedule are limited to 4 requests/minute and bulk results must be cached. | Systematically assembling the whole or a substantial part of the OSM database creates a derivative database and triggers share-alike. | Not a route to a national feature database. Treat any bulk build as a separate rights assessment. |
| Overture Places (CDLA-Permissive-2.0 and Apache-2.0, per record source) | Allowed subject to the selected record's license. | Place coordinates and addresses are usable. | Point locations, not boundaries. | Permissive licenses set no retention window; Places carries no ODbL share-alike. | Permitted; keep notices and per-source provenance. | Allowed with per-source attribution and notices. Foursquare records are Apache-2.0 and need their NOTICE; AllThePlaces is CC0. |
| Overture Buildings, Base, Divisions, Transportation (theme ODbL) | Allowed; copied geometry or attributes are ODbL material. | Not applicable. | Theme geometry is ODbL. | ODbL sets no retention window. | Share-alike applies to a derivative database. | ODbL terms, including attribution and license notice. Theme sources add their own credits (for example CC BY 4.0 contributors). |
| Overture Addresses (per-source permissive terms) | Allowed subject to the selected country/source terms. | Usable. | Address points, not boundaries. | Source-specific. | Source-specific. | Per-source terms. Examples include the National Address Database license and the G-NAF end-user licence; not a single blanket license. |
| Google Maps imagery and Maps JavaScript API | In-application use only. | Not applicable. | Prohibited: no tracing or digitizing roads, building outlines, utility posts or electrical lines (standard terms 3.2.3(c)(i)). | No caching except as the service-specific terms allow. | No creating content from Google Maps Content (3.2.3(c)). | Not permitted. |
| Google Places API | In-application use; no use with a non-Google map. | Place coordinates are usable in the application. | Not applicable. | Latitude/longitude may be cached for up to 30 consecutive days (service terms 14.3). | No directory or listing service and no augmenting an advertising product (3.2.3(d)(iii)). | Not permitted. |
| Google Geocoding API | In-application use; no use with a non-Google map. | Yes, inside the application. | Not applicable. | Latitude/longitude cache up to 30 days; a narrow indefinite cache is allowed only for the initiating end user's direct application use, isolated per user and not as a replacement for a new call (6.3.2). | No bulk assembly of a feature database. | Not permitted. |
| GCCA cold-storage directory (proposed in #280) | First-party directory is a discovery route only. | Not applicable. | Not applicable. | Terms do not grant a bulk cache. | Limited noncommercial copying with notices. | Republication or other dissemination needs written permission. |

Public access or a paid subscription does not by itself grant public-download or
GeoParquet redistribution rights. Payment buys service access under the
provider's terms, not ownership of the content.

## Overture theme and record licenses

Overture is not one license. Pin the release and read the theme and record
source before copying anything.

- Places is published under CDLA-Permissive-2.0 and Apache-2.0 and, as of the
  October 2026 guide, contains no OpenStreetMap data and carries no ODbL
  share-alike. Sources include Meta, Microsoft, BrightQuery and others under
  CDLA-Permissive-2.0, Foursquare under Apache-2.0, and AllThePlaces under CC0.
- Buildings, Base, Divisions and Transportation currently identify ODbL at theme
  level, with additional CC BY 4.0 contributors. The Transportation theme also
  lists TomTom data.
- Addresses carries per-source permissive terms with country-specific
  conditions. It is not automatically compatible with an ODbL theme.
- Joining Places to OSM is permitted, but the Overture Places guide warns that
  the combined result may need ODbL if it is a derivative database.
- A cross-theme join inherits every input's obligations. Do not infer
  compatibility from Overture publishing the themes together.

## Candidate sources by dataset

These are candidate research routes from #36, #38, #39, #115, #251, #277, #280
and #329. None is an accepted source, and none changes a dataset's source of
record.

| Issue | Dataset | Candidate use | Additional requirement |
| --- | --- | --- | --- |
| #36 | Child Care Centers | OSM or Overture Places for discovery and matching against an independently acquired government roster. | The government roster stays the source of record. A POI match cannot establish licensed status or national coverage. |
| #38 | Colleges and Universities | Same discovery and matching role. | Same. |
| #115 | Hospitals | Same discovery and matching role. | Same. |
| #251 | Nursing Homes | Same discovery and matching role. | Same. |
| #39 | Colleges and Universities Campuses | Campus boundary or parcel authority is required. | An Overture building footprint or OSM fence is a different geometry and product. Do not trace Google imagery. |
| #277 | Prison Boundaries | Boundary or parcel authority is required. | Same; a building footprint is not the facility boundary. |
| #329 | Transmission Lines | Physical network geometry and attributes are required. | Google tracing is prohibited under standard terms. An OSM power-line extract would be an ODbL-licensed alternative product, not the ORNL/HIFLD line data. |
| #280 | Public Refrigerated Warehouses | GCCA directory is the candidate directory; Overture or OSM may locate a site. | GCCA republication needs written permission independently of whether Overture can locate the warehouse. |

## Commercial-origin HIFLD compilations

Reviewed 2026-11-17 for #967. This section covers the six inventories the issue
names. It is not legal advice and accepts no source, product, license or release.
No provider was contacted, no data was bought, no terms were accepted and no
banned mirror was fetched.

The archive label `CC-PDM-1.0` is the project operator's label for the archived
HIFLD Open snapshot. It does not establish that a contractor or association
supplied the underlying content on public-domain terms. The archive label and the
source's own terms are separate facts.

### Provenance and rights

Inventory dates below are from `HIFLD_Open_Inventory_12112025.csv`. They are
inventory metadata dates, not proof of a current release; the archive
modification date is separate. Prior issue evidence is reused for endpoint
probes; those probes were not repeated against banned mirrors.

| Issue / inventory | Origin and edition | Rights evidence and holder status | Surviving original release | Verdict |
| --- | --- | --- | --- | --- |
| #155 Mines and Mineral Resources | TechniGraphics/TGS Industrial PinPointer compilation. The HSIP Gold 2010 commodity provider list names TGS Industrial PinPointer. The layer metadata records contact dates from 2006-06-06 to 2006-10-12 and calls the data legacy. Inventory row: publisher Techni Graphic Systems, Inc., agency TechniGraphics Inc., inventory date 2017-06-12. | CACI acquired 100 percent of TechniGraphics on 2010-11-01 (CACI 10-K, Note 4, year ended 2011-06-30). No public TGS/CACI product page, license, contract or transfer/sublicensing terms were found. The current rights-holder is not established beyond that acquisition lead. | HIFLD Open item and service are deleted (2026-11-12 probes). The DataLumos copy is a banned mirror; the GII copy is credential-gated. No pipeline-legal original release. | Rights unresolved. Do not copy TGS geometry or identifiers. This is a procurement/contract question, not a pipeline task. |
| #280 Public Refrigerated Warehouses | GCCA directory. The current `Warehouse Facility` category is a changed successor, not the 2017 layer. The existing candidate captures 1,420 global and 778 U.S. records (2026-10-21 evidence); this pass did not recapture it. Inventory row: publisher and agency GCCA, inventory date 2017-06-08. | GCCA terms read 2026-11-17: GCCA, GCCF and CEBA claim copyright in published materials. Copying or dissemination is allowed only when it is not for commercial advantage, the copyright and its date appear, and a permission notice appears. "To disseminate otherwise, or to republish, requires written permission." No bulk, API or GeoParquet grant. Member-supplied content and coordinate rights are unresolved. | The current public directory is live. The historical 2017 archive is inaccessible (403) and on a banned host. | Written permission required. Rights unresolved. The existing candidate keeps `license` as "Reuse terms not accepted" and every rights flag false (`src/dagster_hifld/sources/gcca_warehouse_directory.py:425`, `:446`). |
| #159 Mobile Home Parks | ORNL HIFLD layer. ORNL's geospatial page lists mobile home parks among layers developed by ORNL. Not a commercial compilation; no commercial rights-holder identified. Inventory row: publisher ORNL, agency DOE, inventory date 2018-09-17. | No ORNL or DOE license was found. The dated DHS crosswalk read says GII Access Required Yes and DUA Required No; the GII/DUA instrument is the access route. | No public producer feed. The only continuation is credential-gated GII; mirrors are banned. | Access blocker, not commercial licensing. The GII/DUA redistribution terms remain unread. |
| #322 Supplemental Colleges | ORNL GIST/HSIP Freedom education compilation, not an NCES product. Final HIFLD Open edition 2017-09-09. The only accessible official copy is the frozen 2015 EPA `ORNL_Education` layer 6 (1,221 points, 34 fields), which cannot recreate the 2017 edition. Inventory row: publisher blank, agency NCES, inventory date 2017-09-09. | No commercial ownership established. NCES IPEDS is federal data but excludes these features by definition. No license was found for the ORNL/HSIP enrichment. HIFLD Secure (GII) is the credential-gated continuation; its terms are unread. | No maintained publication. The frozen 2015 EPA copy is historical; mirrors are banned. | Source/access blocker, not a commercial rights blocker. Rights are unread. |
| #330 Trauma Levels | ORNL Hospitals-family layer. The inventory description matches the hospitals layer; the DHS crosswalk has no separate Trauma Levels row. Not established as commercial. Inventory row: publisher ORNL, agency DOE, inventory date 2024-07-03. | No ORNL or DOE license was found. The Hospitals-family crosswalk row says GII Access Required Yes and DUA Required No. | No public producer feed. The only continuation is credential-gated GII. | Access blocker, not commercial licensing. Rights and DUA terms are unread. |
| #332 Truck Driving Schools | TechniGraphics/TGS. The DHS crosswalk marks the layer Deprecate with no successor and no delegation. Inventory row: publisher Techni Graphic Systems, Inc., agency TechniGraphics Inc., inventory date 2017-06-07. | Same TGS/CACI chain-of-title gap as #155: CACI acquired TechniGraphics on 2010-11-01, and no contract or license was found. Do not assume #155 and #332 rights are identical. | HIFLD Open service and item are deleted (2026-11-17 probes). The crosswalk points to GII ("Please see GII"); mirrors are banned. | Rights unresolved. The card still waits on a GII, FMCSA-product or park/retire decision. |

### Fields and geometry

This maps what a licensed or association source controls against what a
government or open source can independently supply. "Rebuildable" does not mean
the result is the same product; scope, identifiers and enrichment still differ.

| Issue | Rights-sensitive content | Independently rebuildable from government or open sources | Unrecoverable |
| --- | --- | --- | --- |
| #155 | TGS point placement and coordinates, normalized names and addresses, contact and geolocation provenance (CONTDATE, CONTHOW, GEOPREC, GEODATE), MINEID/SRCLNKID manufacturing linkage, selection and normalization. | Mine and deposit names, coordinates, commodities, operator and regulator identifiers from named USGS, MSHA and state records, each with its own terms and currentness check. | TGS identifiers, selection, normalization, manufacturing links, enriched location accuracy and 2006 contact dates. |
| #280 | GCCA directory identifiers and parentid, membership and category, company descriptions, certification, enriched coordinates and directory inclusion criteria. | Facility names, addresses and regulator identifiers from state or local license or inspection rosters where one exists. Coordinates only from a separately licensed source. | GCCA membership, directory identifiers and parent relationships, categorization, historical 2017 inclusion criteria and a national warehouse census. |
| #159 | ORNL MHPID, scope and selection, status and open/closed fields, units and size, VAL_METHOD and VAL_DATE, positional enrichment, source and imagery fields. | Licensed-park names, addresses and license identifiers from state licensing records. RV-park inclusion is a separate question. Geocoding needs separate rights. | ORNL scope, identifiers, positional enrichment and historical completeness. |
| #322 | ORNL UNIQUEID, the non-IPEDS selection, enrichment fields (POPULATION, ENROLL, TOT_EMPLOYEES, VAL_METHOD, VAL_DATE, SHELTERID) and positional data. | IPEDS institution names, addresses and identifiers from NCES/IPEDS. Non-IPEDS names, addresses and authorization identifiers from state education licensing or approval rosters where published. | Non-IPEDS coverage, historical ORNL enrichment and the exact 2017 edition. |
| #330 | The ORNL hospital universe, identifiers and points, plus the trauma-level assignment and its enrichment. | Facility, trauma level, effective date and official identifier from state health department designation rosters. Geocoding needs separate rights. | The ORNL hospitals universe, historical identifiers and points, and assignment provenance. |
| #332 | TGS names, addresses, coordinates, identifiers, contact dates and selection. | FMCSA's ELDT Training Provider Registry is a different population; a bulk feed and export rights are not established. State school licensing rosters are research candidates. | The TGS historical universe, identifiers, contact and geolocation enrichment, and equivalence to all historical truck schools. |

### Questions for a human procurement or outreach decision

These are questions to authorize, not actions taken. A named maintainer product
decision is required before any contact or purchase.

1. TGS/CACI and the commissioning agency (#155, #332). Ask the original
   commissioning agency's data-contract custodian and CACI, as the claimed
   successor, to identify the exact product and edition, the chain of title, the
   government license and data-rights clauses, third-party exclusions, and
   whether an official original release can be provided. Ask whether the agency
   can authorize public full-feature redistribution. Ask separately for Mines and
   for Truck Driving Schools. Ask whether the HIFLD Open public-domain label was
   authorized for the TGS content or only for the compilation.
2. GCCA (#280). Ask which agreement governs the directory and API, and who
   controls member content and coordinates. Ask whether automated full-category
   capture, raw retention, derived geometry and joins, and a perpetual anonymous
   GeoParquet download with downstream commercial reuse are permitted; what
   notices are required; what renewal, revocation and deletion limits apply; and
   whether native identifiers and parent links may be published. Ask separately
   about the historical 2017 compilation.
3. ORNL/DOE/DHS GII (#159, #322, #330). Ask for item- and edition-specific
   constituent source and rights records through the producer or GII route. Ask
   whether the GII/DUA permits public redistribution of derived GeoParquet, not
   only access, and whether ORNL asserts copyright in the compilation or its
   enrichment. For #322, ask whether an official release of the 2017 supplemental
   product exists.
4. FMCSA and state agencies (#159, #322, #330, #332). Ask whether the FMCSA
   Training Provider Registry has a bulk feed, a public export and redistribution
   terms. Ask state agencies for the terms that cover their rosters. These are
   research routes, not accepted substitutes.

### What this record resolves for the linked cards

- #155: it does not resolve rights. It supports the existing park/retire record
  on source availability, but the TGS contract and current rights-holder remain
  unknown. The issue is already closed not planned; do not reopen it from this
  record.
- #280: it does not grant rights. GCCA written permission is still required, and
  the current candidate stays unapproved with every rights flag false.
- #159: it is not a commercial compilation. The blocker is a GII access grant and
  the unread DUA terms, not a commercial license.
- #322: it is not established as commercial. The blocker is the missing
  maintained source and access, not a commercial license.
- #330: it is not established as commercial. The blocker is GII access and the
  unread DUA terms, not a commercial license.
- #332: it leaves TGS rights unresolved. The source record is merged, and the
  card still waits on the human GII, FMCSA-product or park/retire decision.

### Boundaries

No source or product is accepted. No proprietary geometry or identifiers were
copied. The archived `CC-PDM-1.0` label is not treated as a grant for TGS, GCCA or
ORNL content. Every runtime approval flag stays false and every release schedule
stays stopped.

## Manifest and provenance fields to reuse

Use the existing fields rather than adding a new licensing mechanism:

- `source_manifest` / data dictionary metadata keys: `license`, `attribution`,
  `publisher`, `source_url`, `inputs`, `transformations`, `lineage`
  (`src/dagster_hifld/catalog.py:288`).
- Per-input receipts already used by adapters, for example `license` and
  `attribution` in the Amtrak and aviation source records
  (`src/dagster_hifld/amtrak_extract.py:611`,
  `src/dagster_hifld/aviation_extract.py:576`).
- Portolan catalog `license_id` and `license_href`
  (`src/dagster_hifld/portolan/workflow.py:177`).

For every licensed input, record the publisher, pinned release or theme, record
source, copied fields, whether geometry was copied or derived, and whether
matching results are retained. Keep the source's native identifiers and license
text. An attribution string is evidence, not a rights grant.

## Remaining decisions

- A release-specific record-license audit for each selected Overture theme and
  release has not been done.
- No national pilot has tested matching quality or coverage against a government
  roster.
- No Google contract determination has been made for any proposed use.
- GCCA written permission for directory republication has not been requested.
- The TGS/CACI chain of title and the original government contracts for #155 and
  #332 have not been located.
- The ORNL/DOE GII and DUA redistribution terms for #159, #322 and #330 have not
  been read.
- No FMCSA Training Provider Registry bulk feed or export-rights determination
  has been made.
- ODbL mixture and aggregation questions for any combined product need rights
  review.
- Every runtime approval flag stays false and every release schedule stays
  stopped.

## Primary sources

Accessed 2026-11-17 unless noted:

- OpenStreetMap copyright and license: https://www.openstreetmap.org/copyright
- Open Database License 1.0: https://opendatacommons.org/licenses/odbl/1-0/
- OSMF geocoding guideline: https://osmfoundation.org/wiki/Licence/Community_Guidelines/Geocoding_-_Guideline
- Nominatim usage policy: https://operations.osmfoundation.org/policies/nominatim/
- Overture attribution and theme licenses: https://docs.overturemaps.org/attribution/
- Overture Places guide (October 2026 sources and licenses): https://docs.overturemaps.org/guides/places/
- CDLA Permissive 2.0: https://cdla.dev/permissive-2-0/
- Google Maps Platform terms, sections 3.1-3.2: https://cloud.google.com/maps-platform/terms
- Google Maps Platform service-specific terms, sections 6.3 and 14.3: https://cloud.google.com/maps-platform/terms/maps-service-terms
- GCCA terms and conditions: https://www.gcca.org/terms-conditions/
- GCCA directory: https://www.gcca.org/directory/
- CACI 10-K Note 4, acquisitions (TechniGraphics acquired 2010-11-01): https://www.sec.gov/Archives/edgar/data/16058/000119312511234902/R11.htm
- HSIP Gold 2010 provider list, NCTCOG briefing (2026-11-12 evidence): http://gis.nctcog.org/ris/gis/presentations/HIFLD_HSIP.pdf
- USGS Mineral Resources Data System: https://www.usgs.gov/publications/mineral-resources-data-system-mrds
- USGS MRDS current status and downloads: https://mrdata.usgs.gov/mrds/
- USGS 2021 California MRDS enhancement: https://www.usgs.gov/data/update-mineral-resources-data-system-california-including-mineral-deposit-types
- ORNL geospatial data sets: https://www.ornl.gov/project/geospatial-data-sets
- FMCSA Entry-Level Driver Training and Training Provider Registry: https://www.fmcsa.dot.gov/registration/commercial-drivers-license/entry-level-driver-training-eldt
- NCES IPEDS institutional groupings: https://nces.ed.gov/IPEDS/use-the-data/institutional-groupings-in-ipeds

Dated issue evidence reused for endpoint probes and crosswalk reads, not repeated
here: #155 comment 6825144813, #159 comment 6825826450, #322 comment 6872101827,
#330 comment 6872655556, #332 comment 6873048598 and the merged #332 source record
(`docs/dataset-sources/truck-driving-schools/README.md`).

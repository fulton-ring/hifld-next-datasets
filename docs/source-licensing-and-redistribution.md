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

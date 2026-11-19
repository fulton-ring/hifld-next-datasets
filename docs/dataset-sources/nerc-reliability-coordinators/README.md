# NERC Reliability Coordinators source evidence

Research-only outcome: no ingest asset, schema contract or pipeline exists yet,
because no acceptable, anonymously fetchable source was verified. This file
records what was checked and the concrete next requirement.

## Inventory row and crosswalk match

Inventory `nerc-reliability-coordinators-1` ("NERC Reliability Coordinators",
agency Department of Homeland Security / Homeland Security Infrastructure
Program, issued 2017-09-19): a polygon layer of the areas served by NERC
Reliability Coordinators. The DHS HIFLD Open crosswalk
([10.3886/E241367V1](https://doi.org/10.3886/E241367V1)) matches this row
exactly. The official crosswalk copy lives on DataLumos, which the source
policy excludes, so a preserved third-party copy of the same GeoPlatform
spreadsheet was used for research
([atcoordinates.info copy](https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx),
61,689 bytes, SHA-256 `7f3394fe…dc38772bc`, last-modified 2025-08-08; byte
identity with the official document is not established). Its Crosswalk sheet
(254 Deprecate, 51 Migrated rows) says for this row:

- Status **Migrated** — "Migrated to GII due to the deprecation of HILFD Open".
- New Hosting Location **GII Portal**; GII Access Required **Yes**; DUA
  Required **No**.
- Agency **Oak Ridge National Laboratory (ORNL)** — consistent with the HSIP
  producing-team attribution on the layer's published metadata; NERC is the
  subject-matter producer of the factual RC roster and areas.
- Old hosting: GeoPlatform/HIFLD Open item `ee47fbb73fc04d609510f340e1c1952c`
  with REST service `NERC_Reliability_Coordinators` — both now gone; new IDs
  say "Please see GII".

## Why there is no pipeline today

Probed 2026-11-12, anonymous TLS fetches (full receipts in `source.json`):

1. **The official successor requires access the factory does not have.** DHS
   migrated this layer to the GII Portal (HIFLD Secure). The
   [DHS HIFLD program page](https://www.dhs.gov/gmo/hifld) documents only
   HIFLD Secure access: a GII account and profile (DHS PIV/CAC, or a
   DHS-authorized Login.gov account for non-DHS users). Anonymous probes of
   `gii.dhs.gov` return the portal SPA and a Web Adaptor error page, with no
   anonymous REST listing or data access.
2. **The former official public channel is deleted.** The old GeoPlatform
   FeatureServer returns `400 Invalid URL`; the GeoPlatform org directory
   (532 services) has no NERC service; the old open item returns
   `CONT_0001 Item does not exist or is inaccessible`.
3. **NERC, the subject-matter producer, publishes no vector boundaries.** Its
   current Reliability Coordinators page is a Code/Name table only; a full
   sitemap enumeration (3,356 URLs) shows no GIS downloads. No ArcGIS item is
   owned by NERC (`owner:"NERC"` search returns 0 items).
4. **Every surviving copy is an excluded substitute.** DataLumos/ICPSR
   project 239751 (rescue archive, excluded host), PASDA HIFLD_FEMA layer 104
   (banned host, never fetched), NASA NCCS May-2019 frozen HIFLD rescue, the
   ORNL OpenEnergyDataPortal metadata shell (0 records, no attachments,
   creator `HostedByHIFLD`), the Data Rescue Project index, and third-party
   ArcGIS items owned by Ali.Rebollo (2021), HSEMA_PPD (2023), an Oregon DEQ
   account (2025 subset) and WECC SPDP (owner `chelsandrews`, self-described
   "approximate boundaries", 2025-03-26). None is owned by the producing
   agency and none carries NERC delegation.
5. **Catalogs carry no payload.** The data.gov CKAN search API is retired and
   the surviving catalog record (2017) lists no resources.

The former layer's published schema (OBJECTID, ID, NAME, ADDRESS, CITY,
STATE, ZIP, TELEPHONE, COUNTRY, SOURCE, SOURCEDATE, VAL_METHOD, VAL_DATE,
WEBSITE, ABBRV; polygon, EPSG:4326) is recorded in `source.json` as research
context only — no mirror payload was captured, so no schema or ID contract is
proposed and nothing is staged.

## Next requirement

Either a DHS GII access grant (non-DHS route: DHS-authorized Login.gov
account plus GII account and profile) so the migrated item's actual payload
can be inspected and a faithful source adapter drafted, or an explicit
maintainer decision on a substitute source product — none identified here is
acceptable under the current source policy. All acceptance flags remain
false; no schedule, publication or release is implied.

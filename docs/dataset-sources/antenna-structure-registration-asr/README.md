# FCC Antenna Structure Registration (ASR): maintained official source identified

Producer: Federal Communications Commission (FCC), Wireless Telecommunications Bureau — the
Antenna Structure Registration program within the ULS public-access program. The FCC publishes
the ASR tower public-access database files weekly (and as six-day daily transactions) at
`https://data.fcc.gov/download/pub/uls/complete/`, the same official directory family as the
other ULS weekly public-access files (`index.html` lists `a_tower.zip`, `d_tower.zip`,
`r_tower.zip` alongside the ULS service files). The FCC's own documentation is at
`https://www.fcc.gov/wireless/data/public-access-files-database-downloads` (ASR section:
introduction, database field definitions, `asr_sql_table_definitions_3.txt`, code definitions)
with the tower-table field layouts in `https://www.fcc.gov/sites/default/files/pubacc_vs_sql_tbl_def.txt`
and `https://www.fcc.gov/sites/default/files/patower-4.pdf` (RA registration/application, CO
coordinates, EN entity, HS history, RE remarks, SC special condition, DE FAA determinations).

## Dated access evidence (2026-11-18, anonymous, current runtime)

- `https://data.fcc.gov/download/pub/uls/complete/index.html` returned HTTP 200 listing
  `a_tower.zip`, `d_tower.zip`, `r_tower.zip`, each dated 2026-11-15.
- `r_tower.zip` (weekly registration tables): 37,937,572 bytes, SHA-256
  `51ff4fd54169bb7525b52b30422d856296033656e0dded246db810fa445b3243`; members `counts`, `CO.dat`,
  `EN.dat`, `HS.dat`, `RA.dat`, `RE.dat`, `SC.dat`; `counts` records "File Creation Date:
  Sun Nov 15 06:19:55 EST 2026" with per-member record counts (RA 198,069; CO 203,304;
  EN 374,343; HS 325,165; RE 23,483; SC 4,299).
- The superseded 2026-11-12 probe captured the 2026-11-08 edition: `r_tower.zip` 37,877,036
  bytes, SHA-256 `fe21a1ac2ac9b168172a17dd8bf21560727c1b833305092aec8bfc7598cc4ab5`, counts
  "File Creation Date: Sun Nov  8 06:21:02 EST 2026" (RA 198,010; CO 203,245; EN 374,223),
  and `a_tower.zip` 209,500,055 bytes.
- The superseded October probes: the prior runtime's fresh anonymous GETs on 2026-10-19
  returned origin HTTP 403 Akamai Access Denied (see
  [blocker comment](https://github.com/fulton-ring/hifld-next-datasets/issues/18#issuecomment-6260784882),
  [evidence replay](https://github.com/fulton-ring/hifld-next-datasets/issues/18#issuecomment-6261396324),
  [independent blocker review](https://github.com/fulton-ring/hifld-next-datasets/issues/18#issuecomment-6261880063)),
  and today the retired path family `https://data.fcc.gov/download/pub/antenna-structure-registration/`
  returns HTTP 302 to a generic FCC page. The maintained distribution lives under
  `pub/uls/complete/`, where 2026-11-12 anonymous requests succeeded (HTTP 200/206) from the
  current runtime; the October 403s were an access artifact of that environment, not an
  absence of an official feed.

## Observed weekly registry facts (2026-11-15 edition, from `r_tower.zip` bytes)

- `RA.dat` (49 pipe-delimited fields per FCC's tower table layout): 198,069 registration
  records; `registration_number` and `unique_system_identifier` are each distinct across all
  rows; status codes C 142,026 / G 18,441 / A 15,925 / I 12,753 / T 8,911 / empty 13;
  archive flags C 160,716 / A 37,352 / empty 1.
- `CO.dat` (18 fields): 203,304 coordinate rows — 198,068 tower (`T`) rows (one per coordinate
  registration) and 5,236 array (`A`) rows for 5,236 multi-point array registrations. Every CO
  row joins to RA on (`registration_number`, `unique_system_identifier`); coordinate type is
  part of the published row key, not a physical structure ID. Only 354 `A` rows carry
  `array_tower_position`; 4,882 do not. `array_total_tower` is preserved as a field and
  does not establish that the file supplies one coordinate per array member. Exactly one RA
  registration has no CO row; 124 CO rows carry empty or unparsable coordinate components.
  Latitude directions N 203,161 / S 23 / empty 120; longitude W 203,082 / E 102 / empty 120;
  DMS-decimal points span latitude -15.15 to 71.310361 and longitude -176.705472 to 174.138861
  (including Caribbean and Pacific territories). ASR coordinates are NAD83 (the FCC ASR search
  help states "Based on NAD83"), i.e. EPSG:4269, matching the FCC-required ULS datum policy.
- `EN.dat` (25 fields): 374,343 entity rows carrying registrant/owner names per
  (`registration_number`, `unique_system_identifier`).

## Proposed draft contract (unapproved)

Publish one row per (`registration_number`, `unique_system_identifier`, CO coordinate row) —
`T` tower-coordinate and `A` array-coordinate records as separate points when mappable —
preserving the declared RA, CO, and joined EN fields verbatim (including empty values, leading
zeros and the legacy `char` padding), plus the DMS→decimal derived `latdec`/`londec` values and
WKB Point geometry in the native NAD83 datum (EPSG:4269), with no transformation, repair, or
substitute IDs. Draft rows without parsable coordinates are retained with null geometry and
counted, matching the established ULS-family pattern. All approval flags (source release,
freshness, schema/ID/coverage contracts, rights, baseline, mapping, release) remain false, and
the refresh schedule stays stopped until maintainers accept the contract.

The maintained feed's population (198,010 registrations) is materially larger than the frozen
HIFLD v1.0.0 published candidate (17,666 rows), which is retained only as unapproved
compatibility evidence (data SHA-256 `fc43f476d4181538ad139d0e6504122cf4f258e0dd1d4817981377c62b73258d`;
its 11-distinct-value `id`, 17,655 duplicate rows, non-direction direction/url values, and
declared-coordinate/geometry disagreement stay recorded in
[issue #18](https://github.com/fulton-ring/hifld-next-datasets/issues/18)); no positional
mapping, replacement IDs, or geometry repair is accepted. Publishing the current registry
instead of the frozen snapshot is a compatibility decision for maintainers, not a draft
implementation blocker. The historical research archive and candidate archives referenced in
issue #18 remain the preserved evidence of the earlier blocker round; do not treat the
historical FCC-associated GIS service, a different Socrata population, or any archive as the
current official feed.

## Status

Draft pipeline implemented at the source-adapter level: `src/dagster_hifld/sources/antenna_structure_registration_asr.py`
captures and validates the complete weekly `r_tower.zip`, publishes one GeoParquet candidate row per CO
coordinate row (verbatim `ra_`/`co_`/`en_`-prefixed declared fields plus derived `latdec`/`londec` and WKB
Point geometry in EPSG:4269), and registers the stopped-refresh ingest asset, blocking completeness check,
draft job and schedule through `src/dagster_hifld/assets/antenna_structure_registration_asr.py`. The joined
EN fields come from the registration's unique EN owner (`entity_type` O) row; the 15 published rows whose
registration carries no EN owner row keep null joined EN fields, and the 124 coordinate rows without
parsable degrees and directions keep null geometry, matching the proposed contract above. This file records
the source, evidence, and proposed contract for that work. No source, baseline, schema/ID/coverage/rights,
release, merge, or production approval is granted or implied.

## Entity and completeness audit (2026-11-15, issue #955)

This is a code and existing-evidence audit, not a new FCC acquisition or a new
weekly edition comparison. Counts above refer to the successfully acquired
2026-11-08 edition documented on 2026-11-12, superseding the older environment's
403/no-source conclusion. A subsequent access failure alone cannot invalidate
that acquisition. FCC identifies the official files and field definitions on its
[public-access downloads page](https://www.fcc.gov/wireless/data/public-access-files-database-downloads)
and [daily/weekly page](https://www.fcc.gov/uls/transactions/daily-weekly).

The following describes the actual draft adapters. All inputs are published by
FCC WTB in the [official weekly directory](https://data.fcc.gov/download/pub/uls/complete/).
The weekly cadence describes the producer's distribution, not acceptance of a
new edition: the ASR adapter witnesses record population against the payload's
own counts member at capture time (its retired edition pins were snapshot
constants, and the counts member file creation date is the observed edition
marker, never an equality gate), while the other ULS adapters in this table
retain their pinned population/identity checks until their follow-up issues land.
`source_population_complete` means completeness of the declared source file and
selection below; it makes no claim about all physical structures, current
operation, or equivalence to a historical HIFLD release.

| Family / input | Candidate row and native identity | Selection and cardinality |
| --- | --- | --- |
| Cellular / `l_cell.zip` | LO license-location; (`UniqSysID`, `LocNum`) jointly unique in the pinned edition | HD status `A`, service `CL`; 24,362 LO rows from 5,473 total HD licenses (3,870 active). |
| BRS/EBS / `l_mdsitfs.zip` | LO license-location; (`UniqSysID`, `LocNum`) jointly unique | HD status `A`, services `BR`, `ED`; 4,669 LO rows. Geographic authorizations do not require a complete transmitter census in LO. |
| Microwave / `l_micro.zip` | LO license-location; (`UniqSysID`, `LocNum`) retained, including duplicate keys | HD status `A`, complete file service roster; 422,808 LO rows, including 20 extra rows across 16 duplicate keys. Null and anomalous coordinates remain as documented in its existing README. |
| Paging / `l_paging.zip` | LO license-location; (`UniqSysID`, `LocNum`) jointly unique in the pinned edition | HD status `A`; 6,389 LO rows. File services `CA CB CD CG CO CR`; geographic-area codes `CP CZ GC` are outside this file, so this is not all paging infrastructure. |
| Land Mobile Commercial / `l_LMcomm.zip` | LO license-location; (`UniqSysID`, `LocNum`) jointly unique | HD status `A`, complete file service roster; no structure-type filter. Missing coordinates remain null; documented local-datum coordinates are retained without falsely assigning NAD83 geometry. |
| Land Mobile Private / `l_LMpriv.zip` | LO license-location; (`UniqSysID`, `LocNum`) retained, not unique | HD status `A`, both degree components present, excluding local-datum `MP`/`UM` filings from candidate points. 603,775 rows, 27 duplicate keys; 322,063 active rows without a coordinate pair remain in raw capture, 15 local-datum rows also preserved in audit. |
| Land Mobile Broadcast Auxiliary / `l_LMbcast.zip` | LO fixed-location record; native `unique_system_identifier` + `location_number` retained without an invented row or structure ID | LO type `F`, services `LP RP LV`, **all** HD statuses; only mappable coordinates enter candidate points. Missing/invalid coordinates are counted and retained raw. This adapter does not assert uniqueness of the native location pair. |
| ASR / `r_tower.zip` | CO registration-coordinate; (`ra_registration_number`, `ra_unique_system_identifier`, `co_coordinate_type`) jointly unique, duplicate keys fail closed | All RA status and archive codes; record population completeness witnessed against the payload's own counts member at capture time. 2026-11-15 edition observed: 198,069 RA registrations, 203,304 CO rows (198,068 `T`, 5,236 `A`), one RA without CO has no candidate row, 124 CO rows keep null geometry. No expansion by array total or position. |

ULS joins HD and the unique EN licensee (`L`) on the native license system ID;
one license may have multiple LO rows. ASR joins CO to RA on registration number
and system ID and joins only the EN owner (`O`) into candidate columns; all other
EN rows remain in raw staging. Fifteen candidate CO rows have no owner and keep
null EN values. ASR registration counts, coordinate counts and array totals are
different measures. Neither license identity, callsign, registration number nor
a shared coordinate establishes one unique physical tower across these families.
The legacy tower/transmitter dataset names remain stable release-path labels.

A separate physical-structure product would need an explicit scope (including
unregistered structures and non-tower supports), an accepted mapping from native
license/site/registration IDs to structures, cross-service co-location evidence,
array-member evidence, temporal/status rules, and measured matching uncertainty.
`TowReg` can support a source-preserving ASR linkage where populated and verified;
it is not a universal structure key. Coordinate equality, proximity and matching
names alone do not justify merging records. This audit generates no structure
IDs, deduplicates nothing, changes no row filters or schema, and repairs no source
coordinates. Existing raw captures, native types and anomalies remain intact.

Generated source descriptions (also used by catalog metadata) now state these
entity/completeness limits; ASR descriptions distinguish `T` and `A`, and broadcast
metadata describes its actual coordinate exclusion instead of claiming null-point
retention. Remaining comparisons include current-week versus pinned editions and
accepted historical-baseline/schema mappings. Rights, coverage and release
acceptance remain pending; approval flags stay false and schedules stopped.

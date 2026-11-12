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

## Dated access evidence (2026-11-12, anonymous, current runtime)

- `https://data.fcc.gov/download/pub/uls/complete/index.html` returned HTTP 200 listing
  `a_tower.zip`, `d_tower.zip`, `r_tower.zip`, each dated 2026-11-08.
- `r_tower.zip` (weekly registration tables): 37,877,036 bytes, SHA-256
  `fe21a1ac2ac9b168172a17dd8bf21560727c1b833305092aec8bfc7598cc4ab5`; members `counts`, `CO.dat`,
  `EN.dat`, `HS.dat`, `RA.dat`, `RE.dat`, `SC.dat`; `counts` records "File Creation Date:
  Sun Nov  8 06:21:02 EST 2026" with per-member record counts (RA 198,010; CO 203,245;
  EN 374,223; HS 325,193; RE 23,483; SC 4,297).
- `a_tower.zip` (weekly application tables): 209,500,055 bytes, SHA-256
  `bcbc5fa4b32b0d549536bebc42d099053458aa9dc5345c2f8c85b1415f083b47`.
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

## Observed weekly registry facts (2026-11-08 edition, from `r_tower.zip` bytes)

- `RA.dat` (49 pipe-delimited fields per FCC's tower table layout): 198,010 registration
  records; `registration_number` and `unique_system_identifier` are each distinct across all
  rows; status codes C 141,986 / G 18,447 / A 15,913 / I 12,740 / T 8,911 / empty 13;
  archive flags C 160,682 / A 37,327 / empty 1.
- `CO.dat` (18 fields): 203,245 coordinate rows — 198,009 tower (`T`) rows (one per coordinate
  registration) and 5,236 array (`A`) rows for 5,236 multi-point array registrations. Every CO
  row joins to RA on (`registration_number`, `unique_system_identifier`); exactly one RA
  registration has no CO row; 124 CO rows carry empty or unparsable coordinate components.
  Latitude directions N 203,102 / S 23 / empty 120; longitude W 203,023 / E 102 / empty 120;
  DMS-decimal points span latitude -15.15 to 71.310361 and longitude -176.705472 to 174.138861
  (including Caribbean and Pacific territories). ASR coordinates are NAD83 (the FCC ASR search
  help states "Based on NAD83"), i.e. EPSG:4269, matching the FCC-required ULS datum policy.
- `EN.dat` (25 fields): 374,223 entity rows carrying registrant/owner names per
  (`registration_number`, `unique_system_identifier`).

## Proposed draft contract (unapproved)

Publish one row per (`registration_number`, `unique_system_identifier`, CO coordinate row) —
tower coordinates as single points and array positions as separate array-position points —
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

Draft pipeline implementation (source adapter, ingest asset, focused tests, isolated Dagster
verification against the real weekly bytes) is the immediate next work, using the established
ULS-family module pattern; this file records the source, evidence, and proposed contract for
that work. No source, baseline, schema/ID/coverage/rights, release, merge, or production
approval is granted or implied.

# Oil and Natural Gas Platforms — reconstruction feasibility

Inventory `oil-and-natural-gas-platforms`, DOE / Oak Ridge National Laboratory
(ORNL). Research for [#961](https://github.com/fulton-ring/hifld-next-datasets/issues/961)
and [#255](https://github.com/fulton-ring/hifld-next-datasets/issues/255), checked
**2026-11-15 UTC**. No runtime, release contract or publication changes.

**Public producer constituents are inspectable and recover substantially more
than coordinates.** BSEE Gulf and BOEM Pacific master tables contain staffing,
equipment and operational attributes. The previous assertion that neither agency
publishes these fields was incorrect: it considered the five-field BSEE GIS
export and described Pacific tables without inspecting their payloads. Exact ORNL
product recovery remains unverified; missing evidence is not proof that recovery
is impossible. GII access is a requirement for inspecting the migrated exact
product, not a blocker to this public-constituent investigation.

## Exact-product evidence and its limits

The existing 2026-11-13 research recorded the DHS crosswalk row as Migrated to
GII, ORNL producer, GII access required, no external landing page. It recorded the
old HIFLD item/service as unavailable and no public exact ORNL successor found
in ORNL, Open Energy Hub, OSTI or data.gov searches. Those are **prior findings,
not new live checks**. The prior crosswalk byte-copy hash was
`7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc`;
its distribution and legacy schema observations were mirror-derived, and neither
has been fetched again or admitted as a pipeline input here.

The prior note reported 7,294 historical points, Gulf and Pacific coverage, a
2011–2016 observation period and 37 columns. These do not establish an accepted
original contract. This investigation has no authoritative original ORNL payload,
full data dictionary, ID crosswalk or GII extraction. Consequently original field
types/nullability, complete field roster, row membership, geometries and values
cannot be compared. The field mapping below covers every named legacy field in
the existing note; any unnamed columns in that reported 37-column schema remain
unknown. Do not turn this provisional mapping into a published ORNL schema.

## Producer publications inspected

All requests were anonymous HTTP GETs returning 200 at the named original agency
host, without redirects or credentials. Complete bodies and scratch analysis were
kept outside Git under `/tmp/hifld961-evidence` for this session. No prohibited
mirror, archive or nonproducer ArcGIS service was fetched.

- [BSEE Mapping](https://www.data.bsee.gov/Main/Mapping.aspx) explicitly links the
  [platform GIS ZIP](https://www.data.bsee.gov/Mapping/Files/platform.zip) and
  [metadata](https://www.data.bsee.gov/Mapping/Files/platform_meta.html).
  Metadata: Gulf of Mexico federal OCS only; monthly; publication 2026-03-02,
  period end 2026-04-01; NAD27; 7,317 points. These metadata dates are not a
  November release date. The live ZIP has five DBF attributes:
  `STRUCTURE_` numeric width 5, `COMPLEX_ID` numeric width 10, `STRUCTURE1`
  character width 15, `INSTALL_DA` and `REMOVAL_DA` dates. Geometry is Point.
  It includes removed structures (5,784 nonempty removal dates), not just active
  installations. It is already handled separately by #257 and the existing
  `bsee_ocs_platforms.py` adapter.
- [BSEE Platform/Rig](https://www.data.bsee.gov/Main/Platform.aspx) directly
  publishes Gulf master, structure and location fixed-width ZIPs. Page last-updated
  values: 2026-11-02 10:02:22, 10:12:11 and 10:06:56, respectively (timezone
  unspecified). Master row revisions span 1993-11-09–2026-10-19; structure
  revisions span 1994-05-24–2026-10-19. Export timestamps do not make every
  observation current. No ASCII refresh cadence is stated on the inspected page.
- [BOEM Pacific Platform](https://www.data.boem.gov/Main/PacificPlatform.aspx)
  explicitly limits the three files to Pacific OCS. Page last-updated values:
  master 2026-11-01 06:32:10, structures 06:32:11, locations 06:32:09 (timezone
  unspecified). Master row revisions span 1999-04-07–2025-02-24; structure
  revisions span 2012-08-21–2019-02-20. No cadence is stated on this page.

These are producer publications because their platform landing pages explicitly
publish and define the files, not merely because they use `.gov` domains.

| Complete ZIP input | Bytes | SHA-256 |
| --- | ---: | --- |
| [BSEE GIS](https://www.data.bsee.gov/Mapping/Files/platform.zip) | 254550 | `91c748d981818e07ab109fbabc08c62e733fbb9634f6e986f2f34b8b1438ad97` |
| [BSEE masters](https://www.data.bsee.gov/Platform/Files/platmastfixed.zip) | 142259 | `9854de556e8c6d0d6ebe048460c503d1bc6de19e9c9aa4f17e4b39dcea8c83a9` |
| [BSEE structures](https://www.data.bsee.gov/Platform/Files/platstrufixed.zip) | 209393 | `9957906ecc9e67d41c3cc1b9d7da167b8655a4829d5c856f803e944cf4f73720` |
| [BSEE locations](https://www.data.bsee.gov/Platform/Files/platlocfixed.zip) | 238817 | `85aa398e9e8ade8f5aaff4a33fd3e62d57d12fef76c89a37c4102f87c85f4421` |
| [BOEM masters](https://www.data.boem.gov/Platform/Files/platmastpacdelimit.zip) | 1414 | `c937520cba6a70a0d883188298ac11e6ba329f7c515d25028109a2d2caffdbe1` |
| [BOEM structures](https://www.data.boem.gov/Platform/Files/platstrupacdelimit.zip) | 1165 | `814b7ca756388302a42a6a19779983fc82ec40aba10c47101fd9b6ba6b5b9912` |
| [BOEM locations](https://www.data.boem.gov/Platform/Files/platlocpacdelimit.zip) | 1439 | `4d933f9e333be3ae67f1657f0ecde897b52d8a6c256d3f2a5202fadab6544904` |

## Native entities, schema and observed joins

Parsed all fixed-width rows using BSEE's published start/length definitions and
all headerless, quoted Pacific CSV rows in their published column order. These
are text exports: keep original lexical IDs, padding and bytes; widths are not
an accepted public numeric type contract. Gulf definitions give complex ID width
8, structure number width 3, company number width 5; Pacific widths are 9, 3
and 5. Company values such as `02531` contain meaningful leading zeros.

Layouts: [Gulf masters](https://www.data.bsee.gov/Main/HtmlPage.aspx?page=platformMasters),
[Gulf structures](https://www.data.bsee.gov/Main/HtmlPage.aspx?page=platformStructures),
[Gulf locations](https://www.data.bsee.gov/Main/HtmlPage.aspx?page=platformLocations),
[Pacific masters](https://www.data.boem.gov/Main/HtmlPage.aspx?page=pacplatformMasters),
[Pacific structures](https://www.data.boem.gov/Main/HtmlPage.aspx?page=pacplatformStructures),
[Pacific locations](https://www.data.boem.gov/Main/HtmlPage.aspx?page=pacplatformLocations).

The definitions identify a complex as one structure or a walkway-connected group;
a structure number is unique **within its complex**. Use the native pair, scoped
to producer/region, when investigating structure identity. Neither a complex ID
alone nor a well API number is a structure key. No cross-region uniqueness or
legacy `PLATFRMID` correspondence is established.

| Measurement on complete captured tables | Gulf | Pacific |
| --- | ---: | ---: |
| Master rows / fields | 6940 / 42 | 34 / 42 |
| Structure rows / fields | 7092 / 23 | 24 / 19 |
| Location rows / fields | 7306 / 14 | 24 / 14 |
| Duplicate master complex IDs | 0 | 0 |
| Duplicate structure native pairs / location native pairs | 0 / 0 | 0 / 0 |
| Structure rows lacking a master / location rows lacking a master | 0 / 0 | 0 / 0 |
| Structure pairs absent from locations | 8 | 0 |
| Location pairs absent from structures | 222 | 0 |
| Masters without a structure | 759 | 10 |

Thus each observed structure/location maps to exactly one master; Gulf complexes
have 1–17 structure rows and Pacific represented complexes have one each.
Pacific's 10 extra masters also lack locations. These are observed cardinalities,
not guarantees for future exports. A Gulf structure/location inner join would
drop 8 structures and 222 locations. The Gulf GIS export has 7,317 distinct native
pairs in 6,399 complexes; comparison against the 7,092 ASCII structures yields
7 structure-only and 232 GIS-only pairs. Exports are not interchangeable or
synchronized; discrepancy causes remain unverified.

Gulf locations have one blank lon/lat pair (`91001`, `1`); Pacific has one
`(0,0)` pair (`51012`, `1`). Preserve and flag these rather than geocode or repair.
Remaining location ranges are Gulf longitude -97.113264 to -87.65580275,
latitude 26.03569735 to 30.19497352; Pacific nonzero longitude -120.73043522 to
-118.11739226, latitude 33.56380336 to 34.6104033.
ASCII definitions describe coordinates without establishing a datum sufficient
for an approved cross-region geometry contract. The separate GIS ZIP explicitly
specifies NAD27; do not assume every ASCII coordinate has that datum.

## Provisional field recoverability

“Candidate” means the producer publishes a corresponding concept; equivalence of
units, code domains, grain, vintage and ORNL transformations is **unverified**.
Gulf source labels below follow its layout aliases; Pacific machine names follow
its definitions. Both master tables contain the listed candidate concepts unless
specified. No values were joined to ORNL records.

| Legacy field(s) reported in prior note | Producer candidate / limit |
| --- | --- |
| `COMPLEXID` | Master/structure/location complex ID; native key available; ORNL type/value mapping unknown |
| `PLATFRMID` | No verified ORNL-to-native structure ID mapping; do not concatenate/generate replacements |
| `NOSTR` | Structure counts can be measured per complex, but ORNL definition and inclusion rules unknown |
| `NAICS_CODE`, `NAICS_DESC` | Absent from inspected seven payloads; no verified reconstruction |
| `OWNER` | Absent; designated operator company number is not ownership |
| `OPERNAME` | `Mms Company Num` / `MMS_COMPANY_NUM` only; name lookup and historical association not inspected |
| `POSREL` | No verified equivalent |
| `COMMODITY` | Oil/gas/condensate/water flags exist; ORNL commodity derivation unknown |
| `MANNED24HR` | `Manned 24 Hr Fl` / `MANNED_24_HR_FL`; observed Gulf blanks 630/6940, Pacific 0/34 |
| `DISTLAND` | `Distance To Shore` / `DISTANCE_TO_SH`; Gulf nautical miles; Pacific definition does not state units |
| `WATERDEPTH` | `Water Depth` / `WATER_DEPTH`; Gulf feet from mean sea level to mud line; Pacific unit unconfirmed |
| `RIGCOUNT` | `Rig Count` / `RIG_COUNT`; Gulf blanks 2907/6940, Pacific 12/34 |
| `PWRSOURCE` | `Power Source Type` / `POWER_SOURCE_TYP`; Gulf blanks 5961/6940, Pacific 11/34; domains need checking |
| `PWRGENFLAG` | `Power Gen Flag` / `POWER_GEN_FLAG` |
| `OILPRDFLAG` | `Oil Prod Flag` / `OIL_PROD_FLAG` |
| `GASMTRFLAG` | `Gas Sale Mtr Flag` / `GAS_SALE_MTR_FL`; allocation and LACT meters are separate concepts |
| `COMPREFLAG` | `Compressor Flag` / `COMPRESSOR_FLAG` |
| `COMPRDFLAG` | `Comgl Prod Flag` / `COMGL_PROD_FLAG` is a candidate only; legacy abbreviation/definition unverified |
| `HELPRTFLAG` | `Heliport Flag` / `HELIPORT_FLAG` |
| `BEDCOUNT` | `Bed Count` / `BED_COUNT`; Gulf blanks 5903/6940, Pacific 11/34 |
| `BLOCKNO`, `LEASENO`, `AREANUMBER` | `Block Number`, `Lease Number`, `Area Code` (Pacific `BLOCK_NUMBER`, `LEASE_NUMBER`, `AREA_CODE`); area codes are not proven ORNL area numbers |
| `REGION` | Regional product scope is known; ORNL coded values unknown |
| `SOURCE`, `SOURCEDATE` | New producer provenance/revision dates are available; cannot restore ORNL per-row source lineage |
| `VAL_METHOD`, `VAL_DATE` | Absent; producer revision dates are not ORNL validation dates |
| `GlobalID` | Absent; exact ORNL identifiers unrecovered |
| Geometry / unnamed legacy fields | Native source coordinates available with defects above; exact legacy geometry, CRS and remaining schema unknown |

No blanket “enrichment unavailable” conclusion is justified. Conversely, a
published field can be blank, stale or at complex grain; copying it onto every
structure does not prove ORNL structure-level values. Gulf date strings use
`DD-MON-YYYY`, Pacific revisions use `YYYYMMDD`; retain native values before any
explicit conversion. Install/removal dates are structure attributes, not a
license to filter all historic structures to active records.

## Regional coverage and product choices

| Region | Accessible constituent evidence | Limit against original ORNL inventory |
| --- | --- | --- |
| Gulf federal OCS | BSEE GIS plus three richer ASCII tables | Different membership/dates across exports; original ORNL rows unknown |
| Pacific OCS | BOEM 34 masters, 24 structures/locations | Includes removed structure and zero-coordinate row; original ORNL coverage/selection unknown |
| State waters / Alaska / Atlantic / inland | Not established by these regional products | No claim of nationwide completeness or justified absence |

An exact GII recovery could supply ORNL IDs, lineage and the authoritative schema,
but access does not itself establish currency, rights or reproducibility. A
transparent current constituent product is technically plausible and can expose
native complex/structure grain, source dates, unmatched records and missing
attributes. It would require a **separate named dataset and versioned contract**
with regional scope, coordinate treatment, source membership and enrichment
mappings explicitly chosen by a maintainer. It cannot silently replace historical
`oil-and-natural-gas-platforms` releases. If a pilot is authorized later, reuse
existing OCS adapters; `boem_ocs_wells.py` concerns wells, not interchangeable
platform entities. No composite or new runtime is implemented by this research.

## Rights, checks and remaining work

The [BSEE GIS metadata](https://www.data.bsee.gov/Mapping/Files/platform_meta.html)
states government development and no proprietary rights, disclaims accuracy and
liability, and prohibits legal/navigational reliance. The
[BOEM Data Center disclaimer](https://www.data.boem.gov/Main/HtmlPage.aspx?page=Disclaimer)
describes a working database assembled from submissions, possible errors and
nonlegal/nonnavigational use. These inspected notices do not establish ORNL/GII
redistribution rights or approve a combined product. No agreement was accepted,
provider contacted or credential gate bypassed.

Verification performed: complete ZIP retrieval, byte counts/SHA-256, ZIP member
readback, physical DBF descriptors/counts, fixed-width layout parsing, CSV row
widths, complete-table key/null/date/range checks and the set comparisons above.
Scratch scripts and payloads remain outside Git. No dataset tests or Dagster
materializations were run because only this source note changes; no claims of
runtime execution, source acceptance or parity are made.

Research deliverable #961 is reviewable without a human access grant. Remaining
comparisons require an authoritative ORNL schema/payload and access to the migrated
product if exact recovery is selected. Company-name joins, code domains, Pacific
units/datum and discrepant Gulf membership need investigation before any pilot
contract. A changed-scope product requires a maintainer choice; existing false
approval flags, stopped schedules and stable historical release paths remain in
force. Neither this note nor code review grants source, baseline, ID/schema,
rights or release acceptance. No implementation of the national ORNL product is
claimed complete.

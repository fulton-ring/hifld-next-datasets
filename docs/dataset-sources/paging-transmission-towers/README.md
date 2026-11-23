# Paging Transmission Towers (FCC ULS)

The draft source is the weekly complete Paging licenses file (`l_paging.zip`) from
the FCC Universal Licensing System public access downloads at
`https://data.fcc.gov/download/pub/uls/complete/l_paging.zip`, the official bulk
distribution to which the FCC moved ULS downloads in July 2021 (the retired
`wireless2.fcc.gov` host now returns HTTP 403). The published population is every
location (`LO`) record of every license whose header (`HD`) license status is `A`
(active), radio service roster CA (commercial air-ground), CB (BETRS), CD (paging
and radiotelephone), CG (general aviation air-ground), CO (offshore
radiotelephone) and CR (rural radiotelephone), the Part 22 site-based paging
codes; the geographic-area paging
codes CP, CZ and GC named on FCC's Paging page are not carried by this weekly
file group, an unaccepted coverage question. Columns follow the historical HIFLD
Paging Transmission Towers contract (`UniqSysID`, `Licensee`, `Callsign`,
`LocNum`, DMS coordinates, address fields, `Nepa`, `QZone`, `TowReg`,
`SupStruc`, `AllStruc`, `StrucType`, `LicStatus`, derived `latdec`/`londec`,
`url`). `UniqSysID` and `LocNum` are retained unchanged and jointly unique;
geometry is the DMS-to-decimal conversion written unchanged as WKB points in the
FCC-required native NAD83 datum (EPSG:4269). ULS carries only the site locations
licensees filed, so this is the FCC's filed-site population, not a census of
physical paging towers.

The capture retains the complete zip body, wire body and request receipt. The
FCC serves the weekly file at one fixed URL and replaces it in place, with no
edition name in the path; the payload's own `counts` member is therefore the
record population witness and its file creation date the observed edition
marker, both recorded in the audit and the candidate result. Record population
completeness is checked against the publisher's own `counts` member at capture
time: the decoded HD unique license-ID roster (via the shared
`validate_capture_witness` path) and the EN/LO row counts must equal the line
counts the publisher recorded for those members, and the counts file row counts
are verified against every zip member as a completeness gate. Validation fails
closed on capture roster and receipt drift, zip member roster, counts-file row
counts, per-record-type field counts and record-type prefixes, radio service
roster, licensee and callsign joins, duplicate `(UniqSysID, LocNum)` keys, DMS
direction codes, out-of-nominal DMS component count, derived-decimal coordinate
range and null-geometry count (rows with fully empty DMS keep null geometry).
The license status distribution, active-location count, service-row
distribution and the active-ID and state-roster digests are recorded as
per-capture audit observations rather than gates, so a regenerated weekly
edition is adopted and its drift against an approved release is reported by the
registered non-blocking approved-baseline drift check. Raw DMS components are
preserved verbatim, including out-of-nominal latitude seconds and empty
minutes, which the FCC's own layer description anticipates ("It is known that
there are some errors in the licensing information - Latitude, Longitude and
Ground Elevation data"). The weekly refresh schedule is stopped. The archived
HIFLD OPEN Paging Transmission Towers layer (DataLumos project 240567) and the
DHS crosswalk DOI (10.3886/E241367V1) are banned as pipeline inputs by the
source policy, so archive crosswalk identity remains unverified; the former
HIFLD-labeled Esri copies are research context only and their 6541 features are
a historical baseline hint against the weekly edition's active-location
population.
Source edition, baseline, schema/ID/coverage contracts, rights and release
approval remain unaccepted.

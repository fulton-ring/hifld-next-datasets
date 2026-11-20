# Cellular Towers (FCC ULS)

The draft source is the weekly complete Cellular - 47 CFR Part 22 licenses file
(`l_cell.zip`) from the FCC Universal Licensing System public access downloads at
`https://data.fcc.gov/download/pub/uls/complete/l_cell.zip`, the official bulk
distribution to which the FCC moved ULS downloads in July 2021. The published
population is every location (`LO`) record of every license whose header (`HD`)
license status is `A` (active), all radio service code `CL`, under the
historical HIFLD Cellular Towers column
contract (`UniqSysID`, `Licensee`, `Callsign`, `LocNum`, DMS coordinates,
address fields, `Nepa`, `QZone`, `TowReg`, `SupStruc`, `AllStruc`,
`StrucType`, `LicStatus`, derived `latdec`/`londec`, `url`). `UniqSysID` and
`LocNum` are retained unchanged and jointly unique; `TowReg` is preserved as the
source string rather than the historical float. ULS cellular licensing is
geographic, so this is the FCC's filed-site population, not a census of physical
towers.

The capture retains the complete zip body, wire body and request receipt. The
FCC serves the weekly file at one fixed URL and replaces it in place, with no
edition name in the path; the payload's own `counts` member is therefore the
completeness witness and its file creation date the observed edition marker,
both recorded in the audit and the candidate result. Record population
completeness is checked against the publisher's own `counts` member at capture
time: the decoded HD unique license-ID roster (via the shared
`validate_capture_witness` path) and the EN/LO row counts must equal the
line counts the publisher recorded for those members. Validation fails closed
on capture roster and receipt drift, zip member roster, per-record-type field
counts and record-type prefixes, radio service roster, licensee and callsign
joins, duplicate `(UniqSysID, LocNum)` keys, DMS direction codes,
derived-decimal coordinate range and null-geometry count. The license status
distribution, active-location count, full active ID roster digest and state
roster digest are recorded as per-capture audit observations rather than
gates, so a regenerated weekly edition is adopted and its drift against an
approved release is reported by the registered non-blocking
approved-baseline drift check. Raw DMS components are preserved verbatim,
including the population's out-of-nominal values (seconds 60.0, minutes 73), which the
FCC's own layer description anticipates ("It is known that there are some
errors in the licensing information - Latitude, Longitude and Ground Elevation
data"). The weekly refresh schedule is stopped. The archived HIFLD
OPEN Cellular Towers layer (DataLumos project 238960, DOI 10.3886/E238960V1)
and the DHS crosswalk DOI (10.3886/E241367V1) return HTTP 403 anonymously, so
archive crosswalk identity remains unverified; the existing published HIFLD
Next v1.0.0 frozen copy is retained as comparison-only context. Source edition,
baseline, schema/ID/coverage contracts, rights and release approval remain
unaccepted.

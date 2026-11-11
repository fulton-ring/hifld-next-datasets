# Cellular Towers (FCC ULS)

The draft source is the weekly complete Cellular - 47 CFR Part 22 licenses file
(`l_cell.zip`) from the FCC Universal Licensing System public access downloads at
`https://data.fcc.gov/download/pub/uls/complete/l_cell.zip`, the official bulk
distribution to which the FCC moved ULS downloads in July 2021. The published
population is every location (`LO`) record of every license whose header (`HD`)
license status is `A` (active) — 24362 locations in the pinned weekly file, all
radio service code `CL` — under the historical HIFLD Cellular Towers column
contract (`UniqSysID`, `Licensee`, `Callsign`, `LocNum`, DMS coordinates,
address fields, `Nepa`, `QZone`, `TowReg`, `SupStruc`, `AllStruc`,
`StrucType`, `LicStatus`, derived `latdec`/`londec`, `url`). `UniqSysID` and
`LocNum` are retained unchanged and jointly unique; `TowReg` is preserved as the
source string rather than the historical float. ULS cellular licensing is
geographic, so this is the FCC's filed-site population, not a census of physical
towers.

The capture retains the complete zip body, wire body and request receipt; the
payload's own `counts` file creation date and the HTTP receipt provide dated
freshness evidence distinct from fetch time. Validation fails closed on capture
roster and receipt drift, zip member roster, per-record-type field counts and
record-type prefixes, HD/EN/LO record totals, radio service roster, license
status distribution, licensee and callsign joins, duplicate `(UniqSysID,
LocNum)` keys, active-population count, full active ID roster digest, state
roster digest, DMS direction codes, derived-decimal coordinate range and
null-geometry count. Raw DMS components are preserved verbatim, including the
pinned population's out-of-nominal values (seconds 60.0, minutes 73), which the
FCC's own layer description anticipates ("It is known that there are some
errors in the licensing information - Latitude, Longitude and Ground Elevation
data"). The weekly refresh schedule is stopped. The archived HIFLD
OPEN Cellular Towers layer (DataLumos project 238960, DOI 10.3886/E238960V1)
and the DHS crosswalk DOI (10.3886/E241367V1) return HTTP 403 anonymously, so
archive crosswalk identity remains unverified; the existing published HIFLD
Next v1.0.0 frozen copy is retained as comparison-only context. Source edition,
baseline, schema/ID/coverage contracts, rights and release approval remain
unaccepted.

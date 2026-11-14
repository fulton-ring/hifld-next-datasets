# Paging Transmission Towers (FCC ULS)

The draft source is the weekly complete Paging licenses file (`l_paging.zip`) from
the FCC Universal Licensing System public access downloads at
`https://data.fcc.gov/download/pub/uls/complete/l_paging.zip`, the official bulk
distribution to which the FCC moved ULS downloads in July 2021 (the retired
`wireless2.fcc.gov` host now returns HTTP 403). The published population is every
location (`LO`) record of every license whose header (`HD`) license status is `A`
(active) — 6389 locations in the pinned 2026-11-08 weekly edition, radio service
roster CA (commercial air-ground), CB (BETRS), CD (paging and radiotelephone), CG
(general aviation air-ground), CO (offshore radiotelephone) and CR (rural
radiotelephone), the Part 22 site-based paging codes; the geographic-area paging
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

The capture retains the complete zip body, wire body and request receipt; the
payload's own `counts` file creation date and the HTTP receipt provide dated
freshness evidence distinct from fetch time, and the counts file row counts are
verified against every zip member as a completeness gate. Validation fails
closed on capture roster and receipt drift, zip member roster, counts-file row
counts, per-record-type field counts and record-type prefixes, HD/EN/LO record
totals, radio service roster, license status distribution, licensee and callsign
joins, duplicate `(UniqSysID, LocNum)` keys, active-population count, full
active ID roster digest, service-row distribution, state roster digest, DMS
direction codes, out-of-nominal DMS component count, derived-decimal coordinate
range and null-geometry count (23 source rows carry no DMS degrees and keep null
geometry). Raw DMS components are preserved verbatim, including the pinned
population's out-of-nominal latitude seconds 70.0 and one row with empty
minutes, which the FCC's own layer description anticipates ("It is known that
there are some errors in the licensing information - Latitude, Longitude and
Ground Elevation data"). The weekly refresh schedule is stopped. The archived
HIFLD OPEN Paging Transmission Towers layer (DataLumos project 240567) and the
DHS crosswalk DOI (10.3886/E241367V1) are banned as pipeline inputs by the
source policy, so archive crosswalk identity remains unverified; the former
HIFLD-labeled Esri copies are research context only and their 6541 features are
a historical baseline hint against this edition's 6389 active locations.
Source edition, baseline, schema/ID/coverage contracts, rights and release
approval remain unaccepted.

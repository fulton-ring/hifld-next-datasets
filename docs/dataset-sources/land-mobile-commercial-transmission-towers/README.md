# Land Mobile Commercial Transmission Towers (FCC ULS)

The draft source is the weekly complete Land Mobile - Commercial licenses file
(`l_LMcomm.zip`) from the FCC Universal Licensing System public access downloads
at `https://data.fcc.gov/download/pub/uls/complete/l_LMcomm.zip`, the official
bulk distribution to which the FCC moved ULS downloads in July 2021 and the
"Land Mobile - Commercial / Licenses" file listed on the FCC Daily & Weekly
Transaction Files page. The historical HIFLD OPEN layer's own description says
it "represents Land Mobile Commercial transmission tower locations as recorded
by the Federal Communications Commission, extracted from the FCC Licensing
Database", so this weekly licenses file is the original agency distribution of
the same ULS data family. The published population is every location (`LO`)
record of every license whose header (`HD`) license status is `A` (active) —
25501 locations in the pinned weekly file (HD 81081, EN 153247, LO 185230
records; statuses A 8096 / C 59916 / E 11450 / T 1619) — under the historical
HIFLD column contract (`UniqSysID`, `Licensee`, `Callsign`, `LocNum`, DMS
coordinates, address fields, `Nepa`, `QZone`, `TowReg`, `SupStruc`, `AllStruc`,
`StrucType`, `LicStatus`, derived `latdec`/`londec`, `url`). `UniqSysID` and
`LocNum` are retained unchanged and jointly unique; `TowReg` is preserved as
the source string rather than the historical float. The pinned file carries 15
of the 17 FCC Land Mobile - Commercial radio service codes (GM, GR, GS, GX, IK,
LN, LW, NC, QD, QO, QT, YK, YM, YS, YX; GL and YL have no records). 7530
active location records are filed without latitude/longitude components
(7429 with all DMS components empty, 97 with only the direction codes, 4 with
only the longitude direction) and are published with null geometry rather than
dropped. Land Mobile - Commercial is a site-based ULS service group (FCC public
access files documentation), so this is the FCC's filed-site population, not a
census of physical towers.

The capture retains the complete zip body, wire body and request receipt; the
payload's own `counts` file creation date and the HTTP receipt provide dated
freshness evidence distinct from fetch time. Validation fails closed on capture
roster and receipt drift, zip member roster (33 members), per-record-type field
counts and record-type prefixes, HD/EN/LO record totals, radio service roster,
license status distribution, licensee and callsign joins, duplicate
`(UniqSysID, LocNum)` keys, active-population count, full active ID roster
digest, state roster digest, DMS direction codes, derived-decimal coordinate
range and null-geometry count. Raw DMS components are preserved verbatim,
including out-of-nominal values that the FCC's own layer description
anticipates ("It is know that there are some errors in the licensing
information - Latitude, Longitude and Ground Elevation data"). The derived
`latdec`/`londec` follow the ULS-derived tower-family convention (DMS formula
formatted to 12 significant digits); all 19148 published rows of the frozen
HIFLD Next v1.0.0 copy match this formula computed from that copy's own DMS
components. Coordinates are declared in the FCC-required native NAD83 datum
(EPSG:4269) with no transformation. The weekly refresh schedule is stopped. The
archived HIFLD OPEN layer (DataLumos project 239395, the inventory's download
location) and the DHS crosswalk DOI 10.3886/E241367V1 return HTTP 403
anonymously, so archive identity remains unverified; the existing published
HIFLD Next v1.0.0 frozen copy (19148 point features, issued 2024-01-24) is
retained as comparison-only context — 17566 of its keys are still active
(1582 dropped with licenses now expired, cancelled, removed or relocated),
7935 current active keys are new (405 with coordinates and 7530 coordinate-less
records the frozen copy does not contain, a coverage difference flagged for the
maintainer compatibility decision), and the shared keys agree field-for-field
on stable records. Source edition, baseline, schema/ID/coverage contracts,
rights and release approval remain unaccepted.

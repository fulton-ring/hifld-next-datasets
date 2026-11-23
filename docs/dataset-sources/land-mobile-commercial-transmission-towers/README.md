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
record of every license whose header (`HD`) license status is `A` (active),
under the historical HIFLD column contract (`UniqSysID`, `Licensee`,
`Callsign`, `LocNum`, DMS coordinates, address fields, `Nepa`, `QZone`,
`TowReg`, `SupStruc`, `AllStruc`, `StrucType`, `LicStatus`, derived
`latdec`/`londec`, `url`). `UniqSysID` and `LocNum` are retained unchanged and
jointly unique; `TowReg` is preserved as the source string rather than the
historical float. Radio service codes are the FCC Land Mobile - Commercial
roster; in the observed editions 15 of the 17 roster codes appear (GM, GR, GS,
GX, IK, LN, LW, NC, QD, QO, QT, YK, YM, YS, YX; GL and YL have no records).
Active location records filed without latitude/longitude components are
published with null geometry rather than dropped, and active locations in the
FCC Do-Not-Convert datum area Northern Mariana Islands (ULS state code MP) are
also published without geometry, with their raw DMS components and derived
decimal coordinates retained, because the FCC ULS datum policy leaves those
coordinates in their applicable local datum. Land Mobile - Commercial is a
site-based ULS service group (FCC public access files documentation), so this
is the FCC's filed-site population, not a census of physical towers.

The capture retains the complete zip body, wire body and request receipt. The
FCC serves the weekly file at one fixed URL and replaces it in place, with no
edition name in the path; the payload's own `counts` member is therefore the
completeness witness and its file creation date the observed edition marker,
both recorded in the audit and the candidate result. Record population
completeness is checked against the publisher's own `counts` member at capture
time: the decoded HD unique license-ID roster (via the shared
`validate_capture_witness` path) and the EN/LO row counts must equal the
line counts the publisher recorded for those members. Validation fails closed
on capture roster and receipt drift, zip member roster (33 members),
per-record-type field counts and record-type prefixes, radio service roster
(the Land Mobile - Commercial source-identity contract), licensee and callsign
joins, duplicate `(UniqSysID, LocNum)` keys, DMS direction codes,
derived-decimal coordinate range. The license status distribution,
active-location count, null-geometry count, Do-Not-Convert withheld-geometry
count, full active ID roster digest and state roster digest are recorded as
per-capture audit observations rather than gates, so a regenerated weekly
edition is adopted and its drift against an approved release is reported by
the registered non-blocking approved-baseline drift check. Raw DMS components
are preserved verbatim, including out-of-nominal values that the FCC's own
layer description anticipates ("It is know that there are some errors in the
licensing information - Latitude, Longitude and Ground Elevation data"). The
derived
`latdec`/`londec` follow the ULS-derived tower-family convention (DMS formula
formatted to 12 significant digits); all 19148 published rows of the frozen
HIFLD Next v1.0.0 copy match this formula computed from that copy's own DMS
components. Coordinates are declared in the FCC-required native NAD83 datum
(EPSG:4269) with no transformation: the FCC ULS datum policy requires NAD83 in
its converted areas (CONUS, Alaska, Hawaii, Puerto Rico, the U.S. Virgin
Islands via NCAT; American Samoa and Guam via its specialized Methods #1 and
#2), while its Do-Not-Convert areas (Baker, Howland, Jarvis, Johnston, Kingman
Reef, Midway, Northern Mariana Islands, Palmyra, Wake) keep coordinates in an
applicable local datum the FCC does not name and ULS does not record, so
geometry is withheld for those rows rather than mislabeled NAD83. The weekly
refresh schedule is stopped. The
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

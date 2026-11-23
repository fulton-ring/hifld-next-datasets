# Microwave Service Towers (FCC ULS)

The draft source is the weekly complete Microwave service-group licenses file
(`l_micro.zip`) from the FCC Universal Licensing System public access downloads
at `https://data.fcc.gov/download/pub/uls/complete/l_micro.zip`, the official
bulk distribution to which the FCC moved ULS downloads in July 2021. The
historical HIFLD OPEN layer is described by its publisher as "an extract of the
Universal Licensing System (ULS) licensed by the Wireless Telecommunications
Bureau (WTB). It consists of Microwave Transmitters (see Rule Part: Fixed
Microwave Service (47 CFR Part 101))", received by HIFLD from the FCC as data
provider. The published population is every location (`LO`) record of every
license whose header (`HD`) license status is `A` (active), across the ULS
Microwave service-group radio service roster (`CF`, `MG`, `PF`, `MM`, `NN`,
`CT` and the co-located broadcast auxiliary codes), under the ULS-derived tower
column contract (`UniqSysID`, `Licensee`, `Callsign`, `LocNum`, DMS coordinates,
address fields, `Nepa`, `QZone`, `TowReg`, `SupStruc`, `AllStruc`,
`StrucType`, `LicStatus`, derived `latdec`/`londec`, `url`). `UniqSysID` and
`LocNum` are retained unchanged; the source's own duplicate `(UniqSysID,
LocNum)` location rows (re-filed coordinates for one location number) are
published verbatim rather than dropped or re-keyed — a publish-policy contract,
with the observed duplicate-key roster recorded per capture rather than pinned.
Microwave licensing authorizes point links, so this is the FCC's filed-site
population, not a census of physical towers.

The capture retains the complete zip body, wire body and request receipt. The
FCC serves the weekly file at one fixed URL and replaces it in place, with no
edition name in the path; the payload's own `counts` member is therefore the
completeness witness and its file creation date the observed edition marker,
both recorded in the audit and the candidate result. Record population
completeness is checked against the publisher's own `counts` member at capture
time: the decoded HD unique license-ID roster (via the shared
`validate_capture_witness` path) and the EN/LO row counts must equal the
line counts the publisher recorded for those members. Validation fails closed
on capture roster and receipt drift, zip member roster (40 members),
per-record-type field counts and record-type prefixes, radio service roster,
licensee and callsign joins, partial-DMS rows and derived-decimal coordinate
range. The license status distribution, active-location count, service-row
distribution, duplicate-key roster, null-geometry count, invalid-direction
count and the active-ID and state-roster digests are recorded as per-capture
audit observations rather than gates, so a regenerated weekly edition is
adopted and its drift against an approved release is reported by the registered
non-blocking approved-baseline drift check. Source data quality anomalies are
preserved, not repaired: fully empty DMS rows stay null geometry, direction
letters outside the nominal set are preserved and converted by the plain DMS
formula without sign flip, and one-sided empty DMS rows fail closed.
GeoParquet geometry is a WKB Point in EPSG:4269 (NAD83), the datum FCC requires
for ULS coordinates; the DMS-derived decimal values are written unchanged with
no datum transformation. Per FCC policy
(https://www.fcc.gov/wireless/systems-utilities/north-american-datum-1983-coordinates),
CONUS, Alaska, Hawaii, Puerto Rico and U.S. Virgin Islands coordinates are
converted to NAD83 (NCAT), American Samoa and Guam use FCC-specified shifts to
NAD83 from their local datums, and other Pacific island areas (Baker, Howland,
Jarvis, Johnston, Kingman Reef, Midway, Northern Mariana Islands, Palmyra,
Wake) remain in the applicable local datum. The weekly refresh schedule
is stopped. The archived HIFLD OPEN Microwave Service Towers layer (DataLumos
project 239596, DOI 10.3886/E239596V1) and the DHS crosswalk DOI
(10.3886/E241367V1) return HTTP 403 anonymously and are banned as pipeline
inputs by the source policy, so archive identity is cited from the DataLumos
landing metadata rather than fetched payloads, and the exact historical
frozen-copy feature-count comparison remains unverified. Source edition,
baseline, schema/ID/coverage contracts, rights and release approval remain
unaccepted.

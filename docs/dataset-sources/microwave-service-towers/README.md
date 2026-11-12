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
license whose header (`HD`) license status is `A` (active) — 422808 locations
in the pinned 2026-11-08 weekly file, across the complete ULS Microwave
service-group radio service roster (`CF`, `MG`, `PF`, `MM`, `NN`, `CT` and the
co-located broadcast auxiliary codes) — under the ULS-derived tower column
contract (`UniqSysID`, `Licensee`, `Callsign`, `LocNum`, DMS coordinates,
address fields, `Nepa`, `QZone`, `TowReg`, `SupStruc`, `AllStruc`,
`StrucType`, `LicStatus`, derived `latdec`/`londec`, `url`). `UniqSysID` and
`LocNum` are retained unchanged; the source's own duplicate location rows (16
duplicate `(UniqSysID, LocNum)` keys, 20 extra rows in the pinned edition) are
published verbatim rather than dropped or re-keyed. Microwave licensing
authorizes point links, so this is the FCC's filed-site population, not a
census of physical towers.

The capture retains the complete 209 MB zip body, wire body and request
receipt; the payload's own `counts` file creation date and the HTTP receipt
provide dated freshness evidence distinct from fetch time. Validation fails
closed on capture roster and receipt drift, zip member roster (40 members),
per-record-type field counts and record-type prefixes, HD/EN/LO record totals,
radio service roster, license status distribution, licensee and callsign
joins, active-population count, full active ID roster digest, service-row
distribution, duplicate-key count and roster digest, state roster digest,
DMS direction preservation count, derived-decimal coordinate range and
null-geometry count. Source data quality anomalies are preserved, not
repaired: 449 pinned rows have fully empty DMS components and stay null
geometry, 3 rows carry direction letters outside the nominal set (`W`
latitude, `N` longitude) that the plain DMS formula converts without sign
flip, and one-sided empty DMS rows fail closed. The weekly refresh schedule
is stopped. The archived HIFLD OPEN Microwave Service Towers layer (DataLumos
project 239596, DOI 10.3886/E239596V1) and the DHS crosswalk DOI
(10.3886/E241367V1) return HTTP 403 anonymously and are banned as pipeline
inputs by the source policy, so archive identity is cited from the DataLumos
landing metadata rather than fetched payloads, and the exact historical
frozen-copy feature-count comparison remains unverified. Source edition,
baseline, schema/ID/coverage contracts, rights and release approval remain
unaccepted.

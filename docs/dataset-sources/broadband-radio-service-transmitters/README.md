# Broadband Radio Service (BRS) and Educational Broadband Service (EBS) Transmitters (FCC ULS)

The draft source is the weekly complete MDS/ITFS licenses file (`l_mdsitfs.zip`)
from the FCC Universal Licensing System public access downloads at
`https://data.fcc.gov/download/pub/uls/complete/l_mdsitfs.zip`, the official bulk
distribution to which the FCC moved ULS downloads in July 2021. `l_` files are
the weekly license-table exports (`licweekzipdata`); `a_` files carry the weekly
application-table exports. MDS/MMDS is the former name of the Broadband Radio
Service (BRS, radio service code `BR`) and ITFS is the former name of the
Educational Broadband Service (EBS, radio service code `ED`).

The published population is every location (`LO`) record of every license whose
header (`HD`) license status is `A` (active), radio services `BR` and `ED`
only, under the ULS-derived tower column contract (`UniqSysID`,
`Licensee`, `Callsign`, `LocNum`, DMS coordinates, address fields, `Nepa`,
`QZone`, `TowReg`, `SupStruc`, `AllStruc`, `StrucType`, `LicStatus`, derived
`latdec`/`londec`, `url`). `UniqSysID` and `LocNum` are retained unchanged and
jointly unique; `TowReg` is preserved as the source string. 84 published filed
sites carry antenna structure registration numbers. Published geometry is
declared in the native NAD83 datum (EPSG:4269) — the datum the FCC requires for
ULS location coordinates
(https://www.fcc.gov/wireless/systems-utilities/north-american-datum-1983-coordinates;
the BRS/EBS license-search help states location coordinates are referenced in
NAD83, https://wireless2.fcc.gov/helpfiles/licenseSearch/helpBrsEbs.html) — with
the DMS-derived decimal values written unchanged and no datum transformation,
reprojection, repair or geocoding.

Scope caveat (documented in the source lineage): BRS and EBS licenses are
geographic area licenses (47 CFR 27.1208); a blanket license covers all fixed
stations anywhere within the authorized service area, subject to narrow
individually-licensed exceptions (international coordination, environmental
assessment under 1.1307, radio quiet zones under 1.924). ULS therefore carries
only the site locations licensees filed, and this candidate is the FCC's
filed-site population for the 2.5 GHz band, not a census of physical
transmitters. Market areas, service-area polygons and centroids are never
substituted for filed sites.

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
counts and record-type prefixes, radio service roster (BR/ED only), licensee
and callsign joins (verbatim; a blank LO callsign is tolerated only when the
header callsign is blank, and every published row must carry a callsign),
duplicate `(UniqSysID, LocNum)` keys, DMS direction codes and derived-decimal
coordinate range. The license status distribution, active-location count,
null-geometry count (filed rows without usable coordinates are retained with
null geometry), full active ID roster digest and state roster digest are
recorded as per-capture audit observations rather than gates, so a regenerated
weekly edition is adopted and its drift against an approved release is
reported by the registered non-blocking approved-baseline drift check. Raw DMS
components are preserved verbatim even when out of nominal range. The weekly
refresh schedule is stopped. The archived HIFLD OPEN layer (DataLumos project 238712,
the inventory's download location) returns HTTP 403 anonymously, so the
historical column and coordinate-convention comparison remains unexecuted and
the derived-column convention is documented as unverified. Source edition,
baseline, schema/ID/coverage contracts, rights and release approval remain
unaccepted.

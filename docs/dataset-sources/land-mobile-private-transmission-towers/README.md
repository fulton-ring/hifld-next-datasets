# Land Mobile Private Transmission Towers (FCC ULS)

The draft source is the weekly complete Land Mobile {Private} licenses file
(`l_LMpriv.zip`) from the FCC Universal Licensing System public access downloads
at `https://data.fcc.gov/download/pub/uls/complete/l_LMpriv.zip`, the official
bulk distribution to which the FCC moved ULS downloads in July 2021. The
published population is every location (`LO`) record whose license header (`HD`)
status is `A` (active) and whose LO record carries both latitude and longitude
degree components — 603778 coordinate-bearing locations of 861663 licenses
(925853 active location rows; 322063 active filed locations without a complete
coordinate pair are excluded, matching the historical layer, whose 605297 rows
all carry geometry; a further 12 active coordinate-bearing filings in the FCC
do-not-convert local-datum territories are excluded from the published
coordinate-bearing population and preserved in the audit, see the datum
paragraph below) — under the historical HIFLD Land Mobile Private
Transmission Towers column contract (`UniqSysID`, `Licensee`, `Callsign`,
`LocNum`, DMS coordinates, address fields, `Nepa`, `QZone`, `TowReg`,
`SupStruc`, `AllStruc`, `StrucType`, `LicStatus`, derived `latdec`/`londec`,
`url`). `UniqSysID` and `LocNum` are retained unchanged; duplicate
`(UniqSysID, LocNum)` filed records (27 keys in the pinned edition, e.g.
superseded plus re-filed versions of one site) are preserved as distinct rows
exactly as the historical layer preserved its two duplicate keys. ULS private
land mobile licensing is site-specific, so this is the FCC's filed-site
population, not a census of physical towers.

The capture retains the complete zip body (425978433 bytes in the pinned
edition, sha256 `73670d67b85cb66c66d81bd4ea4228aa6ef659b7eb1ebf55cbf8bc11b8cba428`),
wire body and request receipt; the payload's own `counts` file creation date
(Sun Nov  8 09:16:00 EST 2026) and the HTTP receipt provide dated freshness
evidence distinct from fetch time, and the counts file's per-member record
totals are cross-checked against the parsed HD/EN/LO records (861663 / 1528207 /
2008597). Validation fails closed on capture roster and receipt drift, zip
member roster, per-record-type field counts and record-type prefixes (only the
pinned empty-trailing-field artifact rows — one HD, two EN — are tolerated, and
LO anomalies fail closed), the full 35-member counts-file map, radio service
distribution (30 codes, IG 609503 … SP 2) and license status distribution
(A 332080, E 420397, C 66936, T 42250), unique license and licensee identities,
LO/HD callsign join, the coordinate-bearing population count, the full active ID
roster digest, state roster digest (60 values, including the verbatim `$`,
`US` and lowercase `ca` source values), DMS direction codes, bounds,
empty-callsign rows (2, one license with no callsign filed), out-of-range
derived coordinates (1) and extra-field row counts. Raw DMS components are
preserved verbatim; the pinned edition contains one filed latitude (93 degrees,
Grundy County MO) whose derived decimal leaves the nominal range and 16 rows
whose direction components are empty or non-canonical (`''`, `0`, `3`, lowercase
`w`) — all are published as filed, with the derived columns following the
historical extract's hemisphere convention (only `S` marks southern latitude
and only `E` marks eastern longitude, so missing or non-canonical direction
components default to north/west, exactly as the frozen layer's own rows for
the same keys show), and pinned by count instead of being clamped or dropped,
because the FCC's own layer description anticipates exactly such errors ("It is
known that there are some errors in the licensing information - Latitude,
Longitude and Ground Elevation data"). The weekly refresh schedule is stopped.

The geometry column declares the FCC ULS source datum, NAD83 (EPSG:4269), not
WGS 84: FCC policy requires that "geographic coordinates provided to the
Commission via the Universal Licensing System must be referenced to the North
American Datum of 1983"
(https://www.fcc.gov/wireless/systems-utilities/north-american-datum-1983-coordinates),
so the filed DMS components and derived `latdec`/`londec` are published
unchanged with no datum transformation or reprojection. Per the FCC's
territorial conversion table, American Samoa and Guam filings were converted to
NAD83 with the FCC's specified shifts, while the do-not-convert Pacific island
areas (Baker, Howland, Jarvis, Johnston, Kingman Reef, Midway, the Northern
Mariana Islands, Palmyra and Wake) remain referenced to their applicable local
datum — the FCC publishes neither the local datum nor a conversion for those
areas, and a GeoParquet column declares one CRS for every row, so their filed
coordinates cannot be represented accurately under the published contract.
Those filings (identified by the ULS location state codes of the do-not-convert
territories: `MP` = Northern Mariana Islands, `UM` = the aggregated U.S. Minor
Outlying Islands containing the do-not-convert islands) are excluded from the
published coordinate-bearing population — 12 rows in the pinned edition, all
`MP` — and preserved in full (all filed attributes plus their derived
`latdec`/`londec`) in the source audit (`source_local_datum_quarantined_records`,
pinned by count and record digest), never silently removed, shifted or
mislabeled NAD83; they are not re-labeled NAD83 in the geometry column.

Column contract relative to the frozen copy: the published HIFLD Next v1.0.0
frozen copy (605297 point features, issued 2024-01-24) types `TowReg` as int64
with 0 for unregistered records and stores 0.0 for empty `SupStruc`/`AllStruc`
and single spaces for empty strings; the source instead carries empty strings,
`N/A` (82916 TowReg values) and real numbers, so the candidate retains
source-verbatim strings (`TowReg` as string) and nulls for empty numerics, and
documents the frozen artifacts as historical extraction artifacts (alongside
its `bbox` column and 8 rows whose stored latdec/londec carry more precision
than their own DMS columns derive). `LatDeg`/`LatMin`/`LonDeg`/`LonMin` are
typed float to match the frozen contract. The archived HIFLD OPEN layer
(DataLumos project 239397, the inventory row's archive link) and the DHS
crosswalk DOI (10.3886/E241367V1) return HTTP 403 anonymously, so archive
identity is cited from the DataLumos landing page rather than fetched payloads.
The frozen copy is retained as comparison-only context: its roster overlaps the
candidate's coordinate-bearing keys on 545011 of 605297; 60284 frozen-only keys
no longer carry coordinates in this edition (84 remain filed without
coordinates, the rest are absent from the current file), 58752 current-only
keys were filed or re-coordinated since, and 1976 overlapping rows have
source-edited coordinates while stable rows agree field-for-field. Source
edition, baseline, schema/ID/coverage contracts, rights and release approval
remain unaccepted.

Run focused checks: `uv run --frozen python -m unittest tests.test_land_mobile_private_transmission_towers`.

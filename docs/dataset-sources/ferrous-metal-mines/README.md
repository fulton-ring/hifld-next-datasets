# Ferrous Metal Mines source evidence

**Candidate status: draft; not approved for release.** The pipeline stages an
explicitly supplied preserved capture of the retired producer archive and
captures the surviving producer metadata document. It never fetches a default
URL, never substitutes IDs, and refuses any drift into quarantine.

## Source and lineage

The HIFLD inventory row is `ferrous-metal-mines`, agency US Geological Survey,
publisher "USGS, National Atlas of the United States". The DHS crosswalk is
[10.3886/E241367V1](https://doi.org/10.3886/E241367V1). The exact producer
publication is the National Atlas layer `minop3x020` ("Ferrous Metal Mines"),
surveyed by the USGS Minerals Information Team for operations active in 2003
and published 200512 (December 2005). The inventory describes the layer as
"legacy and not expected to be updated".

The surviving producer FGDC metadata document is still served by the USGS
National Geospatial Technical Operations Center legacy WAF at
`https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop3x020.xml`
(18,347 bytes, SHA-256
`8302b0f0028247f00d0a96a3b9d537c2e8db712f2425e09a2f1fc1317f94a232`, observed
2026-10-21 and byte-pinned in the pipeline). It declares the NAD83 horizontal
datum, a Shapefile distribution, and the attribute roster Minop3x020,
Commodity, Site_name, Company_na, State_loca, County, Latitude, Longitude, and
Plant_min. The original direct download
`http://dds.cr.usgs.gov/pub/data/nationalatlas/minop3x020_nt00020.tar.gz`
returns 404 on the live Data Distribution Service (observed 2026-10-21); the
producer data itself is removed from the live servers.

## Access status of the inventoried archive

The DataLumos project
[239022 version V1](https://www.datalumos.org/datalumos/project/239022/version/V1/view)
is the inventoried archive. Anonymous access returns a Cloudflare challenge
(HTTP 403, `cf-mitigated: challenge`, "Just a moment...") for both the project
page and API paths, observed 2026-10-21. No Wayback Machine snapshot of the
project exists, and archive.org services were themselves offline during this
session. The Canadian open-government mirror
(`6250_ferrous_metal_mines_2003.zip`) holds only Atlas 6th-edition raster
plates (JPG/PDF/metadata), not the vector layer. USGS MRDS is a different
producer dataset with different identifiers; substituting it, a mirror, or a
geocoded replacement requires an explicit maintainer decision, so none is used.

## Draft pipeline

`sources/ferrous_metal_mines.py` requires an explicit preserved capture of the
producer archive (`archive_path` plus a hash-pinned retrieval receipt). It
independently captures the pinned FGDC metadata document (live, or a preserved
replay with its own receipt), decodes the tar.gz members byte-strictly (DBF
field roster, fixed-point scale, ASCII, deleted-record and tail checks; SHP/SHX
point records, offsets, index and finite-coordinate checks; `.prj` CRS parse),
cross-checks each DBF LATITUDE/LONGITUDE decimal against its native point
within the documented rounding tolerance, and writes a typed GeoParquet
candidate with original IDs (`MINOP3X020`, plus implicit `source_fid` and SHP
record number), exact decimal types, and native CRS metadata. A WGS84 `.prj`
contradicting the FGDC NAD83 declaration is recorded as a datum contradiction,
not silently corrected. Content-derived versioning and the first-construction
witness bind code, complete source content, and output bytes; cache replay
verifies stored objects and preserves original receipts and wire bytes.

## Local materialization and release refusal

Isolated local Dagster evidence is recorded in
[`local-materialization.json`](local-materialization.json) using a synthetic
labeled fixture (the real archive is not anonymously obtainable, so no real
bytes could be pinned yet). Ingest run `c3f11f46-0fb3-41e5-a7c2-da053168e640`
materialized the registered ingest asset and passed the blocking
`ferrous_metal_mines_population_complete` check; cache replay run
`ee3e1538-6cda-4f34-8856-f356e52688ad` reused the sealed candidate unchanged;
registered publish-partition catalog run `2ff9d598-783d-4f34-ad05-de1007e8d7ca`
produced the standard quality manifest (0 null/invalid geometries, native NAD83
points, decimal(9,3)/decimal(11,3) attributes preserved). The exact-object
promotion attempt was refused with `ReleaseApprovalError:
missing_independent_release_approval_registry` and the published inventory
remained empty before and after the refusal. No publication occurred.

The draft refresh schedule is defined and stopped. Schema/ID acceptance,
baseline comparison against a real HIFLD capture, coverage/vintage decision,
rights, attribution, and release approval remain maintainer decisions. The
concrete next requirement is an authorized DataLumos download (or a maintainer
decision on a substitute source) so the real producer archive can be captured
with a dated receipt and pinned.

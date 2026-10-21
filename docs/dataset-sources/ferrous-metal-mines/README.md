# Ferrous Metal Mines source evidence

**Candidate status: draft; not approved for release.** The pipeline stages an
explicitly supplied preserved capture of the retired USGS producer archive and
captures the surviving producer metadata. It makes no maintained-source or
current-freshness claim.

## Source and lineage

The HIFLD inventory row is `ferrous-metal-mines`, agency US Geological Survey,
publisher “USGS, National Atlas of the United States”. The DHS crosswalk is
[10.3886/E241367V1](https://doi.org/10.3886/E241367V1). The producer
publication is National Atlas layer `minop3x020` (“Ferrous Metal Mines”),
surveyed by the USGS Minerals Information Team for operations active in 2003
and published 200512 (December 2005). HIFLD describes it as legacy and not
expected to be updated.

The original producer download URL in the FGDC metadata is
`http://dds.cr.usgs.gov/pub/data/nationalatlas/minop3x020_nt00020.tar.gz`.
It now returns 404 on the live USGS endpoint. Its exact package was recovered
from the Internet Archive capture `20121002015130`. The CDX record reports
HTTP 200; both the original archived content length and complete downloaded
body are 6,269 bytes. The SHA-256 is
`4e1e4ede4969241285f67a010405ca6296d496622bb4ef99304a3f82877112bc`. The
package contains `minop3x020.dbf`, `.shp`, `.shx`, and `.txt`, and has no
`.prj`. The original URL, capture URL, response headers, CDX response, package,
metadata, and receipts are retained in
`/workspace/86-review-evidence/capture/20261021/`.

The producer FGDC XML remains on the USGS National Geospatial Technical
Operations Center legacy WAF at
`https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop3x020.xml`.
It is 18,347 bytes with SHA-256
`8302b0f0028247f00d0a96a3b9d537c2e8db712f2425e09a2f1fc1317f94a232`. Its
`onlink` names the same original tarball and it declares `North American Datum
of 1983` and `Decimal degrees`. Since the package has no `.prj`, the pipeline
uses that exact hash-pinned declaration for NAD83; it does not infer or correct
a datum from coordinates.

The HIFLD inventory also points to DataLumos project 239022 V1. Anonymous
DataLumos access still returns a Cloudflare challenge; this capture therefore
does not establish byte identity with that repository’s copy or the HIFLD
version. The Canadian archive found during research holds raster plates, and
USGS MRDS is a different dataset. Neither is used as a substitute.

## Native source inspection

The archive contains all 24 point features. The nine DBF fields retain their
physical source declarations: `MINOP3X020 N(2,0)`, `COMMODITY C(10)`,
`SITE_NAME C(21)`, `COMPANY_NA C(37)`, `STATE_LOCA C(10)`, `COUNTY C(11)`,
`LATITUDE N(9,3)`, `LONGITUDE N(11,3)`, and `PLANT_MIN C(10)`. `MINOP3X020`
IDs are unique and contiguous from 1 to 24. Each DBF literal value and each native SHP WKB point is preserved unchanged in
the 24-row GeoParquet candidate. The maximum observed difference between
DBF `LATITUDE`/`LONGITUDE` and SHP Y/X is 0.00090426318336 degrees. The
physical DBF coordinate scale is three decimal places (0.001 degree per unit),
but FGDC defines no DBF-to-SHP equivalence or rounding rule. The pipeline
therefore uses 0.001 degrees only as a proposed diagnostic sanity bound; this
is not a producer-approved coordinate mapping or acceptance decision. No
coordinate is rounded, replaced, transformed, or reprojected. All 24 per-row
literal values and deltas are recorded in the retained source audit. The
`candidate/typed-source.parquet` and `geoparquet/ferrous-metal-mines.parquet`
objects are byte-identical. Full parity details are in
`/workspace/86-review-evidence/native-parity.json`.

## Draft pipeline and verification

`sources/ferrous_metal_mines.py` requires an explicitly supplied archive and
hash-pinned receipt. It preserves archive bytes, all original members, the
FGDC metadata and receipts, validates DBF/SHP/SHX layouts, ID continuity, the proposed DBF/SHP coordinate
sanity bound and geometry, and writes an unapproved typed
GeoParquet candidate. Numeric DBF fields preserve their exact decimal width
and scale. The source contract, historical baseline, mapping, rights and
release remain unapproved.

The actual registered Dagster ingest and blocking population check passed for
all 24 features (run `7be90306-9dc3-4f55-9e3a-eb62ccbb53da`). A second run
used the local preserved inputs with network access disabled and reused the
same sealed candidate (run `d2f379e9-94e6-4977-9a1d-aa7ec49086b5`). The
registered catalog asset passed its quality check with 24 features and zero
null or invalid geometries (run `23f7e63f-f527-4a72-8df2-d847da0e30f4`). The
exact-object promotion gate refused because no independent release approval
registry was supplied; the published inventory stayed empty.

The current registered-gate proof ran against an isolated copy of this
candidate and a populated three-object synthetic published inventory. Its
false-quality copy was created only for the negative gate case: the real
candidate’s passing quality manifest and all staged objects remained
unchanged. Both the quality-failure and empty-approval cases left the
synthetic published inventory unchanged. Evidence is in
`/workspace/86-review-evidence/registered-gate-run/registered-gate-proof.json`.
Focused source tests passed 11/11.

The draft refresh schedule remains stopped. Historical HIFLD baseline and
record-level comparison, coverage/vintage interpretation, schema and ID
acceptance, rights, attribution, and release acceptance remain undecided.

# Ferrous Metal Processing Plants historical reconstruction candidate

- Inventory ID: `ferrous-metal-processing-plants` (issue #87); legacy USGS National Atlas mineral operations layer `minop4x020`, explicitly "legacy and not expected to be updated". No maintained agency feed is known.
- Producer: U.S. Geological Survey (Minerals Information Team's 2003 Automated Minerals Information System survey; Minerals Yearbook Volume 1). Producer FGDC metadata declares publication month `200512`; no exact day is published, so release dates remain unknown with that recorded reason.
- Original producer publication `https://dds.cr.usgs.gov/pub/data/nationalatlas/minop4x020_nt00021.tar.gz` is retired. The pinned capture is the Internet Archive identity snapshot `https://web.archive.org/web/20161212153047id_/https://dds.cr.usgs.gov/pub/data/nationalatlas/minop4x020_nt00021.tar.gz` (byte-identical across repeated fetches and the `id_` identity form; archived `x-archive-orig-last-modified: 2005-12-14`). Role: `archive` — a historical archive of the producer publication, not a maintained feed or an authorized current distributor.
- Producer-published historical metadata, still served by USGS and pinned: `https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop4x020.xml` (role `producer_publication`; the `Last-Modified: 2014-10-03` is a document edit time, not a data release). The bundled `minop4x020.txt` FGDC text metadata inside the package is retained as raw evidence.
- DataLumos HIFLD item (`https://www.datalumos.org/datalumos/project/239023/version/V1/view`, inventory modification date 2024-04-09) returned Cloudflare CAPTCHA to anonymous API and page requests on 2026-10-21 from this environment. It is kept as a separately labeled fallback mirror candidate; it was not fetched, and no mirror substitution was made. Equivalence between the National Atlas package and the HIFLD 2017-12-18 copy is unresolved.
- Local job: `ferrous_metal_processing_plants_draft_refresh`; its monthly schedule is STOPPED.

## Pins and payload (verified 2026-10-21)

- Package tar.gz: 7420 bytes, SHA-256 `6137b0a5c6f3f5dc3a92262abc4e879001ff4f82f3d660e6214a0b01c3b3d168`, members exactly `minop4x020.dbf` (6991), `minop4x020.shp` (1192), `minop4x020.shx` (412), `minop4x020.txt` (18119). No `.prj` sidecar exists.
- Metadata XML: 18646 bytes, SHA-256 `5580370deb4f4589c9e37a05c58bb223f847cbd1b090d6ab8d24088e0aa72eaa`.
- 39 point features. DBF header 321, record width 171, trailing `0x1A`. Fields and declared physical types, preserved exactly:
  `MINOP4X020 N(2,0)`, `COMMODITY C(26)`, `SITE_NAME C(29)`, `COMPANY_NA C(57)`, `STATE_LOCA C(14)`, `COUNTY C(12)`, `LATITUDE N(9,3)`, `LONGITUDE N(11,3)`, `PLANT_MIN C(10)`.
- Original IDs `MINOP4X020` 1..39 in DBF order and SHP record order; DBF record indices and SHP record numbers are separate physical identity columns. No substitute row identifiers are generated.
- Native SHP point coordinates carry more precision than the declared 0.000278-degree attribute resolution; the maximum per-record coordinate-vs-attribute delta (about 0.0012 degrees) is recorded in the audit and is not corrected or reconciled.
- CRS: the package ships no PRJ and the producer metadata declares geographic coordinates on the North American Datum of 1983. The candidate records a null GeoParquet CRS plus that declaration; no datum correction, no synthesized PRJ, no WGS84 assertion.

## Proposed (unapproved) contract

- Raw evidence: full tar.gz package, USGS metadata XML, every wire body, extracted masters, and hash-pinned receipts under `raw/`; preserved masters under `shapefile/`.
- Candidate output: one typed GeoParquet (`geoparquet/ferrous-metal-processing-plants.parquet`) with the nine declared fields at declared decimal widths/scales plus the two physical identity columns and native WKB points. Shapefile ZIP, GeoPackage, and PMTiles are unsupported outputs pending an approved mapping.
- Every acceptance flag (`source_release_accepted`, freshness, schema/ID contract, coverage, rights, mapping, baseline, release) is false. Publication requires maintainer source, baseline, schema/ID, rights, and release approvals; a potential-breaking-change decision is required before any contract is accepted.

No production write, release acceptance, or source acceptance is implied.

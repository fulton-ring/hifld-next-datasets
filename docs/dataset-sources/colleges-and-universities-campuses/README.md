# Colleges and Universities Campuses

Inventory ID `colleges-and-universities-campuses` (issue #39). The maintained
successor is NCES EDGE **postsecondary reporting-institution points**, not the
retired 2022 ORNL campus polygons.

## Maintained successor

Maintainer decision 2026-11-17 (issue #1055): publish NCES EDGE postsecondary
school locations as the successor to this dataset. The successor is **points**.
It does not include campus outlines, ORNL enrichment or the historical HIFLD
published identifiers.

- Producer and publisher: National Center for Education Statistics (NCES),
  U.S. Department of Education, Education Demographic and Geographic Estimates
  (EDGE).
- Product: Postsecondary School Geocodes, school year **2025–2026**.
- Landing page: <https://nces.ed.gov/programs/edge/Geographic/SchoolLocations>
- Complete ZIP:
  <https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_POSTSECSCH_2526.zip>
  (2,651,816 bytes, SHA-256
  `4325bd9d8ab69155a95381f4a09eb62ad75e7960c288af6c192f224ca49b5114`).
- Technical documentation:
  <https://nces.ed.gov/programs/edge/docs/EDGE_GEOCODE_POSTSEC_TECHDOC.pdf>
  (477,818 bytes, SHA-256
  `77aafe42e538bfcb27227be0102c2ed3fcb5b2e47e5c78c33cf723b52ebecf42`).
- Coverage: 6,438 reporting-institution points in 59 state and territory codes,
  native EPSG:4326, native text `UNITID`, 21 DBF attributes and original file
  FIDs. Every native value, type and WKB point is retained.
- Last data update: the ZIP identifies school year 2025–2026. Its embedded
  metadata gives creation 2026-03-05 and processing 2026-03-10; the exact
  publication date remains unknown. The embedded metadata describes an annually
  updated IPEDS population, so the expected cadence is one school-year release
  per year. The school-year label does not date each coordinate.

The reporting-institution point is not one physical campus. An NCES reporting
institution can represent a main campus, a main campus with satellite or
off-campus locations, a community-college district or a university system. This
successor therefore has national reporting-institution coverage, not an
exhaustive campus inventory, and it carries no campus polygons.

## Implementation

The existing Colleges and Universities pipeline (`#38`,
`ingest/colleges-and-universities/colleges-and-universities`) already reads this
same NCES product. This dataset is a **derived alias** over that adapter rather
than a second fetch:

- `src/dagster_hifld/assets/colleges_universities_campuses.py` depends on the
  Colleges and Universities NCES ingest and stages a byte-faithful alias under
  `colleges-and-universities-campuses/colleges-and-universities-campuses/{version}`.
- The alias renames the native GeoPackage and GeoParquet to the target file slug,
  rewrites the manifest title and prebuilt-output path, and records the
  derivation as a transformation. Source inputs, rows, geometry and provenance
  stay the source candidate's.
- The alias does not pin a school year in its own code. It inherits whatever
  release the Colleges and Universities adapter selects. Current `main` pins the
  current 2025–2026 release; PR #1018 (issue #1073) adds latest-published-school-
  year discovery to that adapter, and the alias inherits it without a code
  change.
- The shared publish graph catalogs and releases the alias independently. All
  approval flags stay `False` and the monthly schedule stays `STOPPED`.

Pipeline input URLs fetched by the source adapter:

| URL | Publisher | Last data update |
| --- | --- | --- |
| <https://nces.ed.gov/programs/edge/Geographic/SchoolLocations> | NCES, U.S. Department of Education | Landing page for the 2025–2026 release |
| <https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_POSTSECSCH_2526.zip> | NCES, U.S. Department of Education | School year 2025–2026; metadata created 2026-03-05 |
| <https://nces.ed.gov/programs/edge/docs/EDGE_GEOCODE_POSTSEC_TECHDOC.pdf> | NCES, U.S. Department of Education | Technical documentation for the 2025–2026 release |

## Historical ORNL campus-polygon product

The inventory row describes a 2022 ORNL/DOE product of campus polygons: 5,898
polygons across 59 jurisdictions, with ORNL enrichment such as population,
enrollment and NAICS fields. That product is not maintained, and its original
publication is not available as a permitted pipeline input. The DataLumos archive
link in the inventory is a prohibited archive host, and the only complete
surviving copies are PASDA mirrors, also prohibited. The historical
preservation draft was unregistered on 2026-11-12 because its only inputs were
those mirrors; its module and tests remain as audit context, not as a live
pipeline.

The successor does not reconstruct those polygons. No buffer, hull, convex
hull or synthetic boundary is an acceptable substitute for a campus outline, and
no ORNL enrichment or historical published-ID mapping is inferred from NCES.
Historical ORNL parity remains unmeasured rather than disproved.

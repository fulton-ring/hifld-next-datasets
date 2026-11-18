# Public Schools (NCES EDGE)

This draft candidate uses the official NCES EDGE public-school locations archive for the 2024-25 Common Core of Data year. The archived HIFLD Open Public Schools dataset is described by DataLumos as 2022-23 CCD data. The producer is shared, but this implementation has no feature-level crosswalk and does not claim historical HIFLD equivalence.

The retained NCES archive contains 102,178 point records in EPSG:4326. The pipeline preserves all 23 native shapefile attributes in source order, the 12-character NCESSCH identifier, and each source point coordinate. It checks the complete archive member roster and CRCs, all school IDs, coverage bounds, coordinate attributes, and every value in the accompanying pipe-delimited text file against the shapefile. GeoParquet is the only prebuilt candidate output. No release, source, freshness, baseline, schema, ID, coverage, rights, or mapping acceptance is asserted; the refresh schedule is STOPPED.

The source ZIP, decoded response, wire response, and HTTP receipt are retained in the local evidence packet. The isolated no-Git Dagster replay on the clean-main integration, candidate cache reuse, clean quality check, injected invalid-geometry quality refusal, and exact-object release refusal are recorded in [local-materialization.json](local-materialization.json). The persistent run database and full run record remain under `/workspace/work/281-evidence`; no production dataset was promoted.

## Edition refresh audit — 2026-11-15 (#957)

The [NCES School Geocodes & Geoassignments page](https://nces.ed.gov/programs/edge/Geographic/SchoolLocations)
was fetched on 2026-11-15. Its public-school download links run through
[`EDGE_GEOCODE_PUBLICSCH_2425.zip`](https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2425.zip);
the 2025–26 section currently links a **postsecondary** file. A newer section
heading therefore does not establish a newer public-school edition. NCES describes
CCD as annual; that is the input collection cadence, not a guarantee of an annual
archive publication date. The archive Last-Modified in source.json remains the
2026-10-20 observation until a separately dated capture is recorded. The complete
older execution in local-materialization.json is retained unchanged.

The runtime now discovers the release on every live run:

- **Discovery:** a live run GETs the official
  [NCES School Geocodes & Geoassignments page](https://nces.ed.gov/programs/edge/Geographic/SchoolLocations)
  and selects the newest school-year `EDGE_GEOCODE_PUBLICSCH_<yy><yy+1>.zip` named by a
  real anchor. Future-year links that NCES leaves in HTML comments and script/chrome
  links are ignored. The page is fetched again after the ZIP, and the run refuses if the
  selection changed during capture.
- **Completeness:** the publisher-written DBF header count, SHX index count, SHP record
  walk and decoded row count must agree, and the complete companion TXT member must match
  every shapefile value. No school year, row count or ID digest is pinned in code; the
  discovered school year is recorded in the audit, manifest and candidate metadata.
- **Replay:** `capture_directory` reads exactly `source.body`, `source.wire` and
  `source.receipt.json`, derives the release from the receipt URL and verifies request
  identity and response digests. It makes no request and establishes no new freshness.
- **Cache:** source bytes, record/geometry/schema hashes, the capture receipt and
  processing identity determine the version. Replaying the same capture checks immutable
  objects and the sealed construction state before reuse.

At capture time on 2026-11-18 the newest active public-school anchor is still
`EDGE_GEOCODE_PUBLICSCH_2425.zip`; the 2025-26 section's public link is commented out and
only its postsecondary link is active. A newer active anchor will be selected
automatically without a code change.

### Review and adoption procedure

Discovery and completeness are code behavior. Source-edition acceptance and release
approval remain separate, reviewed decisions:

1. Confirm the official landing page, selected anchor, producer and school year, and
   retain the bounded landing/ZIP receipts outside Git. Never synthesize a successful
   receipt or rename an old capture as fresh.
2. Compare the newly selected population against the prior accepted capture, including
   added/removed IDs, schema and coverage. Equal totals do not establish equal
   populations. Schema drift is a contract change that needs review; it is not a count
   pin.
3. Bind any acceptance to the accepted baseline's dataset/file/version, catalog/object
   checksums, contract and complete candidate rows through `release_approval.py`. Keep
   all runtime acceptance flags false and the schedule STOPPED. Code merge does not
   authorize publication.

### Cellular comparison (read-only)

At the audit base, `sources/cellular_towers.py` GETs the official rolling weekly
[FCC ULS complete cellular-license ZIP](https://data.fcc.gov/download/pub/uls/complete/l_cell.zip).
Unlike NCES's year-specific URL, that URL can expose another edition without code
changes. Its parser still pins HD/EN/LO totals, license statuses, active-location
count, full composite-ID roster and state roster; ordinary publisher additions or
removals therefore refuse validation. It retains attempts and supports sealed
capture replay, but fetching a rolling URL does not make changed populations
adoptable. The resulting population is filed locations on active cellular licenses,
not an exhaustive census of physical towers. No FCC request, new weekly capture,
row comparison or adoption was performed for this audit; FCC implementation work
is coordinated separately under #955. Neither pilot establishes historical HIFLD
identity or an accepted release baseline from a matching producer alone.

### Current execution evidence

At source commit `4dbabf09`, a fresh official archive GET at
`2026-11-15T20:24:26.327625+00:00` returned HTTP 200, identity encoding and
30,448,565 bytes, SHA-256
`9dc51ecd5cdf6e38bdb679f7ac95e2bf73ee3fb48718b8612cf830800cffb288`.
This matches the pinned archive; no newer public-school edition was acquired.
Two isolated Dagster materializations of that retained response passed the ingest
and blocking population check: `b1d59c08-ea22-4b2a-b751-aee63ad48f4b`
(first construction) and `7c060859-59eb-4081-936d-434bd9a49fdf` (cache reuse).
Both returned version `v85e48552129823430edfc1cf4e1e9644`, all 102,178 records,
and source-capture SHA-256
`c2b731db7e20cd6d2e207ca5a330a65aeae129fe864cd5b2e9ce19f617ed7897`.
The candidate GeoParquet readback is 6,563,017 bytes, SHA-256
`287cf32e965d6856c5c0e14d4e12169c3f1d9c7514182e72fa53bc66756cacd7`.
Direct `require_release_approval` refused these actual objects with
`missing_independent_release_approval_registry`; their bytes were unchanged and
the isolated published destination remained empty. All acceptance flags stayed
false. The nine focused tests include a registered Dagster HTTP-503 refusal with
no asset materialization, archive-drift quarantine, invalid-ZIP refusal after live
capture cleanup, rejected replay symlinks, row/ID/schema drift and native GeoParquet preservation.

Evidence and scratch runners remain outside Git under `/tmp/hifld-957-evidence`
and `/tmp/hifld-957-*.py`. An earlier concurrent native attempt was killed with
exit 137; another pre-commit attempt refused mismatching construction code blobs.
Neither is counted as successful execution. The successful committed-source runs
were serialized. Catalog regeneration, a changed annual edition, an exhaustive
prior-edition row diff, an accepted HIFLD baseline comparison and a full release
Dagster graph were not run in this audit. These results grant no release acceptance.

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

The runtime has three distinct behaviors:

- **Replay:** `capture_directory` reads exactly `source.body`, `source.wire` and
  `source.receipt.json`. It verifies request identity and response digests, then the
  archive's byte/hash, member, population, all-ID, schema, coordinate and TXT-value
  pins. This makes no request and establishes no new freshness.
- **Update detection:** without that directory it GETs the hard-coded 2425 URL.
  Replaced bytes, changed rows/IDs, and schema drift refuse the candidate. A new
  annual URL is invisible to this job. Its STOPPED schedule is a pinned-edition
  acquisition/verifier, not demonstrated annual edition discovery.
- **Cache:** the source capture evidence (including receipt observation time),
  native record/geometry/schema hashes and processing identity determine the
  version. Replaying the same capture and code checks immutable objects and the
  sealed construction state before reuse. Another live receipt or changed code
  can produce another candidate version even when source bytes are unchanged;
  a version change alone does not prove a new publisher edition.

Acquisition, receipt and archive/conversion refusals retain bounded response evidence under unique `_quarantine/public-schools/`
keys, with no ready candidate or construction-state seal. This change repairs two
losses: failed HTTP responses previously produced only a failure marker, and a
post-download validation refusal tried reading the wire file after its temporary
capture directory had been deleted. Quarantine now uses the already captured bytes,
keeps the original receipt separately, and identifies the attempted body by its own
hash. This is evidence retention, not permission to parse or adopt drifted data.

### Review and adoption procedure

There is no runtime edition selector or automatic adoption path today. The smallest
supported adoption is a reviewed, source-specific adapter change:

1. Inspect the official NCES landing page, resolve the public-school link and verify
   publisher/product/school year. Retain that page and a bounded full GET's original
   receipt, wire and decoded body outside Git. Keep existing captures unchanged;
   never synthesize a successful receipt or rename an old capture as fresh.
2. Inspect every outer/nested ZIP member and CRC before extraction. Inventory the
   full native schema/types, all NCESSCH strings, row count, bounds and all TXT
   values. Reconcile every added/removed ID and all retained-record attributes and
   coordinates against the prior capture, including geography/category coverage.
   Equal totals do not establish equal populations; samples cannot establish
   completeness. Legitimate additions/deletions require new reviewed edition pins,
   not disabling count/ID checks. Schema drift requires an explicit contract change.
3. In one reviewed PR, update the source URL/file/vintage, archive/member names,
   bytes/hash/count/ID/bounds pins, year-specific audit/manifest/input identifiers,
   asset description/check expectations and this evidence together. Retain old
   immutable staging/published paths. Add regressions for the actual edition
   differences and exercise ingest, cache replay, catalog and refusal paths in
   isolated staging. Until that work is done, a newly discovered annual URL remains
   an unadopted source proposal; the existing job will not acquire it.
4. Reuse [#417](https://github.com/fulton-ring/hifld-next-datasets/issues/417)'s
   existing `source_manifest.json`, native verification, baseline comparison and
   `release_approval.py` boundary. Bind comparisons to the accepted baseline's
   dataset/file/version, catalog/object checksums, contract, code and complete
   candidate rows. Source-edition acceptance and release approval are distinct.
   An independent deployment-owned `HIFLD_RELEASE_APPROVALS` decision must bind
   exact objects and the accepted baseline/contract; staging manifests cannot
   approve themselves. Keep all runtime acceptance flags false and the schedule
   STOPPED. Code merge does not authorize publication.

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

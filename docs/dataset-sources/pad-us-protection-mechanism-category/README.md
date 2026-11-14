# PAD-US Protection Mechanism Category — source evidence

Research snapshot: 2026-10-20, updated with dated probes on 2026-11-13. This file
records source identity and bounded access probes; the draft pipeline pins the
maintained USGS-linked view service. It does not establish a release, accepted
schema/ID contract, or publication approval.

## Official source path

USGS's [PAD-US Web Services](https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-web-services)
page lists "Protection Mechanism Category" and links the feature-layer view
(ArcGIS item `80c7a138989c4cf8937c4502371d6e99`; fetched HTTP 200 with a browser
User-Agent on 2026-11-13 and captured by the pipeline as identity evidence).
The underlying public layer is
`https://services.arcgis.com/v01gqwM5QqNysAAi/ArcGIS/rest/services/Protection_Mechanism_Category/FeatureServer/0`;
its hosted service item is `22670023fd124c799d5ddd08297dde85`. Both ArcGIS
Online items are owned by `usgs.gov` accounts (`mcroft@usgs.gov_USGS`,
`sboyer@usgs.gov_USGS`) in USGS's organization `v01gqwM5QqNysAAi` (item
documents captured 2026-11-13). This is an explicitly USGS-linked
distribution, not an inferred third-party mirror. The service describes the
combined PAD-US view of fee, designation, easement, marine, and proclamation
categories.

## Version relationship (documented, unaccepted)

Observed 2026-11-13:

- The service's own labels are stale producer labels: the layer is named
  "Protected Areas Database of the United States (PAD-US) v3.0" and the
  copyright cites the 2022 PAD-US 3.0 release (DOI `10.5066/P9Q9LQ4B`).
- The service's data was last edited **2024-03-20** (`editingInfo.lastEditDate`
  1710963253666 = 2026-03-20T19:34:13Z minus timezone; captured live).
- USGS's published FGDC metadata for this service ("PAD-US Protection
  Mechanism Category", datacommons.psu.edu metadata clearinghouse copy)
  describes it as **PAD-US 4.0**, publication date 2024-04-25, processing
  environment `PADUS4_0Geodatabase.gdb`.
- The [USGS PAD-US Data Download](https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-data-download)
  page (bot-blocked to this runtime's plain retrieval; cited lineage context)
  identifies PAD-US 4.1 (2024; DOI `10.5066/P96WBCHS`) as the latest release.
  ScienceBase item `652d4fc5d34e44db0e2ee45e` ("PAD-US 4.1 Full Inventory
  Database") answered **HTTP 200 anonymously on 2026-11-13** (the 2026-10-20
  research had recorded 403) and lists `PADUS4_1Geodatabase.zip` (1,523,434,496
  bytes) plus version-history and FGDC metadata files; the ZIP URL is a
  `sciencebase.usgs.gov/manager/...` link whose anonymous fetchability was not
  verified this session.
- The 4.1 national geodatabase publishes the separate PAD-US feature classes,
  not this combined producer view, so the draft pins the maintained
  USGS-linked view service and records the version relationship as an
  unaccepted maintainer freshness/succession decision. Do not relabel the
  service's observed content as 3.0, 4.0 or 4.1.

## Live population and extraction probes (2026-11-13)

Anonymous JSON access verified:

- `returnCountOnly` → `{"count":441609}` (also 441,609 on 2026-10-20).
- `returnIdsOnly` → 441,609 unique OBJECTIDs, contiguous 1..441,609 (no gaps);
  the sorted-roster SHA-256 is pinned in the source module
  (`6971d2092b3ae91a8f3ec092687fd184a1057ce622d832e9686e9b6fa677d765`).
- Layer document: polygon geometry, hasZ/hasM false, native SR 102100/3857,
  `maxRecordCount` 2000, pagination supported, `OBJECTID`/`GlobalID` id fields,
  37 fields (pinned plain-field SHA-256 `2afc737f...25310e` in the module).
- Geometry conventions verified on 4,098 rings over 3,200 probed features:
  every ring closed, clockwise exteriors, counter-clockwise holes, two-ordinate
  vertices, no null geometries observed.
- Transfer: the producer answers **gzip** when requested — a 1,000-feature
  page near offset 200,000 transferred 0.71 MB gzip in ~1.7 s while identity
  transfer of the same content is ~3.75x larger. The population's geometry
  weight is heavily skewed: the first 1,000 features alone transferred 82 MB of
  gzip wire in ~251 s (huge national polygons at the head of the keyset), while
  mid/tail 1,000-feature pages ran ~0.3-0.8 MB and ~2-3 s. A complete capture
  is therefore planned as a resumable gzip keyset walk
  (`where=OBJECTID>last ORDER BY OBJECTID ASC`, 1,000-feature pages, tail-page
  witness), not as fixed 2,000-row `resultOffset` requests (which timed out on
  2026-10-20 under identity transfer).

No complete capture, candidate, or production materialization was produced by
the probes; the draft module retains every response and wire receipt in a
resumable capture directory, re-verifies the pinned count/IDs/schema/extent
before and after the walk, quarantines incomplete or drifted captures, and
keeps all release approvals false with the refresh schedule stopped.

## Complete capture (2026-11-14)

- The resumable gzip keyset walk completed: all 442 1,000-feature pages plus
  the empty tail-page witness (OBJECTID>441,609) were retained with
  per-response receipts between 2026-11-14T02:59Z and 2026-11-14T03:47Z, and
  count/IDs/schema/extent were re-verified unchanged before and after the walk
  (ids `6971d209...`, schema `2afc737f...`, count 441,609).
- The full pass over the retained population computed the per-value and
  per-geometry baselines now pinned in the source module: record
  `19b85e32...8d6619`, geometry `368ea3a4...cb5f707`.
- The producer serves 342 features whose rings cannot form a valid OGC polygon
  (302 `source_polygon_ring_degenerate`, 40 `source_polygon_hole_unassigned`)
  and 3,479 further polygons that fail OGC validity (self-intersections and
  similar producer defects; verified independently over the retained pages).
  The first capture attempt failed closed on the unusable rings as designed;
  both observed counts are now pinned, the unusable-ring candidate geometries
  stay explicitly null-gated (never silently coerced), the invalid-but-usable
  polygons are retained verbatim without repair, and no producer null
  geometries exist beyond the gated ones. The pinned counts are part of the
  gated-null contract the blocking ingest check and the catalog population
  gates verify; any change fails the next capture and is quarantined.
- No production materialization was published; pipeline verification ran on
  isolated local staging, and all release approvals remain false with the
  refresh schedule stopped.

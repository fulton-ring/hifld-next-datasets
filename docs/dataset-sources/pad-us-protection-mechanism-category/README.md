# PAD-US Protection Mechanism Category — source evidence

Research snapshot: 2026-10-20, updated with dated probes on 2026-11-13 and
corrected/extended with dated probes on 2026-11-14. This file records source
identity and bounded access probes; the draft pipeline pins the maintained
USGS-linked view service. It does not establish a release, accepted schema/ID
contract, or publication approval.

## Official source path

USGS's [PAD-US Web Services](https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-web-services)
page lists "Protection Mechanism Category" and links the feature-layer view
(ArcGIS item `80c7a138989c4cf8937c4502371d6e99`; fetched HTTP 200 with a browser
User-Agent on 2026-11-13 and again on 2026-11-14, captured by the pipeline as
identity evidence). That item's `url` field (owner `mcroft@usgs.gov_USGS`, org
`v01gqwM5QqNysAAi`) is the maintained combined view service:

`https://services.arcgis.com/v01gqwM5QqNysAAi/ArcGIS/rest/services/Protection_Mechanism_Category_PADUS/FeatureServer`
(layer 0 `PADUS4_1ProtMechCat`, observed 2026-11-14).

The view's anonymously readable `/0/sources` document names the underlying
source layer `ProtMechCat_A/FeatureServer/0` (hosted service item
`126eef9409ce48b0a9e55c2fe8fd77b8`; the item document answers anonymous
requests with HTTP 403 and the source service answers with token-required, so
neither is fetched — the sources document is the pinned view-to-input
relationship evidence, observed 2026-11-14). This is an explicitly USGS-linked
distribution, not an inferred third-party mirror. The view describes the
combined PAD-US inventory of fee, designation, easement, marine, and
proclamation categories.

## Superseded service (research context only, never fetched)

The 2026-11-13 session probed and first captured the combined service
`Protection_Mechanism_Category/FeatureServer` (hosted item
`22670023fd124c799d5ddd08297dde85`; 441,609 features; layer self-labeled
"Protected Areas Database of the United States (PAD-US) v3.0" with copyright
citing the 2022 PAD-US 3.0 release, DOI `10.5066/P9Q9LQ4B`; data last edited
2024-03-20; USGS FGDC metadata describing it as PAD-US 4.0, publication date
2024-04-25). On 2026-11-14 the USGS-linked view item was found to point at the
`Protection_Mechanism_Category_PADUS` view instead (656,986 features,
PAD-US 4.1 labels), which supersedes that older service. The older service's
2026-11-13 probes and its sealed 442-page 2026-11-14 capture are research
context only; the pipeline no longer fetches it, and its baselines/pins were
retired with the source switch.

## Version relationship (observed 2026-11-14, documented, unaccepted)

- The view layer's copyrightText cites the current release: "U.S. Geological
  Survey (USGS) Gap Analysis Project (GAP), 2025, Protected Areas Database of
  the United States (PAD-US) 4.1: U.S. Geological Survey data release,
  https://doi.org/10.5066/P96WBCHS" — the same release DOI as the PAD-US 4.1
  Full Inventory Database (ScienceBase item `652d4fc5d34e44db0e2ee45e`,
  observed HTTP 200 anonymously on 2026-11-13, listing
  `PADUS4_1Geodatabase.zip`).
- The layer is named `PADUS4_1ProtMechCat`; the service data was last edited
  **2025-06-04** (`editingInfo.dataLastEditDate` 1749055657551 =
  2025-06-04T16:47:37.551Z) and the schema last edited 2026-04-22
  (`lastEditDate` 1776880667386).
- The view service document itself carries an empty `copyrightText` and
  `serviceDescription` as served; the producer identity statement is pinned on
  the layer document.
- The 4.1 national geodatabase publishes the separate PAD-US feature classes,
  not this combined producer view; the draft pins the maintained USGS-linked
  view. The exact source release date and refresh cadence remain unaccepted
  maintainer decisions; do not relabel the view's observed content beyond what
  the producer's own documents state.

## Live population and extraction probes (2026-11-14)

Anonymous JSON access verified on the maintained view:

- `returnCountOnly` → `{"count":656986}`.
- `returnIdsOnly` → 656,986 unique OBJECTIDs; the sorted roster is contiguous
  1..656,986 (the served order is not ascending); sorted-roster SHA-256 pinned
  in the source module (`136194b0dfac75e6d781c82e4d3d2a2a0e39074059326628c9182730bd312c7b`).
- Layer document: polygon geometry, hasZ/hasM false, native SR 102100/3857,
  `maxRecordCount` 2000, pagination supported, `objectIdField` OBJECTID with an
  empty `globalIdField` (the view layer declares **no GlobalID field**),
  copyright pinned as observed, 37 fields — versus the superseded service:
  `WebMercAc` and `GlobalID` are gone; `WebServiceMangName` (string 150) and
  `TranspPct` (small integer, nullable) are added (pinned plain-field SHA-256
  `020f3a55a746d9280b8750d6401a8fdd6c1579ffb6a0c8d325dd854ff4762f4a`).
- Geometry conventions verified on probed features: rings closed, two-ordinate
  vertices, clockwise exteriors, counter-clockwise holes; query responses omit
  the optional hasZ/hasM flags (the non-Z/M shape stays pinned by the layer
  document; an explicit true remains drift).
- Transfer model carried forward from the 2026-11-13 probes of the superseded
  service: the producer answers **gzip** when requested; the population's
  geometry weight is heavily skewed (the first 1,000 features alone
  transferred 82 MB of gzip wire in ~251 s there), so the complete capture is
  a resumable gzip keyset walk (`where=OBJECTID>last ORDER BY OBJECTID ASC`,
  1,000-feature pages, empty tail-page witness), not fixed `resultOffset`
  requests.

No complete capture of the maintained view, candidate, or production
materialization has been produced yet; the draft module retains every response
and wire receipt in a resumable capture directory, re-verifies the pinned
count/IDs/schema/extent before and after the walk, quarantines incomplete or
drifted captures, and keeps all release approvals false with the refresh
schedule stopped. The per-value and per-geometry baseline digests stay
unpinned until the first complete capture of this view (656,986 features,
~657 pages); the superseded service's sealed capture (record `19b85e32...`,
geometry `368ea3a4...`, 342 unusable-ring features, 3,479 OGC-invalid
polygons) belongs to the retired source and is never an input.

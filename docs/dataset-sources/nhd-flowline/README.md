# nhd-flowline — Flowline (USGS 3DHP_all layer 50)

HIFLD Open inventory entry `nhd/flowline` ("Flowline", agency US Geological
Survey, issued 2025-02-27). Researched 2026-11-13.

## Crosswalk match

The DHS HIFLD Open crosswalk (DOI 10.3886/E241367V1) consolidates the
NHD-family rows into one "3D Hydrography Program (3DHP) Layers" row (Open ID
`650c07e542fa43b6a327c7f683681712`) whose Open REST Service is the USGS
`3dhp.nationalmap.gov` `usgs_3dhp_all` FeatureServer. This inventory entry's
description is that 3DHP service description verbatim (not the legacy-NHD
"Please use 3DHP for updated data" text carried by the `flowline-*-scale-*`
entries), and the service roster contains exactly one "Flowline" layer
(id 50). Match: **3DHP_all FeatureServer layer 50 "Flowline"** — the maintained
producer publication, not a mirror.

## Producer publication

- Primary: `https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all/FeatureServer`
  (USGS The National Map / NGTOC).
- Alias: `https://hydro.nationalmap.gov/arcgis/rest/services/3DHP_all/FeatureServer`
  (same agency; captured as an identity witness, not a fallback — the capture
  pins both, and the pipeline fetches only these two hosts plus the USGS page
  below).
- Provenance page: `https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products`
  (captured as `service_doc` evidence; www.usgs.gov requires a browser-like
  User-Agent, recorded in the fetch receipt).

## URLs the pipeline fetches as input (all USGS-owned)

| URL | Publisher | Last data update |
|---|---|---|
| `https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all/FeatureServer` (root, layer 50, `/query` count and keyset pages) | U.S. Geological Survey, The National Map — 3D Hydrography Program | Service `copyrightText`: "Data refreshed September 4, 2026." (service refresh, not a per-snapshot release date; no release date is published) |
| `https://hydro.nationalmap.gov/arcgis/rest/services/3DHP_all/FeatureServer` (alias root only) | same | identical copyright statement |
| `https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products` (documentation page only) | U.S. Geological Survey | product documentation page |

The DataLumos project 240108 copy named by the issue is identification and
comparison context only and is never fetched. No mirror, archive or
third-party host is a pipeline input.

## Observed population (2026-11-13, anonymous GET, receipts retained by capture)

- Feature count (pinned): **30,251,533** (`where=1=1&returnCountOnly=true`).
- Layer 50: `esriGeometryPolyline`, `hasZ=true`, `hasM=true`,
  `maxRecordCount=2500`, native SR EPSG:102100/3857, pages requested at
  EPSG:4326 (`outSR=4326`), 33 fields
  (OBJECTID … `workunitid`, service-generated `Shape__Length`).
- `returnIdsOnly` over the full population fails server-side (HTTP 200 error
  document, `code 500` — observed), so completeness uses the pinned count, the
  strict ascending OBJECTID keyset walk (`where=OBJECTID><last>`), the
  page-count ceiling and the empty tail-page witness instead of an enumerated
  roster. The maximum OBJECTID (30,251,595) exceeds the count, so the OID
  space has gaps and only a keyset walk is valid.
- Observed sample pages: 2,500-feature pages carried 6.4–8.5 MB; vertices
  carry exactly four ordinates (x, y, z, m); no null geometry, no
  out-of-range coordinates on the observed pages.
- `exceededTransferLimit` semantics verified on real pages: full pages can
  carry `true`; a short page must carry `false` (otherwise the capture refuses
  as truncated).

## Scale and resumability

One 2,500-feature page is ~6.4–8.5 MB of JSON, so the complete raw capture is
~100 GB per retained representation (wire + decoded body are both retained by
the repository's capture helper), takes multiple hours of sequential keyset
fetching at the observed 0.5–3.2 s/page, and roughly doubles again when the
sealed candidate copies the raw evidence into staging. The ingest therefore:

- streams with the FRS INTERESTS-style resumable capture state
  (`capture_state.json`, one retained page + receipt at a time);
- refuses to start unless the staging root has ≥ 350 GB free;
- pins the count and schema today, and leaves the per-value, geometry and
  ID-roster baseline digests (`""`) to be pinned from the first complete
  capture via `build_nhd_flowline_baseline` (reported in `computed_baseline.json`);
- keeps the complete-capture scale statement in the module docstring; changing
  the repository's raw-evidence-in-candidate contract for this dataset is a
  maintainer decision, not a local substitution.

## Succession notes (unaccepted, pending maintainer decisions)

- USGS retired the NHD as a maintained product on 2023-10-01; 3DHP is the
  successor and this service is its first general publication. The 3DHP schema
  (33 fields, non-persistent base-36 `id3dhp`) replaces the legacy
  NHDFlowline schema wholesale; legacy attributes (Permanent_Identifier,
  ReachCode, FType/FCode, Resolution) have no successor equivalents.
- EDH (elevation-derived hydrography) areas replace NHD-derived content over
  time (per-feature `workunitid`), so coverage changes between refreshes are
  expected and unaccepted.
- No per-snapshot release date is published; the September 4, 2026 service
  refresh statement is freshness evidence only.

All runtime approval flags stay false, the draft refresh schedule is stopped,
and promotion is refused without the independent release-approval registry.

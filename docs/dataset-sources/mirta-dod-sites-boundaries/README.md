# DISDI MIRTA DoD Sites - Boundaries draft

- Producer: Defense Installation Spatial Data Infrastructure (DISDI) Program,
  Office of the Assistant Secretary of Defense for Energy, Installations, and
  Environment (U.S. Department of Defense). The official machine distribution
  is the DISDI landing page download
  `https://www.acq.osd.mil/eie/imr/rpid/disdi/Downloads/installations_ranges.zip`
  (linked from `https://www.acq.osd.mil/eie/imr/rpid/disdi/index.html`),
  3,723,148 bytes, captured live 2026-11-12 with measured sha256
  `c8f15d5de307528ec4f7d2648181e5182c89ecd33d337a0e1a128817ce084e83`
  (the source publishes no hash, so this is a measured pin). The release notes
  PDF (110,939 bytes, sha256
  `132712b7a464158d75eed8f90bfdfe6c7880aa4310e01ab3605b97fdfbe3f638`,
  dated 2025-09-02) is captured alongside the landing page. acq.osd.mil sends
  an incomplete TLS chain; the adapter completes verification with the
  IdenTrust TrustID Server CA O1 intermediate from the leaf certificate's own
  AIA URI (sha256-pinned in `src/dagster_hifld/sources/mirta_dod_sites.py`),
  rooted at IdenTrust Commercial Root CA 1 in the standard trust store.
- The zip contains one File Geodatabase, `FY24_MIRTA_Final.gdb` (53 members,
  5,455,472 bytes uncompressed), with two layers: boundaries
  `FY24MirtaLocations_A` (825 MultiPolygon features, EPSG:4326) and points
  `FY24MirtaLocations` (831 Point features). The draft selects only the
  boundary layer; the point layer is census-receipted and belongs to the
  sibling `mirta-dod-sites-points` issue.
- HIFLD edition relationship: the HIFLD Open inventory entry tracked the
  FY 2022 Version 1 MIRTA edition (edition date 2023-07-31, DISDI custodian).
  The FY22 FGDC metadata bounding rectangle
  (-168.013054, 13.309109, 174.156519, 71.344323) is identical to this FY24
  boundary layer extent. The exact FY22 distribution
  (services1.arcgis.com `FY22_MIRTA_Boundaries_gdb/FeatureServer`, verified
  dead "Invalid URL" 2026-11-12) is gone and the DHS crosswalk
  (DOI 10.3886/E241367V1) routes the layer to the credential-gated GII portal
  (DUA not required, GII access required), so the draft ingests the current
  producer annual release instead (release notes: FY 2024 Base Structure
  Report sites plus FIRRMA 31 CFR Part 802 map sites; DISDI compiles each
  summer; next update stated 2024-07-31 cadence).
- Research corroboration, not pipeline inputs: the USACE-hosted MIRTA viewer
  service referenced by the DISDI landing page
  (`services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/mirta`,
  item owner `usace_crrel_als`, boundary layer 825 features)
  served boundary attributes multiset-equal to the FY24 GDB boundary layer
  after brace/case normalization on 2026-11-12 (its point layer holds 737
  rows, a DoD-side skew that is not ours to resolve). The REPI
  `repi.osd.mil` `FY19_MIRTA_Boundaries` hosted layer (745 features, 73
  California polygons, no Space Force components) is an older partial DoD
  copy that was not selected. Neither service is fetched by the pipeline.
- Boundary-layer census pinned in code: 13 producer attribute fields in
  producer order with GDB widths and release-notes aliases (countryName 25,
  featureDescription 2000, featureName 80, isCui/isFirrmaSite/isJointBase 3,
  mediaId/mirtaLocationsIdpk 40, sdsId GUID, siteName 100,
  siteOperationalStatus 4, siteReportingComponent 22, stateNameCode 5) plus
  SHAPE_Length/SHAPE_Area and native OBJECTID (825 unique ascending values,
  1..992 with producer gaps). sdsId keeps its 202 nulls and 4 duplicate rows
  across 3 shared GUID groups (one group spans two different siteName
  values); mirtaLocationsIdpk is a single-space literal on every row;
  featureName equals siteName on all 825 rows.
- Geometry fidelity: original GDB WKB subtype census
  {MultiPolygon 704, MultiSurface 121 (CurvePolygon rings with
  CompoundCurve/CircularString members)}; 724,735 stored source coordinates.
  The 704 linear features keep their exact GDB WKB; the 121 curved features
  are linearized through a temporary linear-only ESRI Shapefile vessel with
  GDAL's own arc code at the pinned 0.01-degree `OGR_ARC_STEPSIZE`
  (single-Polygon results wrapped deterministically into one-member
  MultiPolygon WKB). Candidate geometry: 3,808,488 vertices, bounds
  [-168.01305372299998, 13.30910940700005, 174.15651899800002,
  71.34432336800006], 28 invalid producer geometries preserved unrepaired
  (15 among the linear features, 13 among the linearized). Curved originals
  are retained bit-exact inside the raw archive capture. The linearization
  is a declared transformation and remains release-blocked pending an
  approved curved-geometry contract.
- Registered local job: `mirta_dod_sites_boundaries_draft_refresh`; its
  monthly schedule remains STOPPED. The blocking completeness check pins the
  825-feature population, unique OBJECTID coverage, zero null geometries and
  the 28-invalid-geometry census, and keeps `release_blocked`.
- TLS/hosts: the only pipeline-fetched hosts are
  `www.acq.osd.mil` (landing page, release notes, archive) - all
  producer-owned DoD infrastructure. The geoplatform/DHS services1 items from
  the crosswalk, the DataLumos archive copy and the PSU metadata mirror were
  read as research context only and are never pipeline inputs.

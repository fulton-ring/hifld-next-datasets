# PAD-US Protection Mechanism Category — source evidence

Research snapshot: 2026-10-20. This file records source identity and bounded
access probes only; it does not establish a complete capture, release, or
schema/ID contract.

## Official source path

USGS's [PAD-US Web Services](https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-web-services)
page lists “Protection Mechanism Category” and links a feature-layer view
(ArcGIS item `80c7a138989c4cf8937c4502371d6e99`). The underlying public layer is
`https://services.arcgis.com/v01gqwM5QqNysAAi/ArcGIS/rest/services/Protection_Mechanism_Category/FeatureServer/0`;
its service item is `22670023fd124c799d5ddd08297dde85`. The page describes a
combined PAD-US view of fee, designation, easement, marine, and proclamation
categories. This is an explicitly USGS-linked distribution, not an inferred
third-party mirror.

The live service identifies its layer as PAD-US v3.0 and its copyright cites
the 2022 PAD-US 3.0 release (DOI `10.5066/P9Q9LQ4B`). Layer metadata reports
polygon geometry, EPSG:3857, anonymous Query capability, `OBJECTID` as the
system-maintained object ID, `GlobalID`, and a 2,000-record page limit. A
2026-10-20 anonymous count query returned 441,609 features; the ID query
returned 441,609 IDs spanning 1–441,609. These are point-in-time service
observations, not a complete feature capture or a dated producer release.

There is a version gap to resolve. The separate [USGS PAD-US Data Download](https://www.usgs.gov/programs/gap-analysis-project/science/pad-us-data-download)
page says PAD-US 4.1 (2024; DOI `10.5066/P96WBCHS`) is the latest release and
links its national geodatabase through ScienceBase item
`652d4fc5d34e44db0e2ee45e`. The official web-service page still points to the
v3.0 service above. The ScienceBase page returned HTTP 403 to the available
retrieval path during this research. Do not relabel the v3.0 service as v4.1
or claim current release equivalence. Determine whether the intended draft
should represent the service's v3.0 view or a verified v4.1 derivative before
pinning an accepted source version.

## Extraction probes and limits

On 2026-10-20, anonymous JSON count and ID-only queries completed and returned
the population above. A request for ten complete features with geometry,
`outFields=*`, and `outSR=4326` returned 40,711 bytes in 0.267 seconds; a
100-feature request returned 1,094,796 bytes in 0.429 seconds. A 2,000-feature
request at the advertised page limit timed out after 20 seconds. An earlier
larger geometry request also failed to return within a bounded wait and was
interrupted. These probes show that metadata/count/ID access is available but
do not establish complete geometry-transfer feasibility. No page roster,
complete raw capture, candidate, or Dagster materialization was produced.

Required next work: test bounded page sizes against a full representative
capture plan; verify source stability with before/after count, IDs, schema,
extent, and service metadata; retain every response and wire receipt; then
compare the exact archived inventory candidate and a separately pinned
official release. If v4.1 ScienceBase access remains blocked, resolve whether
the official v3.0 view is an acceptable draft source and record the decision.
Any eventual refresh must quarantine incomplete pages or schema/ID/geometry/
coverage drift, remain stopped by default, and preserve all release approvals
as false.

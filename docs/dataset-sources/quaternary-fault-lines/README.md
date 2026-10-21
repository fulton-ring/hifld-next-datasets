# Quaternary Fault Lines source audit

The inventory entry matches the USGS Quaternary Fault and Fold Database (Qfaults), an official
USGS Earthquake Hazards Program publication. The live national layer is `haz/Qfaults` layer 21
("National Database") on `earthquake.usgs.gov`; it is not a PASDA, DataLumos, SeerAI, or Esri
HIFLD mirror.

The service identifies itself as the "2020 Release". That is a map-release label, not a dated
data-release or edition identifier. The inventory date (2024-03-12) and the DataLumos archive
modification date (2025-04-08) are archive metadata, not source release dates. On 2026-10-21 an
anonymous complete capture observed 112,809 features, OBJECTID 1 through 115,128, and
993,315 native XY vertices. The DataLumos project page returned an anonymous Cloudflare challenge,
so the archived HIFLD file was not downloaded or compared; the DHS crosswalk DOI is therefore a
cited historical reference, not a verified current source.

Schema/ID contract, HIFLD archive equivalence, coverage, rights, attribution, and release approval
remain open. The pipeline is a draft candidate producer only: it preserves every declared attribute
and raw response body, pins the complete ID/record/geometry/schema digests, and refuses to promote
on any drift. No production data is published.

The official USGS Faults page also links a downloadable GIS archive (retrieved 2026-10-21;
SHA-256 `447eadc5926256710d988c30e5996ba540604caa0154f9746c48bad637926893`). It contains a
112,809-feature EPSG:4326 shapefile and a 112,944-feature EPSG:4326 FileGDB layer. The shapefile
has no native OBJECTID and DBF-truncated field names; the FileGDB has a different native ID roster
and schema. These official variants do not establish HIFLD archive continuity or record-level
parity with the 112,809-feature EPSG:3857 API layer, so the candidate pipeline remains pinned to
the official API layer and does not substitute either download representation.

The refresh emits a fresh timestamped candidate on each scheduled execution. Exact-version replay
uses immutable writes: identical bytes are accepted, and any changed object under that version
fails closed. This is an immutable same-version guard, not a cross-run source cache. Replay of a
retained capture is available through the explicit `capture_directory` asset config.

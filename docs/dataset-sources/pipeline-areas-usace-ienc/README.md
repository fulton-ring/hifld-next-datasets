# Pipeline Areas (USACE IENC)

The current draft source is the USACE Army Geospatial Center's `PIPE_AREA`
layer (ID 82) in `USACE_IENC_Master_Service`, item
`6ee858a220a143a48c8d40a6cf0d3680`. The item is owned by the AGC's USACE
organizational ArcGIS account, describes the service as all IENC vector data,
and was modified in October 2026. The official [IENCCloud S-57 download
page](https://ienccloud.us/ienc_s57.html) is the authorized chart distribution
point and describes current product release 2.4. This establishes an official
USACE IENC source path; it does not by itself prove that layer 82 generated
the archived HIFLD snapshot.

The 2026-10-20 extraction returned all 112 `PIPE_AREA` polygons. The independent
object-ID roster and feature payload agree; native integer `OBJECTID`, string
`Feature_ID`, the complete source attribute set, and WGS84 polygon coordinates
are retained without conversion. The raw GeoJSON response and staged GeoJSON
share SHA-256
`1f8f1d7ba5d77736db7a627e9937722b4eb10f9e6a4f4392eb30a3abbbf006c5` (217,261
bytes). Source receipt includes the object-ID digest, schema, service links,
retrieval time and lineage limitation.

The extractor pins the reviewed count, ID roster and schema. It refuses to
stage a refresh if count, ID roster, schema, geometry, or native `Feature_ID`
uniqueness changes. A rejected refresh therefore cannot replace a previous
accepted staged version. The draft does not assert that the IENC service's
edit date is a pipeline survey date.

The HIFLD inventory identifies a 2022-edited archive listing 7,260 miles on 21
rivers. Its source package and exact extraction rules have not been compared
to the current service. The HIFLD archive/crosswalk endpoint did not provide a
readable extract during research. Maintainers still need to confirm same-item
continuity or successor semantics, source rights, and the schema/identity and
coverage contract before catalog or publication acceptance.

Machine-readable details and observed checksums are in [source.json](source.json).

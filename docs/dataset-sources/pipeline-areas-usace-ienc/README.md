# Pipeline Areas (USACE IENC)

The source is the exact `PIPE_AREA` feature class in USACE IENCCloud's official
IENC Feature Classes service, layer 82. USDOT BTS item
`3607d485a9854d6fa458bf507058ceae` is public, describes USACE/AGC bi-monthly
updates, warns users about inaccuracies in chart-derived data, and directly
links both the master file geodatabase download and the IENC Feature Classes
service. The official service directory lists `IENC_Feature_Classes/PIPE_AREA`
as a MapServer. This establishes the source's producer/delegation path.

The complete layer currently reports 112 polygons with native integer IDs
1–112, 12 non-geometry fields and EPSG:4326 coordinates. The acquisition
captures the BTS item, service directory, layer schema, independent count/ID
queries before and after, and the full ESRI JSON response page. Exact HTTP wire
bytes, decoded bodies and receipts are retained. The prebuilt GeoParquet
candidate preserves source field names and values, integer/string physical
types and polygon rings without reprojection or geometry repair. No missing IDs
or substitute IDs are synthesized.

Source release metadata does not expose a dated PIPE_AREA publication. The
BTS-described bi-monthly cadence is recorded separately from retrieval time.
This layer is limited to inland chart coverage; it does not represent all
pipeline assets across the United States. Its equivalence to the archived
HIFLD item and its original clipping rules remain unverified, as do rights and
the published schema/identity contract. The refresh schedule is registered as
stopped. Catalog candidates remain unapproved until maintainers resolve those
questions.

See [source.json](source.json) for the field declarations, checksums, URLs and
acceptance flags.

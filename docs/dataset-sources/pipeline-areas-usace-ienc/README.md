# Pipeline Areas (USACE IENC)

The source is the exact `PIPE_AREA` feature class in USACE IENCCloud's official
IENC Feature Classes service, layer 82. The current public USDOT BTS PDF item
`7ec34864c8804468a9cf52d437fe4b6c` describes USACE/AGC bi-monthly updates,
warns users about inaccuracies in chart-derived data, and links both the master
file geodatabase download and IENC Feature Classes service. The former BTS
map-service item `3607d485a9854d6fa458bf507058ceae` is no longer anonymously
accessible. The 2026-10-21 capture preserves the current item and service
response bytes. The prior 2026-10-20 capture used the old item; its retained
PIPE_AREA page has the same SHA-256 and decoded feature content as the new page,
but that does not establish or approve succession continuity between BTS items.
The official service directory lists `IENC_Feature_Classes/PIPE_AREA` as a
MapServer.

The complete layer currently reports 112 polygons with native integer IDs
1–112, 12 non-geometry fields and EPSG:4326 coordinates. The acquisition
captures the BTS item, service directory, layer schema, independent count/ID
queries before and after, and the full ESRI JSON response page. Exact HTTP wire
bytes, decoded bodies and receipts are retained. The prebuilt GeoParquet
candidate preserves source field names and values, integer/string physical
types and polygon rings without reprojection or geometry repair. No missing IDs
or substitute IDs are synthesized.

Source release metadata does not expose a dated PIPE_AREA publication. BTS item
7ec is used as the current distribution guard, not as proof that the prior
map-service record was superseded. The BTS-described bi-monthly cadence is
recorded separately from retrieval time.
This layer is limited to inland chart coverage; it does not represent all
pipeline assets across the United States. Its equivalence to the archived
HIFLD item and its original clipping rules remain unverified, as do rights and
the published schema/identity contract. The refresh schedule is registered as
stopped. Catalog candidates remain unapproved until maintainers resolve those
questions.

The exact-head clean-main implementation captured the current item and complete
layer in Dagster run `10c133e1-0172-4067-908b-b705cf3c6813`. A no-Git packaged
runtime replay of that retained capture succeeded in run
`21e23c18-3e6b-47d2-8951-69dad09f970a`, with catalog run
`bd52bb32-3096-4943-b36b-5b68fe8345e6`, cache-reuse run
`89fffab5-9e76-4f86-a9ec-6405c2d21ad5`, and all-row quality-check run
`a37abe05-5b4d-4f96-89db-6b2f15ad2c2c`. The registered verifier refused the
exact candidate for missing approval in run
`9d4cade7-225e-44c8-bde8-48352463a45c`; a populated isolated published fixture
was unchanged. The package identity scheme, capture hashes, candidate digest,
and approval limits are recorded in [source.json](source.json).

A separate registered-graph negative test copied that candidate into an
isolated staging root and changed only the copy's quality manifest to
`quality_check_passed: false`. The blocking `publish/catalog` quality check
failed in run `da5796af-56e4-4249-83d3-e74c0b5650c9`. The selected verifier
and promotion assets had no start events, and the populated fixture remained
unchanged. The original staged candidate, source capture and positive quality
evidence were left intact. The Dagster event record and candidate-copy hashes
are retained under `/workspace/work/270-clean-evidence/false-quality-graph-run.json`.

See [source.json](source.json) for the field declarations, checksums, URLs and
acceptance flags.

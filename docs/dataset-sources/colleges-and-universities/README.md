# Colleges and Universities

## Maintained edition research — 2026-11-16

The official School Locations HTML still publishes **2025–2026** as the newest
postsecondary download. Its real anchor targets
`/programs/edge/data/EDGE_GEOCODE_POSTSECSCH_2526.zip`; a later year must be
discovered from a published anchor, never guessed from the date or obtained from
another school family. HTML comments are not published download links.
The producer ZIP retrieved in this check is 2,651,816 bytes with SHA256
`4325bd9d8ab69155a95381f4a09eb62ad75e7960c288af6c192f224ca49b5114`.
The linked technical PDF has SHA256
`77aafe42e538bfcb27227be0102c2ed3fcb5b2e47e5c78c33cf723b52ebecf42`.
The landing page describes IPEDS as an annual survey; the school-year label
identifies the reporting edition, not an exact publication date, coordinate
observation date, operational-status guarantee or full physical-campus roster.
The existing native field declarations remain the schema contract. Future
publisher schema or packaging changes need explicit source-adapter review.

The normal asset now selects that latest real postsecondary anchor and captures
the page, selected ZIP and technical PDF twice. Changed selection, ZIP or PDF
during the bounded observation is quarantined; there is no older-edition
fallback. The complete original nested archive and extracted metadata XML are
retained. Current completeness compares physical DBF and SHX counts, point
record/index structure, every decoded row and the producer's complete TXT
representation. The existing field/type/CRS/Point contract remains mandatory;
observed 2025–26 row counts, IDs and geometry hashes no longer constrain a new
current observation.

Candidate identity binds all captured evidence bodies and the processing code,
excluding retrieval times. Unchanged evidence reuses a fully verified sealed
candidate; a same-year corrected archive creates a distinct version. Original
construction witnesses and earlier version paths are preserved. The explicit
`HIFLD_COLLEGES_CAPTURE_DIR` option retains the historical pinned 2025–26 replay
contract. It is not a replacement for current release discovery. All approval
flags remain false and the monthly schedule remains STOPPED. Shared metadata
projection from the P1 change is required to carry the new source semantic and
documentation fields into downstream catalogs.

Complete NCES EDGE 2025–2026 reporting-institution location source: https://nces.ed.gov/programs/edge/Geographic/SchoolLocations . The adapter preserves all 6,438 UNITIDs, original file FIDs, 21 DBF attributes and native EPSG:4326 Points. It is an **unapproved changed successor** to the enriched DOE/ORNL product.

## Source fidelity review — 2026-11-15

The source is NCES Education Demographic and Geographic Estimates (EDGE),
**Postsecondary School Geocodes, school year 2025–2026**. The
[official School Geocodes page](https://nces.ed.gov/programs/edge/Geographic/SchoolLocations)
currently links the [pinned ZIP](https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_POSTSECSCH_2526.zip).
The [dated source check](https://github.com/fulton-ring/hifld-next-datasets/issues/954#issuecomment-6903214379)
retrieved 2,651,816 bytes with SHA256
`4325bd9d8ab69155a95381f4a09eb62ad75e7960c288af6c192f224ca49b5114`,
matching the existing adapter pin, and inspected 6,438 DBF records with 21 fields.
This was a byte/schema check, not a repeated full-population pipeline or historical
ORNL comparison; the retained complete execution evidence below remains separate.

[NCES IPEDS institutional-grouping guidance](https://nces.ed.gov/IPEDS/use-the-data/institutional-groupings-in-ipeds)
explains that a reporting institution can represent a main campus, a main campus
with satellite/off-campus locations, a community-college district or a university
system. These are reporting-institution points, not an exhaustive inventory of
every physical campus or evidence of operating status today. `UNITID` is the
native text identifier in a DBF C8 field; it is not one-to-one with OPEID.
`OGC_FID` is a separate file-local index, not institutional identity or an approved
historical published-ID mapping. The embedded metadata describes an annually
updated IPEDS population of postsecondary institutions participating in federal
student aid; completeness reflects that reporting universe when the shapefile
was created, not all institutions or all locations today.

The native fields are `UNITID, NAME, STREET, CITY, STATE, ZIP, STFIP, CNTY,
NMCNTY, LOCALE, LAT, LON, CBSA, NMCBSA, CBSATYPE, CSA, NMCSA, CD, SLDL,
SLDU, SCHOOLYEAR`. There is no operational-status field, OPEID crosswalk, ORNL
enrichment or campus polygon in this source. The WGS84/EPSG:4326 point coordinates
remain native; no geocoding, synthetic boundary or enrichment is implied.

The ZIP's embedded metadata identifies `SCHOOLYEAR=2025-2026`, creation
2026-03-05 and processing 2026-03-10. Its citation date is 2026-12-31, which is
future-dated relative to this review and is not an observed publication date.
The exact publication date remains unknown. The landing page identifies TIGER
2025 geography/locale inputs, 119th-Congress codes and OMB July 2023 metro/micro
delineations. School year, metadata processing, joined geography vintages and
coordinate observation dates are distinct; the school-year label does not date
each coordinate or establish present-day completeness.

The [ORNL product-family page](https://www.ornl.gov/project/geospatial-data-sets)
does not establish exact historical release membership, joins, enrichment or
geometry. Historical DOE/ORNL parity remains **unmeasured**, not disproved.
The maintainer must explicitly choose a named NCES reporting-institution
successor versus faithful recovery of the enriched ORNL product before accepting
a replacement. Keep existing versioned release paths stable; this draft does not
authorize silent replacement of an ORNL release. The separate
[campus polygon work (#39)](../colleges-and-universities-campuses/README.md) is not
reconstructed by these points, buffers or hulls.

Before successor acceptance, compare adjacent official NCES years by UNITID over
the full population: additions/removals, null/duplicate keys, names/addresses,
jurisdiction coverage, coordinate displacement, school years and geography codes.
Absence alone does not establish closure; investigate with matching-year IPEDS
status/universe definitions. ORNL parity needs the producer's exact edition,
field dictionary and documented join keys first. Source/product, rights,
contract/baseline and release acceptance remain pending; no acceptance is granted
by the source check or this documentation correction.

With fresh isolated `DAGSTER_HOME`, `HIFLD_STAGING_DIR` and `HIFLD_DATASETS_DIR`, and cloud bucket variables unset, run `uv run dagster job execute -m dagster_hifld.definitions -a defs -j colleges_source_integrity`. Optionally set `HIFLD_COLLEGES_CAPTURE_DIR` to the complete restored capture. The monthly schedule defaults STOPPED. Focused tests: `uv run python -m unittest discover -s tests -p 'test_colleges*.py'`; set `COLLEGES_TEST_CAPTURE_DIR` for complete-real cases.

Checksummed artifacts under the coordinator's `/workspace/work/hifld-coordinator/38/cleanup/` preserve full inputs, baselines, reports, replay scripts, native outputs and construction contexts:

- `original-evidence-929eceb.tar.gz`: SHA256 `0308ca2c2b6dabeb6aa279970573403e0935cdcb104f0162d02a172515b20fab`.
- `colleges-audit-native-artifact.tar.gz`: SHA256 `314214d79fa0bce37f04bf4d518e907b58e4460985ef5acb6cf71c1d0eada713`.
- `review-repair-artifact-6efc19d.tar.gz`: SHA256 `87769aa56703bc0e4b2acfee0112c8a7978ea8f6bbe52cb2d9ceb70f2beb9c7e` (current lifecycle/error tests and registered replay).

Source/successor scope, crosswalk, rights, contract, baseline and all ID/schema/type/coverage/year/CRS/geometry/format changes still require acceptance. Quality and independent release approval block promotion. Current results and decisions belong to issue #38 / PR #444; no source or release approval is granted here.

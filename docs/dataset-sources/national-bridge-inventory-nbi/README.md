# FHWA National Bridge Inventory SNBI draft

Producer publication: https://www.fhwa.dot.gov/bridge/nbi/nbi_data.cfm lists the newest
`nbi_data_YYYY.cfm` release. The pipeline discovers that year from the index, reads the
release page's per-state JSON links and counts, and fetches every state and territory file.
2025 was the final Coding Guide ASCII year; 2026 is the first SNBI-format release.
The 2026 page was updated 2026-10-07 and lists 53 state/territory files totaling 624,939 bridges.
The 2026 page's Massachusetts link points at `MS26.json`; the pipeline uses the state name to
request `MA26.json` (5,333 records, state code 25) and records the producer link correction.
The producer's SNBI submittal schema is https://www.fhwa.dot.gov/bridge/snbi/schema.cfm
(`SNBI.Schema.v04.json`), and the official Coding Guide transition map is
https://www.fhwa.dot.gov/bridge/snbi/datacrosswalk.xlsx.
Run registered `nbi_source_integrity` with isolated local staging; its quarterly schedule is STOPPED.
The asset's `capture_directory` run config selects complete original `.body`/`.receipt.json` inputs instead of live HTTP.
The output is one row per bridge. Every top-level SNBI field keeps its data tag, native JSON type and value.
Nested SNBI arrays (Features/Routes, Elements, Inspections, PostingEvaluations, PostingStatuses,
SpanSets, SubstructureSets, Works) are retained as JSON text rather than flattened or dropped.
Clean 1995 Coding Guide transitions from the crosswalk are recorded in each column's metadata.
SNBI fields with no clean Coding Guide equivalent keep their SNBI tag; no Coding Guide value is invented.
The 2026 files differ from the published schema inside nested arrays: Routes.BF01 is served but not
declared, and Elements.BE02 is declared required but often absent. Nested values are preserved as served.
Points derive only from SNBI B.L.05/B.L.06 decimal degrees (EPSG:4326); blank or partial coordinates
yield null geometry, while zero and out-of-envelope values are preserved and counted.
No producer row ID exists; the composite (BL01, BID01) pair is a proposed join key with duplicates
counted, not merged. The archived DataLumos 239684 copy is research context only and is not fetched.
Native GeoParquet only; raw bodies, receipts, declarations and first-construction witnesses persist in staging.
Source, schema/ID contract, coordinate basis, rights and publication acceptance remain pending human decisions.

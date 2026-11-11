# EPA FRS SEMS interests draft

Official federal catalog: https://catalog.data.gov/dataset/epa-facility-registry-service-frs-sems
The linked EPA_GEO service contains the complete FRS SEMS interest view, including non-NPL sites.
Run registered `epa_sems_source_integrity` with isolated local staging; its monthly schedule is STOPPED.
`HIFLD_EPA_SEMS_CAPTURE_DIR` selects complete original `.body`/`.receipt.json` inputs instead of live HTTP.
The adapter verifies complete ID-targeted pages and before/after IDs, count, schema and editing metadata.
All 32 attributes and original NAD83 points survive; geometry is never rebuilt from coordinate attributes.
Native GeoParquet only; raw bodies, original receipts, declarations and first-construction witnesses persist.
Producer OBJECTIDs are temporary. REGISTRY_ID, program IDs and KEY_FIELD remain distinct; no ID join accepted.
Unspecified-width OIDs use proposed int64; Date epoch milliseconds retain int64 and original UTC declarations.
These encodings and the published string-Date schema difference require human contract acceptance.
Current 14,242 versus published 14,761 coverage and all geometry/ID/type changes remain unapproved.
Quality/independent approval gates block release; no source, baseline, rights or release approval is implied.
Complete source/baseline captures and execution proofs are checksummed external artifacts linked on issue #80.

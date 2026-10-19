# Independent review repairs for Aquifers

The independent review of PR434 at `ac8a6a02cf3f6e6ea1c04e1257d2c8abf955a88f` found two P2 implementation defects: failed sequential writes exposed incomplete canonical versions, and self-repinned cached derivatives were trusted without fresh semantic reconciliation. These repairs change local candidate exposure and reuse validation. Producer data, contracts, release decisions and payload writers remain unchanged.

A complete candidate is now written into a hidden sibling build directory. Only after all source objects and the ready marker are present does a single directory rename expose the semver version. A per-version local lock prevents cooperating refreshes from replacing another completed version. Any interrupted build remains outside normal discovery and is removed; original input bytes and the failure report remain quarantined. Existing versions remain untouched.

Reuse now freshly decodes the pinned producer archive and the actual cached GeoPackage and typed Parquet. It reconciles all IDs, values/nulls, physical integer widths and Arrow nullability, native geometry/CRS, source provenance, code/runtime identity and the complete source-object roster. The original 20 invalid polygons remain unchanged. Self-repinned data, geometry, physical schema or attribution changes fail. Missing/extra outputs, malformed WKB and malformed inventory types fail. A rejected cached version moves as a whole into quarantine, preserving its bytes, original raw captures and other versions. Normal catalog metadata and its own bounded snapshots remain compatible, including after repeated catalog generation.

The new real-input regression cases exercise an injected typed-Parquet write interruption with the actual normal discovery iterator and ten self-repinned or malformed cache variants: Parquet attribute, integer width, native geometry, nullability and malformed WKB; GeoPackage attribute; source attribution; extra output; missing output; and a non-object inventory. Every case requires its specific failure, quarantine, no discoverable damaged version, and preserved raw/prior local fixture. One disposable fixture is reused by moving/restoring test-only copies; runtime never repairs rejected cache contents. Complete full-data copies are not retained per negative case.

Final results, run IDs and exact code head are recorded in the accompanying repair validation evidence. Earlier full-suite results remain separately attributed to their original tested head. No terminal-format rerun is inferred: the native payload writers and raw ZIP, GeoPackage, typed Parquet and canonical source record bytes remain identical to the earlier verified outputs. No generic sensor or Definitions changes occurred.

Both original review findings require independent narrow re-review of the final PR head. Source/baseline/contract acceptance and the real ID, schema, CRS, geometry and crosswalk decisions remain unresolved. No merge, issue closure or production write is authorized.

Verified repair source head: `4e7a9dbc261c67bbf0b5f5b822fa7207a68fd164`. Seven focused tests pass (449.187s), including every specific corruption reason, repeated real catalog snapshots and preserved raw/prior versions.

| Actual registered Dagster run | Result |
| --- | --- |
| `d9d5c952-7cbd-4a2d-9570-a03732587da0` | Complete original 4,637-row source SUCCESS |
| `735ab310-3529-4f78-9369-3a5b2345bade` | Fresh original-derived cache revalidation and idempotent reuse SUCCESS |
| `f40c6a5a-f9bb-4535-bd76-e013ab440780` | Expected normal quality denial for 20 unmodified invalid polygons |
| `7283f241-bac1-475c-8d97-1f015873cf31` | Expected existing verifier denial for missing independent approval registry |

All original raw/candidate objects and the prior local accepted-data fixture survive both denials. [`evidence/review-repairs/validation.json`](evidence/review-repairs/validation.json) records precise scope; complete negative probe results, native graph logs and unchanged payload hashes accompany it. The earlier 479-test full-suite result remains historical at its original tested head; no final-repair-head full-suite or CI success is inferred.

The follow-up independent review of source `4e7a9d` (identical source in docs head `6c17bbb`) found one remaining P2: changing only `gpkg_contents.srs_id` and repinning the ready hash could preserve a stale source-fidelity claim. Final source/tests checkpoint `d9b5df75a952c9552723fa33887c6def0e72fd88` reconstructs the canonical GeoPackage from the freshly decoded complete original rows. It compares all contents, geometry-column and spatial-reference rows, every native binary geometry header/envelope and all RTree bounds exactly. A measured deterministic complete-file match additionally permits strict whole-container byte equality, covering ancillary metadata, indexes and DDL. Any mismatch quarantines the existing version; no tolerance, geometry repair or cache repair is introduced.

Nine authentic complete-source self-repin variants passed at the metadata-check checkpoint `b6c3fe2` (323.588s): contents CRS/bounds, geometry-column CRS, substituted EPSG4326 definition, changed authority code, RTree bounds, geometry-header CRS/envelope, and the earlier attribute case. The final `d9b5df7` checkpoint passed four focused tests (118.390s), including an additional ancillary-table mutation rejected by the canonical-container guard. Every negative requires quarantine, absence from actual normal discovery, and preservation of original raw/prior fixture bytes. Disposable test fixtures are restored only after these assertions so one complete copy can serve multiple probes; production runtime never restores a rejected cache.

Final actual registered Dagster results at `d9b5df7`:

| Run ID | Result |
| --- | --- |
| `aa736700-be17-47e4-b706-e1489d48395c` | Complete original 4,637-row source SUCCESS |
| `e60beb45-69f1-463e-aafd-39704d8f1d99` | Fresh original-derived canonical cache reuse SUCCESS |
| `c085a30d-46fb-43bf-be83-f831beb24160` | Expected self-repinned contents-CRS cache rejection |
| `5131fa9c-e911-44c3-a710-e4817ec135ec` | Restored original test fixture reuse SUCCESS |
| `e2e72b8b-4fdb-435a-a921-e006e746593c` | Expected self-repinned EPSG4326-definition cache rejection |
| `169097dd-3ca4-43db-8c28-da1bf026924a` | Restored original test fixture reuse SUCCESS |
| `0601d7f5-836d-4aa0-bbce-a1e2434f1470` | Expected normal quality denial for 20 retained invalid polygons |
| `335c738f-4463-484b-a604-39c118f28ca8` | Expected missing independent approval registry denial |

The actual produced source and cache retain all 4,637 IDs, all seven attributes and nulls, physical types, original native Polygon geometry and coverage. Catalog input provenance remains identical to the validated source manifest. Raw ZIP, GeoPackage, typed Parquet and canonical records match the earlier verified payload bytes exactly; their writer block remains unchanged. No terminal-format rerun, final-head full-suite or CI pass is inferred. The earlier 479-test suite retains its original tested-head scope.

[`evidence/native-metadata-repair/validation.json`](evidence/native-metadata-repair/validation.json) records the precise checkpoints, negative results, final run logs and unchanged hashes. Native independent review of the exact final PR head remains pending. The original geometry, ID/schema/CRS, crosswalk and source/baseline/contract/release decisions remain blocked.

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

# Integrated native-policy verification

Source/test/runner head a5bb99654cd66148341a615b4d2826b3b498d42f integrates the independently reviewed shared 4800e4f5df550bfadd3631585268ce68a0fa25d9 through root merge fa50853a8c2aa30a96d95f35dbf8d12fb2443d2f. These are actual local tests, not CI or dataset acceptance.

All 15 actual Dagster run assertions passed: complete 1,693-source ingest, byte-idempotent refresh, two normal catalog/format cycles with missing approval, refresh after each, six original exact-reason cache attacks with whole bad-directory quarantine, restored complete positive, synthetic staged prebuilt-byte mutation and failed geometry-quality preservation. Source DATE/date32 values, all 564 SU_Date nulls, source FIDs, six native fields, native WKB and all original raw inputs remain unchanged. Every generic GeoParquet regeneration/reuse function was patched to fail if called; actual count was 0.

The exact prebuilt object policy is retained in source_manifest, data_dictionary, resolved_provenance and hash-bound authored manifest snapshots. The inherited/full-format bans remain hard bans; this producer's native artifact uses a separate exact-byte roster. No fake acceptance was created for the real dataset. Source/baseline/contract/ID/rights/geometry acceptance remains pending.

The full suite ran 526 tests: 525 passed, 1 intentionally skipped production-GCS write test, 0 errors, 221.800s. The real complete pinned sibling IaC snapshot 56aa10757129c457c530d707d9a40bae13e0907f was present; historical missing-IaC failures remain historical. Bad-cache archive manifests retain every member pin and restore argv; preserve the referenced complete runtime archives.

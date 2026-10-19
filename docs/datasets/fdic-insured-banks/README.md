# FDIC locations reconstruction draft — issue #82

This adapter preserves the complete official FDIC **locations** population. The existing HIFLD release contains branches and headquarters; replacing it with the institutions population would change its grain. Source selection, baseline, contract, rights, identifier mappings and the proposed coordinate interpretation require independent acceptance before release.

The pinned 2026-10-16 index has 78,028 records: 4,216 headquarters and 73,812 other offices. Eight full pages are bounded by two complete ID censuses. Counts, index and sorted unique identifiers agree; this is observed coherence, not a transactional snapshot guarantee. All original responses and aware observation receipts remain required external artifacts.

Canonical GeoJSON preserves every native property, JSON primitive, absent key and explicit null. Seven fields mix strings and integers, 18,270 records omit `CBSA_METRO_NAME`, 33,831 have explicit null `ACQDATE`, and 7,405 ZIP strings start with zero. Native `ID` strings and `UNINUM` integers remain separate. The producer's documentation and actual JSON physical types differ; the adapter retains the bytes' types.

Points are a proposed `[LONGITUDE, LATITUDE]` interpretation, not a producer-declared CRS approval. All 803 literal integer `(0,0)` pairs remain. Two latitude values exceed 90: native IDs `259663` (158.227518485, 6.880382388) and `522154` (138.097, 9.538). Their proposed geometries remain null and their literal coordinate properties remain unchanged. The full-population blocking check fails on these **805** rows. No swapping, geocoding, filling or row removal occurs.

The complete historical v1.0.0 candidate has 79,542 Parquet rows, including a literal header row and 5,766 zero points. Its `OBJECTID`, `source_ID`, `UNINUM` and other identifiers remain in original raw files and the complete compressed lineage sidecar. Diagnostic UNINUM joins do not approve an identifier mapping or type/date migration. GeoPackage, Parquet, Shapefile and PMTiles generation are explicitly unsupported until a lossless mixed-type/presence encoding and the relevant format contracts are accepted.

`fdic_locations_draft_reconstruction` runs source integrity and preservation **without** the blocking quality check. Its success establishes staging only. The full quality check remains registered; promotion also requires the independent release registry and verifier. The weekly refresh schedule is STOPPED. Official refresh rechecks all 24 page/census requests; schema, values, count, coverage or index drift is quarantined for an explicit new source decision. The published baseline is never automatically refetched or accepted.

Reproduce from the complete preserved capture artifact, using the filenames and SHA-256/length pins in `fdic_locations_spec.py` plus the receipt pins in `replay.json`:

```sh
PYTHONPATH=src python scripts/verify_fdic_locations_draft.py \
  --capture-dir /operator/preserved-fdic-captures \
  --runtime-dir /operator/empty-isolated-fdic-runtime
HIFLD_FDIC_LOCATIONS_TEST_CAPTURE_DIR=/operator/preserved-fdic-captures \
  PYTHONPATH=src python -m unittest discover -s tests -p 'test_fdic_locations*.py'
```

The six-record fixture is structural evidence only. Full source/baseline captures and generated audits are checksummed artifacts, not Git fixtures. Matching original artifacts are required for historical replay; live refetch is not a substitute. Actual execution results and exact commits will be recorded after the portable source checkpoint is published and independently reviewed. No CI, release approval or production materialization is asserted here.

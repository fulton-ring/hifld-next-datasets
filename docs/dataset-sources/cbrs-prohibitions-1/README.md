# CBRS Prohibitions working draft (#30)

This implements a local reconstruction from the full **producer-hosted FWS 2024-12-19 ZIP**, not an archive-refresh substitute. Source, baseline, field/ID mapping and release acceptance remain pending. The user expressly authorized reviewable draft code and local tests before contract approval; this does not remove any release gate.

`src/dagster_hifld/sources/cbrs_prohibitions.py` reads the exact `20241219_CBRS_Prohibitions` shapefile, preserves all 25 ZIP members and all 4 original input bodies/receipts, and builds a complete hidden candidate before atomic installation. `assets/fws_prohibitions.py` explicitly registers source ingest; the normal registry supplies catalog, formats, quality checks and publication gates. Its monthly on the 1st at 13:00 America/New_York source-only schedule is **STOPPED**, with explicit local-storage guards.

The complete source has 1,693 rows: 1,121 Polygon and 572 MultiPolygon; all native geometries are valid, in NAD83/EPSG:4269. Six DBF fields are preserved, including **DATE** values (`GPKG DATE` and Arrow`date32`) and 564 genuine null`SU_Date` values. Intrinsic shapefile FIDs 0–1692 are retained under the proposed `__source_fid` string column; the primary shapefile has no DBF`OBJECTID`. Original DBF floating-point (`F`, width 19, decimal count 11) scientific-notation ASCII bytes remain in the immutable raw archive; decoded float64 representations are reconciled against the source. Generated container FIDs do not replace source IDs.

The adapter independently re-derives full actual GeoPackage and Parquet rows, physical Arrow schema/field metadata, native CRS, geometry and complete source-owned object roster on every reuse. GeoPackage checks also reconcile contents/geometry-column bounds and SRS declarations, referenced spatial-reference definitions/authority and every geometry-header SRS. A self-repinned ready manifest is insufficient. Changed edition, members, count, schema, identifiers, null coverage, malformed receipts, transfer errors or cache drift are quarantined before a discoverable candidate or accepted output can be changed.

Normal GeoPackage-to-GeoParquet conversion currently changes native date columns to strings. This pipeline explicitly excludes that regeneration, supplies its independently verified native-date32 GeoParquet, and leaves a published string-date mapping for review. Regenerated shapefile and PMTiles are similarly excluded for evidenced ID/geometry loss. No format exclusion approves another mapping. `evidence/physical-dbf-audit.json` independently decodes every original DBF C/D/F field and proves all 1,693 decoded values equal the adapter input, retaining every original field string.

`evidence/complete-comparison.json` compares **every row** of the complete, separately pinned published candidate. Both populations have 1,693 rows, but published geometries are all MultiPolygon: 571 are exact native WKB matches; 1,691 match only after diagnostic single-part wrapping; 2 geometry joins remain unresolved. Of 1,691 uniquely joined rows, 1,685 have attribute differences, including 1,679`SHAPE_Leng` and 927`SHAPE_Area` differences. All original published`OBJECTID`/`id` values remain in the full candidate and report; the geometry-join crosswalk is diagnostic and unapproved.

The official landing page directly delegates the USGS MapServer/1 service and links the producer ZIP/metadata. A separately captured complete service population has 1,693 unique OBJECTIDs. Exactly 1,691 original-order native coordinate-ring arrays match the ZIP; 2 differ, and the API numeric fields retain additional precision relative to the original DBF F field payloads. `evidence/native-api-comparison.json` identifies the differences. **The pipeline explicitly chooses the pinned producer ZIP and does not assert ZIP/service geometry equivalence or a new 2026 release.** The full original API response is preserved as audit evidence, not silently substituted into the pipeline.

The DOI crosswalk and DataLumos returned origin 403 at the recorded final URLs. The archive crosswalk is therefore explicitly unmatched; those failures do not block the reachable producer ZIP pipeline. Complete original responses and receipts are retained. Historical issue 40 background was read separately and contains no CBRS-specific findings.

Reproduce with the repository's locked environment and Tippecanoe available on PATH:

```sh
PYTHONPATH=src uv run python -m unittest discover -s tests -p test_cbrs_prohibitions_source.py
PYTHONPATH=src uv run python scripts/verify_cbrs_prohibitions_draft.py --runtime /tmp/cbrs-prohibitions-isolated-new
PYTHONPATH=src uv run python docs/dataset-sources/cbrs-prohibitions-1/evidence/compare.py
PYTHONPATH=src uv run python docs/dataset-sources/cbrs-prohibitions-1/evidence/api_compare.py
PYTHONPATH=src uv run python -m unittest discover tests
```

The runtime path must not already exist. The runner uses isolated DAGSTER_HOME, local staging and publication fixtures, complete real inputs, registered partitions and actual Dagster source/refresh/normal-format/quality/approval runs. Failed approval or quality gates must preserve the preexisting synthetic accepted fixture. Synthetic fixtures demonstrate implementation behavior only, not acceptance of this dataset.

The full-suite test requires the **real** sibling `hifld-next-iac/environments/prod/dagster.tf`. This session uses the complete unmodified sibling snapshot at 56aa10757129c457c530d707d9a40bae13e0907f; historical missing-sibling failures elsewhere remain historical. No IaC fixture, suppression, infrastructure change or deployment is involved. Local tests are not CI success.

Release requires explicit source/rights and edition acceptance, resolution of the two ZIP/service and two published geometry differences, native-versus-published geometry/date/decimal schema decisions, a non-destructive ID contract, and independent exact candidate/contract/baseline release decisions. Full FWS metadata use constraints remain authoritative; this data is not a survey or definitive in/out determination near boundaries. Do not merge, close or publish production data from this draft.

Actual implementation verification at `a4116a0079ee33bd9c96316493fa7b69f0161ba8`: **509 passed, 1 intentionally skipped, 510 total** (139.503s) with the real sibling IaC snapshot. `evidence/execution.json` records all actual Dagster run IDs: complete source, byte-idempotent refresh, two normal format/catalog runs with missing approval, post-catalog refreshes, three complete real-candidate self-repin metadata attacks and synthetic failed quality. Every gate test preserved the prior local publication fixture. `evidence/actual-candidate-object-inventory.json` pins every actual source candidate and normal graph object. Independent exact-head review is pending; these results do not establish real dataset acceptance.

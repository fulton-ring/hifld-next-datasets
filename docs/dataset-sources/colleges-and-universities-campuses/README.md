# Colleges and Universities Campuses — unapproved historical draft

This implements a **2022 HIFLD/ORNL campus historical preservation candidate**, not a maintained current-source replacement. PASDA supplies two complete third-party historical mirrors. The original DHS catalog and independent ORNL publication establish the product family; neither establishes acceptance of these exact mirrored release bytes. The user-authorized draft scope permits implementation and local tests while source selection, compatibility and release decisions remain pending.

This clean-main recovery uses the six original body/receipt pairs restored from retained source commit `cb98e912ad6f55503c68d84faf539ad0dfc97482`. They are held outside Git at `/workspace/work/39-evidence/captures`; the source was not requested again. The validation and run records described later in this file belong to earlier legacy-branch checkpoints and do not establish a packaged-runtime replay for this clean-main implementation.

## Clean-main packaged-runtime evidence

The source implementation was committed at `ccdc7b0f510512a5b7f680cc17d0e7e8501a2988` before the isolated runtime replay. The replay used a source package tree with no `.git` metadata and emitted `sha256-packaged-dagster-hifld-v1`, with identity `packaged-sha256:0bf16ea9967927d4499f935399cf4b58a33f1b40e179a6fd560fbc75b979dfea`; it did not claim an observed clean Git working tree. A focused test verifies that the adapter computes and validates this packaged identity, and the registry test verifies that the source asset, blocking check, and STOPPED schedule are present. Both tests passed.

The registered offline source run `4bdc9a73-bae6-47d7-b677-1f750f8aec6b` created draft version `v0.0.0-draft-campus2022-dc353315e4c3-1a5e98e35af843ec` from the restored captures. A separate raw decode matched every native EPSG:3857 primary table attribute, null/date/type value, source FID, and WKB byte-for-byte across all 5,898 rows. All six original raw bodies also matched their immutable staging copies and original receipt hashes. The 15 invalid polygons remain unrepaired; the source manifest and candidate READY record remain unapproved. Durable evidence, including the persistent Dagster database and full parity record, is under `/workspace/work/39-evidence/`.

The actual catalog/quality graph run `1dfb0188-0f83-406d-b3a2-8157027a1948` selected catalog, quality, format, verify, and promote steps. Its registered quality check failed with 15 invalid geometries and correctly prevented verify and promote from starting. A separate approval-only run `53d7c6b7-d5e2-4871-8ab3-d39d4c2d4a4d` selected verify and promote with an empty approval registry and intentionally omitted the catalog quality check because the unchanged source candidate fails that check. Verify failed on the missing exact object approval; promote did not start. The source quality manifest remained false, and both gate runs left the populated synthetic prior PublishedStorage fixture unchanged. Neither run is a quality pass, source acceptance, or publication.

## Inputs and complete comparison

All six original input bodies and their complete HTTP request/final URL, observation interval, size and SHA-256 receipts are retained outside the repository at `/workspace/work/39-evidence/captures`. The adapter pins every original archive member and physical DBF declaration in `src/dagster_hifld/sources/campus_preservation_spec.py`. Both archives contain **5,898 campus polygons**, covering all original 59 jurisdiction codes. They are not NCES institution points, buffers, hulls or partial regional substitutes.

| Input | Interpretation |
| --- | --- |
| PASDA `Colleges_and_Universities_Campuses.zip` | Complete native EPSG:4326, 27 original attributes including OBJECTID, original file FIDs and all geometry |
| PASDA `colleges-and-universities-campuses-hifld.zip` | Complete native EPSG:3857, 26 original attributes, original file FIDs and all geometry; proposed canonical representation only |
| Preserved published GeoParquet | Complete unapproved historical candidate, 5,898 rows, 24 original attribute values plus published id/geometry/bbox |
| Preserved HIFLD collection metadata | Historical publication context; no current source or acceptance inferred |
| Federal DHS catalog raw record | HIFLD dataset identifier `MGMT-GMO-HIFLD-290527`, modified 2022-10-24; distribution list empty |
| ORNL geospatial data sets page | Producer product-family lineage; no acceptance of exact mirrored release bytes |

All 24 historical published attribute values match both archives on the **diagnostic original UNIQUEID join**. Every original UNIQUEID, source FID, source OBJECTID where present, and published ID remains available. Published IDs have 5,888 unique values and ten duplicate groups (IDs 1–10, twenty affected rows); no source ID is substituted for them. The source's native date32 and DBF integer-width interpretations are preserved rather than silently adopting historical published string/type conversions.

The native EPSG:3857 geometry differs from every published geometry at binary/ordered-coordinate level. Descriptive maximum normalized coordinate difference is about 0.000050005 m; this is **not an accepted tolerance**. Diagnostic projection/wrapping of EPSG:4326 explains some historical differences but is not used to construct the native candidate. Both originals and the published candidate retain fifteen invalid geometries. No geometry repair, polygon wrapping, reprojection or domain exception is approved.

The archived FGDC metadata attributes ORNL and says publication 2022-07-18, source dates 2017-03-29 through 2022-07-18, and public-domain/public-use terms. These are preserved historical claims, not an independently accepted current release, maintenance cadence or license decision. The old original ArcGIS item is inaccessible and its old service returns an ArcGIS error; reachable current HIFLD landing pages do not themselves expose a verified replacement. DataLumos/DHS crosswalk reads that return provider 403 remain unverified.

## Working local draft

- `sources/campus_preservation.py` requires explicit complete preserved captures, bounded regular-file reads, exact HTTP/body/archive/member/schema/ID/count/coverage pins, and rejects source drift before construction. Malformed receipts are retained byte-for-byte as untrusted diagnostics. Oversized originals remain untouched in place with a bounded diagnostic, not a false complete-capture claim.
- Lossless raw bodies and observations remain outside versioned candidates. The deterministic native EPSG:3857 GeoPackage/GeoParquet retain all source rows, native attributes/types/nulls/IDs/WKB; a separate native EPSG:4326 table and complete published Parquet remain in `candidate/`. Full reports explicitly retain unapproved mappings and blockers.
- Shared provenance validation and real Git/source-code identity run before candidate writes. Complete first-construction manifests and original receipts are sealed through the reviewed shared construction-state API only after the private candidate and native validation finish. Later observations and documentation commits cannot replace original construction context.
- Per-version locks cover validation, installation and quarantine. Corrupt cache payloads and missing-ready candidates move intact outside normal discovery. A durable refusal journal prevents retries from resetting history after **observed** total outside-witness loss; exact trusted records require explicit operator restoration. Interrupted journal writes fail closed; an incomplete refusal record requires explicit operator restoration of original context and journal, never automatic healing. Simultaneous total loss before any observation is not claimed detectable.
- Cache reuse rederives complete source-owned payloads and validates both native row/type/WKB fidelity and exact fresh deterministic GeoPackage container bytes, including auxiliary SQLite metadata. Only explicitly known downstream catalog files are allowed.

The explicit `ornl_campus_historical_reconstruction` job selects the source **without checks**: success means preservation-stage construction only. Its monthly schedule is **STOPPED**, uses explicit `HIFLD_CAMPUS_CAPTURE_DIR`, and performs no network polling. The separately registered blocking `campus_historical_full_quality` check examines all 5,898 geometries and must fail on the retained fifteen invalid polygons. Generic publish-only selections do not automatically select that source check. Normal release approval gates remain independent and mandatory.

## Reproduce

From the repository with its managed environment:

```sh
PYTHONPATH=src python -m unittest discover -s tests -p test_campus_preservation.py
PYTHONPATH=src python scripts/verify_campus_preservation.py --runtime-dir /absolute/empty/local/runtime
```

The runner creates isolated DAGSTER_HOME, staging and local output directories; records actual run IDs, selections, checks and errors immediately after each run; registers the dynamic publish partition; exercises source staging/reuse, full-population failed quality, two normal catalog/format cycles, missing approval, observed trust-loss retries and explicit restoration. Its prior accepted-output fixture is synthetic and establishes preservation behavior only. Read the committed execution receipt for the actual completed scope, not this description of intended checks.

## Decisions still required

1. Verify the exact ORNL/HIFLD release/delegation or explicitly permit this named historical-mirror preservation scope. No current-source replacement is inferred.
2. Approve a baseline and contract covering original/published IDs (including duplicate published IDs), physical types, native geometry and historical precision differences. All destructive mappings and migrations remain quarantined proposals.
3. Resolve the fifteen original invalid geometries without silently repairing them or accepting an exception. Full-population quality remains blocking.
4. Approve rights/attribution and the exact independently reviewed release before promotion. No source/baseline/contract approval, merge, closure or production publication is performed here.

## Recorded local validation

The first complete local source checkpoint `df1ece1` passed all fourteen source tests (256.527 s). Native local checkpoint `b55cabe` completed twelve actual Dagster runs with the expected successful preservation/reuse/restoration and failed quality/approval/trust-loss outcomes; see `evidence/dagster-execution-local-b55.json`. Both actual catalog cycles materialized **catalog only**: quality correctly blocked downstream format/promotion steps. Native GeoPackage/GeoParquet are produced and fully validated by the source stage itself. The receipt records actual selected jobs and materialized asset keys.

An initial 552-test suite attempt at the native local checkpoint had three execution errors: the newly added relative-root test accidentally constructed an explicitly forbidden `..` path, and two existing PMTiles tests lacked the installed tippecanoe directory on PATH. The corrected valid-relative-descendant fixture and installed tippecanoe PATH rerun passed **552 tests: 551 passed, one skipped, 195.889 s**, at native local `22b8d5da5d138d28443b85627f55b4beed757a28`; final log SHA-256 `a3fabd9ef4d54f14d4cb08f68edbc8bebeca8a41fc0fe75aee5abcba4b8f81b8`. These prior errors are retained as test/execution setup history, not quality approval. The actual unchanged sibling IaC source snapshot `56aa10757129c457c530d707d9a40bae13e0907f` supplied the earlier missing test input; this is not an IaC Git checkout, infrastructure validation, deployment or CI claim.

Descriptive full-population diagnostics can be reproduced from pinned complete originals with the scripts in `evidence/replay/` (each requires `--output-dir`; use `PYTHONPATH=src`). `diagnose_coordinates.py` explicitly projects only for diagnosis, never for canonical construction, geometry repair or accepted tolerance. `compare_native.py` and `audit_raw.py` retain every compared identifier and source coordinate multiplicity in their generated reports.


The repository token expired before the native repaired commits could be pushed. The installed repository connector created **a distinct public commit** `c2b69c5834744c1a694c6c34565b7df94600cebc` with the exact complete native `22b8d5d` tree `cfdaac7a3f4ad3c8ff7cc70539502bffcdc1ba16`; native history remains backed up. This does not pretend the native and connector commit SHAs are the same. See `evidence/github-c0-portability.json`.

A fresh isolated **public-commit** construction `9f1a0233-6495-4333-ac46-e4f13de28652` binds that real reachable commit in all original provenance. Its seven actual runs verify source construction/reuse, full-population failed quality `4a870b09-6f95-445e-926c-bb196a63b6a1`, missing approval `9b9c004e-09b8-44d1-93f4-44c69feb9c44`, observed total witness loss/repeated refusal and explicit exact restoration/cold rebuild `065af7d7-933a-46f8-9b3a-3f43cd73c3d9`. Both 5,898-row native representations retain every typed value/null/date/ID/WKB and source-owned object pin; the synthetic prior output fixture is unchanged. See `evidence/dagster-execution-public-c0.json`, the exact original `evidence/source-manifest-public-c0.json` (verified against its source-owned pin), and `evidence/validation.json`. This narrower checkpoint does **not** repeat or reattribute the earlier two catalog cycles or full suite: their code/tree identity and earlier scopes are explicit. `evidence/replay/verify_public_checkpoint.py` is the portable narrow runner (same source; omit the already-proven catalog loops and perform one newer-observation refusal retry).

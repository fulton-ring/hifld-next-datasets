# Amtrak Stations — working local reconstruction draft (#17)

This implements the explicit normal Dagster ingest, catalog and derivative graph using the official Amtrak-attributed USDOT/BTS NTAD distribution. The user authorized reviewable draft code and isolated local testing while contract acceptance is pending. This does **not** approve the source replacement, baseline, identifier mapping, output contract or production release.

The preserved official item is owned by `USDOT_BTS`, credits Amtrak as producer and BTS as distributor, and explicitly dates this source population **October 08, 2026**. Portal edit time is recorded separately. Its current layer yields **1,036 complete native EPSG:4326 points and 16 named source attributes**, with distinct original `OBJECTID` and string `Code`. Before/after item, layer schema/edit/extent, count and complete ID witnesses agree. Every bounded response and attempt receipt includes its exact original request, observed final URL, UTC retrieval time, byte count and SHA-256. Missing redirect evidence is rejected. Raw bytes remain intact on parsing, completeness, type or geometry failure.

The complete preserved published candidate is **1,056 rows**, generation `1791506349323303`, SHA-256 `38d7c8e2431805734c5db6718629070e6eeb325160aff5fcca3a17eaa4ffa9a7`; its complete bytes agree with the pinned STAC item checksum/size. It is a baseline **candidate**, not an accepted baseline. The source has `OBJECTID`; the historical candidate has `id`. Neither is replaced or manufactured. A complete **diagnostic** join on original `Code` finds 1,018 common / 18 source-only / 38 published-only rows; only 17 joined source `OBJECTID` values equal historical `id`. All 15 shared attributes and every geometry were compared, with full per-row differences and coverage profiles preserved in `evidence/complete-baseline-comparison.zip`. All 1,018 joined geometries differ at exact native numeric precision; no tolerance or identity mapping is approved. The DHS crosswalk DOI resolves to a provider-side 403, so the crosswalk relationship remains explicitly unmatched. The official data dictionary DOI returned a complete XLSX, preserved here.

`src/dagster_hifld/amtrak_extract.py` validates strict JSON, official item/layer identity, pinned complete source field metadata, exact request/final URL receipts, census/pagination/schema/CRS, all source IDs and every coordinate. It rejects coercion, duplicates, non-finite values, invalid geographic coordinates and numeric rounding. It stages all raw inputs before rejection, creates a typed native GeoParquet candidate, and uses an Arrow-stream GeoPackage bridge to retain original physical widths, nullability, text, numeric values and WKB. Immutable objects bind complete raw/receipt hashes, installed pipeline fingerprint and proposed schema policy. Replays preserve original retrieval/processing attribution; conflicting raw inputs, modified provenance or builder code cannot overwrite a candidate.

The existing `assets/bts.py` export remains registered under `ingest/amtrak-stations/amtrak-stations`. The normal repository also registers `amtrak_draft_refresh_job` and a proposed Monday 08:00 America/New_York schedule, default **STOPPED**, selecting only that ingest. Source helper, asset and schedule reject remote and `PublishedStorageResource` destinations before network/write. Nothing activates this schedule.

Actual normal local Dagster execution at source/registration checkpoint `be49bcb93f1901b408e709b6df33bb58dcc98681` produced:

| Operation | Run ID | Result |
| --- | --- | --- |
| Ingest | `7e957330-9394-4e39-8aa7-b403ecaf5258` | 1,036 rows, every source attribute/value and native WKB retained |
| Identical refresh | `a9d3fc7a-bdfc-41ae-aaab-3d8f30630287` | Every version byte unchanged |
| Normal catalog/GPKG/Parquet/PMTiles/verify graph | `31a4790d-6d47-4167-8fe1-a34b852ee7a9` | Quality and derivative steps pass; final verification blocks missing independent release approval; promotion never runs |
| Synthetic corrupted local geometry capture | `664fdf76-addd-4ad7-a98d-c20baef34f63` | Source quality gate fails; rejected raw bytes remain quarantined |

The accepted local sentinel and real raw/candidate bytes remain unchanged after both failures. This sentinel demonstrates preservation behavior only; it is not acceptance of the real dataset. See `evidence/local-dagster-execution.json` for complete assertions and failures. Isolated state, storage, event logs and generated derivatives remain under `/workspace/work/hifld-coordinator/17/run-final/` in this runtime.

The canonical candidate and GPKG retain all 16 source names, physical types, nullability, values and native WKB. The generic terminal Parquet retains every value/type/WKB but adds `bbox` and changes `OBJECTID`/`geometry` nullable metadata from false to true. Those are **unapproved** output differences. Regenerated Shapefile is explicitly excluded from this draft because long field names and nullable whitespace strings lack a reviewed lossless DBF mapping; omitting that historical published format also requires a decision. Local PMTiles is a visualization derivative; no claim of lossless tile geometry or production acceptance is made.

Verification: 18 focused adapter tests pass, plus the normal repository registration test. The full suite ran 492 tests: 490 passed, one production test skipped, and the sibling-IaC test errored because `../hifld-next-iac/environments/prod/dagster.tf` is absent. Full suite started at source checkpoint `992690b`; the later registration was separately tested. No full-suite or CI success is claimed.

To replay the complete preserved input:

```bash
UV_CACHE_DIR=work/uv-cache uv sync --frozen
mkdir -p work/amtrak-capture
unzip docs/dataset-sources/amtrak-stations/evidence/complete-source-20261019.zip -d work/amtrak-capture
PYTHONPATH=src uv run python scripts/verify_amtrak_draft.py \
  --capture work/amtrak-capture --output work/amtrak-local-run
```

`tippecanoe` must be on `PATH` for the PMTiles step. The runner explicitly selects local directories, its own `DAGSTER_HOME`, and no release registry. It registers the actual dynamic publish partition and expects the unapproved release gate to fail after constructing candidates. It also injects a clearly labeled synthetic null geometry into a separate copy and verifies failure preservation. Complete baseline comparison can be rerun with `scripts/compare_amtrak_baseline.py` using the emitted canonical candidate and the complete preserved baseline binary. The evidence-checksum inventory covers every retained evidence object.

Before release, a maintainer must accept an exact complete baseline and crosswalk relationship, approve a source/published ID-preservation and migration contract, decide the full population/attribute/geometry changes, and approve output schema/nullability/format/provenance mappings. The shared #417 gate remains closed until that exact independent decision exists. Independent code review is still required on the final PR head.

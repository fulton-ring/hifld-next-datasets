# Census Combined Statistical Areas draft reconstruction

This implements the exact inventory's **2024 Census national geodatabase** layer `Combined_Statistical_Area`. It preserves the complete original 71-member ZIP, all 184 original `OBJECTID` FIDs and eight source attributes, producer double area values, native NAD83 coordinates and geometry. The source asset produces a deterministic, discoverable SEMVER candidate with typed GeoPackage and Parquet itself. No runner copies files into the candidate namespace.

The monthly `csa_source_integrity` job checks the pinned release; its schedule is **STOPPED**. Without `HIFLD_CSA_CAPTURE_DIR`, the source asset retrieves the official pinned2024 URL with bounded retries. Changed/partial responses or source/schema/ID/type/geometry drift quarantine. Complete captured replays require the original successful receipt, with exact request/final URL, HTTP status, aware timestamp, byte count and SHA256. Unrelated failed diagnostics are never consumed as source inputs. Published/cloud storage is refused before acquisition or writes.

Before any discoverable version exists, the adapter validates standard shared provenance and every source/output value, ID, declared representation and native geometry. A hidden temporary tree is atomically renamed. Installed Python/native engines, effective distributions, source code and project/lock identities bind cache reuse. Refresh independently decodes cached native outputs against fresh original source rows, including after catalog/format execution; repinning a tampered ready receipt cannot replace that reconciliation. Extra/missing source-owned files and unrecognized downstream files are rejected.

## Validation

`evidence/review-fix-materialization.json` records the repaired actual isolated Dagster source, refresh, normal catalog/format/quality/approval, synthetic failed-quality and post-format refresh runs. Full184-row assertions cover all original attributes/IDs, driver/output physical types, native WKB, declared CRS and coverage in primary outputs and terminal GeoParquet. The normal graph reaches the expected missing-independent-registry failure. The synthetic false quality flag blocks verify/promote; original quality metadata is restored. Complete raw inputs, immutable candidates and the prior published fixture remain unchanged. The fixture exercises preservation behavior and is **not approval of real data**.

Reproduce with the complete preserved inputs (or separately capture the exact official ZIP and original receipt):

```bash
PYTHONPATH=src PATH=/path/to/tippecanoe:$PATH uv run python \
  docs/dataset-sources/combined-statistical-areas-3/replay.py \
  --capture-dir /path/to/complete-capture --runtime-dir /path/to/fresh-isolated-runtime

PYTHONPATH=src uv run python \
  docs/dataset-sources/combined-statistical-areas-3/compare_preserved.py \
  --capture-dir /path/to/complete-capture

PYTHONPATH=src uv run python -m unittest discover -s tests \
  -p test_combined_statistical_areas_source.py
```

The capture directory must contain `official2024_nationgeo.zip`, `official2024_nationgeo.receipt.json`, and (for comparison) complete `baseline.parquet`. Full original ZIP SHA256: `68f517f0ef6ab6bcbc2e89102fa960fd1c7c7a130039a1aee2f475319917aeb4`, 271,963,133 bytes. The ZIP exceeds GitHub's file limit; its original and checksums remain preserved in coordinator runtime. Canonical candidates refer to a single immutable staging raw-capture copy, avoiding writable aliases and repeated272MB copies. `evidence/runtime-artifacts.json` pins full runtime logs and source/check receipts.

Nine focused tests pass. The repository suite at source checkpoint `2c990c2` passes **506 total tests:505 passed, one production-only skip**, using actual sibling IaC and explicit tippecanoe PATH. Earlier failed cache-identity and test/tool-path attempts are retained as history in the runtime manifest. No CI success is inferred. Files referenced by the original issue at `tools/pipeline_workers/README.md` and `docs/agency-source-recovery/README.md` do not exist in this integration base; no missing-file instruction is invented.

## Acceptance remains blocked

User authorization permits reviewable draft implementation/local tests while acceptance is pending; it does not approve a source, baseline, compatibility contract or publication. `proposed-contract.json` records source physical declarations without pretending they are an approved published contract.

The complete unapproved baseline has184 rows and all184 `GEOID`s overlap diagnostically. **Zero** original `OBJECTID`s equal published `id`, and zero geometries match by WKB, exact coordinates or topology in their declared coordinate values. All area fields differ by type or value;155 `INTPTLON` values differ. Published-only fields, CRS/geometry representation, terminal `bbox` and nullability metadata require explicit review. No source/published ID mapping, destructive integer coercion, geometry tolerance, reprojection or repair is accepted. Both original ID populations and historical releases remain separately preserved.

The exact inventory links a2024 GDB but a2023 metadata URL, now404; DHS/DataLumos crosswalk identity remains unreviewed. Actual source release date is unknown:2024 is an edition label and HTTP Last-Modified is packaging evidence. The official2026 directory is reachable and lists a newer national GDB. It is a separate successor discovery, not a silent replacement or a claim that2024 is current. Edition/lineage, baseline, IDs, schema and geometry decisions must be established before release. PMTiles and regenerated Shapefile are explicitly unsupported pending reviewed lossy-format mappings.

The shared #417 native verifier independently recomputes both complete native containers (`evidence/shared-native-baseline_comparison.json` and pinned normalization receipts). Every original source column and geometry has zero changes, with no added/removed rows or changed coverage. Its result remains `needs_decision`: GeoPackage's independent intrinsic storage FID has a representation absent from Arrow, and physical container schemas differ. Those184 feature-ID representation differences are not hidden or approved; `shared-native-baseline_field_changes.jsonl` distinguishes them from unchanged original `OBJECTID` and eight source attributes.

GeoPackage `gpkg_contents.last_change` uses the fixed normalization sentinel `2024-01-01T00:00:00.000Z` to make container bytes reproducible. This generated container field is **not** an asserted producer release/effective date; original source bytes/ZIP timestamps remain intact and source provenance explicitly records the release date as unknown.


## Review corrections

Original `GetLayerDefinition` XML establishes **OBJECTID esriFieldTypeOID Length4**, required, nonnullable and noneditable. GDAL `return_fids` exposes int64 and the draft output retains that int64 representation with every original value intact. This is an explicit unapproved width/representation mapping, not proof of native int64 storage. The complete original GDB and its declaration remain preserved. `ALAND`/`AWATER` are genuinely native esriFieldTypeDouble Length8. Earlier driver-schema labels in historical evidence are superseded by this distinction.

Cache reconciliation now compares the full GeoParquet schema/metadata with deterministic expectations freshly derived from the pinned source: CRS, primary column, WKB encoding, geometry types, bounding box, field declarations and geometry extension metadata. It also checks GeoPackage geometry-column/SRS/dimension and content-bounds declarations. Rehashing a malformed metadata-only candidate into the ready roster cannot authorize it. Nine focused tests and seven authentic184-row self-repin Dagster failures cover these declarations and preserve raw/prior release bytes (`evidence/review-fix-metadata-probes.json`). A restored valid refresh subsequently succeeds.


To reproduce the complete metadata-only self-repin rejection tests after local materialization, run `probe_metadata_rejection.py --capture-dir /path/to/complete-capture --runtime-dir /path/to/isolated-runtime --allow-mutate-isolated-candidate`. The probe explicitly mutates only the isolated candidate and ready receipt, runs actual Dagster refreshes, and restores both in a `finally` block. It records per-case run IDs and asserts raw/prior fixture preservation. Never point this test harness at production or accepted release storage.

GeoPackage envelope declarations are independently reconstructed through the same pinned GDAL writer in a cleaned temporary directory. This handles deterministic container-envelope serialization exactly; it does not accept a coordinate tolerance or alter WKB coordinates. Both final data payload hashes remain identical to the prior independently reviewed GPKG/Parquet outputs; only validation, audit/provenance and compatibility declarations changed.

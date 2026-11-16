# Maintained-source refresh and publication implementation plan

> **For agentic workers:** Use the `subagent-driven-development` or `executing-plans` skill to implement one work package at a time. Checklist items below define implementation and verification work; this document does not grant source, rights or production-release acceptance.

**Goal:** Capture the latest available release of a maintained upstream dataset, verify its scope and completeness, and publish accepted changes as a new immutable catalog version.

**Architecture:** Retain explicit, source-specific Dagster ingest assets and the existing catalog, format, release-verification and Portolan publication stages. Add a small shared metadata/refresh contract, while keeping release discovery, source consistency and completeness rules with each source adapter. Retain independent publication decisions and existing version-addressable consumer paths.

**Tech stack:** Python, Dagster, existing HTTP capture/storage resources, Polars/PyArrow, GDAL/GeoPandas where already used, GeoParquet/GeoPackage/PMTiles/Shapefile, STAC and the existing SQLite catalog. No new orchestrator, source mega-registry or dependency is proposed.

**Research date:** 2026-11-16. **Code baseline:** `origin/main` at `5eea5a18` (the earlier ORNL audit used `ef291e79`; the intervening commit parks an unrelated mineral dataset). Source availability and publication dates must be rechecked at implementation time.

## 1. Scope and decisions already established

- The objective is ongoing refresh, not recovering historical payloads. Historical HIFLD metadata may explain scope and lineage; historical source downloads are not an implementation deliverable.
- All 26 ORNL-associated inventory entries are accounted for in section 7. Two additional implementations with historical ORNL credits are addressed separately.
- An implemented downloader, a technically complete capture, a maintained source, an accepted dataset and a published release are distinct milestones.
- An unchanged latest release is a successful check with no publication. A failed capture must never be reported as an unchanged source.
- New releases preserve earlier published versions. Never overwrite a pinned GeoParquet path used by civicdata.
- A partial maintained successor can be useful under a separately named scope. It must not silently replace a broader original product.
- HIFLD Next publishes reference features. Proposed-project filings and time-stamped administrative actions belong in civicdata; facility status attributes and existing/operating features remain in scope here. Where source files contain proposed/retired units, retain their input evidence and explicitly define the published feature population.
- Existing mirror prohibitions and runtime publication gates remain in force. An official host alone does not prove original production, maintenance or redistribution rights.
- The planning audit used existing dataset and cross-cutting cards. The user subsequently authorized implementation and requested an assigned umbrella tracker: [#1012](https://github.com/fulton-ring/hifld-next-datasets/issues/1012). That issue records active ownership, child work and remaining scope.

### Recommended approach and alternatives

**Recommended: shared contract plus source-family migrations.** Deliver independently reviewable packages, first with current EIA electricity and the existing NCES/CMS drafts, then with state reconstructions. This delivers working refreshes without waiting for every blocked dataset.

**Alternative: repair each dataset independently.** Faster for one card, but repeats update detection and loses consistent metadata through format/catalog stages.

**Alternative: build a universal ingestion registry first.** Reject: releases, mutable services, state rosters and licensed geometry have different semantics, and the repository explicitly favors explicit assets.

## 2. What already works, and what must change

| Existing component | Keep | Fix or extend |
|---|---|---|
| `provenance.py`, `source_manifest.py` | Per-input producer/host/role, successful capture hashes, retrieval versus release dates, transformations, code identity, pinned manifest layers | Carry refresh evidence, semantic definitions, documentation references and coverage through every allowlist and projection |
| `http_capture.py` and source-specific captures | Bounded requests, receipts, immutable bytes and incomplete-capture diagnostics | Latest-release selection and before/after consistency must apply to the release actually selected |
| `sources/epa_frs_common.py` | Content-derived versions and sealed-candidate/cache verification already used by other sources | Reuse these mechanisms where their source contract fits; do not invent a competing candidate cache |
| `catalog.py`, `native_geoparquet_catalog.py` | CRS, physical schema, geometry and statistical summaries | Merge publisher field meanings, units, domains and identifier semantics; label sampled statistics |
| `assets/publish.py`, `output_policy.py` | Format exclusions, pinned prebuilt output reuse, output layout manifests | Record actual format transformations/losses and verify source-to-output completeness |
| `portolan/provenance.py` and `portolan/catalog.py` | Downloadable metadata, asset hashes, STAC/SQLite consistency and pinned dependencies | Expose refresh/coverage/documentation and use accurate metadata media types |
| `release_approval.py`, `portolan/release.py`, `portolan/workflow.py` | Exact generated-object approval and immutable reviewed catalog-generation application | Bind new metadata artifacts into the same inventory; do not bypass approval for recurring updates |
| `definitions.py::version_discovery_sensor` | Discovers and registers staged publish partitions | **It currently emits no RunRequests.** Add a separate opt-in, staging-only orchestration path; do not assume the sensor launches catalog or publication |
| Explicit dataset asset modules | Module-owned `INGEST_ASSETS`, `ASSET_CHECKS`, `JOBS`, `SCHEDULES` | Replace pinned-release refresh behavior and stopped historical jobs for each migrated source |

The checked-in AGENTS description of the discovery sensor is older than its actual implementation. Update that paragraph alongside orchestration changes so future workers do not accidentally restore automatic publication behavior.

Already-open [PR #1006](https://github.com/fulton-ring/hifld-next-datasets/pull/1006) implements capture-time completeness for one maintained source; [PR #1007](https://github.com/fulton-ring/hifld-next-datasets/pull/1007) changes native polygon handling. Both were open at research time. Review/reuse their eventual merged interfaces instead of duplicating them. They are prerequisites for relevant migrations, not completed fixes across every caller.

## 3. Metadata contract

Extend the existing versioned source manifest, rather than introducing a second authoritative manifest. Preserve old published releases as readable legacy records. New maintained-refresh assets must emit the following information; missing publisher information is explicit, not fabricated.

### 3.1 Source inputs and maintenance evidence

Each input retains existing identifiers, producer, distributor/host, source role, canonical/retrieval/final URLs, retrieval time, sizes and hashes, license and attribution. Add `purpose` with values `data`, `schema`, `methodology`, `license` or `geocoding`; keep the existing authority `role` separate.

For each data or geocoding input, add `refresh` with:

| Field | Meaning |
|---|---|
| `maintenance_status` | `maintained`, `unverified` or `retired`, based on cited publisher evidence |
| `maintenance_evidence_input_id` | Reference to captured source metadata or documentation establishing the claim |
| `source_release_id` | Publisher edition/version, or null with an explanation for an unversioned live service |
| `data_period` | Publisher reference period; not the HTTP Last-Modified or our retrieval time |
| `update_cadence` | Publisher-described cadence, or explicitly unknown |
| `latest_checked_at` | When release discovery for this capture occurred |
| `latest_evidence_input_id` | Captured release listing/service metadata used to select the latest release |
| `selection_rule` | Human-readable release-selection rule, including final versus preliminary policy |
| `source_state_fingerprint` | Digest of the relevant release/service state used for consistency and change detection |
| `unknown_reasons` | Explanations for unavailable release ID, data period or cadence |

The immutable version records the discovery evidence used for that capture. Subsequent unchanged checks belong in Dagster observations/cursor state, not edits to the published manifest. A document or license input does not need an artificial data-update cadence.

For mutable services, a timestamp/OID roster is evidence of consistency, not a claim of transactional snapshot isolation. Record which consistency guarantees the publisher actually supplies. Maintenance can be legitimate even when feature content changes rarely; do not infer retirement solely from an old edit timestamp.

### 3.2 Scope, relationships and semantic fields

Add these top-level sections and carry them through `CATALOG_METADATA_FIELDS`, `PROVENANCE_FIELDS`, catalog dictionaries/quality manifests and Portolan projections:

- `coverage`: intended population, included/excluded jurisdictions, facility types/statuses, per-jurisdiction source/count/as-of details, known gaps and geometry coverage. Distinguish a complete roster from a partially geocoded roster.
- `source_relationship`: `equivalent`, `changed_successor`, `partial_successor` or `unverified`, with comparison evidence and identity/field differences. This is about the old HIFLD product, not maintenance status.
- `documentation`: relative object path, input reference, purpose, actual media type, size and SHA-256 of preserved source documents. Expose these as metadata assets in the catalog.
- `field_definitions`: keyed by output field name, with source field/input references, description, units, code/domain definitions, null/sentinel meanings, identifier meaning and definition basis (`publisher`, `derived` or `unknown`). Keep original field declarations alongside output types.
- `format_transformations`: per-output-format records of field mapping, filters, CRS changes, simplification/quantization, geometry changes, tool versions and explicit representational losses. Link to the existing complete output inventory rather than maintaining a second file roster.

Use existing `inputs`, `transformations`, `lineage`, `code_commit` and processing-identity machinery for joins, selection, geocoding and code provenance. Do not duplicate them into a competing graph.

Publisher omissions of field prose or positional accuracy produce visible unknowns. They do not by themselves invalidate otherwise useful data. Missing CRS needed for interpretation, unaccounted row loss, unsupported redistribution or an incomplete capture are different failures and need explicit resolution.

### 3.3 Geometry and multi-source construction

For each output record assembled from several inputs, retain an auditable link to original source IDs and the input supplying geometry. Use native geometry first where its meaning/CRS are documented. For address-derived coordinates, retain provider, request identity, resolved benchmark/edition, match result/quality, response date and coordinate provenance. Link this evidence in a sidecar when adding many audit columns would distort the feature schema.

Account for every roster record as native geometry, geocoded, unmatched, ambiguous, excluded by an explicit population rule, invalid address or deferred. Never represent a ten-address pilot, matched-only subset, or a state subset as the national facility population. Nonspatial audit rows may remain metadata sidecars; publish geometry only where actually known.

### 3.4 Format metadata and publication

- GeoParquet: primary geometry column, encoding/types/dimensions, correct CRS, bounds, physical/logical schema, complete object/partition layout and reader compatibility. Preserve native analytical geometry where supported. Do not coerce an unsupported M dimension into an inaccurate standards-conformance claim.
- GeoPackage: actual layer names, CRS and conversion/type/precision differences, with output readback.
- PMTiles: actual vector-layer names, zoom range, properties, clipping/simplification/quantization and dropped-feature behavior. Display geometry is not automatically an analytical replacement.
- Shapefile ZIP: original-to-short-name mapping, encoding, DBF field widths/precision, geometry limitations and a complete sidecar roster. Unsupported representations get the existing evidence-based format exclusion.
- STAC/SQLite: data and metadata asset URLs, sizes/hashes, correct media types, source providers/roles, license/attribution, spatial and data-period extent, release label, predecessor/successor links and explicit latest selection. Retrieval time must not become temporal coverage.

Preserve original documentation bytes where redistribution permits. If a licensed input prohibits retention or redistribution, record the permitted evidence/contract reference and keep restricted material outside the public object prefix. Never publish authentication tokens or signed URLs in metadata.

## 4. Refresh and release behavior

1. Source-specific discovery selects the latest applicable release using publisher metadata. Current final annual data and preliminary monthly data remain distinct sources/products; a newer page timestamp does not establish a newer dataset.
2. Conditional requests can avoid needless downloads, but ETags/Last-Modified are transport hints, not proof of currentness. For unversioned services, probe the actual source state at a cadence supported by the publisher and verify downloaded content periodically.
3. Capture the selected source, its schema, release listing and methodology/rights evidence. Use immutable capture directories and receipts. For services, reconcile source IDs/counts and edit state before/after capture and retry if they change.
4. Compute raw-byte hashes and decoded record/schema/geometry identities using existing helpers. A repacked ZIP or reordered response with unchanged records need not cause a new data release; preserve the new capture evidence and record the no-op decision. Publisher release changes and meaningful schema/rights/metadata changes must still be detected and reviewed.
5. Derive a candidate identity from source content, selected release identities, relevant construction parameters and code identity. A correction under the same publisher release ID must create a distinct candidate. A pipeline fix against unchanged source can also create a new candidate.
6. Seal a ready candidate only after complete capture and output verification. Record a source-version vector for multi-source datasets; do not silently combine a failed current input with an old cached constituent and call the result current.
7. Register the explicit dataset/file/version partition and launch only staging catalog/check/format work through a narrowly selected job. Retries reuse the verified candidate. A crashed run must not become permanently suppressed by an advanced discovery cursor.
8. Prepare the new release metadata with an explicit semantic catalog label. Verify the exact generated inventory and applicable source/scope/rights decisions using existing gates.
9. After independent acceptance, promote the immutable version and apply the reviewed catalog generation. Update the catalog's latest selection only when data and metadata are fully available. Never mutate previous version URLs.

Initial staging schedules remain opt-in until two consecutive polls (including a no-op) and a synthetic revised release pass. Production promotion and catalog application stay independently controlled. Any later standing approval for routine refreshes is a separate explicit policy change, not part of this plan.

## 5. Quality policy

Use **capture-time source completeness**, not frozen historical counts, to assess a current release. Keep pinned fixtures for parser regressions, but do not reject legitimate publisher growth because a constant says there used to be 14,686 providers.

| Check | Required behavior |
|---|---|
| Source ID/count reconciliation | Match the current source's complete selected population; account for every page/file/partition and all documented exclusions |
| Decoding and formatting | Full-population record/attribute and supported geometry preservation; account explicitly for approved transformations |
| Cross-release differences | Report added/removed IDs, count changes by jurisdiction/type/status, schema changes, nulls and geometry movement; escalate material unexplained changes |
| Source scope | Reject a national claim when expected jurisdictions are missing; allow an honestly named, complete state subset |
| Joins | Report unmatched keys, duplicate keys, many-to-many expansion and input/output accounting |
| Geocoding | Preserve all outcomes; evaluate match rates by source and jurisdiction using documented, dataset-specific acceptance criteria |
| Geometry | Review actual examples of defects, curves, boundaries and dimensionality before deciding to repair/reject. Preserve intentional source geometry and separate display limitations from analytical losses |
| Currentness | Check that the selected release is latest under the stated rule and that maintenance evidence remains valid; a source outage is a failure, not no change |
| Publication | Re-read formatted files, metadata and complete object hashes; prove catalog links and SQLite projections match immutable published objects |

Do not invent a universal acceptable decline percentage or a universal 100% geocoding target. First measure the current sources, explain legitimate variation, and record explicit per-dataset thresholds/review rules. Technical completeness and fitness for the chosen scope both need evidence.

## 6. Implementable work packages

Each package is a small PR or a short sequence of source-family PRs. Start with failing behavior tests for runtime changes, make the smallest implementation, run the focused suite, and report measured results. Documentation-only source decisions need evidence review, not artificial unit tests. Commit/push/handoff follows the current factory assignment; the referenced factory playbook was absent in this environment, so no board routing is assumed here.

### P0 — Park frozen acquisition paths and make status truthful

**Files:** `assets/ornl_power_plants.py`, `assets/child_care_centers.py`, `assets/eia_natural_gas_pipelines.py`, `assets/natural_gas_trading_hubs.py`, their matching `sources/` modules and `tests/` files; existing dataset READMEs. Apply the same disposition to confirmed additional cases in section 10.

- [ ] Add tests that normal refresh entrypoints reject known historical-only sources before HTTP, cache replay or new staging writes. Existing natural-gas-processing and retail-territory guards are precedents. Assert the HTTP client is never called and no ready candidate is created.
- [ ] Remove stopped historical schedules from the maintained-refresh registration where appropriate; a stopped schedule is not evidence of a refresh implementation. Preserve published versions and offline parser fixtures. Do not build a historical-preservation backlog.
- [ ] Rename misleading display/job descriptions and report the actual producer/distributor and stopped-source reason. Internal module renaming is optional; do not break consumers merely to remove `ornl` from a filename.
- [ ] Record each card's actionable blocker: no maintained source, access, geometry, rights or product scope. Do not collapse all into an ORNL-authentication blocker.
- [ ] Verify with `uv run python -m unittest tests.test_ornl_power_plants tests.test_eia_natural_gas_pipelines tests.test_natural_gas_trading_hubs_source tests.test_eia_natural_gas_processing_plants tests.test_electric_retail_service_territories tests.test_child_care_centers`. Expected: explicit guarded entrypoints, existing processing/retail guards remain intact, no publication.

**Done:** none of the known frozen paths can be counted as a successful maintained refresh; existing data URLs remain intact.

### P1 — Add refresh, scope and document metadata to existing manifests

**Modify:** `src/dagster_hifld/provenance.py`, `source_manifest.py`, `catalog.py`; `tests/test_provenance.py`, `test_source_manifest.py`, `test_published_provenance.py`. **Create only if needed:** `src/dagster_hifld/refresh.py` for small pure validation/fingerprint helpers, with `tests/test_refresh.py`. Release discovery stays in source modules.

- [ ] Add fixtures for one maintained annual source, one mutable service, one retired source and a multi-source partial product. Reference actual captured input IDs for latest/maintenance evidence.
- [ ] Extend validation and both projection allowlists for section 3's additive fields. Old manifests remain readable; new maintained adapters require the new evidence. Reject unresolved input references, contradictory dates/statuses and credential-bearing URLs. Do not reject missing publisher prose with an honest unknown reason.
- [ ] Add merge tests proving that dataset/file/version layers cannot silently discard or contradict release-selection and documentation evidence.
- [ ] Pass a manifest through catalog resolution, dictionary/quality emission and resolved provenance; assert all new sections survive with unchanged definitions and input hashes.
- [ ] Run `uv run python -m unittest tests.test_provenance tests.test_source_manifest tests.test_published_provenance` and the new helper tests if created. Expected: legacy fixtures pass, complete modern evidence survives, malformed modern evidence fails explicitly.

**Done:** one authoritative source manifest carries the evidence end to end; no adapter-only field disappears at a projection boundary.

### P2 — Publish usable semantic dictionaries and original documentation

**Modify:** `catalog.py::generate_data_dictionary`, `native_geoparquet_catalog.py::_dictionary`, `portolan/workflow.py::columns_from_dictionary`, `portolan/catalog.py::ColumnRecord`, `portolan/provenance.py::attach_published_provenance`; tests `test_catalog.py`, `test_native_geoparquet_catalog.py`, `test_portolan_catalog.py`, `test_portolan_workflow.py`, `test_published_provenance.py`.

- [ ] Create a fixture with a leading-zero identifier, an integer above floating-point exact range, a coded domain, units, a sentinel value, missing semantic prose and a referenced XML/PDF document.
- [ ] Enrich observed field entries from `field_definitions`, preserving physical Arrow/DBF type information and statistical scope. Keep observed examples distinct from publisher domains. Ensure `documentation_url` is propagated where currently omitted.
- [ ] Expose preserved source documents from `metadata/source/` through the existing asset mechanism with actual XML/PDF/text/JSON media types. Where documentation already lives under `raw/`, reference it explicitly or retain one pinned metadata copy; avoid unnecessary duplicate large payloads.
- [ ] Carry descriptions and the remaining semantic fields into catalog-visible metadata. Use existing JSON metadata/statistics storage where sufficient; change SQLite schema/version only if a queryable field genuinely requires it, with a migration/read-compatibility test.
- [ ] Verify every metadata asset resolves to the exact bytes/size/hash represented by STAC/SQLite, including altered-document refusal.
- [ ] Run `uv run python -m unittest tests.test_catalog tests.test_native_geoparquet_catalog tests.test_catalog_native_float tests.test_portolan_catalog tests.test_portolan_workflow tests.test_published_provenance`. Expected: exact types/values remain intact, semantics and evidence are readable, absent definitions remain explicit unknowns.

**Done:** a consumer can interpret fields and download the actual publisher documentation without opening a raw ZIP manually.

### P3 — Latest-release discovery, idempotency and staging orchestration

**Modify:** each migrated asset/source module, `sources/epa_frs_common.py` only if shared candidate semantics need an additive parameter, `assets/discovery.py` only to collect a module-owned sensor list if using sensors; `tests/test_dynamic_partitions.py`, `test_schedule_config_validation.py`, source tests and `test_refresh.py` if created. Keep registration-only `version_discovery_sensor` behavior intact; add a dedicated staging-only job/sensor module if needed.

- [ ] Fixture-test active release selection, misleading HTTP timestamps, unpublished/commented links, same-label corrections, reordered rows, byte-only repacks and material documentation/schema changes.
- [ ] Reuse `content_derived_version`, `processing_identity`, sealed readiness and verified cache reuse. If material source semantics/release IDs must enter identity, add a backwards-compatible explicit optional digest rather than disguising source metadata as a code commit. Preserve old call behavior.
- [ ] Add a staging-only orchestration path selected by the returned candidate partition. A sensor must have a bounded cursor, deterministic run key and retry/reconciliation behavior; cursor advancement cannot permanently suppress a failed run. A schedule must not guess the output partition from the current date.
- [ ] Test incomplete/quarantined candidate refusal, restarted discovery, duplicate ticks, retry after failed catalog, same-release changed content, and exact unchanged-candidate reuse. Assert verify/promote/catalog-apply assets are excluded.
- [ ] Use the same durable Dagster instance and storage configuration for ingest, partition registration and staging work. Preserve `HIFLD_SHADOW_MANUAL_ONLY` behavior. Update AGENTS's stale sensor description.
- [ ] Run `uv run python -m unittest tests.test_dynamic_partitions tests.test_schedule_config_validation` plus each migrated source's fixture suite. Expected: ready candidates reach staging checks once, failures are retryable, source failures are not no-ops, publication remains impossible from refresh orchestration.

**Done:** new upstream content creates a new candidate; unchanged content creates an observation; corrections and failures are handled without overwriting or silently skipping work.

### P4 — Capture-time completeness and source-faithful geometry

**Modify:** source-specific checks and parsers; `catalog.py::_pinned_producer_geometry_defects`, `native_geoparquet_catalog.py` where shared changes are necessary. Reuse `native_verification.py`, `validation.py`, `transformation_accounting.py`. Reconcile #994/#997 and PRs #1006/#1007 before changing their interfaces.

- [ ] Replace fixed observed population constants as runtime completeness rules with current-release count/ID/partition witnesses. Keep fixed fixture expectations in tests. Verify both a legitimate source increase and a truncated response.
- [ ] Test source mutation during capture, missing/duplicate pages, regional omission and a same-count-but-different-ID population.
- [ ] Preserve native geometry and IDs through full readback. Pin defect identities/evidence where source defects are accepted for investigation; a matching invalid-geometry count alone does not establish equivalence.
- [ ] Add manually reviewed examples for actual geometry findings, recording affected IDs, source/candidate view, method, explanation and disposition. Do not auto-repair a boundary solely because a generic validator dislikes it.
- [ ] Reuse transformation accounting for joins, filters, deduplication and aggregation, with every unmatched/discarded row explained.
- [ ] Run `uv run python -m unittest tests.test_native_verification tests.test_transformation_accounting tests.test_native_geoparquet_catalog` plus source-specific tests. Expected: complete evolving source accepted technically; omitted/swapped/duplicated records and undocumented geometry changes refused.

**Done:** technical success proves complete capture of the selected current population, not equality to a frozen historical count.

### P5 — Format transformation receipts and loss-aware outputs

**Modify:** `conversion.py`, `assets/publish.py`, `output_policy.py`, catalog metadata writing; tests `test_conversion.py`, `test_conversion_crs.py`, `test_conversion_integer_fidelity.py`, `test_prebuilt_outputs.py`, `test_publish.py`.

- [ ] Test the actual risky paths: unknown CRS in `_to_wgs84`, repair in `_repair_geometries_for_fgb`, null/empty filtering in `_filter_valid_geometries`, reserved FID renaming, DBF field truncation and dimension loss.
- [ ] Stop silently assigning WGS84 when source CRS is unknown. Require evidenced CRS or exclude the incompatible derivative. Preserve native analytical outputs where possible.
- [ ] Write per-format receipts recording inputs, tool/options, output hashes, row accounting, field mappings, CRS/dimensions and actual changes/losses. Do not describe `make_valid` or `buffer(0)` as source preservation.
- [ ] Preserve approved pinned prebuilt outputs; do not regenerate them through a lossy intermediate. Use existing unsupported-output declarations when a format cannot represent the data faithfully.
- [ ] Verify attribute/geometry readback for analytical formats and declared behavior for PMTiles. PMTiles simplification is a documented display transformation rather than a requirement for coordinate equality.
- [ ] Run `uv run python -m unittest tests.test_conversion tests.test_conversion_crs tests.test_conversion_integer_fidelity tests.test_prebuilt_outputs tests.test_publish`. Expected: no silent loss/CRS invention, explicit exclusions work, receipts match actual generated objects.

**Done:** each offered format has a known, verified relationship to the authoritative feature data.

### P6 — Maintained EIA generator and plant products

**Create:** `sources/eia_electricity_inventory.py`, `assets/eia_electricity_inventory.py`, `tests/test_eia_electricity_inventory.py`; extend `partitions.py` only with the module-owned dataset/file pairs; update existing #105/#274 source READMEs. Confirm separate source-native slugs before reusing historical product paths.

- [ ] Implement discovery against the active EIA-860M publication rows and current annual EIA-860 listing. **Raw HTML includes commented future-month links**: strip comments/parse active rows and cross-check workbook period, rather than taking the maximum URL date.
- [ ] Test bounded XLSX/ZIP reading, observed sheet roster, release-period mismatch, workbook corruption, same-month correction and normal no-op refresh. Prefer existing readers; add a dependency only if the managed environment lacks a justified XLSX reader.
- [ ] Preserve Plant ID and Generator ID lexical semantics; generator identity is their composite, not Unit Code. Include the dedicated Puerto Rico sheets. Publish explicitly chosen existing-status populations; retain excluded source rows/accounting separately.
- [ ] Confirm coordinate datum from actual EIA survey documentation before assigning CRS. Monthly coordinates are plant-location points, not individual turbine footprints. Keep null/conflicting locations visible.
- [ ] Create a separate plant output by an explicit annual plant-table contract or a documented monthly generator-to-plant aggregation. Do not average conflicting coordinates or duplicate plants through ownership joins. Keep annual enrichment's as-of date distinct from monthly status.
- [ ] Capture the matching-year layout/dictionaries and EIA reuse notice. Measure entire current populations and source-to-output accounting; no 2021 count/hash gate.
- [ ] Register explicit ingest assets/checks/jobs using module lists; initial schedules stopped. Run `uv run python -m unittest tests.test_eia_electricity_inventory tests.test_dynamic_partitions tests.test_schedule_config_validation`.

**Done:** a real current release can be captured, repeated as a no-op, corrected under the same month, formatted and cataloged in staging with full IDs, Puerto Rico coverage and documented scope. Independent acceptance remains separate.

### P7 — Upgrade existing NCES and CMS drafts to release discovery

**Modify:** `sources/colleges_universities.py`, `nces_public_schools.py`, `nces_private_schools.py`, `cms_certified_nursing_homes.py`, matching asset modules/tests and source READMEs.

- [ ] NCES: select the latest actually published EDGE edition for each product, preserving the different CCD/PSS/IPEDS reporting cadences. Capture ZIP XML, technical docs, source identity and school-year semantics. A source pin alone is not evidence that its edition is still latest.
- [ ] CMS: resolve the latest provider CSV from dataset metadata; validate before/after metadata and the downloaded edition. Replace `EXPECTED_COUNT=14686` as the runtime completeness contract with source-derived current population accounting. Preserve CCN text, raw coordinate values and verified CRS/accuracy evidence.
- [ ] Measure full adjacent-capture changes in IDs, school/provider status, coverage, schema and coordinates; keep source ID differences from historical HIFLD visible. Do not require old IDs/extra ORNL fields to exist in the new upstream product.
- [ ] Keep college reporting units distinct from physical campus polygons, CMS-certified homes distinct from all licensed care facilities, and public/private school populations distinct.
- [ ] Run `uv run python -m unittest tests.test_colleges_universities tests.test_colleges_registration tests.test_nces_public_schools_source tests.test_nces_private_schools tests.test_cms_certified_nursing_homes`.

**Done:** existing source-native drafts refresh across editions with source-based completeness and usable dictionaries; no duplicate CMS/NCES implementation is created.

### P8 — State rosters, separate geocoding and maintained partial products

**Modify:** `sources/childcare_roster_pilot.py`, `assets/childcare_roster_pilot.py`, `tests/test_childcare_roster_pilot.py`; existing `state_electric_territories.py` modules/tests. New state connectors get their own source/asset modules and focused tests.

- [ ] For NY/WA child care, verify live source scope/status and public redistribution conditions, then implement deterministic complete pagination beyond the current pilot ceiling. New capture directories/checkpoints must not replay an older roster as a fresh poll.
- [ ] Replace the ten-address pilot limit only after implementing resumable bounded batches and full outcome accounting. Cache by normalized address, provider/config and observed benchmark state; re-geocode changed addresses and reassess unchanged addresses when the benchmark changes or its mutable identity cannot be pinned.
- [ ] Resolve and retain Census benchmark/geography-vintage responses where used. `Public_AR_Current` is mutable; do not claim its name or numeric ID pins immutable source data. Preserve actual results and timestamps. Census interpolation is not rooftop accuracy.
- [ ] Keep full rosters and outcome ledgers beside the matched-only spatial companion. Verify unmatched/ambiguous/deferred cases remain accounted for and that source-provided coordinates are not silently blended with geocoder output.
- [ ] NY excludes NYC day-care centers; treat an NYC connector as a separate source investigation before broadening coverage. Add other states only with their own scope/status/rights evidence.
- [ ] Retail territory pilots: verify current Iowa/Wisconsin producer maintenance, retain native geometry/CRS, overlaps and utility identifiers. EIA county membership can support diagnostics, not fabricated service boundaries.
- [ ] Run `uv run python -m unittest tests.test_childcare_roster_pilot tests.test_state_electric_territories tests.test_transformation_accounting`.

**Done:** useful partial current products have exact scope, complete source accounting and traceable coordinates; no national-completeness claim is inferred from a pilot.

### P9 — Implement next verified facility-source families

**New source/asset/test modules:** `cms_hospitals.py` / `test_cms_hospitals.py`; separately scoped `ny_manufactured_home_parks.py` / `test_ny_manufactured_home_parks.py` only after schema/rights discovery. **Extend:** existing `bsee_ocs_platforms.py` modules/tests; use new regional constituent modules if needed for BSEE/BOEM table parsing.

- [ ] CMS hospitals: resolve dataset `xubh-q36u` metadata, capture the current CSV and dictionary, verify actual coordinates and provider-versus-site grain, and expose a separately named Medicare hospital product. If it lacks supported coordinates, use P8's geocoding accounting approach. State/VA/IHS/DoD coverage is separate work.
- [ ] Platforms: capture current Gulf BSEE and Pacific BOEM master/structure/location tables and their layouts. Keep regional schemas/IDs/datums distinct, measure unsynchronized as-of dates, unmatched joins, removed structures and coordinate anomalies. Preserve structure versus complex grain. Do not label OCS coverage as every offshore platform.
- [ ] NY parks: inspect the current maintained registration schema, define a latest effective reference snapshot from repeated registrations, verify facility identifiers and public geometry/address fields, and geocode separately if necessary. Registrations/actions themselves are not the feature product. Limit public fields to the accepted feature scope.
- [ ] Add complete current-source fixture/readback tests per family: edition changes, lexical IDs, duplicate/unmatched joins, source mutation, missing jurisdiction, coordinate/CRS uncertainty and publication refusal. Live full captures are bounded implementation evidence, not CI requirements.
- [ ] Run `uv run python -m unittest tests.test_cms_hospitals tests.test_bsee_ocs_platforms` after adding the hospital tests; run `tests.test_ny_manufactured_home_parks` only after its source contract is established.

**Done:** each product has a measured maintained source, explicit population and reproducible current capture. Unverified state sources do not become implementation promises.

### P10 — Resolve research/access/rights blockers with bounded deliverables

**Files:** existing `docs/dataset-sources/<slug>/README.md` and existing cards; no runtime code until a source contract passes. Cases are listed individually in section 7.

- [ ] For each research card, record original producer, actual maintainer, exact current bulk endpoint or explicit absence, latest data period, maintenance evidence, native IDs/geometry/CRS, coverage, and public redistribution conditions.
- [ ] Distinguish access from maintenance. An SSO/token-gated service is not automatically current; a current public viewer is not automatically a permitted bulk source.
- [ ] Evaluate current EIA-191 and PHMSA LNG/UNGS schemas with a bounded current sample. Deliver a concrete source-native feature/geometry go/no-go; route dated operational observations to civicdata.
- [ ] For campuses, prisons, law enforcement, trauma and national utility polygons, identify exact state/producer candidates and decide whether to build a named partial product, obtain licensed data or leave the original scope blocked. Do not approximate polygons from counties, buffers or facility points.
- [ ] Research OSM or licensed data only for a specified missing field/geometry with measurable coverage and redistribution conditions. No external contact, purchase or license acceptance is authorized by this plan.
- [ ] Update the existing card with evidence, one precise next action and the blocking decision. A research package is done when it produces a supported go/no-go, not when it invents a downloader.

**Done:** each unresolved source has an evidence-based path or a clear reason it cannot meet the current-refresh objective.

### P11 — Release verification, rollout and operations

**Modify as needed:** `release_approval.py`, `assets/publish.py`, `assets/portolan.py`, `portolan/release.py`, `portolan/workflow.py`, `portolan/provenance.py`; tests `test_promotion.py`, `test_portolan_release.py`, `test_stac_release_version.py`, `test_portolan_workflow.py`, `test_portolan_validation.py`.

- [ ] Include currentness, dictionary/source-document, geometry-review and per-format receipts in the same exact immutable release inventory. Existing approval validation must reject altered evidence as it rejects altered data.
- [ ] Exercise a local end-to-end candidate with registered dynamic partitions and a durable Dagster instance: ingest → staging catalog/checks → formats → release preparation. Confirm verify/promote refuses without independent acceptance even after technical checks pass.
- [ ] In an isolated accepted test fixture, verify new semantic release labels, identical retries, correction under the same upstream label, baseline/catalog-head races, full metadata asset links and reviewed catalog prepare/apply. Production data remains untouched.
- [ ] Run `uv run python -m unittest tests.test_promotion tests.test_portolan_release tests.test_stac_release_version tests.test_portolan_workflow tests.test_portolan_validation tests.test_published_provenance`, then `uv run python -m unittest discover tests` after integrating runtime packages.
- [ ] For each admitted source, perform one complete current-source staging capture, a second unchanged poll and a fixture-driven revised-release run. Record source bytes/rows, geographic counts, quality differences, output sizes/hashes, run IDs and costs/rate limits.
- [ ] Document source-specific polling windows, bounded retries, credential rotation where required, stale-source alerts, schema-change escalation, restart/resume instructions and named operational ownership. Enable source schedules only after their operating contract is accepted; promotion remains controlled.

**Done:** technical refresh runs reliably, consumers can retrieve each immutable version, and catalog latest never points to an incomplete or unaccepted release.

### P12 — Expanded-audit migrations and maintenance reconciliation

**Modify:** `sources/ferry_terminals.py`, `sources/ferry_routes.py`, `assets/bts_ferry_terminals.py`, `assets/bts_ferry_routes.py`, their `tests/test_ferry_*.py`; `sources/eia_ethanol_plants.py`, `sources/eia_biodiesel_plants.py`, matching assets/tests; the nine WBD source/asset/test modules and shared `sources/wbd_hu_family.py`. **Create if the roster contract is accepted:** `sources/eia_biofuels_capacity.py`, `assets/eia_biofuels_capacity.py`, `tests/test_eia_biofuels_capacity.py` for the common current annual roster.

- [ ] Ferry terminal PR: discover NCFO2024/latest source tables from the official landing, adopt one-record-per-terminal grain, retain operator relationships separately, verify native ID/coordinate datum, and measure survey response coverage. The current data family reports 63% operator response; latest data must not be advertised as complete national response. Test old/new table grain, null coordinates, duplicate IDs, missing pages and next-edition selection.
- [ ] Ferry route PR: capture current segment/terminal references as a separately scoped product only after feature scope acceptance. The current segment table has no route-path geometry. A line product remains blocked until maintained native geometry or an explicitly accepted construction method exists. Do not generate straight connections as a quiet substitute.
- [ ] Biofuels PR: resolve the latest combined EIA report and workbook, preserving ethanol/biodiesel/renewable-diesel types and source capacities. Test edition changes, units and the absence of native coordinates/IDs/addresses. Keep existing spatial adapters parked until a current location/identity constituent is proven. A nonspatial reference product needs the repository's explicit scope decision.
- [ ] WBD source-decision PRs: remove unsupported maintenance claims and reconcile USGS's legacy policy against the October2026 service notice, then either establish an actual maintained original publication or park the refresh route. Do not replace HUC IDs or geometries with unrelated 3DHP features. Preserve old published objects and optional-level coverage disclosures.
- [ ] HGL/dormant-code cleanup: block historical refresh capture/replay where still callable; preserve existing published releases. Do not register dormant IPCD/mineral code to make it easier to test.
- [ ] Run `uv run python -m unittest tests.test_ferry_terminals tests.test_ferry_routes tests.test_eia_ethanol_plants tests.test_eia_biodiesel_plants tests.test_hydrocarbon_gas_liquid_pipelines_source tests.test_wbd_hu_family tests.test_wbd_hu8_subbasin`; run each touched WBD module's existing focused suite and `tests.test_eia_biofuels_capacity` if created. Expected: selected current editions and honest geometry/coverage limitations, with retired acquisition prevented.

**Done:** additional historical-only inputs cannot pass as maintained refreshes; verified current successors have scoped implementations and unresolved geometry/maintenance claims have explicit dispositions.

## 7. Exhaustive disposition of the 26 ORNL-associated inventory entries

“Verified route” means a maintained publication or existing producer adapter was identified; it does not grant rights, source-scope or release acceptance. “Research” means no implementation-ready complete maintained replacement was confirmed. Existing NCES/private-school latest selection still requires a successful fresh publisher listing check; a pinned filename is not enough.

| Existing card / inventory dataset | Current situation | Next action / acceptance boundary |
|---|---|---|
| #36 Child Care Centers | EPA distributes ORNL's 2015 edition; NY/WA current-roster/geocoding pilot exists | P0 + P8: retire historical refresh route, build complete state captures and outcome accounting; NYC and national gaps remain explicit |
| #38 Colleges and Universities | NCES EDGE 2025–26 source-native point draft exists | P7: latest EDGE discovery; optional matching-year IPEDS directory enrichment only after join/grain validation |
| #39 Colleges and Universities Campuses | Local historical preservation code; no verified national maintained polygon feed | P10: university/state-system native campus polygon pilot; NCES reporting points/buffers are not boundaries |
| #43 Control Areas | Current EIA-861/operational BA lists exist; national native polygon route unverified | P10: BA-owned/agency geometry and rights research; county membership does not define control-area borders |
| #61 Electric Holding Company Areas | Current corporate ownership and service geometry are separate unresolved inputs | P10: current parent/subsidiary reference plus native retail areas/effective dates; filings are civicdata inputs/events, not ready polygons |
| #62 Electric Planning Areas | FERC-714 current XBRL identifies reporting entities, not accepted polygon boundaries | P10: native producer planning-area geometry; do not equate BA, RTO and planning-area concepts |
| #63 Electric Retail Service Territories | Frozen NBAM entrypoint already refuses; Iowa/Wisconsin state-native assets exist | P8: verify state maintenance/rights and expand state-by-state; no national completeness claim |
| #105 Generating Units | ORNL/I-Fac public current payload not verified | P6: maintained EIA-860M existing-generator points with composite identity and PR sheets; plant-location accuracy and new scope explicit |
| #115 Hospitals | Original national product absent; CMS current Medicare directory verified | P9: separate CMS hospital product, source dictionaries and measured coordinates/site grain; state/non-CMS constituents require separate work |
| #146 Local Law Enforcement Locations | No current national station roster verified; BJS surveys enumerate agencies | P10: one maintained state regulator agency-location source, physical versus mailing address and station/agency distinction |
| #159 Mobile Home Parks | No current national source verified; NY registrations are a concrete state candidate | P9 after schema discovery: current park-reference snapshot, native IDs/address/geometry, explicit three-or-more-home NY scope; Maine feed remains research |
| #171 Natural Gas Interstate and Intrastate Pipelines | Reachable EIA January2020 capture; current NPMS geometry restricted | P0 + P10: park historical refresh, verify producer public geometry/access/redistribution; capacity/project tables do not replace line network |
| #172 Natural Gas Processing Plants | Entry point already refuses; EIA-757 Schedule A suspended, historical report max2017 | P10: maintained state/producer plant rosters or authorized current GIS; token access alone does not establish maintenance |
| #173 Natural Gas Service Territories | EIA-176 current company/county attributes, no verified national LDC polygons | P10: state PUC/native local polygons; counties describe presence, not exact service boundaries |
| #174 Natural Gas Trading Hubs | Reachable EIA February2020 point capture | P0 + P10: verify current source-native hub definitions/coordinates and rights; current prices do not date old points |
| #175 Natural Gas Underground Storage | EIA-191 current monthly/annual attributes and PHMSA UNGS public annual data exist | P10: inspect current identifiers/location/public disclosure and build only verified reference geometry; operational measurements may belong civicdata |
| #251 Nursing Homes | Original broad care-facility scope absent; separate CMS draft #959 exists | P7 for CMS; P10 state licensing constituents for assisted living/non-certified facilities; preserve separate product names |
| #255 Oil and Natural Gas Platforms | Existing narrower BSEE GIS draft; Gulf/Pacific master/structure/location tables identified | P9: native regional parsers, layouts, joins and CRS; OCS and structure/complex grain explicit |
| #266 Peak Shaving Facilities | PHMSA LNG current public annual data is a candidate; full public coordinates not yet verified | P10: bounded schema/identity/geometry discovery; distinguish LNG peak-shaving from export/import/other LNG uses |
| #272 POL Terminals | Existing BTS pipeline-terminal subset is not full POL population | P10: inspect EIA/agency terminal constituent inventories, current native geometry and commodity/terminal definitions; no current full successor verified |
| #274 Power Plants | Reachable EIA August2021 GIS snapshot | P0 + P6: current EIA annual plant/monthly generator route, explicit aggregation and >=1MW survey population |
| #277 Prison Boundaries | No verified maintained national polygon replacement | P10: DOC/native boundary sources by jurisdiction; BOP points/parcel ownership do not establish all secure-facility extents |
| #278 Private Schools | NCES EDGE 2023–24 draft exists | P7: confirm latest actual PSS publication, native PPIN/geometry and field docs; note service/file CRS differences |
| #281 Public Schools | NCES EDGE 2024–25 draft exists | P7: latest CCD/EDGE selection, native lexical NCESSCH/LEAID, source-status/coverage accounting |
| #329 Transmission Lines | No maintained public national producer line source confirmed | P10: current producer/ISO/state geometry plus rights; EIA/DOE atlas/HIFLD links are not independent source evidence |
| #330 Trauma Levels | State designation registries are primary route; ACS verified subset has reuse restrictions | P10 then one state pilot: adult/pediatric level and effective status, hospital-site matching and coordinate lineage; obtain authorization before ACS reuse |

Additional ORNL credits: FERC Regions historical preservation does not satisfy maintained refresh; verify current FERC responsibilities/native geometry before implementation. USACE Waterway Nodes/Lines already fetch USACE/BTS, so retain ORNL's historical creation credit and audit actual present maintenance/metadata rather than replacing their source solely because ORNL appears in lineage.

## 8. Open/licensed geometry and geocoding decisions

1. Prefer documented native coordinates/geometry from the actual maintained producer. Do not geocode a precise native facility point again merely to standardize providers.
2. Census geocoding is a concrete government option for address rosters. Its documented coordinates are calculated along address ranges; retain the actual provider result, CRS, benchmark/vintage evidence and quality limitations. No rooftop claim.
3. OSM is a possible separately declared geometry/reference input, not proof of a complete regulator roster. Resolve matching, coverage and ODbL attribution/share-alike treatment for the intended combined output before implementation. Use extracts or an appropriately provisioned service, not systematic POI scraping or recurring bulk use of the public Nominatim endpoint.
4. Standard Google Geocoding/Places terms do not establish permission to store and redistribute a permanent public feature dataset. Do not build Google-derived catalog coordinates without an applicable agreement explicitly permitting this use. Record licensing as a concrete blocker when that route is necessary, not a generic warning for every public source.
5. Licensed national utility/facility geometry may be the only credible route for some products. The research card must state the required rights: recurring acquisition, durable retention/reproducibility, public redistribution of derivatives, attribution, use in all offered formats and whether historical published releases remain distributable after expiry. No purchase/contact is part of this task.

## 9. Sequencing, handoff and completion

| Wave | Packages | Can proceed independently | Exit evidence |
|---|---|---|---|
| A: stop false refresh claims | P0 and section 10 dispositions | One source family per worker | Known retired acquisition no longer appears as maintained refresh; registered versus dormant state is documented |
| B: shared contract and first current feeds | P1, P2; P6/P7 in parallel using agreed manifest fields | Metadata worker, electricity worker, NCES/CMS worker | Current maintained sources reach staging with complete population/dictionaries |
| C: reliable repeated operation | P3, P4, P5 | Orchestration, completeness and format work need explicit interface coordination | No-op, revised release, source mutation and format-loss tests pass |
| D: broader coverage | P8, P9, bounded P10 investigations | One state/source family per worker | Honest maintained partial products and supported go/no-go decisions |
| D2: additional stale-source replacements | P12 and section 10 | Ferry, biofuels and WBD investigations independently | Superseded inputs replaced where supported; genuine retired sources parked |
| E: acceptance and operation | P11 per admitted dataset | Independent reviewers after implementation | Exact release inventory and reviewed catalog plan; no automatic production publication |

Dependencies: P1 precedes metadata integration; P3 needs sealed candidate identity; P5/P11 bind the final evidence bytes. Source research may proceed immediately. Do not have parallel workers edit shared registry lists or approve their own source/rights/release decisions. Estimate effort after the first current capture and source schema inspection, rather than promise national reconstruction dates without accessible geometry.

Each implementation handoff must state: existing issue/PR, exact commit, selected source release and maintenance evidence, intended population and measured coverage, full-capture/readback results, metadata artifacts, tests run, retry/no-op behavior, geometry/rights/scope decisions still open, schedule state and production effects. “Ready for independent review” must not mean “published.”

### Campaign completion criteria

- [ ] All 26 original entries and the additional findings have an implemented maintained route, an honestly named maintained partial successor, or an evidence-backed blocked/retired disposition.
- [ ] No normal refresh path uses an excluded historical mirror or silently replays a frozen capture as current.
- [ ] Every admitted source resolves latest releases, captures complete current content and handles unchanged polls and same-release corrections.
- [ ] Semantic dictionaries, original documentation, multi-input geometry lineage, actual format transformations and quality evidence reach downloadable catalog assets.
- [ ] Dataset-specific coverage/quality criteria are measured and accepted; intentional geometry is not “fixed” without evidence.
- [ ] Each accepted update can be staged and reviewed as a new immutable catalog version without changing an earlier release.
- [ ] Staging refresh automation and production release authorization remain separate and testable.

## 10. Expanded audit: other snapshot-based implementations

The user expanded the request during research to cover non-ORNL implementations. Classify by actual execution reachability and source maintenance, not filenames, old effective dates or stopped schedules. An intentionally historical product is not automatically a mistaken current-refresh pipeline. See the evidence-based findings below; broader candidate screening must use the same standard.

### 10.1 High-priority WBD family: nine registered layers need maintenance correction

Registered source modules `wbd_wbdline.py`, `wbd_2_digit_hu.py`, `wbd_4_digit_hu_subregion.py`, `wbd_6_digit_hu.py`, `wbd_hu8_subbasin.py`, `wbd_10_digit_hu.py`, `wbd_12_digit_hu.py`, `wbd_14_digit_hu.py`, `wbd_16_digit_hu.py` fetch the legacy USGS `hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer` layers 0–8.

Their implementation calls this maintained based partly on a service notice saying data refreshed October2026. However, USGS's current 3DHP access page explicitly classifies legacy **NHD, WBD and NHDPlus HR** as no longer updated/maintained and available for reference. The WBD program page calls its authoritative national publication a snapshot while attention shifts to 3DHP. Service republication is not proof that original hydrographic delineations are still maintained.

**Action:** correct maintenance claims and gate these as unverified/legacy under P0/P1 while resolving the specific discrepancy. Preserve existing releases. Investigate whether a current original stewardship publication supersedes the snapshot and whether 3DHP's hydrographic units can meet each HUC/line product's semantics. Do not rename a 3DHP layer to WBD without a schema/identity/coverage decision. The 14/16-digit levels are already incomplete nationally by design; do not invent full national coverage. This is a publisher-policy conflict requiring resolution, not proof that every WBD attribute has been unchanged since one date.

### 10.2 EIA ethanol and biodiesel: superseded spatial snapshots

Registered `eia_ethanol_plants.py` and `eia_biodiesel_plants.py` capture GIS packages with **January2021** content, using exact old population/hash pins. Their default live requests remain reachable; stopped schedules do not prevent manual fetches. The current combined EIA Biofuels Capacity and Feedstocks Update supplies **January2026** plant rosters, released September24,2026.

**Action:** P0 for old GIS refresh; implement the maintained combined report's source-native roster and source metadata. The current workbook inspected in research has entity, facility, state, PADD, capacities and facility type, but no native ID, coordinates or address. A current attribute roster is therefore verified; current point reconstruction still needs identity/location constituents or separately justified geocoding. Do not borrow 2021 coordinates for new/current facilities without an explicit crosswalk and evidence.

### 10.3 Hydrocarbon gas liquid pipelines: January2020 GIS snapshot

Registered `hydrocarbon_gas_liquid_pipelines.py` has a reachable historical EIA GIS capture and fixed old-source pins. No complete maintained public line-network successor was verified.

**Action:** park historical refresh acquisition/replay and investigate current producer-native geometry and redistribution, alongside #171. Current capacities, projects or an accessible ArcGIS URL do not establish a current national line network.

### 10.4 Ferry routes and terminals: old adapter while a newer survey exists

Registered ferry route/terminal adapters use **NCFO2020 with operational data year2019**. BTS now publishes **2024** NCFO operational data. The current terminal dataset has native coordinates and terminal identifiers, but changes the table grain from operator-terminal combinations to one row per terminal.

**Action:** replace release selection and schema/grain handling with latest NCFO discovery. Terminal points are a concrete current-source route; preserve operator relationships separately. Current route records exist, but a maintained native route-path geometry contract still needs verification. Do not draw straight endpoint connections and label them navigation routes. Add source-year, cardinality and coordinate tests, then the P3/P4/P11 refresh/release checks.

### 10.5 Dormant mineral reconstruction code

- `ferrous_metal_processing_plants.py` still has callable default acquisition of an Internet Archive copy of a USGS **2005 publication based on a 2003 survey**. The implementation is **not registered in current Dagster definitions**. Disable the remaining archival network path rather than describe it as an active refresh.
- `ferrous_metal_mines.py` accepts explicit preserved historical input plus optional USGS metadata; `construction_minerals.py` and `crushed_stone.py` are local historical-input reconstructions. They are also unregistered at this baseline. Local preservation cannot satisfy current facility refresh.
- Current MSHA/state mine/facility registers are research candidates, not verified replacements for every smelter/processor/quarry. `mineplant` producer metadata reports Complete/None planned. Old source fidelity cannot establish operating status today.

**Action:** record dormant/parked status accurately, prevent historical inputs being admitted as maintained refresh, and research source-native current subsets only where the intended facility scope is supported. US Coal Fields and Miscellaneous Industrial Mineral Operations are already parked; do not count them as active stale ingesters.

### 10.6 EPA FRS/EIA power generation: stale constituent evidence, not proven abandonment

Registered FRS power-generation point adapters and related NAICS/SIC tables reference an EPA service whose description still dates its EIA compilation to **June2014**, despite current FRS refresh timestamps. The two point inventory entries reuse one source adapter.

**Action:** perform a field-level currentness/identity audit and retain constituent dates. The FRS publication may still be maintained; a 2014 description does not prove every current value is from2014, while a 2026 service edit does not prove the EIA constituent was refreshed. If needed, construct a separately documented contemporary EIA/FRS crosswalk using current sources with unmatched/multiple joins, reusing P6 rather than duplicating electricity ingestion.

### 10.7 Already disabled and false positives

- BTS IPCD explicitly says no longer updated, latest information2022. Its capture function remains callable but all module registration lists are empty. Keep it out of maintained admission; a source cleanup can disable the unused capture path. It is not an active registered refresh.
- Do not flag Red Cross merely because the service URL contains `2022`: live metadata identifies FY27 with edition notices. Verify actual effective edition and authority instead.
- Do not call migrated 3DHP flowline/waterbody implementations retired NHD merely because their inventory slug starts `nhd/`. Inspect the actual service URL. Fixed source counts in a maintained-source adapter are a separate refresh-readiness defect covered by P4.
- Census fixed-reference geography, GeoMAC2000–2018 and NOAA historical event series have intentional temporal scope. Assess their source maintenance/purpose individually; do not substitute current-period data under their historical identities.
- Quaternary fault metadata/search retirement and old scientific-map publication dates alone do not establish total dataset abandonment. BTS terminals/bridges/air-truck old observation dates similarly require source-specific verification before a retired-source claim.

### 10.8 Secondary maintenance investigations

Four registered EPA region/EEZ products consume the older `OEI/EPA_Locations/MapServer` family (`epa_large_scale.py`, `epa_small_scale.py`, `epa_eez.py` and corresponding asset modules). The small-scale layer explicitly has 2011 TIGER lineage and the service carries a 2012 title. No inspected publisher statement establishes discontinuation. Investigate current EPA boundary stewardship and delegated replacement geometry before classifying these as retired. The separate `epa-regions` product uses a different service and must not be conflated with them; the EPA office-facilities/labels code is dormant, not part of the registered region count.

BTS Air-to-Truck (2020 product with 2017 airport basis), pipeline terminals (2021), marine RoRo (2022) and railroad bridges (2022 compilation) also need bounded maintenance checks. Record the age and exact source evidence, but do not infer cessation from those dates alone. FAA airspace/aviation facilities and FRA NARN showed current producer update evidence and are excluded from the stale-source findings.

## 11. Primary-source evidence and research limits

The following links are research evidence, not licenses or acceptance decisions. Current landing/metadata pages were checked on 2026-11-16 where available. No prohibited mirror payload was acquired. One current EIA-860M workbook and one current biofuels workbook were inspected for schema; that is not a full source-to-output quality certification. NCES SchoolLocations timed out during this research, so existing current-draft evidence is distinguished from fresh latest-release proof. Some candidate bulk endpoints remain unvalidated and are explicitly research work above.

| Topic | Primary evidence |
|---|---|
| Annual plant/generator source and layouts | [EIA-860](https://www.eia.gov/electricity/data/eia860/): latest final2025 at research time, plant/generator/utility files and layout documentation |
| Monthly generator source | [EIA-860M](https://www.eia.gov/electricity/data/eia860m/): active latest September2026, released Oct23; preliminary estimates may be revised without separate announcement |
| Utility/service presence versus polygons | [EIA-861](https://www.eia.gov/electricity/data/eia861/): county/state service records and balancing-authority lists |
| EIA reuse conditions | [EIA copyright/reuse](https://www.eia.gov/about/copyrights_reuse.php): government data generally reusable, third-party exceptions matter |
| Gas report currency | [EIA NGQS report metadata](https://www.eia.gov/naturalgas/ngqs/data/report), [release dates](https://www.eia.gov/naturalgas/ngqs/release_dates.json): current storage versus ended processing report |
| Public PHMSA annual records | [PHMSA annual data](https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids), [LNG data/maps](https://www.phmsa.dot.gov/pipeline/liquified-natural-gas/lng-data-and-maps) |
| Restricted pipeline geometry | [NPMS data access](https://www.npms.phmsa.dot.gov/PipelineData.aspx), [viewer limitations](https://www.npms.phmsa.dot.gov/AboutPublicViewer.aspx) |
| Current planning entity reports | [FERC-714](https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-no-714-annual-electric) |
| School locations and source universes | [NCES EDGE School Locations](https://nces.ed.gov/programs/edge/Geographic/SchoolLocations), [IPEDS files](https://nces.ed.gov/ipeds/datacenter/DataFiles.aspx) |
| Maintained nursing provider metadata | [CMS Provider Information](https://data.cms.gov/provider-data/dataset/4pq5-n9py), [dataset metadata](https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/4pq5-n9py?show-reference-ids), [dictionary](https://data.cms.gov/provider-data/sites/default/files/data_dictionaries/nursing_home/NH_Data_Dictionary.pdf) |
| Maintained hospital metadata | [CMS Hospital General Information](https://data.cms.gov/provider-data/dataset/xubh-q36u), [dataset metadata](https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/xubh-q36u?show-reference-ids), [dictionary](https://data.cms.gov/provider-data/sites/default/files/data_dictionaries/hospital/HOSPITAL_Data_Dictionary.pdf): release Nov12, reference modification Oct26 at research time |
| Childcare state coverage | [NY regulated programs](https://data.ny.gov/Human-Services/Child-Care-Regulated-Programs-Map/s8uq-s4wq/about), [WA Child Care Check](https://www.dcyf.wa.gov/services/earlylearning-childcare/child-care-check); exact current API IDs are retained in the existing pilot |
| Manufactured-home park candidate | [NY current registration dataset](https://data.ny.gov/Government-Finance/Manufactured-Home-Park-Registrations-Beginning-202/nq2i-9jge), [NY registration rules](https://www.tax.ny.gov/bus/mhp/) |
| Platform constituents | [BSEE Gulf platform data](https://www.data.bsee.gov/Main/Platform.aspx), [BOEM Pacific platform data](https://www.data.boem.gov/Main/PacificPlatform.aspx): master/structure/location tables and layouts; cadence/CRS remain source-specific |
| Trauma designation versus verification/rights | [ACS program Q&A](https://www.facs.org/quality-programs/trauma/quality/verification-review-and-consultation-program/trauma-verification-qas/program-qas/), [Florida trauma program](https://www.floridahealth.gov/licensing-regulations/trauma-system/) |
| Geocoder behavior and mutable benchmark | [Census API specification](https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html), [benchmark discovery](https://geocoding.geo.census.gov/geocoder/benchmarks) |
| OSM input rights and public-service constraints | [OSM license](https://www.openstreetmap.org/copyright), [Nominatim policy](https://operations.osmfoundation.org/policies/nominatim/): use extracts/provisioned services for this campaign |
| Google permanent redistribution constraint | [Google Geocoding policies](https://developers.google.com/maps/documentation/geocoding/policies): ordinary API access does not establish durable public-dataset rights |
| GeoParquet representation | [GeoParquet1.1 specification](https://geoparquet.org/releases/v1.1.0/): explicit CRS/geometry metadata and dimensionality limits; use actual supported output version |
| Catalog representation | [STAC1.1 community standard](https://docs.ogc.org/cs/25-004/25-004.html): providers, licenses, assets and extensible metadata; no wholesale catalog-schema replacement is proposed |
| Legacy hydrography maintenance | [USGS NHD retirement](https://www.usgs.gov/national-hydrography/national-hydrography-dataset), [3DHP access/current versus legacy](https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products), [WBD authoritative snapshot](https://www.usgs.gov/national-hydrography/watershed-boundary-dataset), [legacy WBD service](https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer?f=pjson) |
| Maintained biofuel rosters | [EIA combined biofuels report](https://www.eia.gov/biofuels/capacity/): January2026 data; geometry/identity not supplied by the inspected workbook |
| Latest ferry survey | [BTS NCFO](https://www.bts.gov/NCFO): latest2024 operational data; terminal grain and route geometry need explicit treatment |
| Current ferry native tables | [NCFO2024 family](https://data.bts.gov/stories/s/2024-NCFO-Data-Files/y2vm-k4wf/), [terminal coordinates](https://data.bts.gov/Maritime-and-Waterways/2024-NCFO-Terminals-File/difu-6bgs), [segment references](https://data.bts.gov/Maritime-and-Waterways/2024-NCFO-Segments-File/2ygc-ds4z), [operator segments](https://data.bts.gov/Maritime-and-Waterways/2024-NCFO-Operator-Segment-File/7umd-qc2k) |
| Frozen passenger connectivity | [BTS IPCD service metadata](https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Intermodal_Passenger_Connectivity_Database/FeatureServer?f=pjson) |
| FRS constituent freshness | [EPA FRS power-plant service metadata](https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FRS_PowerPlants/FeatureServer?f=pjson): June2014 EIA compilation statement, not proof of every field's current date |
| EPA regional maintenance investigation | [EPA Locations service](https://geodata.epa.gov/arcgis/rest/services/OEI/EPA_Locations/MapServer?f=pjson), [small-scale layer](https://geodata.epa.gov/arcgis/rest/services/OEI/EPA_Locations/MapServer/2?f=pjson): legacy lineage is established; cessation is not |

## 12. Validation of this planning deliverable

This is a documentation/research change only. Validate all referenced implementation/test paths, all 26 inventory rows, the additional nine WBD modules, the baseline commit, source citations and the distinction between registered, dormant, local-only and refused paths. Run `git diff --check` and review the plan for contradictory scope, missing dependencies and unjustified maintenance claims. Runtime tests belong to the implementation packages and were not claimed as executed by this planning task.

## 13. Full-audit reconciliation and additional implementation scope

The [full source-maintenance audit](../../audits/2026-11-16-source-maintenance/README.md) supersedes the earlier candidate scan as the remediation inventory: 380 registered ingest assets, nine shared downstream assets and 14 dormant ingest definitions at the same baseline. All 305 crosswalk rows are retained; crosswalk hosting deprecation is not producer retirement. The complete per-asset evidence and recommendations are in the audit JSON.

The audit identifies 15 superseded selections, 61 maintenance-unverified selections, 296 with producer/program maintenance evidence, five intentional references and three already parked. Those categories do not imply publication acceptance. Separately, 211 live-capture assets enforce fixed observed constraints and 104 select fixed editions/snapshots; current2026 editions and intentional references are not automatically stale.

**WBD correction:** fresh authoritative national XML names publication2026-09-02 and update frequency “As needed.” Retain this alongside the legacy-program retirement language. All nine WBD rows are maintenance-unverified, not confirmed abandoned sources. Reconcile stewardship before replacing/parking their geometry; source-faithful2026 snapshot availability is distinct from proof of ongoing maintenance.

### P13 — Current Census editions and NBI format transition

**Modify:** source/asset modules for `census_block_groups`, `census_designated_places`, `census_tracts`, `combined_statistical_areas`, `county_subdivisions`, `incorporated_places`, `secondary_school_districts`, `unified_school_districts`, shared `census_school_districts`, and `census_primary_roads`; their existing focused tests. Modify `sources/national_bridge_inventory.py`, matching asset and `tests/test_national_bridge_inventory.py` only after the current SNBI source contract is verified.

- [ ] Replace eight2024 Census selections and the2025 primary-roads selection with actual current2026 producer editions, preserving native CRS, identifiers and source schema. Add release discovery so future annual updates do not require repeating this migration. Keep block-group/tract supplied-input modes explicit until actual acquisition is implemented.
- [ ] Test latest published directory selection, a later annual release, missing jurisdiction/files, changed schema/IDs, full source accounting, no-op/revision behavior and stable earlier published paths. Do not relabel119th congressional districts as120th or2020 VTD as current-term equivalents.
- [ ] Verify the applicable public SNBI release, bulk schema, native identifiers and coordinates through FHWA. The existing2025 ASCII/Coding Guide path cannot be assumed to advance indefinitely: FHWA says2025 is its final submission year. Deliver an evidence-backed SNBI go/no-go before code changes; preserve the older published bridge product.
- [ ] For an accepted SNBI feature product, implement explicit field/grain/coordinate mappings and a complete current capture/readback. Test changed-width/type/identifier fields, source-population loss, current edition selection and immutable versions, using P1–P5/P11 rather than a parallel framework.

**Done:** these additional older editions/format contracts are either replaced with verified current source-native pipelines or explicitly blocked on a concrete current-source/schema decision. No historical backfill is required.

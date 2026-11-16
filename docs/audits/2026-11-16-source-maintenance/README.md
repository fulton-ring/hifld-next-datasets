# Full source-maintenance audit

Audited 2026-11-16 against `origin/main` **5eea5a18f39e3209cfc194643a742f1b2ea45645**.

## Scope and coverage

This is exhaustive for the code baseline: **389 registered Dagster assets = 380 ingest assets across 290 modules + 9 shared catalog/format/release assets**. Every registered ingest key has one audit record. **14 additional dormant ingest definitions** are listed separately. This is a source-maintenance and refresh-behavior audit, not a new full-population loading/quality certification or production acceptance. Unmerged PRs and not-yet-implemented datasets are outside the registered-pipeline count.

Three parallel source-family reviews plus a separate energy/EPA review traced actual acquisition and source-specific constraints. Shared source families reuse publisher maintenance evidence; there were not 380 independent live source probes. HTTP paths were traced statically and selected first-party metadata/pages were checked; no production materializations or geographic-payload downloads were performed for this full-audit pass. Failed/blocked/unknown evidence stays visible.

Artifacts: [every registered pipeline](pipelines.md), [machine-readable complete audit and evidence](inventory.json), [all crosswalk rows and mappings](crosswalk.json), [exact original crosswalk workbook](hifld-open-crosswalk.xlsx), [registered graph coverage witness](registered-asset-graph.json), [retained primary observations](primary-observations.json).

## HIFLD crosswalk reconciliation

The DHS-authored workbook has **305 data rows: 51 Migrated and 254 Deprecate**. The definitions sheet explains deprecation as removal from HIFLD hosting; it does not establish that the original producer stopped maintenance. Many deprecated rows point to active agency sources. Conversely a Migrated row can require GII access without proving that our selected public copy is current.

Recovered metadata copy: [https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx](https://atcoordinates.info/wp-content/uploads/2025/08/hifld_open_crosswalk_geoplatform.xlsx); 61,689 bytes; SHA-256 `7f3394fed2712fdcc176b7ce658293fdded656b9b719f0d90154c07dc38772bc`. This matches the crosswalk identity already cited by source implementations. This is the routing/metadata workbook, not an archived geographic dataset.

**376/380 registered ingests have a crosswalk row/family association covering 245 distinct crosswalk rows.** The remaining four are GSA IOLP buildings, GSA IOLP leases, NLDI streamgage monitoring locations and U.S. landslide regions; they have no asserted crosswalk match. All 305 crosswalk rows remain in the artifact, including the 60 with no registered implementation mapping. A shared family row may map many assets, and separately scoped successors are not claims of original-product equivalence.

Normalized endpoint comparisons find **216** ingests using the same crosswalk endpoint/service family and **160** using a different endpoint. A different endpoint is a review prompt, not an automatic defect: original Census downloads, FCC bulk files, current NCES, USACE-native services and explicit partial products can be better routes. The per-row evidence explains source selection.

Three EIA endpoints named Open REST Service in the crosswalk currently return ArcGIS499 Token Required (biodiesel, ethanol, gas hubs). Access failure does not establish retirement and does not justify substituting an old ZIP as a maintained source. Those bounded responses are retained in primary-observations.json.

## Selected-source disposition

| Disposition | Registered ingests | Meaning |
|---|---:|---|
| Maintained program/source evidence | 296 | Publisher/program evidence supports continued maintenance; does not certify every field or current pipeline behavior. |
| Selected edition superseded | 15 | An identified later producer edition or successor exists; the adapter still selects an older contract. |
| Maintenance unresolved | 61 | Currentness/cadence/source declarations remain unresolved; no invented retirement claim. |
| Intentional fixed reference | 5 | Explicit fixed term, boundary edition or historical period; not automatically a broken current-feed replacement. |
| Already parked/refuses | 3 | Unconditional acquisition refusal already prevents misleading refresh. |

**These categories sum to 380; “maintained” is not “ready to publish.”** Four FRS power-generation point/table ingests retain June2014 EIA constituent descriptions despite ongoing FRS distribution. Nine WBD ingests are in the unresolved category because source declarations conflict, as detailed below.

## Adapter refresh behavior

| Behavior | Assets | Interpretation |
|---|---:|---|
| Live acquisition with fixed observed constraints | 211 | Source can be maintained while old counts, IDs, records, geometry, extent or vertex pins reject legitimate updates. Each row identifies the actual constraints; schema checks alone are not treated as frozen-population checks. |
| Fixed edition/snapshot contract | 104 | Includes current2026 annual Census files and intentional references as well as superseded editions. Not all104 are stale today, but fixed-edition selection is not automatic discovery of a later release. |
| Dynamic live-population capture | 57 | Current counts/pages/IDs are reconciled rather than requiring old observed population. Metadata, schema, coverage and publication acceptance still apply. |
| Explicit latest-release resolver | 3 | GSA buildings, GSA leases, NCUA. This is distinct from live-service capture, which may not need an edition resolver. |
| Supplied local inputs only | 2 | Census block groups and tracts; do not report a new upstream refresh when replaying retained archives. |
| Source acquisition already blocked | 3 | Natural-gas processing plants, coal fields and miscellaneous industrial mineral operations. |

**375** ingests have reachable acquisition paths under their documented configuration/resource guards; **2** consume local inputs only; **3** refuse source acquisition unconditionally. Local staging requirements, stopped schedules and closed publication gates do not make a reachable HTTP path offline. Three NOAA/congressional assets require explicit supported local/live opt-ins; these are activation guards, not declarations that the producer is retired.

## Fifteen concrete superseded selections

| Pipeline | Selected source | Later maintained route / correction |
|---|---|---|
| `ingest/biodiesel-plants-1/biodiesel-plants-1` | 2021-01 | 2026-01 current combined biofuels roster. Park historical refresh; use current combined roster and separately establish current facility identity/location constituents. |
| `ingest/census-block-groups-3/census-block-groups-3` | 2024 | TIGER/Line 2026 / TGRGDB26. Migrate onto 2026 equivalent Census layer with explicit old/new field, identifier and geometry comparison, then add annual latest-release discovery. |
| `ingest/census-designated-places/census-designated-places` | 2024 | TIGER/Line 2026 / TGRGDB26. Migrate onto 2026 equivalent Census layer with explicit old/new field, identifier and geometry comparison, then add annual latest-release discovery. |
| `ingest/census-tracts-2/census-tracts-2` | 2024 | TIGER/Line 2026 / TGRGDB26. Migrate onto 2026 equivalent Census layer with explicit old/new field, identifier and geometry comparison, then add annual latest-release discovery. |
| `ingest/combined-statistical-areas-3/combined-statistical-areas-3` | 2024 | TIGER/Line 2026 / TGRGDB26. Migrate onto 2026 equivalent Census layer with explicit old/new field, identifier and geometry comparison, then add annual latest-release discovery. |
| `ingest/county-subdivisions-1/county-subdivisions-1` | 2024 | TIGER/Line 2026 / TGRGDB26. Migrate onto 2026 equivalent Census layer with explicit old/new field, identifier and geometry comparison, then add annual latest-release discovery. |
| `ingest/ethanol-plants/ethanol-plants` | 2021-01 | 2026-01 current combined biofuels roster. Park historical refresh; use current combined roster and separately establish current facility identity/location constituents. |
| `ingest/ferry-routes-1/ferry-routes-1` | 2019 observations / NCFO2020 | NCFO2024 operational data. Replace superseded census selection with latest NCFO; adopt terminal grain/IDs/coords explicitly, preserve response coverage and separately resolve maintained route-path geometry. |
| `ingest/ferry-terminals/ferry-terminals` | 2019 observations / NCFO2020 | NCFO2024 operational data. Replace superseded census selection with latest NCFO; adopt terminal grain/IDs/coords explicitly, preserve response coverage and separately resolve maintained route-path geometry. |
| `ingest/incorporated-places/incorporated-places` | 2024 | TIGER/Line 2026 / TGRGDB26. Migrate onto 2026 equivalent Census layer with explicit old/new field, identifier and geometry comparison, then add annual latest-release discovery. |
| `ingest/national-bridge-inventory-nbi/national-bridge-inventory-nbi` | 2025 | 2025 final Coding Guide ASCII edition. Implement official SNBI/NextGen distribution and schema for future releases; keep2025 ASCII historical release stable. |
| `ingest/power-plants-1/power-plants-1` | 2021-08 | 2025 annual EIA-860 / September2026 EIA-860M. Replace historical refresh with current EIA plant/generator source-native products and explicit population/location semantics. |
| `ingest/primary-roads-2/primary-roads-2` | 2025 | TIGER/Line 2026 / TGRGDB26. Migrate onto 2026 equivalent Census layer with explicit old/new field, identifier and geometry comparison, then add annual latest-release discovery. |
| `ingest/secondary-school-districts/secondary-school-districts` | 2024 | TIGER/Line 2026 / TGRGDB26. Migrate onto 2026 equivalent Census layer with explicit old/new field, identifier and geometry comparison, then add annual latest-release discovery. |
| `ingest/unified-school-districts/unified-school-districts` | 2024 | TIGER/Line 2026 / TGRGDB26. Migrate onto 2026 equivalent Census layer with explicit old/new field, identifier and geometry comparison, then add annual latest-release discovery. |

- **Biofuels:** current EIA2026 workbook is a real maintained attribute roster, but has no native coordinates, addresses or unique facility ID. It cannot directly replace2021 points. Preserve fuel-type distinctions and resolve identity/location constituents. [EIA report](https://www.eia.gov/biofuels/capacity/).
- **Ferries:** latest BTS NCFO2024 terminal data has coordinates/terminal IDs and a new one-row-per-terminal grain; current segment references do not establish route paths. The data-family statement reports63% operator response, so “latest” is not “complete national response.” [BTS NCFO](https://www.bts.gov/NCFO), [current data family](https://data.bts.gov/stories/s/2024-NCFO-Data-Files/y2vm-k4wf/).
- **NBI:** FHWA says2025 is the final legacy ASCII/Coding Guide submission year; the maintained program moves to SNBI. Do not present the old-format adapter as a perpetual current pipeline. Latest applicable SNBI public release/schema must be established before migration. [FHWA ASCII transition notice](https://www.fhwa.dot.gov/bridge/nbi/ascii.cfm).
- **Census:** eight adapters select2024 products and one selects2025 primary roads while2026 is published. Keep published versions stable, add current edition handling and review schema/identity changes. Term-specific119th Congress and2020 VTD are separately intentional references. [Current TIGER/Line](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html).

## WBD: conflicting evidence, not a confirmed dead-source list

All nine WBD pipelines fetch legacy USGS WBD layers0–8. The 3DHP access page classifies legacy WBD among products no longer maintained, but the authoritative national XML names **publication2026-09-02**, **progress Complete**, **update As needed**. The service also says refreshed October2026; older agency guidance describes periodic WBD updates. These are contradictory signals. **This audit classifies all nine maintenance_unverified, not definitively dead.** A publication/republishing date alone cannot establish the maintenance scope of individual boundaries.

Before replacing/parking these sources, reconcile the actual original-producer stewardship and available current product. 3DHP is maintained but not a proven interchangeable source for every HUC/WBDLine contract. Preserve optional14/16-digit incomplete-coverage semantics.

[USGS program transition](https://www.usgs.gov/3d-hydrography-program/access-3dhp-data-products), [WBD program/snapshot](https://www.usgs.gov/national-hydrography/watershed-boundary-dataset), [current national FGDC metadata](https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/National/GDB/WBD_National_GDB.xml). The bounded XML observation and SHA are retained in primary-observations.json.

## Remaining unresolved maintenance cases

All61 unresolved registered rows are individually listed in pipelines.md/inventory.json. They include the nine WBD layers; EPA legacy region services; several FEMA/USACE office/reference layers; FY24 FUDS/MIRTA products; older BTS freight/bridge compilations; old EIA pipeline/hub geometry; and scientific/event services whose continued source maintenance was not established by this pass. A reachable endpoint or modern ArcGIS edit stamp was not accepted as sufficient proof. None is silently declared unrecoverable.

Fix the missing evidence per row: source stewardship/update policy, actual latest applicable edition, data-versus-publication dates, scope/rights, and native geometry. Do not treat every unresolved record as an abandoned producer product. NOAA historical series, Holocene scientific scope and a2022-named Red Cross service are not automatically old snapshots; current producer evidence matters.

## Dormant implemented definitions

These14 are not in the registered Definitions graph. “Dormant fetch enabled” describes a callable function, not an enabled schedule or active factory worker. Some still require isolated local storage.

| Dormant key | Runtime | Source disposition / action |
|---|---|---|
| `ingest/above-ground-lng-storage-facilities/above-ground-lng-storage-facilities` | dormant_fetch_enabled | Third-party PASDA historical mirror remains a callable download; absent from registered graph. Disable excluded mirror acquisition; investigate maintained LNG facility sources. |
| `ingest/aquifers/aquifers` | dormant_fetch_enabled | Official static2003 principal-aquifer reference publication; ongoing edition maintenance not established. Keep distinct reference scope; not an admitted current-refresh facility/source. |
| `ingest/child-care-centers/child-care-centers` | dormant_fetch_enabled | EPA-distributed ORNL2015 database, not verified maintained; absent from registered graph. Use maintained government-roster reconstruction; do not count historical capture as refresh. |
| `ingest/colleges-and-universities-campuses/colleges-and-universities-campuses` | dormant_local_only | Local preserved2022 HIFLD/ORNL campus inputs only. No maintained polygon pipeline; investigate producer campus boundaries. |
| `ingest/construction-minerals-operations/construction-minerals-operations` | dormant_local_only | Local historical2003-survey/2005 mineplant reconstruction; producer metadata Complete/None planned. Retire preservation-as-refresh; investigate current source-native facilities. |
| `ingest/crushed-stone-operations/crushed-stone-operations` | dormant_local_only | Explicit historical preservation inputs; no current original producer feed selected. Do not admit as maintained refresh; research current quarry references. |
| `ingest/dialysis-centers/dialysis-centers` | dormant_fetch_enabled | RxOpen Inactive map is an operating state, not proof collection is discontinued. Verify producer currency, ownership and maintained population before admission. |
| `ingest/electric-retail-service-territories/electric-retail-service-territories` | dormant_refuses | Frozen2025 NBAM HIFLD mirror entrypoint refuses before acquisition. Preserve refusal; use current state-native partial sources. |
| `ingest/epa-facilities-labels/epa-facilities-labels` | dormant_fetch_enabled | EPA2012-titled service; no verified ongoing office-location maintenance. Resolve current office-location publication; do not confuse FRS regulated facilities. |
| `ingest/epa-facilities/epa-facilities` | dormant_fetch_enabled | EPA2012-titled service; no verified ongoing office-location maintenance. Resolve current office-location publication; do not confuse FRS regulated facilities. |
| `ingest/ferc-regions/ferc-regions` | dormant_local_only | Local historical preservation with no current polygon acquisition. Research current FERC responsibility definitions/native boundaries. |
| `ingest/ferrous-metal-mines/ferrous-metal-mines` | dormant_fetch_enabled | Dataset payload requires preserved local archive; optional surviving USGS metadata can be fetched.2003 survey/2005 product. Retire historic refresh; current regulated-mine scope requires distinct source contract. |
| `ingest/ferrous-metal-processing-plants/ferrous-metal-processing-plants` | dormant_fetch_enabled | Callable default archive acquisition remains for2005 NationalAtlas/2003 survey, despite unregistered asset. Disable historical archive network path; investigate maintained processor inventories. |
| `ingest/intermodal-passenger-connectivity-database-ipcd/intermodal-passenger-connectivity-database-ipcd` | dormant_fetch_enabled | Producer explicitly stopped updates; latest information2022. Empty module registration lists; callable old capture remains. Keep unregistered and guard residual acquisition rather than revive historical pipeline. |

The remaining callable archival download in ferrous-metal processing and PASDA LNG mirror path should be disabled rather than revived for refresh. IPCD is explicitly discontinued but already unregistered. Campus/FERC/local mineral preservation is not a maintained refresh implementation. No excluded mirror payload was fetched by this audit.

## Nine shared downstream assets

All nine were included in the graph census. They format/catalog staged versions, prepare/verify/promote releases or apply reviewed catalog generations; they do not select a new original upstream release. Their success therefore cannot certify source currency. The [implementation plan](../../superpowers/plans/2026-11-16-maintained-source-refresh.md) covers metadata propagation, format losses, readiness/orchestration, release acceptance and immutable publication.

| Asset | Code |
|---|---|
| `publish/catalog` | [src/dagster_hifld/assets/catalog.py:67](https://github.com/fulton-ring/hifld-next-datasets/blob/5eea5a18f39e3209cfc194643a742f1b2ea45645/src/dagster_hifld/assets/catalog.py#L67) |
| `publish/formats/geopackage` | [src/dagster_hifld/assets/publish.py:1448](https://github.com/fulton-ring/hifld-next-datasets/blob/5eea5a18f39e3209cfc194643a742f1b2ea45645/src/dagster_hifld/assets/publish.py#L1448) |
| `publish/formats/geoparquet` | [src/dagster_hifld/assets/publish.py:1469](https://github.com/fulton-ring/hifld-next-datasets/blob/5eea5a18f39e3209cfc194643a742f1b2ea45645/src/dagster_hifld/assets/publish.py#L1469) |
| `publish/formats/pmtiles` | [src/dagster_hifld/assets/publish.py:1494](https://github.com/fulton-ring/hifld-next-datasets/blob/5eea5a18f39e3209cfc194643a742f1b2ea45645/src/dagster_hifld/assets/publish.py#L1494) |
| `publish/formats/shapefile_zip` | [src/dagster_hifld/assets/publish.py:1515](https://github.com/fulton-ring/hifld-next-datasets/blob/5eea5a18f39e3209cfc194643a742f1b2ea45645/src/dagster_hifld/assets/publish.py#L1515) |
| `publish/portolan_catalog` | [src/dagster_hifld/assets/portolan.py:38](https://github.com/fulton-ring/hifld-next-datasets/blob/5eea5a18f39e3209cfc194643a742f1b2ea45645/src/dagster_hifld/assets/portolan.py#L38) |
| `publish/promote` | [src/dagster_hifld/assets/publish.py:1408](https://github.com/fulton-ring/hifld-next-datasets/blob/5eea5a18f39e3209cfc194643a742f1b2ea45645/src/dagster_hifld/assets/publish.py#L1408) |
| `publish/release_metadata` | [src/dagster_hifld/assets/portolan.py:15](https://github.com/fulton-ring/hifld-next-datasets/blob/5eea5a18f39e3209cfc194643a742f1b2ea45645/src/dagster_hifld/assets/portolan.py#L15) |
| `publish/verify` | [src/dagster_hifld/assets/publish.py:1377](https://github.com/fulton-ring/hifld-next-datasets/blob/5eea5a18f39e3209cfc194643a742f1b2ea45645/src/dagster_hifld/assets/publish.py#L1377) |

## Implementation order

1. Replace the15 superseded selections with scoped maintained successors; resolve biofuel locations, ferry paths and SNBI schema before promising equivalent geometry.
2. Migrate live-source fixed-observation checks to capture-time completeness under existing #994, preserving transport/schema/native-ID/geometry integrity and documented review of actual source changes.
3. Resolve the61 maintenance-evidence gaps; WBD needs reconciliation rather than unsupported retirement or maintenance claims.
4. Guard dormant archive/mirror acquisition and keep already-parked sources contained.
5. Propagate source dates, maintenance evidence, semantic dictionaries, original documentation, geometry lineage and per-format changes through existing Dagster/catalog/release boundaries. Each accepted change becomes a new immutable catalog version; no source, rights or production-release acceptance is implied by this audit.

## Ownership release

At the user’s request, interactive ownership was removed from20 open issues: #997, #994, #967, #966, #965, #964, #963, #960, #959, #958, #956, #954, #953, #952, #372, #355, #352, #208, #201 and #172. Each received a handoff comment, and an immediate GitHub check returned no open issues assigned to this agent account. Review lanes, existing managed runs and dependency holds were preserved. #208 remains under #1009/#1008 and must not be independently relaunched.

## Verification and limits

- Real Definitions enumeration:389 unique keys; exact set equality with380 audited ingests and9 shared downstream records.14 dormant definitions are disjoint.
- Crosswalk:305 unique data rows; source workbook hash preserved; all per-row associations refer to existing rows.245 crosswalk rows map registered implementations,60 do not. No maintenance certification is inferred for unimplemented rows.
- Every ingest has source paths/URLs, maintenance disposition, refresh/runtime behavior, evidence references, code references and a recommended action. Evidence IDs and existing code-file/line references are validated.
- Source maintenance evidence is live first-party where available, otherwise explicitly repository-retained primary evidence or unresolved. Family maintenance is not a claim that every feature or constituent is recent.
- No full geographic captures, end-to-end loading tests, production publication, new tickets, purchases or external provider contacts were performed for this audit. The earlier research plan’s source-schema samples are not recast as audit load certifications.
- The worktree still uses the pinned baseline even if factory work advances main later; rerun/review changed modules before implementing fixes.

# AGENTS.md: hifld-next-datasets

Dagster pipelines that rebuild each HIFLD Open dataset from its original agency
source, stage candidates, generate catalog metadata and derived formats, and
publish approved releases. Factory roles, board flow and merge procedure are in
the factory playbook (`/data/home/.factory/instructions/projects/hifld-next.md`),
not here.

## Scope

- HIFLD Next holds features: things that exist now, usually with geometry
  (boundaries, infrastructure, facilities, service territories).
- Dated records of actions (filings, permits, interconnection requests, notices)
  belong in civicdata (`fulton-ring/bigass-data-masheen`), which joins to HIFLD
  Next by identifiers and reads its published GeoParquet at a pinned release. A
  substation belongs here; a proposed substation in an interconnection request
  belongs in civicdata.
- Published paths are `{dataset}/{file}/{version}/...` and must stay stable and
  addressable, because consumers pin versions. Layers keep their source CRS;
  consumers read it from the file.

## Layout

- `src/dagster_hifld/sources/<dataset>.py`: source adapter (capture, validation,
  candidate construction).
- `src/dagster_hifld/assets/<dataset>.py`: ingest asset, blocking completeness
  check, draft job and stopped schedule, registered through module lists.
- `src/dagster_hifld/assets/{catalog,publish,portolan}.py`,
  `native_geoparquet_catalog.py`, `conversion.py`, `release_approval.py`,
  `promotion.py`, `portolan/`: shared catalog, format and release stages.
- `src/dagster_hifld/definitions.py`: Dagster `defs` and `version_discovery_sensor`.
- `tests/test_*.py`: unittest modules, offline fixtures (`httpx.MockTransport`).
- `src/hifld_next_datasets/` and `prompts/`: the separate Agno source-finder CLI
  (`uv run find-dataset-sources`), used only for source research.
- `HIFLD_Open_Inventory_12112025.csv`: the archived HIFLD Open inventory.
  `helm/`, `Dockerfile`: user-code image and chart values. `ops/`: acceptance
  fixtures and GCS lifecycle policies.

## Commands

- `uv sync --locked` (Python 3.12). Run everything through `uv run`. Change
  dependencies with `uv add`/`uv remove` so `uv.lock` stays in sync.
- Full suite: `uv run python -m unittest discover tests`.
- One module: `PYTHONPATH=tests uv run python -m unittest test_<name>`.
- PMTiles tests need `tippecanoe` and `tippecanoe-decode` on `PATH`.
- Local Dagster: `uv run dagster dev -m dagster_hifld.definitions` with a
  persistent `DAGSTER_HOME`. Storage comes from `HIFLD_STAGING_BUCKET` or
  `HIFLD_STAGING_DIR` (default `data/staging`), and `HIFLD_DATASETS_BUCKET` or
  `HIFLD_DATASETS_DIR`.

## CI

- `.github/workflows/ci.yml` runs on the PR merged into current `main`, with full
  Git history (provenance tests read old blobs), local storage only, a writable
  `/workspace`, and tippecanoe built from felt/tippecanoe `2d548bed`. Test modules
  are split across four shards by file name. The required check is `test`.
- `publish-images.yml` only builds on PRs and on this fork. Images are published
  from the upstream `Public-Environmental-Data-Partners/hifld-next-datasets`.
- Do not change `.github/` from dataset or maintenance work.
- When `main` is red, every open PR is red. Tests must be deterministic. Do not
  assert byte identity of GeoPackage, DBF or other outputs that embed creation
  times. Use `gdal.with_geopackage_creation_time` and set the DBF date, or compare
  decoded content. Do not depend on the wall clock or on run-time receipts in
  content-derived versions.

## Source rules

- Pipeline inputs come only from the producing agency, or from a distributor the
  agency explicitly delegates (for example BTS NTAD for FRA or USACE layers). For
  an ArcGIS item, check that the owner and org are the agency's.
- Never fetch from mirrors or archives, even as a fallback or comparison: PASDA,
  DataLumos/ICPSR, Stanford EarthWorks (`stacks`/`purl.stanford.edu`),
  `web.archive.org`, Source Cooperative/SeerAI, Esri or DHS HIFLD copies, or
  community compilations. They may be cited as research context, but never fetched.
- Build on the current maintained product. Before writing code, check the
  source's last data update and refresh cadence (`editingInfo`, release notes,
  FGDC `Status/Update`, file dates) and whether the agency has superseded it (a
  newer TIGER vintage, the EIA Energy Atlas instead of `eia.gov/maps/map_data`
  zips, EPA_GEO FRS services instead of frozen `geodata.epa.gov` layers, a revised
  USGS data release). Label historical sources as historical.
- Do not build a pipeline on a release the publisher no longer maintains (retired,
  frozen, "None planned", or a one-time release) unless currently published data
  can recreate it. Otherwise, record what you checked on the issue and stop. Do
  not write a docs-only PR for that.
- If the only route to a current source needs a token or access grant, stop and
  ask. Do not substitute an older public file.
- OSM, Overture, Google and commercial POI or imagery sources are not pipeline
  inputs without a recorded rights decision. See
  `docs/source-licensing-and-redistribution.md` for permitted operations,
  attribution and redistribution obligations.

## Engineering rules

- Prefer native platform features and existing, well-maintained libraries
  (GDAL/OGR, Shapely, pyogrio, pyarrow, httpx, Dagster, `uv`) over writing your
  own. Do not hand-roll geometry assembly, format readers or writers, retry
  loops, paging or schedulers when a library or platform feature already does it.
- When you are about to copy a function, class or block from another module,
  extract it into a shared helper module instead, and switch the existing copies
  to it in the same PR (or open an issue that lists them if that is too large).
  Two copies is the limit.

## Dataset pipeline conventions

- A new dataset is normally three files: `sources/<x>.py`, `assets/<x>.py` and
  `tests/test_<x>.py`. Register it by defining module-level `INGEST_ASSETS`,
  `ASSET_CHECKS`, `JOBS` and `SCHEDULES` in `assets/<x>.py`. `assets/discovery.py`
  collects them, and an asset that is not in those lists is not registered. Do
  not add entries to `assets/ingest_registry.py` or the lists in `definitions.py`.
  When resolving a conflict there, move the entries into the module's lists.
- Ingest asset keys are `ingest/<dataset_slug>/<file_slug>`, with the slugs taken
  from the inventory. The ingest asset stages a content-derived candidate under
  `{dataset}/{file}/{candidate}/` and registers the publish partition
  `{dataset}/{file}/{candidate}` (`partitions.build_publish_partition_key`).
  Downstream assets (`publish/catalog`, `publish/formats/*`,
  `publish/release_metadata`, `publish/verify`, `publish/promote`,
  `publish/portolan_catalog`) are shared and need no per-dataset code.
- Keep schedules `DefaultScheduleStatus.STOPPED` (enforced by
  `tests/test_schedule_config_validation.py`), and keep every acceptance or
  approval flag `False`. Merging code never authorizes publication. Release
  approval pins the full staged inventory and happens outside PRs.
- Reuse the shared helpers instead of copying them from another adapter:
  `http_capture.capture_http_response`, `sources/capture_common.py` (generic
  capture, candidate, seal and quarantine primitives), `sources/arcgis_layer.py`
  (ArcGIS layer capture, replay, retry and geometry encoding),
  `sources/esri_common.py`, `sources/esri_geometry.py`, `esri_pbf.py`,
  `sources/arcgis_filegdb.py`, `sources/census_tiger_common.py`,
  `sources/nfhl/`, `sources/epa_icis_wastewater_common.py`,
  `sources/electric_retail_service_territories_common.py`,
  `sources/national_inventory_of_dams_common.py`, `sources/wbd_hu_family.py`,
  `construction_state.py`,
  `construction_provenance.py`, `provenance.py`, `validation.sha256_file` and
  `gdal.py`. If the logic you need exists only inside another dataset module,
  move it into a shared module in the same PR. Do not import it from that dataset
  module, and do not copy it. `tests/test_sources_module_contract.py` enforces
  this: a `sources/` dataset module may import only shared modules (or its own
  `<stem>_spec` pinned manifest), and shared modules never import dataset
  modules.
- Families share infrastructure: FEMA NFHL MapServer layers (`sources/nfhl/`; a
  new layer is one `LAYERS` entry), Census TIGER (`census_tiger_common`), USACE
  IENC, WBD hydrologic units, NHD/3DHP and GNIS. Build a family member as a thin
  adapter over the shared module, and extend the shared module when a member
  needs more.
- ArcGIS layer capture: capture the service and layer documents and the count
  plus an ID roster or OBJECTID count/min/max statistics before paging, page by
  exact ID slices or bisected OBJECTID windows (`sources/nfhl/capture.py`), then
  capture the "after" witnesses once the last page is done. Preserve failed attempts as evidence, keep
  memory bounded (stream pages to disk, not to a population-wide dict), and
  enforce byte budgets during acquisition.
- Preserve the source as served: the native ID column (including an `fid` that
  Fiona exposes as `feature.id`), field types and precision (Esri dates are epoch
  milliseconds), the source datum and CRS (FCC ULS coordinates are NAD83), and the
  geometry. Assemble polygons with GDAL/Shapely, not a hand-written ring
  assembler, and report invalid source geometry rather than repairing it silently.
- Every failure path that quarantines or writes evidence needs a test that
  exercises it. Untested `except` branches have shipped with `NameError`.
- Scratch and capture directories come from asset config (`capture_directory`);
  the shared fallback root is `HIFLD_CAPTURE_ROOT` (`resources.default_capture_dir`).
  Do not add hard-coded capture-root defaults or per-dataset env vars.
- Keep module docstrings to source identity and behavior. Dated investigation
  narrative belongs in the PR and the issue.

## Pull requests

- Target `main`. When the PR completes an issue (meets its acceptance
  criteria), write `Closes #<issue>` in the body so GitHub closes it on merge.
  Use `Refs #<issue>` only for related or partially covered issues, and say what
  remains. One open PR per issue: update the existing PR rather than opening
  another.
- In the PR body, list every URL the pipeline fetches, who publishes it, and its
  last data update.
- Keep PRs focused. Add files only for runtime code, meaningful tests or essential
  provenance. Do not delete, skip or weaken existing tests to pass CI.

## Deployment

- `Public-Environmental-Data-Partners/hifld-next-datasets` (upstream) is the
  deploy source. This fork must not publish images. Changes reach production only
  after they are synced upstream and deployed through `hifld-next-iac`
  (`deploy-dagster.yml` and `scripts/deploy-dagster.sh`, Helm release `dagster` in
  GKE `hifld-prod`, namespace `hifld-next-datasets`).
- Production publication requires independent release approval of the exact
  staged inventory. Staging runs must never run `publish/promote`.

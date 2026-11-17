The `hifld-next-datasets` package wraps the HIFLD inventory CSV and an Agno-powered agent that looks up source download URLs for each row. It also ships a **Dagster** library (`dagster_hifld`) for downloading datasets to staging, generating **quality manifests** and **data dictionaries**, and optionally publishing derived formats.

## Dagster pipeline (`dagster_hifld`)

High-level flow:

1. **Ingest** — One asset per dataset file (e.g. `amtrak-stations/amtrak-stations`); downloads from source URLs and stages raw geodata under the versioned path below.
2. **Catalog** — Partitioned by **version**; reads staged data, writes `metadata/quality_manifest.json` and `metadata/data_dictionary.json`.
3. **Publish** — Partitioned by the staged candidate; prepares formats and release metadata, verifies independent approval, and copies to the semantic release version before publishing Portolan/STAC discovery metadata and the SQLite catalog.

**Staging path shape** (no agency prefix, no `source/` wrapper):

`{dataset_slug}/{file_slug}/{version}/{format}/...`

Example: `gs://<staging-bucket>/amtrak-stations/amtrak-stations/v20260407T204046Z/shapefile/...`

**Candidate IDs** identify staged work. Existing ingests use UTC run timestamps or source/code fingerprints. They are not published release numbers.

**Published versions** come from the standard STAC `version` field in
`{dataset}/{file}/{candidate}/collection.json`. There is no fallback to the
candidate partition or a draft manifest's supplied version.

After generating formats and comparison evidence, materialize
`publish/release_metadata` for the candidate with Dagster configuration:

```yaml
ops:
  publish__release_metadata:
    config:
      version: "v1.5.0"
```

This prepares a STAC Collection with `version: "1.5.0"`, an ID ending in
`v1.5.0`, and final asset URLs. It writes staging only. To override the version,
rerun this preparation with the desired version; it regenerates the collection
and version-dependent style URLs. Do not edit only the Collection's version and
leave its ID or URLs pointing at the old release. The Python helper is
`prepare_staged_release(request, staging, published, staged_version=...)`.

Independent release approval must pin the complete staged inventory, including
`collection.json`, and its exact destination inventory under
`{dataset}/{file}/v1.5.0/`. Changing metadata or files requires fresh approval.
`publish/verify` and `publish/promote` continue to select the candidate partition,
but use the approved STAC version for publication. Promotion preserves evidence
bytes, refuses conflicting published files and permits identical retries.
The terminal catalog asset uses the same release version and the existing
separately approved immutable catalog plan. Existing historical releases are
not renamed. Version selection is explicit; no heuristic automatically decides
compatibility or grants publication acceptance.

**Partitions** — Each `(dataset_slug, file_slug)` pair has its own `DynamicPartitionsDefinition`. Partition keys are version strings. The `version_discovery_sensor` scans staging for versions that have geodata but lack `metadata/quality_manifest.json`, registers the partition, and requests a catalog materialization (for pairs known to the code).

**Configuration** — `StagingStorageResource` / `PublishedStorageResource` read:

- `HIFLD_STAGING_BUCKET` — use GCS when set; otherwise local `HIFLD_STAGING_DIR` (default `data/staging`).
- `HIFLD_DATASETS_BUCKET` — published bucket (or `HIFLD_DATASETS_DIR` locally).

**Capture directories** — every adapter resolves retained-capture and scratch
space through one shared root, `HIFLD_CAPTURE_ROOT` (default: the system
temporary directory). Adapters accept an explicit `capture_directory` (asset
run config or direct argument) for replay or resume; unset runs capture under
`<root>/<dataset>/<file>/<run_id>`. There are no per-dataset capture env vars.

**Run Dagster** (from this repo):

```bash
uv run dagster dev -m dagster_hifld.definitions
# or
uv run dagster-webserver -m dagster_hifld.definitions
```

Use a persistent `DAGSTER_HOME` if you want runs and partitions to survive restarts.

**Tests** — `uv run python -m unittest discover tests`

CI (`.github/workflows/ci.yml`) runs on Python 3.12 with full Git history, local
storage, a writable `/workspace`, and `tippecanoe`/`tippecanoe-decode` built from
felt/tippecanoe `2d548bed`. Test modules are split across four shards; the
required check is `test`.

Retained USACE integration captures are opt-in through
`HIFLD_DATA_COVERAGE_AREAS_CAPTURE_DIR` and `HIFLD_DRY_DOCKS_CAPTURE_DIR`. Missing
configuration skips those external-input tests; a configured missing or invalid
capture fails. The external IaC integration uses `HIFLD_IAC_CHECKOUT` or an
existing sibling `hifld-next-iac` checkout. Default synthetic tests and passing
code checks do not approve real data or authorize publication.


**Agent-oriented detail** — Design choices, multi-file datasets (e.g. 2020 census blocks), sensor behavior, and pitfalls live in [AGENTS.md](AGENTS.md).

### GeoParquet Hive partitions

Partitioned GeoParquet uses normal semantic Hive paths such as
`state_fips=06/part-000.parquet`. Source columns are retained in the Parquet
files; partition keys are not replaced by synthetic names. Hive keys and values
are percent-escaped (`a/b` becomes `a%2Fb`). Null values use Hive's standard
`__HIVE_DEFAULT_PARTITION__` sentinel, and a literal value equal to that
sentinel is rejected because it would be ambiguous.

Within each final Hive partition, features are globally ordered by the S2
Hilbert curve before Parquet row groups are formed. The publisher uses a
compressed, disk-backed SQLite spool so this ordering stays bounded in memory;
Kubernetes steps request a 250 GiB scratch volume for the source, spool, and
output files. Row groups target 128 MiB uncompressed and may grow only to the
160 MiB validation ceiling before being split.

Readers should provide partition types when numeric-looking values represent
strings. For DuckDB, use for example:

```sql
SELECT * FROM read_parquet(
  '.../geoparquet/**/*.parquet',
  hive_partitioning = true,
  hive_types = {'state_fips': 'VARCHAR'}
);
```

For PyArrow, use an explicit Hive partition schema:

```python
partitioning = ds.partitioning(
    pa.schema([("state_fips", pa.string())]), flavor="hive"
)
dataset = ds.dataset(path, format="parquet", partitioning=partitioning)
```

`pyarrow.parquet.ParquetFile` reads one file's physical columns only; it does
not parse partition values from the parent directory. Use a dataset reader when
you need path-derived Hive fields.

### Deployment

`Public-Environmental-Data-Partners/hifld-next-datasets` (upstream) is the deploy
source. This fork does not publish images; `publish-images.yml` only builds here.
Changes reach production after they are synced upstream and deployed by
`hifld-next-iac` (`deploy-dagster.yml` and `scripts/deploy-dagster.sh`, Helm
release `dagster` in GKE `hifld-prod`, namespace `hifld-next-datasets`).

[`helm/values.yaml`](helm/values.yaml) holds the cluster-agnostic chart values
(image, daemon, run launcher, queued coordinator). Cloud-specific settings live in
`hifld-next-iac` and are layered on at install time.

Production publication requires independent release approval of the exact staged
inventory. Staging runs never run `publish/promote`.

### License

The archived HIFLD Open inventory records the operator-confirmed public-domain status as `CC-PDM-1.0` and writes a collection-level `LICENSE.md` notice. The Dagster Portolan catalog asset applies this status only when `HIFLD_PORTOLAN_ARCHIVE_PUBLIC_DOMAIN=1`. It applies only to the archived `hifld` collection. Future uploads remain `license: "other"` unless rights are supplied; an explicit `license_href` takes precedence over the archive notice.

## Environment

1. `uv` created this project layout and installed the pinned dependencies (see `pyproject.toml`; includes `agno`, `polars`, `dagster`, geospatial stack, etc.).
2. A persistent `.venv` lives at the project root; always run commands through `uv run` so they execute inside the managed environment.
3. If you adjust `pyproject.toml`, run `uv sync` to re-sync the lockfile and reinstall.

## Running the agent

Use the exported script that drives the search workflow:

```bash
uv run find-dataset-sources --inventory HIFLD_Open_Inventory_12112025.csv --limit 12
```

- `--inventory` defaults to the CSV already checked in.
- `--limit` can be `0` to process the entire file; pair it with `--offset` to resume from a specific row.
- Results stream to `dataset-source-hints.jsonl` (one JSON object per dataset) and include the search query plus a short list of candidate URLs.

## Configuration

- Set `HIFLD_AGNO_MODEL` to lock the agent to a specific model ID (e.g., `gpt-5.1-codex-mini`).
- Provide whatever credentials the selected Agno provider requires (e.g., `AGNO_OPENAI_API_KEY`, `AGNO_OPENAI_ORG`), ensure DuckDuckGo access is allowed from the host, and optionally set `JINA_API_KEY` if you want the reader tools to authenticate.

## Output schema

Each line produced by the script conforms to `src/hifld_next_datasets/core.py::DatasetSources`:

- `dataset`: friendly dataset name derived from title/filename/path.
- `publisher`: optional agency/publisher metadata from the inventory.
- `query`: the DuckDuckGo search phrase the agent used.
- `summary`: optional reasoning about the matched source.
- `files`: list of `{name, format, urls}` records describing every geospatial file in the dataset; each `urls` entry mirrors a `SourceCandidate` with `label`, `url`, `confidence`, and `notes`.
- `candidates`: optional fallback list of strong download URLs if you want to mirror the files at a higher level.

Re-run the script whenever the inventory changes to refresh the source URLs or to re-target a new subset of rows.

## Source-finder workflow

Board scanning, card moves and worker admission are handled by the factory
(`fulton-ring/fulton-ring-factory`), not by workflows in this repository.

- The workflow (`src/hifld_next_datasets/workflow.py`) constructs one `Parallel` step that spins up an agent per selected dataset row, so every agent executes concurrently instead of sequential iterations. That lets the pipeline surface file-specific URLs for multiple datasets quickly while keeping the CLI glue minimal.
- Each agent run gets access to DuckDuckGo and the Jina Reader toolkit (search/query + read URL), so you can gather search snippets and page excerpts before choosing the highest-confidence candidates.

## Prompt management

- Instructions and prompt scaffolding live in Markdown under `prompts/`; `agent-instructions.md` holds the guidance, and `query-template.md` contains the metadata+query template. The workflow loader reads those files at runtime, meaning you can tune the agent behaviour without touching Python.

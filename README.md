The `hifld-next-datasets` package wraps the HIFLD inventory CSV and an Agno-powered agent that looks up source download URLs for each row. It also ships a **Dagster** library (`dagster_hifld`) for downloading datasets to staging, generating **quality manifests** and **data dictionaries**, and optionally publishing derived formats.

## Dagster pipeline (`dagster_hifld`)

High-level flow:

1. **Ingest** — One asset per dataset file (e.g. `amtrak-stations/amtrak-stations`); downloads from source URLs and stages raw geodata under the versioned path below.
2. **Catalog** — Partitioned by **version**; reads staged data, writes `metadata/quality_manifest.json` and `metadata/data_dictionary.json`.
3. **Publish** — Partitioned by the same version; converts and copies to the published bucket (and promotes metadata). Dataset API calls are intentionally not wired here yet.

**Staging path shape** (no agency prefix, no `source/` wrapper):

`{dataset_slug}/{file_slug}/{version}/{format}/...`

Example: `gs://<staging-bucket>/amtrak-stations/amtrak-stations/v20260407T204046Z/shapefile/...`

**Version IDs** are time-based UTC slugs derived from the Dagster run’s create timestamp: `vYYYYMMDDTHHMMSSZ` (see `download.build_version_id`).

**Partitions** — Each `(dataset_slug, file_slug)` pair has its own `DynamicPartitionsDefinition`. Partition keys are version strings. The `version_discovery_sensor` scans staging for versions that have geodata but lack `metadata/quality_manifest.json`, registers the partition, and requests a catalog materialization (for pairs known to the code).

**Configuration** — `StagingStorageResource` / `PublishedStorageResource` read:

- `HIFLD_STAGING_BUCKET` — use GCS when set; otherwise local `HIFLD_STAGING_DIR` (default `data/staging`).
- `HIFLD_DATASETS_BUCKET` — published bucket (or `HIFLD_DATASETS_DIR` locally).

**Run Dagster** (from this repo):

```bash
uv run dagster dev -m dagster_hifld.definitions
# or
uv run dagster-webserver -m dagster_hifld.definitions
```

Use a persistent `DAGSTER_HOME` if you want runs and partitions to survive restarts.

**Tests** — `uv run python -m unittest discover tests`

**Agent-oriented detail** — Design choices, multi-file datasets (e.g. 2020 census blocks), sensor behavior, and pitfalls live in [AGENTS.md](AGENTS.md) under the Dagster subsection.

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

### Deploying to GKE (manual)

The cluster, namespace `hifld-next-datasets`, the `dagster` ServiceAccount, the `dagster-db` Secret, and the `dagster-env` ConfigMap are all provisioned by Terraform in [`hifld-next-iac/environments/prod/dagster.tf`](../hifld-next-iac/environments/prod/dagster.tf). Make sure that's applied first.

[`helm/values.yaml`](helm/values.yaml) is intentionally cluster-agnostic — it pins the chart's structural choices (image, daemon, run launcher, queued coordinator) but does not assume a cloud. Cloud-specific concerns (e.g. GCS compute log persistence, Workload Identity bindings) live in `hifld-next-iac` and are layered on at install time.

The standard path is the deploy script in iac:

```bash
../hifld-next-iac/scripts/deploy-dagster.sh
```

It builds and pushes the user-code image, fetches Terraform outputs, and runs `helm upgrade --install` with both the chart values from this repo and the GCP overlay [`hifld-next-iac/scripts/dagster-gcp-values.yaml`](../hifld-next-iac/scripts/dagster-gcp-values.yaml).

GitHub Actions publishes the reproducible user-code image as `ghcr.io/fulton-ring/hifld-next-datasets/dagster-user:<git-sha>`. The source repository is public, but verify the GHCR package itself is **public** and anonymously pullable before deploying it to GKE; repository and package visibility are separate. Workload Identity does not authenticate Kubernetes image pulls to GHCR. Only the reduced inventory columns used by the publisher are copied into the runtime image; review the inventory descriptions before publication because they remain public data in that image.

The archived HIFLD Open inventory records the operator-confirmed public-domain status as `CC-PDM-1.0` and writes a collection-level `LICENSE.md` notice. Ordinary HIFLD Dagster publication applies this status by default; set `HIFLD_PORTOLAN_ARCHIVE_PUBLIC_DOMAIN=0` to opt out. It applies only to the archived `hifld` collection. Future uploads remain `license: "other"` unless rights are supplied; an explicit `license_href` takes precedence over the archive notice.

If you need to run the steps by hand:

```bash
SHA=$(git rev-parse --short HEAD)
gcloud auth configure-docker --quiet
docker build --platform linux/amd64 -t gcr.io/hifld-next/dagster-user:$SHA .
docker push gcr.io/hifld-next/dagster-user:$SHA

gcloud container clusters get-credentials hifld-prod \
  --region us-central1 --project hifld-next

TFDIR=../hifld-next-iac/environments/prod
PG_HOST=$(terraform -chdir=$TFDIR output -raw dagster_postgres_host)
PG_USER=$(terraform -chdir=$TFDIR output -raw dagster_postgres_user)
PG_DB=$(terraform -chdir=$TFDIR output -raw dagster_postgres_database)
LOGS_BUCKET=$(terraform -chdir=$TFDIR output -raw dagster_logs_bucket_name)

helm repo add dagster-io https://dagster-io.github.io/helm
helm repo update
helm upgrade --install dagster dagster-io/dagster \
  --namespace hifld-next-datasets \
  -f helm/values.yaml \
  -f ../hifld-next-iac/scripts/dagster-gcp-values.yaml \
  --set "postgresql.postgresqlHost=$PG_HOST" \
  --set "postgresql.postgresqlUsername=$PG_USER" \
  --set "postgresql.postgresqlDatabase=$PG_DB" \
  --set "computeLogManager.config.gcsComputeLogManager.bucket=$LOGS_BUCKET" \
  --set "dagsterWebserver.image.tag=$SHA" \
  --set "dagsterDaemon.image.tag=$SHA" \
  --set "dagster-user-deployments.deployments[0].image.tag=$SHA" \
  --set "runLauncher.config.k8sRunLauncher.image.tag=$SHA"

kubectl port-forward -n hifld-next-datasets svc/dagster-dagster-webserver 3000:80
# open http://localhost:3000
```

Pin `--version <X>` on the `helm upgrade` command once you've verified a chart release that matches the pinned `dagster` package in [`pyproject.toml`](pyproject.toml).

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

## Workflow dispatch

- The workflow (`src/hifld_next_datasets/workflow.py`) constructs one `Parallel` step that spins up an agent per selected dataset row, so every agent executes concurrently instead of sequential iterations. That lets the pipeline surface file-specific URLs for multiple datasets quickly while keeping the CLI glue minimal.
- Each agent run gets access to DuckDuckGo and the Jina Reader toolkit (search/query + read URL), so you can gather search snippets and page excerpts before choosing the highest-confidence candidates.

## Prompt management

- Instructions and prompt scaffolding live in Markdown under `prompts/`; `agent-instructions.md` holds the guidance, and `query-template.md` contains the metadata+query template. The workflow loader reads those files at runtime, meaning you can tune the agent behaviour without touching Python.

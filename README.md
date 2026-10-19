The `hifld-next-datasets` package wraps the HIFLD inventory CSV and an Agno-powered agent that looks up source download URLs for each row. It also ships a **Dagster** library (`dagster_hifld`) for downloading datasets to staging, generating **quality manifests** and **data dictionaries**, and optionally publishing derived formats.

## Dagster pipeline (`dagster_hifld`)

High-level flow:

1. **Ingest** — One asset per dataset file (e.g. `amtrak-stations/amtrak-stations`); downloads from source URLs and stages raw geodata under the versioned path below.
2. **Catalog** — Partitioned by **version**; reads staged data, writes `metadata/quality_manifest.json` and `metadata/data_dictionary.json`.
3. **Publish** — Partitioned by the same version; converts and copies to the published bucket, promotes metadata, and publishes Portolan/STAC discovery metadata and the SQLite catalog.

The historical Dataset API integration and export-seeding CLI are retired.
See [the retirement note](docs/legacy-retirement.md) for the removed interfaces,
verification, and compatibility retained for existing source data.

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

GitHub Actions publishes the user-code image as `ghcr.io/<owner>/<repository>/dagster-user:<git-sha>` and tags successful public main builds as `latest`. The owner/repository path is lowercased and derived from the workflow's repository, not hardcoded. The workflow supplies the source-repository image label so the package links to its current repository. Pull requests only build; only the publishing job gets `packages: write`.

### Finding the image and coordinating a PEDP transfer

Open **Actions → Publish datasets image → a publish run → Summary** for the image reference and direct **Open container package** link. Images are GitHub **Packages**, not release attachments or workflow artifacts. The current package is [fulton-ring's datasets Dagster image](https://github.com/fulton-ring/hifld-next-datasets/pkgs/container/hifld-next-datasets%2Fdagster-user).

Before using a PEDP-owned image, coordinate with a PEDP organization/package admin:

1. Allow creation of public container packages and publishing by the transferred repository using its `GITHUB_TOKEN` with `packages: write`. No personal access token or GCP credential is needed for this publishing workflow.
2. After the first push creates the package, open **Package settings → Change visibility → Public**. New GHCR packages default to private even when the source repository is public; the workflow deliberately does not change visibility. If the package already exists, ensure it is connected to this repository and the repository has Actions write access to it.
3. Rerun the workflow. It verifies the SHA-tagged manifest using an empty Docker credential configuration before promoting `latest`. If anonymous access fails, the run fails with instructions in its summary; the pushed SHA image may already exist privately.
4. After transferring to `Public-Environmental-Data-Partners`, the new image target will be `ghcr.io/public-environmental-data-partners/hifld-next-datasets/dagster-user:<git-sha>`. Only after the public-pull check passes should `hifld-next-iac/.github/workflows/deploy-dagster.yml` change its `DAGSTER_IMAGE` and `DATASETS_REPOSITORY` references. Keep the existing fulton-ring image available for rollback; GHCR packages stay with their owner rather than moving with the repository.

Repository and package visibility are separate, and Workload Identity does not authenticate Kubernetes image pulls to GHCR. See GitHub's [container-registry documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry) and [package access settings](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility). Only the reduced inventory columns used by the publisher are copied into the runtime image; review the inventory descriptions before publication because they remain public data in that image.

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

## Issue 81: Ethanol Plants source limitation

As checked on 2026-10-19, the dedicated [EIA ethanol capacity report](https://www.eia.gov/petroleum/ethanolcapacity/) publishes a workbook with State, Respondent, City, MMgal/yr, and Mb/d fields, but no coordinates, geometry, or spatial IDs. The two candidate [EIA Atlas dataset routes](https://atlas.eia.gov/datasets/eia::ethanol-plants-1/about) returned 404, and its [public catalog API query](https://atlas.eia.gov/api/search/v1/collections/all/items?q=ethanol) returned `numberMatched: 0`; this is a scoped check, not evidence that no other EIA feed exists. EIA's ArcGIS organization exposes item `1f0f8fd4458c43bd93642a01a02959e8`, owned by `amber.dashnaw.ctr` (profile: HSDL Librarian) and hosted at `services7.arcgis.com/FGr1D95XCGALKXqM/…/Ethanol_Plants_US_EIA/FeatureServer/112`. Its item metadata identifies EIA as the capacity attribute source and says locations are approximate points researched from public sources. Despite `access: public`, the exact layer metadata, count, and ID queries all returned ArcGIS `499 Token Required` on 2026-10-19. The separate `esinnott1` mirror is hosted under another ArcGIS account and currently returns one ID. A faithful native spatial pipeline is blocked pending restored public query access and a source-owner decision confirming location lineage, rights, and attribution; approximate locations alone do not resolve that decision. Raw response bodies and SHA-256 pins are retained outside Git under `/tmp/hifld-native-reconstruction/81/source-evidence/`.

## Workflow dispatch

- The workflow (`src/hifld_next_datasets/workflow.py`) constructs one `Parallel` step that spins up an agent per selected dataset row, so every agent executes concurrently instead of sequential iterations. That lets the pipeline surface file-specific URLs for multiple datasets quickly while keeping the CLI glue minimal.
- Each agent run gets access to DuckDuckGo and the Jina Reader toolkit (search/query + read URL), so you can gather search snippets and page excerpts before choosing the highest-confidence candidates.

## Prompt management

- Instructions and prompt scaffolding live in Markdown under `prompts/`; `agent-instructions.md` holds the guidance, and `query-template.md` contains the metadata+query template. The workflow loader reads those files at runtime, meaning you can tune the agent behaviour without touching Python.

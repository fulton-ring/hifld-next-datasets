## Agent development practices for `hifld-next-datasets`

### Environments & dependencies

- This project is bootstrapped with `uv`, which manages the vendored `.venv` and lockfile. Run `uv run <script>` so dependencies (polars, Agno, ddgs, fastapi, openai, python-dotenv, etc.) load from the managed environment.
- Keep the dependency list lean: we now rely on `polars` for CSV I/O instead of `pandas`, and only retain packages required by Agno’s search tools (DuckDuckGo, Jina Reader) and workflow plumbing (FastAPI, ddgs, OpenAI/openrouter adapters).
- When updating `pyproject.toml`, rerun `uv sync` to refresh `uv.lock` and confirm the installed packages match the manifest.

### Prompt & schema updates

- Prompts live in `prompts/agent-instructions.md` and `prompts/query-template.md`. Edit those Markdown files when you need to adjust the agent’s tone, tool guidance, or target formats; the workflow loads them at runtime so no code changes are necessary for prompt tweaks.
- Agno’s structured output is anchored by `src/hifld_next_datasets/core.py::DatasetSources`, which now includes a `files` list (each entry has `name`, `format`, and `urls` containing `SourceCandidate` records). Keep new fields minimal and descriptive to avoid schema parsing errors with structured models.
- The agent is expected to describe every geospatial file (shapefile, GeoJSON, KMZ, GDB, etc.) plus each format-specific URL and confidence. Do not prompt for free-form JSON; rely on Agno’s schema validation instead.

### Workflow & execution

- **Tool call limit:** Agno's `tool_call_limit` is **per agent run** (each parallel agent has its own budget). It is configurable via `HIFLD_TOOL_CALL_LIMIT` (default 200). If an agent hits the limit before emitting structured output, increase it or tighten the prompt.
- `src/hifld_next_datasets/workflow.py` constructs a `Parallel` step that spins up one agent per dataset row and runs them asynchronously via `workflow.arun()`. Each agent’s results are flattened into `dataset-source-hints.jsonl` by `src/hifld_next_datasets/scraper.py`, so the CLI only needs to slice the CSV with `--offset/--limit` and await the workflow.
- To run a test pass, use:
  ```bash
  uv run find-dataset-sources --inventory HIFLD_Open_Inventory_12112025.csv --limit <n>
  ```
  The script automatically loads `.env` (for `OPENROUTER_API_KEY`, `HIFLD_AGNO_MODEL`, etc.) and defaults the model to `gpt-5.1-codex-mini`.
- Default is to process all rows; use `--limit <n>` to cap how many rows run. Use `--offset <start>` to resume or reprocess a subset (e.g. `--offset 20 --limit 10`).

### Code style & best practices

- **Top-level imports:** Always use top-level imports rather than inline/local imports when integrating Agno models or tools. For example, `from agno.models.openrouter import OpenRouter` should be at the top of the file, not inside the `build_agent` function. This prevents repetitive runtime overhead and makes dependencies explicit.
- **Environment variables:** Use `load_dotenv(override=True)` instead of `load_dotenv()` when configuring CLI entrypoints like `scraper.py`. This ensures your local `.env` keys (such as `OPENROUTER_API_KEY`) correctly override any lingering keys in your global shell environment.
- **Clean up test files:** Remove temporary debug scripts (e.g., `test_*.py`) once a pipeline bug is verified and resolved.
- **Custom Native Tools:** Rather than writing complex subclasses for simple validation tasks, define standard Python functions with type hints and docstrings (like `check_download_url`) and pass them directly into the agent's `tools=[]` list. Agno natively wraps these functions. Ensure operations that might hang (like HTTP checks) use timeouts and do not eagerly download large payloads.
- **Handling ArcGIS Hub SPA pages:** Many federal datasets use ArcGIS Hub which relies on client-side JS rendering. We provide explicit instructions in the prompt for the agent to bypass the HTML and interact with the ArcGIS Hub V3 API (e.g. `/api/v3/datasets?filter[slug]=...`) to dynamically fetch the dataset ID and construct static REST download links.

### Troubleshooting

- If Agno reports “failed to parse JSON” or a schema error, inspect the command output (or add `LOG_LEVEL=debug`) to capture the raw response. Schema mismatches usually mean the agent emitted malformed JSON; adjust the prompt to keep each field simple, or add validators in `DatasetSources` if you need to coerce certain values.
- Keep a close eye on tool dependencies: DuckDuckGo requires `ddgs`, Jina Reader uses `httpx`, and the default `openrouter` model requires `openai` or the OpenRouter SDK. Reinstall missing packages via `uv add <pkg>`/`uv remove <pkg>` as needed and recompile with `python -m py_compile` to catch syntax issues early.

### Future run tips

- Encourage the agent to fingerprint source agencies (NOAA, USGS, Census, etc.) before grabbing URLs; we rely on that detail when selecting between duplicates/mirrors.
- When a dataset ships in multiple formats, create separate `DatasetFile` records so downstream consumers know which file corresponds to which format and URL.
- Instrument new tooling (e.g., a `--dry-run` mode or telemetry tagging) in the workflow if you need fine-grained auditing for later ingestion pipelines.

---

### Dagster pipeline (`src/dagster_hifld`)

This is separate from the Agno `find-dataset-sources` CLI. It implements a **three-stage** asset graph: **ingest → catalog → publish**, backed by GCS or local paths via `StagingStorageResource` / `PublishedStorageResource` (`resources.py`).

#### Design principles (learnings)

- **Explicit assets, not a mega-registry.** Each dataset (or logical file) has its own ingest asset and matching catalog/publish assets. Do not reintroduce a centralized `DATASET_SPECS_BY_SYMBOL`-style registry that tries to treat every download the same way; that pattern broke for heterogeneous sources (e.g. NOAA MapServer layers vs Census ZIPs).
- **Large multi-file datasets colocate their slug lists** next to their ingest logic. Example: `src/dagster_hifld/assets/census_blocks_2020.py` defines `CENSUS_2020_TABBLOCK20_STATE_GEOIDS`, derived file slugs, and `CENSUS_2020_DATASET_FILE_PAIRS`. `partitions.py` only **merges** that list into `DATASET_FILE_PAIRS` so the shared partition map stays small and other datasets follow the same pattern.
- **Versions are partition keys**, not hidden loops inside catalog/publish. Catalog and publish assets use `context.partition_key` for the version directory. Older “scan all versions in one asset body” helpers were removed to align with Dagster’s model and to make backfills targetable (`v20260214`, etc.).
- **Asset checks** on catalog assets must use `context.partition_key` when reading `metadata/quality_manifest.json`, not `build_version_id(context)` (that encodes the *current run* time, not the staged version being checked).

#### Object layout and metadata

- Paths: `{dataset_slug}/{file_slug}/{version}/` with format subdirs (`shapefile`, `geopackage`, `geojson`, `file_geodatabase`, etc.).
- Catalog writes: `metadata/quality_manifest.json`, `metadata/data_dictionary.json` (see `catalog.py` helpers).
- **Legacy staging:** Some copies use a folder named `unknown/` for shapefile sidecars. The version-discovery logic treats `unknown` as a signal that geodata exists (`definitions.py`), and `_load_best_file` in `catalog.py` can read `.shp` from `unknown/` after trying canonical `shapefile/`.

#### Partition registry

- `partitions.py` builds one `DynamicPartitionsDefinition` per `(dataset_slug, file_slug)` in `DATASET_FILE_PAIRS` and exposes `PARTITIONS_BY_PAIR` and `get_partition_name` (`{dataset_slug}--{file_slug}`).
- Adding a new **simple** dataset: add the pair to `_DATASET_FILE_PAIRS_BASE` in `partitions.py`, add ingest + catalog + publish wiring (and checks if desired), matching existing agency modules.
- Adding a **large** dataset with many file slugs: add `*_DATASET_FILE_PAIRS` (or equivalent) in that dataset’s module (see census blocks), then extend `DATASET_FILE_PAIRS` in `partitions.py` with `*THAT_LIST` only—keep the long tables out of `partitions.py`.

#### Sensors

- `version_discovery_sensor` (`definitions.py`) walks staging (GCS via `gcsfs.find`, local via directory walk), finds `dataset_slug/file_slug/version` with at least one recognized format dir, skips if `metadata/quality_manifest.json` exists, then `add_dynamic_partitions` + `RunRequest` for the corresponding **catalog** asset. Pairs not in `PARTITIONS_BY_PAIR` are skipped (no silent asset creation).

#### Environment variables

- `HIFLD_STAGING_BUCKET` — GCS bucket for staging (no `gs://` prefix in code; value is the bucket name).
- `HIFLD_STAGING_DIR` — local staging root when bucket unset.
- `HIFLD_DATASETS_BUCKET` / `HIFLD_DATASETS_DIR` — publish target.
- `DAGSTER_HOME` — use a real directory for durable run/partition state in dev and when using the CLI against a long-lived instance.

#### Usage notes for humans and agents

- **Materializing partitioned catalog/publish assets** from Python requires passing a `DagsterInstance` that has the dynamic partition registered (`materialize(..., instance=instance, partition_key=...)`). Otherwise Dagster raises about the instance not being available for dynamic partitions.
- **Ephemeral `materialize()`** from a random script without `DAGSTER_HOME` / instance wiring will not show up in a separately launched Dagster UI; use `dagster asset materialize` (or `dg launch`) against the same instance you use for the webserver if you need runs in the UI.
- **CLI / sensors** must see the same `HIFLD_STAGING_BUCKET` (and credentials) as your GCS staging; otherwise catalog will write metadata locally or to the wrong backend.

#### Testing

- Run the full suite: `uv run python -m unittest discover tests`
- Tests cover partition wiring, sensor behavior (with mocks), catalog helpers, explicit asset keys (e.g. NOAA distinct layers), and resource path behavior. When changing `DATASET_FILE_PAIRS` or discovery logic, extend `tests/test_dynamic_partitions.py` or add focused tests rather than relying only on manual GCS checks.

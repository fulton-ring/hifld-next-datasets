# GeoParquet maintenance inventory and staging restore

`hifld-geoparquet-maintenance` inventories published dataset versions and can
restore the selected processing source to durable staging. Both workflows read
storage configuration from `HIFLD_DATASETS_BUCKET`/`HIFLD_DATASETS_DIR` and
`HIFLD_STAGING_BUCKET`/`HIFLD_STAGING_DIR`.

Inventory production before restoring anything:

```bash
uv run hifld-geoparquet-maintenance inventory
uv run hifld-geoparquet-maintenance inventory --dataset DATASET --file FILE --version VERSION
```

Restore is a dry run unless `--apply` is supplied:

```bash
uv run hifld-geoparquet-maintenance restore-staging --dataset DATASET
uv run hifld-geoparquet-maintenance restore-staging --dataset DATASET --apply
```

The command selects one source using `geopackage`, `file_geodatabase`,
`shapefile`, then `geojson` precedence. It never restores GeoParquet, PMTiles,
or legacy `unknown/` paths. A valid legacy Shapefile is instead copied to its
canonical `shapefile/` destination. Blocking or failed versions make the command
exit nonzero.

Existing identical objects are reused. A conflicting canonical staging source
or managed metadata/provenance object blocks restoration. After reviewing the
dry-run report, use `--overwrite-existing-sources` to replace only the
selected versions' conflicting managed source, provenance, and manifest objects.
The command never deletes production objects.

Before canonical staging changes, the command builds and catalogs the complete
restoration under `_temporary/`. A published version-level source manifest is
preserved there as `metadata/upstream_source_manifest.json`; the durable
`metadata/source_manifest.json` is always regenerated from the current
dataset/file manifests and that current version override, with inventory as the
fallback. Final promotion backs up affected staging objects and restores them if
any copy fails.

The restore command refuses to start when published and staging resolve to the
same or nested local directory, or to the same GCS bucket with equal or nested
prefixes. Same-bucket sibling prefixes are allowed. Inventory records object
generation, size, and available checksums; apply uses those snapshots as GCS
generation preconditions and local identity checks so a concurrent source or
destination change fails instead of being overwritten.

If final promotion fails, the command restores preexisting objects from its
operation backup. If that rollback is incomplete, it removes only the candidate,
retains the backup, reports its stable `<operation>/backup` location, and exits
nonzero for operator recovery.

## Canonical GeoParquet audit

`audit-geoparquet` is read-only. It scans only canonical
`{dataset}/{file}/{version}/geoparquet/**.parquet` objects and the corresponding
`metadata/geoparquet_layout.json`; `_temporary/` and `_rollback/` are ignored.
Selectors are optional, but an explicit selector that matches no version blocks.
The report checks the manifest's declared object set, hashes, sizes, feature/file/
row-group counts, footer schemas and CRS, row-group limits, and the S2 layout
rule. A non-compliant report exits nonzero.

GeoParquet files target approximately 2 GiB compressed physical size. The audit
allows multiple numbered sibling files for semantic layouts (`admin`,
`derived_huc`, and `derived_prefix`) regardless of their aggregate compressed
size or file count. An unpartitioned `single_file` layer whose compressed
snapshot size exceeds 2 GiB must use the S2 fallback; legacy `_s2` strategies
must declare the S2 Hive key and include it in every declared output path.

Each row group targets 128 MiB and remains bounded to 160 MiB uncompressed.
Every output manifest
entry records its exact serialized Parquet footer, and the combined footer
metadata for a dataset version must not exceed 128 MiB. Semantic partitioning
takes precedence over the S2 fallback: S2 is a fallback for a large
unpartitioned layer, not a requirement caused by dataset-wide uncompressed or
semantic size.

```bash
uv run hifld-geoparquet-maintenance audit-geoparquet
uv run hifld-geoparquet-maintenance audit-geoparquet --dataset DATASET --file FILE --version VERSION
```

## Repack replacement

A repack producer writes a complete candidate under the staging namespace:
`_temporary/repack/RUN_ID/{dataset}/{file}/{version}/`. The candidate must include
the canonical Parquet objects and layout manifest. Validate it first with the
dry-run command:

```bash
uv run hifld-geoparquet-maintenance replace-geoparquet \
  --dataset DATASET --file FILE --version VERSION --run-id RUN_ID
```

After reviewing the report, `--apply` backs up only the current canonical
GeoParquet objects and layout manifest under
`_rollback/geoparquet/RUN_ID/{dataset}/{file}/{version}/`, conditionally removes
the old canonical set, and promotes the validated candidate. Source formats,
PMTiles, quality manifests, and data dictionaries are never touched. A deliberate
deletion gap exists between the conditional delete and promotion; the report
identifies it. On failure, only generations created by this run are removed and
the backup is restored with CAS. If restoration fails, the backup is retained and
reported for operator recovery. Reusing a run ID with conflicting backup content
fails closed. No discovery or Kubernetes command is invoked automatically.

Suspend production discovery before applying a replacement, then resume and run
scoped discovery afterward:

```bash
kubectl -n hifld-prod scale deployment dataset-discovery-hifld-prod --replicas=0
uv run hifld-geoparquet-maintenance replace-geoparquet --dataset DATASET --file FILE --version VERSION --run-id RUN_ID --apply
kubectl -n hifld-prod scale deployment dataset-discovery-hifld-prod --replicas=1
kubectl -n hifld-prod create job --from=cronjob/dataset-discovery-hifld-prod dataset-discovery-DATASET-RUN_ID --env=DISCOVER_PREFIX=DATASET
```

If promotion reports retained backup, stop discovery, inspect the listed backup
keys, restore the canonical set with conditional copies, and resume discovery.

## Tile benchmark

Run `benchmark-tiles` after scoped discovery, with a live query ID and token. The
token argument is an environment-variable name and the token value is never
printed. Each tile is requested repeatedly from the MVT endpoint using the
`X-HIFLD-Query-Token` header. HTTP 200 MVT responses and empty 204 responses are
accepted; the default median target is at most 8 seconds and every request must
finish below 10 seconds.

```bash
export HIFLD_QUERY_TOKEN='...'
uv run hifld-geoparquet-maintenance benchmark-tiles \
  --base-url https://api.example --query-id QUERY_ID --token-env HIFLD_QUERY_TOKEN \
  --tile 0/0/0 --tile 1/1/1 --repetitions 3
```

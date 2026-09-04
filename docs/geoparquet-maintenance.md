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

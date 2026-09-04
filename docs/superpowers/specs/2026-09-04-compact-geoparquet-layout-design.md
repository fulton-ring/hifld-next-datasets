# Compact GeoParquet Layout Design

## Goal

Publish GeoParquet that remains efficient for DuckDB analytics and selective web-map
queries without creating one object per row group.

## Layout policy

- Treat 2 GiB as the target compressed physical size of each Parquet object.
- Treat 128 MiB as the maximum uncompressed Parquet row-group size.
- Keep multiple spatially ordered row groups in each object until the file target is
  reached; flushing an in-memory row-group buffer must not close the file.
- Keep datasets smaller than the file target in one file unless they have an explicit
  semantic partitioning policy.
- Prefer configured or discovered semantic/admin partitions. If one semantic partition
  is larger than the file target, roll it into numbered sibling files in that same Hive
  directory.
- Use S2 only when a dataset has no usable semantic partitioning scheme and is large
  enough to require multiple files, or when a dataset explicitly opts into S2.
- Preserve GeoParquet 1.1 bbox covering columns and spatial ordering inside every row
  group.
- Reject a complete dataset version when the combined serialized Parquet footer metadata
  exceeds 128 MiB.

## Validation and manifests

The layout manifest records each output's compressed file size, footer size, row counts,
uncompressed row-group sizes, and checksum. It records the dataset metadata ceiling in
the thresholds block. Publication validates all layers together before writing the
authoritative manifest; an over-budget version is removed by the existing failure cleanup.

The maintenance audit verifies the declared footer sizes and combined metadata ceiling.
It no longer requires S2 for semantically partitioned datasets merely because their total
size is large. It still flags an oversized unpartitioned layout that did not fall back to
S2.

## Deployment and live validation

Build and push a new immutable Dagster image, upgrade the existing Helm release, then
materialize the restored Census Block Groups asset as a quick semantic-partition canary.
After it passes, materialize NFHL. Inspect the resulting object count, file sizes, row-group
sizes, footer total, spatial metadata, and feature count with DuckDB/PyArrow, and benchmark
representative selective and aggregate DuckDB queries.

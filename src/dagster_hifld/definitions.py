"""Dagster definitions: assets, asset checks, resources, and sensors."""

from __future__ import annotations

import os
from pathlib import Path

from dagster import (
    AssetKey,
    Definitions,
    RunRequest,
    SensorResult,
    SkipReason,
    sensor,
)

from dagster_hifld.assets import hifld_dataset_assets
from dagster_hifld.checks import hifld_asset_checks
from dagster_hifld.partitions import PARTITIONS_BY_PAIR, get_partition_name
from dagster_hifld.resources import (
    PublishedStorageResource,
    StagingStorageResource,
)

# "unknown" catches legacy layouts where shapefile components were staged outside
# the canonical shapefile/ prefix (still geospatial source data).
_SOURCE_FORMAT_DIRS = {
    "shapefile",
    "file_geodatabase",
    "geojson",
    "geopackage",
    "unknown",
}
MAX_SENSOR_RUN_REQUESTS_PER_TICK = int(
    os.environ.get("HIFLD_SENSOR_MAX_RUN_REQUESTS_PER_TICK", "25")
)


def _iter_staged_version_paths(storage: StagingStorageResource):
    prefix = storage.prefix.strip("/")
    if storage.use_local or not storage.bucket:
        root = Path(storage.local_dir).resolve()
        if prefix:
            root = root / prefix
        if not root.exists():
            return
        for dataset_dir in root.iterdir():
            if not dataset_dir.is_dir():
                continue
            for file_dir in dataset_dir.iterdir():
                if not file_dir.is_dir():
                    continue
                for version_dir in file_dir.iterdir():
                    if not version_dir.is_dir():
                        continue
                    format_dirs = {
                        child.name for child in version_dir.iterdir() if child.is_dir()
                    }
                    if format_dirs & _SOURCE_FORMAT_DIRS:
                        yield dataset_dir.name, file_dir.name, version_dir.name
        return

    import gcsfs

    fs = gcsfs.GCSFileSystem()
    root = f"{storage.bucket}/{prefix}" if prefix else storage.bucket
    seen: set[tuple[str, str, str]] = set()
    for path in fs.find(root, maxdepth=5):
        rel = path.replace(f"{storage.bucket}/", "", 1).strip("/")
        if prefix and rel.startswith(prefix):
            rel = rel[len(prefix):].strip("/")
        parts = [part for part in rel.split("/") if part]
        if len(parts) < 5:
            continue
        dataset_slug, file_slug, version, format_dir = parts[:4]
        if format_dir not in _SOURCE_FORMAT_DIRS:
            continue
        candidate = (dataset_slug, file_slug, version)
        if candidate in seen:
            continue
        seen.add(candidate)
        yield candidate


@sensor(name="version_discovery_sensor", minimum_interval_seconds=300)
def version_discovery_sensor(context):
    staging_storage = StagingStorageResource.from_env()
    run_requests: list[RunRequest] = []
    for dataset_slug, file_slug, version in _iter_staged_version_paths(staging_storage) or []:
        if len(run_requests) >= MAX_SENSOR_RUN_REQUESTS_PER_TICK:
            break
        if (dataset_slug, file_slug) not in PARTITIONS_BY_PAIR:
            continue
        quality_key = staging_storage.build_target_location(
            dataset_slug,
            file_slug,
            version,
            "metadata/quality_manifest.json",
        )
        if staging_storage.object_exists(quality_key):
            continue
        partition_name = get_partition_name(dataset_slug, file_slug)
        if not context.instance.has_dynamic_partition(partition_name, version):
            context.instance.add_dynamic_partitions(partition_name, [version])
        run_requests.append(
            RunRequest(
                run_key=f"{dataset_slug}/{file_slug}/{version}",
                asset_selection=[AssetKey(["catalog", dataset_slug, file_slug])],
                partition_key=version,
                tags={
                    "dataset_slug": dataset_slug,
                    "file_slug": file_slug,
                    "version": version,
                },
            )
        )

    if not run_requests:
        return SkipReason("No staged versions are missing catalog metadata.")
    return SensorResult(run_requests=run_requests)


defs = Definitions(
    assets=hifld_dataset_assets,
    asset_checks=hifld_asset_checks,
    sensors=[version_discovery_sensor],
    resources={
        "staging_storage": StagingStorageResource.from_env(),
        "published_storage": PublishedStorageResource.from_env(),
    },
)

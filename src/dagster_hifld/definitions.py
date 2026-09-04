"""Dagster definitions: assets, asset checks, resources, and sensors."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dagster import (
    Definitions,
    SensorResult,
    sensor,
)

from dagster_hifld.assets import hifld_dataset_assets
from dagster_hifld.checks import hifld_asset_checks
from dagster_hifld.partitions import (
    PUBLISH_PARTITION_NAME,
    PUBLISH_PARTITIONS,
    SEMVER_VERSION_RE,
    build_publish_partition_key,
)
from dagster_hifld.resources import (
    PublishedStorageResource,
    StagingStorageResource,
)

# Staged source discovery intentionally excludes derived output folders. Shapefiles
# are generated as zipped derived outputs, not copied as staging inputs.
_SOURCE_FORMAT_DIRS = {
    "file_geodatabase",
    "geoparquet",
    "geojson",
    "geopackage",
    "unknown",
}
MAX_SENSOR_RUN_REQUESTS_PER_TICK = int(os.environ.get("HIFLD_SENSOR_MAX_RUN_REQUESTS_PER_TICK", "25"))
MAX_SENSOR_DATASETS_PER_TICK = int(os.environ.get("HIFLD_SENSOR_MAX_DATASETS_PER_TICK", "100"))
MAX_SENSOR_GCS_LIST_WORKERS = int(os.environ.get("HIFLD_SENSOR_GCS_LIST_WORKERS", "32"))


def _default_executor():
    if not os.environ.get("KUBERNETES_SERVICE_HOST"):
        return None

    from dagster_k8s import k8s_job_executor

    return k8s_job_executor.configured(
        {
            "max_concurrent": 1,
            "step_k8s_config": {
                "container_config": {
                    "volume_mounts": [
                        {
                            "name": "dagster-step-scratch",
                            "mount_path": "/tmp",
                        }
                    ],
                    "resources": {
                        "requests": {
                            "cpu": "4",
                            "memory": "48Gi",
                            "ephemeral-storage": "10Gi",
                        },
                        "limits": {
                            "cpu": "8",
                            "memory": "64Gi",
                            "ephemeral-storage": "10Gi",
                        },
                    },
                },
                "pod_spec_config": {
                    "volumes": [
                        {
                            "name": "dagster-step-scratch",
                            "ephemeral": {
                                "volumeClaimTemplate": {
                                    "spec": {
                                        "accessModes": ["ReadWriteOnce"],
                                        "storageClassName": "dynamic-rwo",
                                        "resources": {
                                            "requests": {
                                                "storage": "100Gi",
                                            }
                                        },
                                    }
                                }
                            },
                        }
                    ],
                },
                "pod_template_spec_metadata": {
                    "annotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
                    },
                },
                "job_spec_config": {
                    "ttl_seconds_after_finished": 120,
                },
            },
        }
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
                    if not version_dir.is_dir() or not SEMVER_VERSION_RE.match(version_dir.name):
                        continue
                    format_dirs = {child.name for child in version_dir.iterdir() if child.is_dir()}
                    if format_dirs & _SOURCE_FORMAT_DIRS:
                        yield dataset_dir.name, file_dir.name, version_dir.name
        return

    seen: set[tuple[str, str, str]] = set()
    for rel in _iter_gcs_staged_source_object_names(storage.bucket, prefix):
        parts = [part for part in rel.split("/") if part]
        if len(parts) < 5:
            continue
        dataset_slug, file_slug, version, format_dir = parts[:4]
        if not SEMVER_VERSION_RE.match(version):
            continue
        if format_dir not in _SOURCE_FORMAT_DIRS:
            continue
        candidate = (dataset_slug, file_slug, version)
        if candidate in seen:
            continue
        seen.add(candidate)
        yield candidate


def _iter_gcs_staged_source_object_names(bucket: str, prefix: str):
    """Yield source object names using server-side glob filters when available."""
    try:
        from google.cloud import storage as gcs_storage

        client = gcs_storage.Client()
        prefix_arg = f"{prefix.rstrip('/')}/" if prefix else None
        for format_dir in sorted(_SOURCE_FORMAT_DIRS):
            glob = f"{prefix_arg or ''}*/*/v*.*.*/{format_dir}/**"
            for blob in client.list_blobs(
                bucket,
                prefix=prefix_arg,
                match_glob=glob,
            ):
                name = blob.name
                if prefix_arg and name.startswith(prefix_arg):
                    name = name[len(prefix_arg) :]
                yield name.strip("/")
        return
    except TypeError:
        pass
    except Exception:
        return

    import gcsfs

    fs = gcsfs.GCSFileSystem()
    root = f"{bucket}/{prefix}" if prefix else bucket
    for path in fs.find(root, maxdepth=5):
        rel = path.replace(f"{bucket}/", "", 1).strip("/")
        if prefix and rel.startswith(prefix):
            rel = rel[len(prefix) :].strip("/")
        yield rel


def _gcs_dataset_slugs(bucket: str, prefix: str) -> list[str]:
    from google.cloud import storage as gcs_storage

    client = gcs_storage.Client()
    prefix_arg = f"{prefix.rstrip('/')}/" if prefix else ""
    iterator = client.list_blobs(bucket, prefix=prefix_arg, delimiter="/")
    slugs: set[str] = set()
    for page in iterator.pages:
        for child_prefix in page.prefixes:
            rel = child_prefix
            if prefix_arg and rel.startswith(prefix_arg):
                rel = rel[len(prefix_arg) :]
            slug = rel.strip("/").split("/", 1)[0]
            if slug:
                slugs.add(slug)
    return sorted(slugs)


def _iter_gcs_staged_source_object_names_for_datasets(
    bucket: str,
    prefix: str,
    dataset_slugs: list[str],
):
    from google.cloud import storage as gcs_storage

    if not dataset_slugs:
        return
    client = gcs_storage.Client()
    worker_count = max(1, min(MAX_SENSOR_GCS_LIST_WORKERS, len(dataset_slugs)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        for object_names in executor.map(
            lambda dataset_slug: list(
                _iter_gcs_staged_source_object_names_for_dataset(
                    client,
                    bucket,
                    prefix,
                    dataset_slug,
                )
            ),
            dataset_slugs,
        ):
            yield from object_names


def _iter_gcs_staged_source_object_names_for_dataset(
    client,
    bucket: str,
    prefix: str,
    dataset_slug: str,
):
    prefix_arg = f"{prefix.rstrip('/')}/" if prefix else ""
    dataset_prefix = f"{prefix_arg}{dataset_slug}/"
    for format_dir in sorted(_SOURCE_FORMAT_DIRS):
        glob = f"{dataset_prefix}*/v*.*.*/{format_dir}/**"
        for blob in client.list_blobs(
            bucket,
            prefix=dataset_prefix,
            match_glob=glob,
        ):
            name = blob.name
            if prefix_arg and name.startswith(prefix_arg):
                name = name[len(prefix_arg) :]
            yield name.strip("/")


def _iter_staged_version_paths_for_gcs_tick(
    storage: StagingStorageResource,
    cursor: str | None,
) -> tuple[list[tuple[str, str, str]], str]:
    prefix = storage.prefix.strip("/")
    dataset_slugs = _gcs_dataset_slugs(storage.bucket or "", prefix)
    if not dataset_slugs:
        return [], "0"
    start = int(cursor or "0")
    if start >= len(dataset_slugs):
        start = 0
    end = min(start + MAX_SENSOR_DATASETS_PER_TICK, len(dataset_slugs))
    next_cursor = "0" if end >= len(dataset_slugs) else str(end)
    selected_slugs = dataset_slugs[start:end]

    seen: set[tuple[str, str, str]] = set()
    versions: list[tuple[str, str, str]] = []
    for rel in _iter_gcs_staged_source_object_names_for_datasets(
        storage.bucket or "",
        prefix,
        selected_slugs,
    ):
        parts = [part for part in rel.split("/") if part]
        if len(parts) < 5:
            continue
        dataset_slug, file_slug, version, format_dir = parts[:4]
        if not SEMVER_VERSION_RE.match(version) or format_dir not in _SOURCE_FORMAT_DIRS:
            continue
        candidate = (dataset_slug, file_slug, version)
        if candidate in seen:
            continue
        seen.add(candidate)
        versions.append(candidate)
    return versions, next_cursor


@sensor(name="version_discovery_sensor", minimum_interval_seconds=300)
def version_discovery_sensor(context):
    staging_storage = StagingStorageResource.from_env()
    if not staging_storage.use_local and staging_storage.bucket:
        staged_versions, next_cursor = _iter_staged_version_paths_for_gcs_tick(
            staging_storage,
            context.cursor,
        )
    else:
        staged_versions = list(_iter_staged_version_paths(staging_storage) or [])
        next_cursor = context.cursor

    existing_partition_keys = set(context.instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME))
    partition_keys_to_add: list[str] = []
    for dataset_slug, file_slug, version in staged_versions:
        partition_key = build_publish_partition_key(dataset_slug, file_slug, version)
        if partition_key not in existing_partition_keys:
            partition_keys_to_add.append(partition_key)

    dynamic_partitions_requests = (
        [PUBLISH_PARTITIONS.build_add_request(partition_keys_to_add)] if partition_keys_to_add else []
    )
    return SensorResult(
        skip_reason=f"Discovered {len(partition_keys_to_add)} new publish partitions.",
        dynamic_partitions_requests=dynamic_partitions_requests,
        cursor=next_cursor,
    )


defs = Definitions(
    assets=hifld_dataset_assets,
    asset_checks=hifld_asset_checks,
    sensors=[version_discovery_sensor],
    executor=_default_executor(),
    resources={
        "staging_storage": StagingStorageResource.from_env(),
        "published_storage": PublishedStorageResource.from_env(),
    },
)

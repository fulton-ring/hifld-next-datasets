"""Dagster definitions: assets, asset checks, resources, and sensors."""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterable

from dagster import (
    Definitions,
    SensorResult,
    sensor,
)

from dagster_hifld.assets import hifld_dataset_assets
from dagster_hifld.assets.quaternary_fault_lines import (
    quaternary_fault_lines_draft_job,
    quaternary_fault_lines_draft_schedule,
    quaternary_fault_lines_population_complete,
)
from dagster_hifld.assets.faa import (
    faa_airspace_draft_refresh_job,
    faa_airspace_draft_refresh_schedule,
)
from dagster_hifld.assets.noaa_contiguous import (
    noaa_contiguous_draft_refresh_job,
    noaa_contiguous_draft_refresh_schedule,
)
from dagster_hifld.assets.noaa import (
    noaa_eez_draft_refresh_job,
    noaa_eez_draft_weekly_schedule,
)
from dagster_hifld.assets.usace_pylon_areas import (
    usace_pylon_areas_draft_schedule,
    usace_pylon_areas_complete,
    usace_pylon_areas_draft_job,
)
from dagster_hifld.assets.principal_ports import (
    principal_ports_source_complete,
    principal_ports_draft_schedule,
)
from dagster_hifld.assets.bsee_ocs_platforms import (
    bsee_ocs_platforms_complete,
    bsee_ocs_platforms_draft_job,
    bsee_ocs_platforms_draft_schedule,
)
from dagster_hifld.assets.usace_berths_areas import (
    usace_berths_areas_complete,
    usace_berths_areas_draft_schedule,
)
from dagster_hifld.assets.usace_overhead_pipeline_lines import (
    usace_overhead_pipeline_lines_complete,
    usace_overhead_pipeline_lines_draft_job,
    usace_overhead_pipeline_lines_draft_schedule,
)
from dagster_hifld.assets.usace_pylon_locations import (
    pylon_locations_draft_refresh_job,
    pylon_locations_draft_refresh_schedule,
)
from dagster_hifld.assets.usace_recommended_tracks import (
    recommended_tracks_draft_job,
    recommended_tracks_draft_schedule,
    usace_recommended_tracks_complete,
)
from dagster_hifld.assets.fuel_stations import (
    fuel_stations_draft_refresh_job,
    fuel_stations_draft_refresh_schedule,
)
from dagster_hifld.assets.secondary_school_districts import (
    secondary_school_districts_draft_refresh_job,
    secondary_school_districts_draft_refresh_schedule,
)
from dagster_hifld.assets.unified_school_districts import (
    unified_school_districts_draft_refresh_job,
    unified_school_districts_draft_refresh_schedule,
)
from dagster_hifld.assets.census_primary_roads import (
    census_primary_roads_source_complete,
    census_primary_roads_draft_schedule,
)
from dagster_hifld.checks import hifld_asset_checks
from dagster_hifld.native_geoparquet_catalog import discover_prebuilt_native
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
from dagster_hifld.source_formats import (
    CANONICAL_SOURCE_FORMAT_DIRS,
    discover_legacy_unknown_shapefile,
    discover_legacy_unknown_shapefile_keys,
)
_SOURCE_FORMAT_DIRS = CANONICAL_SOURCE_FORMAT_DIRS
_STAGED_DISCOVERY_FORMAT_DIRS = CANONICAL_SOURCE_FORMAT_DIRS | {"unknown", "geoparquet"}
logger = logging.getLogger(__name__)
MAX_SENSOR_RUN_REQUESTS_PER_TICK = int(
    os.environ.get("HIFLD_SENSOR_MAX_RUN_REQUESTS_PER_TICK", "25")
)
MAX_SENSOR_DATASETS_PER_TICK = int(
    os.environ.get("HIFLD_SENSOR_MAX_DATASETS_PER_TICK", "100")
)
MAX_SENSOR_GCS_LIST_WORKERS = int(
    os.environ.get("HIFLD_SENSOR_GCS_LIST_WORKERS", "32")
)


def _step_resources() -> dict[str, dict[str, str]]:
    resources = {
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
    }
    if os.environ.get("HIFLD_SHADOW_MANUAL_ONLY") != "1":
        return resources

    env_names = {
        "requests.cpu": "HIFLD_SHADOW_STEP_CPU_REQUEST",
        "requests.memory": "HIFLD_SHADOW_STEP_MEMORY_REQUEST",
        "requests.ephemeral-storage": "HIFLD_SHADOW_STEP_EPHEMERAL_STORAGE_REQUEST",
        "limits.cpu": "HIFLD_SHADOW_STEP_CPU_LIMIT",
        "limits.memory": "HIFLD_SHADOW_STEP_MEMORY_LIMIT",
        "limits.ephemeral-storage": "HIFLD_SHADOW_STEP_EPHEMERAL_STORAGE_LIMIT",
    }
    for path, env_name in env_names.items():
        value = os.environ.get(env_name)
        if value:
            scope, key = path.split(".", 1)
            resources[scope][key] = value
    return resources


def _step_scratch_storage() -> str:
    if os.environ.get("HIFLD_SHADOW_MANUAL_ONLY") == "1":
        value = os.environ.get("HIFLD_SHADOW_STEP_SCRATCH_STORAGE")
        if value:
            return value
    # Production default remains the original inline value: "storage": "250Gi".
    return "250Gi"


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
                    "resources": _step_resources(),
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
                                                "storage": _step_scratch_storage(),
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
                    if (
                        not version_dir.is_dir()
                        or not SEMVER_VERSION_RE.match(version_dir.name)
                    ):
                        continue
                    format_dirs = {
                        child.name for child in version_dir.iterdir() if child.is_dir()
                    }
                    if format_dirs & _SOURCE_FORMAT_DIRS:
                        yield dataset_dir.name, file_dir.name, version_dir.name
                        continue
                    if "unknown" in format_dirs:
                        try:
                            legacy_source = discover_legacy_unknown_shapefile(
                                version_dir / "unknown"
                            )
                        except ValueError as exc:
                            logger.warning(
                                "Skipping invalid legacy source %s: %s",
                                version_dir / "unknown",
                                exc,
                            )
                            continue
                        if legacy_source is not None:
                            yield dataset_dir.name, file_dir.name, version_dir.name
                        continue
                    if "geoparquet" in format_dirs:
                        candidate = (dataset_dir.name, file_dir.name, version_dir.name)
                        try:
                            if discover_prebuilt_native(storage, "/".join(candidate)):
                                yield candidate
                        except (ValueError, OSError, TypeError, RuntimeError) as exc:
                            logger.warning("Skipping invalid proposed native GeoParquet %s: %s", version_dir, exc)
        return

    yield from _iter_validated_gcs_staged_versions(
        _iter_gcs_staged_source_object_names(storage.bucket, prefix), storage=storage
    )


def _iter_validated_gcs_staged_versions(
    object_names: Iterable[str],
    *, storage: StagingStorageResource | None = None,
):
    keys_by_version: dict[tuple[str, str, str], list[str]] = {}
    formats_by_version: dict[tuple[str, str, str], set[str]] = {}
    for rel in object_names:
        parts = [part for part in rel.split("/") if part]
        if len(parts) < 5:
            continue
        dataset_slug, file_slug, version, format_dir = parts[:4]
        if not SEMVER_VERSION_RE.match(version):
            continue
        if format_dir not in _STAGED_DISCOVERY_FORMAT_DIRS:
            continue
        candidate = (dataset_slug, file_slug, version)
        keys_by_version.setdefault(candidate, []).append(rel)
        formats_by_version.setdefault(candidate, set()).add(format_dir)

    for candidate, formats in formats_by_version.items():
        if formats & _SOURCE_FORMAT_DIRS:
            yield candidate
            continue
        try:
            legacy_keys = discover_legacy_unknown_shapefile_keys(
                key for key in keys_by_version[candidate] if "/unknown/" in key
            )
        except ValueError as exc:
            logger.warning(
                "Skipping invalid legacy source %s/%s/%s/unknown: %s",
                *candidate,
                exc,
            )
            continue
        if legacy_keys:
            yield candidate
            continue
        if "unknown" not in formats and "geoparquet" in formats and storage is not None:
            try:
                if discover_prebuilt_native(storage, "/".join(candidate)):
                    yield candidate
            except (ValueError, OSError, TypeError, RuntimeError) as exc:
                logger.warning("Skipping invalid proposed native GeoParquet %s: %s", candidate, exc)


def _iter_gcs_staged_source_object_names(bucket: str, prefix: str):
    """Yield source object names using server-side glob filters when available."""
    try:
        from google.cloud import storage as gcs_storage

        client = gcs_storage.Client()
        prefix_arg = f"{prefix.rstrip('/')}/" if prefix else None
        for format_dir in sorted(_STAGED_DISCOVERY_FORMAT_DIRS):
            glob = f"{prefix_arg or ''}*/*/v*.*.*/{format_dir}/**"
            for blob in client.list_blobs(
                bucket,
                prefix=prefix_arg,
                match_glob=glob,
            ):
                name = blob.name
                if prefix_arg and name.startswith(prefix_arg):
                    name = name[len(prefix_arg):]
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
            rel = rel[len(prefix):].strip("/")
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
                rel = rel[len(prefix_arg):]
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
    for format_dir in sorted(_STAGED_DISCOVERY_FORMAT_DIRS):
        glob = f"{dataset_prefix}*/v*.*.*/{format_dir}/**"
        for blob in client.list_blobs(
            bucket,
            prefix=dataset_prefix,
            match_glob=glob,
        ):
            name = blob.name
            if prefix_arg and name.startswith(prefix_arg):
                name = name[len(prefix_arg):]
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

    versions = list(
        _iter_validated_gcs_staged_versions(
            _iter_gcs_staged_source_object_names_for_datasets(
                storage.bucket or "",
                prefix,
                selected_slugs,
            ), storage=storage,
        )
    )
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

    existing_partition_keys = set(
        context.instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME)
    )
    partition_keys_to_add: list[str] = []
    for dataset_slug, file_slug, version in staged_versions:
        partition_key = build_publish_partition_key(dataset_slug, file_slug, version)
        if partition_key not in existing_partition_keys:
            partition_keys_to_add.append(partition_key)

    dynamic_partitions_requests = (
        [PUBLISH_PARTITIONS.build_add_request(partition_keys_to_add)]
        if partition_keys_to_add
        else []
    )
    return SensorResult(
        skip_reason=f"Discovered {len(partition_keys_to_add)} new publish partitions.",
        dynamic_partitions_requests=dynamic_partitions_requests,
        cursor=next_cursor,
    )


def _configured_sensors():
    if os.environ.get("HIFLD_SHADOW_MANUAL_ONLY") == "1":
        return []
    return [version_discovery_sensor]


defs = Definitions(
    assets=hifld_dataset_assets,
    asset_checks=[
        *hifld_asset_checks,
        census_primary_roads_source_complete,
        quaternary_fault_lines_population_complete,
        usace_pylon_areas_complete,
        usace_recommended_tracks_complete,
        usace_overhead_pipeline_lines_complete,
        principal_ports_source_complete,
        bsee_ocs_platforms_complete,
        usace_berths_areas_complete,
    ],
    jobs=[
        quaternary_fault_lines_draft_job,
        faa_airspace_draft_refresh_job,
        noaa_eez_draft_refresh_job,
        noaa_contiguous_draft_refresh_job,
        usace_pylon_areas_draft_job,
        pylon_locations_draft_refresh_job,
        recommended_tracks_draft_job,
        fuel_stations_draft_refresh_job,
        usace_overhead_pipeline_lines_draft_job,
        secondary_school_districts_draft_refresh_job,
        unified_school_districts_draft_refresh_job,
        bsee_ocs_platforms_draft_job,
    ],
    schedules=[
        quaternary_fault_lines_draft_schedule,
        census_primary_roads_draft_schedule,
        usace_pylon_areas_draft_schedule,
        faa_airspace_draft_refresh_schedule,
        noaa_eez_draft_weekly_schedule,
        noaa_contiguous_draft_refresh_schedule,
        pylon_locations_draft_refresh_schedule,
        recommended_tracks_draft_schedule,
        fuel_stations_draft_refresh_schedule,
        usace_overhead_pipeline_lines_draft_schedule,
        secondary_school_districts_draft_refresh_schedule,
        principal_ports_draft_schedule,
        usace_berths_areas_draft_schedule,
        unified_school_districts_draft_refresh_schedule,
        bsee_ocs_platforms_draft_schedule,
    ],
    sensors=_configured_sensors(),
    executor=_default_executor(),
    resources={
        "staging_storage": StagingStorageResource.from_env(),
        "published_storage": PublishedStorageResource.from_env(),
    },
)

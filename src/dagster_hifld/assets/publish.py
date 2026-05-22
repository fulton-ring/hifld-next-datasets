"""Publish assets for staged dataset versions."""

from __future__ import annotations

import asyncio
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import fiona
import geopandas as gpd
from dagster import AssetKey, Output, asset
from shapely.geometry import shape

from dagster_hifld.catalog import (
    summarize_staged_catalog,
    _with_large_geojson_support,
    write_catalog_metadata,
)
from dagster_hifld.conversion import (
    GeoParquetWritePolicy,
    ShapefileZipPolicy,
    _build_layer_filename,
    _discover_staged_formats,
    _StorageAdapter,
    geoparquet_policy_for,
    process_layer_partitioned_geoparquet,
    process_layer_chunked,
    select_processing_input,
    write_geopackage_chunked,
    write_shapefile_zip,
)
from dagster_hifld.partitions import PUBLISH_PARTITIONS, parse_publish_partition_key
from dagster_hifld.resources import (
    DatasetApiResource,
    PublishedStorageResource,
    StagingStorageResource,
)
from dagster_hifld.source_manifest import load_resolved_source_manifest

_PROMOTED_SOURCE_FORMAT_DIRS = {
    "geojson",
    "geopackage",
    "file_geodatabase",
    "unknown",
}
_PROCESSING_SOURCE_FORMAT_DIRS = (
    "file_geodatabase",
    "geopackage",
    "geojson",
    "unknown",
)


@dataclass(frozen=True)
class PublishedFormatOutput:
    file_slug: str
    format_type: str
    path: str
    source_metadata: dict[str, Any] | None = None


def _copy_metadata_files(
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> list[str]:
    copied: list[str] = []
    for filename in ("quality_manifest.json", "data_dictionary.json"):
        rel_path = f"metadata/{filename}"
        contents = staging_storage.read_bytes(dataset_slug, file_slug, version, rel_path)
        copied.append(
            published_storage.write(dataset_slug, file_slug, version, rel_path, contents)
        )
    return copied


def _copy_version_files(
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> list[str]:
    published_storage.delete_prefix(f"{dataset_slug}/{file_slug}/{version}")
    keys = staging_storage.list_keys(dataset_slug, file_slug, version)
    return sorted(staging_storage.copy_keys_to(published_storage, keys))


def _copy_source_format_files(
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    keys: list[str],
) -> list[str]:
    copied: list[str] = []
    version_prefix = f"{dataset_slug}/{file_slug}/{version}/"
    for key in keys:
        if "/metadata/" in key:
            continue
        rel_path = key.removeprefix(version_prefix)
        if not rel_path:
            continue
        format_dir = Path(rel_path).parts[0]
        if format_dir not in _PROMOTED_SOURCE_FORMAT_DIRS:
            continue
        contents = staging_storage.read_bytes(dataset_slug, file_slug, version, key)
        copied.append(
            published_storage.write(dataset_slug, file_slug, version, rel_path, contents)
        )
    return copied


def _copy_source_files(
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> list[PublishedFormatOutput]:
    keys = staging_storage.list_keys(dataset_slug, file_slug, version)
    copied = _copy_source_format_files(
        staging_storage,
        published_storage,
        dataset_slug,
        file_slug,
        version,
        keys,
    )
    version_prefix = f"{dataset_slug}/{file_slug}/{version}/"
    return [
        PublishedFormatOutput(
            file_slug=file_slug,
            format_type=Path(key.removeprefix(version_prefix)).parts[0],
            path=key,
        )
        for key in copied
    ]


def _read_layer(path: Path, format_type: str, layer_name: str | None) -> gpd.GeoDataFrame:
    with _with_large_geojson_support():
        if format_type in {"geopackage", "file_geodatabase"} and layer_name:
            return gpd.read_file(path, layer=layer_name)
        return gpd.read_file(path)


def _fiona_open_kwargs(format_type: str, layer_name: str | None) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if format_type == "file_geodatabase":
        kwargs["driver"] = "OpenFileGDB"
    if format_type in {"geopackage", "file_geodatabase"} and layer_name:
        kwargs["layer"] = layer_name
    return kwargs


def _is_spatial_source_layer(
    path: Path,
    format_type: str,
    layer_name: str | None,
    *,
    sample_limit: int = 1_000,
) -> bool:
    try:
        with _with_large_geojson_support(), fiona.open(
            str(path), **_fiona_open_kwargs(format_type, layer_name)
        ) as src:
            schema = src.schema or {}
            geometry_type = str(schema.get("geometry") or "").lower()
            if geometry_type in {"", "none"}:
                return False
            for idx, feature in enumerate(src):
                geom = feature.get("geometry")
                if geom is not None:
                    try:
                        shapely_geom = shape(geom)
                    except Exception:
                        shapely_geom = None
                    if shapely_geom is not None and not shapely_geom.is_empty:
                        return True
                if idx + 1 >= sample_limit:
                    break
    except Exception:
        return False
    return False


def _layer_filename(file_slug: str, layers: list[tuple[str, str | None]], layer_name: str) -> str:
    if len(layers) == 1:
        return file_slug
    return _build_layer_filename(file_slug, layer_name)


def _upload_tree(
    storage: StagingStorageResource | PublishedStorageResource,
    local_root: Path,
    dataset_slug: str,
    file_slug: str,
    version: str,
    rel_root: str,
) -> list[str]:
    adapter = _StorageAdapter(storage)
    uploaded: list[str] = []
    for path in sorted(local_root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(local_root).as_posix()
        remote_path = f"{dataset_slug}/{file_slug}/{version}/{rel_root}/{rel}"
        asyncio.run(adapter.upload_file(path, remote_path))
        uploaded.append(remote_path)
    return uploaded


def _upload_paths(
    storage: StagingStorageResource | PublishedStorageResource,
    paths: list[Path],
    local_root: Path,
    dataset_slug: str,
    file_slug: str,
    version: str,
    rel_root: str,
) -> list[str]:
    adapter = _StorageAdapter(storage)
    uploaded: list[str] = []
    for path in sorted(paths):
        try:
            rel = path.relative_to(local_root).as_posix()
        except ValueError:
            rel = path.name
        remote_path = f"{dataset_slug}/{file_slug}/{version}/{rel_root}/{rel}"
        asyncio.run(adapter.upload_file(path, remote_path))
        uploaded.append(remote_path)
    return uploaded


def _published_format_keys(
    storage: StagingStorageResource | PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    format_dir: str,
) -> list[str]:
    prefix = f"{dataset_slug}/{file_slug}/{version}/{format_dir}/"
    return [
        key
        for key in storage.list_keys(dataset_slug, file_slug, version)
        if key.startswith(prefix)
    ]


def _publish_overwrite_enabled() -> bool:
    return os.environ.get("HIFLD_PUBLISH_OVERWRITE", "true").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _prepare_format_publish(
    storage: StagingStorageResource | PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    format_dir: str,
) -> list[str]:
    existing = _published_format_keys(
        storage,
        dataset_slug,
        file_slug,
        version,
        format_dir,
    )
    if not existing:
        return []
    if not _publish_overwrite_enabled():
        return existing
    storage.delete_prefix(f"{dataset_slug}/{file_slug}/{version}/{format_dir}")
    return []


def _existing_format_outputs(
    dataset_slug: str,
    file_slug: str,
    version: str,
    existing_keys: list[str],
) -> list[PublishedFormatOutput]:
    return _published_outputs_from_keys(dataset_slug, file_slug, version, existing_keys)


def _skip_output(file_slug: str, format_type: str, reason: str) -> PublishedFormatOutput:
    return PublishedFormatOutput(
        file_slug=file_slug,
        format_type=format_type,
        path="",
        source_metadata={"skip_reason": reason},
    )


def _source_tree_size_bytes(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(child.stat().st_size for child in path.rglob("*") if child.is_file())


def _preferred_remote_source_keys(
    storage: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> tuple[str | None, list[str]]:
    version_prefix = f"{dataset_slug}/{file_slug}/{version}/"
    keys = storage.list_keys(dataset_slug, file_slug, version)
    for format_dir in _PROCESSING_SOURCE_FORMAT_DIRS:
        prefix = f"{version_prefix}{format_dir}/"
        format_keys = [key for key in keys if key.startswith(prefix)]
        if format_keys:
            return format_dir, format_keys
    return None, []


def _remote_source_size_bytes(
    storage: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> int | None:
    _format_dir, keys = _preferred_remote_source_keys(storage, dataset_slug, file_slug, version)
    if not keys:
        return None
    return sum(storage.key_size(key) for key in keys)


def _copy_or_generate_geopackage(
    staging_storage: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> list[PublishedFormatOutput]:
    keys = staging_storage.list_keys(dataset_slug, file_slug, version)
    staged_geopackage_keys = [
        key
        for key in keys
        if f"/{version}/geopackage/" in key
    ]
    if staged_geopackage_keys:
        return [
            PublishedFormatOutput(
                file_slug=file_slug,
                format_type="geopackage",
                path=key,
            )
            for key in staged_geopackage_keys
        ]

    existing = _prepare_format_publish(
        staging_storage,
        dataset_slug,
        file_slug,
        version,
        "geopackage",
    )
    if existing:
        return _existing_format_outputs(dataset_slug, file_slug, version, existing)

    outputs: list[PublishedFormatOutput] = []
    with staging_storage.get_local_version_dir(dataset_slug, file_slug, version) as version_dir:
        processed = _discover_staged_formats(Path(version_dir))
        preferred, data_file, format_type = select_processing_input(processed)
        if not preferred or data_file is None or format_type is None:
            return [_skip_output(file_slug, "geopackage", "non_spatial_source")]
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir) / "geopackage"
            out_dir.mkdir()
            for layer_name, _geom_type in preferred["layers"]:
                source_layer_name = layer_name if layer_name != "default" else None
                if not _is_spatial_source_layer(data_file, format_type, source_layer_name):
                    outputs.append(_skip_output(file_slug, "geopackage", "non_spatial_source"))
                    continue
                layer_file = _layer_filename(file_slug, preferred["layers"], layer_name)
                output_gpkg = out_dir / f"{layer_file}.gpkg"
                asyncio.run(
                    write_geopackage_chunked(
                        file_path=data_file,
                        format_type=format_type,
                        layer_name=source_layer_name,
                        output_gpkg=output_gpkg,
                    )
                )
            for remote_path in _upload_tree(
                staging_storage,
                out_dir,
                dataset_slug,
                file_slug,
                version,
                "geopackage",
            ):
                outputs.append(PublishedFormatOutput(file_slug, "geopackage", remote_path))
    return outputs


def _write_and_publish_geoparquet(
    staging_storage: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    policy: GeoParquetWritePolicy | None = None,
) -> list[PublishedFormatOutput]:
    existing = _prepare_format_publish(staging_storage, dataset_slug, file_slug, version, "geoparquet")
    if existing:
        return _existing_format_outputs(dataset_slug, file_slug, version, existing)
    outputs: list[PublishedFormatOutput] = []
    with staging_storage.get_local_version_dir(dataset_slug, file_slug, version) as version_dir:
        processed = _discover_staged_formats(Path(version_dir))
        preferred, data_file, format_type = select_processing_input(processed)
        if not preferred or data_file is None or format_type is None:
            return [_skip_output(file_slug, "geoparquet", "non_spatial_source")]
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir) / "geoparquet"
            dest_storage = _StorageAdapter(staging_storage)
            dest_folder = f"{dataset_slug}/{file_slug}/{version}/"
            for layer_name, _geom_type in preferred["layers"]:
                source_layer_name = layer_name if layer_name != "default" else None
                if not _is_spatial_source_layer(data_file, format_type, source_layer_name):
                    outputs.append(_skip_output(file_slug, "geoparquet", "non_spatial_source"))
                    continue
                layer_file = _layer_filename(file_slug, preferred["layers"], layer_name)
                result = asyncio.run(
                    process_layer_partitioned_geoparquet(
                        file_path=data_file,
                        format_type=format_type,
                        layer_name=source_layer_name,
                        layer_filename=layer_file,
                        dest_folder=dest_folder,
                        dest_storage=dest_storage,
                        work_dir=out_dir,
                        policy=policy or geoparquet_policy_for(dataset_slug, file_slug),
                    )
                )
                if result.get("error"):
                    raise ValueError(result["error"])
                if not result.get("geoparquet_paths"):
                    outputs.append(_skip_output(file_slug, "geoparquet", "non_spatial_source"))
                    continue
                is_hive_partitioned = result.get("partitioning") != "single_file"
                output_path = (
                    f"{dataset_slug}/{file_slug}/{version}/geoparquet/**/*.parquet"
                    if is_hive_partitioned
                    else result["geoparquet_paths"][0]
                )
                outputs.append(
                    PublishedFormatOutput(
                        file_slug=file_slug,
                        format_type="geoparquet",
                        path=output_path,
                        source_metadata={
                            "hive_partitioned": is_hive_partitioned,
                            "partitioning": result.get("partitioning"),
                            "partition_columns": result.get("partition_columns", []),
                            "row_group_target_bytes": result.get("row_group_target_bytes"),
                            "target_file_size_bytes": result.get("target_file_size_bytes"),
                            "feature_count": result.get("feature_count"),
                        },
                    )
                )
    return outputs


def _write_and_publish_pmtiles(
    staging_storage: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> list[PublishedFormatOutput]:
    existing = _prepare_format_publish(staging_storage, dataset_slug, file_slug, version, "pmtiles")
    if existing:
        return _existing_format_outputs(dataset_slug, file_slug, version, existing)
    outputs: list[PublishedFormatOutput] = []
    with staging_storage.get_local_version_dir(dataset_slug, file_slug, version) as version_dir:
        processed = _discover_staged_formats(Path(version_dir))
        preferred, data_file, format_type = select_processing_input(processed)
        if not preferred or data_file is None or format_type is None:
            return [_skip_output(file_slug, "pmtiles", "non_spatial_source")]
        dest_storage = _StorageAdapter(staging_storage)
        dest_folder = f"{dataset_slug}/{file_slug}/{version}/"
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            for layer_name, _geom_type in preferred["layers"]:
                source_layer_name = layer_name if layer_name != "default" else None
                if not _is_spatial_source_layer(data_file, format_type, source_layer_name):
                    outputs.append(_skip_output(file_slug, "pmtiles", "non_spatial_source"))
                    continue
                layer_file = _layer_filename(file_slug, preferred["layers"], layer_name)
                result = asyncio.run(
                    process_layer_chunked(
                        file_path=data_file,
                        format_type=format_type,
                        layer_name=source_layer_name,
                        layer_filename=layer_file,
                        dest_folder=dest_folder,
                        dest_storage=dest_storage,
                        work_dir=work_dir,
                        skip_parquet=True,
                        skip_pmtiles=False,
                    )
                )
                if result.get("pmtiles_path"):
                    outputs.append(
                        PublishedFormatOutput(
                            file_slug=file_slug,
                            format_type="pmtiles",
                            path=result["pmtiles_path"],
                            source_metadata={
                                "feature_count": result.get("feature_count"),
                                "dropped_for_pmtiles_count": result.get("dropped_for_pmtiles_count"),
                            },
                        )
                    )
    return outputs


def _write_and_publish_shapefile_zip(
    staging_storage: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    policy: ShapefileZipPolicy | None = None,
) -> list[PublishedFormatOutput]:
    existing = _prepare_format_publish(staging_storage, dataset_slug, file_slug, version, "shapefile")
    if existing:
        return _existing_format_outputs(dataset_slug, file_slug, version, existing)
    shapefile_policy = policy or ShapefileZipPolicy()
    if dataset_slug in shapefile_policy.disabled_dataset_families:
        return [_skip_output(file_slug, "shapefile", "disabled_dataset_family")]
    remote_source_size = _remote_source_size_bytes(staging_storage, dataset_slug, file_slug, version)
    if (
        remote_source_size is not None
        and remote_source_size > shapefile_policy.max_estimated_zip_bytes
    ):
        return [_skip_output(file_slug, "shapefile", "estimated_size_exceeds_limit")]
    outputs: list[PublishedFormatOutput] = []
    with staging_storage.get_local_version_dir(dataset_slug, file_slug, version) as version_dir:
        processed = _discover_staged_formats(Path(version_dir))
        preferred, data_file, format_type = select_processing_input(processed)
        if not preferred or data_file is None or format_type is None:
            return [_skip_output(file_slug, "shapefile", "non_spatial_source")]
        source_size = _source_tree_size_bytes(data_file)
        if source_size > shapefile_policy.max_estimated_zip_bytes:
            return [_skip_output(file_slug, "shapefile", "estimated_size_exceeds_limit")]
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir) / "shapefile"
            for layer_name, _geom_type in preferred["layers"]:
                source_layer_name = layer_name if layer_name != "default" else None
                if not _is_spatial_source_layer(data_file, format_type, source_layer_name):
                    outputs.append(_skip_output(file_slug, "shapefile", "non_spatial_source"))
                    continue
                layer_file = _layer_filename(file_slug, preferred["layers"], layer_name)
                gdf = _read_layer(data_file, format_type, source_layer_name)
                result = write_shapefile_zip(gdf, out_dir, layer_file, shapefile_policy)
                if result.created and result.path is not None:
                    remote_paths = _upload_paths(
                        staging_storage,
                        [result.path],
                        out_dir,
                        dataset_slug,
                        file_slug,
                        version,
                        "shapefile",
                    )
                    outputs.extend(
                        PublishedFormatOutput(file_slug, "shapefile", remote_path)
                        for remote_path in remote_paths
                    )
    return outputs


def _copy_catalog_metadata(
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> list[PublishedFormatOutput]:
    return [
        PublishedFormatOutput(file_slug, "metadata", key)
        for key in _copy_metadata_files(
            staging_storage,
            published_storage,
            dataset_slug,
            file_slug,
            version,
        )
    ]


def register_published_outputs(
    api_resource: DatasetApiResource,
    published_storage: PublishedStorageResource,
    dataset_slug: str,
    version: str,
    storage_location_name: str,
    outputs: list[PublishedFormatOutput],
    catalog_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    files = [
        {
            "file_slug": output.file_slug,
            "format": output.format_type,
            "path": output.path,
            "source_metadata": output.source_metadata or {},
        }
        for output in outputs
    ]
    payload = {
        "dataset_slug": dataset_slug,
        "version": version,
        "storage_location_name": storage_location_name,
        "files": files,
    }
    if catalog_metadata:
        payload["catalog_metadata"] = catalog_metadata
    if getattr(api_resource, "enabled", False):
        api_resource.upsert_dataset_version(
            dataset_slug=dataset_slug,
            version=version,
            storage_location_name=storage_location_name,
            files=files,
            overwrite_existing=False,
        )
    return payload


def _published_outputs_from_keys(
    dataset_slug: str,
    file_slug: str,
    version: str,
    keys: list[str],
) -> list[PublishedFormatOutput]:
    prefix = f"{dataset_slug}/{file_slug}/{version}/"
    outputs: list[PublishedFormatOutput] = []
    geoparquet_keys: list[str] = []
    for key in sorted(keys):
        if not key.startswith(prefix):
            continue
        rel = key.removeprefix(prefix)
        parts = Path(rel).parts
        if not parts:
            continue
        format_type = parts[0]
        if format_type == "geoparquet" and key.endswith(".parquet"):
            geoparquet_keys.append(key)
            continue
        outputs.append(PublishedFormatOutput(file_slug, format_type, key))

    if geoparquet_keys:
        is_partitioned = any(
            len(Path(key.removeprefix(prefix)).parts) > 2
            for key in geoparquet_keys
        )
        if is_partitioned:
            outputs.append(
                PublishedFormatOutput(
                    file_slug,
                    "geoparquet",
                    f"{dataset_slug}/{file_slug}/{version}/geoparquet/**/*.parquet",
                    {"hive_partitioned": True},
                )
            )
        elif len(geoparquet_keys) > 1:
            outputs.append(
                PublishedFormatOutput(
                    file_slug,
                    "geoparquet",
                    f"{dataset_slug}/{file_slug}/{version}/geoparquet/*.parquet",
                    {"hive_partitioned": False, "partitioning": "streaming_chunks"},
                )
            )
        else:
            outputs.extend(
                PublishedFormatOutput(file_slug, "geoparquet", key)
                for key in geoparquet_keys
            )
    return outputs


def run_local_version_pipeline(
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
    api_resource: DatasetApiResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    storage_location_name: str,
) -> dict[str, Any]:
    resolved_manifest = load_resolved_source_manifest(
        staging_storage,
        dataset_slug,
        file_slug,
        version,
    )
    summary = summarize_staged_catalog(
        staging_storage,
        dataset_slug,
        file_slug,
        version,
        dataset_slug,
        source_metadata=resolved_manifest.metadata,
    )
    write_catalog_metadata(
        staging_storage,
        dataset_slug=dataset_slug,
        file_slug=file_slug,
        version=version,
        quality_dict=summary.quality_manifest,
        dictionary_dict=summary.data_dictionary,
    )

    outputs: list[PublishedFormatOutput] = []
    outputs.extend(
        _copy_or_generate_geopackage(
            staging_storage,
            dataset_slug,
            file_slug,
            version,
        )
    )
    outputs.extend(_write_and_publish_geoparquet(staging_storage, dataset_slug, file_slug, version))
    outputs.extend(_write_and_publish_pmtiles(staging_storage, dataset_slug, file_slug, version))
    outputs.extend(_write_and_publish_shapefile_zip(staging_storage, dataset_slug, file_slug, version))
    promoted = [
        PublishedFormatOutput(file_slug, Path(key.removeprefix(f"{dataset_slug}/{file_slug}/{version}/")).parts[0], key)
        for key in _copy_version_files(
            staging_storage,
            published_storage,
            dataset_slug,
            file_slug,
            version,
        )
    ]
    outputs.extend(promoted)

    api_payload = register_published_outputs(
        api_resource=api_resource,
        published_storage=published_storage,
        dataset_slug=dataset_slug,
        version=version,
        storage_location_name=storage_location_name,
        outputs=outputs,
        catalog_metadata=resolved_manifest.metadata,
    )
    return {"outputs": outputs, "api_payload": api_payload}


def _output_for_partition(
    context,
    outputs: list[PublishedFormatOutput],
    format_name: str,
) -> Output[dict]:
    dataset_slug, file_slug, version = parse_publish_partition_key(context.partition_key)
    files = [output.path for output in outputs if output.path]
    skip_reasons = [
        output.source_metadata.get("skip_reason")
        for output in outputs
        if output.source_metadata and output.source_metadata.get("skip_reason")
    ]
    metadata = {
        "dataset_slug": dataset_slug,
        "file_slug": file_slug,
        "version": version,
        "format": format_name,
        "file_count": len(files),
    }
    if skip_reasons:
        metadata["skip_reason"] = skip_reasons[0]
    return Output(
        {"version": version, "files": files},
        metadata=metadata,
    )


@asset(
    key=AssetKey(["publish", "promote"]),
    partitions_def=PUBLISH_PARTITIONS,
    group_name="publish",
    deps=[
        AssetKey(["publish", "catalog"]),
        AssetKey(["publish", "formats", "geopackage"]),
        AssetKey(["publish", "formats", "geoparquet"]),
        AssetKey(["publish", "formats", "pmtiles"]),
        AssetKey(["publish", "formats", "shapefile_zip"]),
    ],
    description="Copy the complete staged dataset version prefix to the published bucket.",
)
def publish_version(
    context,
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
) -> Output[dict]:
    dataset_slug, file_slug, version = parse_publish_partition_key(context.partition_key)
    copied = _copy_version_files(
        staging_storage,
        published_storage,
        dataset_slug,
        file_slug,
        version,
    )
    outputs = [
        PublishedFormatOutput(
            file_slug,
            Path(key.removeprefix(f"{dataset_slug}/{file_slug}/{version}/")).parts[0],
            key,
        )
        for key in copied
    ]
    return _output_for_partition(context, outputs, "version")


@asset(
    key=AssetKey(["publish", "formats", "geopackage"]),
    partitions_def=PUBLISH_PARTITIONS,
    group_name="publish",
    deps=[AssetKey(["publish", "catalog"])],
    description="Stage or confirm GeoPackage output for any staged dataset version.",
)
def publish_geopackage(
    context,
    staging_storage: StagingStorageResource,
) -> Output[dict]:
    dataset_slug, file_slug, version = parse_publish_partition_key(context.partition_key)
    outputs = _copy_or_generate_geopackage(
        staging_storage,
        dataset_slug,
        file_slug,
        version,
    )
    return _output_for_partition(context, outputs, "geopackage")


@asset(
    key=AssetKey(["publish", "formats", "geoparquet"]),
    partitions_def=PUBLISH_PARTITIONS,
    group_name="publish",
    deps=[AssetKey(["publish", "catalog"]), AssetKey(["publish", "formats", "geopackage"])],
    description="Generate optimized GeoParquet for any staged dataset version.",
)
def publish_geoparquet(
    context,
    staging_storage: StagingStorageResource,
) -> Output[dict]:
    dataset_slug, file_slug, version = parse_publish_partition_key(context.partition_key)
    outputs = _write_and_publish_geoparquet(
        staging_storage,
        dataset_slug,
        file_slug,
        version,
    )
    return _output_for_partition(context, outputs, "geoparquet")


@asset(
    key=AssetKey(["publish", "formats", "pmtiles"]),
    partitions_def=PUBLISH_PARTITIONS,
    group_name="publish",
    deps=[AssetKey(["publish", "formats", "geopackage"])],
    description="Generate PMTiles for any staged dataset version.",
)
def publish_pmtiles(
    context,
    staging_storage: StagingStorageResource,
) -> Output[dict]:
    dataset_slug, file_slug, version = parse_publish_partition_key(context.partition_key)
    outputs = _write_and_publish_pmtiles(
        staging_storage,
        dataset_slug,
        file_slug,
        version,
    )
    return _output_for_partition(context, outputs, "pmtiles")


@asset(
    key=AssetKey(["publish", "formats", "shapefile_zip"]),
    partitions_def=PUBLISH_PARTITIONS,
    group_name="publish",
    deps=[AssetKey(["publish", "catalog"]), AssetKey(["publish", "formats", "geopackage"])],
    description="Generate zipped shapefile output for small staged dataset versions.",
)
def publish_shapefile_zip(
    context,
    staging_storage: StagingStorageResource,
) -> Output[dict]:
    dataset_slug, file_slug, version = parse_publish_partition_key(context.partition_key)
    outputs = _write_and_publish_shapefile_zip(
        staging_storage,
        dataset_slug,
        file_slug,
        version,
    )
    return _output_for_partition(context, outputs, "shapefile_zip")


publish_assets = [
    publish_version,
    publish_geopackage,
    publish_geoparquet,
    publish_pmtiles,
    publish_shapefile_zip,
]

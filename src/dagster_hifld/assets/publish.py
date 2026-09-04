"""Publish assets for staged dataset versions."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from collections.abc import Mapping, Sequence
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
    DEFAULT_GEOPARQUET_MAX_DATASET_FOOTER_BYTES,
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
from dagster_hifld.source_formats import (
    CANONICAL_SOURCE_FORMAT_DIRS,
    CANONICAL_SOURCE_FORMAT_PRECEDENCE,
    discover_legacy_unknown_shapefile_keys,
)

_PROMOTED_SOURCE_FORMAT_DIRS = CANONICAL_SOURCE_FORMAT_DIRS
_PROCESSING_SOURCE_FORMAT_DIRS = CANONICAL_SOURCE_FORMAT_PRECEDENCE
logger = logging.getLogger(__name__)


def _validate_geoparquet_footer_budget(
    layers: Sequence[Mapping[str, object]],
) -> int:
    total = 0
    for layer in layers:
        outputs = layer.get("outputs")
        if not isinstance(outputs, list):
            raise ValueError(
                "GeoParquet layout layer outputs are missing or invalid."
            )
        for output in outputs:
            if not isinstance(output, Mapping):
                raise ValueError("GeoParquet layout output is invalid.")
            footer_size = output.get("footer_size_bytes")
            if (
                not isinstance(footer_size, int)
                or isinstance(footer_size, bool)
                or footer_size < 0
            ):
                raise ValueError(
                    "GeoParquet layout output footer_size_bytes must be a "
                    "nonnegative integer."
                )
            total += footer_size
    if total > DEFAULT_GEOPARQUET_MAX_DATASET_FOOTER_BYTES:
        raise ValueError(
            "GeoParquet dataset footer metadata exceeds the limit of "
            f"{DEFAULT_GEOPARQUET_MAX_DATASET_FOOTER_BYTES} bytes."
        )
    return total


def _validate_geoparquet_manifest_footer_fields(
    manifest: Mapping[str, object],
    layers: Sequence[Mapping[str, object]],
) -> None:
    footer_total = _validate_geoparquet_footer_budget(layers)
    declared_total = manifest.get("footer_metadata_bytes")
    if (
        not isinstance(declared_total, int)
        or isinstance(declared_total, bool)
        or declared_total < 0
        or declared_total != footer_total
    ):
        raise ValueError(
            "GeoParquet layout manifest footer_metadata_bytes is invalid."
        )
    declared_limit = manifest.get("max_dataset_footer_bytes")
    if (
        not isinstance(declared_limit, int)
        or isinstance(declared_limit, bool)
        or declared_limit != DEFAULT_GEOPARQUET_MAX_DATASET_FOOTER_BYTES
    ):
        raise ValueError(
            "GeoParquet layout manifest max_dataset_footer_bytes is invalid."
        )


def _actual_geoparquet_footer_size(
    storage: StagingStorageResource,
    storage_key: str,
) -> int:
    from pyarrow import parquet

    if storage.use_local or not storage.bucket:
        parquet_file = parquet.ParquetFile(
            Path(storage.local_dir).resolve() / storage_key
        )
        return int(parquet_file.metadata.serialized_size)

    import gcsfs

    fs = gcsfs.GCSFileSystem()
    with fs.open(f"{storage.bucket}/{storage_key}", "rb") as source:
        parquet_file = parquet.ParquetFile(source)
        return int(parquet_file.metadata.serialized_size)


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
    published_storage.delete_prefix(
        published_storage.build_target_location(dataset_slug, file_slug, version, "")
    )
    keys = staging_storage.list_keys(dataset_slug, file_slug, version)
    selected_pairs = [
        (key, relative_key)
        for key in keys
        if (relative_key := _relative_version_key(
            staging_storage,
            dataset_slug,
            file_slug,
            version,
            key,
        ))
        and Path(relative_key).parts[0] != "unknown"
    ]
    copied = list(
        staging_storage.copy_keys_to(
            published_storage,
            [key for key, _relative_key in selected_pairs],
            destination_keys=[
                _logical_version_key(dataset_slug, file_slug, version, relative_key)
                for _key, relative_key in selected_pairs
            ],
        )
    )
    copied.extend(
        _copy_legacy_unknown_shapefile(
            staging_storage,
            published_storage,
            dataset_slug,
            file_slug,
            version,
            keys,
        )
    )
    return sorted(copied)


def _copy_source_format_files(
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    keys: list[str],
) -> list[str]:
    source_pairs: list[tuple[str, str]] = []
    for key in sorted(keys):
        rel_path = _relative_version_key(
            staging_storage,
            dataset_slug,
            file_slug,
            version,
            key,
        )
        if not rel_path:
            continue
        format_dir = Path(rel_path).parts[0]
        if format_dir not in _PROMOTED_SOURCE_FORMAT_DIRS:
            continue
        source_pairs.append(
            (
                key,
                _logical_version_key(dataset_slug, file_slug, version, rel_path),
            )
        )
    copied = list(
        staging_storage.copy_keys_to(
            published_storage,
            [key for key, _destination_key in source_pairs],
            destination_keys=[destination_key for _key, destination_key in source_pairs],
        )
    )
    copied.extend(
        _copy_legacy_unknown_shapefile(
            staging_storage,
            published_storage,
            dataset_slug,
            file_slug,
            version,
            keys,
        )
    )
    return copied


def _copy_legacy_unknown_shapefile(
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    keys: list[str],
) -> list[str]:
    relative_keys = {
        key: relative_key
        for key in keys
        if (relative_key := _relative_version_key(
            staging_storage,
            dataset_slug,
            file_slug,
            version,
            key,
        ))
    }
    if any(
        relative_key.startswith("shapefile/") and Path(key).suffix.lower() == ".shp"
        for key, relative_key in relative_keys.items()
    ):
        return []
    unknown_keys = [
        key for key, relative_key in relative_keys.items() if relative_key.startswith("unknown/")
    ]
    if not unknown_keys:
        return []

    try:
        legacy_keys = discover_legacy_unknown_shapefile_keys(unknown_keys)
    except ValueError as exc:
        logger.warning(
            "Skipping invalid legacy source %s/%s/%s/unknown: %s",
            dataset_slug,
            file_slug,
            version,
            exc,
        )
        return []
    if not legacy_keys:
        return []

    destination_keys = [
        _logical_version_key(
            dataset_slug,
            file_slug,
            version,
            f"shapefile/{relative_keys[key].removeprefix('unknown/')}",
        )
        for key in legacy_keys
    ]
    return list(
        staging_storage.copy_keys_to(
            published_storage,
            list(legacy_keys),
            destination_keys=destination_keys,
        )
    )


def _storage_version_prefix(
    storage: StagingStorageResource | PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> str:
    logical_prefix = f"{dataset_slug}/{file_slug}/{version}"
    configured_prefix = getattr(storage, "prefix", "")
    storage_prefix = configured_prefix.strip("/") if isinstance(configured_prefix, str) else ""
    if storage_prefix:
        return f"{storage_prefix}/{logical_prefix}/"
    return f"{logical_prefix}/"


def _relative_version_key(
    storage: StagingStorageResource | PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    key: str,
) -> str | None:
    version_prefix = _storage_version_prefix(
        storage,
        dataset_slug,
        file_slug,
        version,
    )
    normalized_key = key.lstrip("/")
    if not normalized_key.startswith(version_prefix):
        return None
    return normalized_key.removeprefix(version_prefix)


def _logical_version_key(
    dataset_slug: str,
    file_slug: str,
    version: str,
    relative_key: str,
) -> str:
    return f"{dataset_slug}/{file_slug}/{version}/{relative_key.lstrip('/')}"


def _split_version_key(
    dataset_slug: str,
    file_slug: str,
    version: str,
    key: str,
) -> tuple[str, str] | None:
    """Return a key's version root and version-relative path, preserving storage prefix."""
    normalized_key = key.lstrip("/")
    marker = f"{dataset_slug}/{file_slug}/{version}/"
    marker_start = normalized_key.rfind(marker)
    if marker_start < 0 or (marker_start > 0 and normalized_key[marker_start - 1] != "/"):
        return None
    relative_key = normalized_key[marker_start + len(marker) :]
    if not relative_key:
        return None
    version_root = normalized_key[: marker_start + len(marker)].rstrip("/")
    return version_root, relative_key


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
    return [
        PublishedFormatOutput(
            file_slug=file_slug,
            format_type=Path(relative_key).parts[0],
            path=key,
        )
        for key in copied
        if (
            relative_key := _relative_version_key(
                published_storage,
                dataset_slug,
                file_slug,
                version,
                key,
            )
        )
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
        uploaded.append(asyncio.run(adapter.upload_file(path, remote_path)))
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
        uploaded.append(asyncio.run(adapter.upload_file(path, remote_path)))
    return uploaded


def _published_format_keys(
    storage: StagingStorageResource | PublishedStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    format_dir: str,
) -> list[str]:
    return [
        key
        for key in storage.list_keys(dataset_slug, file_slug, version)
        if (
            relative_key := _relative_version_key(
                storage,
                dataset_slug,
                file_slug,
                version,
                key,
            )
        )
        and relative_key.startswith(f"{format_dir}/")
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
    storage.delete_prefix(
        storage.build_target_location(
            dataset_slug, file_slug, version, format_dir
        )
    )
    return []


def _existing_format_outputs(
    dataset_slug: str,
    file_slug: str,
    version: str,
    existing_keys: list[str],
) -> list[PublishedFormatOutput]:
    return _published_outputs_from_keys(dataset_slug, file_slug, version, existing_keys)


def _validate_reusable_geoparquet_layout(
    storage: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    existing_keys: list[str],
) -> None:
    relative_path = "metadata/geoparquet_layout.json"
    manifest_key = storage.build_target_location(
        dataset_slug, file_slug, version, relative_path
    )
    if not storage.object_exists(manifest_key):
        raise ValueError(
            "Cannot reuse existing GeoParquet without an authoritative layout manifest."
        )
    try:
        manifest = json.loads(
            storage.read_bytes(dataset_slug, file_slug, version, relative_path)
        )
        layers = manifest["layers"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "Cannot reuse existing GeoParquet: authoritative layout manifest is invalid."
        ) from exc
    if (
        manifest.get("schema_version") != 1
        or manifest.get("validation_status") != "valid"
        or not isinstance(layers, list)
        or not layers
        or any(
            not isinstance(layer, dict)
            or layer.get("validation_status") != "valid"
            for layer in layers
        )
    ):
        raise ValueError(
            "Cannot reuse existing GeoParquet: authoritative layout manifest is invalid."
        )
    typed_layers = [layer for layer in layers if isinstance(layer, dict)]
    try:
        _validate_geoparquet_manifest_footer_fields(manifest, typed_layers)
    except ValueError as exc:
        raise ValueError(
            "Cannot reuse existing GeoParquet: authoritative layout manifest is invalid."
        ) from exc
    declared_paths: list[str] = []
    for layer in layers:
        layer_outputs = layer.get("outputs")
        if not isinstance(layer_outputs, list):
            raise ValueError(
                "Cannot reuse existing GeoParquet: authoritative layout manifest is invalid."
            )
        for output in layer_outputs:
            if not isinstance(output, dict) or not isinstance(output.get("path"), str):
                raise ValueError(
                    "Cannot reuse existing GeoParquet: authoritative layout manifest is invalid."
                )
            declared_paths.append(output["path"])
    existing_parquet_paths = {key for key in existing_keys if key.endswith(".parquet")}
    if (
        not declared_paths
        or not existing_parquet_paths
        or len(declared_paths) != len(set(declared_paths))
        or set(declared_paths) != existing_parquet_paths
    ):
        raise ValueError(
            "Cannot reuse existing GeoParquet: layout manifest output set does not match "
            "current objects."
        )
    for layer in typed_layers:
        outputs = layer.get("outputs")
        if not isinstance(outputs, list):
            continue
        for output in outputs:
            if not isinstance(output, dict):
                continue
            path = output.get("path")
            footer_size = output.get("footer_size_bytes")
            if not isinstance(path, str) or not isinstance(footer_size, int):
                continue
            try:
                actual_footer_size = _actual_geoparquet_footer_size(storage, path)
            except (OSError, ValueError, RuntimeError) as exc:
                raise ValueError(
                    "Cannot reuse existing GeoParquet: output footer is unreadable."
                ) from exc
            if actual_footer_size < 0 or footer_size != actual_footer_size:
                raise ValueError(
                    "Cannot reuse existing GeoParquet: output footer size mismatch."
                )


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
    keys = storage.list_keys(dataset_slug, file_slug, version)
    for format_dir in _PROCESSING_SOURCE_FORMAT_DIRS:
        format_keys = [
            key
            for key in keys
            if (
                relative_key := _relative_version_key(
                    storage,
                    dataset_slug,
                    file_slug,
                    version,
                    key,
                )
            )
            and relative_key.startswith(f"{format_dir}/")
        ]
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
        if (
            relative_key := _relative_version_key(
                staging_storage,
                dataset_slug,
                file_slug,
                version,
                key,
            )
        )
        and relative_key.startswith("geopackage/")
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
    existing = _prepare_format_publish(
        staging_storage,
        dataset_slug,
        file_slug,
        version,
        "geoparquet",
    )
    if existing:
        _validate_reusable_geoparquet_layout(
            staging_storage, dataset_slug, file_slug, version, existing
        )
        return _existing_format_outputs(dataset_slug, file_slug, version, existing)
    manifest_relative_path = "metadata/geoparquet_layout.json"
    manifest_key = staging_storage.build_target_location(
        dataset_slug, file_slug, version, manifest_relative_path
    )
    staging_storage.delete_prefix(manifest_key)
    outputs: list[PublishedFormatOutput] = []
    layouts: list[dict[str, Any]] = []
    try:
        with staging_storage.get_local_version_dir(
            dataset_slug, file_slug, version
        ) as version_dir:
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
                    if not _is_spatial_source_layer(
                        data_file, format_type, source_layer_name
                    ):
                        outputs.append(
                            _skip_output(file_slug, "geoparquet", "non_spatial_source")
                        )
                        continue
                    layer_file = _layer_filename(
                        file_slug, preferred["layers"], layer_name
                    )
                    result = asyncio.run(
                        process_layer_partitioned_geoparquet(
                            file_path=data_file,
                            format_type=format_type,
                            layer_name=source_layer_name,
                            layer_filename=layer_file,
                            dest_folder=dest_folder,
                            dest_storage=dest_storage,
                            work_dir=out_dir,
                            policy=policy
                            or geoparquet_policy_for(dataset_slug, file_slug),
                        )
                    )
                    if result.get("error"):
                        raise ValueError(result["error"])
                    geoparquet_paths = result.get("geoparquet_paths")
                    if not geoparquet_paths:
                        outputs.append(
                            _skip_output(file_slug, "geoparquet", "non_spatial_source")
                        )
                        continue
                    layout = result.get("layout")
                    if not isinstance(layout, dict):
                        raise ValueError(
                            "GeoParquet writer returned files without an authoritative layout."
                        )
                    layouts.append(layout)
                    output_path, is_hive_partitioned = _geoparquet_glob_and_hive_status(
                        geoparquet_paths
                    )
                    outputs.append(
                        PublishedFormatOutput(
                            file_slug=file_slug,
                            format_type="geoparquet",
                            path=output_path,
                            source_metadata={
                                "hive_partitioned": is_hive_partitioned,
                                "partitioning": result.get("partitioning"),
                                "partition_columns": result.get(
                                    "partition_columns", []
                                ),
                                "hive_partition_columns": result.get(
                                    "hive_partition_columns", {}
                                ),
                                "chosen_s2_level": result.get("chosen_s2_level"),
                                "row_group_target_bytes": result.get(
                                    "row_group_target_bytes"
                                ),
                                "target_file_size_bytes": result.get(
                                    "target_file_size_bytes"
                                ),
                                "feature_count": result.get("feature_count"),
                            },
                        )
                    )
        if layouts:
            _write_geoparquet_layout_manifest_set(
                staging_storage, dataset_slug, file_slug, version, layouts
            )
    except Exception:
        staging_storage.delete_prefix(
            staging_storage.build_target_location(
                dataset_slug, file_slug, version, "geoparquet"
            )
        )
        staging_storage.delete_prefix(manifest_key)
        raise
    return outputs


def _geoparquet_glob_and_hive_status(paths: list[str]) -> tuple[str, bool]:
    first_root, separator, _ = paths[0].rpartition("/geoparquet/")
    if not separator:
        return paths[0], False
    relative_paths = [path.rpartition("/geoparquet/")[2] for path in paths]
    is_nested = any(len(Path(relative).parts) > 1 for relative in relative_paths)
    is_hive_partitioned = any(
        "=" in segment
        for relative in relative_paths
        for segment in Path(relative).parts[:-1]
    )
    if is_nested or len(paths) > 1:
        return f"{first_root}/geoparquet/**/*.parquet", is_hive_partitioned
    return paths[0], is_hive_partitioned


def _write_geoparquet_layout_manifest(
    storage: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    layer_layout: dict[str, Any],
) -> str:
    """Merge one validated layer layout into the version-level manifest."""
    relative_path = "metadata/geoparquet_layout.json"
    key = storage.build_target_location(dataset_slug, file_slug, version, relative_path)
    layers: list[dict[str, Any]] = []
    if storage.object_exists(key):
        existing = json.loads(
            storage.read_bytes(dataset_slug, file_slug, version, relative_path)
        )
        existing_layers = existing.get("layers", [])
        if isinstance(existing_layers, list):
            layers = [layer for layer in existing_layers if isinstance(layer, dict)]

    layer_name = layer_layout.get("layer")
    source_format = layer_layout.get("source_format")
    layers = [
        layer
        for layer in layers
        if not (
            layer.get("layer") == layer_name
            and layer.get("source_format") == source_format
        )
    ]
    normalized_layout = dict(layer_layout)
    normalized_layout.pop("schema_version", None)
    layers.append(normalized_layout)
    return _write_geoparquet_layout_manifest_set(
        storage, dataset_slug, file_slug, version, layers
    )


def _write_geoparquet_layout_manifest_set(
    storage: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    layer_layouts: list[dict[str, Any]],
) -> str:
    """Publish one authoritative, fully validated layer set."""
    relative_path = "metadata/geoparquet_layout.json"
    layers = []
    for layer_layout in layer_layouts:
        normalized_layout = dict(layer_layout)
        normalized_layout.pop("schema_version", None)
        layers.append(normalized_layout)
    layers.sort(
        key=lambda layer: (
            str(layer.get("layer", "")),
            str(layer.get("source_format", "")),
        )
    )
    footer_metadata_bytes = _validate_geoparquet_footer_budget(layers)
    validation_status = (
        "valid"
        if layers
        and all(layer.get("validation_status") == "valid" for layer in layers)
        else "invalid"
    )
    payload = {
        "schema_version": 1,
        "layers": layers,
        "footer_metadata_bytes": footer_metadata_bytes,
        "max_dataset_footer_bytes": DEFAULT_GEOPARQUET_MAX_DATASET_FOOTER_BYTES,
        "validation_status": validation_status,
    }
    return storage.write(
        dataset_slug,
        file_slug,
        version,
        relative_path,
        (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )


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
    staged_shapefile_keys = _published_format_keys(
        staging_storage,
        dataset_slug,
        file_slug,
        version,
        "shapefile",
    )
    if any(Path(key).suffix.lower() == ".shp" for key in staged_shapefile_keys):
        return _published_outputs_from_keys(
            dataset_slug,
            file_slug,
            version,
            staged_shapefile_keys,
        )
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
    outputs: list[PublishedFormatOutput] = []
    geoparquet_keys: list[tuple[str, str, str]] = []
    for key in sorted(keys):
        split_key = _split_version_key(dataset_slug, file_slug, version, key)
        if split_key is None:
            continue
        version_root, rel = split_key
        parts = Path(rel).parts
        if not parts:
            continue
        format_type = parts[0]
        if format_type == "geoparquet" and key.endswith(".parquet"):
            geoparquet_keys.append((key, version_root, rel))
            continue
        outputs.append(PublishedFormatOutput(file_slug, format_type, key))

    if geoparquet_keys:
        is_nested = any(
            len(Path(rel).parts) > 2 for _key, _root, rel in geoparquet_keys
        )
        is_hive_partitioned = any(
            "=" in segment
            for _key, _root, rel in geoparquet_keys
            for segment in Path(rel).parts[1:-1]
        )
        version_root = geoparquet_keys[0][1]
        if is_nested or len(geoparquet_keys) > 1:
            outputs.append(
                PublishedFormatOutput(
                    file_slug,
                    "geoparquet",
                    f"{version_root}/geoparquet/**/*.parquet",
                    {"hive_partitioned": is_hive_partitioned},
                )
            )
        else:
            outputs.extend(
                PublishedFormatOutput(file_slug, "geoparquet", key)
                for key, _root, _rel in geoparquet_keys
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
    promoted = _published_outputs_from_keys(
        dataset_slug,
        file_slug,
        version,
        _copy_version_files(
            staging_storage,
            published_storage,
            dataset_slug,
            file_slug,
            version,
        ),
    )
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

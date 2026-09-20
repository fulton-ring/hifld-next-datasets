"""Bounded two-bucket Portolan publication workflow."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pyarrow.dataset as pa_dataset
import pyarrow.parquet as pa_parquet
from dagster import DagsterInstance, Field, Out, job, op
from pyproj import CRS, Transformer
from shapely import from_wkb, total_bounds

from dagster_hifld.assets.publish import run_local_version_pipeline
from dagster_hifld.resources import (
    DatasetApiResource,
    PublishedStorageResource,
    StagingStorageResource,
    StorageObjectSnapshot,
)
from dagster_hifld.source_formats import CANONICAL_SOURCE_FORMAT_DIRS

from .catalog import (
    HIFLD_ARCHIVE_LICENSE_HREF,
    HIFLD_ARCHIVE_PUBLIC_DOMAIN_MARK,
    AssetRecord,
    CatalogRecord,
    ColumnRecord,
    build_catalog_sqlite,
    render_portolan_tree,
    update_catalog_sqlite,
    version_sort_key,
)
from .pmtiles import extract_vector_layer_ids
from .release import ReleasePointer
from .thumbnail import render_geoparquet_thumbnail
from .validation import normalize_candidate_tree, validate_candidate_tree

DEFAULT_PUBLIC_ROOT = "http://localhost:8333/hifld-local-published"
CATALOG_KEY = "_catalog/catalog.sqlite"
RELEASE_POINTER_KEY = "_catalog/current.json"


def normalize_stac_datetime(value: object) -> str | None:
    """Normalize authored ISO dates to STAC-compatible RFC 3339 timestamps."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if len(text) == 10:
        try:
            date.fromisoformat(text)
        except ValueError:
            return None
        return f"{text}T00:00:00Z"
    try:
        datetime.fromisoformat(text)
    except ValueError:
        return None
    return text


def _required_text(values: Mapping[str, object], key: str) -> str:
    value = values.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Portolan record requires non-empty {key}.")
    return value.strip()


def _source_text(values: Mapping[str, object], key: str) -> str:
    value = values.get(key)
    return value if isinstance(value, str) else ""


def _source_bounds(value: object) -> tuple[float, float, float, float] | None:
    if not isinstance(value, list) or len(value) != 4:
        return None
    if not all(
        isinstance(coordinate, (int, float)) and not isinstance(coordinate, bool)
        for coordinate in value
    ):
        return None
    return (float(value[0]), float(value[1]), float(value[2]), float(value[3]))


def manifest_tags(value: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
    tags = value.get("tags")
    if not isinstance(tags, dict):
        return ()
    result: list[tuple[str, str]] = []
    for key, values in tags.items():
        if not isinstance(key, str):
            continue
        for item in values if isinstance(values, list) else [values]:
            if isinstance(item, str):
                result.append((key, item))
    return tuple(dict.fromkeys(result))


def _read_source_manifest(
    storage: StagingStorageResource, key: str
) -> dict[str, object]:
    value = json.loads(storage.read_key(key))
    if not isinstance(value, dict):
        raise TypeError(f"Source manifest must be an object: {key}")
    return value


def _metadata_path(
    storage: StagingStorageResource, logical_prefix: str, filename: str
) -> str:
    """Use the source projection when present, otherwise preserve copied metadata."""
    normalized_prefix = logical_prefix.strip("/")
    for relative_path in (
        f"metadata/source/{filename}",
        f"metadata/{filename}",
    ):
        key = "/".join(part for part in (normalized_prefix, relative_path) if part)
        if storage.object_exists(key):
            return relative_path
    raise FileNotFoundError(
        f"Metadata file is absent from {normalized_prefix or 'catalog root'}: {filename}"
    )


def _version_metadata_path(
    storage: StagingStorageResource,
    request: PortolanPublishRequest,
    filename: str,
) -> str:
    return _metadata_path(
        storage,
        f"{request.dataset_slug}/{request.file_slug}/{request.version}",
        filename,
    )


def _metadata_key(logical_prefix: str, relative_path: str) -> str:
    return "/".join(part for part in (logical_prefix.strip("/"), relative_path) if part)


@dataclass(frozen=True)
class PortolanPublishRequest:
    collection_slug: str
    dataset_slug: str
    file_slug: str
    version: str
    title: str
    description: str
    provider: str
    license_href: str | None = None
    tags: tuple[str, ...] = ()
    public_root: str = DEFAULT_PUBLIC_ROOT
    storage_slug: str = "seaweedfs-local-published"
    collection_title: str | None = None
    archive_public_domain: bool = False

    @property
    def resolved_license(self) -> tuple[str, str | None]:
        if self.license_href:
            return ("other", self.license_href)
        if self.archive_public_domain:
            if self.collection_slug != "hifld":
                raise ValueError("Archived HIFLD status requires the hifld collection.")
            return (HIFLD_ARCHIVE_PUBLIC_DOMAIN_MARK, HIFLD_ARCHIVE_LICENSE_HREF)
        return ("other", None)

    @classmethod
    def from_mapping(cls, values: Mapping[str, object]) -> PortolanPublishRequest:
        license_value = values.get("license_href")
        if license_value is not None and not isinstance(license_value, str):
            raise ValueError("license_href must be a string when supplied.")
        tags_value = values.get("tags", ())
        if not isinstance(tags_value, (list, tuple)) or not all(
            isinstance(tag, str) for tag in tags_value
        ):
            raise ValueError("tags must be a list of strings.")
        configured_public_root = os.environ.get("HIFLD_PORTOLAN_PUBLIC_ROOT")
        public_root = (
            values.get("public_root") or configured_public_root or DEFAULT_PUBLIC_ROOT
        )
        configured_storage_slug = os.environ.get("HIFLD_PORTOLAN_STORAGE_SLUG")
        storage_slug = (
            values.get("storage_slug")
            or configured_storage_slug
            or "seaweedfs-local-published"
        )
        if not isinstance(public_root, str) or not isinstance(storage_slug, str):
            raise TypeError("public_root and storage_slug must be strings.")
        provider = values.get("provider", values.get("publisher"))
        archive_public_domain = values.get("archive_public_domain", False)
        if not isinstance(archive_public_domain, bool):
            raise TypeError("archive_public_domain must be a boolean.")
        return cls(
            collection_slug=_required_text(values, "collection_slug"),
            dataset_slug=_required_text(values, "dataset_slug"),
            file_slug=_required_text(values, "file_slug"),
            version=_required_text(values, "version"),
            title=_source_text(values, "title"),
            description=_source_text(values, "description"),
            provider=provider if isinstance(provider, str) else "",
            license_href=license_value.strip() if license_value else None,
            tags=tuple(tags_value),
            public_root=public_root.rstrip("/"),
            storage_slug=storage_slug,
            archive_public_domain=archive_public_domain,
        )


@dataclass(frozen=True)
class GeoParquetFacts:
    feature_count: int
    native_crs: str | None
    geometry_column: str
    geometry_type: str | None
    feature_id_column: str | None
    native_bbox: tuple[float, float, float, float] | None
    crs84_bbox: tuple[float, float, float, float] | None
    columns: tuple[ColumnRecord, ...]


def _scalar_values(value: object) -> tuple[str | int | float | bool | None, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(
        item
        for item in value
        if item is None or isinstance(item, (str, int, float, bool))
    )


def columns_from_dictionary(dictionary: object) -> tuple[ColumnRecord, ...]:
    """Preserve the authored source schema, not the converted asset schema."""
    if not isinstance(dictionary, dict) or not isinstance(
        dictionary.get("columns"), list
    ):
        raise TypeError("Production data dictionary must contain a columns list.")
    enriched: list[ColumnRecord] = []
    for ordinal, source in enumerate(dictionary["columns"]):
        if not isinstance(source, dict):
            raise TypeError("Production dictionary columns must be objects.")
        name = source.get("name")
        data_type = source.get("type")
        nullable = source.get("nullable")
        if not isinstance(name, str) or not isinstance(data_type, str):
            raise TypeError("Production dictionary columns require name and type.")
        if not isinstance(nullable, bool):
            raise TypeError("Production dictionary columns require boolean nullable.")
        description = source.get("description")
        length = source.get("length")
        null_count = source.get("numNullValues")
        unique_count = source.get("numUniqueValues")
        enriched.append(
            ColumnRecord(
                name=name,
                data_type=data_type,
                ordinal=ordinal,
                nullable=nullable,
                is_geometry=data_type == "geometry",
                null_count=null_count if isinstance(null_count, int) else None,
                unique_count=unique_count if isinstance(unique_count, int) else None,
                description=description if isinstance(description, str) else None,
                min_value=str(source["min"]) if source.get("min") is not None else None,
                max_value=str(source["max"]) if source.get("max") is not None else None,
                example_values=_scalar_values(source.get("exampleValues")),
                possible_values=_scalar_values(source.get("possibleValues")),
                length=length if isinstance(length, int) else None,
            )
        )
    return tuple(enriched)


def inspect_geoparquet(version_dir: Path) -> GeoParquetFacts:
    paths = sorted((version_dir / "geoparquet").rglob("*.parquet"))
    if not paths:
        raise ValueError("Generated GeoParquet is required before publication.")
    first = pa_parquet.ParquetFile(paths[0])
    metadata = first.schema_arrow.metadata or {}
    geo_raw = metadata.get(b"geo")
    if geo_raw is None:
        raise ValueError("Generated GeoParquet has no geo footer metadata.")
    geo_value = json.loads(geo_raw)
    if not isinstance(geo_value, dict):
        raise TypeError("Generated GeoParquet geo metadata is invalid.")
    primary = geo_value.get("primary_column")
    geo_columns = geo_value.get("columns")
    if not isinstance(primary, str) or not isinstance(geo_columns, dict):
        raise TypeError("Generated GeoParquet primary geometry is invalid.")
    geometry_value = geo_columns.get(primary)
    if not isinstance(geometry_value, dict):
        raise TypeError("Generated GeoParquet geometry metadata is invalid.")
    crs_value = geometry_value.get("crs")
    bbox_value = geometry_value.get("bbox")
    native_bbox: tuple[float, float, float, float] | None = None
    if isinstance(bbox_value, list) and len(bbox_value) == 4:
        native_bbox = (
            float(bbox_value[0]),
            float(bbox_value[1]),
            float(bbox_value[2]),
            float(bbox_value[3]),
        )
    dataset = pa_dataset.dataset([str(path) for path in paths], format="parquet")
    table = dataset.to_table()
    if native_bbox is None:
        geometries = from_wkb(
            [value for value in table[primary].to_pylist() if value is not None]
        )
        if len(geometries):
            bounds = total_bounds(geometries)
            if all(float(value) == float(value) for value in bounds):
                native_bbox = (
                    float(bounds[0]),
                    float(bounds[1]),
                    float(bounds[2]),
                    float(bounds[3]),
                )
    effective_crs = crs_value if crs_value is not None else "OGC:CRS84"
    native_crs = json.dumps(effective_crs, sort_keys=True)
    crs84_bbox = None
    if native_bbox is not None:
        transformer = Transformer.from_crs(
            CRS.from_user_input(effective_crs), CRS.from_epsg(4326), always_xy=True
        )
        transformed_bounds = transformer.transform_bounds(*native_bbox, densify_pts=21)
        crs84_bbox = (
            float(transformed_bounds[0]),
            float(transformed_bounds[1]),
            float(transformed_bounds[2]),
            float(transformed_bounds[3]),
        )
    feature_id = None
    for candidate in ("id", "objectid"):
        actual = next(
            (name for name in table.column_names if name.lower() == candidate), None
        )
        if actual is None:
            continue
        values = table[actual].to_pylist()
        if all(
            value is not None
            and not isinstance(value, bool)
            and isinstance(value, (str, int))
            for value in values
        ) and len(set(values)) == len(values):
            feature_id = actual
            break
    columns = tuple(
        ColumnRecord(
            name=field.name,
            data_type=str(field.type),
            ordinal=index,
            nullable=field.nullable,
            is_geometry=field.name == primary,
            null_count=table[field.name].null_count,
        )
        for index, field in enumerate(table.schema)
    )
    geometry_types = geometry_value.get("geometry_types")
    geometry_type = (
        geometry_types[0]
        if isinstance(geometry_types, list)
        and len(geometry_types) == 1
        and isinstance(geometry_types[0], str)
        else None
    )
    return GeoParquetFacts(
        table.num_rows,
        native_crs,
        primary,
        geometry_type,
        feature_id,
        native_bbox,
        crs84_bbox,
        columns,
    )


_MEDIA_TYPES = {
    ".parquet": "application/vnd.apache.parquet",
    ".pmtiles": "application/vnd.pmtiles",
    ".png": "image/png",
    ".gpkg": "application/geopackage+sqlite3",
    ".zip": "application/zip",
    ".json": "application/json",
}
_PUBLISHED_DATA_FORMATS = CANONICAL_SOURCE_FORMAT_DIRS | {"geoparquet", "pmtiles"}
_PUBLISHED_ASSET_FORMATS = _PUBLISHED_DATA_FORMATS | {"styles", "thumbnail"}
_STYLE_MEDIA_TYPE = "application/vnd.mapbox.style+json"


def _record_assets(
    published: PublishedStorageResource, request: PortolanPublishRequest
) -> tuple[AssetRecord, ...]:
    logical_prefix = f"{request.dataset_slug}/{request.file_slug}/{request.version}"
    assets: list[AssetRecord] = []
    for key in published.list_prefix(logical_prefix):
        if "/metadata/" in key:
            continue
        relative = key.removeprefix(f"{request.collection_slug}/")
        asset_parts = Path(relative).relative_to(logical_prefix).parts
        if len(asset_parts) < 2 or asset_parts[0] not in _PUBLISHED_ASSET_FORMATS:
            continue
        snapshot = published.object_snapshot(key)
        if snapshot is None:
            raise RuntimeError(f"Published object disappeared during asset scan: {key}")
        sha256 = snapshot.sha256
        if sha256 is None:
            sha256 = published.sha256_key(key)
            current = published.object_snapshot(key)
            if (
                current is None
                or current.generation != snapshot.generation
                or current.size != snapshot.size
            ):
                raise RuntimeError(
                    f"Published object changed during SHA-256 scan: {key}"
                )
        format_key = asset_parts[0]
        pmtiles_layers: tuple[str, ...] = ()
        if format_key == "pmtiles":

            def read_pmtiles_range(
                offset: int,
                length: int,
                *,
                object_key: str = key,
                object_snapshot=snapshot,
            ) -> bytes:
                return published.read_key_range(
                    object_key, offset, length, object_snapshot
                )

            try:
                pmtiles_layers = extract_vector_layer_ids(
                    read_pmtiles_range,
                    snapshot.size,
                )
            except (TypeError, ValueError) as error:
                raise RuntimeError(
                    f"Published PMTiles metadata is invalid for {key}: {error}"
                ) from error
        asset_key = f"{format_key}-{hashlib.sha256(relative.encode()).hexdigest()[:12]}"
        assets.append(
            AssetRecord(
                key=asset_key,
                format_key=format_key,
                title=Path(key).name,
                href=f"{request.public_root}/{key}",
                media_type=(
                    _STYLE_MEDIA_TYPE
                    if format_key == "styles"
                    else _MEDIA_TYPES.get(
                        Path(key).suffix.lower(), "application/octet-stream"
                    )
                ),
                size_bytes=snapshot.size,
                sha256=sha256,
                storage_slug=request.storage_slug,
                storage_revision=snapshot.generation,
                object_key=key,
                pmtiles_layers=pmtiles_layers,
                roles=("style", "default")
                if format_key == "styles"
                else ("thumbnail",)
                if format_key == "thumbnail"
                else ("data",),
            )
        )
    return tuple(assets)


def _write_default_pmtiles_style(
    published: PublishedStorageResource,
    request: PortolanPublishRequest,
    assets: tuple[AssetRecord, ...],
) -> bool:
    """Publish one generic display style using verified PMTiles layer IDs."""
    pmtiles_assets = tuple(asset for asset in assets if asset.format_key == "pmtiles")
    if not pmtiles_assets:
        return False
    sources: dict[str, object] = {}
    layers: list[dict[str, object]] = []
    for index, asset in enumerate(pmtiles_assets):
        source_id = f"pmtiles-{index}"
        sources[source_id] = {"type": "vector", "url": f"pmtiles://{asset.href}"}
        for layer_id in asset.pmtiles_layers:
            layers.extend(
                (
                    {
                        "id": f"{source_id}-{layer_id}-fill",
                        "type": "fill",
                        "source": source_id,
                        "source-layer": layer_id,
                        "paint": {"fill-color": "#1d4ed8", "fill-opacity": 0.32},
                    },
                    {
                        "id": f"{source_id}-{layer_id}-line",
                        "type": "line",
                        "source": source_id,
                        "source-layer": layer_id,
                        "paint": {"line-color": "#1d4ed8", "line-width": 1.5},
                    },
                    {
                        "id": f"{source_id}-{layer_id}-point",
                        "type": "circle",
                        "source": source_id,
                        "source-layer": layer_id,
                        "paint": {
                            "circle-color": "#1d4ed8",
                            "circle-radius": 3,
                            "circle-stroke-color": "#ffffff",
                            "circle-stroke-width": 0.75,
                        },
                    },
                )
            )
    document = {
        "version": 8,
        "name": request.title,
        "sources": sources,
        "layers": layers,
    }
    published.write(
        request.dataset_slug,
        request.file_slug,
        request.version,
        "styles/maplibre.json",
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8"),
    )
    return True


def publish_portolan_record(
    request: PortolanPublishRequest,
    *,
    staging: StagingStorageResource | None = None,
    published: PublishedStorageResource | None = None,
    catalog_only: bool = False,
    use_release_pointer: bool = False,
) -> str:
    """Convert, promote, publish STAC, then atomically replace the catalog DB last."""
    staging_storage = staging or StagingStorageResource.from_env()
    published_base = published or PublishedStorageResource.from_env()
    record = _prepare_portolan_record(
        request,
        staging_storage,
        published_base,
        convert=not catalog_only,
        promote=not catalog_only,
    )
    return _publish_catalog(
        (record,), request, published_base, use_release_pointer=use_release_pointer
    )


def _promote_source_metadata(
    staging: StagingStorageResource,
    published: PublishedStorageResource,
    request: PortolanPublishRequest,
) -> None:
    """Publish the pinned source documents referenced by the rendered STAC."""
    for filename in (
        "data_dictionary.json",
        "quality_manifest.json",
        "source_manifest.json",
    ):
        logical_prefix = f"{request.dataset_slug}/{request.file_slug}/{request.version}"
        key = _metadata_key(
            logical_prefix, _version_metadata_path(staging, request, filename)
        )
        if not published.object_exists(key):
            staging.copy_key_to(published, key, key)
    for logical_prefix, filename in (
        ("", "collections.json"),
        (request.dataset_slug, "source_manifest.json"),
        (f"{request.dataset_slug}/{request.file_slug}", "source_manifest.json"),
    ):
        try:
            key = _metadata_key(
                logical_prefix, _metadata_path(staging, logical_prefix, filename)
            )
        except FileNotFoundError:
            continue
        if not published.object_exists(key):
            staging.copy_key_to(published, key, key)


def _promote_staged_data(
    staging: StagingStorageResource,
    published: PublishedStorageResource,
    request: PortolanPublishRequest,
) -> None:
    """Promote already-derived, generation-pinned data without rewriting bytes."""
    logical_prefix = f"{request.dataset_slug}/{request.file_slug}/{request.version}"
    for key in staging.list_prefix(logical_prefix):
        relative = key.removeprefix(f"{request.collection_slug}/")
        parts = Path(relative).relative_to(logical_prefix).parts
        if len(parts) >= 2 and parts[0] in _PUBLISHED_DATA_FORMATS:
            staging.copy_key_to(published, relative, relative)


def _prepare_portolan_record(
    request: PortolanPublishRequest,
    staging_storage: StagingStorageResource,
    published_base: PublishedStorageResource,
    *,
    convert: bool = True,
    promote: bool = True,
) -> CatalogRecord:
    if staging_storage.prefix.strip("/") != request.collection_slug:
        staging_storage = staging_storage.model_copy(
            update={"prefix": request.collection_slug}
        )
    if published_base.prefix.strip("/") != request.collection_slug:
        published_data = published_base.model_copy(
            update={"prefix": request.collection_slug}
        )
    else:
        published_data = published_base
    if convert:
        run_local_version_pipeline(
            staging_storage,
            published_data,
            DatasetApiResource(),
            request.dataset_slug,
            request.file_slug,
            request.version,
            request.storage_slug,
        )
    elif promote:
        _promote_staged_data(staging_storage, published_data, request)
    if promote:
        _promote_source_metadata(staging_storage, published_data, request)
    _write_default_pmtiles_style(
        published_data, request, _record_assets(published_data, request)
    )
    with staging_storage.get_local_version_dir(
        request.dataset_slug,
        request.file_slug,
        request.version,
        include_directories=("geoparquet",),
    ) as version_dir:
        local_version_dir = Path(version_dir)
        facts = inspect_geoparquet(local_version_dir)
        thumbnail = render_geoparquet_thumbnail(
            local_version_dir, facts.geometry_column
        )
    if thumbnail is not None:
        published_data.write(
            request.dataset_slug,
            request.file_slug,
            request.version,
            "thumbnail/thumbnail.png",
            thumbnail,
        )
    quality_path = (
        "metadata/quality_manifest.json"
        if convert
        else _version_metadata_path(staging_storage, request, "quality_manifest.json")
    )
    quality_value = json.loads(
        staging_storage.read_bytes(
            request.dataset_slug,
            request.file_slug,
            request.version,
            quality_path,
        )
    )
    if not isinstance(quality_value, dict):
        raise TypeError("Quality manifest must be a JSON object.")
    source_dictionary = json.loads(
        staging_storage.read_bytes(
            request.dataset_slug,
            request.file_slug,
            request.version,
            _version_metadata_path(staging_storage, request, "data_dictionary.json"),
        )
    )
    if not isinstance(source_dictionary, dict):
        raise TypeError("Production data dictionary must be a JSON object.")
    source_quality = json.loads(
        staging_storage.read_bytes(
            request.dataset_slug,
            request.file_slug,
            request.version,
            _version_metadata_path(staging_storage, request, "quality_manifest.json"),
        )
    )
    if not isinstance(source_quality, dict):
        raise TypeError("Production quality manifest must be a JSON object.")
    source_manifest = json.loads(
        staging_storage.read_bytes(
            request.dataset_slug,
            request.file_slug,
            request.version,
            _version_metadata_path(staging_storage, request, "source_manifest.json"),
        )
    )
    if not isinstance(source_manifest, dict):
        raise TypeError("Production source manifest must be a JSON object.")
    dataset_manifest = _read_source_manifest(
        staging_storage,
        _metadata_key(
            request.dataset_slug,
            _metadata_path(
                staging_storage, request.dataset_slug, "source_manifest.json"
            ),
        ),
    )
    file_manifest = _read_source_manifest(
        staging_storage,
        _metadata_key(
            f"{request.dataset_slug}/{request.file_slug}",
            _metadata_path(
                staging_storage,
                f"{request.dataset_slug}/{request.file_slug}",
                "source_manifest.json",
            ),
        ),
    )
    collections_value = json.loads(
        staging_storage.read_key(
            _metadata_key("", _metadata_path(staging_storage, "", "collections.json"))
        )
    )
    if not isinstance(collections_value, list):
        raise TypeError("Production collections export must be an array.")
    collection_manifest = next(
        (
            item
            for item in collections_value
            if isinstance(item, dict) and item.get("slug") == request.collection_slug
        ),
        None,
    )
    if collection_manifest is None:
        raise ValueError(
            f"Collection absent from production export: {request.collection_slug}"
        )
    quality_feature_count = quality_value.get("feature_count")
    if quality_feature_count != facts.feature_count:
        raise ValueError(
            "Quality manifest feature count does not match generated GeoParquet."
        )
    invalid_geometry_count = quality_value.get("invalid_geometry_count", 0)
    null_geometry_count = quality_value.get("null_geometry_count", 0)
    quality_passed = quality_value.get("quality_check_passed", False)
    if (
        not isinstance(invalid_geometry_count, int)
        or not isinstance(null_geometry_count, int)
        or not isinstance(quality_passed, bool)
    ):
        raise TypeError("Quality manifest contains invalid quality fields.")
    title_value = source_dictionary.get("title", source_manifest.get("title"))
    description_value = source_dictionary.get(
        "description", source_manifest.get("description")
    )
    provider_value = source_dictionary.get(
        "publisher", source_manifest.get("publisher")
    )
    keywords_value = source_dictionary.get("keywords")
    issued_value = source_dictionary.get("date_issued")
    modified_value = source_dictionary.get("date_modified")
    sampled_feature_count = source_quality.get("sampled_feature_count")
    sampled_invalid_count = source_quality.get("sampled_invalid_geometry_count")
    sampled_null_count = source_quality.get("sampled_null_geometry_count")
    source_columns_hash = source_quality.get("columns_hash")
    agency_value = source_dictionary.get("agency", source_manifest.get("agency"))
    office_value = source_dictionary.get("office", source_manifest.get("office"))
    source_url_value = source_dictionary.get(
        "source_url", source_manifest.get("source_url")
    )
    manifest_role_value = source_manifest.get("manifest_role")
    manifest_keys_value = source_manifest.get("manifest_keys")
    source_tags = source_manifest.get("tags")
    categories = (
        source_tags.get("categories") if isinstance(source_tags, dict) else None
    )
    keyword_tags = (
        tuple(item for item in keywords_value if isinstance(item, str))
        if isinstance(keywords_value, list)
        else ()
    )
    category_tags = (
        tuple(item for item in categories if isinstance(item, str))
        if isinstance(categories, list)
        else ()
    )
    metadata_sources_value = source_dictionary.get("metadata_sources")
    resolved_value = source_dictionary.get("metadata_resolved_from")
    inventory_match_value = source_dictionary.get("inventory_match_type")
    return CatalogRecord(
        request.collection_slug,
        request.dataset_slug,
        request.file_slug,
        request.version,
        title_value
        if isinstance(title_value, str)
        else _source_text(file_manifest, "title"),
        description_value
        if isinstance(description_value, str)
        else _source_text(file_manifest, "description"),
        "spatial",
        facts.feature_count,
        _record_assets(published_data, request),
        collection_title=request.collection_title
        or _source_text(collection_manifest, "name"),
        collection_description=_source_text(collection_manifest, "description"),
        collection_created_at=_source_text(collection_manifest, "created_at") or None,
        collection_updated_at=_source_text(collection_manifest, "updated_at") or None,
        dataset_title=_source_text(dataset_manifest, "title"),
        dataset_description=_source_text(dataset_manifest, "description"),
        dataset_tags=manifest_tags(dataset_manifest),
        file_tags=manifest_tags(file_manifest),
        tags=tuple(dict.fromkeys((*keyword_tags, *category_tags))),
        columns=columns_from_dictionary(source_dictionary),
        native_crs=facts.native_crs,
        geometry_column=facts.geometry_column,
        geometry_type=facts.geometry_type,
        feature_id_column=facts.feature_id_column,
        native_bbox=facts.native_bbox,
        crs84_bbox=facts.crs84_bbox,
        source_version_description=_source_text(source_quality, "description") or None,
        source_version_bounds=_source_bounds(source_quality.get("bounds")),
        quality_passed=quality_passed,
        invalid_geometry_count=invalid_geometry_count,
        null_geometry_count=null_geometry_count,
        sampled_feature_count=(
            sampled_feature_count if isinstance(sampled_feature_count, int) else None
        ),
        sampled_invalid_geometry_count=(
            sampled_invalid_count if isinstance(sampled_invalid_count, int) else None
        ),
        sampled_null_geometry_count=(
            sampled_null_count if isinstance(sampled_null_count, int) else None
        ),
        source_columns_hash=(
            source_columns_hash if isinstance(source_columns_hash, str) else None
        ),
        quality_manifest_href=quality_path,
        quality_provenance="generated_with_pinned_source_samples",
        license_id=request.resolved_license[0],
        license_href=request.resolved_license[1],
        provider=provider_value if isinstance(provider_value, str) else None,
        agency=agency_value if isinstance(agency_value, str) else None,
        office=office_value if isinstance(office_value, str) else None,
        source_url=source_url_value if isinstance(source_url_value, str) else None,
        metadata_sources=(
            tuple(item for item in metadata_sources_value if isinstance(item, str))
            if isinstance(metadata_sources_value, list)
            else ()
        ),
        metadata_resolved_from=(
            tuple(
                (key, value)
                for key, value in resolved_value.items()
                if isinstance(key, str) and isinstance(value, str)
            )
            if isinstance(resolved_value, dict)
            else ()
        ),
        inventory_match_type=(
            inventory_match_value if isinstance(inventory_match_value, str) else None
        ),
        manifest_role=(
            manifest_role_value if isinstance(manifest_role_value, str) else None
        ),
        manifest_keys=(
            tuple(item for item in manifest_keys_value if isinstance(item, str))
            if isinstance(manifest_keys_value, list)
            else ()
        ),
        created_at=normalize_stac_datetime(issued_value),
        updated_at=normalize_stac_datetime(modified_value),
    )


def _source_publisher_from_storage(
    published: PublishedStorageResource, version_path: str
) -> str | None:
    """Read authored publisher evidence for a retained version Collection."""
    parts = version_path.split("/")
    if len(parts) != 4:
        raise ValueError(f"Invalid Portolan version path: {version_path}")
    collection, dataset, file_slug, _ = parts
    metadata_keys = (
        f"{version_path}/metadata/data_dictionary.json",
        f"{version_path}/metadata/source_manifest.json",
        f"{collection}/{dataset}/{file_slug}/metadata/source_manifest.json",
        f"{collection}/{dataset}/metadata/source_manifest.json",
    )
    for key in metadata_keys:
        if not published.object_exists(key):
            continue
        document = json.loads(published.read_key(key))
        if not isinstance(document, dict):
            raise TypeError(f"Source metadata is not an object: {key}")
        publisher = document.get("publisher")
        if isinstance(publisher, str) and publisher.strip():
            return publisher.strip()
    return None


def _sync_corrected_spatial_extents(database: Path, root: Path) -> None:
    """Keep the disposable SQLite projection aligned with corrected STAC boxes."""
    with sqlite3.connect(database) as connection:
        for path in sorted(root.rglob("collection.json")):
            document = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(document, dict) or document.get(
                "hifld:spatial_extent_status"
            ) not in {"unknown_source_bbox", "clamped_to_crs84"}:
                continue
            identifier = document.get("id")
            extent = document.get("extent")
            spatial = extent.get("spatial") if isinstance(extent, dict) else None
            boxes = spatial.get("bbox") if isinstance(spatial, dict) else None
            bounds = (
                _source_bounds(boxes[0])
                if isinstance(boxes, list) and len(boxes) == 1
                else None
            )
            if not isinstance(identifier, str) or bounds is None:
                raise ValueError(f"Corrected spatial extent is invalid: {path}")
            serialized = json.dumps(list(bounds))
            connection.execute(
                "UPDATE versions SET crs84_bbox_json = ? WHERE version_path = ?",
                (serialized, identifier),
            )
            connection.execute(
                "UPDATE asset_objects SET crs84_bbox_json = ? "
                "WHERE asset_path IN "
                "(SELECT asset_path FROM assets WHERE version_path = ?)",
                (serialized, identifier),
            )


def _publish_catalog(
    records: tuple[CatalogRecord, ...],
    request: PortolanPublishRequest,
    published: PublishedStorageResource,
    *,
    use_release_pointer: bool = False,
) -> str:
    if use_release_pointer:
        return _publish_release_catalog(records, request, published)
    # STAC hierarchy and SQLite are bucket-root objects; only data assets use
    # the collection-prefixed storage resource.
    published = published.model_copy(update={"prefix": ""})
    with tempfile.TemporaryDirectory(prefix="hifld-portolan-") as temporary:
        root = Path(temporary)
        previous = published.object_snapshot(CATALOG_KEY)
        previous_documents = _seed_release_bundle(published, root, None)
        render_portolan_tree(root, records, public_root=request.public_root)
        _merge_and_rebase_release_documents(
            root,
            previous_documents,
            request.public_root,
            request.public_root,
            previous_release=False,
        )
        normalize_candidate_tree(
            root,
            source_publisher=lambda version_path: _source_publisher_from_storage(
                published, version_path
            ),
        )
        validate_candidate_tree(root)
        database = root / CATALOG_KEY
        database.parent.mkdir(parents=True, exist_ok=True)
        root_href = f"{request.public_root}/catalog.json"
        if previous is None:
            generation = build_catalog_sqlite(database, records, root_href=root_href)
        else:
            database.write_bytes(published.read_key(CATALOG_KEY))
            generation = update_catalog_sqlite(database, records)
        _sync_corrected_spatial_extents(database, root)
        for path in sorted(root.rglob("*")):
            if path.is_file() and path != database:
                relative_path = path.relative_to(root).as_posix()
                body = path.read_bytes()
                if path.suffix == ".json":
                    document = json.loads(body)
                    if not isinstance(document, dict):
                        raise TypeError(
                            f"Rendered STAC document must be an object: {path}"
                        )
                    if path.name == "catalog.json" and published.object_exists(
                        relative_path
                    ):
                        previous_document = json.loads(
                            published.read_key(relative_path)
                        )
                        document = _merge_catalog_children(previous_document, document)
                    body = (json.dumps(document, indent=2) + "\n").encode()
                published.write_key(relative_path, body)
        published.write_key_if_unchanged(CATALOG_KEY, database.read_bytes(), previous)
    return generation


def _publish_release_catalog(
    records: tuple[CatalogRecord, ...],
    request: PortolanPublishRequest,
    published_base: PublishedStorageResource,
) -> str:
    """Publish one immutable STAC/SQLite bundle then conditionally commit it."""
    published = published_base.model_copy(update={"prefix": ""})
    previous_pointer_snapshot = published.object_snapshot(RELEASE_POINTER_KEY)
    previous_pointer = (
        ReleasePointer.parse(published.read_key(RELEASE_POINTER_KEY))
        if previous_pointer_snapshot is not None
        else None
    )
    generation = str(uuid4())
    release_prefix = f"releases/{generation}"
    release_root = f"{request.public_root.rstrip('/')}/{release_prefix}"
    with tempfile.TemporaryDirectory(prefix="hifld-portolan-release-") as temporary:
        root = Path(temporary)
        previous_documents = _seed_release_bundle(published, root, previous_pointer)
        database = root / "_catalog" / "catalog.sqlite"
        database.parent.mkdir(parents=True, exist_ok=True)
        root_href = f"{release_root}/catalog.json"
        if database.exists():
            try:
                database_generation = update_catalog_sqlite(
                    database, records, catalog_generation=generation
                )
            except ValueError:
                if previous_pointer is not None:
                    raise
                database.unlink()
                database_generation = build_catalog_sqlite(
                    database,
                    records,
                    catalog_generation=generation,
                    root_href=root_href,
                )
        else:
            database_generation = build_catalog_sqlite(
                database,
                records,
                catalog_generation=generation,
                root_href=root_href,
            )
        if database_generation != generation:
            raise RuntimeError("Catalog generation did not match release generation.")
        render_portolan_tree(root, records, public_root=release_root)
        _merge_and_rebase_release_documents(
            root,
            previous_documents,
            (
                f"{request.public_root.rstrip('/')}/releases/{previous_pointer.generation}"
                if previous_pointer is not None
                else request.public_root
            ),
            release_root,
            previous_release=previous_pointer is not None,
        )
        normalize_candidate_tree(
            root,
            source_publisher=lambda version_path: _source_publisher_from_storage(
                published, version_path
            ),
        )
        _sync_corrected_spatial_extents(database, root)
        validate_candidate_tree(root)
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            relative_path = path.relative_to(root).as_posix()
            published.write_key_if_unchanged(
                f"{release_prefix}/{relative_path}",
                path.read_bytes(),
                None,
            )
        database_bytes = database.read_bytes()
        pointer = ReleasePointer(
            generation=generation,
            catalog_key=f"{release_prefix}/{CATALOG_KEY}",
            root_key=f"{release_prefix}/catalog.json",
            sha256=hashlib.sha256(database_bytes).hexdigest(),
            size_bytes=len(database_bytes),
            published_at=_release_timestamp(),
        )
        published.write_key_if_unchanged(
            RELEASE_POINTER_KEY,
            pointer.to_bytes(),
            previous_pointer_snapshot,
        )
    return generation


def rollback_portolan_release(
    target_generation: str,
    *,
    published: PublishedStorageResource | None = None,
    expected_snapshot: StorageObjectSnapshot | None = None,
) -> ReleasePointer:
    """Conditionally select a verified prior release without mutating its contents."""
    _validate_release_generation(target_generation)
    storage = (published or PublishedStorageResource.from_env()).model_copy(
        update={"prefix": ""}
    )
    current_snapshot = expected_snapshot or storage.object_snapshot(RELEASE_POINTER_KEY)
    if current_snapshot is None:
        raise ValueError("Cannot roll back without an active release pointer.")
    if current_snapshot.key != RELEASE_POINTER_KEY:
        raise ValueError("Rollback snapshot does not reference the release pointer.")

    release_prefix = f"releases/{target_generation}"
    catalog_key = f"{release_prefix}/{CATALOG_KEY}"
    root_key = f"{release_prefix}/catalog.json"
    if storage.object_snapshot(root_key) is None:
        raise ValueError("Rollback release has no root STAC document.")
    catalog_bytes = storage.read_key(catalog_key)
    if _catalog_generation(catalog_bytes) != target_generation:
        raise ValueError("Rollback catalog generation does not match selected release.")

    pointer = ReleasePointer(
        generation=target_generation,
        catalog_key=catalog_key,
        root_key=root_key,
        sha256=hashlib.sha256(catalog_bytes).hexdigest(),
        size_bytes=len(catalog_bytes),
        published_at=_release_timestamp(),
    )
    storage.write_key_if_unchanged(
        RELEASE_POINTER_KEY, pointer.to_bytes(), current_snapshot
    )
    return pointer


def _catalog_generation(catalog_bytes: bytes) -> str:
    """Read the catalog generation from a candidate without retaining a temp file."""
    descriptor, path_name = tempfile.mkstemp(prefix="hifld-release-", suffix=".sqlite")
    path = Path(path_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(catalog_bytes)
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            row = connection.execute(
                "SELECT catalog_generation FROM catalog_metadata WHERE singleton = 1"
            ).fetchone()
        finally:
            connection.close()
    except sqlite3.Error as error:
        raise ValueError(
            "Rollback catalog is not a readable Portolan SQLite catalog."
        ) from error
    finally:
        path.unlink(missing_ok=True)
    if row is None or not isinstance(row[0], str):
        raise ValueError("Rollback catalog has no catalog generation.")
    return row[0]


def _release_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _validate_release_generation(value: str) -> None:
    ReleasePointer(
        generation=value,
        catalog_key=f"releases/{value}/{CATALOG_KEY}",
        root_key=f"releases/{value}/catalog.json",
        sha256="0" * 64,
        size_bytes=1,
        published_at="1970-01-01T00:00:00Z",
    )


def _seed_release_bundle(
    published: PublishedStorageResource,
    root: Path,
    previous_pointer: ReleasePointer | None,
) -> dict[str, object]:
    """Materialize the prior catalog bundle without ever copying data assets."""
    if previous_pointer is None:
        source_keys = tuple(
            key for key in published.list_prefix() if _is_catalog_bundle_key(key)
        )
        database_key = CATALOG_KEY
    else:
        release_prefix = f"releases/{previous_pointer.generation}/"
        source_keys = tuple(
            key
            for key in published.list_prefix(release_prefix)
            if _is_catalog_bundle_key(key.removeprefix(release_prefix))
        )
        database_key = previous_pointer.catalog_key
    documents: dict[str, object] = {}
    for source_key in source_keys:
        relative_key = (
            source_key
            if previous_pointer is None
            else source_key.removeprefix(f"releases/{previous_pointer.generation}/")
        )
        destination = root / relative_key
        destination.parent.mkdir(parents=True, exist_ok=True)
        body = published.read_key(source_key)
        destination.write_bytes(body)
        if destination.suffix == ".json":
            value = json.loads(body)
            if isinstance(value, dict):
                documents[relative_key] = value
    if published.object_exists(database_key):
        destination = root / CATALOG_KEY
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(published.read_key(database_key))
    return documents


def _is_catalog_bundle_key(key: str) -> bool:
    name = Path(key).name
    return key == "catalog.json" or name in {
        "catalog.json",
        "collection.json",
        "AGENTS.md",
        "README.md",
        "LICENSE.md",
    }


def _merge_and_rebase_release_documents(
    root: Path,
    previous_documents: dict[str, object],
    public_root: str,
    release_root: str,
    *,
    previous_release: bool,
) -> None:
    for path in sorted(root.rglob("*.json")):
        relative_path = path.relative_to(root).as_posix()
        value = json.loads(path.read_bytes())
        if not isinstance(value, dict):
            raise TypeError(f"Rendered STAC document must be an object: {path}")
        previous = previous_documents.get(relative_path)
        if isinstance(previous, dict):
            _rebase_navigation_links(
                previous,
                public_root,
                release_root,
                all_links=previous_release,
            )
        if path.name == "catalog.json" and previous is not None:
            value = _merge_catalog_children(previous, value)
        _rebase_navigation_links(
            value, public_root, release_root, all_links=previous_release
        )
        path.write_bytes((json.dumps(value, indent=2) + "\n").encode())


def _rebase_navigation_links(
    document: dict[str, object],
    old_root: str,
    new_root: str,
    *,
    all_links: bool = False,
) -> None:
    links = document.get("links")
    if not isinstance(links, list):
        return
    navigation_rels = {"root", "parent", "self", "child", "latest-version"}
    old_prefix = old_root.rstrip("/") + "/"
    new_prefix = new_root.rstrip("/") + "/"
    for link in links:
        if not isinstance(link, dict) or (
            not all_links and link.get("rel") not in navigation_rels
        ):
            continue
        href = link.get("href")
        if (
            isinstance(href, str)
            and href.startswith(old_prefix)
            and not href.startswith(new_prefix)
        ):
            link["href"] = new_prefix + href.removeprefix(old_prefix)


def _merge_catalog_children(
    previous: object, current: dict[str, object]
) -> dict[str, object]:
    """Retain published sibling links when rendering only the changed record."""
    if not isinstance(previous, dict):
        raise TypeError("Published STAC catalog must be an object.")
    previous_links = previous.get("links")
    current_links = current.get("links")
    if not isinstance(previous_links, list) or not isinstance(current_links, list):
        raise TypeError("Published STAC catalogs require links arrays.")
    children: dict[str, dict[str, object]] = {}
    for link in (*previous_links, *current_links):
        if isinstance(link, dict) and link.get("rel") == "child":
            href = link.get("href")
            if isinstance(href, str):
                children[href] = link
    merged_links = [
        link
        for link in current_links
        if isinstance(link, dict) and link.get("rel") not in {"child", "latest-version"}
    ]
    merged_links.extend(children[href] for href in sorted(children))
    if any(
        isinstance(link, dict) and link.get("rel") == "latest-version"
        for link in (*previous_links, *current_links)
    ):
        versions: list[tuple[tuple[int, int, int, int, int, str, str], str, str]] = []
        for href in children:
            parts = href.rstrip("/").split("/")
            if len(parts) < 2 or parts[-1] != "collection.json":
                continue
            label = parts[-2]
            versions.append((version_sort_key(label), href, label))
        if versions:
            _, href, label = max(versions)
            merged_links.append(
                {
                    "rel": "latest-version",
                    "href": href,
                    "type": "application/json",
                    "title": label,
                }
            )
    current["links"] = merged_links
    return current


def _ensure_absolute_self_link(
    document: dict[str, object], relative_path: str, public_root: str
) -> dict[str, object]:
    """Add a canonical absolute self link while preserving authored relations."""
    links_value = document.get("links")
    links = list(links_value) if isinstance(links_value, list) else []
    links = [
        link
        for link in links
        if not (isinstance(link, dict) and link.get("rel") == "self")
    ]
    links.append(
        {
            "rel": "self",
            "href": f"{public_root.rstrip('/')}/{relative_path.lstrip('/')}",
            "type": "application/json",
        }
    )
    document["links"] = links
    return document


def load_publish_requests(value: object) -> tuple[PortolanPublishRequest, ...]:
    """Parse either one authored record or a fixture manifest records array."""
    raw_records: object
    if isinstance(value, dict) and "records" in value:
        raw_records = value["records"]
    else:
        raw_records = [value]
    if not isinstance(raw_records, list) or not raw_records:
        raise ValueError(
            "Portolan fixture manifest requires a non-empty records array."
        )
    requests: list[PortolanPublishRequest] = []
    by_identity: dict[tuple[str, str, str, str], PortolanPublishRequest] = {}
    for raw_record in raw_records:
        if not isinstance(raw_record, dict):
            raise TypeError("Each Portolan fixture record must be a JSON object.")
        request = PortolanPublishRequest.from_mapping(raw_record)
        identity = (
            request.collection_slug,
            request.dataset_slug,
            request.file_slug,
            request.version,
        )
        existing = by_identity.get(identity)
        if existing is not None and existing != request:
            raise ValueError(f"Authored metadata disagrees for fixture {identity}.")
        if existing is None:
            by_identity[identity] = request
            requests.append(request)
    return tuple(requests)


def publish_portolan_manifest(
    requests: tuple[PortolanPublishRequest, ...],
    *,
    catalog_only: bool = False,
    use_release_pointer: bool = True,
) -> tuple[str, ...]:
    """Publish every record in one bounded fixture manifest."""
    if not requests:
        raise ValueError("Cannot publish an empty Portolan manifest.")
    staging = StagingStorageResource.from_env()
    published = PublishedStorageResource.from_env()
    records = tuple(
        _prepare_portolan_record(
            request,
            staging,
            published,
            convert=not catalog_only,
            promote=not catalog_only,
        )
        for request in requests
    )
    generation = _publish_catalog(
        records,
        requests[0],
        published,
        use_release_pointer=use_release_pointer,
    )
    return (generation,)


_JOB_CONFIG = {
    "manifest_json": Field(str, default_value=""),
    "manifest_path": Field(str, default_value=""),
    "catalog_only": Field(bool, default_value=False),
}


@op(config_schema=_JOB_CONFIG, out=Out(list[str]))
def publish_portolan(context) -> list[str]:
    manifest_json = context.op_config["manifest_json"]
    manifest_path = context.op_config["manifest_path"]
    if bool(manifest_json) == bool(manifest_path):
        raise ValueError("Configure exactly one of manifest_json or manifest_path.")
    raw = (
        Path(manifest_path).read_text(encoding="utf-8")
        if manifest_path
        else manifest_json
    )
    return list(
        publish_portolan_manifest(
            load_publish_requests(json.loads(raw)),
            catalog_only=context.op_config["catalog_only"],
        )
    )


@job(description="Convert and publish one authored HIFLD Portolan fixture record.")
def portolan_publish_job() -> None:
    publish_portolan()


def execute_portolan_manifest(
    manifest_path: Path,
    dagster_home: Path,
    *,
    catalog_only: bool = False,
):
    """Execute one inspectable local Dagster run against a fixture manifest."""
    dagster_home.mkdir(parents=True, exist_ok=True)
    os.environ["DAGSTER_HOME"] = str(dagster_home.resolve())
    os.environ.setdefault("DAGSTER_DISABLE_TELEMETRY", "1")
    with DagsterInstance.local_temp(str(dagster_home)) as instance:
        return portolan_publish_job.execute_in_process(
            instance=instance,
            run_config={
                "ops": {
                    "publish_portolan": {
                        "config": {
                            "manifest_path": str(manifest_path.resolve()),
                            "catalog_only": catalog_only,
                        }
                    }
                }
            },
        )

"""Bounded two-bucket Portolan publication workflow."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pyarrow.dataset as pa_dataset
import pyarrow.parquet as pa_parquet
from dagster import DagsterInstance, Field, Out, job, op
from pyproj import CRS, Transformer
from shapely import from_wkb, total_bounds

from dagster_hifld.assets.publish import run_local_version_pipeline, _copy_version_files
from dagster_hifld.promotion import promote_immutable_objects, write_immutable_object
from dagster_hifld.release_approval import _storage_identity, require_generated_object_approval, verifier_implementation_sha256
from dagster_hifld.evidence_io import validate_evidence_directory, write_json_atomic
from dagster_hifld.resources import (
    PublishedStorageResource,
    StagingStorageResource,
    StorageObjectSnapshot,
)
from dagster_hifld.source_formats import CANONICAL_SOURCE_FORMAT_DIRS
from dagster_hifld.source_manifest import inventory_source_publisher, load_resolved_source_manifest
from dagster_hifld.provenance import provenance_projection

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
from .provenance import (attach_published_provenance, prepared_asset_dependencies,
                         verify_prepared_asset_dependencies, verify_prepared_metadata_dependencies,
                         verify_sqlite_asset_projection)
from .release import ReleasePointer, read_staged_release, require_current_baseline, semantic_version_key
from .thumbnail import render_geoparquet_thumbnail
from .validation import normalize_candidate_tree, validate_candidate_tree

DEFAULT_PUBLIC_ROOT = "http://localhost:8333/hifld-local-published"
CATALOG_KEY = "_catalog/catalog.sqlite"
RELEASE_POINTER_KEY = "_catalog/current.json"


def normalize_stac_datetime(value: object) -> str | None:
    """Normalize authored ISO dates to STAC-compatible RFC 3339 timestamps."""
    if value is None or value == "":
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Authored coverage timestamp must be an ISO date or datetime.")
    text = value.strip()
    if len(text) == 10:
        try:
            date.fromisoformat(text)
        except ValueError as error:
            raise ValueError("Authored coverage timestamp is not an ISO date.") from error
        return f"{text}T00:00:00Z"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as error:
        raise ValueError("Authored coverage timestamp is not an ISO datetime.") from error
    if parsed.tzinfo is None:
        raise ValueError("Authored coverage timestamp must include a timezone.")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


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


def validate_converted_columns(
    columns: tuple[ColumnRecord, ...], facts: GeoParquetFacts
) -> None:
    """Reject newly converted output missing attributes described by the catalog."""
    physical = {column.name for column in facts.columns}
    missing = sorted(
        {
            column.name
            for column in columns
            if not column.is_geometry and column.name not in physical
        }
    )
    if missing:
        raise ValueError(
            "Generated GeoParquet is missing catalog-described fields: "
            + ", ".join(missing)
        )
    if any(column.is_geometry for column in columns) and not any(
        column.name == facts.geometry_column and column.is_geometry
        for column in facts.columns
    ):
        raise ValueError("Generated GeoParquet is missing its primary geometry field.")


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
        relative = key.removeprefix(published.prefix.rstrip("/") + "/") if published.prefix else key
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
    write_immutable_object(
        published,
        f"{request.dataset_slug}/{request.file_slug}/{request.version}/styles/maplibre.json",
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
    """Convert/promote and run the guarded immutable publication path."""
    if not use_release_pointer:
        raise ValueError("Legacy in-place catalog publication is disabled; use an independently reviewed immutable release plan")
    staging_storage = staging or StagingStorageResource.from_env()
    published_base = published or PublishedStorageResource.from_env()
    record = _prepare_approved_portolan_record(request, staging_storage, published_base, catalog_only=catalog_only)
    request = replace(request, version=record.version_label)
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
            promote_immutable_objects(staging, published, [(key, key)])
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
            promote_immutable_objects(staging, published, [(key, key)])


def _promote_staged_data(
    staging: StagingStorageResource,
    published: PublishedStorageResource,
    request: PortolanPublishRequest,
) -> None:
    """Promote already-derived, generation-pinned data without rewriting bytes."""
    logical_prefix = f"{request.dataset_slug}/{request.file_slug}/{request.version}"
    pairs: list[tuple[str, str]] = []
    for key in staging.list_prefix(logical_prefix):
        relative = key.removeprefix(f"{request.collection_slug}/")
        parts = Path(relative).relative_to(logical_prefix).parts
        if len(parts) >= 2 and parts[0] in _PUBLISHED_DATA_FORMATS:
            pairs.append((relative, relative))
    promote_immutable_objects(staging, published, pairs, version_prefix=logical_prefix)


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
    # Validate authored modern claims before conversion or any promotion.
    provenance_projection(load_resolved_source_manifest(staging_storage, request.dataset_slug, request.file_slug, request.version).metadata)
    if convert:
        run_local_version_pipeline(
            staging_storage,
            published_data,
            request.dataset_slug,
            request.file_slug,
            request.version,
        )
    elif promote:
        _promote_staged_data(staging_storage, published_data, request)
    if promote:
        _promote_source_metadata(staging_storage, published_data, request)
    version_prefix = f"{request.dataset_slug}/{request.file_slug}/{request.version}"
    if convert or not published_data.object_exists(
        f"{version_prefix}/styles/maplibre.json"
    ):
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
        thumbnail = None
        if convert or not published_data.object_exists(
            f"{version_prefix}/thumbnail/thumbnail.png"
        ):
            thumbnail = render_geoparquet_thumbnail(
                local_version_dir, facts.geometry_column
            )
    if thumbnail is not None:
        write_immutable_object(
            published_data,
            f"{request.dataset_slug}/{request.file_slug}/{request.version}/thumbnail/thumbnail.png",
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
    columns = columns_from_dictionary(source_dictionary)
    if convert:
        validate_converted_columns(columns, facts)
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
    issued_value = _source_text(source_dictionary, "date_issued") or _source_text(
        source_manifest, "date_issued"
    )
    modified_value = (
        _source_text(source_dictionary, "date_modified")
        or _source_text(source_dictionary, "source_modified")
        or _source_text(source_manifest, "date_modified")
        or _source_text(source_manifest, "source_modified")
    )
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
    manifest_resolved_value = source_manifest.get("metadata_resolved_from")
    dictionary_resolved_value = source_dictionary.get("metadata_resolved_from")
    resolved_value = {
        **(
            manifest_resolved_value
            if isinstance(manifest_resolved_value, dict)
            else {}
        ),
        **(
            dictionary_resolved_value
            if isinstance(dictionary_resolved_value, dict)
            else {}
        ),
    }
    inventory_match_value = source_dictionary.get("inventory_match_type")
    record = CatalogRecord(
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
        columns=columns,
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
        source_issued_date=issued_value if isinstance(issued_value, str) else None,
        source_modified_date=(
            modified_value if isinstance(modified_value, str) else None
        ),
        temporal_start=normalize_stac_datetime(source_dictionary.get("temporal_start")),
        temporal_end=normalize_stac_datetime(source_dictionary.get("temporal_end")),
    )

    return attach_published_provenance(record, published_data, version_prefix, request.public_root)


def _source_publisher_from_storage(
    published: PublishedStorageResource, version_path: str
) -> str | None:
    """Read authored publisher evidence for a retained version Collection."""
    parts = version_path.split("/")
    if len(parts) != 4:
        raise ValueError(f"Invalid Portolan version path: {version_path}")
    collection, dataset, file_slug, version = parts
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
    if collection == "hifld":
        return inventory_source_publisher(dataset, file_slug, version)
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
    if not use_release_pointer:
        raise ValueError("Legacy in-place catalog publication is disabled; use an independently reviewed immutable release plan")
    return _publish_release_catalog(records, request, published)


def catalog_execution_binding(request: PortolanPublishRequest, staging: StagingStorageResource,
                              published: PublishedStorageResource) -> dict:
    return {"partition_key": f"{request.dataset_slug}/{request.file_slug}/{request.version}",
            "collection": request.collection_slug, "staging": _storage_identity(staging),
            "published": _storage_identity(published), "public_root": request.public_root.rstrip("/"),
            "storage_slug": request.storage_slug, "archive_public_domain": request.archive_public_domain}


def _plan_outside_storage(directory: Path, *stores: StagingStorageResource) -> None:
    for store in stores:
        if (store.use_local or not store.bucket) and directory.resolve().is_relative_to(Path(store.local_dir).resolve()):
            raise ValueError("Persist review plans outside staging and published storage")


def prepare_promoted_portolan_catalog(request: PortolanPublishRequest, staging: StagingStorageResource,
                                     published: PublishedStorageResource, plan_dir: Path) -> dict:
    """Read an already promoted modern version, render a persistent plan only.

    Unlike the legacy conversion helper this does not create styles/thumbnails,
    promote data or rewrite source metadata. Logical collections never silently
    replace the supplied physical storage prefix.
    """
    _plan_outside_storage(plan_dir, staging, published)
    document, prefix = read_staged_release(published, f"{request.dataset_slug}/{request.file_slug}/{request.version}")
    if document["id"] != f"{request.collection_slug}/{prefix}":
        raise ValueError("Promoted STAC identity differs from catalog selection")
    key = f"{prefix}/collection.json"
    approval = require_generated_object_approval(published, key, published.read_key(key))
    require_current_baseline(published, document, approval["baseline"])
    record = _catalog_record_from_storage(request, published)
    return prepare_portolan_release_bundle((record,), request, published, plan_dir,
                                          execution_binding=catalog_execution_binding(request, staging, published))


def _catalog_record_from_storage(request: PortolanPublishRequest, published: StagingStorageResource) -> CatalogRecord:
    """Inspect prepared bytes without conversion, promotion, or metadata writes."""
    version = f"{request.dataset_slug}/{request.file_slug}/{request.version}"
    dictionary = json.loads(published.read_key(version + "/metadata/data_dictionary.json"))
    quality = json.loads(published.read_key(version + "/metadata/quality_manifest.json"))
    if provenance_projection(dictionary) is None:
        raise ValueError("Persisted catalog preparation requires complete modern input provenance")
    with published.get_local_version_dir(request.dataset_slug, request.file_slug, request.version,
                                         include_directories=("geoparquet",)) as directory:
        facts = inspect_geoparquet(Path(directory))
    if (quality.get("quality_check_passed") is not True or quality.get("feature_count") != facts.feature_count
            or quality.get("invalid_geometry_count") != 0):
        raise ValueError("Promoted quality evidence does not describe a valid complete candidate")
    columns = columns_from_dictionary(dictionary)
    validate_converted_columns(columns, facts)
    record = CatalogRecord(
        request.collection_slug, request.dataset_slug, request.file_slug, request.version,
        request.title or dictionary.get("title") or request.file_slug,
        request.description or dictionary.get("description") or request.file_slug,
        "spatial", facts.feature_count, _record_assets(published, request),
        collection_title=request.collection_title, columns=columns, native_crs=facts.native_crs,
        geometry_column=facts.geometry_column, geometry_type=facts.geometry_type,
        feature_id_column=facts.feature_id_column, native_bbox=facts.native_bbox, crs84_bbox=facts.crs84_bbox,
        provider=dictionary.get("publisher") or request.provider, agency=dictionary.get("agency"), office=dictionary.get("office"),
        source_url=dictionary.get("source_url"), license_id=request.resolved_license[0], license_href=request.resolved_license[1],
        quality_passed=True, invalid_geometry_count=0, null_geometry_count=quality.get("null_geometry_count", 0))
    return attach_published_provenance(record, published, version, request.public_root)


def _prepare_approved_portolan_record(request: PortolanPublishRequest, staging: StagingStorageResource,
                                      published: PublishedStorageResource, *, catalog_only: bool) -> CatalogRecord:
    prefix = f"{request.dataset_slug}/{request.file_slug}/{request.version}"
    document, target = read_staged_release(published if catalog_only else staging, prefix)
    if document["id"] != f"{request.collection_slug}/{target}":
        raise ValueError("Staged STAC collection differs from selected catalog")
    if not catalog_only:
        _copy_version_files(staging, published, request.dataset_slug, request.file_slug, request.version)
    return _catalog_record_from_storage(replace(request, version=target.rsplit("/", 1)[1]), published)


def prepare_staged_release(request: PortolanPublishRequest, staging: StagingStorageResource,
                           published: PublishedStorageResource, *, staged_version: str) -> dict:
    """Prepare the standard STAC version and final URLs; never approve or publish."""
    semantic_version_key(request.version)
    source_prefix = f"{request.dataset_slug}/{request.file_slug}/{staged_version}"
    target_prefix = f"{request.dataset_slug}/{request.file_slug}/{request.version}"
    source_root = staging._ensure_prefixed(source_prefix)
    target_root = published._ensure_prefixed(target_prefix)
    # Styles are staged artifacts too. Regenerate their own data URLs when a
    # release version is overridden; provenance/custody evidence stays verbatim.
    def style_url(url: str) -> str:
        scheme = "pmtiles://" if url.startswith("pmtiles://") else ""
        bare = url.removeprefix(scheme) if scheme else url
        marker = f"/{request.dataset_slug}/{request.file_slug}/"
        if not bare.startswith(request.public_root.rstrip("/") + "/") or marker not in bare:
            return url
        tail = bare.split(marker, 1)[1].split("/", 1)
        return f"{scheme}{request.public_root.rstrip('/')}/{target_root}/{tail[1]}" if len(tail) == 2 else url

    for key in staging.list_prefix(f"{source_prefix}/styles"):
        if key.endswith(".json"):
            document = json.loads(staging.read_key(key))
            for style_source in document.get("sources", {}).values():
                if isinstance(style_source, dict) and isinstance(style_source.get("url"), str):
                    style_source["url"] = style_url(style_source["url"])
            staging.write_key(key, json.dumps(document, sort_keys=True, separators=(",", ":")).encode())
    record = _catalog_record_from_storage(replace(request, version=staged_version), staging)

    def final_asset(asset: AssetRecord) -> AssetRecord:
        key = asset.object_key
        if not key or not key.startswith(source_root + "/"):
            raise ValueError("Staged asset is outside the selected candidate")
        target = target_root + "/" + key.removeprefix(source_root + "/")
        return replace(asset, href=f"{request.public_root.rstrip('/')}/{target}", object_key=target,
                       storage_slug=request.storage_slug, storage_revision=None)

    record = replace(record, version_label=request.version, assets=tuple(map(final_asset, record.assets)),
                     metadata_assets=tuple(map(final_asset, record.metadata_assets)))
    with tempfile.TemporaryDirectory(prefix="hifld-staged-release-") as directory:
        root = Path(directory).resolve()
        render_portolan_tree(root, (record,), public_root=request.public_root)
        normalize_candidate_tree(root)
        validate_candidate_tree(root)
        for name in ("collection.json", "README.md", "AGENTS.md"):
            contents = (root / record.version_path / name).read_bytes()
            if name == "collection.json":
                document = json.loads(contents)
                # This is also a standalone Collection beside canonical data.
                # Discovery-tree parent/root links are assigned by the separately
                # reviewed catalog bundle, whose generation is not known yet.
                document["links"] = [link for link in document["links"] if link["rel"] not in {"root", "parent"}]
                filenames = {"self": "collection.json", "agents": "AGENTS.md", "describedby": "README.md"}
                for link in document["links"]:
                    if link["rel"] in filenames:
                        link["href"] = f"{request.public_root.rstrip('/')}/{target_root}/{filenames[link['rel']]}"
                contents = json.dumps(document, sort_keys=True, indent=2).encode() + b"\n"
            staging.write_key(f"{source_prefix}/{name}", contents)
    document, _ = read_staged_release(staging, source_prefix)
    return document


def apply_selected_portolan_catalog(request: PortolanPublishRequest, staging: StagingStorageResource,
                                   published: PublishedStorageResource, plan_dir: Path) -> str:
    """Apply a selected persisted plan without source conversion or rendering."""
    _plan_outside_storage(plan_dir, staging, published)
    validate_evidence_directory(plan_dir)
    plan = json.loads((plan_dir / "bundle_plan.json").read_text())
    selected = [{"collection": request.collection_slug, "dataset": request.dataset_slug,
                 "file": request.file_slug, "version": request.version}]
    if (plan.get("execution_binding") != catalog_execution_binding(request, staging, published)
            or plan.get("selected_versions") != selected or plan.get("public_root") != request.public_root.rstrip("/")):
        raise ValueError("Prepared catalog selection/partition/storage/public-root identity mismatch")
    path = plan_dir / "tree" / request.collection_slug / request.dataset_slug / request.file_slug / request.version / "collection.json"
    if not path.is_file():
        raise ValueError("Selected partition is absent from prepared STAC")
    return apply_portolan_release_bundle(plan_dir, published)


def prepare_portolan_release_bundle(
    records: tuple[CatalogRecord, ...], request: PortolanPublishRequest,
    published_base: PublishedStorageResource, plan_dir: Path, *, execution_binding: dict | None = None,
) -> dict:
    """Prepare exact immutable STAC/SQLite/pointer bytes without public writes.

    A new empty hardened local folder retains the generated UUID, timestamp,
    schemas and checksums for independent approval; apply never regenerates them.
    """
    _plan_outside_storage(plan_dir, published_base)
    validate_evidence_directory(plan_dir)
    if plan_dir.exists() and any(plan_dir.iterdir()):
        raise ValueError("Use a new empty catalog plan directory")
    plan_dir.mkdir(parents=True, exist_ok=True)
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
    root = plan_dir / "tree"
    root.mkdir()
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
    verify_sqlite_asset_projection(root)
    asset_dependencies = prepared_asset_dependencies(root, published, request.public_root)
    pending_documents = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative_path = path.relative_to(root).as_posix()
        pending_documents.append((f"{release_prefix}/{relative_path}", path.read_bytes()))
    database_bytes = database.read_bytes()
    pointer = ReleasePointer(
        generation=generation,
        catalog_key=f"{release_prefix}/{CATALOG_KEY}",
        root_key=f"{release_prefix}/catalog.json",
        sha256=hashlib.sha256(database_bytes).hexdigest(),
        size_bytes=len(database_bytes),
        published_at=_release_timestamp(),
    )
    pointer_file = plan_dir / "pointer.json"
    pointer_file.write_bytes(pointer.to_bytes())
    objects = [{"key": key, "path": "tree/" + key.removeprefix(release_prefix + "/"),
                "sha256": hashlib.sha256(contents).hexdigest(), "bytes": len(contents)} for key, contents in pending_documents]
    objects.append({"key": RELEASE_POINTER_KEY, "path": "pointer.json", "sha256": hashlib.sha256(pointer.to_bytes()).hexdigest(), "bytes": len(pointer.to_bytes())})
    metadata_keys = {asset.get("hifld:object_key") for path in root.rglob("collection.json")
                     for asset in json.loads(path.read_text()).get("assets", {}).values()
                     if "metadata" in asset.get("roles", [])}
    dependencies = {key: value for key, value in asset_dependencies.items() if key in metadata_keys}
    plan = {"schema_version": 1, "classification": "prepared bytes; no acceptance", "generation": generation,
            "destination": _storage_identity(published), "implementation_sha256": verifier_implementation_sha256(),
            "previous_pointer_snapshot": previous_pointer_snapshot.to_dict() if previous_pointer_snapshot else None,
            "objects": objects, "metadata_dependencies": dependencies,
            "asset_dependencies": asset_dependencies, "public_root": request.public_root.rstrip("/"),
            "selected_versions": [{"collection": record.collection_slug, "dataset": record.dataset_slug,
                                   "file": record.file_slug, "version": record.version_label} for record in records],
            "execution_binding": execution_binding}
    write_json_atomic(plan_dir / "bundle_plan.json", plan)
    return plan


def prepared_plan_approval(plan: dict) -> tuple[str, bytes]:
    """Exact review of canonical plan values; logical approval key, no write.

    Bind selection, dependency snapshots, destination and prior pointer as well
    as generated bytes. No parsed plan can grant or relabel its own approval.
    """
    return (f"_catalog/reviewed-plans/{plan['generation']}.json",
            json.dumps(plan, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode())


def apply_portolan_release_bundle(plan_dir: Path, published_base: PublishedStorageResource) -> str:
    """Preflight every prepared object, then conditionally select the bundle last."""
    _plan_outside_storage(plan_dir, published_base)
    validate_evidence_directory(plan_dir)
    plan = json.loads((plan_dir / "bundle_plan.json").read_text())
    published = published_base.model_copy(update={"prefix": ""})
    if (plan.get("schema_version") != 1 or plan.get("destination") != _storage_identity(published)
            or plan.get("implementation_sha256") != verifier_implementation_sha256()):
        raise ValueError("Catalog plan destination/implementation mismatch")
    if not (published.use_local or not published.bucket):
        raise ValueError("Dependency-guarded catalog apply is local-only; remote transactional publication is unsupported")
    objects = plan.get("objects")
    if not isinstance(objects, list) or not objects:
        raise ValueError("Missing exact prepared catalog inventory")
    selected = []
    keys, paths = set(), set()
    for descriptor in objects:
        key, relative = descriptor["key"], descriptor["path"]
        if (not isinstance(key, str) or not key or Path(key).is_absolute() or ".." in Path(key).parts
                or Path(key).as_posix() != key or "\\" in key):
            raise ValueError("Unsafe prepared publication object key")
        path = Path(relative)
        if path.is_absolute() or path.as_posix() != relative or any(part in {".", ".."} for part in path.parts) or relative in paths or key in keys:
            raise ValueError("Unsafe or duplicate prepared catalog object")
        path = plan_dir / path
        contents = path.read_bytes()
        if len(contents) != descriptor.get("bytes") or hashlib.sha256(contents).hexdigest() != descriptor.get("sha256"):
            raise ValueError("Prepared catalog bytes changed")
        if key != RELEASE_POINTER_KEY and not key.startswith(f"releases/{plan['generation']}/"):
            raise ValueError("Prepared object is outside immutable release")
        selected.append((key, contents))
        keys.add(key); paths.add(relative)
    actual = {path.relative_to(plan_dir).as_posix() for path in plan_dir.rglob("*") if path.is_file()}
    if actual != paths | {"bundle_plan.json"}:
        raise ValueError("Prepared catalog inventory has missing or extra files")
    pointer_pairs = [(key, contents) for key, contents in selected if key == RELEASE_POINTER_KEY]
    if len(pointer_pairs) != 1:
        raise ValueError("Prepared catalog requires one release pointer")
    pointer = ReleasePointer.parse(pointer_pairs[0][1])
    bodies = dict(selected)
    if (pointer.generation != plan["generation"] or pointer.catalog_key not in bodies or pointer.root_key not in bodies
            or hashlib.sha256(bodies[pointer.catalog_key]).hexdigest() != pointer.sha256 or len(bodies[pointer.catalog_key]) != pointer.size_bytes
            or _catalog_generation(bodies[pointer.catalog_key]) != pointer.generation):
        raise ValueError("Prepared pointer does not match the actual catalog")
    validate_candidate_tree(plan_dir / "tree")
    root_document = json.loads((plan_dir / "tree/catalog.json").read_text())
    self_links = [link.get("href") for link in root_document.get("links", []) if link.get("rel") == "self"]
    if self_links != [plan["public_root"] + f"/releases/{plan['generation']}/catalog.json"]:
        raise ValueError("Prepared public root differs from rendered catalog URL")
    verify_sqlite_asset_projection(plan_dir / "tree")
    current = published.object_snapshot(RELEASE_POINTER_KEY)
    if (current.to_dict() if current else None) != plan.get("previous_pointer_snapshot"):
        raise ValueError("Release pointer changed since planning")
    verify_prepared_metadata_dependencies(plan_dir / "tree", published, plan.get("metadata_dependencies", {}))
    verify_prepared_asset_dependencies(plan_dir / "tree", published, plan["public_root"], plan.get("asset_dependencies", {}))
    # Approval is external configuration. A plan can never approve itself.
    require_generated_object_approval(published, *prepared_plan_approval(plan))
    for key, contents in selected:
        require_generated_object_approval(published, key, contents)
        if key != RELEASE_POINTER_KEY and published.object_exists(key):
            raise ValueError("Prepared immutable release object already exists")
    written = []
    try:
        for key, contents in selected:
            if key != RELEASE_POINTER_KEY:
                written.append(published.write_key_if_unchanged(key, contents, None))
        # Dependencies can change after the first preflight. Check again inside
        # the pointer commit, after its temporary bytes have been fsynced.
        published.write_key_with_dependencies_if_unchanged(
            RELEASE_POINTER_KEY, pointer_pairs[0][1], current, plan["asset_dependencies"]
        )
    except BaseException:
        # These are only this attempt's immutable newly-created objects. Never
        # delete/replace accepted objects or a changed object owned by another
        # writer; retain the complete local plan for retry/review.
        # An interruption may arrive after the conditional pointer write has
        # committed. In that case keep the selected generation complete.
        if published.object_snapshot(RELEASE_POINTER_KEY) == current:
            for snapshot in reversed(written):
                published.delete_key_if_unchanged(snapshot.key, snapshot)
        raise
    return pointer.generation


def _publish_release_catalog(records: tuple[CatalogRecord, ...], request: PortolanPublishRequest,
                             published_base: PublishedStorageResource) -> str:
    with tempfile.TemporaryDirectory(prefix="hifld-portolan-release-") as temporary:
        directory = Path(temporary) / "plan"
        prepare_portolan_release_bundle(records, request, published_base, directory)
        return apply_portolan_release_bundle(directory, published_base)


def rollback_portolan_release(
    target_generation: str,
    *,
    published: PublishedStorageResource | None = None,
    expected_snapshot: StorageObjectSnapshot | None = None,
) -> ReleasePointer:
    """Fail closed until rollback has an equivalent independently reviewed plan."""
    raise ValueError("Legacy rollback is disabled; a separately reviewed persistent rollback plan with complete dependency verification is required")


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
        body = published.read_key(source_key)
        if previous_pointer is None and Path(relative_key).name == "collection.json":
            # A release descriptor copied beside canonical data is not a legacy
            # discovery tree when its logical collection root is elsewhere.
            identifier = json.loads(body).get("id", "")
            parent = Path(relative_key).parent.as_posix()
            if (isinstance(identifier, str) and len(identifier.split("/")) == 4
                    and identifier != parent and parent.endswith("/".join(identifier.split("/")[1:]))):
                continue
        destination = root / relative_key
        destination.parent.mkdir(parents=True, exist_ok=True)
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
    if not use_release_pointer:
        raise ValueError("Legacy in-place catalog publication is disabled; use an independently reviewed immutable release plan")
    if not requests:
        raise ValueError("Cannot publish an empty Portolan manifest.")
    staging = StagingStorageResource.from_env()
    published = PublishedStorageResource.from_env()
    records = tuple(
        _prepare_approved_portolan_record(
            request,
            staging,
            published,
            catalog_only=catalog_only,
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

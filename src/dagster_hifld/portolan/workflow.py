"""Bounded two-bucket Portolan publication workflow."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

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

DEFAULT_PUBLIC_ROOT = "http://localhost:8333/hifld-local-published"
CATALOG_KEY = "_catalog/catalog.sqlite"


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
        public_root = values.get("public_root", DEFAULT_PUBLIC_ROOT)
        storage_slug = values.get("storage_slug", "seaweedfs-local-published")
        if not isinstance(public_root, str) or not isinstance(storage_slug, str):
            raise TypeError("public_root and storage_slug must be strings.")
        provider = values.get("provider", values.get("publisher"))
        archive_public_domain = values.get("archive_public_domain", False)
        if not isinstance(archive_public_domain, bool):
            raise ValueError("archive_public_domain must be a boolean.")
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
    native_bbox = (
        tuple(float(value) for value in bbox_value)
        if isinstance(bbox_value, list) and len(bbox_value) == 4
        else None
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
                native_bbox = tuple(float(value) for value in bounds)
    effective_crs = crs_value if crs_value is not None else "OGC:CRS84"
    native_crs = json.dumps(effective_crs, sort_keys=True)
    crs84_bbox = None
    if native_bbox is not None:
        transformer = Transformer.from_crs(
            CRS.from_user_input(effective_crs), CRS.from_epsg(4326), always_xy=True
        )
        crs84_bbox = tuple(
            float(value)
            for value in transformer.transform_bounds(*native_bbox, densify_pts=21)
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
    ".gpkg": "application/geopackage+sqlite3",
    ".zip": "application/zip",
    ".json": "application/json",
}
_PUBLISHED_DATA_FORMATS = CANONICAL_SOURCE_FORMAT_DIRS | {"geoparquet", "pmtiles"}


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
        if len(asset_parts) < 2 or asset_parts[0] not in _PUBLISHED_DATA_FORMATS:
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
                raise RuntimeError(f"Published object changed during SHA-256 scan: {key}")
        format_key = asset_parts[0]
        asset_key = f"{format_key}-{hashlib.sha256(relative.encode()).hexdigest()[:12]}"
        assets.append(
            AssetRecord(
                key=asset_key,
                format_key=format_key,
                title=Path(key).name,
                href=f"{request.public_root}/{key}",
                media_type=_MEDIA_TYPES.get(
                    Path(key).suffix.lower(), "application/octet-stream"
                ),
                size_bytes=snapshot.size,
                sha256=sha256,
                storage_slug=request.storage_slug,
                storage_revision=snapshot.generation,
                object_key=key,
            )
        )
    return tuple(assets)


def publish_portolan_record(
    request: PortolanPublishRequest,
    *,
    staging: StagingStorageResource | None = None,
    published: PublishedStorageResource | None = None,
    catalog_only: bool = False,
) -> str:
    """Convert, promote, publish STAC, then atomically replace the catalog DB last."""
    staging_storage = staging or StagingStorageResource.from_env()
    published_base = published or PublishedStorageResource.from_env()
    record = _prepare_portolan_record(
        request, staging_storage, published_base, convert=not catalog_only
    )
    return _publish_catalog((record,), request, published_base)


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
        key = (
            f"{request.dataset_slug}/{request.file_slug}/{request.version}/"
            f"metadata/source/{filename}"
        )
        staging.copy_key_to(published, key, key)
    for key in (
        "metadata/source/collections.json",
        f"{request.dataset_slug}/metadata/source/source_manifest.json",
        f"{request.dataset_slug}/{request.file_slug}/metadata/source/source_manifest.json",
    ):
        if staging.object_exists(key):
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
            DatasetApiResource(enabled=False),
            request.dataset_slug,
            request.file_slug,
            request.version,
            request.storage_slug,
        )
    else:
        _promote_staged_data(staging_storage, published_data, request)
    _promote_source_metadata(staging_storage, published_data, request)
    with staging_storage.get_local_version_dir(
        request.dataset_slug, request.file_slug, request.version
    ) as version_dir:
        facts = inspect_geoparquet(Path(version_dir))
    quality_path = (
        "metadata/quality_manifest.json"
        if convert
        else "metadata/source/quality_manifest.json"
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
            "metadata/source/data_dictionary.json",
        )
    )
    if not isinstance(source_dictionary, dict):
        raise TypeError("Production data dictionary must be a JSON object.")
    source_quality = json.loads(
        staging_storage.read_bytes(
            request.dataset_slug,
            request.file_slug,
            request.version,
            "metadata/source/quality_manifest.json",
        )
    )
    if not isinstance(source_quality, dict):
        raise TypeError("Production quality manifest must be a JSON object.")
    source_manifest = json.loads(
        staging_storage.read_bytes(
            request.dataset_slug,
            request.file_slug,
            request.version,
            "metadata/source/source_manifest.json",
        )
    )
    if not isinstance(source_manifest, dict):
        raise TypeError("Production source manifest must be a JSON object.")
    dataset_manifest = _read_source_manifest(
        staging_storage, f"{request.dataset_slug}/metadata/source/source_manifest.json"
    )
    file_manifest = _read_source_manifest(
        staging_storage,
        f"{request.dataset_slug}/{request.file_slug}/metadata/source/source_manifest.json",
    )
    collections_value = json.loads(
        staging_storage.read_key("metadata/source/collections.json")
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
        collection_title=request.collection_title or _source_text(collection_manifest, "name"),
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
        quality_manifest_href="metadata/source/quality_manifest.json",
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


def _publish_catalog(
    records: tuple[CatalogRecord, ...],
    request: PortolanPublishRequest,
    published: PublishedStorageResource,
) -> str:
    # STAC hierarchy and SQLite are bucket-root objects; only data assets use
    # the collection-prefixed storage resource.
    published = published.model_copy(update={"prefix": ""})
    with tempfile.TemporaryDirectory(prefix="hifld-portolan-") as temporary:
        root = Path(temporary)
        render_portolan_tree(root, records, public_root=request.public_root)
        database = root / "catalog.sqlite"
        previous = published.object_snapshot(CATALOG_KEY)
        root_href = f"{request.public_root}/catalog.json"
        if previous is None:
            generation = build_catalog_sqlite(database, records, root_href=root_href)
        else:
            database.write_bytes(published.read_key(CATALOG_KEY))
            generation = update_catalog_sqlite(database, records)
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
                    if path.name == "catalog.json" and published.object_exists(relative_path):
                        previous_document = json.loads(published.read_key(relative_path))
                        document = _merge_catalog_children(previous_document, document)
                    body = (json.dumps(document, indent=2) + "\n").encode()
                published.write_key(relative_path, body)
        published.write_key_if_unchanged(CATALOG_KEY, database.read_bytes(), previous)
    return generation


def _merge_catalog_children(previous: object, current: dict[str, object]) -> dict[str, object]:
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
    if any(isinstance(link, dict) and link.get("rel") == "latest-version" for link in (*previous_links, *current_links)):
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
                {"rel": "latest-version", "href": href, "type": "application/json", "title": label}
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
) -> tuple[str, ...]:
    """Publish every record in one bounded fixture manifest."""
    if not requests:
        raise ValueError("Cannot publish an empty Portolan manifest.")
    staging = StagingStorageResource.from_env()
    published = PublishedStorageResource.from_env()
    records = tuple(
        _prepare_portolan_record(request, staging, published, convert=not catalog_only)
        for request in requests
    )
    generation = _publish_catalog(records, requests[0], published)
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

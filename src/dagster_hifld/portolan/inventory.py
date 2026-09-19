"""Build a Portolan catalog from object inventories without reading data assets."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from urllib.parse import quote

import httpx
from pyproj import CRS, Transformer
from pyproj.exceptions import CRSError, ProjError

from .catalog import (
    HIFLD_ARCHIVE_LICENSE_HREF,
    HIFLD_ARCHIVE_PUBLIC_DOMAIN_MARK,
    AssetRecord,
    CatalogRecord,
)
from .workflow import columns_from_dictionary, manifest_tags, normalize_stac_datetime

_METADATA_FILENAMES = (
    "metadata/source_manifest.json",
    "metadata/data_dictionary.json",
    "metadata/quality_manifest.json",
)
_MEDIA_TYPES = {
    ".parquet": "application/vnd.apache.parquet",
    ".pmtiles": "application/vnd.pmtiles",
    ".gpkg": "application/geopackage+sqlite3",
    ".geojson": "application/geo+json",
    ".json": "application/json",
    ".zip": "application/zip",
}


@dataclass(frozen=True)
class InventoryObject:
    name: str
    generation: str
    size: int
    md5_hash: str | None
    content_type: str | None
    updated: str | None


@dataclass(frozen=True)
class InventoryBuildReport:
    version_count: int
    asset_count: int
    nonspatial_version_count: int
    multipart_geoparquet_version_count: int


@dataclass(frozen=True)
class InventoryCatalog:
    records: tuple[CatalogRecord, ...]
    report: InventoryBuildReport


SourceMetadataLoader = Callable[[InventoryObject], Mapping[str, object]]


class GenerationPinnedMetadataLoader:
    """Load and verify only small JSON production metadata objects."""

    def __init__(self, source_bucket: str, cache_dir: Path) -> None:
        self._source_bucket = source_bucket
        self._client = httpx.Client(timeout=30.0, follow_redirects=True)
        self._cache: dict[str, Mapping[str, object]] = {}
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def __call__(self, item: InventoryObject) -> Mapping[str, object]:
        cached = self._cache.get(item.name)
        if cached is not None:
            return cached
        cache_path = self._cache_path(item)
        if cache_path.is_file():
            decoded = json.loads(cache_path.read_text(encoding="utf-8"))
            if not isinstance(decoded, dict):
                raise TypeError(f"Cached metadata must be an object: {item.name}")
            self._cache[item.name] = decoded
            return decoded
        if not item.name.endswith(".json"):
            raise ValueError(f"Only JSON metadata may be fetched: {item.name}")
        url = (
            f"https://storage.googleapis.com/{self._source_bucket}/"
            f"{quote(item.name, safe='/')}?generation={quote(item.generation, safe='')}"
        )
        response = self._client.get(url)
        response.raise_for_status()
        content = response.content
        if item.md5_hash is not None:
            actual = base64.b64encode(hashlib.md5(content).digest()).decode("ascii")
            if actual != item.md5_hash:
                raise ValueError(
                    f"Production metadata MD5 does not match inventory: {item.name}"
                )
        decoded = json.loads(content)
        if not isinstance(decoded, dict):
            raise TypeError(f"Production metadata must be an object: {item.name}")
        self._cache[item.name] = decoded
        cache_path.write_text(json.dumps(decoded, sort_keys=True), encoding="utf-8")
        return decoded

    def prefetch(self, items: tuple[InventoryObject, ...]) -> None:
        with ThreadPoolExecutor(max_workers=8) as executor:
            tuple(executor.map(self, items))

    def close(self) -> None:
        self._client.close()

    def _cache_path(self, item: InventoryObject) -> Path:
        identity = f"{item.name}\0{item.generation}\0{item.md5_hash or ''}"
        return self._cache_dir / f"{hashlib.sha256(identity.encode()).hexdigest()}.json"


def build_catalog_from_inventories(
    source_inventory_path: Path,
    published_inventory_path: Path,
    parquet_footer_path: Path,
    *,
    source_metadata: SourceMetadataLoader,
    published_root: str,
    collection_slug: str = "hifld",
    published_prefix: str = "hifld",
    collection_title: str | None = "HIFLD Next",
    collection_description: str = "",
    collection_created_at: str | None = None,
    collection_updated_at: str | None = None,
    dataset_timestamps: Mapping[str, tuple[str | None, str | None]] | None = None,
) -> InventoryCatalog:
    """Create typed catalog records from exact source and copied-object inventories.

    The target inventory supplies storage generations; source JSON is fetched by its
    original generation. No data object is opened or hashed by this builder.
    """
    source = _load_inventory(source_inventory_path)
    published = _load_inventory(published_inventory_path)
    target_by_source_name = _target_objects(published, published_prefix)
    footers = _load_footer_facts(parquet_footer_path)
    source_by_name = {item.name: item for item in source}
    if isinstance(source_metadata, GenerationPinnedMetadataLoader):
        source_metadata.prefetch(
            tuple(item for item in source if item.name.endswith(".json"))
        )
    records: list[CatalogRecord] = []
    multipart_geoparquet = 0
    for identity in _version_identities(source):
        dataset_slug, file_slug, version_label = identity
        prefix = f"{dataset_slug}/{file_slug}/{version_label}/"
        source_assets = tuple(
            item
            for item in source
            if item.name.startswith(prefix) and not "/metadata/" in item.name
        )
        if not source_assets:
            continue
        target_assets = tuple(
            _validated_target_asset(item, target_by_source_name)
            for item in source_assets
        )
        documents = _load_version_documents(
            source_by_name, source_metadata, dataset_slug, file_slug, version_label
        )
        version_footer = tuple(
            footers[item.name] for item in source_assets if item.name in footers
        )
        geoparquet_footers = tuple(
            fact for fact in version_footer if fact["format_key"] == "geoparquet"
        )
        if len(geoparquet_footers) > 1:
            multipart_geoparquet += 1
        records.append(
            _catalog_record(
                collection_slug,
                dataset_slug,
                file_slug,
                version_label,
                target_assets,
                documents,
                version_footer,
                published_root,
                published_prefix,
                collection_title,
                collection_description,
                collection_created_at,
                collection_updated_at,
                dataset_timestamps.get(dataset_slug, (None, None))
                if dataset_timestamps is not None
                else (None, None),
            )
        )
    ordered = tuple(sorted(records, key=lambda record: record.version_path))
    return InventoryCatalog(
        ordered,
        InventoryBuildReport(
            version_count=len(ordered),
            asset_count=sum(len(record.assets) for record in ordered),
            nonspatial_version_count=sum(
                record.spatial_status == "non_spatial_source" for record in ordered
            ),
            multipart_geoparquet_version_count=multipart_geoparquet,
        ),
    )


def _load_inventory(path: Path) -> tuple[InventoryObject, ...]:
    text = path.read_text(encoding="utf-8")
    decoded = json.loads(text)
    if not isinstance(decoded, list):
        raise TypeError(f"Inventory must be an array: {path}")
    return tuple(_inventory_object(value) for value in decoded)


def inventory_from_copy_report(path: Path) -> tuple[InventoryObject, ...]:
    """Read copied target metadata from the JSONL report emitted by the copy spike."""
    objects: list[InventoryObject] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        if not isinstance(entry, dict) or entry.get("status") != "copied":
            continue
        metadata = entry.get("metadata")
        if not isinstance(metadata, dict):
            raise TypeError("Copied object report entries require metadata objects.")
        objects.append(_inventory_object(metadata))
    return tuple(objects)


def _inventory_object(value: object) -> InventoryObject:
    if not isinstance(value, dict):
        raise TypeError("Inventory entries must be objects.")
    name = value.get("name")
    generation = value.get("generation")
    size = value.get("size")
    if not isinstance(name, str) or not isinstance(generation, str):
        raise TypeError("Inventory entries require string name and generation.")
    if isinstance(size, str) and size.isdigit():
        parsed_size = int(size)
    elif isinstance(size, int) and size >= 0:
        parsed_size = size
    else:
        raise TypeError(f"Inventory entry has invalid size: {name}")
    md5_hash = value.get("md5Hash")
    content_type = value.get("contentType")
    updated = value.get("updated")
    return InventoryObject(
        name,
        generation,
        parsed_size,
        md5_hash if isinstance(md5_hash, str) else None,
        content_type if isinstance(content_type, str) else None,
        updated if isinstance(updated, str) else None,
    )


def _target_objects(
    published: tuple[InventoryObject, ...], published_prefix: str
) -> dict[str, InventoryObject]:
    prefix = f"{published_prefix.strip('/')}/"
    result: dict[str, InventoryObject] = {}
    for item in published:
        if item.name.startswith(prefix):
            result[item.name.removeprefix(prefix)] = item
    return result


def _version_identities(
    source: tuple[InventoryObject, ...],
) -> tuple[tuple[str, str, str], ...]:
    identities: set[tuple[str, str, str]] = set()
    for item in source:
        parts = item.name.split("/")
        if len(parts) >= 5 and parts[2].startswith("v"):
            identities.add((parts[0], parts[1], parts[2]))
    return tuple(sorted(identities))


def _validated_target_asset(
    source: InventoryObject, targets: Mapping[str, InventoryObject]
) -> InventoryObject:
    target = targets.get(source.name)
    if target is None:
        raise ValueError(f"Copied target object is absent: {source.name}")
    if target.size != source.size:
        raise ValueError(f"Copied target object size differs: {source.name}")
    if source.md5_hash is not None and target.md5_hash != source.md5_hash:
        raise ValueError(f"Copied target object MD5 differs: {source.name}")
    return replace(source, generation=target.generation)


def _load_version_documents(
    source: Mapping[str, InventoryObject],
    loader: SourceMetadataLoader,
    dataset_slug: str,
    file_slug: str,
    version_label: str,
) -> dict[str, Mapping[str, object]]:
    keys = {
        "dataset": f"{dataset_slug}/metadata/source_manifest.json",
        "file": f"{dataset_slug}/{file_slug}/metadata/source_manifest.json",
        "version": f"{dataset_slug}/{file_slug}/{version_label}/metadata/source_manifest.json",
        "dictionary": f"{dataset_slug}/{file_slug}/{version_label}/metadata/data_dictionary.json",
        "quality": f"{dataset_slug}/{file_slug}/{version_label}/metadata/quality_manifest.json",
    }
    documents: dict[str, Mapping[str, object]] = {}
    for role, key in keys.items():
        item = source.get(key)
        if item is None and role == "version":
            documents[role] = {}
            continue
        if item is None:
            raise ValueError(
                f"Required production metadata is absent from inventory: {key}"
            )
        documents[role] = loader(item)
    return documents


def _load_footer_facts(path: Path) -> dict[str, dict[str, object]]:
    decoded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(decoded, list):
        raise TypeError("Parquet footer spike must be an array.")
    result: dict[str, dict[str, object]] = {}
    for value in decoded:
        if not isinstance(value, dict):
            raise TypeError("Parquet footer entries must be objects.")
        key = value.get("key")
        if not isinstance(key, str):
            raise TypeError("Parquet footer entries require key.")
        result[key] = {**value, "format_key": "geoparquet"}
    return result


def _catalog_record(
    collection_slug: str,
    dataset_slug: str,
    file_slug: str,
    version_label: str,
    assets: tuple[InventoryObject, ...],
    documents: Mapping[str, Mapping[str, object]],
    facts: tuple[Mapping[str, object], ...],
    published_root: str,
    published_prefix: str,
    collection_title: str | None,
    collection_description: str,
    collection_created_at: str | None,
    collection_updated_at: str | None,
    dataset_timestamps: tuple[str | None, str | None],
) -> CatalogRecord:
    version = documents["version"]
    dictionary = documents["dictionary"]
    quality = documents["quality"]
    source_tags = version.get("tags")
    dictionary_title = _text(dictionary, "title")
    title = (
        dictionary_title or _text(version, "title") or _text(documents["file"], "title")
    )
    description = _text(dictionary, "description") or _text(version, "description")
    geoparquet = tuple(fact for fact in facts if fact["format_key"] == "geoparquet")
    primary = _consistent_text(geoparquet, "primary_column")
    crs = _consistent_value(geoparquet, "crs")
    bbox = _consistent_bbox(geoparquet)
    feature_count = sum(
        value for fact in geoparquet if isinstance((value := fact.get("rows")), int)
    )
    if not geoparquet:
        feature_count = _integer(
            quality, "feature_count", _integer(dictionary, "feature_count", 0)
        )
    columns = columns_from_dictionary(dictionary)
    return CatalogRecord(
        collection_slug=collection_slug,
        dataset_slug=dataset_slug,
        file_slug=file_slug,
        version_label=version_label,
        title=title,
        description=description,
        spatial_status="spatial" if primary is not None else "non_spatial_source",
        feature_count=feature_count,
        assets=tuple(
            _asset_record(item, published_root, published_prefix) for item in assets
        ),
        collection_title=collection_title,
        collection_description=collection_description,
        collection_created_at=collection_created_at,
        collection_updated_at=collection_updated_at,
        dataset_title=_text(documents["dataset"], "title"),
        dataset_description=_text(documents["dataset"], "description"),
        dataset_created_at=dataset_timestamps[0],
        dataset_updated_at=dataset_timestamps[1],
        dataset_tags=manifest_tags(documents["dataset"]),
        file_tags=manifest_tags(documents["file"]),
        tags=_tag_values(source_tags, "categories"),
        columns=columns,
        native_crs=json.dumps(crs, sort_keys=True) if crs is not None else None,
        geometry_column=primary,
        geometry_type=_single_geometry_type(geoparquet),
        native_bbox=bbox,
        crs84_bbox=_to_crs84_bbox(bbox, crs),
        source_version_description=_optional_text(quality, "description"),
        source_version_bounds=_bounds(quality.get("bounds")),
        quality_manifest_href="metadata/quality_manifest.json",
        quality_passed=_bool(quality, "quality_check_passed", False),
        invalid_geometry_count=_integer(quality, "invalid_geometry_count", 0),
        null_geometry_count=_integer(quality, "null_geometry_count", 0),
        source_columns_hash=_text(quality, "columns_hash") or None,
        quality_provenance="production_metadata_and_parquet_footer_spike",
        license_id=(
            HIFLD_ARCHIVE_PUBLIC_DOMAIN_MARK if collection_slug == "hifld" else "other"
        ),
        license_href=(
            HIFLD_ARCHIVE_LICENSE_HREF if collection_slug == "hifld" else None
        ),
        provider=_text(dictionary, "publisher") or _text(version, "publisher") or None,
        agency=_text(dictionary, "agency") or _text(version, "agency") or None,
        office=_text(dictionary, "office") or _text(version, "office") or None,
        source_url=_text(dictionary, "source_url")
        or _text(version, "source_url")
        or None,
        manifest_role=_text(version, "manifest_role") or None,
        manifest_keys=_string_tuple(version.get("manifest_keys")),
        created_at=normalize_stac_datetime(dictionary.get("date_issued")),
        updated_at=normalize_stac_datetime(dictionary.get("date_modified")),
    )


def _asset_record(
    item: InventoryObject, published_root: str, published_prefix: str
) -> AssetRecord:
    parts = item.name.split("/")
    format_key = parts[3]
    suffix = Path(item.name).suffix.lower()
    return AssetRecord(
        key=f"{format_key}-{hashlib.sha256(item.name.encode()).hexdigest()[:12]}",
        format_key=format_key,
        title=Path(item.name).name,
        href=f"{published_root.rstrip('/')}/{published_prefix.strip('/')}/{item.name}",
        media_type=_MEDIA_TYPES.get(
            suffix, item.content_type or "application/octet-stream"
        ),
        size_bytes=item.size,
        sha256=None,
        checksum_multihash=_md5_multihash(item.md5_hash),
        storage_slug="gcp-portolan-published",
        storage_revision=item.generation,
        object_key=f"{published_prefix.strip('/')}/{item.name}",
    )


def _md5_multihash(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        digest = base64.b64decode(value, validate=True)
    except ValueError as error:
        raise ValueError("Inventory contains an invalid GCS md5Hash.") from error
    if len(digest) != 16:
        raise ValueError("Inventory contains a non-MD5 md5Hash.")
    return f"d50110{digest.hex()}"


def _text(value: Mapping[str, object], key: str) -> str:
    candidate = value.get(key)
    return candidate.strip() if isinstance(candidate, str) else ""


def _optional_text(value: Mapping[str, object], key: str) -> str | None:
    candidate = value.get(key)
    return candidate if isinstance(candidate, str) else None


def _bounds(value: object) -> tuple[float, float, float, float] | None:
    if not isinstance(value, list) or len(value) != 4:
        return None
    if not all(
        isinstance(coordinate, (int, float)) and not isinstance(coordinate, bool)
        for coordinate in value
    ):
        return None
    return (float(value[0]), float(value[1]), float(value[2]), float(value[3]))


def _integer(value: Mapping[str, object], key: str, default: int) -> int:
    candidate = value.get(key)
    return candidate if isinstance(candidate, int) and candidate >= 0 else default


def _bool(value: Mapping[str, object], key: str, default: bool) -> bool:
    candidate = value.get(key)
    return candidate if isinstance(candidate, bool) else default


def _string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _tag_values(value: object, key: str) -> tuple[str, ...]:
    if not isinstance(value, dict):
        return ()
    return _string_tuple(value.get(key))


def _consistent_text(facts: tuple[Mapping[str, object], ...], key: str) -> str | None:
    values = {value for fact in facts if isinstance((value := fact.get(key)), str)}
    return values.pop() if len(values) == 1 else None


def _consistent_value(
    facts: tuple[Mapping[str, object], ...], key: str
) -> str | dict[str, object] | None:
    values = [fact.get(key) for fact in facts if fact.get(key) is not None]
    serialized = {json.dumps(value, sort_keys=True) for value in values}
    if len(serialized) != 1 or not values:
        return None
    value = values[0]
    return value if isinstance(value, (str, dict)) else None


def _consistent_bbox(
    facts: tuple[Mapping[str, object], ...],
) -> tuple[float, float, float, float] | None:
    values = [fact.get("bbox") for fact in facts]
    if not values or not all(
        isinstance(value, list) and len(value) == 4 for value in values
    ):
        return None
    if not all(
        isinstance(coordinate, (int, float)) for value in values for coordinate in value
    ):
        return None
    return (
        min(float(value[0]) for value in values),
        min(float(value[1]) for value in values),
        max(float(value[2]) for value in values),
        max(float(value[3]) for value in values),
    )


def _to_crs84_bbox(
    bbox: tuple[float, float, float, float] | None,
    crs: str | dict[str, object] | None,
) -> tuple[float, float, float, float] | None:
    if bbox is None:
        return None
    effective_crs = crs if crs is not None else "OGC:CRS84"
    try:
        transformer = Transformer.from_crs(
            CRS.from_user_input(effective_crs), CRS.from_epsg(4326), always_xy=True
        )
        return tuple(
            float(value)
            for value in transformer.transform_bounds(*bbox, densify_pts=21)
        )
    except (CRSError, ProjError, ValueError):
        return None


def _single_geometry_type(facts: tuple[Mapping[str, object], ...]) -> str | None:
    values: set[str] = set()
    for fact in facts:
        geometry_types = fact.get("geometry_types")
        if isinstance(geometry_types, list):
            values.update(value for value in geometry_types if isinstance(value, str))
    return values.pop() if len(values) == 1 else None

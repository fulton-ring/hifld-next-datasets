"""Source metadata manifest resolution for staged dataset versions."""

from __future__ import annotations

import ast
import csv
import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from dagster_hifld.resources import StagingStorageResource

CATALOG_METADATA_FIELDS = (
    "title",
    "description",
    "publisher",
    "agency",
    "office",
    "keywords",
    "source_url",
    "license",
    "source_modified",
    "date_issued",
    "date_modified",
    "temporal_start",
    "temporal_end",
)


@dataclass(frozen=True)
class ResolvedSourceManifest:
    metadata: dict[str, Any]
    manifest_keys: list[str]


def load_resolved_source_manifest(
    staging: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> ResolvedSourceManifest:
    layers: list[tuple[str, str, dict[str, Any]]] = []
    for source_name, key in _manifest_lookup_keys(dataset_slug, file_slug, version):
        raw = _read_optional_key(staging, key)
        if raw is None:
            continue
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid source manifest JSON at {key}: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"Source manifest at {key} must be a JSON object.")
        layers.append((source_name, key, _compact_manifest(parsed)))

    inventory = _inventory_metadata_for(dataset_slug, file_slug, version)
    if layers:
        resolved = _merge_manifest_layers(layers)
        if inventory is None:
            return resolved
        metadata = dict(inventory)
        metadata.update(
            {
                field: value
                for field, value in resolved.metadata.items()
                if field
                not in {"metadata_sources", "metadata_resolved_from", "manifest_keys"}
            }
        )
        metadata["metadata_sources"] = [
            "inventory",
            *resolved.metadata["metadata_sources"],
        ]
        metadata["metadata_resolved_from"] = {
            **inventory["metadata_resolved_from"],
            **resolved.metadata["metadata_resolved_from"],
        }
        metadata["manifest_keys"] = resolved.manifest_keys
        return ResolvedSourceManifest(metadata, resolved.manifest_keys)

    if inventory:
        return ResolvedSourceManifest(inventory, [])

    generated = {
        "title": file_slug,
        "description": f"Staged dataset file {dataset_slug}/{file_slug}.",
        "metadata_sources": ["generated"],
        "metadata_resolved_from": {
            "title": "generated",
            "description": "generated",
        },
        "manifest_keys": [],
    }
    return ResolvedSourceManifest(generated, [])


def _manifest_lookup_keys(
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> list[tuple[str, str]]:
    return [
        ("dataset", f"{dataset_slug}/metadata/source_manifest.json"),
        ("file", f"{dataset_slug}/{file_slug}/metadata/source_manifest.json"),
        (
            "version",
            f"{dataset_slug}/{file_slug}/{version}/metadata/source_manifest.json",
        ),
    ]


def snapshot_source_metadata(
    staging: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> None:
    """Pin authored metadata before catalog generation replaces version files."""
    version_root = f"{dataset_slug}/{file_slug}/{version}"
    keys = [
        f"{dataset_slug}/metadata/source_manifest.json",
        f"{dataset_slug}/{file_slug}/metadata/source_manifest.json",
        f"{version_root}/metadata/source_manifest.json",
        f"{version_root}/metadata/data_dictionary.json",
        f"{version_root}/metadata/quality_manifest.json",
    ]
    for key in keys:
        destination = str(Path(key).parent / "source" / Path(key).name)
        if staging.object_exists(destination) or not staging.object_exists(key):
            continue
        staging.write_key_if_unchanged(destination, staging.read_key(key), None)


def _read_optional_key(staging: StagingStorageResource, key: str) -> bytes | None:
    key = staging._ensure_prefixed(key)
    if staging._uses_s3():
        return staging.read_key(key) if staging.object_exists(key) else None
    if staging.use_local or not staging.bucket:
        full = Path(staging.local_dir).resolve() / key
        return full.read_bytes() if full.is_file() else None

    import gcsfs

    fs = gcsfs.GCSFileSystem()
    path = f"{staging.bucket}/{key}"
    return fs.read_bytes(path) if fs.exists(path) else None


def _compact_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value for key, value in manifest.items() if value not in (None, "", [], {})
    }


def _merge_manifest_layers(
    layers: list[tuple[str, str, dict[str, Any]]],
) -> ResolvedSourceManifest:
    merged: dict[str, Any] = {}
    resolved_from: dict[str, str] = {}
    sources: list[str] = []
    manifest_keys: list[str] = []

    for source_name, key, metadata in layers:
        sources.append(source_name)
        manifest_keys.append(key)
        for field, value in metadata.items():
            if field in {"metadata_sources", "metadata_resolved_from", "manifest_keys"}:
                continue
            merged[field] = value
            resolved_from[field] = source_name

    merged["metadata_sources"] = sources
    merged["metadata_resolved_from"] = resolved_from
    merged["manifest_keys"] = manifest_keys
    return ResolvedSourceManifest(merged, manifest_keys)


def _inventory_metadata_for(
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> dict[str, Any] | None:
    if version != "v1.0.0":
        return None
    match = _lookup_inventory_row(dataset_slug, file_slug)
    if not match:
        return None
    row, match_type = match
    title = row.get("title") or file_slug
    if match_type == "parent_family" and file_slug != _slug(row.get("filename", "")):
        title = f"{title} - {file_slug}"
    metadata = {
        "title": title,
        "description": row.get("description"),
        "publisher": row.get("publisher") or row.get("agency"),
        "agency": row.get("agency"),
        "office": row.get("office"),
        "keywords": _parse_keywords(row.get("keywords")),
        "source_url": row.get("Download Location"),
        "date_issued": row.get("date_issued"),
        "date_modified": row.get("date_modified"),
        "metadata_sources": ["inventory"],
        "metadata_resolved_from": {},
        "inventory_match_type": match_type,
        "manifest_keys": [],
    }
    metadata = _compact_manifest(metadata)
    metadata["metadata_resolved_from"] = {
        field: "inventory"
        for field in metadata
        if field not in {"metadata_sources", "metadata_resolved_from", "manifest_keys"}
    }
    metadata["manifest_keys"] = []
    return metadata


def inventory_source_publisher(
    dataset_slug: str, file_slug: str, version: str
) -> str | None:
    """Return authored inventory source evidence for an original v1.0.0 layer."""
    metadata = _inventory_metadata_for(dataset_slug, file_slug, version)
    publisher = metadata.get("publisher") if metadata is not None else None
    return (
        publisher.strip() if isinstance(publisher, str) and publisher.strip() else None
    )


def _lookup_inventory_row(
    dataset_slug: str,
    file_slug: str,
) -> tuple[dict[str, str], str] | None:
    rows = _inventory_rows()
    normalized_pair = f"{dataset_slug}/{file_slug}"
    for row in rows:
        if _normalize_inventory_path(row.get("path", "")) == normalized_pair:
            return row, "exact_path"

    for row in rows:
        filename = _slug(row.get("filename", ""))
        path_root = _normalize_inventory_path(row.get("path", "")).split("/", 1)[0]
        if filename == file_slug and path_root in {dataset_slug, file_slug}:
            return row, "filename_same_root"

    for row in rows:
        path = _normalize_inventory_path(row.get("path", ""))
        filename = _slug(row.get("filename", ""))
        if path == dataset_slug or filename == dataset_slug:
            return row, "parent_family"

    normalized_file = _strip_format_words(file_slug)
    for row in rows:
        filename = _strip_format_words(_slug(row.get("filename", "")))
        path = _normalize_inventory_path(row.get("path", ""))
        if filename == normalized_file and path.startswith(dataset_slug):
            return row, "alias"

    unscoped = [
        row
        for row in rows
        if _normalize_inventory_path(row.get("path", "")) in {"", "done"}
        and _slug(row.get("filename", "")) == file_slug
    ]
    if len(unscoped) == 1:
        return unscoped[0], "unscoped_filename"
    return None


@lru_cache(maxsize=1)
def _inventory_rows() -> tuple[dict[str, str], ...]:
    path = Path(
        os.environ.get(
            "HIFLD_INVENTORY_PATH",
            Path(__file__).resolve().parents[2] / "HIFLD_Open_Inventory_12112025.csv",
        )
    )
    if not path.is_file():
        return ()
    with path.open(newline="", encoding="utf-8-sig") as f:
        return tuple(csv.DictReader(f))


def _normalize_inventory_path(value: str) -> str:
    normalized = value.strip().replace("\\", "/").strip("./").strip("/")
    return "/".join(_slug(part) for part in normalized.split("/") if part)


def _slug(value: str) -> str:
    return value.strip().lower().replace("_", "-")


def _strip_format_words(value: str) -> str:
    return re.sub(
        r"-(geopackage|shapefile|geojson|file-geodatabase|file_geodatabase)(-.*)?$",
        "",
        value,
    )


def _parse_keywords(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = ast.literal_eval(value)
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item)]
    except Exception:
        pass
    return [part.strip() for part in value.split(",") if part.strip()]

"""Bind rendered multi-input provenance to actual downloadable metadata bytes."""
from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from dagster_hifld.provenance import provenance_projection, validate_provenance, validate_provenance_url
from dagster_hifld.resources import PublishedStorageResource

from .catalog import AssetRecord, CatalogRecord

_REQUIRED = {"source_manifest.json", "resolved_provenance.json", "data_dictionary.json", "quality_manifest.json"}


def attach_published_provenance(record: CatalogRecord, published: PublishedStorageResource,
                                version_prefix: str, public_root: str) -> CatalogRecord:
    """Read canonical metadata, validate full input propagation and pin each asset.

    Older archive records without modern input provenance retain their existing
    scope. Modern claims require the complete resolved manifest and dictionary.
    Original successful-input request/final URLs and captured notices are copied
    verbatim; failed diagnostic attempts are not reclassified as source inputs.
    """
    manifest_key = f"{version_prefix}/metadata/source_manifest.json"
    if not published.object_exists(manifest_key):
        if record.source_provenance is not None:
            raise ValueError("Missing downloadable source provenance")
        return record
    manifest = json.loads(published.read_key(manifest_key))
    authored = provenance_projection(manifest)
    if authored is None:
        return record
    resolved_key = f"{version_prefix}/metadata/resolved_provenance.json"
    dictionary_key = f"{version_prefix}/metadata/data_dictionary.json"
    if not published.object_exists(resolved_key) or not published.object_exists(dictionary_key):
        raise ValueError("Missing downloadable complete input provenance")
    resolved = json.loads(published.read_key(resolved_key))
    validate_provenance(resolved)
    dictionary = json.loads(published.read_key(dictionary_key))
    if provenance_projection(dictionary) != resolved:
        raise ValueError("Published dictionary lost or changed input provenance")
    # Authored version inputs can be augmented by dataset/file manifest inputs;
    # conflicts cannot be hidden by the resolved projection.
    indexed = {item["input_id"]: item for item in resolved["inputs"]}
    if any(indexed.get(item["input_id"]) != item for item in authored["inputs"]):
        raise ValueError("Published resolved provenance conflicts with authored source inputs")
    for layer in resolved.get("manifest_layers", []):
        snapshot_name = layer.get("snapshot", "")
        if (not isinstance(snapshot_name, str) or not snapshot_name.startswith("metadata/source/")
                or ".." in Path(snapshot_name).parts or published.sha256_key(f"{version_prefix}/{snapshot_name}") != layer.get("sha256")):
            raise ValueError("Published authored manifest layer snapshot is missing or changed")
    assets = []
    names = set()
    for key in published.list_prefix(f"{version_prefix}/metadata"):
        relative = key.removeprefix(published.prefix.rstrip("/") + "/") if published.prefix else key
        logical = Path(relative).relative_to(version_prefix).as_posix()
        snapshot = published.object_snapshot(key)
        if snapshot is None:
            raise ValueError("Published metadata disappeared")
        digest = published.sha256_key(key)
        if published.object_snapshot(key) != snapshot:
            raise ValueError("Published metadata changed during provenance scan")
        names.add(logical.removeprefix("metadata/"))
        assets.append(AssetRecord(key="metadata-" + logical.replace("/", "--"), format_key="metadata",
                                  title=logical, href=f"{public_root.rstrip('/')}/{published._ensure_prefixed(relative)}",
                                  media_type="application/json" if logical.endswith(".json") else "application/x-ndjson" if logical.endswith(".jsonl") else "text/markdown" if logical.endswith(".md") else "application/octet-stream",
                                  size_bytes=snapshot.size, sha256=digest, roles=("metadata",),
                                  storage_revision=snapshot.generation, object_key=published._ensure_prefixed(relative)))
    if not _REQUIRED.issubset(names):
        raise ValueError("Required published provenance metadata is not downloadable")
    return replace(record, source_provenance=resolved, metadata_assets=tuple(assets))


def verify_prepared_metadata_dependencies(root: Path, published: PublishedStorageResource, expected: dict) -> None:
    """Recompute dependency bindings from approved STAC, not plan labels alone."""
    derived = {}
    root_document = json.loads((root / "catalog.json").read_text())
    self_href = next(link["href"] for link in root_document["links"] if link["rel"] == "self")
    data_root = self_href.rsplit("/releases/", 1)[0] if "/releases/" in self_href else self_href.removesuffix("/catalog.json")
    for path in root.rglob("collection.json"):
        document = json.loads(path.read_text())
        provenance = document.get("hifld:provenance")
        expected_native_metadata = []
        for asset in document.get("assets", {}).values():
            href = asset.get("href", "")
            if "data" not in asset.get("roles", []) or not isinstance(href, str) or not href.startswith(data_root + "/"):
                continue
            key = href.removeprefix(data_root + "/")
            if Path(key).is_absolute() or ".." in Path(key).parts or Path(key).as_posix() != key or "\\" in key:
                raise ValueError("Unsafe published data asset key")
            parts = Path(key).parts
            format_index = next((index for index in range(len(parts) - 2, -1, -1) if parts[index] in {"geopackage", "geoparquet", "pmtiles", "shapefile"}), None)
            if format_index is None:
                continue
            prefix = "/".join(parts[:format_index])
            resolved_key = prefix + "/metadata/resolved_provenance.json"
            manifest_key = prefix + "/metadata/source_manifest.json"
            if published.object_exists(manifest_key):
                authored = provenance_projection(json.loads(published.read_key(manifest_key)))
                if authored is not None:
                    if not published.object_exists(resolved_key):
                        raise ValueError("Published source is missing complete input provenance")
                    expected_native_metadata.append(json.loads(published.read_key(resolved_key)))
        if provenance is None:
            if expected_native_metadata or any("metadata" in asset.get("roles", []) for asset in document.get("assets", {}).values()):
                raise ValueError("Modern published source cannot lose its input provenance")
            continue
        if any(value != provenance for value in expected_native_metadata):
            raise ValueError("Published source does not match prepared complete input provenance")
        validate_provenance(provenance)
        canonical = {}
        for asset in document.get("assets", {}).values():
            if "metadata" not in asset.get("roles", []):
                continue
            key = asset.get("hifld:object_key")
            if (not isinstance(key, str) or not key or Path(key).is_absolute() or ".." in Path(key).parts
                    or Path(key).as_posix() != key or "\\" in key):
                raise ValueError("Missing or unsafe actual metadata object key")
            validate_provenance_url(asset["href"])
            dependency = {"sha256": asset["file:checksum"][4:], "bytes": asset["file:size"]}
            if key in derived and derived[key] != dependency:
                raise ValueError("Conflicting prepared metadata dependencies")
            derived[key] = dependency
            if asset.get("title") in {"metadata/resolved_provenance.json", "metadata/data_dictionary.json"}:
                canonical[asset["title"]] = json.loads(published.read_key(key))
        if (canonical.get("metadata/resolved_provenance.json") != provenance
                or provenance_projection(canonical.get("metadata/data_dictionary.json", {})) != provenance):
            raise ValueError("Published metadata does not match prepared complete input provenance")
    if set(derived) != set(expected):
        raise ValueError("Prepared provenance dependency inventory changed")
    for key, dependency in derived.items():
        if any(expected[key].get(name) != value for name, value in dependency.items()):
            raise ValueError("Prepared metadata pins differ from rendered assets")
        snapshot = published.object_snapshot(key)
        if (snapshot is None or snapshot.to_dict() != expected[key].get("snapshot") or snapshot.size != dependency["bytes"]
                or published.sha256_key(key) != dependency["sha256"] or published.object_snapshot(key) != snapshot):
            raise ValueError("Published provenance metadata changed since planning")

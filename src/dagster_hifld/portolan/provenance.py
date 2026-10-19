"""Bind rendered multi-input provenance to actual downloadable metadata bytes."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

from dagster_hifld.provenance import provenance_projection, validate_provenance, validate_provenance_url
from dagster_hifld.resources import PublishedStorageResource

from .catalog import AssetRecord, CatalogRecord

_REQUIRED = {"source_manifest.json", "resolved_provenance.json", "data_dictionary.json", "quality_manifest.json"}


def _asset_key(asset: dict, public_root: str) -> str:
    key = asset.get("hifld:object_key")
    # Older imported STAC can supply an unambiguous same-root URL instead of
    # the newer redundant object_key. Neither spelling may name another host.
    if key is None and isinstance(asset.get("href"), str) and asset["href"].startswith(public_root + "/"):
        key = asset["href"].removeprefix(public_root + "/")
    if (not isinstance(key, str) or not key or Path(key).is_absolute() or ".." in Path(key).parts
            or Path(key).as_posix() != key or "\\" in key or any(char in key for char in "?#%")):
        raise ValueError("Missing or unsafe actual asset object key")
    if asset.get("href") != public_root + "/" + key:
        raise ValueError("Downloadable asset href does not match pinned published object")
    validate_provenance_url(asset["href"])
    return key


def prepared_asset_dependencies(root: Path, published: PublishedStorageResource, public_root: str) -> dict:
    """Derive and read every rendered dependency, including prior collections.

    Data, styles, thumbnails and metadata all bind actual keys, URLs, complete
    byte digests and storage identities. Unpinned external assets fail explicitly.
    """
    public_root = public_root.rstrip("/")
    validate_provenance_url(public_root)
    dependencies = {}
    for path in sorted(root.rglob("collection.json")):
        for asset in json.loads(path.read_text()).get("assets", {}).values():
            key = _asset_key(asset, public_root)
            checksum, size = asset.get("file:checksum"), asset.get("file:size")
            if (not isinstance(checksum, str) or len(checksum) != 68 or not checksum.startswith("1220")
                    or any(char not in "0123456789abcdef" for char in checksum[4:])
                    or isinstance(size, bool) or not isinstance(size, int) or size < 0):
                raise ValueError("Rendered asset lacks exact SHA256/length dependency pins")
            snapshot = published.object_snapshot(key)
            if (snapshot is None or snapshot.size != size or published.sha256_key(key) != checksum[4:]
                    or published.object_snapshot(key) != snapshot):
                raise ValueError("Published asset dependency changed or differs from rendered pins")
            dependency = {"sha256": checksum[4:], "bytes": size, "snapshot": snapshot.to_dict()}
            if key in dependencies and dependencies[key] != dependency:
                raise ValueError("Conflicting rendered asset dependencies")
            dependencies[key] = dependency
    return dependencies


def verify_prepared_asset_dependencies(root: Path, published: PublishedStorageResource, public_root: str, expected: dict) -> None:
    if prepared_asset_dependencies(root, published, public_root) != expected:
        raise ValueError("Prepared complete asset dependency inventory changed")


def verify_sqlite_asset_projection(root: Path) -> None:
    """Reconcile full SQLite asset/location/object populations with actual STAC."""
    expected = {}
    for path in sorted(root.rglob("collection.json")):
        version = path.parent.relative_to(root).as_posix()
        document = json.loads(path.read_text())
        for key, asset in document.get("assets", {}).items():
            expected[version + "/" + key] = (version, key, asset, document)
    with sqlite3.connect(f"file:{root / '_catalog/catalog.sqlite'}?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        rows = {row["asset_path"]: row for row in connection.execute("SELECT * FROM assets")}
        if set(rows) != set(expected):
            raise ValueError("Prepared SQLite asset inventory differs from rendered STAC")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise ValueError("Prepared SQLite has orphaned asset dependencies")
        for asset_path, (version, key, asset, document) in expected.items():
            row = rows[asset_path]
            checksum = row["checksum_multihash"] or ("1220" + row["sha256"] if row["sha256"] else None)
            if row["sha256"] is not None and "1220" + row["sha256"] != asset.get("file:checksum"):
                raise ValueError("Prepared SQLite SHA256 differs from rendered STAC")
            if ((row["version_path"], row["asset_key"], row["title"], row["href"], row["media_type"],
                 json.loads(row["roles_json"]), row["size_bytes"], checksum) !=
                    (version, key, asset.get("title"), asset.get("href"), asset.get("type"),
                     asset.get("roles"), asset.get("file:size"), asset.get("file:checksum"))):
                raise ValueError("Prepared SQLite asset fields differ from rendered STAC")
            if (("hifld:format_key" in asset and asset["hifld:format_key"] != row["format_key"])
                    or (row["format_key"] == "metadata") != ("metadata" in asset["roles"])):
                raise ValueError("Prepared SQLite format differs from rendered STAC")
            locations = connection.execute("SELECT * FROM asset_locations WHERE asset_path=?", (asset_path,)).fetchall()
            objects = connection.execute("SELECT * FROM asset_objects WHERE asset_path=?", (asset_path,)).fetchall()
            if len(locations) != 1 or len(objects) != 1:
                raise ValueError("Prepared SQLite asset location/object inventory differs from STAC")
            location, obj = locations[0], objects[0]
            object_checksum = obj["checksum_multihash"] or ("1220" + obj["sha256"] if obj["sha256"] else None)
            if obj["sha256"] is not None and "1220" + obj["sha256"] != asset.get("file:checksum"):
                raise ValueError("Prepared SQLite object SHA256 differs from rendered STAC")
            if (location["href"] != asset["href"] or location["is_canonical"] != 1
                    or obj["asset_location_path"] != location["asset_location_path"]
                    or obj["object_key"] != asset.get("hifld:object_key", asset["href"])
                    or obj["relative_path"] != obj["object_key"].rsplit("/", 1)[-1]
                    or obj["size_bytes"] != asset["file:size"] or object_checksum != asset["file:checksum"]):
                raise ValueError("Prepared SQLite asset object fields differ from rendered STAC")
            native_bbox = json.loads(obj["native_bbox_json"]) if obj["native_bbox_json"] is not None else None
            crs84_bbox = json.loads(obj["crs84_bbox_json"]) if obj["crs84_bbox_json"] is not None else None
            if (native_bbox != document.get("hifld:native_bbox")
                    or crs84_bbox != document["extent"]["spatial"]["bbox"][0]):
                raise ValueError("Prepared SQLite asset object bounds differ from rendered STAC")


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
            key = _asset_key(asset, data_root)
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

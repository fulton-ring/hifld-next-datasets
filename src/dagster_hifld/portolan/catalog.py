"""Create the Portolan tree and its disposable SQLite search projection."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from uuid import uuid4

CATALOG_APPLICATION_ID = 0x4849464C
CATALOG_SCHEMA_VERSION = 2
PORTOLAN_PROFILE_URI = "https://portolan.dev/profile/0.2"
PORTOLAN_STAC_EXTENSION = "https://schemas.portolan-sdi.org/portolan/v0.2.0/schema.json"
WEB_MAP_LINKS_EXTENSION = (
    "https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json"
)
STAC_JSON_MEDIA_TYPE = "application/json"
UNKNOWN_SPATIAL_EXTENT = [-180.0, -90.0, 180.0, 90.0]
HIFLD_ARCHIVE_PUBLIC_DOMAIN_MARK = "CC-PDM-1.0"
HIFLD_ARCHIVE_LICENSE_HREF = "../../../LICENSE.md"
HIFLD_ARCHIVE_RIGHTS_NOTICE = """# Archived HIFLD Open data rights

HIFLD Next identifies the archived HIFLD Open snapshot as public-domain material
based on the project operator's confirmation that these datasets were public domain
when the original portal was taken down. The Creative Commons Public Domain Mark 1.0
is an informational status label, not a new license grant.

https://creativecommons.org/publicdomain/mark/1.0/

This notice applies only to the archived HIFLD Open snapshot and its format
conversions. It does not apply automatically to later uploads. An applicable
dataset-specific rights file or source notice takes precedence.
"""
HIFLD_NEXT_HOST_NAME = "HIFLD Next"
HIFLD_NEXT_HOST_URL = "https://hifld.publicenvirodata.org"
JsonScalar = str | int | float | bool | None


@dataclass(frozen=True)
class AssetRecord:
    key: str
    format_key: str
    title: str
    href: str
    media_type: str
    size_bytes: int
    sha256: str | None
    checksum_multihash: str | None = None
    roles: tuple[str, ...] = ("data",)
    storage_slug: str = "canonical"
    storage_revision: str | None = None
    object_key: str | None = None
    pmtiles_layers: tuple[str, ...] = ()


@dataclass(frozen=True)
class ColumnRecord:
    name: str
    data_type: str
    ordinal: int
    nullable: bool
    is_geometry: bool = False
    null_count: int | None = None
    unique_count: int | None = None
    description: str | None = None
    min_value: str | None = None
    max_value: str | None = None
    example_values: tuple[JsonScalar, ...] = ()
    possible_values: tuple[JsonScalar, ...] = ()
    length: int | None = None


@dataclass(frozen=True)
class CatalogRecord:
    collection_slug: str
    dataset_slug: str
    file_slug: str
    version_label: str
    title: str
    description: str
    spatial_status: str
    feature_count: int
    assets: tuple[AssetRecord, ...]
    collection_title: str | None = None
    collection_description: str | None = None
    collection_created_at: str | None = None
    collection_updated_at: str | None = None
    dataset_title: str | None = None
    dataset_description: str | None = None
    dataset_created_at: str | None = None
    dataset_updated_at: str | None = None
    tags: tuple[str, ...] = ()
    dataset_tags: tuple[tuple[str, str], ...] = ()
    file_tags: tuple[tuple[str, str], ...] = ()
    columns: tuple[ColumnRecord, ...] = ()
    native_crs: str | None = None
    geometry_column: str | None = None
    geometry_type: str | None = None
    feature_id_column: str | None = None
    native_bbox: tuple[float, float, float, float] | None = None
    crs84_bbox: tuple[float, float, float, float] | None = None
    source_version_description: str | None = None
    source_version_bounds: tuple[float, float, float, float] | None = None
    quality_manifest_href: str = "metadata/quality_manifest.json"
    quality_passed: bool = True
    invalid_geometry_count: int = 0
    null_geometry_count: int = 0
    sampled_feature_count: int | None = None
    sampled_invalid_geometry_count: int | None = None
    sampled_null_geometry_count: int | None = None
    source_columns_hash: str | None = None
    quality_provenance: str = "generated"
    license_id: str = "other"
    license_href: str | None = None
    provider: str | None = None
    agency: str | None = None
    office: str | None = None
    source_url: str | None = None
    metadata_sources: tuple[str, ...] = ()
    metadata_resolved_from: tuple[tuple[str, str], ...] = ()
    inventory_match_type: str | None = None
    manifest_role: str | None = None
    manifest_keys: tuple[str, ...] = ()
    created_at: str | None = None
    updated_at: str | None = None

    @property
    def collection_path(self) -> str:
        return self.collection_slug

    @property
    def dataset_path(self) -> str:
        return f"{self.collection_slug}/{self.dataset_slug}"

    @property
    def file_path(self) -> str:
        return f"{self.dataset_path}/{self.file_slug}"

    @property
    def version_path(self) -> str:
        return f"{self.file_path}/{self.version_label}"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _format_definition(asset: AssetRecord) -> tuple[str, str, int]:
    spatial = int(
        asset.format_key
        in {
            "geoparquet",
            "geopackage",
            "geojson",
            "shapefile",
            "pmtiles",
            "file_geodatabase",
        }
    )
    return asset.format_key, asset.media_type, spatial


def _root_metadata(
    records: tuple[CatalogRecord, ...],
) -> tuple[str | None, str]:
    collection_slugs = {record.collection_slug for record in records}
    if len(collection_slugs) != 1:
        return None, ""
    record = records[0]
    return record.collection_title, record.collection_description or ""


def _apply_schema(connection: sqlite3.Connection) -> None:
    schema = Path(__file__).with_name("catalog_schema.sql").read_text(encoding="utf-8")
    connection.executescript(schema)


def build_catalog_sqlite(
    destination: Path,
    records: tuple[CatalogRecord, ...],
    *,
    catalog_generation: str | None = None,
    root_href: str = "catalog.json",
) -> str:
    """Build a closed, validated SQLite projection from typed publishing records."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        destination.unlink()
    generation = catalog_generation or str(uuid4())
    created_at = _now()
    root_title, _ = _root_metadata(records)
    connection = sqlite3.connect(destination)
    try:
        _apply_schema(connection)
        with connection:
            connection.execute(
                "INSERT INTO catalog_metadata VALUES (1, ?, ?, ?, ?, ?, ?)",
                (
                    CATALOG_SCHEMA_VERSION,
                    generation,
                    created_at,
                    PORTOLAN_PROFILE_URI,
                    root_href,
                    root_title or "catalog",
                ),
            )
            for record in records:
                _insert_record(connection, record)
            _set_latest_versions(connection, records)
        connection.execute("PRAGMA foreign_key_check").fetchall()
        failures = connection.execute("PRAGMA foreign_key_check").fetchall()
        if failures:
            raise ValueError(f"Catalog foreign-key validation failed: {failures}")
        if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("Catalog SQLite integrity check failed.")
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        connection.close()
    return generation


def update_catalog_sqlite(
    destination: Path,
    records: tuple[CatalogRecord, ...],
    *,
    catalog_generation: str | None = None,
) -> str:
    """Idempotently add or replace versions while retaining unrelated records."""
    generation = catalog_generation or str(uuid4())
    if not destination.exists():
        return build_catalog_sqlite(destination, records, catalog_generation=generation)
    connection = sqlite3.connect(destination)
    try:
        if (
            connection.execute("PRAGMA application_id").fetchone()[0]
            != CATALOG_APPLICATION_ID
        ):
            raise ValueError("Existing catalog has an unsupported application ID.")
        if (
            connection.execute("PRAGMA user_version").fetchone()[0]
            != CATALOG_SCHEMA_VERSION
        ):
            raise ValueError("Existing catalog has an unsupported schema version.")
        with connection:
            for record in records:
                _delete_version_projection(connection, record.version_path)
                _insert_record(connection, record)
            _set_latest_versions(connection, records)
            connection.execute(
                "DELETE FROM formats WHERE format_key NOT IN "
                "(SELECT DISTINCT format_key FROM assets)"
            )
            root_title, _ = _root_metadata(records)
            connection.execute(
                "UPDATE catalog_metadata SET catalog_generation = ?, created_at = ?, "
                "root_title = ? WHERE singleton = 1",
                (generation, _now(), root_title or "catalog"),
            )
        failures = connection.execute("PRAGMA foreign_key_check").fetchall()
        if failures:
            raise ValueError(f"Catalog foreign-key validation failed: {failures}")
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        connection.close()
    return generation


def _delete_version_projection(
    connection: sqlite3.Connection, version_path: str
) -> None:
    asset_paths = [
        row[0]
        for row in connection.execute(
            "SELECT asset_path FROM assets WHERE version_path = ?", (version_path,)
        )
    ]
    for asset_path in asset_paths:
        connection.execute(
            "DELETE FROM asset_objects WHERE asset_path = ?", (asset_path,)
        )
        connection.execute(
            "DELETE FROM asset_locations WHERE asset_path = ?", (asset_path,)
        )
    connection.execute("DELETE FROM assets WHERE version_path = ?", (version_path,))
    connection.execute("DELETE FROM columns WHERE version_path = ?", (version_path,))
    connection.execute("DELETE FROM quality WHERE version_path = ?", (version_path,))
    connection.execute("DELETE FROM versions WHERE version_path = ?", (version_path,))


def version_sort_key(label: str) -> tuple[int, int, int, int, int, str, str]:
    """Order semver numerically while tolerating legacy non-semver labels."""
    core, _, prerelease = label.removeprefix("v").partition("-")
    core = core.split("+", 1)[0]
    numbers = core.split(".")
    if not 1 <= len(numbers) <= 3 or not all(number.isdecimal() for number in numbers):
        return (0, 0, 0, 0, 0, "", label)
    major, minor, patch = (
        int(number) for number in (*numbers, *("0",) * (3 - len(numbers)))
    )
    return (1, major, minor, patch, int(not prerelease), prerelease, label)


def _set_latest_versions(
    connection: sqlite3.Connection, records: tuple[CatalogRecord, ...]
) -> None:
    for file_path in {record.file_path for record in records}:
        labels = [
            row[0]
            for row in connection.execute(
                "SELECT version_label FROM versions WHERE file_path = ?", (file_path,)
            )
        ]
        if not labels:
            continue
        latest = max(labels, key=version_sort_key)
        connection.execute(
            "UPDATE versions SET is_latest = (version_label = ?) WHERE file_path = ?",
            (latest, file_path),
        )
        connection.execute(
            "UPDATE files SET latest_version = ? WHERE file_path = ?",
            (latest, file_path),
        )


def _insert_record(connection: sqlite3.Connection, record: CatalogRecord) -> None:
    created_at = record.created_at
    updated_at = record.updated_at
    dataset_title = (
        record.title if record.dataset_title is None else record.dataset_title
    )
    dataset_description = (
        record.description
        if record.dataset_description is None
        else record.dataset_description
    )
    connection.execute(
        "INSERT INTO collections VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(collection_path) DO UPDATE SET "
        "title = excluded.title, description = excluded.description, "
        "catalog_href = excluded.catalog_href, license_href = excluded.license_href, "
        "created_at = excluded.created_at, updated_at = excluded.updated_at",
        (
            record.collection_path,
            record.collection_slug,
            record.collection_title or record.collection_slug,
            record.collection_description or "",
            f"{record.collection_path}/catalog.json",
            record.license_href,
            record.collection_created_at,
            record.collection_updated_at,
        ),
    )
    connection.execute(
        "INSERT INTO datasets VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(dataset_path) DO UPDATE SET "
        "title = excluded.title, description = excluded.description, "
        "catalog_href = excluded.catalog_href, created_at = excluded.created_at, "
        "updated_at = excluded.updated_at",
        (
            record.dataset_path,
            record.collection_path,
            record.dataset_slug,
            dataset_title,
            dataset_description,
            f"{record.dataset_path}/catalog.json",
            record.dataset_created_at,
            record.dataset_updated_at,
        ),
    )
    connection.execute(
        "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?) "
        "ON CONFLICT(file_path) DO UPDATE SET "
        "title = excluded.title, description = excluded.description, "
        "catalog_href = excluded.catalog_href, updated_at = excluded.updated_at",
        (
            record.file_path,
            record.dataset_path,
            record.file_slug,
            record.title,
            record.description,
            f"{record.file_path}/catalog.json",
            created_at,
            updated_at,
        ),
    )
    connection.execute(
        "UPDATE versions SET is_latest = 0 WHERE file_path = ?", (record.file_path,)
    )
    connection.execute(
        "INSERT OR REPLACE INTO versions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
        (
            record.version_path,
            record.file_path,
            record.version_label,
            f"{record.version_path}/collection.json",
            created_at,
            updated_at,
            record.spatial_status,
            _json(record.native_bbox) if record.native_bbox else None,
            _json(record.crs84_bbox) if record.crs84_bbox else None,
            record.native_crs,
            record.geometry_column,
            record.geometry_type,
            record.feature_id_column,
            record.feature_count,
        ),
    )
    connection.execute(
        "UPDATE files SET latest_version = ? WHERE file_path = ?",
        (record.version_label, record.file_path),
    )
    for asset in record.assets:
        format_key, media_type, is_spatial = _format_definition(asset)
        connection.execute(
            "INSERT OR IGNORE INTO formats VALUES (?, ?, ?, ?)",
            (format_key, format_key.replace("_", " ").title(), media_type, is_spatial),
        )
        asset_path = f"{record.version_path}/{asset.key}"
        connection.execute(
            "INSERT OR REPLACE INTO assets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                asset_path,
                record.version_path,
                asset.key,
                asset.format_key,
                asset.title,
                asset.href,
                asset.media_type,
                _json(asset.roles),
                asset.size_bytes,
                asset.sha256,
                asset.checksum_multihash,
            ),
        )
        location_path = f"{asset_path}/{asset.storage_slug}"
        connection.execute(
            "INSERT OR REPLACE INTO asset_locations VALUES (?, ?, ?, ?, 1)",
            (location_path, asset_path, asset.storage_slug, asset.href),
        )
        object_path = (
            f"{location_path}/{hashlib.sha256(asset.href.encode()).hexdigest()[:16]}"
        )
        object_key = asset.object_key or asset.href
        connection.execute(
            "INSERT OR REPLACE INTO asset_objects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)",
            (
                object_path,
                asset_path,
                location_path,
                object_key,
                object_key.rsplit("/", 1)[-1],
                asset.size_bytes,
                asset.sha256,
                asset.checksum_multihash,
                asset.storage_revision,
                _json(record.native_bbox) if record.native_bbox else None,
                _json(record.crs84_bbox) if record.crs84_bbox else None,
            ),
        )
    for column in record.columns:
        column_path = f"{record.version_path}/{column.ordinal}"
        connection.execute(
            "INSERT OR REPLACE INTO columns (column_path,version_path,ordinal,name,data_type,description,nullable,is_geometry,null_count,unique_count,min_value,max_value,statistics_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                column_path,
                record.version_path,
                column.ordinal,
                column.name,
                column.data_type,
                column.description,
                int(column.nullable),
                int(column.is_geometry),
                column.null_count,
                column.unique_count,
                column.min_value,
                column.max_value,
                _json(
                    {
                        "exampleValues": column.example_values,
                        "possibleValues": column.possible_values,
                        "length": column.length,
                        "numNullValues": column.null_count,
                        "numUniqueValues": column.unique_count,
                    }
                ),
            ),
        )
    columns_hash = (
        record.source_columns_hash
        or hashlib.sha256(
            _json(
                [(column.name, column.data_type) for column in record.columns]
            ).encode()
        ).hexdigest()
    )
    connection.execute(
        "INSERT OR REPLACE INTO quality VALUES (?, ?, ?, ?, ?, ?)",
        (
            record.version_path,
            record.quality_manifest_href,
            int(record.quality_passed),
            record.invalid_geometry_count,
            record.null_geometry_count,
            columns_hash,
        ),
    )
    connection.execute("DELETE FROM tags WHERE entity_path = ?", (record.dataset_path,))
    connection.execute("DELETE FROM tags WHERE entity_path = ?", (record.file_path,))
    for tag_key, tag_value in record.dataset_tags:
        connection.execute(
            "INSERT OR IGNORE INTO tags VALUES (?, ?, ?, ?)",
            (
                f"{record.dataset_path}/{tag_key}/{tag_value}",
                record.dataset_path,
                tag_key,
                tag_value,
            ),
        )
    for tag_key, tag_value in (
        *record.file_tags,
        *(("keyword", tag) for tag in record.tags),
    ):
        connection.execute(
            "INSERT OR IGNORE INTO tags VALUES (?, ?, ?, ?)",
            (
                f"{record.file_path}/{tag_key}/{tag_value}",
                record.file_path,
                tag_key,
                tag_value,
            ),
        )
    dataset_tags = " ".join(
        (*record.tags, *(value for _, value in record.dataset_tags))
    )
    file_tags = " ".join((*record.tags, *(value for _, value in record.file_tags)))
    connection.execute(
        "DELETE FROM dataset_fts WHERE dataset_path = ?", (record.dataset_path,)
    )
    connection.execute(
        "INSERT INTO dataset_fts VALUES (?, ?, ?, ?, ?)",
        (
            record.dataset_path,
            dataset_title,
            dataset_description,
            dataset_tags,
            record.dataset_slug,
        ),
    )
    connection.execute("DELETE FROM file_fts WHERE file_path = ?", (record.file_path,))
    connection.execute(
        "INSERT INTO file_fts VALUES (?, ?, ?, ?, ?)",
        (
            record.file_path,
            record.title,
            record.description,
            file_tags,
            record.file_slug,
        ),
    )


def render_portolan_tree(
    root: Path, records: tuple[CatalogRecord, ...], *, public_root: str | None = None
) -> None:
    """Render deterministic STAC Catalog/Collection JSON and companion docs."""
    root_title, root_description = _root_metadata(records)
    _write_json(
        root / "catalog.json",
        _catalog(
            root_title,
            root_description,
            [
                (
                    f"{slug}/catalog.json",
                    _required_title(
                        next(
                            record.collection_title
                            for record in records
                            if record.collection_slug == slug
                        ),
                        slug,
                    ),
                    STAC_JSON_MEDIA_TYPE,
                )
                for slug in sorted({record.collection_slug for record in records})
            ],
            root_href="catalog.json",
            self_href="catalog.json",
            created_at=records[0].collection_created_at if records else None,
            updated_at=records[0].collection_updated_at if records else None,
        ),
    )
    _write_docs(root, root_title or "catalog", root_description)
    for record in records:
        _render_record(root, record)
    if any(
        record.collection_slug == "hifld"
        and record.license_id == HIFLD_ARCHIVE_PUBLIC_DOMAIN_MARK
        for record in records
    ):
        (root / "hifld" / "LICENSE.md").write_text(
            HIFLD_ARCHIVE_RIGHTS_NOTICE, encoding="utf-8"
        )
    # Parent catalogs are projections of all records, never last-record-wins.
    for collection_slug in sorted({record.collection_slug for record in records}):
        collection_records = [
            record for record in records if record.collection_slug == collection_slug
        ]
        dataset_slugs = sorted({record.dataset_slug for record in collection_records})
        _write_json(
            root / collection_slug / "catalog.json",
            _catalog(
                collection_records[0].collection_title,
                collection_records[0].collection_description or "",
                [
                    (
                        f"{slug}/catalog.json",
                        _required_title(
                            next(
                                record.title
                                if record.dataset_title is None
                                else record.dataset_title
                                for record in collection_records
                                if record.dataset_slug == slug
                            ),
                            slug,
                        ),
                        STAC_JSON_MEDIA_TYPE,
                    )
                    for slug in dataset_slugs
                ],
                identifier=collection_slug,
                root_href="../catalog.json",
                parent_href="../catalog.json",
                self_href="catalog.json",
                created_at=collection_records[0].collection_created_at,
                updated_at=collection_records[0].collection_updated_at,
            ),
        )
        for dataset_slug in dataset_slugs:
            dataset_records = [
                record
                for record in collection_records
                if record.dataset_slug == dataset_slug
            ]
            file_slugs = sorted({record.file_slug for record in dataset_records})
            _write_json(
                root / collection_slug / dataset_slug / "catalog.json",
                _catalog(
                    dataset_records[0].title
                    if dataset_records[0].dataset_title is None
                    else dataset_records[0].dataset_title,
                    dataset_records[0].description
                    if dataset_records[0].dataset_description is None
                    else dataset_records[0].dataset_description,
                    [
                        (
                            f"{slug}/catalog.json",
                            _required_title(
                                next(
                                    record.title
                                    for record in dataset_records
                                    if record.file_slug == slug
                                ),
                                slug,
                            ),
                            STAC_JSON_MEDIA_TYPE,
                        )
                        for slug in file_slugs
                    ],
                    identifier=f"{collection_slug}/{dataset_slug}",
                    keywords=dataset_records[0].tags,
                    source_tags=dataset_records[0].dataset_tags,
                    root_href="../../catalog.json",
                    parent_href="../catalog.json",
                    self_href="catalog.json",
                    created_at=dataset_records[0].dataset_created_at,
                    updated_at=dataset_records[0].dataset_updated_at,
                ),
            )
            for file_slug in file_slugs:
                file_records = [
                    record
                    for record in dataset_records
                    if record.file_slug == file_slug
                ]
                versions = sorted(
                    (record.version_label for record in file_records),
                    key=version_sort_key,
                )
                _write_json(
                    root / collection_slug / dataset_slug / file_slug / "catalog.json",
                    _catalog(
                        file_records[0].title,
                        file_records[0].description,
                        [
                            (
                                f"{version}/collection.json",
                                _required_title(
                                    next(
                                        record.title
                                        for record in file_records
                                        if record.version_label == version
                                    ),
                                    version,
                                ),
                                STAC_JSON_MEDIA_TYPE,
                            )
                            for version in versions
                        ],
                        latest=versions[-1],
                        identifier=f"{collection_slug}/{dataset_slug}/{file_slug}",
                        keywords=file_records[0].tags,
                        source_tags=file_records[0].file_tags,
                        root_href="../../../catalog.json",
                        parent_href="../catalog.json",
                        self_href="catalog.json",
                        created_at=file_records[0].created_at,
                        updated_at=file_records[0].updated_at,
                    ),
                )

    if public_root:
        _absolutize_stac_tree(root, public_root)


def _absolutize_stac_tree(root: Path, public_root: str) -> None:
    normalized_root = public_root.rstrip("/")
    parsed_root = urlparse(normalized_root)
    if parsed_root.scheme not in {"http", "https"} or not parsed_root.netloc:
        raise ValueError("public_root must be an absolute HTTP(S) URL.")
    for path in (
        candidate
        for candidate in root.rglob("*.json")
        if candidate.name in {"catalog.json", "collection.json"}
    ):
        document = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict):
            raise TypeError(f"Rendered STAC document must be an object: {path}")
        relative_path = path.relative_to(root).as_posix()
        document_href = f"{normalized_root}/{relative_path}"
        links = document.get("links")
        if isinstance(links, list):
            for link in links:
                if isinstance(link, dict):
                    href = link.get("href")
                    relation = link.get("rel")
                    if (
                        isinstance(href, str)
                        and relation not in {"agents", "describedby"}
                        and not _is_absolute_href(href)
                    ):
                        link["href"] = urljoin(document_href, href)
        assets = document.get("assets")
        if isinstance(assets, dict):
            for asset in assets.values():
                if isinstance(asset, dict):
                    href = asset.get("href")
                    if isinstance(href, str) and not _is_absolute_href(href):
                        asset["href"] = urljoin(f"{normalized_root}/", href)
        _write_json(path, document)


def _is_absolute_href(href: str) -> bool:
    return bool(urlparse(href).scheme)


def _providers(record: CatalogRecord) -> list[dict[str, object]]:
    providers: list[dict[str, object]] = []
    if record.provider and record.provider != HIFLD_NEXT_HOST_NAME:
        providers.append({"name": record.provider, "roles": ["producer"]})
    providers.append(
        {
            "name": HIFLD_NEXT_HOST_NAME,
            "roles": ["host"],
            "url": HIFLD_NEXT_HOST_URL,
        }
    )
    return providers


def _render_record(root: Path, record: CatalogRecord) -> None:
    collection_dir = root / record.collection_path
    dataset_dir = root / record.dataset_path
    file_dir = root / record.file_path
    version_dir = root / record.version_path
    _write_json(
        collection_dir / "catalog.json",
        _catalog(
            record.collection_title,
            record.collection_description or "",
            [
                (
                    f"{record.dataset_slug}/catalog.json",
                    _required_title(
                        record.title
                        if record.dataset_title is None
                        else record.dataset_title,
                        record.dataset_slug,
                    ),
                    STAC_JSON_MEDIA_TYPE,
                )
            ],
            identifier=record.collection_slug,
            root_href="../catalog.json",
            parent_href="../catalog.json",
            self_href="catalog.json",
        ),
    )
    _write_docs(
        collection_dir,
        record.collection_title or record.collection_slug,
        record.collection_description or "",
    )
    _write_json(
        dataset_dir / "catalog.json",
        _catalog(
            record.title if record.dataset_title is None else record.dataset_title,
            record.description
            if record.dataset_description is None
            else record.dataset_description,
            [
                (
                    f"{record.file_slug}/catalog.json",
                    _required_title(record.title, record.file_slug),
                    STAC_JSON_MEDIA_TYPE,
                )
            ],
            identifier=record.dataset_path,
            keywords=record.tags,
            source_tags=record.dataset_tags,
            root_href="../../catalog.json",
            parent_href="../catalog.json",
            self_href="catalog.json",
        ),
    )
    _write_docs(
        dataset_dir,
        record.title if record.dataset_title is None else record.dataset_title,
        record.description
        if record.dataset_description is None
        else record.dataset_description,
    )
    _write_json(
        file_dir / "catalog.json",
        _catalog(
            record.title,
            record.description,
            [
                (
                    f"{record.version_label}/collection.json",
                    _required_title(record.title, record.version_label),
                    STAC_JSON_MEDIA_TYPE,
                )
            ],
            latest=record.version_label,
            identifier=record.file_path,
            keywords=record.tags,
            source_tags=record.file_tags,
            root_href="../../../catalog.json",
            parent_href="../catalog.json",
            self_href="catalog.json",
        ),
    )
    _write_docs(file_dir, record.title, record.description)
    assets = {
        asset.key: {
            "href": asset.href,
            "type": asset.media_type,
            "title": asset.title,
            "roles": list(asset.roles),
            "file:size": asset.size_bytes,
            **(
                {"file:checksum": asset.checksum_multihash}
                if asset.checksum_multihash is not None
                else (
                    {"file:checksum": f"1220{asset.sha256}"}
                    if asset.sha256 is not None
                    else {}
                )
            ),
        }
        for asset in record.assets
    }
    links: list[dict[str, object]] = [
        {
            "rel": "root",
            "href": "../../../../catalog.json",
            "type": STAC_JSON_MEDIA_TYPE,
        },
        {"rel": "parent", "href": "../catalog.json", "type": STAC_JSON_MEDIA_TYPE},
        {"rel": "self", "href": "collection.json", "type": STAC_JSON_MEDIA_TYPE},
        {"rel": "agents", "href": "AGENTS.md", "type": "text/markdown"},
        {"rel": "describedby", "href": "README.md", "type": "text/markdown"},
    ]
    if record.license_href:
        links.append(
            {"rel": "license", "href": record.license_href, "type": "text/markdown"}
        )
    if record.source_url:
        links.append(
            {
                "rel": "via",
                "href": record.source_url,
                "type": "text/html",
            }
        )
        links.append({"rel": "derived_from", "href": record.source_url})
    pmtiles_assets = [asset for asset in record.assets if asset.format_key == "pmtiles"]
    for asset in pmtiles_assets:
        links.append(
            {
                "rel": "pmtiles",
                "href": asset.href,
                "type": asset.media_type,
                "title": asset.title,
                "pmtiles:layers": list(asset.pmtiles_layers),
            }
        )
    table_columns = [
        {
            "name": column.name,
            "type": column.data_type,
            "nullable": column.nullable,
            "is_geometry": column.is_geometry,
            **({"description": column.description} if column.description else {}),
            **({"min": column.min_value} if column.min_value is not None else {}),
            **({"max": column.max_value} if column.max_value is not None else {}),
            **(
                {"exampleValues": list(column.example_values)}
                if column.example_values
                else {}
            ),
            **(
                {"possibleValues": list(column.possible_values)}
                if column.possible_values
                else {}
            ),
            **({"length": column.length} if column.length is not None else {}),
            **(
                {"null_count": column.null_count}
                if column.null_count is not None
                else {}
            ),
            **(
                {"numNullValues": column.null_count}
                if column.null_count is not None
                else {}
            ),
            **(
                {"unique_count": column.unique_count}
                if column.unique_count is not None
                else {}
            ),
            **(
                {"numUniqueValues": column.unique_count}
                if column.unique_count is not None
                else {}
            ),
        }
        for column in sorted(record.columns, key=lambda item: item.ordinal)
    ]
    collection = {
        "stac_version": "1.1.0",
        "stac_extensions": [
            PORTOLAN_STAC_EXTENSION,
            "https://stac-extensions.github.io/file/v2.1.0/schema.json",
            "https://stac-extensions.github.io/version/v1.2.0/schema.json",
            "https://stac-extensions.github.io/table/v1.2.0/schema.json",
            *([WEB_MAP_LINKS_EXTENSION] if pmtiles_assets else []),
        ],
        "type": "Collection",
        "id": record.version_path,
        "title": record.title,
        "description": record.description,
        "license": record.license_id,
        "providers": _providers(record),
        "keywords": list(record.tags),
        "links": links,
        "extent": {
            "spatial": {
                "bbox": [
                    list(record.crs84_bbox)
                    if record.crs84_bbox is not None
                    else UNKNOWN_SPATIAL_EXTENT
                ]
            },
            "temporal": {"interval": [[record.created_at, record.updated_at]]},
        },
        "assets": assets,
        "table:columns": table_columns,
        "hifld:feature_count": record.feature_count,
        "hifld:spatial_status": record.spatial_status,
        "hifld:native_crs": record.native_crs,
        "hifld:geometry_column": record.geometry_column,
        "hifld:geometry_type": record.geometry_type,
        "hifld:feature_id_column": record.feature_id_column,
        "hifld:native_bbox": list(record.native_bbox) if record.native_bbox else None,
        "hifld:source_version_description": record.source_version_description,
        "hifld:source_version_bounds": (
            list(record.source_version_bounds)
            if record.source_version_bounds is not None
            else None
        ),
        "hifld:quality": {
            "passed": record.quality_passed,
            "invalid_geometry_count": record.invalid_geometry_count,
            "null_geometry_count": record.null_geometry_count,
            "manifest_href": record.quality_manifest_href,
            "sampled_feature_count": record.sampled_feature_count,
            "sampled_invalid_geometry_count": record.sampled_invalid_geometry_count,
            "sampled_null_geometry_count": record.sampled_null_geometry_count,
            "columns_hash": record.source_columns_hash,
            "provenance": record.quality_provenance,
        },
        "hifld:agency": record.agency,
        "hifld:office": record.office,
        "hifld:metadata_sources": list(record.metadata_sources),
        "hifld:metadata_resolved_from": dict(record.metadata_resolved_from),
        "hifld:inventory_match_type": record.inventory_match_type,
        "hifld:manifest_role": record.manifest_role,
        "hifld:manifest_keys": list(record.manifest_keys),
        **({"updated": record.updated_at} if record.updated_at else {}),
        **({"hifld:created_at": record.created_at} if record.created_at else {}),
        **({"hifld:updated_at": record.updated_at} if record.updated_at else {}),
    }
    _write_json(version_dir / "collection.json", collection)
    _write_docs(
        version_dir,
        record.title,
        f"Version {record.version_label}. Preferred asset: GeoParquet when present.",
    )


def _catalog(
    title: str | None,
    description: str,
    children: list[tuple[str, str, str]],
    latest: str | None = None,
    identifier: str | None = None,
    keywords: tuple[str, ...] = (),
    source_tags: tuple[tuple[str, str], ...] = (),
    root_href: str = "catalog.json",
    parent_href: str | None = None,
    self_href: str = "catalog.json",
    created_at: str | None = None,
    updated_at: str | None = None,
) -> dict[str, object]:
    links: list[dict[str, str]] = [
        {"rel": "root", "href": root_href, "type": STAC_JSON_MEDIA_TYPE},
        *(
            [{"rel": "parent", "href": parent_href, "type": STAC_JSON_MEDIA_TYPE}]
            if parent_href is not None
            else []
        ),
        {"rel": "self", "href": self_href, "type": STAC_JSON_MEDIA_TYPE},
        {"rel": "agents", "href": "AGENTS.md", "type": "text/markdown"},
        {"rel": "describedby", "href": "README.md", "type": "text/markdown"},
    ]
    links.extend(
        {
            "rel": "child",
            "href": href,
            "title": child_title,
            "type": media_type,
        }
        for href, child_title, media_type in children
    )
    if latest:
        links.append(
            {
                "rel": "latest-version",
                "href": f"{latest}/collection.json",
                "type": STAC_JSON_MEDIA_TYPE,
                "title": latest,
            }
        )
    catalog: dict[str, object] = {
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_STAC_EXTENSION],
        "type": "Catalog",
        "id": identifier or (title or "catalog").lower().replace(" ", "-"),
        "description": _required_description(description),
        "links": links,
    }
    catalog["title"] = _required_title(title, identifier or "catalog")
    if keywords:
        catalog["keywords"] = list(keywords)
    if source_tags:
        catalog["hifld:tags"] = _group_tags(source_tags)
    if created_at:
        catalog["hifld:created_at"] = created_at
    if updated_at:
        catalog["hifld:updated_at"] = updated_at
    return catalog


def _required_title(title: str | None, fallback: str) -> str:
    return title if title and title.strip() else fallback


def _required_description(description: str) -> str:
    return description if description.strip() else "Catalog of published data."


def _group_tags(tags: tuple[tuple[str, str], ...]) -> dict[str, str | list[str]]:
    grouped: dict[str, list[str]] = {}
    for key, value in tags:
        grouped.setdefault(key, []).append(value)
    return {
        key: values[0] if len(values) == 1 else values
        for key, values in grouped.items()
    }


def _write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _write_docs(directory: Path, title: str, description: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "README.md").write_text(
        f"# {title}\n\n{description}\n", encoding="utf-8"
    )
    (directory / "AGENTS.md").write_text(
        "Use catalog.json for machine navigation and range reads for large assets.\n",
        encoding="utf-8",
    )

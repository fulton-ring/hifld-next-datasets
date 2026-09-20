-- Stable read-only runtime projection.  All externally visible identities are
-- full slug paths; numeric join identities are deliberately not present.
PRAGMA application_id = 1212761676; -- 0x4849464c ("HIFL")
PRAGMA user_version = 2;
PRAGMA foreign_keys = ON;

CREATE TABLE catalog_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 2),
    catalog_generation TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    portolan_profile_uri TEXT NOT NULL,
    root_href TEXT NOT NULL,
    root_title TEXT NOT NULL
) STRICT;

CREATE TABLE collections (
    collection_path TEXT PRIMARY KEY,
    collection_slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    catalog_href TEXT NOT NULL UNIQUE,
    license_href TEXT,
    created_at TEXT,
    updated_at TEXT
) STRICT;

CREATE TABLE datasets (
    dataset_path TEXT PRIMARY KEY,
    collection_path TEXT NOT NULL REFERENCES collections(collection_path),
    dataset_slug TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    catalog_href TEXT NOT NULL UNIQUE,
    created_at TEXT,
    updated_at TEXT,
    UNIQUE (collection_path, dataset_slug)
) STRICT;

CREATE TABLE files (
    file_path TEXT PRIMARY KEY,
    dataset_path TEXT NOT NULL REFERENCES datasets(dataset_path),
    file_slug TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    catalog_href TEXT NOT NULL UNIQUE,
    latest_version TEXT,
    created_at TEXT,
    updated_at TEXT,
    UNIQUE (dataset_path, file_slug)
) STRICT;

CREATE TABLE versions (
    version_path TEXT PRIMARY KEY,
    file_path TEXT NOT NULL REFERENCES files(file_path),
    version_label TEXT NOT NULL,
    collection_href TEXT NOT NULL UNIQUE,
    created_at TEXT,
    updated_at TEXT,
    spatial_status TEXT NOT NULL CHECK (spatial_status IN ('spatial', 'all_null_geometry', 'non_spatial_source')),
    native_bbox_json TEXT,
    crs84_bbox_json TEXT,
    native_crs TEXT,
    geometry_column TEXT,
    geometry_type TEXT,
    feature_id_column TEXT,
    feature_count INTEGER NOT NULL CHECK (feature_count >= 0),
    is_latest INTEGER NOT NULL CHECK (is_latest IN (0, 1)),
    UNIQUE (file_path, version_label)
) STRICT;
CREATE UNIQUE INDEX versions_one_latest_per_file ON versions(file_path) WHERE is_latest = 1;

CREATE TABLE formats (
    format_key TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    media_type TEXT NOT NULL,
    is_spatial INTEGER NOT NULL CHECK (is_spatial IN (0, 1))
) STRICT;

CREATE TABLE assets (
    asset_path TEXT PRIMARY KEY,
    version_path TEXT NOT NULL REFERENCES versions(version_path),
    asset_key TEXT NOT NULL,
    format_key TEXT NOT NULL REFERENCES formats(format_key),
    title TEXT NOT NULL,
    href TEXT NOT NULL,
    media_type TEXT NOT NULL,
    roles_json TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    sha256 TEXT,
    checksum_multihash TEXT,
    UNIQUE (version_path, asset_key)
) STRICT;

CREATE TABLE asset_locations (
    asset_location_path TEXT PRIMARY KEY,
    asset_path TEXT NOT NULL REFERENCES assets(asset_path),
    storage_slug TEXT NOT NULL,
    href TEXT NOT NULL,
    is_canonical INTEGER NOT NULL CHECK (is_canonical IN (0, 1)),
    UNIQUE (asset_path, storage_slug)
) STRICT;
CREATE UNIQUE INDEX asset_locations_one_canonical ON asset_locations(asset_path) WHERE is_canonical = 1;

CREATE TABLE asset_objects (
    asset_object_path TEXT PRIMARY KEY,
    asset_path TEXT NOT NULL REFERENCES assets(asset_path),
    asset_location_path TEXT NOT NULL REFERENCES asset_locations(asset_location_path),
    object_key TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    sha256 TEXT,
    checksum_multihash TEXT,
    storage_revision TEXT,
    partition_json TEXT,
    covering_json TEXT,
    native_bbox_json TEXT,
    crs84_bbox_json TEXT,
    UNIQUE (asset_location_path, relative_path)
) STRICT;

CREATE TABLE columns (
    column_path TEXT PRIMARY KEY,
    version_path TEXT NOT NULL REFERENCES versions(version_path),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    name TEXT NOT NULL,
    data_type TEXT NOT NULL,
    description TEXT,
    nullable INTEGER NOT NULL CHECK (nullable IN (0, 1)),
    is_geometry INTEGER NOT NULL CHECK (is_geometry IN (0, 1)),
    null_count INTEGER,
    unique_count INTEGER,
    min_value TEXT,
    max_value TEXT,
    statistics_json TEXT,
    UNIQUE (version_path, ordinal),
    UNIQUE (version_path, name)
) STRICT;

CREATE TABLE quality (
    version_path TEXT PRIMARY KEY REFERENCES versions(version_path),
    manifest_href TEXT NOT NULL,
    passed INTEGER NOT NULL CHECK (passed IN (0, 1)),
    invalid_geometry_count INTEGER NOT NULL CHECK (invalid_geometry_count >= 0),
    null_geometry_count INTEGER NOT NULL CHECK (null_geometry_count >= 0),
    columns_hash TEXT NOT NULL
) STRICT;

CREATE TABLE tags (
    tag_path TEXT PRIMARY KEY,
    entity_path TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    UNIQUE (entity_path, tag_key, tag_value)
) STRICT;
CREATE INDEX tags_entity_path ON tags(entity_path);

CREATE VIRTUAL TABLE dataset_fts USING fts5(
    dataset_path UNINDEXED, title, description, tags, slug,
    tokenize = 'unicode61 remove_diacritics 2'
);
CREATE VIRTUAL TABLE file_fts USING fts5(
    file_path UNINDEXED, title, description, tags, slug,
    tokenize = 'unicode61 remove_diacritics 2'
);

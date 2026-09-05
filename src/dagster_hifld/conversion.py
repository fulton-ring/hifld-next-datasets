"""Native conversion pipeline for staged geospatial datasets.

This module ports the performance-critical conversion behavior from
`dataset-api/scripts/process_gcs_datasets.py` into this Dagster repo.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import pickle
import random
import shutil
import sqlite3
import subprocess
import tempfile
import warnings
import zipfile
import zlib
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from types import TracebackType
from typing import Any, Iterable, Optional
from urllib.parse import quote

import fiona
import geopandas as gpd
import geopandas.io.arrow as geopandas_arrow
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s2sphere
from pyproj import CRS, Transformer
from shapely.geometry import Point, shape
from shapely.ops import transform as transform_geometry

from dagster_hifld.file_geodatabase import iter_file_geodatabases
from dagster_hifld.gdal import with_large_geojson_support as _with_large_geojson_support
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_formats import (
    CANONICAL_SOURCE_FORMAT_PRECEDENCE,
    SOURCE_FORMAT_EXTENSIONS,
    discover_canonical_source_file,
    discover_legacy_unknown_shapefile,
    shapefile_dataset_files,
)

logger = logging.getLogger(__name__)

try:
    import psutil

    PSUTIL_AVAILABLE = True
except Exception:
    PSUTIL_AVAILABLE = False


FORMAT_PRIORITY = [
    (format_name, SOURCE_FORMAT_EXTENSIONS[format_name][0])
    for format_name in CANONICAL_SOURCE_FORMAT_PRECEDENCE
]
CHUNKED_READABLE_FORMATS = {"geopackage", "shapefile", "file_geodatabase"}
FORMAT_SUFFIXES = [
    "-file_geodatabase",
    "-file-geodatabase",
    "-geopackage",
    "-shapefile",
    "-geojson",
]

DEFAULT_ROW_GROUP_SIZE = 100_000
DEFAULT_DATA_PAGE_SIZE_BYTES = 1024 * 1024
DEFAULT_FGB_CHUNK_SIZE_MB = 100
DEFAULT_MEMORY_ESTIMATE_MULTIPLIER = 5.0
DEFAULT_GEOPARQUET_ROW_GROUP_TARGET_BYTES = 128 * 1024 * 1024
DEFAULT_GEOPARQUET_WRITE_BUFFER_BYTES = 128 * 1024 * 1024
DEFAULT_GEOPARQUET_MAX_ROW_GROUP_BYTES = 160 * 1024 * 1024
DEFAULT_GEOPARQUET_MAX_DATASET_FOOTER_BYTES = 128 * 1024 * 1024
DEFAULT_GEOPARQUET_AGGREGATE_BUFFER_BYTES = 512 * 1024 * 1024
DEFAULT_LARGE_GEOPARQUET_THRESHOLD_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_GEOPARQUET_TARGET_FILE_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_GEOPARQUET_COMPRESSION_SAMPLE_BYTES = 64 * 1024 * 1024
DEFAULT_GEOPARQUET_COMPRESSION_SAMPLE_TABLES = 64
DEFAULT_GEOPARQUET_COMPRESSION_SAMPLE_TABLE_BYTES = 1 * 1024 * 1024
DEFAULT_S2_CANDIDATE_LEVELS = tuple(range(2, 17))


@dataclass(frozen=True)
class GeoParquetWritePolicy:
    """Policy for writing one logical GeoParquet layer."""

    candidate_admin_columns: tuple[str, ...] = ()
    force_admin_columns: tuple[str, ...] = ()
    derived_huc_column: str | None = None
    derived_huc_partition_columns: tuple[str, ...] = ()
    derived_prefix_column: str | None = None
    derived_prefix_partitions: tuple[tuple[str, int], ...] = ()
    large_dataset_threshold_bytes: int = DEFAULT_LARGE_GEOPARQUET_THRESHOLD_BYTES
    target_row_group_bytes: int = DEFAULT_GEOPARQUET_ROW_GROUP_TARGET_BYTES
    write_buffer_bytes: int = DEFAULT_GEOPARQUET_WRITE_BUFFER_BYTES
    max_row_group_bytes: int = DEFAULT_GEOPARQUET_MAX_ROW_GROUP_BYTES
    max_dataset_footer_bytes: int = DEFAULT_GEOPARQUET_MAX_DATASET_FOOTER_BYTES
    target_file_size_bytes: int = DEFAULT_GEOPARQUET_TARGET_FILE_BYTES
    aggregate_buffer_bytes: int = DEFAULT_GEOPARQUET_AGGREGATE_BUFFER_BYTES
    preflight_chunk_rows: int = 1_000
    max_row_group_rows: int = 100_000
    compression_level: int = 15
    force_s2: bool = False
    s2_candidate_levels: tuple[int, ...] = DEFAULT_S2_CANDIDATE_LEVELS
    # Retained for callers that customized the original policy fields.
    s2_fine_level: int = 12
    s2_parent_candidates: tuple[int, ...] = DEFAULT_S2_CANDIDATE_LEVELS


@dataclass(frozen=True)
class GeoParquetWriteResult:
    paths: list[Path]
    glob_path: str
    partitioning: str
    partition_columns: list[str]
    source_metadata: dict[str, Any]


DEFAULT_ADMIN_PARTITION_CANDIDATES = (
    "statefp",
    "state_fips",
    "state",
    "stusps",
    "countyfp",
    "county_fips",
    "county",
    "fips",
    "huc",
    "huc2",
    "huc4",
    "huc6",
    "huc8",
)


GEOPARQUET_POLICY_REGISTRY: dict[tuple[str, str], GeoParquetWritePolicy] = {
    ("nfhl", "national-flood-hazard-layer-area-nfhl-1-east"): GeoParquetWritePolicy(
        derived_prefix_column="DFIRM_ID",
        derived_prefix_partitions=(("state_fips", 2),),
    ),
    ("nfhl", "national-flood-hazard-layer-area-nfhl-1-west"): GeoParquetWritePolicy(
        derived_prefix_column="DFIRM_ID",
        derived_prefix_partitions=(("state_fips", 2),),
    ),
    (
        "nfhl",
        "national-flood-hazard-layer-nfhl-geopackage-area-1-east",
    ): GeoParquetWritePolicy(
        derived_prefix_column="DFIRM_ID",
        derived_prefix_partitions=(("state_fips", 2),),
    ),
    (
        "nfhl",
        "national-flood-hazard-layer-nfhl-geopackage-area-1-west",
    ): GeoParquetWritePolicy(
        derived_prefix_column="DFIRM_ID",
        derived_prefix_partitions=(("state_fips", 2),),
    ),
    ("nfhl", "national-flood-hazard-layer-line-nfhl-1"): GeoParquetWritePolicy(
        derived_prefix_column="DFIRM_ID",
        derived_prefix_partitions=(("state_fips", 2),),
        max_row_group_rows=50_000,
    ),
    ("nfhl", "water-lines-1"): GeoParquetWritePolicy(
        derived_prefix_column="DFIRM_ID",
        derived_prefix_partitions=(("state_fips", 2),),
    ),
    ("census-block-groups-3", "census-block-groups-3"): GeoParquetWritePolicy(
        # TIGER/Line source metadata uses STATE; retain STATEFP for older inputs.
        force_admin_columns=("STATEFP", "STATE")
    ),
    ("voting-districts", "voting-districts"): GeoParquetWritePolicy(
        force_admin_columns=("STATE",)
    ),
    ("nhd", "flowline-large-scale-2"): GeoParquetWritePolicy(
        derived_prefix_column="REACHCODE",
        derived_prefix_partitions=(("huc2", 2),),
    ),
    ("nhd", "waterbody-large-scale-2"): GeoParquetWritePolicy(force_s2=True),
    ("nhd", "flowline-small-scale-2"): GeoParquetWritePolicy(force_s2=True),
    ("nhd", "flowline"): GeoParquetWritePolicy(force_admin_columns=("workunitid",)),
    ("nhd", "area-large-scale-2"): GeoParquetWritePolicy(force_s2=True),
    ("wbd", "10-digit-hu-watershed"): GeoParquetWritePolicy(
        derived_huc_column="huc10",
        derived_huc_partition_columns=("huc2",),
    ),
    ("wbd", "12-digit-hu-subwatershed"): GeoParquetWritePolicy(
        derived_huc_column="huc12",
        derived_huc_partition_columns=("huc2",),
    ),
    ("wbd", "wbdline"): GeoParquetWritePolicy(force_admin_columns=("hudigit",)),
}


def geoparquet_policy_for(dataset_slug: str, file_slug: str) -> GeoParquetWritePolicy:
    return (
        GEOPARQUET_POLICY_REGISTRY.get((dataset_slug, file_slug))
        or GEOPARQUET_POLICY_REGISTRY.get((dataset_slug, "*"))
        or GeoParquetWritePolicy()
    )


@dataclass(frozen=True)
class ShapefileZipPolicy:
    max_estimated_zip_bytes: int = 500 * 1024 * 1024
    disabled_dataset_families: tuple[str, ...] = (
        "nfhl",
        "nhd",
        "wbd",
        "2020-census-blocks-1",
        "frs",
        "address-ranges",
        "voting-districts",
    )


@dataclass(frozen=True)
class ShapefileZipResult:
    created: bool
    path: Optional[Path]
    reason: Optional[str] = None


class _StorageAdapter:
    """Storage adapter with logical writes and storage-qualified reads.

    Upload/list inputs are logical keys. Upload/list outputs and every read-like
    input are storage-qualified keys.
    """

    def __init__(self, resource: StagingStorageResource | PublishedStorageResource):
        self.resource = resource
        self.bucket = resource.bucket
        self.prefix = resource.prefix.strip("/")
        self.local_dir = Path(resource.local_dir).resolve()
        self.use_local = resource.use_local or not resource.bucket
        self.fs = None
        if not self.use_local:
            import gcsfs

            self.fs = gcsfs.GCSFileSystem()

    def qualify_key(self, logical_path: str) -> str:
        """Convert one logical unprefixed key to a storage-qualified key."""
        normalized = logical_path.lstrip("/")
        if not self.prefix:
            return normalized
        return f"{self.prefix}/{normalized}"

    def logical_key(self, qualified_path: str) -> str:
        """Remove this adapter's configured prefix from a qualified key."""
        normalized = qualified_path.lstrip("/")
        if not self.prefix:
            return normalized
        qualified_prefix = f"{self.prefix}/"
        if not normalized.startswith(qualified_prefix):
            raise ValueError(
                f"Storage key '{qualified_path}' is not qualified by '{self.prefix}'."
            )
        return normalized.removeprefix(qualified_prefix)

    async def list_files(self, logical_prefix: str) -> list[str]:
        prefix = self.qualify_key(logical_prefix)
        if self.use_local:
            p = (self.local_dir / prefix).resolve()
            if not p.exists():
                return []
            if p.is_file():
                return [str(p.relative_to(self.local_dir))]
            return [
                str(x.relative_to(self.local_dir)) for x in p.rglob("*") if x.is_file()
            ]
        path = f"{self.bucket}/{prefix.strip('/')}"
        try:
            items = self.fs.ls(path)
            files = []
            for item in items:
                if item.endswith("/"):
                    continue
                rel = item.replace(f"{self.bucket}/", "", 1)
                files.append(rel)
            return sorted(files)
        except Exception:
            return []

    async def file_exists(self, qualified_path: str) -> bool:
        remote_path = qualified_path.lstrip("/")
        if self.use_local:
            return (self.local_dir / remote_path).exists()
        return bool(self.fs.exists(f"{self.bucket}/{remote_path}"))

    async def download_file(self, qualified_path: str, local_path: Path) -> None:
        remote_path = qualified_path.lstrip("/")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if self.use_local:
            src = self.local_dir / remote_path
            if src.is_dir():
                shutil.copytree(src, local_path, dirs_exist_ok=True)
            else:
                shutil.copy2(src, local_path)
            return
        self.fs.get(f"{self.bucket}/{remote_path}", str(local_path))

    async def upload_file(self, local_path: Path, logical_path: str) -> str:
        remote_path = self.qualify_key(logical_path)
        if self.use_local:
            dst = self.local_dir / remote_path
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, dst)
            return remote_path
        self.fs.put(str(local_path), f"{self.bucket}/{remote_path}")
        return remote_path

    async def get_file_size(self, qualified_path: str) -> int:
        remote_path = qualified_path.lstrip("/")
        if self.use_local:
            p = self.local_dir / remote_path
            return p.stat().st_size if p.exists() else 0
        try:
            info = self.fs.info(f"{self.bucket}/{remote_path}")
            return int(info.get("size", 0))
        except Exception:
            return 0

    def get_public_url(self, qualified_path: str) -> str:
        remote_path = qualified_path.lstrip("/")
        if self.use_local:
            return f"file://{self.local_dir / remote_path}"
        return f"https://storage.googleapis.com/{self.bucket}/{remote_path}"

    def path_to_storage_uri(self, qualified_path: str) -> str:
        path = qualified_path.lstrip("/")
        if self.use_local:
            return str(self.local_dir / path)
        return f"gs://{self.bucket}/{path}"

    async def read_bytes(self, qualified_path: str) -> bytes:
        remote_path = qualified_path.lstrip("/")
        if self.use_local:
            return (self.local_dir / remote_path).read_bytes()
        return self.fs.read_bytes(f"{self.bucket}/{remote_path}")


def _detect_format_from_path(path: str) -> str:
    path_lower = path.lower()
    if path_lower.endswith(".gdb.zip"):
        return "file_geodatabase"
    for fmt, _ in FORMAT_PRIORITY:
        if f"-{fmt}.zip" in path_lower or f"-{fmt.replace('_', '-')}.zip" in path_lower:
            return fmt
    if path_lower.endswith(".geojson") or "-geojson.zip" in path_lower:
        return "geojson"
    if path_lower.endswith(".gpkg"):
        return "geopackage"
    if path_lower.endswith(".shp"):
        return "shapefile"
    if path_lower.endswith(".gdb"):
        return "file_geodatabase"
    return "unknown"


def _strip_format_suffix(name: str) -> str:
    base = name
    for suffix in FORMAT_SUFFIXES:
        if base.endswith(suffix):
            return base[: -len(suffix)]
    return base


def _normalize_duplicate_leading_folder(path: str) -> str:
    parts = [p for p in path.split("/") if p]
    while len(parts) >= 3 and parts[0] == parts[1]:
        parts = parts[1:]
    return "/".join(parts)


def get_memory_usage_mb() -> float:
    if not PSUTIL_AVAILABLE:
        return 0.0
    try:
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)
    except Exception:
        return 0.0


def _get_fiona_driver(format_type: str) -> Optional[str]:
    driver_map = {
        "shapefile": "ESRI Shapefile",
        "geojson": "GeoJSON",
        "geopackage": "GPKG",
        "file_geodatabase": "OpenFileGDB",
    }
    return driver_map.get(format_type)


def _ensure_id_column(gdf: gpd.GeoDataFrame, start_id: int = 1) -> gpd.GeoDataFrame:
    return gdf


def _safe_layer_suffix(layer_name: str) -> str:
    return layer_name.replace("/", "-").replace("\\", "-")


def list_layers_in_file(
    file_path: Path, format_type: str
) -> list[tuple[str, Optional[str]]]:
    if format_type in {"geopackage", "file_geodatabase"}:
        try:
            layers = fiona.listlayers(str(file_path))
            return [(name, None) for name in layers] or [("default", None)]
        except Exception:
            return [("default", None)]
    return [("default", None)]


def _extract_geospatial_from_zip(
    zip_file: Path, extract_dir: Path
) -> Optional[tuple[str, Path]]:
    with zipfile.ZipFile(zip_file, "r") as zf:
        zf.extractall(extract_dir)

    geodatabases = sorted(
        path
        for path in extract_dir.rglob("*")
        if path.is_dir() and path.suffix.lower() == ".gdb"
    )
    for format_name in CANONICAL_SOURCE_FORMAT_PRECEDENCE:
        if format_name == "file_geodatabase":
            if geodatabases:
                return (format_name, geodatabases[0])
            continue
        for extension in SOURCE_FORMAT_EXTENSIONS[format_name]:
            found = next(
                (
                    path
                    for path in sorted(extract_dir.rglob("*"))
                    if path.is_file() and path.suffix.lower() == extension
                ),
                None,
            )
            if found:
                return (format_name, found)
    return None


async def unzip_from_storage(
    storage: _StorageAdapter, remote_path: str, extract_dir: Path
) -> Optional[tuple[str, Path]]:
    remote_name = Path(remote_path).name
    local_file = extract_dir / (remote_name or "source_file")
    await storage.download_file(remote_path, local_file)

    suffix = local_file.suffix.lower()
    if suffix in {".geojson", ".gpkg", ".shp"}:
        return (
            {".geojson": "geojson", ".gpkg": "geopackage", ".shp": "shapefile"}[suffix],
            local_file,
        )

    if suffix == ".zip":
        try:
            return _extract_geospatial_from_zip(local_file, extract_dir)
        except zipfile.BadZipFile:
            return None

    try:
        return _extract_geospatial_from_zip(local_file, extract_dir)
    except zipfile.BadZipFile:
        return None


def _build_dest_folder(source_rel_path: str) -> str:
    normalized = _normalize_duplicate_leading_folder(source_rel_path)
    parts = [p for p in normalized.split("/") if p]
    if not parts:
        return ""
    zip_stem = Path(parts[-1]).stem
    parent = "/".join(parts[:-1])
    if parent:
        return f"{parent}/{zip_stem}/"
    return f"{zip_stem}/"


def _build_layer_filename(base_filename: str, layer_name: str) -> str:
    if layer_name == "default":
        return base_filename
    return f"{base_filename}-{_safe_layer_suffix(layer_name)}"


def _write_geodataframe_parquet(
    gdf: gpd.GeoDataFrame,
    output_path: Path,
    *,
    row_group_size: Optional[int] = None,
    data_page_size_bytes: Optional[int] = None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    kwargs: dict[str, Any] = {
        "compression": "zstd",
        "schema_version": "1.1.0",
        "index": False,
    }
    if row_group_size:
        kwargs["row_group_size"] = row_group_size
    if data_page_size_bytes:
        kwargs["data_page_size"] = data_page_size_bytes
    attempts = [
        {**kwargs, "compression_level": 15, "write_covering_bbox": True},
        {**kwargs, "write_covering_bbox": True},
        {
            **{key: value for key, value in kwargs.items() if key != "schema_version"},
            "write_covering_bbox": True,
        },
    ]
    last_error: TypeError | None = None
    for attempt in attempts:
        try:
            gdf.to_parquet(output_path, **attempt)
            return
        except TypeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error


def _has_spatial_features(gdf: pd.DataFrame) -> bool:
    if not isinstance(gdf, gpd.GeoDataFrame):
        return False
    try:
        geom = gdf.geometry
    except AttributeError:
        return False
    if geom is None:
        return False
    try:
        return bool((geom.notna() & ~geom.is_empty).any())
    except Exception:
        return bool(geom.notna().any())


def _sanitize_geopackage_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    reserved = {"id", "fid", "ogc_fid"}
    rename_map: dict[str, str] = {}
    existing = set(gdf.columns)
    for column in gdf.columns:
        if column == gdf.geometry.name:
            continue
        if column.lower() not in reserved:
            continue
        base = f"source_{column}"
        candidate = base
        counter = 2
        while candidate in existing or candidate in rename_map.values():
            candidate = f"{base}_{counter}"
            counter += 1
        rename_map[column] = candidate
    if not rename_map:
        return gdf
    return gdf.rename(columns=rename_map)


def _estimate_parquet_bytes_per_row(gdf: gpd.GeoDataFrame) -> float:
    if len(gdf) == 0:
        return 1.0
    sample_n = min(200, len(gdf))
    step = max(1, len(gdf) // sample_n)
    sample = gdf.iloc[::step].head(sample_n)
    with tempfile.TemporaryDirectory() as tmpdir:
        sample_path = Path(tmpdir) / "sample.parquet"
        _write_geodataframe_parquet(sample, sample_path)
        return max(1.0, sample_path.stat().st_size / max(1, len(sample)))


def _row_group_size_for_policy(
    gdf: gpd.GeoDataFrame, policy: GeoParquetWritePolicy
) -> int:
    bytes_per_row = _estimate_parquet_bytes_per_row(gdf)
    return max(1, int(policy.target_row_group_bytes / bytes_per_row))


def _estimated_parquet_size(gdf: gpd.GeoDataFrame) -> int:
    return int(_estimate_parquet_bytes_per_row(gdf) * max(1, len(gdf)))


def _is_usable_admin_column(gdf: gpd.GeoDataFrame, column: str) -> bool:
    if column not in gdf.columns:
        return False
    series = gdf[column]
    if len(series) == 0:
        return False
    non_null_ratio = float(series.notna().mean())
    if non_null_ratio < 0.95:
        return False
    cardinality = int(series.nunique(dropna=True))
    if cardinality <= 1:
        return False
    return cardinality <= max(1, len(series) // 2)


def _select_admin_column(
    gdf: gpd.GeoDataFrame,
    policy: GeoParquetWritePolicy,
    estimated_size: int,
) -> Optional[str]:
    if estimated_size < policy.large_dataset_threshold_bytes:
        return None
    for column in policy.force_admin_columns:
        if column in gdf.columns:
            return column
    for column in policy.candidate_admin_columns:
        if _is_usable_admin_column(gdf, column):
            return column
    return None


def _annotate_s2_and_hilbert(
    gdf: gpd.GeoDataFrame, policy: GeoParquetWritePolicy
) -> tuple[gpd.GeoDataFrame, dict[str, str]]:
    internal_columns = _allocate_generated_columns(
        ["s2_cell", "s2_parent_cell", "hilbert_cell"],
        {str(column) for column in gdf.columns},
    )
    try:
        import s2sphere
    except Exception:
        out = gdf.copy()
        reps = out.geometry.representative_point()
        out[internal_columns["s2_cell"]] = [0 for _ in reps]
        out[internal_columns["s2_parent_cell"]] = [0 for _ in reps]
        out[internal_columns["hilbert_cell"]] = [
            int((point.x + 180.0) * 1_000_000) + int((point.y + 90.0) * 1_000)
            for point in reps
        ]
        return (
            out.sort_values(
                [
                    internal_columns["s2_parent_cell"],
                    internal_columns["hilbert_cell"],
                ],
                kind="stable",
            ),
            internal_columns,
        )

    out = _to_wgs84(gdf.copy())
    reps = out.geometry.representative_point()
    parent_level = _pick_s2_parent_level(reps, policy)
    fine_cells: list[int] = []
    parent_cells: list[int] = []
    hilbert_cells: list[int] = []
    for point in reps:
        cell = s2sphere.CellId.from_lat_lng(
            s2sphere.LatLng.from_degrees(float(point.y), float(point.x))
        )
        fine_cells.append(cell.parent(policy.s2_fine_level).id())
        parent = cell.parent(parent_level)
        parent_cells.append(parent.id())
        hilbert_cells.append(_hilbert_like_key(point.x, point.y))
    out[internal_columns["s2_cell"]] = fine_cells
    out[internal_columns["s2_parent_cell"]] = parent_cells
    out[internal_columns["hilbert_cell"]] = hilbert_cells
    return (
        out.sort_values(
            [
                internal_columns["s2_parent_cell"],
                internal_columns["hilbert_cell"],
            ],
            kind="stable",
        ).reset_index(drop=True),
        internal_columns,
    )


def _pick_s2_parent_level(points: gpd.GeoSeries, policy: GeoParquetWritePolicy) -> int:
    try:
        import s2sphere
    except Exception:
        return policy.s2_parent_candidates[0]
    target_rows = max(1, int(policy.target_row_group_bytes / max(1.0, 1024.0)))
    best_level = policy.s2_parent_candidates[0]
    best_distance = float("inf")
    for level in policy.s2_parent_candidates:
        counts: dict[int, int] = {}
        for point in points:
            cell_id = (
                s2sphere.CellId.from_lat_lng(
                    s2sphere.LatLng.from_degrees(float(point.y), float(point.x))
                )
                .parent(level)
                .id()
            )
            counts[cell_id] = counts.get(cell_id, 0) + 1
        median = float(pd.Series(list(counts.values())).median()) if counts else 1.0
        distance = abs(median - target_rows)
        if distance < best_distance:
            best_distance = distance
            best_level = level
    return best_level


def _hilbert_like_key(x: float, y: float) -> int:
    """Return the unsigned leaf-cell ID from S2's Hilbert space-filling curve."""
    longitude = max(-180.0, min(float(x), 180.0))
    latitude = max(-90.0, min(float(y), 90.0))
    return s2sphere.CellId.from_lat_lng(
        s2sphere.LatLng.from_degrees(latitude, longitude)
    ).id()


def write_geoparquet_dataset(
    gdf: gpd.GeoDataFrame,
    output_dir: Path,
    layer_filename: str,
    policy: GeoParquetWritePolicy,
) -> GeoParquetWriteResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    estimated_size = _estimated_parquet_size(gdf)
    row_group_size = _row_group_size_for_policy(gdf, policy)
    admin_column = _select_admin_column(gdf, policy, estimated_size)
    source_columns = {str(column) for column in gdf.columns}

    if estimated_size >= policy.large_dataset_threshold_bytes and admin_column is None:
        sorted_gdf, internal_columns = _annotate_s2_and_hilbert(gdf, policy)
        hive_columns = _allocate_semantic_hive_keys(
            ["s2_parent_cell"], source_columns | set(internal_columns.values())
        )
        paths = _write_partitioned_parquet(
            sorted_gdf,
            output_dir,
            internal_columns["s2_parent_cell"],
            row_group_size,
            hive_partition_columns={
                internal_columns["s2_parent_cell"]: hive_columns["s2_parent_cell"]
            },
            generated_columns=set(internal_columns.values()),
        )
        return GeoParquetWriteResult(
            paths=paths,
            glob_path="**/*.parquet",
            partitioning="s2",
            partition_columns=["s2_parent_cell"],
            source_metadata={
                "hive_partitioned": True,
                "partitioning": "s2",
                "partition_columns": ["s2_parent_cell"],
                "hive_partition_columns": hive_columns,
                "row_group_target_bytes": policy.target_row_group_bytes,
                "s2_columns": list(internal_columns.values()),
                "s2_column_mapping": internal_columns,
            },
        )

    if admin_column:
        sorted_gdf = gdf.sort_values(admin_column, kind="stable").reset_index(drop=True)
        hive_columns = _allocate_semantic_hive_keys([admin_column], source_columns)
        paths = _write_partitioned_parquet(
            sorted_gdf,
            output_dir,
            admin_column,
            row_group_size,
            hive_partition_columns={admin_column: hive_columns[admin_column]},
        )
        return GeoParquetWriteResult(
            paths=paths,
            glob_path="**/*.parquet",
            partitioning="admin",
            partition_columns=[admin_column],
            source_metadata={
                "hive_partitioned": True,
                "partitioning": "admin",
                "partition_columns": [admin_column],
                "hive_partition_columns": hive_columns,
                "row_group_target_bytes": policy.target_row_group_bytes,
            },
        )

    output_path = output_dir / f"{layer_filename}.parquet"
    _write_geodataframe_parquet(
        gdf.reset_index(drop=True), output_path, row_group_size=row_group_size
    )
    return GeoParquetWriteResult(
        paths=[output_path],
        glob_path=output_path.name,
        partitioning="single_file",
        partition_columns=[],
        source_metadata={
            "hive_partitioned": False,
            "partitioning": "single_file",
            "row_group_target_bytes": policy.target_row_group_bytes,
        },
    )


def _write_partitioned_parquet(
    gdf: gpd.GeoDataFrame,
    output_dir: Path,
    partition_column: str | list[str],
    row_group_size: int,
    hive_partition_columns: dict[str, str] | None = None,
    generated_columns: set[str] | None = None,
) -> list[Path]:
    partition_columns = (
        [partition_column] if isinstance(partition_column, str) else partition_column
    )
    paths: list[Path] = []
    group_key = (
        partition_columns[0] if len(partition_columns) == 1 else partition_columns
    )
    for _idx, (value, part) in enumerate(
        gdf.groupby(group_key, dropna=False, sort=True)
    ):
        values = value if isinstance(value, tuple) else (value,)
        part_dir = output_dir
        for column, raw_value in zip(partition_columns, values):
            hive_column = (hive_partition_columns or {}).get(column, column)
            part_dir = part_dir / f"{hive_column}={_encoded_hive_value(raw_value)}"
        part_path = part_dir / "part-000.parquet"
        published_part = part.drop(
            columns=list(generated_columns or ()),
            errors="ignore",
        )
        _write_geodataframe_parquet(
            published_part.reset_index(drop=True),
            part_path,
            row_group_size=row_group_size,
        )
        paths.append(part_path)
    return paths


@dataclass(frozen=True)
class GeoParquetOutputLayout:
    path: str
    relative_path: str
    file_size_bytes: int
    footer_size_bytes: int
    sha256: str
    row_counts: list[int]
    row_group_uncompressed_sizes: list[int]


@dataclass(frozen=True)
class GeoParquetLayout:
    schema_version: int
    layer: str
    source_format: str
    feature_count: int
    partition_strategy: str
    partition_columns: list[str]
    hive_partition_columns: dict[str, str]
    chosen_s2_level: int | None
    footer_size_bytes: int
    thresholds: dict[str, int | float | str]
    outputs: list[GeoParquetOutputLayout]
    validation_status: str


@dataclass(frozen=True)
class _GeoParquetPreflight:
    feature_count: int
    uncompressed_bytes: int
    estimated_compressed_bytes: int
    compression_ratio: float
    compression_estimation_method: str
    estimate_multiplier: float
    partitioning: str
    partition_columns: list[str]
    hive_partition_columns: dict[str, str]
    chosen_s2_level: int | None
    resolved_policy: GeoParquetWritePolicy


@dataclass
class _ParquetWriterState:
    path: Path
    writer: pq.ParquetWriter
    footer_estimate_bytes: int


class _SpatialFeatureSpool:
    """Disk-backed feature spool providing a global sort per final partition."""

    _COMMIT_INTERVAL = 1_000

    def __init__(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory(
            prefix="geoparquet-spatial-sort-"
        )
        database_path = Path(self._temporary_directory.name) / "features.sqlite3"
        self._connection: sqlite3.Connection | None = sqlite3.connect(database_path)
        self._connection.execute("PRAGMA temp_store=FILE")
        self._connection.execute("PRAGMA cache_size=-32768")
        self._connection.execute(
            """
            CREATE TABLE features (
                partition_dir TEXT NOT NULL,
                sort_key TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                estimated_bytes INTEGER NOT NULL,
                feature BLOB NOT NULL
            )
            """
        )
        self._pending = 0

    def add(
        self,
        partition_dir: str,
        hilbert_key: int,
        ordinal: int,
        estimated_bytes: int,
        feature: dict[str, Any],
    ) -> None:
        if self._connection is None:
            raise RuntimeError("Spatial feature spool is closed.")
        self._connection.execute(
            "INSERT INTO features VALUES (?, ?, ?, ?, ?)",
            (
                partition_dir,
                f"{hilbert_key:016x}",
                ordinal,
                estimated_bytes,
                zlib.compress(
                    pickle.dumps(feature, protocol=pickle.HIGHEST_PROTOCOL), level=1
                ),
            ),
        )
        self._pending += 1
        if self._pending >= self._COMMIT_INTERVAL:
            self._connection.commit()
            self._pending = 0

    def prepare(self) -> None:
        if self._connection is None:
            raise RuntimeError("Spatial feature spool is closed.")
        self._connection.commit()
        self._connection.execute(
            "CREATE INDEX feature_order ON features(partition_dir, sort_key, ordinal)"
        )
        self._connection.commit()

    def partitions(self) -> Iterable[str]:
        if self._connection is None:
            raise RuntimeError("Spatial feature spool is closed.")
        rows = self._connection.execute(
            "SELECT DISTINCT partition_dir FROM features ORDER BY partition_dir"
        )
        return (str(row[0]) for row in rows)

    def sorted_features(
        self, partition_dir: str
    ) -> Iterable[tuple[dict[str, Any], int]]:
        if self._connection is None:
            raise RuntimeError("Spatial feature spool is closed.")
        rows = self._connection.execute(
            """
            SELECT feature, estimated_bytes FROM features
            WHERE partition_dir = ? ORDER BY sort_key, ordinal
            """,
            (partition_dir,),
        )
        return (
            (pickle.loads(zlib.decompress(bytes(blob))), int(estimated_bytes))
            for blob, estimated_bytes in rows
        )

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        self._temporary_directory.cleanup()


class _PreflightHistogramStore:
    """Exact SQLite-backed histograms with memory independent of bin count."""

    def __init__(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory(
            prefix="geoparquet-preflight-"
        )
        self.database_path = Path(self._temporary_directory.name) / "histograms.sqlite3"
        self._connection: sqlite3.Connection | None = sqlite3.connect(
            self.database_path
        )
        self._connection.execute(
            """
            CREATE TABLE histogram (
                kind TEXT NOT NULL,
                name TEXT NOT NULL,
                level INTEGER NOT NULL,
                bin TEXT NOT NULL,
                byte_count INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                PRIMARY KEY (kind, name, level, bin)
            ) WITHOUT ROWID
            """
        )
        self._connection.create_function("s2_parent_bin", 2, _s2_parent_bin)

    def __enter__(self) -> _PreflightHistogramStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def add_many(
        self,
        rows: Iterable[tuple[str, str, int, str, int, int]],
    ) -> None:
        connection = self._require_connection()
        connection.executemany(
            """
            INSERT INTO histogram (
                kind, name, level, bin, byte_count, row_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (kind, name, level, bin) DO UPDATE SET
                byte_count = byte_count + excluded.byte_count,
                row_count = row_count + excluded.row_count
            """,
            rows,
        )

    def roll_up_s2(self, kind: str, name: str, levels: tuple[int, ...]) -> None:
        """Derive coarser S2 histograms from the finest level."""
        connection = self._require_connection()
        if not levels:
            return
        source_level = max(levels)
        for level in sorted(
            (candidate for candidate in levels if candidate != source_level),
            reverse=True,
        ):
            connection.execute(
                """
                INSERT INTO histogram (kind, name, level, bin, byte_count, row_count)
                SELECT kind, name, ?, s2_parent_bin(bin, ?),
                       SUM(byte_count), SUM(row_count)
                FROM histogram
                WHERE kind = ? AND name = ? AND level = ?
                GROUP BY kind, name, s2_parent_bin(bin, ?)
                """,
                (level, level, kind, name, source_level, level),
            )
            source_level = level
        connection.commit()

    def cardinality(self, kind: str, name: str = "") -> int:
        row = (
            self._require_connection()
            .execute(
                "SELECT COUNT(*) FROM histogram WHERE kind = ? AND name = ?",
                (kind, name),
            )
            .fetchone()
        )
        return int(row[0]) if row else 0

    def max_bytes(self, kind: str, name: str = "", level: int | None = None) -> int:
        if level is None:
            row = (
                self._require_connection()
                .execute(
                    "SELECT MAX(byte_count) FROM histogram WHERE kind = ? AND name = ?",
                    (kind, name),
                )
                .fetchone()
            )
        else:
            row = (
                self._require_connection()
                .execute(
                    """
                SELECT MAX(byte_count) FROM histogram
                WHERE kind = ? AND name = ? AND level = ?
                """,
                    (kind, name, level),
                )
                .fetchone()
            )
        return int(row[0]) if row and row[0] is not None else 0

    def level_maxima(
        self,
        kind: str,
        name: str,
        levels: tuple[int, ...],
    ) -> dict[int, dict[str, int]]:
        return {
            level: {"largest": self.max_bytes(kind, name, level)} for level in levels
        }

    def close(self) -> None:
        if self._connection is not None:
            self._connection.commit()
            self._connection.close()
            self._connection = None
        self._temporary_directory.cleanup()

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("Preflight histogram store is closed.")
        return self._connection


HIVE_NULL_SENTINEL = "__HIVE_DEFAULT_PARTITION__"
HIVE_ESCAPE_SAFE = "-._~"


def _is_missing_scalar(value: Any) -> bool:
    if value is None:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    try:
        return bool(missing)
    except (TypeError, ValueError):
        return False


def _encoded_hive_value(value: Any) -> str:
    if _is_missing_scalar(value):
        return HIVE_NULL_SENTINEL
    raw_value = str(value)
    if raw_value.casefold() == HIVE_NULL_SENTINEL.casefold():
        raise ValueError(
            f"Hive partition value {raw_value!r} collides with the null sentinel "
            f"{HIVE_NULL_SENTINEL!r}."
        )
    return quote(raw_value, safe=HIVE_ESCAPE_SAFE)


def _allocate_semantic_hive_keys(
    columns: list[str], _source_columns: set[str]
) -> dict[str, str]:
    mapping: dict[str, str] = {}
    allocated: dict[str, str] = {}
    for column in columns:
        candidate = quote(column, safe=HIVE_ESCAPE_SAFE)
        previous = allocated.get(candidate.casefold())
        if previous is not None and previous != column:
            raise ValueError(
                "Hive partition columns collide after escaping: "
                f"{previous!r} and {column!r}."
            )
        mapping[column] = candidate
        allocated[candidate.casefold()] = column
    return mapping


def _allocate_generated_columns(
    columns: list[str], source_columns: set[str]
) -> dict[str, str]:
    """Allocate private physical names for generated sort columns."""
    reserved = {column.casefold() for column in source_columns}
    mapping: dict[str, str] = {}
    for column in columns:
        base = f"__hifld_{column}"
        candidate = base
        counter = 0
        while candidate.casefold() in reserved:
            counter += 1
            candidate = f"{base}_{counter}"
        mapping[column] = candidate
        reserved.add(candidate.casefold())
    return mapping


def _layer_output_namespace(layer_filename: str) -> str:
    return f"layer-{quote(layer_filename, safe='-._~')}"


def _feature_properties(feature: Any) -> dict[str, Any]:
    try:
        return dict(feature.get("properties") or {})
    except AttributeError:
        return dict(feature["properties"] or {})


def _feature_geometry(feature: Any) -> Any:
    try:
        return feature.get("geometry")
    except AttributeError:
        return feature["geometry"]


def _feature_with_properties(
    feature: Any, properties: dict[str, Any]
) -> dict[str, Any]:
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": _feature_geometry(feature),
    }


def _schema_property_names(schema: dict[str, Any]) -> set[str]:
    properties = schema.get("properties") or {}
    try:
        return set(properties.keys())
    except AttributeError:
        return set(properties)


def _resolve_source_column(names: set[str], configured: str, field_name: str) -> str:
    matches = sorted(name for name in names if name.casefold() == configured.casefold())
    if not matches:
        raise ValueError(
            f"Configured {field_name} '{configured}' is absent from source properties."
        )
    if len(matches) > 1:
        raise ValueError(
            f"Configured {field_name} '{configured}' is ambiguous in source properties: "
            + ", ".join(matches)
        )
    return matches[0]


def _resolve_first_source_column(
    names: set[str], configured_columns: tuple[str, ...]
) -> str:
    for configured in configured_columns:
        if any(name.casefold() == configured.casefold() for name in names):
            return _resolve_source_column(names, configured, "force_admin_columns")
    alternatives = ", ".join(repr(column) for column in configured_columns)
    raise ValueError(
        "None of the configured force_admin_columns alternatives are present in "
        f"source properties: {alternatives}."
    )


def _coerce_gdf_to_fiona_schema(
    gdf: gpd.GeoDataFrame,
    schema: dict[str, Any],
) -> gpd.GeoDataFrame:
    """Keep streaming chunks from inferring different Arrow types for sparse fields."""
    properties = schema.get("properties") or {}
    if not properties:
        return gdf

    coerced = gdf.copy()
    for column, raw_type in properties.items():
        if column not in coerced.columns:
            continue
        fiona_type = str(raw_type).lower()
        try:
            list_type = _fiona_list_arrow_type(fiona_type)
            if list_type is not None:
                values = [
                    None
                    if value is None or value is pd.NA
                    else list(value)
                    if isinstance(value, (list, tuple))
                    else value
                    for value in coerced[column].tolist()
                ]
                coerced[column] = pd.Series(
                    pd.array(values, dtype=pd.ArrowDtype(list_type)),
                    index=coerced.index,
                )
            elif fiona_type == "json":
                values = [
                    None
                    if value is None or value is pd.NA
                    else json.dumps(value, separators=(",", ":"), sort_keys=True)
                    for value in coerced[column].tolist()
                ]
                coerced[column] = pd.Series(
                    pd.array(values, dtype=pd.ArrowDtype(pa.json_())),
                    index=coerced.index,
                )
            elif fiona_type.startswith(("str", "date", "time")):
                coerced[column] = coerced[column].astype("string")
            elif fiona_type.startswith(("int", "uint")):
                coerced[column] = pd.to_numeric(
                    coerced[column], errors="coerce"
                ).astype("Int64")
            elif fiona_type.startswith(("float", "real", "double")):
                coerced[column] = pd.to_numeric(
                    coerced[column], errors="coerce"
                ).astype("Float64")
            elif fiona_type.startswith("bool"):
                coerced[column] = coerced[column].astype("boolean")
        except (TypeError, ValueError):
            logger.debug(
                "Could not coerce column %s to Fiona type %s", column, raw_type
            )
    return coerced


def _fiona_list_arrow_type(fiona_type: str) -> pa.DataType | None:
    if not (fiona_type.startswith("list[") and fiona_type.endswith("]")):
        return None
    item_type = fiona_type[5:-1].strip()
    scalar_types = {
        "str": pa.string(),
        "string": pa.string(),
        "int": pa.int64(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "uint": pa.uint64(),
        "uint32": pa.uint32(),
        "uint64": pa.uint64(),
        "float": pa.float64(),
        "float32": pa.float32(),
        "float64": pa.float64(),
        "double": pa.float64(),
        "bool": pa.bool_(),
    }
    arrow_item_type = scalar_types.get(item_type)
    if arrow_item_type is None:
        arrow_item_type = _fiona_list_arrow_type(item_type)
    return pa.list_(arrow_item_type) if arrow_item_type is not None else None


def _select_streaming_partition_columns(
    schema: dict[str, Any],
    policy: GeoParquetWritePolicy,
    *,
    strict: bool = True,
) -> tuple[str, list[str]]:
    names = _schema_property_names(schema)
    derived_huc_column = None
    if policy.derived_huc_column and (
        strict
        or any(
            name.casefold() == policy.derived_huc_column.casefold() for name in names
        )
    ):
        derived_huc_column = _resolve_source_column(
            names, policy.derived_huc_column, "derived_huc_column"
        )
    derived_prefix_column = None
    if policy.derived_prefix_column and (
        strict
        or any(
            name.casefold() == policy.derived_prefix_column.casefold() for name in names
        )
    ):
        derived_prefix_column = _resolve_source_column(
            names, policy.derived_prefix_column, "derived_prefix_column"
        )
    forced_admin_column = None
    if policy.force_admin_columns and (
        strict
        or any(
            name.casefold() == configured.casefold()
            for name in names
            for configured in policy.force_admin_columns
        )
    ):
        forced_admin_column = _resolve_first_source_column(
            names, policy.force_admin_columns
        )
    if derived_huc_column and policy.derived_huc_partition_columns:
        return "derived_huc", list(policy.derived_huc_partition_columns)
    if derived_prefix_column and policy.derived_prefix_partitions:
        return "derived_prefix", [
            name for name, _width in policy.derived_prefix_partitions
        ]
    if forced_admin_column:
        return "admin", [forced_admin_column]
    return "single_file", []


def _select_s2_level(
    histograms: dict[int, dict[str, int]],
    target_bytes: int,
    candidate_levels: tuple[int, ...],
) -> int:
    if not candidate_levels:
        raise ValueError("At least one S2 candidate level is required.")
    for level in candidate_levels:
        bins = histograms.get(level, {})
        if max(bins.values(), default=0) <= target_bytes:
            return level
    return candidate_levels[-1]


def _s2_uncompressed_target_bytes(
    compressed_target_bytes: int, compression_ratio: float
) -> int:
    """Translate a compressed file target into an uncompressed histogram budget."""
    if compression_ratio <= 0:
        return compressed_target_bytes
    return max(1, int(compressed_target_bytes / compression_ratio))


def _policy_s2_levels(policy: GeoParquetWritePolicy) -> tuple[int, ...]:
    if policy.s2_candidate_levels != DEFAULT_S2_CANDIDATE_LEVELS:
        return policy.s2_candidate_levels
    return policy.s2_parent_candidates


def _resolved_policy_for_schema(
    schema: dict[str, Any], policy: GeoParquetWritePolicy, *, strict: bool = True
) -> GeoParquetWritePolicy:
    names = _schema_property_names(schema)
    has_admin_column = any(
        name.casefold() == configured.casefold()
        for name in names
        for configured in policy.force_admin_columns
    )
    has_huc_column = policy.derived_huc_column and any(
        name.casefold() == policy.derived_huc_column.casefold() for name in names
    )
    has_prefix_column = policy.derived_prefix_column and any(
        name.casefold() == policy.derived_prefix_column.casefold() for name in names
    )
    return replace(
        policy,
        force_admin_columns=(
            (_resolve_first_source_column(names, policy.force_admin_columns),)
            if policy.force_admin_columns and (strict or has_admin_column)
            else ()
        ),
        derived_huc_column=(
            _resolve_source_column(
                names, policy.derived_huc_column, "derived_huc_column"
            )
            if policy.derived_huc_column and (strict or has_huc_column)
            else None
        ),
        derived_prefix_column=(
            _resolve_source_column(
                names, policy.derived_prefix_column, "derived_prefix_column"
            )
            if policy.derived_prefix_column and (strict or has_prefix_column)
            else None
        ),
    )


def _representative_point(geometry: Any, transformer: Transformer | None = None) -> Any:
    if geometry is None:
        return Point(0, 0)
    geom = shape(geometry)
    if geom.is_empty:
        return Point(0, 0)
    if transformer is not None:
        geom = transform_geometry(transformer.transform, geom)
    return geom.representative_point()


def _s2_cell_for_point(point: Any, level: int) -> int:
    try:
        import s2sphere

        cell = s2sphere.CellId.from_lat_lng(
            s2sphere.LatLng.from_degrees(float(point.y), float(point.x))
        )
        return cell.parent(level).id()
    except Exception:
        return 0


def _s2_cells_for_point(point: Any, levels: tuple[int, ...]) -> dict[int, int]:
    """Return S2 parent cells for all levels using one lat/lng conversion."""
    try:
        import s2sphere

        cell = s2sphere.CellId.from_lat_lng(
            s2sphere.LatLng.from_degrees(float(point.y), float(point.x))
        )
    except Exception:
        return {level: 0 for level in levels}
    return {
        level: cell.parent(level).id() if 0 <= level <= 30 else 0 for level in levels
    }


def _s2_parent_bin(bin_value: str, level: int) -> str:
    prefix, separator, raw_cell = bin_value.rpartition("|")
    try:
        import s2sphere

        cell_id = int(raw_cell if separator else bin_value)
        if cell_id == 0 or not 0 <= level <= 30:
            return f"{prefix}|0" if separator else "0"
        parent = str(s2sphere.CellId(cell_id).parent(level).id())
        return f"{prefix}|{parent}" if separator else parent
    except Exception:
        return f"{prefix}|0" if separator else "0"


def _huc_prefix_values(
    properties: dict[str, Any],
    source_column: str | None,
    partition_columns: tuple[str, ...],
) -> dict[str, str | None]:
    values: dict[str, str | None] = {column: None for column in partition_columns}
    if not source_column:
        return values
    raw_value = properties.get(source_column)
    if raw_value is None or pd.isna(raw_value):
        return values
    huc = "".join(char for char in str(raw_value).strip() if char.isdigit())
    for width in (2, 4, 6):
        column = f"huc{width}"
        if column in values and len(huc) >= width:
            values[column] = huc[:width]
    return values


def _prefix_partition_values(
    properties: dict[str, Any],
    source_column: str | None,
    partitions: tuple[tuple[str, int], ...],
) -> dict[str, str | None]:
    values: dict[str, str | None] = {column: None for column, _width in partitions}
    if not source_column:
        return values
    raw_value = properties.get(source_column)
    if raw_value is None or pd.isna(raw_value):
        return values
    value = str(raw_value).strip()
    for column, width in partitions:
        if len(value) >= width:
            values[column] = value[:width]
    return values


def _partition_values_equal(left: Any, right: Any) -> bool:
    if _is_missing_scalar(left) or _is_missing_scalar(right):
        return _is_missing_scalar(left) and _is_missing_scalar(right)
    return str(left) == str(right)


def _matching_property_name(properties: dict[str, Any], column: str) -> str | None:
    if column in properties:
        return column
    folded = column.casefold()
    return next(
        (name for name in properties if name.casefold() == folded),
        None,
    )


def _validate_partition_value_collisions(
    properties: dict[str, Any],
    partition_columns: list[str],
    partition_values: list[Any],
) -> None:
    for column, partition_value in zip(partition_columns, partition_values):
        source_column = _matching_property_name(properties, column)
        if source_column is None:
            continue
        source_value = properties[source_column]
        if _partition_values_equal(source_value, partition_value):
            continue
        raise ValueError(
            f"Physical source column {column!r} does not match its semantic "
            f"partition value ({source_value!r} != {partition_value!r})."
        )


def _merge_derived_partition_values(
    properties: dict[str, Any], derived_values: dict[str, str | None]
) -> None:
    for column, derived_value in derived_values.items():
        source_column = _matching_property_name(properties, column)
        if source_column is not None and not _partition_values_equal(
            properties[source_column], derived_value
        ):
            raise ValueError(
                f"Physical source column {column!r} does not match its derived "
                f"partition value ({properties[source_column]!r} != {derived_value!r})."
            )
        if source_column is None:
            properties[column] = derived_value


def _prepare_streaming_feature(
    feature: Any,
    partitioning: str,
    policy: GeoParquetWritePolicy,
) -> dict[str, Any]:
    properties = _feature_properties(feature)
    if partitioning.startswith("derived_huc"):
        _merge_derived_partition_values(
            properties,
            _huc_prefix_values(
                properties,
                policy.derived_huc_column,
                policy.derived_huc_partition_columns,
            ),
        )
    elif partitioning.startswith("derived_prefix"):
        _merge_derived_partition_values(
            properties,
            _prefix_partition_values(
                properties,
                policy.derived_prefix_column,
                policy.derived_prefix_partitions,
            ),
        )
    return _feature_with_properties(feature, properties)


def _geodataframe_to_geoparquet_arrow(gdf: gpd.GeoDataFrame) -> pa.Table:
    attempts = [
        {"schema_version": "1.1.0", "write_covering_bbox": True},
        {"write_covering_bbox": True},
    ]
    last_error: TypeError | None = None
    for kwargs in attempts:
        try:
            return geopandas_arrow._geopandas_to_arrow(
                gdf,
                index=False,
                geometry_encoding="WKB",
                **kwargs,
            )
        except TypeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error


def _canonicalize_geoparquet_batch_metadata(table: pa.Table) -> pa.Table:
    """Remove batch-specific file bbox metadata while preserving covering bbox."""
    metadata = table.schema.metadata
    if not metadata or b"geo" not in metadata:
        return table
    try:
        geo_metadata = json.loads(metadata[b"geo"])
    except (TypeError, ValueError):
        return table
    if not isinstance(geo_metadata, dict):
        return table
    primary_column = geo_metadata.get("primary_column")
    columns = geo_metadata.get("columns")
    if not isinstance(primary_column, str) or not isinstance(columns, dict):
        return table
    primary_metadata = columns.get(primary_column)
    if not isinstance(primary_metadata, dict) or "bbox" not in primary_metadata:
        return table
    primary_metadata = dict(primary_metadata)
    primary_metadata.pop("bbox")
    canonical_columns = dict(columns)
    canonical_columns[primary_column] = primary_metadata
    geo_metadata = dict(geo_metadata)
    geo_metadata["columns"] = canonical_columns
    canonical_metadata = dict(metadata)
    canonical_metadata[b"geo"] = json.dumps(geo_metadata, separators=(",", ":")).encode(
        "utf-8"
    )
    return table.replace_schema_metadata(canonical_metadata)


def _bounded_compression_sample(
    table: pa.Table,
    max_bytes: int | None = None,
) -> pa.Table | None:
    sample_bytes = max(
        1,
        max_bytes
        if max_bytes is not None
        else DEFAULT_GEOPARQUET_COMPRESSION_SAMPLE_BYTES,
    )
    if table.nbytes <= sample_bytes:
        return table
    if len(table) != 1:
        sample_rows = max(
            1,
            min(len(table), int(len(table) * sample_bytes / max(1, table.nbytes))),
        )
        while sample_rows > 1:
            sample = table.slice(0, sample_rows)
            if sample.nbytes <= sample_bytes:
                return sample
            sample_rows = max(1, sample_rows // 2)
        singleton = table.slice(0, 1)
        if singleton.nbytes <= sample_bytes:
            return singleton
        table = singleton

    fields = list(table.schema)
    if not fields:
        return None
    row_values = [table.column(index).to_pylist()[0] for index in range(len(fields))]
    value_budget = max(1, sample_bytes // len(fields))
    for _attempt in range(8):
        try:
            arrays: list[pa.Array] = []
            for field, value in zip(fields, row_values):
                truncated = _truncate_compression_value(value, field.type, value_budget)
                arrays.append(pa.array([truncated], type=field.type))
            sample = pa.Table.from_arrays(arrays, schema=table.schema)
        except (TypeError, ValueError, pa.ArrowException):
            return None
        if sample.nbytes <= sample_bytes:
            return sample
        value_budget = max(1, value_budget // 2)
    return None


def _truncate_compression_value(
    value: Any, data_type: pa.DataType, max_bytes: int
) -> Any:
    if value is None:
        return None
    if (
        pa.types.is_string(data_type)
        or pa.types.is_large_string(data_type)
        or pa.types.is_string_view(data_type)
    ):
        encoded = str(value).encode("utf-8")
        return encoded[:max_bytes].decode("utf-8", errors="ignore")
    if (
        pa.types.is_binary(data_type)
        or pa.types.is_large_binary(data_type)
        or pa.types.is_binary_view(data_type)
    ):
        return bytes(value)[:max_bytes]
    if pa.types.is_dictionary(data_type):
        return _truncate_compression_value(value, data_type.value_type, max_bytes)
    if pa.types.is_map(data_type) and isinstance(value, list):
        item_count = min(len(value), max(1, max_bytes // 16))
        child_budget = max(1, max_bytes // max(1, item_count))
        return [
            (
                _truncate_compression_value(key, data_type.key_type, child_budget),
                _truncate_compression_value(item, data_type.item_type, child_budget),
            )
            for key, item in value[:item_count]
        ]
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        if not isinstance(value, list):
            return value
        item_count = min(len(value), max(1, max_bytes // 8))
        child_budget = max(1, max_bytes // max(1, item_count))
        return [
            _truncate_compression_value(item, data_type.value_type, child_budget)
            for item in value[:item_count]
        ]
    if pa.types.is_struct(data_type) and isinstance(value, dict):
        child_budget = max(1, max_bytes // max(1, len(data_type)))
        return {
            field.name: _truncate_compression_value(
                value.get(field.name), field.type, child_budget
            )
            for field in data_type
        }
    return value


def _zstd_compression_ratio(
    sample_tables: list[pa.Table], compression_level: int = 15
) -> float | None:
    """Estimate compressed bytes per Arrow byte from one bounded sample."""
    if not sample_tables:
        return None
    schema_metadata = sample_tables[0].schema.metadata
    tables = [table.replace_schema_metadata(schema_metadata) for table in sample_tables]
    sample = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
    uncompressed_bytes = max(1, sample.nbytes)
    sink = pa.BufferOutputStream()
    pq.write_table(
        sample,
        sink,
        compression="zstd",
        compression_level=compression_level,
        data_page_size=DEFAULT_DATA_PAGE_SIZE_BYTES,
        row_group_size=max(1, len(sample)),
    )
    compressed_bytes = sink.getvalue().size
    return max(0.01, compressed_bytes / uncompressed_bytes)


def _row_group_uncompressed_sizes(path: Path) -> list[int]:
    metadata = pq.ParquetFile(path).metadata
    return [
        metadata.row_group(index).total_byte_size
        for index in range(metadata.num_row_groups)
    ]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_transformer(crs: Any) -> Transformer | None:
    if not crs:
        return None
    source_crs = CRS.from_user_input(crs)
    if source_crs == CRS.from_epsg(4326):
        return None
    return Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)


def _semantic_partition_values(
    properties: dict[str, Any],
    partitioning: str,
    partition_columns: list[str],
) -> tuple[Any, ...]:
    if partitioning in {"single_file", "s2"}:
        return ()
    return tuple(
        properties.get(_matching_property_name(properties, column), None)
        for column in partition_columns
    )


def _allocate_feature_bytes(feature_sizes: list[int], batch_bytes: int) -> list[int]:
    """Allocate every measured Arrow byte to one feature without zero-byte rows."""
    if not feature_sizes:
        return []
    batch_bytes = max(len(feature_sizes), batch_bytes)
    total_weight = max(1, sum(feature_sizes))
    distributable = batch_bytes - len(feature_sizes)
    allocations = [1 + (distributable * size // total_weight) for size in feature_sizes]
    remainder = batch_bytes - sum(allocations)
    ranked = sorted(
        range(len(feature_sizes)),
        key=lambda index: (distributable * feature_sizes[index]) % total_weight,
        reverse=True,
    )
    for index in ranked[:remainder]:
        allocations[index] += 1
    return allocations


def _preflight_layer(
    file_path: Path,
    open_kwargs: dict[str, Any],
    format_type: str,
    layer_filename: str,
    policy: GeoParquetWritePolicy,
) -> _GeoParquetPreflight:
    with _PreflightHistogramStore() as histogram_store:
        return _preflight_layer_with_histograms(
            file_path,
            open_kwargs,
            format_type,
            layer_filename,
            policy,
            histogram_store,
        )


def _preflight_layer_with_histograms(
    file_path: Path,
    open_kwargs: dict[str, Any],
    format_type: str,
    layer_filename: str,
    policy: GeoParquetWritePolicy,
    histogram_store: _PreflightHistogramStore,
) -> _GeoParquetPreflight:
    del format_type, layer_filename
    feature_count = 0
    uncompressed_bytes = 0
    s2_levels = _policy_s2_levels(policy)
    finest_s2_level = max(s2_levels, default=0)
    serialized_bytes = 0
    compression_sample_tables: list[pa.Table] = []
    compression_sample_seen = 0
    compression_sample_rng = random.Random(0)
    candidate_non_null: dict[str, int] = {}

    with (
        _with_large_geojson_support(),
        fiona.open(str(file_path), **open_kwargs) as src,
    ):
        source_schema = src.schema or {}
        resolved_policy = _resolved_policy_for_schema(
            source_schema, policy, strict=False
        )
        configured_partitioning, configured_columns = (
            _select_streaming_partition_columns(source_schema, resolved_policy)
        )
        base_partitioning = "s2" if configured_partitioning == "s2" else "single_file"
        base_columns = ["s2_parent_cell"] if base_partitioning == "s2" else []
        collect_s2_histograms = (
            configured_partitioning in {"single_file", "s2"} or policy.force_s2
        )
        names = _schema_property_names(source_schema)
        resolved_candidates = [
            _resolve_source_column(names, column, "candidate_admin_columns")
            for column in policy.candidate_admin_columns
            if any(name.casefold() == column.casefold() for name in names)
        ]
        candidate_non_null = {column: 0 for column in resolved_candidates}
        current_crs = src.crs if src.crs else "EPSG:4326"
        transformer = (
            _source_transformer(current_crs) if collect_s2_histograms else None
        )
        batch: list[dict[str, Any]] = []

        def measure_batch(features: list[dict[str, Any]]) -> None:
            nonlocal compression_sample_seen
            nonlocal feature_count, serialized_bytes, uncompressed_bytes
            if not features:
                return
            gdf = gpd.GeoDataFrame.from_features(features, crs=current_crs)
            gdf = _coerce_gdf_to_fiona_schema(gdf, source_schema)
            table = _canonicalize_geoparquet_batch_metadata(
                _geodataframe_to_geoparquet_arrow(gdf)
            )
            batch_bytes = max(len(features), table.nbytes)
            feature_sizes = [
                max(1, _estimate_feature_size_bytes(feature)) for feature in features
            ]
            batch_serialized_bytes = sum(feature_sizes)
            allocated_bytes = _allocate_feature_bytes(feature_sizes, batch_bytes)
            feature_count += len(features)
            uncompressed_bytes += batch_bytes
            serialized_bytes += batch_serialized_bytes
            sample_table = _bounded_compression_sample(
                table, DEFAULT_GEOPARQUET_COMPRESSION_SAMPLE_TABLE_BYTES
            )
            if sample_table is not None:
                compression_sample_seen += 1
                sample_limit = DEFAULT_GEOPARQUET_COMPRESSION_SAMPLE_TABLES
                if len(compression_sample_tables) < sample_limit:
                    compression_sample_tables.append(sample_table)
                else:
                    replacement = compression_sample_rng.randrange(
                        compression_sample_seen
                    )
                    if replacement < len(compression_sample_tables):
                        compression_sample_tables[replacement] = sample_table
            updates: dict[tuple[str, str, int, str], tuple[int, int]] = {}

            def add_update(
                kind: str,
                name: str,
                level: int,
                bin_value: str,
                row_bytes: int,
            ) -> None:
                key = (kind, name, level, bin_value)
                byte_count, row_count = updates.get(key, (0, 0))
                updates[key] = (byte_count + row_bytes, row_count + 1)

            for feature, row_bytes in zip(features, allocated_bytes):
                prepared = _prepare_streaming_feature(
                    feature, base_partitioning, resolved_policy
                )
                properties = _feature_properties(prepared)
                if base_partitioning == "single_file":
                    for column in resolved_candidates:
                        value = properties.get(column)
                        if value is not None and not pd.isna(value):
                            candidate_non_null[column] += 1
                            add_update("candidate", column, -1, str(value), row_bytes)
                if not collect_s2_histograms:
                    continue
                semantic_values = _semantic_partition_values(
                    properties, base_partitioning, base_columns
                )
                semantic_key = json.dumps(semantic_values, default=str)
                point = _representative_point(_feature_geometry(feature), transformer)
                cell = str(
                    _s2_cells_for_point(point, (finest_s2_level,)).get(
                        finest_s2_level, 0
                    )
                )
                level = finest_s2_level
                if base_partitioning in {"single_file", "s2"}:
                    add_update("s2", "", level, cell, row_bytes)
                if semantic_values and policy.force_s2:
                    combined_key = f"{semantic_key}|{cell}"
                    add_update(
                        "semantic_s2",
                        "",
                        level,
                        combined_key,
                        row_bytes,
                    )
                if base_partitioning == "single_file":
                    for column in resolved_candidates:
                        candidate_value = properties.get(column)
                        if candidate_value is None or pd.isna(candidate_value):
                            continue
                        combined_key = f"{candidate_value}|{cell}"
                        add_update(
                            "candidate_s2",
                            column,
                            level,
                            combined_key,
                            row_bytes,
                        )
            histogram_store.add_many(
                (
                    kind,
                    name,
                    level,
                    bin_value,
                    byte_count,
                    row_count,
                )
                for (kind, name, level, bin_value), (
                    byte_count,
                    row_count,
                ) in updates.items()
            )

        for source_feature in src:
            batch.append(
                _feature_with_properties(
                    source_feature,
                    _feature_properties(source_feature),
                )
            )
            if len(batch) >= max(1, policy.preflight_chunk_rows):
                measure_batch(batch)
                batch = []
        measure_batch(batch)

    if base_partitioning in {"single_file", "s2"}:
        histogram_store.roll_up_s2("s2", "", s2_levels)
    elif policy.force_s2:
        histogram_store.roll_up_s2("semantic_s2", "", s2_levels)
    if base_partitioning == "single_file":
        for column in resolved_candidates:
            histogram_store.roll_up_s2("candidate_s2", column, s2_levels)

    if feature_count == 0:
        return _GeoParquetPreflight(
            feature_count=0,
            uncompressed_bytes=0,
            estimated_compressed_bytes=0,
            compression_ratio=1.0,
            compression_estimation_method="conservative_uncompressed",
            estimate_multiplier=1.0,
            partitioning="single_file",
            partition_columns=[],
            hive_partition_columns={},
            chosen_s2_level=None,
            resolved_policy=resolved_policy,
        )

    sampled_compression_ratio = _zstd_compression_ratio(
        compression_sample_tables, policy.compression_level
    )
    if sampled_compression_ratio is None:
        logger.warning(
            "Unable to retain a bounded GeoParquet compression sample; "
            "using a conservative uncompressed estimate."
        )
        compression_ratio = 1.0
        compression_estimation_method = "conservative_uncompressed"
    else:
        compression_ratio = sampled_compression_ratio
        compression_estimation_method = "sampled_zstd"
    estimated_compressed_bytes = max(1, int(uncompressed_bytes * compression_ratio))

    selection_s2_kind = "semantic_s2"
    selection_s2_name = ""
    if (
        base_partitioning == "single_file"
        and estimated_compressed_bytes >= policy.large_dataset_threshold_bytes
    ):
        resolved_policy = _resolved_policy_for_schema(source_schema, policy)
        configured_partitioning, configured_columns = (
            _select_streaming_partition_columns(source_schema, resolved_policy)
        )
        if configured_partitioning not in {"single_file", "s2"}:
            base_partitioning = configured_partitioning
            base_columns = configured_columns
        else:
            forced_admin_column = (
                _resolve_first_source_column(names, resolved_policy.force_admin_columns)
                if resolved_policy.force_admin_columns
                else None
            )
            if forced_admin_column is not None:
                base_partitioning = "admin"
                base_columns = [forced_admin_column]
                selection_s2_kind = "candidate_s2"
                selection_s2_name = forced_admin_column

    if (
        base_partitioning == "single_file"
        and estimated_compressed_bytes >= policy.large_dataset_threshold_bytes
    ):
        for column in candidate_non_null:
            cardinality = histogram_store.cardinality("candidate", column)
            if candidate_non_null[
                column
            ] / feature_count >= 0.95 and 1 < cardinality <= max(1, feature_count // 2):
                base_partitioning = "admin"
                base_columns = [column]
                selection_s2_kind = "candidate_s2"
                selection_s2_name = column
                break

    needs_s2 = (
        base_partitioning == "single_file"
        and estimated_compressed_bytes >= policy.large_dataset_threshold_bytes
    )
    chosen_s2_level = None
    partitioning = base_partitioning
    partition_columns = list(base_columns)
    if needs_s2:
        chosen_s2_level = _select_s2_level(
            histogram_store.level_maxima(
                (
                    selection_s2_kind
                    if base_partitioning not in {"single_file", "s2"}
                    else "s2"
                ),
                (
                    selection_s2_name
                    if base_partitioning not in {"single_file", "s2"}
                    else ""
                ),
                s2_levels,
            ),
            _s2_uncompressed_target_bytes(
                policy.target_file_size_bytes, compression_ratio
            ),
            s2_levels,
        )
        if base_partitioning == "single_file":
            partitioning = "s2"
            partition_columns = ["s2_parent_cell"]
        elif base_partitioning != "s2":
            partitioning = f"{base_partitioning}_s2"
            partition_columns.append("s2_parent_cell")

    hive_partition_columns = _allocate_semantic_hive_keys(partition_columns, names)

    return _GeoParquetPreflight(
        feature_count=feature_count,
        uncompressed_bytes=uncompressed_bytes,
        estimated_compressed_bytes=estimated_compressed_bytes,
        compression_ratio=compression_ratio,
        compression_estimation_method=compression_estimation_method,
        estimate_multiplier=uncompressed_bytes / max(1, serialized_bytes),
        partitioning=partitioning,
        partition_columns=partition_columns,
        hive_partition_columns=hive_partition_columns,
        chosen_s2_level=chosen_s2_level,
        resolved_policy=resolved_policy,
    )


async def process_layer_partitioned_geoparquet(
    file_path: Path,
    format_type: str,
    layer_name: Optional[str],
    layer_filename: str,
    dest_folder: str,
    dest_storage: _StorageAdapter,
    work_dir: Path,
    policy: GeoParquetWritePolicy,
    *,
    target_file_size_bytes: int | None = None,
    multi_layer: bool | None = None,
) -> dict[str, Any]:
    """Preflight and stream one layer to bounded, validated Hive GeoParquet files.

    ``multi_layer`` lets callers that know the source layer count keep a
    single-layer dataset at the stable GeoParquet root. ``None`` preserves
    the historical namespace behavior for direct callers.
    """
    driver = _get_fiona_driver(format_type)
    if not driver:
        return {"error": f"Unsupported format for streaming: {format_type}"}

    open_kwargs: dict[str, Any] = {"driver": driver}
    if layer_name and format_type in {"geopackage", "file_geodatabase"}:
        open_kwargs["layer"] = layer_name

    effective_policy = (
        replace(policy, target_file_size_bytes=target_file_size_bytes)
        if target_file_size_bytes is not None
        else policy
    )
    geoparquet_dir = work_dir / "geoparquet"
    geoparquet_dir.mkdir(parents=True, exist_ok=True)
    writers: dict[str, _ParquetWriterState] = {}
    next_part_index: dict[str, int] = {}
    local_paths: list[Path] = []
    output_layouts: list[GeoParquetOutputLayout] = []
    written_feature_count = 0
    access_counter = 0
    candidate_counter = 0

    try:
        preflight = _preflight_layer(
            file_path, open_kwargs, format_type, layer_filename, effective_policy
        )
    except Exception as exc:
        logger.error("Error in GeoParquet preflight: %s", exc)
        return {"error": str(exc)}

    if preflight.feature_count == 0:
        return {"geoparquet_paths": [], "feature_count": 0}

    partitioning = preflight.partitioning
    partition_columns = preflight.partition_columns
    file_buffer_bytes = min(
        effective_policy.write_buffer_bytes,
        effective_policy.target_file_size_bytes,
    )
    sorted_batch_bytes = min(
        file_buffer_bytes,
        effective_policy.aggregate_buffer_bytes,
    )
    source_schema: dict[str, Any] = {}
    has_named_source_layer = bool(layer_name and layer_name != "default")
    layout_layer_name = (
        layer_filename if not layer_name or layer_name == "default" else layer_name
    )
    partitioned_layer_dir = _layer_output_namespace(layout_layer_name)
    use_layer_namespace = (
        multi_layer
        if multi_layer is not None
        else partitioning != "single_file" or has_named_source_layer
    )

    def output_path(partition_dir: str) -> Path:
        index = next_part_index.get(partition_dir, 0)
        next_part_index[partition_dir] = index + 1
        if partitioning == "single_file":
            filename = (
                f"{layer_filename}.parquet"
                if index == 0
                else f"{layer_filename}-{index:03d}.parquet"
            )
        else:
            filename = f"part-{index:03d}.parquet"
        path_root = (
            geoparquet_dir / partitioned_layer_dir
            if use_layer_namespace
            else geoparquet_dir
        )
        path = path_root / partition_dir / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def finalize_writer(partition_dir: str) -> None:
        state = writers.pop(partition_dir, None)
        if state is None:
            return
        state.writer.close()
        metadata = pq.ParquetFile(state.path).metadata
        row_group_uncompressed_sizes = _row_group_uncompressed_sizes(state.path)
        if any(
            size > effective_policy.max_row_group_bytes
            for size in row_group_uncompressed_sizes
        ):
            raise ValueError(
                "A GeoParquet row group exceeds the hard limit of "
                f"{effective_policy.max_row_group_bytes} uncompressed bytes."
            )
        row_counts = [
            metadata.row_group(index).num_rows
            for index in range(metadata.num_row_groups)
        ]
        footer_size_bytes = max(0, int(metadata.serialized_size))
        local_paths.append(state.path)
        relative_path = (
            f"geoparquet/{state.path.relative_to(geoparquet_dir).as_posix()}"
        )
        output_layouts.append(
            GeoParquetOutputLayout(
                path=f"{dest_folder.rstrip('/')}/{relative_path}",
                relative_path=relative_path,
                file_size_bytes=state.path.stat().st_size,
                footer_size_bytes=footer_size_bytes,
                sha256=_sha256_file(state.path),
                row_counts=row_counts,
                row_group_uncompressed_sizes=row_group_uncompressed_sizes,
            )
        )

    def close_writers() -> None:
        for partition_dir in list(writers):
            finalize_writer(partition_dir)

    def normalize_single_file_paths() -> None:
        if partitioning != "single_file" or not local_paths:
            return
        filenames = (
            [f"{layer_filename}.parquet"]
            if len(local_paths) == 1
            else [
                f"{layer_filename}-{index:03d}.parquet"
                for index in range(len(local_paths))
            ]
        )
        for index, (path, filename) in enumerate(zip(local_paths, filenames)):
            normalized_path = path.with_name(filename)
            if path != normalized_path:
                path.rename(normalized_path)
            local_paths[index] = normalized_path
            relative_path = (
                f"geoparquet/{normalized_path.relative_to(geoparquet_dir).as_posix()}"
            )
            output_layouts[index] = replace(
                output_layouts[index],
                relative_path=relative_path,
                path=f"{dest_folder.rstrip('/')}/{relative_path}",
            )

    def write_validated(partition_dir: str, buffered: list[dict[str, Any]]) -> None:
        nonlocal candidate_counter, written_feature_count
        if not buffered:
            return
        # The SQLite cursor already provides the global order for this partition.
        gdf = gpd.GeoDataFrame.from_features(buffered, crs=current_crs)
        prepared = _coerce_gdf_to_fiona_schema(
            gdf.reset_index(drop=True), source_schema
        )
        table = _canonicalize_geoparquet_batch_metadata(
            _geodataframe_to_geoparquet_arrow(prepared)
        )
        candidate_counter += 1
        candidate = geoparquet_dir / f".candidate-{candidate_counter:06d}.parquet"
        try:
            pq.write_table(
                table,
                candidate,
                compression="zstd",
                compression_level=effective_policy.compression_level,
                data_page_size=DEFAULT_DATA_PAGE_SIZE_BYTES,
                row_group_size=max(1, len(prepared)),
            )
            probe_row_group_sizes = _row_group_uncompressed_sizes(candidate)
            if (
                max(probe_row_group_sizes, default=0)
                > effective_policy.max_row_group_bytes
            ):
                if len(buffered) == 1:
                    raise ValueError(
                        "A single feature exceeds the GeoParquet row-group hard limit "
                        f"of {effective_policy.max_row_group_bytes} uncompressed bytes."
                    )
                midpoint = len(buffered) // 2
                write_validated(partition_dir, buffered[:midpoint])
                write_validated(partition_dir, buffered[midpoint:])
                return

            state = writers.get(partition_dir)
            candidate_size = candidate.stat().st_size
            candidate_metadata = pq.ParquetFile(candidate).metadata
            candidate_footer_size = max(0, int(candidate_metadata.serialized_size))
            if (
                state is not None
                and (
                    state.path.stat().st_size
                    + candidate_size
                    + state.footer_estimate_bytes
                    + 8
                )
                > effective_policy.target_file_size_bytes
            ):
                finalize_writer(partition_dir)
                state = None
            if state is None:
                final_path = output_path(partition_dir)
                state = _ParquetWriterState(
                    path=final_path,
                    writer=pq.ParquetWriter(
                        final_path,
                        table.schema,
                        compression="zstd",
                        compression_level=effective_policy.compression_level,
                        data_page_size=DEFAULT_DATA_PAGE_SIZE_BYTES,
                    ),
                    footer_estimate_bytes=candidate_footer_size,
                )
                writers[partition_dir] = state
            else:
                state.footer_estimate_bytes += candidate_footer_size
            state.writer.write_table(table, row_group_size=max(1, len(prepared)))
            written_feature_count += len(prepared)
        finally:
            candidate.unlink(missing_ok=True)

    spool = _SpatialFeatureSpool()
    try:
        with (
            _with_large_geojson_support(),
            fiona.open(str(file_path), **open_kwargs) as src,
        ):
            current_crs = src.crs if src.crs else "EPSG:4326"
            transformer = _source_transformer(current_crs)
            source_schema = src.schema or {}
            semantic_columns = [
                column for column in partition_columns if column != "s2_parent_cell"
            ]
            base_partitioning = partitioning.removesuffix("_s2")
            for source_feature in src:
                access_counter += 1
                prepared_feature = _prepare_streaming_feature(
                    source_feature, base_partitioning, preflight.resolved_policy
                )
                properties = _feature_properties(prepared_feature)
                point = _representative_point(
                    _feature_geometry(prepared_feature), transformer
                )
                values = list(
                    _semantic_partition_values(
                        properties, base_partitioning, semantic_columns
                    )
                )
                if preflight.chosen_s2_level is not None:
                    values.append(_s2_cell_for_point(point, preflight.chosen_s2_level))
                _validate_partition_value_collisions(
                    properties, partition_columns, values
                )
                if partitioning == "single_file":
                    partition_dir = ""
                else:
                    parts = [
                        f"{preflight.hive_partition_columns[column]}="
                        f"{_encoded_hive_value(value)}"
                        for column, value in zip(partition_columns, values)
                    ]
                    partition_dir = Path(*parts).as_posix()
                feature_estimated_bytes = max(
                    1,
                    int(
                        _estimate_feature_size_bytes(prepared_feature)
                        * preflight.estimate_multiplier
                    ),
                )
                spool.add(
                    partition_dir,
                    _hilbert_like_key(point.x, point.y),
                    access_counter,
                    feature_estimated_bytes,
                    _feature_with_properties(prepared_feature, properties),
                )
                if access_counter % 100_000 == 0:
                    logger.info(
                        "Spooled %s features for global spatial sorting", access_counter
                    )
            logger.info(
                "Indexing %s spooled features by partition and S2 Hilbert key",
                access_counter,
            )
            spool.prepare()
            for partition_dir in spool.partitions():
                logger.info(
                    "Writing globally spatially sorted partition %s",
                    partition_dir or "<root>",
                )
                batch: list[dict[str, Any]] = []
                batch_estimated_bytes = 0
                for feature, feature_estimated_bytes in spool.sorted_features(
                    partition_dir
                ):
                    batch.append(feature)
                    batch_estimated_bytes += feature_estimated_bytes
                    if (
                        batch_estimated_bytes >= sorted_batch_bytes
                        or len(batch) >= effective_policy.max_row_group_rows
                    ):
                        write_validated(partition_dir, batch)
                        batch = []
                        batch_estimated_bytes = 0
                write_validated(partition_dir, batch)
                finalize_writer(partition_dir)
            close_writers()
            normalize_single_file_paths()
            footer_size_bytes = sum(
                output.footer_size_bytes for output in output_layouts
            )
            if footer_size_bytes > effective_policy.max_dataset_footer_bytes:
                raise ValueError(
                    "GeoParquet dataset footer metadata exceeds the limit of "
                    f"{effective_policy.max_dataset_footer_bytes} bytes."
                )
    except Exception as e:
        for state in list(writers.values()):
            try:
                state.writer.close()
            except Exception:
                logger.exception("Error closing GeoParquet writer %s", state.path)
        for candidate in geoparquet_dir.glob(".candidate-*.parquet"):
            candidate.unlink(missing_ok=True)
        logger.error("Error in partitioned GeoParquet processing: %s", e)
        return {"error": str(e)}
    finally:
        spool.close()

    if written_feature_count != preflight.feature_count:
        return {
            "error": (
                "GeoParquet feature count validation failed: "
                f"preflight={preflight.feature_count}, written={written_feature_count}."
            )
        }

    remote_paths: list[str] = []
    uploaded_paths_by_relative: dict[str, str] = {}
    for path in sorted(local_paths):
        rel = path.relative_to(geoparquet_dir).as_posix()
        remote_path = f"{dest_folder.rstrip('/')}/geoparquet/{rel}"
        uploaded_path = await dest_storage.upload_file(path, remote_path)
        remote_paths.append(uploaded_path)
        uploaded_paths_by_relative[f"geoparquet/{rel}"] = uploaded_path
    output_layouts = [
        replace(
            output,
            path=uploaded_paths_by_relative.get(output.relative_path, output.path),
        )
        for output in output_layouts
    ]

    layout = GeoParquetLayout(
        schema_version=1,
        layer=layout_layer_name,
        source_format=format_type,
        feature_count=preflight.feature_count,
        partition_strategy=partitioning,
        partition_columns=partition_columns,
        hive_partition_columns=preflight.hive_partition_columns,
        chosen_s2_level=preflight.chosen_s2_level,
        footer_size_bytes=sum(output.footer_size_bytes for output in output_layouts),
        thresholds={
            "large_dataset_bytes": effective_policy.large_dataset_threshold_bytes,
            "estimated_compressed_bytes": preflight.estimated_compressed_bytes,
            "compression_ratio": preflight.compression_ratio,
            "compression_estimation_method": preflight.compression_estimation_method,
            "row_group_target_bytes": effective_policy.target_row_group_bytes,
            "write_buffer_bytes": effective_policy.write_buffer_bytes,
            "max_row_group_bytes": effective_policy.max_row_group_bytes,
            "target_file_bytes": effective_policy.target_file_size_bytes,
            "effective_file_buffer_bytes": file_buffer_bytes,
            "aggregate_buffer_bytes": effective_policy.aggregate_buffer_bytes,
            "max_dataset_footer_bytes": effective_policy.max_dataset_footer_bytes,
            "footer_size_bytes": sum(
                output.footer_size_bytes for output in output_layouts
            ),
        },
        outputs=output_layouts,
        validation_status="valid",
    )

    return {
        "geoparquet_paths": remote_paths,
        "feature_count": preflight.feature_count,
        "hive_partitioned": partitioning != "single_file",
        "partitioning": partitioning,
        "partition_columns": partition_columns,
        "hive_partition_columns": preflight.hive_partition_columns,
        "chosen_s2_level": preflight.chosen_s2_level,
        "row_group_target_bytes": effective_policy.target_row_group_bytes,
        "target_file_size_bytes": effective_policy.target_file_size_bytes,
        "layout": asdict(layout),
    }


def write_shapefile_zip(
    gdf: gpd.GeoDataFrame,
    output_dir: Path,
    layer_filename: str,
    policy: ShapefileZipPolicy,
) -> ShapefileZipResult:
    if not _has_spatial_features(gdf):
        return ShapefileZipResult(False, None, "non_spatial_source")

    estimated_bytes = int(max(1, gdf.memory_usage(deep=True).sum()) * 2)
    if estimated_bytes > policy.max_estimated_zip_bytes:
        return ShapefileZipResult(False, None, "estimated_size_exceeds_limit")

    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = output_dir / f"{layer_filename}.zip"
    with tempfile.TemporaryDirectory() as tmpdir:
        shp_dir = Path(tmpdir) / "shapefile"
        shp_dir.mkdir(parents=True)
        shp_path = shp_dir / f"{layer_filename}.shp"
        gdf.to_file(shp_path, driver="ESRI Shapefile")
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in shapefile_dataset_files(shp_path):
                zf.write(path, arcname=path.name)
    return ShapefileZipResult(True, zip_path)


async def _upload_geoparquet_files(
    dest_storage: _StorageAdapter,
    geoparquet_files: list[Path],
    dest_folder: str,
    layer_filename: str,
) -> list[str]:
    paths: list[str] = []
    for i, gp_file in enumerate(geoparquet_files):
        remote_path = f"{dest_folder}geoparquet/{layer_filename}-{i}.zstd.parquet"
        paths.append(await dest_storage.upload_file(gp_file, remote_path))
    return paths


def _build_tippecanoe_cmd(
    pmtiles_path: Path, layer_filename: str, fgb_files: list[Path]
) -> list[str]:
    pmtiles_path.parent.mkdir(parents=True, exist_ok=True)
    base = [
        "tippecanoe",
        "-zg",
        "--read-parallel",
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        "--force",
        "--maximum-zoom=14",
        f"--temporary-directory={pmtiles_path.parent}",
    ]
    if len(fgb_files) > 1:
        base.extend(["-l", layer_filename])
    base.extend(["-o", str(pmtiles_path)])
    base.extend([str(f) for f in fgb_files])
    return base


async def _create_and_upload_pmtiles(
    dest_storage: _StorageAdapter,
    fgb_files: list[Path],
    pmtiles_path: Path,
    dest_folder: str,
    layer_filename: str,
) -> Optional[str]:
    if not fgb_files:
        return None
    try:
        cmd = _build_tippecanoe_cmd(pmtiles_path, layer_filename, fgb_files)
        fgb_bytes = sum(path.stat().st_size for path in fgb_files if path.exists())
        logger.info(
            "Starting tippecanoe for %s with %s FGB chunk(s), %.1f MiB input, temp dir %s",
            layer_filename,
            len(fgb_files),
            fgb_bytes / (1024 * 1024),
            pmtiles_path.parent,
        )
        env = os.environ.copy()
        env["TMPDIR"] = str(pmtiles_path.parent)
        process = subprocess.Popen(
            cmd,
            cwd=str(pmtiles_path.parent),
            env=env,
            stdout=None,
            stderr=None,
            text=True,
        )
        returncode = process.wait()
        if returncode != 0:
            logger.warning("tippecanoe failed with exit code %s", returncode)
            return None
        remote_path = f"{dest_folder}pmtiles/{layer_filename}.pmtiles"
        return await dest_storage.upload_file(pmtiles_path, remote_path)
    except FileNotFoundError:
        logger.warning("tippecanoe not found, skipping PMTiles creation")
        return None
    except Exception as e:
        logger.warning("PMTiles creation failed: %s", e)
        return None


def _filter_valid_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", "GeoSeries.notna", UserWarning)
        return gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]


def _repair_geometries_for_fgb(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    repaired = gdf
    try:
        repaired.geometry = repaired.geometry.make_valid()
    except Exception:
        try:
            repaired.geometry = repaired.geometry.buffer(0)
        except Exception:
            return repaired.iloc[0:0]
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", "GeoSeries.notna", UserWarning)
        repaired = repaired[
            repaired.geometry.notna()
            & ~repaired.geometry.is_empty
            & repaired.geometry.is_valid
        ].copy()
    return repaired


def _to_wgs84(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        return gdf.set_crs("EPSG:4326")
    try:
        if gdf.crs.to_epsg() == 4326:
            return gdf
    except Exception:
        logger.warning(
            "Unable to resolve CRS EPSG code; attempting conversion to EPSG:4326."
        )
    try:
        return gdf.to_crs("EPSG:4326")
    except Exception as exc:
        logger.warning("Failed to convert CRS to EPSG:4326: %s", exc)
        return gdf
    return gdf


def _estimate_feature_size_bytes(feature: dict[str, Any]) -> int:
    try:
        return len(json.dumps(feature, ensure_ascii=False, default=str).encode("utf-8"))
    except Exception:
        return 1024


async def process_layer_chunked(
    file_path: Path,
    format_type: str,
    layer_name: Optional[str],
    layer_filename: str,
    dest_folder: str,
    dest_storage: _StorageAdapter,
    work_dir: Path,
    geoparquet_chunk_size_mb: int = 250,
    fgb_chunk_size_mb: int = DEFAULT_FGB_CHUNK_SIZE_MB,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
    data_page_size_bytes: int = DEFAULT_DATA_PAGE_SIZE_BYTES,
    skip_parquet: bool = False,
    skip_pmtiles: bool = False,
) -> dict[str, Any]:
    """Streaming reader/writer path with adaptive chunk sizing and geometry repair."""
    if skip_parquet and skip_pmtiles:
        return {"geoparquet_paths": [], "pmtiles_path": None, "feature_count": 0}

    mem_before = get_memory_usage_mb()
    driver = _get_fiona_driver(format_type)
    if not driver:
        return {"error": f"Unsupported format for streaming: {format_type}"}

    geoparquet_dir = work_dir / "geoparquet"
    pmtiles_dir = work_dir / "pmtiles"
    geoparquet_dir.mkdir(parents=True, exist_ok=True)
    pmtiles_dir.mkdir(parents=True, exist_ok=True)

    geoparquet_files: list[Path] = []
    fgb_files: list[Path] = []
    feature_count = 0
    null_geometry_count = 0
    invalid_geometry_count = 0
    invalid_geometry_count_after_repair = 0
    dropped_for_pmtiles_count = 0
    pmtiles_write_failed = False

    bytes_per_feature: Optional[float] = None
    current_chunk_size: Optional[int] = None
    chunk_features: list[dict[str, Any]] = []
    current_memory_multiplier = DEFAULT_MEMORY_ESTIMATE_MULTIPLIER
    compressed_target_bytes = geoparquet_chunk_size_mb * 1024 * 1024
    chunk_target_bytes = int(compressed_target_bytes * current_memory_multiplier)
    chunk_uncompressed_bytes = 0
    chunk_num = 0
    features_processed = 0
    estimate_sample_size = 10
    open_kwargs: dict[str, Any] = {"driver": driver}
    if layer_name and format_type in {"geopackage", "file_geodatabase"}:
        open_kwargs["layer"] = layer_name

    def flush_parquet_chunk(
        crs: Any, features: list[dict[str, Any]], idx: int, start_id: int
    ) -> int:
        nonlocal \
            bytes_per_feature, \
            current_chunk_size, \
            current_memory_multiplier, \
            chunk_target_bytes

        if skip_parquet:
            actual_uncompressed_bytes = sum(
                _estimate_feature_size_bytes(f) for f in features
            )
            file_size_bytes = max(
                1, int(actual_uncompressed_bytes / max(1.0, current_memory_multiplier))
            )
            chunk_bytes_per_feature = file_size_bytes / max(1, len(features))
            processed_len = len(features)
        else:
            gdf_chunk = gpd.GeoDataFrame.from_features(features, crs=crs)
            gdf_chunk = _ensure_id_column(gdf_chunk, start_id=start_id)
            parquet_path = geoparquet_dir / f"{layer_filename}-{idx}.parquet"
            _write_geodataframe_parquet(
                gdf_chunk,
                parquet_path,
                row_group_size=row_group_size,
                data_page_size_bytes=data_page_size_bytes,
            )
            geoparquet_files.append(parquet_path)
            file_size_bytes = parquet_path.stat().st_size
            chunk_bytes_per_feature = file_size_bytes / max(1, len(gdf_chunk))
            actual_uncompressed_bytes = sum(
                _estimate_feature_size_bytes(f) for f in features
            )
            processed_len = len(gdf_chunk)

        observed_multiplier = actual_uncompressed_bytes / max(1, file_size_bytes)
        current_memory_multiplier = (current_memory_multiplier * 0.7) + (
            observed_multiplier * 0.3
        )
        current_memory_multiplier = max(2.0, min(20.0, current_memory_multiplier))

        if bytes_per_feature is None:
            bytes_per_feature = chunk_bytes_per_feature
        else:
            bytes_per_feature = (bytes_per_feature * 0.7) + (
                chunk_bytes_per_feature * 0.3
            )

        proposed_chunk_size = max(
            1, int(compressed_target_bytes / max(bytes_per_feature, 1))
        )
        chunk_target_bytes = int(compressed_target_bytes * current_memory_multiplier)
        if current_chunk_size is None:
            current_chunk_size = proposed_chunk_size
        else:
            min_size = max(1, int(current_chunk_size * 0.75))
            max_size = max(1, int(current_chunk_size * 1.25))
            current_chunk_size = max(min_size, min(max_size, proposed_chunk_size))
        return processed_len

    def flush_fgb_chunk(crs: Any, features: list[dict[str, Any]], idx: int) -> None:
        nonlocal \
            null_geometry_count, \
            invalid_geometry_count, \
            invalid_geometry_count_after_repair
        nonlocal dropped_for_pmtiles_count, pmtiles_write_failed

        gdf_chunk = gpd.GeoDataFrame.from_features(features, crs=crs)
        gdf_valid = _filter_valid_geometries(gdf_chunk)
        null_geometry_count += len(gdf_chunk) - len(gdf_valid)
        if len(gdf_valid) == 0:
            return

        gdf_valid = _to_wgs84(gdf_valid)
        fgb_path = pmtiles_dir / f"{layer_filename}-chunk-{idx}.fgb"
        try:
            gdf_valid.to_file(fgb_path, driver="FlatGeobuf", engine="pyogrio")
            fgb_files.append(fgb_path)
        except Exception as e:
            logger.warning(
                "FGB write failed for chunk %s (%s). Retrying with repaired geometries.",
                idx,
                e,
            )
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", "GeoSeries.notna", UserWarning)
                valid_mask = gdf_valid.geometry.is_valid
            invalid_before = int((~valid_mask).sum())
            invalid_geometry_count += invalid_before
            repaired = _repair_geometries_for_fgb(gdf_valid)
            if len(repaired) == 0:
                invalid_geometry_count_after_repair += invalid_before
                dropped_for_pmtiles_count += len(gdf_valid)
                pmtiles_write_failed = True
            else:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", "GeoSeries.notna", UserWarning)
                    repaired_valid_mask = repaired.geometry.is_valid
                invalid_after = int((~repaired_valid_mask).sum())
                invalid_geometry_count_after_repair += invalid_after
                dropped_for_pmtiles_count += max(0, len(gdf_valid) - len(repaired))
                try:
                    repaired.to_file(fgb_path, driver="FlatGeobuf", engine="pyogrio")
                    fgb_files.append(fgb_path)
                except Exception:
                    pmtiles_write_failed = True

    try:
        if not skip_parquet:
            with (
                _with_large_geojson_support(),
                fiona.open(str(file_path), **open_kwargs) as src,
            ):
                crs = src.crs if src.crs else "EPSG:4326"
                src_iter = iter(src)
                sample_features: list[dict[str, Any]] = []
                for _ in range(estimate_sample_size):
                    try:
                        sample_features.append(next(src_iter))
                    except StopIteration:
                        break
                if not sample_features:
                    return {
                        "geoparquet_paths": [],
                        "pmtiles_path": None,
                        "feature_count": 0,
                    }

                sample = _ensure_id_column(
                    gpd.GeoDataFrame.from_features(sample_features, crs=crs), start_id=1
                )
                sample_path = geoparquet_dir / "_estimate.parquet"
                _write_geodataframe_parquet(
                    sample,
                    sample_path,
                    row_group_size=row_group_size,
                    data_page_size_bytes=data_page_size_bytes,
                )
                bytes_per_feature = sample_path.stat().st_size / max(1, len(sample))
                sample_path.unlink(missing_ok=True)
                current_chunk_size = max(
                    1, int(compressed_target_bytes / max(bytes_per_feature, 1))
                )
                chunk_target_bytes = int(
                    compressed_target_bytes * current_memory_multiplier
                )

                chunk_features.extend(sample_features)
                chunk_uncompressed_bytes = sum(
                    _estimate_feature_size_bytes(feat) for feat in sample_features
                )
                feature_count = len(sample_features)

                for feat in src_iter:
                    feat_size = _estimate_feature_size_bytes(feat)
                    if (
                        chunk_features
                        and (chunk_uncompressed_bytes + feat_size) > chunk_target_bytes
                    ):
                        processed_count = flush_parquet_chunk(
                            crs, chunk_features, chunk_num, features_processed + 1
                        )
                        features_processed += processed_count
                        chunk_features = []
                        chunk_uncompressed_bytes = 0
                        chunk_num += 1
                    chunk_features.append(feat)
                    chunk_uncompressed_bytes += feat_size
                    feature_count += 1
                    estimated_mb = (len(chunk_features) * (bytes_per_feature or 1)) / (
                        1024 * 1024
                    )
                    if (
                        len(chunk_features) >= (current_chunk_size or 1)
                        or estimated_mb >= geoparquet_chunk_size_mb
                        or chunk_uncompressed_bytes >= chunk_target_bytes
                    ):
                        processed_count = flush_parquet_chunk(
                            crs, chunk_features, chunk_num, features_processed + 1
                        )
                        features_processed += processed_count
                        chunk_features = []
                        chunk_uncompressed_bytes = 0
                        chunk_num += 1
                if chunk_features:
                    processed_count = flush_parquet_chunk(
                        crs, chunk_features, chunk_num, features_processed + 1
                    )
                    features_processed += processed_count
        else:
            with (
                _with_large_geojson_support(),
                fiona.open(str(file_path), **open_kwargs) as src,
            ):
                src_iter = iter(src)
                sample_features: list[dict[str, Any]] = []
                for _ in range(estimate_sample_size):
                    try:
                        sample_features.append(next(src_iter))
                    except StopIteration:
                        break
                if not sample_features:
                    return {
                        "geoparquet_paths": [],
                        "pmtiles_path": None,
                        "feature_count": 0,
                    }
                sample_bytes = sum(
                    _estimate_feature_size_bytes(f) for f in sample_features
                )
                bytes_per_feature = max(
                    1.0, sample_bytes / max(1, len(sample_features))
                )
                current_chunk_size = max(
                    1, int(compressed_target_bytes / max(bytes_per_feature, 1))
                )
                chunk_target_bytes = int(
                    compressed_target_bytes * current_memory_multiplier
                )

        if (feature_count > 0 or skip_parquet) and not skip_pmtiles:
            fgb_bytes_per_feature = max(bytes_per_feature or 1.0, 1.0)
            fgb_compressed_target_bytes = fgb_chunk_size_mb * 1024 * 1024
            fgb_target_rows = max(
                1, int(fgb_compressed_target_bytes / max(fgb_bytes_per_feature, 1))
            )
            fgb_target_bytes = int(
                fgb_compressed_target_bytes * current_memory_multiplier
            )

            with (
                _with_large_geojson_support(),
                fiona.open(str(file_path), **open_kwargs) as src,
            ):
                crs = src.crs if src.crs else "EPSG:4326"
                src_iter = iter(src)
                chunk_features = []
                chunk_uncompressed_bytes = 0
                fgb_chunk_num = 0
                for feat in src_iter:
                    if skip_parquet:
                        feature_count += 1
                    feat_size = _estimate_feature_size_bytes(feat)
                    if (
                        chunk_features
                        and (chunk_uncompressed_bytes + feat_size) > fgb_target_bytes
                    ):
                        flush_fgb_chunk(crs, chunk_features, fgb_chunk_num)
                        chunk_features = []
                        chunk_uncompressed_bytes = 0
                        fgb_chunk_num += 1
                    chunk_features.append(feat)
                    chunk_uncompressed_bytes += feat_size
                    estimated_mb = (len(chunk_features) * fgb_bytes_per_feature) / (
                        1024 * 1024
                    )
                    if (
                        len(chunk_features) >= fgb_target_rows
                        or estimated_mb >= fgb_chunk_size_mb
                        or chunk_uncompressed_bytes >= fgb_target_bytes
                    ):
                        flush_fgb_chunk(crs, chunk_features, fgb_chunk_num)
                        chunk_features = []
                        chunk_uncompressed_bytes = 0
                        fgb_chunk_num += 1
                if chunk_features:
                    flush_fgb_chunk(crs, chunk_features, fgb_chunk_num)

        mem_after = get_memory_usage_mb()
        logger.info(
            "Processed %s features in %s chunk(s) (memory delta %.1f MB)",
            feature_count,
            len(geoparquet_files),
            mem_after - mem_before,
        )

        geoparquet_paths = []
        if not skip_parquet:
            geoparquet_paths = await _upload_geoparquet_files(
                dest_storage=dest_storage,
                geoparquet_files=geoparquet_files,
                dest_folder=dest_folder,
                layer_filename=layer_filename,
            )

        pmtiles_path = None
        if not skip_pmtiles and not pmtiles_write_failed:
            pmtiles_path = await _create_and_upload_pmtiles(
                dest_storage=dest_storage,
                fgb_files=fgb_files,
                pmtiles_path=pmtiles_dir / f"{layer_filename}.pmtiles",
                dest_folder=dest_folder,
                layer_filename=layer_filename,
            )

        return {
            "geoparquet_paths": geoparquet_paths,
            "pmtiles_path": pmtiles_path,
            "feature_count": feature_count,
            "invalid_geometry_count": invalid_geometry_count,
            "invalid_geometry_count_after_repair": invalid_geometry_count_after_repair,
            "dropped_for_pmtiles_count": dropped_for_pmtiles_count,
        }
    except Exception as e:
        logger.error("Error in chunked processing: %s", e)
        return {"error": str(e)}


async def check_format_folder_exists(
    dest_storage: _StorageAdapter,
    dest_folder: str,
    format_name: str,
) -> bool:
    format_folder = f"{dest_folder}{format_name}/"
    try:
        files = await dest_storage.list_files(format_folder)
        return len(files) > 0
    except Exception:
        return False


async def resolve_existing_output_formats(
    dest_storage: _StorageAdapter,
    dest_folder: str,
    skip_format_existing: bool,
) -> dict[str, bool]:
    if not skip_format_existing:
        return {"parquet": False, "pmtiles": False, "geopackage": False}
    parquet_exists = await check_format_folder_exists(
        dest_storage, dest_folder, "parquet"
    )
    pmtiles_exists = await check_format_folder_exists(
        dest_storage, dest_folder, "pmtiles"
    )
    geopackage_exists = await check_format_folder_exists(
        dest_storage, dest_folder, "geopackage"
    )
    return {
        "parquet": parquet_exists,
        "pmtiles": pmtiles_exists,
        "geopackage": geopackage_exists,
    }


async def _upload_extracted_format_files(
    data_file: Path,
    format_type: str,
    format_name: str,
    format_dest_folder: str,
    work_dir: Path,
    dest_storage: _StorageAdapter,
) -> None:
    format_files_dir = work_dir / f"format_{format_name}"
    format_files_dir.mkdir(exist_ok=True)

    if data_file.is_dir():
        shutil.copytree(
            data_file, format_files_dir / data_file.name, dirs_exist_ok=True
        )
    else:
        shutil.copy2(data_file, format_files_dir / data_file.name)
        if format_type == "shapefile":
            for sibling in shapefile_dataset_files(data_file):
                if sibling != data_file:
                    shutil.copy2(sibling, format_files_dir / sibling.name)

    for file_path in format_files_dir.rglob("*"):
        if file_path.is_file():
            rel_path = file_path.relative_to(format_files_dir)
            remote_path = f"{format_dest_folder}{rel_path}"
            await dest_storage.upload_file(file_path, remote_path)


async def _process_single_format_variant(
    format_name: str,
    format_path: str,
    source_storage: _StorageAdapter,
    dest_storage: _StorageAdapter,
    extract_dir: Path,
    work_dir: Path,
    dest_folder: str,
    skip_format_existing: bool,
) -> Optional[dict[str, Any]]:
    format_dest_folder = f"{dest_folder}{format_name}/"
    skip_upload = False
    if skip_format_existing and await check_format_folder_exists(
        dest_storage, dest_folder, format_name
    ):
        skip_upload = True

    unzip_result = await unzip_from_storage(
        source_storage, format_path, extract_dir / format_name
    )
    if unzip_result is None:
        return None
    format_type, data_file = unzip_result

    if format_name == "unknown":
        format_name = format_type
        format_dest_folder = f"{dest_folder}{format_name}/"
        if skip_format_existing and await check_format_folder_exists(
            dest_storage, dest_folder, format_name
        ):
            skip_upload = True

    try:
        layers = list_layers_in_file(data_file, format_type)
    except Exception:
        return None

    if not skip_upload:
        await _upload_extracted_format_files(
            data_file=data_file,
            format_type=format_type,
            format_name=format_name,
            format_dest_folder=format_dest_folder,
            work_dir=work_dir,
            dest_storage=dest_storage,
        )

    return {
        "format_name": format_name,
        "format_type": format_type,
        "layers": layers,
        "data_file": data_file,
        "is_geopackage_source": format_name == "geopackage",
    }


async def write_geopackage_chunked(
    file_path: Path,
    format_type: str,
    layer_name: Optional[str],
    output_gpkg: Path,
    geoparquet_chunk_size_mb: int = 250,
) -> None:
    """Chunked geopackage writer with schema-aware geometry detection."""
    driver = _get_fiona_driver(format_type)
    if not driver:
        raise ValueError(f"Unsupported format for geopackage conversion: {format_type}")

    open_kwargs: dict[str, Any] = {"driver": driver}
    if layer_name and format_type in {"geopackage", "file_geodatabase"}:
        open_kwargs["layer"] = layer_name

    chunk_target_bytes = geoparquet_chunk_size_mb * 1024 * 1024
    chunk_features: list[dict[str, Any]] = []
    chunk_uncompressed_bytes = 0

    with (
        _with_large_geojson_support(),
        fiona.open(str(file_path), **open_kwargs) as src,
    ):
        output_crs = src.crs if src.crs else "EPSG:4326"
        src_iter = iter(src)

        sample_features = []
        for _ in range(10):
            try:
                sample_features.append(next(src_iter))
            except StopIteration:
                break
        if not sample_features:
            return

        sample_gdf = _sanitize_geopackage_columns(
            gpd.GeoDataFrame.from_features(sample_features, crs=output_crs)
        )
        sample_path = output_gpkg.parent / f"{output_gpkg.stem}_sample.gpkg"
        sample_gdf.to_file(
            str(sample_path), driver="GPKG", layer=layer_name if layer_name else None
        )
        bytes_per_feature = sample_path.stat().st_size / max(1, len(sample_features))
        sample_path.unlink(missing_ok=True)

        current_memory_multiplier = DEFAULT_MEMORY_ESTIMATE_MULTIPLIER
        current_chunk_size = max(
            1, int(chunk_target_bytes / (bytes_per_feature * current_memory_multiplier))
        )

        feature_id_counter = 1
        chunk_features.extend(sample_features)
        chunk_uncompressed_bytes = sum(
            _estimate_feature_size_bytes(feat) for feat in sample_features
        )

        if chunk_features:
            chunk_gdf = gpd.GeoDataFrame.from_features(chunk_features, crs=output_crs)
            chunk_gdf = _ensure_id_column(chunk_gdf, start_id=feature_id_counter)
            chunk_gdf = _sanitize_geopackage_columns(chunk_gdf)
            feature_id_counter += len(chunk_gdf)
            chunk_gdf.to_file(
                str(output_gpkg),
                driver="GPKG",
                layer=layer_name if layer_name else None,
                mode="w",
            )
            chunk_features = []
            chunk_uncompressed_bytes = 0

        for feat in src_iter:
            feat_size = _estimate_feature_size_bytes(feat)
            if (
                chunk_features
                and (chunk_uncompressed_bytes + feat_size) > chunk_target_bytes
            ):
                chunk_gdf = gpd.GeoDataFrame.from_features(
                    chunk_features, crs=output_crs
                )
                chunk_gdf = _ensure_id_column(chunk_gdf, start_id=feature_id_counter)
                chunk_gdf = _sanitize_geopackage_columns(chunk_gdf)
                feature_id_counter += len(chunk_gdf)
                chunk_gdf.to_file(
                    str(output_gpkg),
                    driver="GPKG",
                    layer=layer_name if layer_name else None,
                    mode="a",
                )
                chunk_features = []
                chunk_uncompressed_bytes = 0

            chunk_features.append(feat)
            chunk_uncompressed_bytes += feat_size
            if len(chunk_features) >= current_chunk_size:
                chunk_gdf = gpd.GeoDataFrame.from_features(
                    chunk_features, crs=output_crs
                )
                chunk_gdf = _ensure_id_column(chunk_gdf, start_id=feature_id_counter)
                chunk_gdf = _sanitize_geopackage_columns(chunk_gdf)
                feature_id_counter += len(chunk_gdf)
                chunk_gdf.to_file(
                    str(output_gpkg),
                    driver="GPKG",
                    layer=layer_name if layer_name else None,
                    mode="a",
                )
                chunk_features = []
                chunk_uncompressed_bytes = 0

        if chunk_features:
            chunk_gdf = gpd.GeoDataFrame.from_features(chunk_features, crs=output_crs)
            chunk_gdf = _ensure_id_column(chunk_gdf, start_id=feature_id_counter)
            chunk_gdf = _sanitize_geopackage_columns(chunk_gdf)
            chunk_gdf.to_file(
                str(output_gpkg),
                driver="GPKG",
                layer=layer_name if layer_name else None,
                mode="a",
            )


def _select_best_format_for_geopackage(
    processed_formats: dict[str, dict[str, Any]],
) -> tuple[Optional[dict[str, Any]], Optional[Path]]:
    for fmt_name in CANONICAL_SOURCE_FORMAT_PRECEDENCE:
        if fmt_name in processed_formats:
            fmt_info = processed_formats[fmt_name]
            return fmt_info, fmt_info["data_file"]
    return None, None


def select_processing_input(
    processed_formats: dict[str, dict[str, Any]],
    *,
    allow_shapefile_fallback: bool | None = None,
) -> tuple[Optional[dict[str, Any]], Optional[Path], Optional[str]]:
    """Select the highest-precedence canonical source.

    ``allow_shapefile_fallback`` remains accepted for compatibility; Shapefile
    is now a canonical source and is always eligible.
    """
    for format_name in CANONICAL_SOURCE_FORMAT_PRECEDENCE:
        if format_name in processed_formats:
            fmt_info = processed_formats[format_name]
            return fmt_info, fmt_info["data_file"], fmt_info["format_type"]
    return None, None, None


def _select_preferred_processing_format(
    processed_formats: dict[str, dict[str, Any]],
) -> tuple[Optional[dict[str, Any]], Optional[Path], Optional[str]]:
    return select_processing_input(processed_formats)


async def _process_dataset(
    source_rel_path: str,
    source_storage: _StorageAdapter,
    dest_storage: _StorageAdapter,
    skip_format_existing: bool = False,
) -> dict[str, Any]:
    logical_source_path = source_storage.logical_key(source_rel_path)
    path_parts = [p for p in logical_source_path.split("/") if p]
    zip_stem = (
        Path(path_parts[-1]).stem if path_parts else Path(logical_source_path).stem
    )
    base_filename = _strip_format_suffix(zip_stem)
    dest_folder = _build_dest_folder(logical_source_path)
    format_variants = {_detect_format_from_path(source_rel_path): source_rel_path}

    processed_formats: dict[str, dict[str, Any]] = {}
    geopackage_created = False

    with tempfile.TemporaryDirectory() as temp_dir:
        work_dir = Path(temp_dir)
        extract_dir = work_dir / "extracted"
        extract_dir.mkdir(parents=True, exist_ok=True)

        for format_name, format_path in format_variants.items():
            processed = await _process_single_format_variant(
                format_name=format_name,
                format_path=format_path,
                source_storage=source_storage,
                dest_storage=dest_storage,
                extract_dir=extract_dir,
                work_dir=work_dir,
                dest_folder=dest_folder,
                skip_format_existing=skip_format_existing,
            )
            if not processed:
                continue
            processed_formats[processed["format_name"]] = {
                "format_type": processed["format_type"],
                "layers": processed["layers"],
                "data_file": processed["data_file"],
            }
            if processed["is_geopackage_source"]:
                geopackage_created = True

        existing_outputs = await resolve_existing_output_formats(
            dest_storage=dest_storage,
            dest_folder=dest_folder,
            skip_format_existing=skip_format_existing,
        )

        if not geopackage_created and not existing_outputs["geopackage"]:
            best_format, best_format_path = _select_best_format_for_geopackage(
                processed_formats
            )
            if best_format and best_format_path and best_format_path.exists():
                format_type = best_format["format_type"]
                layers = best_format["layers"]
                geopackage_dir = work_dir / "geopackage"
                geopackage_dir.mkdir(exist_ok=True)
                for layer_name, _geom_type in layers:
                    layer_filename = _build_layer_filename(base_filename, layer_name)
                    output_gpkg = geopackage_dir / f"{layer_filename}.gpkg"
                    await write_geopackage_chunked(
                        file_path=best_format_path,
                        format_type=format_type,
                        layer_name=layer_name if layer_name != "default" else None,
                        output_gpkg=output_gpkg,
                    )
                    if output_gpkg.exists():
                        remote_path = f"{dest_folder}geopackage/{layer_filename}.gpkg"
                        await dest_storage.upload_file(output_gpkg, remote_path)
                        geopackage_created = True

        preferred_format, preferred_data_file, preferred_format_type = (
            _select_preferred_processing_format(processed_formats)
        )
        if not preferred_format:
            return {
                "success": False,
                "error": "No processable format found",
                "layers": [],
            }

        layers = preferred_format["layers"]
        skip_parquet_upload = existing_outputs["parquet"]
        skip_pmtiles_upload = existing_outputs["pmtiles"]
        results: list[dict[str, Any]] = []
        for layer_name, _geom_type in layers:
            layer_filename = _build_layer_filename(base_filename, layer_name)
            parquet_result: dict[str, Any] = {"geoparquet_paths": []}
            if not skip_parquet_upload:
                parquet_result = await process_layer_partitioned_geoparquet(
                    file_path=preferred_data_file,
                    format_type=preferred_format_type,
                    layer_name=layer_name if layer_name != "default" else None,
                    layer_filename=layer_filename,
                    dest_folder=dest_folder,
                    dest_storage=dest_storage,
                    work_dir=work_dir,
                    policy=GeoParquetWritePolicy(
                        candidate_admin_columns=DEFAULT_ADMIN_PARTITION_CANDIDATES
                    ),
                    multi_layer=len(layers) > 1,
                )
            pmtiles_result = await process_layer_chunked(
                file_path=preferred_data_file,
                format_type=preferred_format_type,
                layer_name=layer_name if layer_name != "default" else None,
                layer_filename=layer_filename,
                dest_folder=dest_folder,
                dest_storage=dest_storage,
                work_dir=work_dir,
                row_group_size=DEFAULT_ROW_GROUP_SIZE,
                data_page_size_bytes=DEFAULT_DATA_PAGE_SIZE_BYTES,
                skip_parquet=True,
                skip_pmtiles=skip_pmtiles_upload,
            )
            results.append({"layer": layer_name, **parquet_result, **pmtiles_result})
        return {"success": True, "dest_folder": dest_folder, "layers": results}


async def _process_staged_dataset_version_async(
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
    keys: list[str],
) -> dict[str, Any]:
    """Async implementation for staged source conversion and publish."""
    source_keys = [k for k in keys if "/metadata/" not in k]
    if not source_keys:
        return {"success": False, "error": "No source keys to process", "layers": []}

    dest_storage = _StorageAdapter(published_storage)
    logical_source_key = _StorageAdapter(staging_storage).logical_key(source_keys[0])
    parts = logical_source_key.split("/")
    if len(parts) < 4:
        return {
            "success": False,
            "error": "Source keys do not match dataset/file/version layout",
            "layers": [],
        }

    dataset_slug, file_slug, version = parts[:3]
    dest_folder = f"{dataset_slug}/{file_slug}/{version}/"

    with staging_storage.get_local_version_dir(
        dataset_slug, file_slug, version
    ) as version_dir:
        version_path = Path(version_dir)
        processed_formats = _discover_staged_formats(version_path)
        preferred_format, preferred_data_file, preferred_format_type = (
            _select_preferred_processing_input(processed_formats)
        )
        if (
            not preferred_format
            or preferred_data_file is None
            or preferred_format_type is None
        ):
            return {
                "success": False,
                "error": "No processable staged format found",
                "layers": [],
            }

        all_layers: list[dict[str, Any]] = []
        with tempfile.TemporaryDirectory() as temp_dir:
            work_dir = Path(temp_dir)

            if "geopackage" in processed_formats:
                geopackage_source = processed_formats["geopackage"]["data_file"]
                remote_path = f"{dest_folder}geopackage/{geopackage_source.name}"
                await dest_storage.upload_file(geopackage_source, remote_path)
            else:
                geopackage_dir = work_dir / "geopackage"
                geopackage_dir.mkdir(exist_ok=True)
                for layer_name, _geom_type in preferred_format["layers"]:
                    layer_filename = _build_layer_filename(file_slug, layer_name)
                    output_gpkg = geopackage_dir / f"{layer_filename}.gpkg"
                    await write_geopackage_chunked(
                        file_path=preferred_data_file,
                        format_type=preferred_format_type,
                        layer_name=layer_name if layer_name != "default" else None,
                        output_gpkg=output_gpkg,
                    )
                    if output_gpkg.exists():
                        remote_path = f"{dest_folder}geopackage/{layer_filename}.gpkg"
                        await dest_storage.upload_file(output_gpkg, remote_path)

            for layer_name, _geom_type in preferred_format["layers"]:
                layer_filename = _build_layer_filename(file_slug, layer_name)
                layer_result = await process_layer_chunked(
                    file_path=preferred_data_file,
                    format_type=preferred_format_type,
                    layer_name=layer_name if layer_name != "default" else None,
                    layer_filename=layer_filename,
                    dest_folder=dest_folder,
                    dest_storage=dest_storage,
                    work_dir=work_dir,
                    row_group_size=DEFAULT_ROW_GROUP_SIZE,
                    data_page_size_bytes=DEFAULT_DATA_PAGE_SIZE_BYTES,
                    skip_parquet=False,
                    skip_pmtiles=False,
                )
                all_layers.append({"layer": layer_name, **layer_result})

    return {"success": True, "layers": all_layers}


def process_staged_dataset_version(
    staging_storage: StagingStorageResource,
    published_storage: PublishedStorageResource,
    keys: list[str],
) -> dict[str, Any]:
    """Process staged source keys and produce published geopackage/geoparquet/pmtiles artifacts."""
    return asyncio.run(
        _process_staged_dataset_version_async(
            staging_storage=staging_storage,
            published_storage=published_storage,
            keys=keys,
        )
    )


def _discover_staged_formats(version_dir: Path) -> dict[str, dict[str, Any]]:
    processed_formats: dict[str, dict[str, Any]] = {}
    for format_name in CANONICAL_SOURCE_FORMAT_PRECEDENCE:
        search_dir = version_dir / format_name
        if not search_dir.is_dir():
            continue
        data_file: Path | None = None
        if format_name == "file_geodatabase":
            geodatabases = iter_file_geodatabases(search_dir)
            if len(geodatabases) > 1:
                raise ValueError(
                    f"Found multiple canonical {format_name} sources under {search_dir}."
                )
            data_file = geodatabases[0] if geodatabases else None
        else:
            data_file = discover_canonical_source_file(search_dir, format_name)
        if data_file is None:
            continue
        layers = list_layers_in_file(data_file, format_name)
        processed_formats[format_name] = {
            "format_type": format_name,
            "layers": layers,
            "data_file": data_file,
        }

    if "shapefile" not in processed_formats:
        legacy_shapefile = discover_legacy_unknown_shapefile(version_dir / "unknown")
        if legacy_shapefile is not None:
            processed_formats["shapefile"] = {
                "format_type": "shapefile",
                "layers": list_layers_in_file(legacy_shapefile, "shapefile"),
                "data_file": legacy_shapefile,
            }
    return processed_formats


def _select_preferred_processing_input(
    processed_formats: dict[str, dict[str, Any]],
) -> tuple[Optional[dict[str, Any]], Optional[Path], Optional[str]]:
    return select_processing_input(processed_formats)

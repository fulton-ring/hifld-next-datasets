"""Catalog helpers for staged dataset versions."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import geopandas as gpd
from pandas.api.types import is_bool_dtype, is_datetime64_any_dtype, is_float_dtype, is_hashable, is_integer_dtype

from dagster_hifld.resources import StagingStorageResource


def load_staged_geodata(
    staging: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
) -> gpd.GeoDataFrame:
    with staging.get_local_version_dir(dataset_slug, file_slug, version) as version_dir:
        version_path = Path(version_dir)
        gdf = _load_best_file(version_path)
        if gdf is None:
            raise ValueError(
                f"No loadable geodata found for {dataset_slug}/{file_slug}/{version}"
            )
        return _ensure_id_column(gdf)


def generate_quality_manifest(gdf: gpd.GeoDataFrame) -> dict:
    row_count = len(gdf)
    geom_col = gdf.geometry
    geom_types = sorted(geom_col.geom_type.dropna().unique().tolist())
    geometry_type = geom_types[0] if len(geom_types) == 1 else "Mixed"
    invalid_geometry_count = int((~geom_col.is_valid).sum()) if row_count else 0
    null_geometry_count = int(geom_col.isnull().sum()) if row_count else 0

    bounds: list[float] | None = None
    if row_count and geom_col.notnull().any():
        bounds = [round(float(value), 6) for value in geom_col.total_bounds]

    columns = [column for column in gdf.columns if column != gdf.geometry.name]
    columns_hash = hashlib.sha256(
        json.dumps(columns, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return {
        "version": "v1",
        "feature_count": row_count,
        "bounds": bounds,
        "geometry_type": geometry_type,
        "invalid_geometry_count": invalid_geometry_count,
        "quality_check_passed": row_count > 0 and invalid_geometry_count == 0 and null_geometry_count == 0,
        "columns_hash": columns_hash,
    }


def generate_data_dictionary(gdf: gpd.GeoDataFrame, name: str) -> dict:
    columns: list[dict] = []
    df_len = len(gdf)
    use_sampling = df_len > 10_000
    sample_size = min(5_000, df_len) if use_sampling else df_len

    for column_name in gdf.columns:
        column = gdf[column_name]
        column_type = _get_column_datatype(column.dtype)
        null_count = int(column.isnull().sum())

        if column_type == "geometry":
            columns.append(
                {
                    "name": column_name,
                    "type": column_type,
                    "nullable": bool(null_count > 0),
                    "numNullValues": null_count,
                }
            )
            continue

        if df_len < 100_000 and is_hashable(column):
            unique_count = int(column.nunique())
        else:
            unique_count = None

        non_null = column.dropna()
        if len(non_null) > 0:
            if use_sampling and df_len > sample_size:
                sample_values = non_null.sample(
                    min(5, min(sample_size, len(non_null))),
                    random_state=0,
                )
            else:
                sample_values = non_null.sample(min(5, len(non_null)), random_state=0)
            example_values = [str(value) for value in sample_values.values.tolist()]
        else:
            example_values = []

        entry: dict[str, object] = {
            "name": column_name,
            "type": column_type,
            "nullable": bool(null_count > 0),
            "numUniqueValues": unique_count,
            "numNullValues": null_count,
            "exampleValues": example_values,
        }

        if column_type in {"integer", "float"} and len(non_null) > 0:
            sample_col = (
                non_null.sample(min(sample_size, len(non_null)), random_state=0)
                if use_sampling
                else non_null
            )
            entry["min"] = float(sample_col.min())
            entry["max"] = float(sample_col.max())

        if column_type == "string":
            try:
                if len(non_null) > 0:
                    sample_col = (
                        non_null.sample(min(sample_size, len(non_null)), random_state=0)
                        if use_sampling
                        else non_null
                    )
                    entry["length"] = int(sample_col.astype(str).str.len().max())
                    if df_len < 50_000:
                        value_counts = sample_col.value_counts()
                        if len(value_counts) <= 20:
                            entry["possibleValues"] = sorted(
                                value_counts.index.astype(str).tolist()
                            )
            except Exception:
                entry["length"] = None

        columns.append(entry)

    return {
        "name": name,
        "columns": columns,
    }


def write_catalog_metadata(
    staging: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    quality_dict: dict,
    dictionary_dict: dict,
) -> dict[str, str]:
    quality_key = staging.write(
        dataset_slug,
        file_slug,
        version,
        "metadata/quality_manifest.json",
        json.dumps(quality_dict, sort_keys=True, indent=2).encode("utf-8"),
    )
    dictionary_key = staging.write(
        dataset_slug,
        file_slug,
        version,
        "metadata/data_dictionary.json",
        json.dumps(dictionary_dict, sort_keys=True, indent=2).encode("utf-8"),
    )
    return {
        "quality_manifest": quality_key,
        "data_dictionary": dictionary_key,
    }


def _load_best_file(version_dir: Path) -> gpd.GeoDataFrame | None:
    fallback_searches: list[tuple[Path, str]] = [
        (version_dir / "file_geodatabase", ".gdb"),
        (version_dir / "geopackage", ".gpkg"),
        (version_dir / "shapefile", ".shp"),
        (version_dir / "unknown", ".shp"),
        (version_dir / "geojson", ".geojson"),
    ]
    for search_dir, ext in fallback_searches:
        if not search_dir.is_dir():
            continue
        if ext == ".gdb":
            for path in search_dir.iterdir():
                if path.is_dir() and path.suffix.lower() == ".gdb":
                    try:
                        return gpd.read_file(str(path), engine="pyogrio")
                    except Exception:
                        continue
        else:
            for path in search_dir.glob(f"*{ext}"):
                try:
                    return gpd.read_file(str(path))
                except Exception:
                    continue
    return None


def _ensure_id_column(gdf: gpd.GeoDataFrame, start_id: int = 1) -> gpd.GeoDataFrame:
    if "id" not in gdf.columns:
        id_candidates = ["OBJECTID", "FID", "fid", "GlobalID", "gid", "ogc_fid"]
        id_col = next((column for column in id_candidates if column in gdf.columns), None)
        if id_col:
            gdf["id"] = gdf[id_col]
        else:
            gdf["id"] = range(start_id, start_id + len(gdf))
    return gdf


def _get_column_datatype(dtype) -> str:
    dtype_str = str(dtype).lower()
    if dtype_str == "geometry":
        return "geometry"
    if is_integer_dtype(dtype):
        return "integer"
    if is_float_dtype(dtype):
        return "float"
    if is_bool_dtype(dtype):
        return "boolean"
    if is_datetime64_any_dtype(dtype):
        return "timestamp"
    return "string"

"""Catalog helpers for staged dataset versions."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
from pandas.api.types import is_bool_dtype, is_datetime64_any_dtype, is_float_dtype, is_hashable, is_integer_dtype

from dagster_hifld.gdal import with_large_geojson_support as _with_large_geojson_support
from dagster_hifld.file_geodatabase import iter_file_geodatabases
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_formats import (
    CANONICAL_SOURCE_FORMAT_PRECEDENCE,
    discover_canonical_source_file,
    discover_canonical_shapefile,
    discover_legacy_unknown_shapefile,
)

CATALOG_SAMPLE_FEATURE_LIMIT = 5_000


@dataclass(frozen=True)
class CatalogSummary:
    quality_manifest: dict
    data_dictionary: dict


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
        return gdf


def summarize_staged_catalog(
    staging: StagingStorageResource,
    dataset_slug: str,
    file_slug: str,
    version: str,
    dictionary_name: str,
    *,
    source_metadata: dict | None = None,
) -> CatalogSummary:
    with staging.get_local_version_dir(dataset_slug, file_slug, version) as version_dir:
        version_path = Path(version_dir)
        reader_errors: list[str] = []
        geospatial = _summarize_best_geospatial_file(
            version_path,
            dictionary_name,
            source_metadata,
            reader_errors=reader_errors,
        )
        if geospatial is not None:
            return geospatial

        tabular = _summarize_best_tabular_file(version_path, dictionary_name, source_metadata)
        if tabular is not None:
            return tabular

    if reader_errors:
        attempted = "; ".join(reader_errors)
        raise ValueError(
            f"No loadable source files found for {dataset_slug}/{file_slug}/{version}. "
            f"Attempted sources: {attempted}"
        )
    raise ValueError(f"No source files found for {dataset_slug}/{file_slug}/{version}")


def generate_quality_manifest(gdf: pd.DataFrame, *, catalog_mode: str | None = None) -> dict:
    row_count = len(gdf)
    geom_col = getattr(gdf, "geometry", None)
    has_geometry = geom_col is not None
    if has_geometry:
        try:
            non_empty_mask = geom_col.notna() & ~geom_col.is_empty
        except Exception:
            non_empty_mask = geom_col.notna()
        spatial_feature_count = int(non_empty_mask.sum()) if row_count else 0
        geom_types = sorted(geom_col[non_empty_mask].geom_type.dropna().unique().tolist())
        if len(geom_types) == 1:
            geometry_type = geom_types[0]
        elif geom_types:
            geometry_type = "Mixed"
        else:
            geometry_type = None
        invalid_geometry_count = (
            int((~geom_col[non_empty_mask].is_valid).sum()) if spatial_feature_count else 0
        )
        null_geometry_count = row_count - spatial_feature_count
        spatial_status = "spatial" if spatial_feature_count else "all_null_geometry"
    else:
        geometry_type = None
        invalid_geometry_count = 0
        null_geometry_count = 0
        spatial_status = "non_spatial_source"

    bounds: list[float] | None = None
    if has_geometry and row_count and geom_col.notnull().any():
        bounds = [round(float(value), 6) for value in geom_col.total_bounds]

    quality_check_passed = row_count > 0 and invalid_geometry_count == 0
    if spatial_status == "spatial":
        quality_check_passed = quality_check_passed and null_geometry_count == 0

    geometry_name = gdf.geometry.name if has_geometry else None
    columns = [column for column in gdf.columns if column != geometry_name]
    columns_hash = hashlib.sha256(
        json.dumps(columns, sort_keys=True).encode("utf-8")
    ).hexdigest()

    manifest = {
        "version": "v1",
        "feature_count": row_count,
        "bounds": bounds,
        "geometry_type": geometry_type,
        "invalid_geometry_count": invalid_geometry_count,
        "null_geometry_count": null_geometry_count,
        "spatial_status": spatial_status,
        "quality_check_passed": quality_check_passed,
        "columns_hash": columns_hash,
    }
    if catalog_mode:
        manifest["catalog_mode"] = catalog_mode
    return manifest


def generate_data_dictionary(
    gdf: gpd.GeoDataFrame,
    name: str,
    *,
    source_metadata: dict | None = None,
) -> dict:
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

    dictionary = {
        "name": name,
        "columns": columns,
    }
    if source_metadata:
        for key in (
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
            "metadata_sources",
            "metadata_resolved_from",
            "inventory_match_type",
            "manifest_keys",
        ):
            if key in source_metadata and source_metadata[key] not in (None, "", [], {}):
                dictionary[key] = source_metadata[key]
    return dictionary


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
    canonical_shapefile: Path | None = None
    for format_name in CANONICAL_SOURCE_FORMAT_PRECEDENCE:
        search_dir = version_dir / format_name
        if format_name == "file_geodatabase":
            for path in iter_file_geodatabases(search_dir):
                try:
                    with _with_large_geojson_support():
                        return gpd.read_file(str(path), engine="pyogrio")
                except Exception:
                    continue
        elif format_name == "shapefile":
            path = discover_canonical_shapefile(search_dir)
            if path is None:
                continue
            canonical_shapefile = path
            try:
                with _with_large_geojson_support():
                    return gpd.read_file(str(path))
            except Exception:
                continue
        else:
            path = discover_canonical_source_file(search_dir, format_name)
            if path is None:
                continue
            if format_name == "shapefile":
                canonical_shapefile = path
            try:
                with _with_large_geojson_support():
                    return gpd.read_file(str(path))
            except Exception:
                continue
    legacy_shapefile = None
    if canonical_shapefile is None:
        legacy_shapefile = discover_legacy_unknown_shapefile(version_dir / "unknown")
    if legacy_shapefile is not None:
        try:
            with _with_large_geojson_support():
                return gpd.read_file(str(legacy_shapefile))
        except Exception:
            return None
    return None


def _ensure_id_column(gdf: gpd.GeoDataFrame, start_id: int = 1) -> gpd.GeoDataFrame:
    return gdf


def _summarize_best_geospatial_file(
    version_dir: Path,
    dictionary_name: str,
    source_metadata: dict | None,
    *,
    reader_errors: list[str] | None = None,
) -> CatalogSummary | None:
    for source_path, layer_name in _iter_geospatial_sources(version_dir):
        try:
            sample, quality = _summarize_geospatial_source(source_path, layer_name)
        except Exception as exc:
            if reader_errors is not None:
                rel_path = source_path.relative_to(version_dir)
                layer_label = layer_name or "default"
                reader_errors.append(
                    f"{rel_path} ({layer_label}): {type(exc).__name__}: {exc}"
                )
            continue
        return CatalogSummary(
            quality_manifest=quality,
            data_dictionary=generate_data_dictionary(
                sample,
                dictionary_name,
                source_metadata=source_metadata,
            ),
        )
    return None


def _iter_geospatial_sources(version_dir: Path):
    canonical_shapefile: Path | None = None
    for format_name in CANONICAL_SOURCE_FORMAT_PRECEDENCE:
        if format_name == "file_geodatabase":
            paths = iter_file_geodatabases(version_dir / format_name)
        elif format_name == "shapefile":
            path = discover_canonical_shapefile(version_dir / format_name)
            paths = [path] if path is not None else []
        else:
            path = discover_canonical_source_file(version_dir / format_name, format_name)
            paths = [path] if path is not None else []
        if format_name == "shapefile" and paths:
            canonical_shapefile = paths[0]
        for path in paths:
            if format_name in {"geopackage", "file_geodatabase"}:
                try:
                    with _with_large_geojson_support():
                        layers = fiona.listlayers(str(path)) or [None]
                except Exception:
                    layers = [None]
                for layer in layers:
                    yield path, layer
            else:
                yield path, None
    if canonical_shapefile is None:
        legacy_shapefile = discover_legacy_unknown_shapefile(version_dir / "unknown")
        if legacy_shapefile is not None:
            yield legacy_shapefile, None


def _summarize_geospatial_source(path: Path, layer_name: str | None) -> tuple[gpd.GeoDataFrame, dict]:
    open_kwargs: dict[str, object] = {}
    if layer_name:
        open_kwargs["layer"] = layer_name
    with _with_large_geojson_support(), fiona.open(str(path), **open_kwargs) as src:
        crs = src.crs if src.crs else "EPSG:4326"
        schema = src.schema or {}
        feature_count = _safe_feature_count(src)
        bounds = _safe_bounds(src)
        geometry_type = _normalize_geometry_type(schema.get("geometry"))
        sample_features: list[dict] = []
        geom_types: set[str] = set()
        null_geometry_count = 0
        invalid_geometry_count = 0
        spatial_feature_count = 0
        counted = 0
        for feature in src:
            counted += 1
            geom = feature.get("geometry")
            if geom is None:
                null_geometry_count += 1
            else:
                try:
                    shapely_geom = shape(geom)
                    if shapely_geom.is_empty:
                        null_geometry_count += 1
                    else:
                        spatial_feature_count += 1
                        geom_types.add(shapely_geom.geom_type)
                    if not shapely_geom.is_empty and not shapely_geom.is_valid:
                        invalid_geometry_count += 1
                except Exception:
                    invalid_geometry_count += 1
            if len(sample_features) < CATALOG_SAMPLE_FEATURE_LIMIT:
                sample_features.append(feature)
            if counted >= CATALOG_SAMPLE_FEATURE_LIMIT:
                break
        if feature_count == 0 and counted:
            feature_count = counted

    if sample_features:
        sample = gpd.GeoDataFrame.from_features(sample_features, crs=crs)
    else:
        sample = gpd.GeoDataFrame(
            columns=list((schema.get("properties") or {}).keys()) + ["geometry"],
            geometry="geometry",
            crs=crs,
        )
    if bounds is None and len(sample) and sample.geometry.notnull().any():
        bounds = [round(float(value), 6) for value in sample.geometry.total_bounds]
    if not geometry_type and geom_types:
        geometry_type = sorted(geom_types)[0] if len(geom_types) == 1 else "Mixed"

    columns = [column for column in sample.columns if column != sample.geometry.name]
    spatial_status = "spatial"
    if counted and spatial_feature_count == 0:
        spatial_status = "all_null_geometry"
        geometry_type = None
    elif not geometry_type and spatial_feature_count == 0:
        spatial_status = "non_spatial_source"
    quality_check_passed = feature_count > 0 and invalid_geometry_count == 0
    if spatial_status == "spatial":
        quality_check_passed = quality_check_passed and null_geometry_count == 0
    quality = {
        "version": "v1",
        "feature_count": int(feature_count),
        "bounds": bounds,
        "geometry_type": geometry_type,
        "invalid_geometry_count": int(invalid_geometry_count),
        "quality_check_passed": quality_check_passed,
        "columns_hash": hashlib.sha256(
            json.dumps(columns, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "catalog_mode": "streaming_geospatial",
        "spatial_status": spatial_status,
        "sampled_feature_count": len(sample),
        "sampled_null_geometry_count": int(null_geometry_count),
        "sampled_invalid_geometry_count": int(invalid_geometry_count),
    }
    return sample, quality


def _safe_feature_count(src) -> int:
    try:
        count = len(src)
    except Exception:
        return 0
    return int(count) if count >= 0 else 0


def _safe_bounds(src) -> list[float] | None:
    try:
        bounds = src.bounds
    except Exception:
        return None
    if not bounds:
        return None
    return [round(float(value), 6) for value in bounds]


def _normalize_geometry_type(value: object) -> str | None:
    if value is None:
        return None
    geometry_type = str(value)
    if geometry_type.lower() in {"", "none", "unknown"}:
        return None
    return geometry_type


def _summarize_best_tabular_file(
    version_dir: Path,
    dictionary_name: str,
    source_metadata: dict | None,
) -> CatalogSummary | None:
    for path in sorted((version_dir / "unknown").glob("*")):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        try:
            if suffix == ".csv":
                sample = pd.read_csv(path, nrows=5_000)
                with path.open("rb") as handle:
                    total_rows = sum(1 for _ in handle) - 1
            elif suffix in {".json", ".jsonl", ".ndjson"}:
                sample = pd.read_json(path, lines=suffix in {".jsonl", ".ndjson"}, nrows=5_000)
                total_rows = len(sample)
            elif suffix == ".parquet":
                sample = pd.read_parquet(path)
                total_rows = len(sample)
            else:
                continue
        except Exception:
            continue
        quality = generate_quality_manifest(sample, catalog_mode="tabular")
        quality["feature_count"] = max(0, int(total_rows))
        return CatalogSummary(
            quality_manifest=quality,
            data_dictionary=generate_data_dictionary(
                sample,
                dictionary_name,
                source_metadata=source_metadata,
            ),
        )
    return None


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

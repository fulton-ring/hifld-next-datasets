"""Pinned native-file normalization and full comparison; never acceptance.

Readers keep physical schemas and every source field/ID. Canonical JSONL is an
evidence sidecar, not a replacement dataset. No projection, filtering, renaming,
repair, reprojection, deduplication or inferred CRS is supported here.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import re
import sqlite3
import struct
from contextlib import closing
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

import fiona
import pyproj
import shapely
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
from fiona.model import to_dict
from pyproj import CRS
from pyproj.exceptions import CRSError
from shapely import from_wkb, get_srid, has_m
from shapely.errors import ShapelyError
from shapely.geometry import mapping

from dagster_hifld.evidence_io import new_evidence_stream, validate_evidence_directory, write_json_atomic, write_text_atomic
from dagster_hifld.validation import (
    ComparisonLimits,
    _no_duplicate_members,
    compare_full_rows,
    sha256_file,
)

ADAPTER = "native-physical-values-v1"
JSONL_ADAPTER = "typed-geojson-feature-jsonl-v1"
FEATURE_ID = "$hifld.source_feature_id"
PARTITION = "$hifld.partition"
GEOMETRY_STATE = "$hifld.geometry_state"
RESERVED = {FEATURE_ID, PARTITION, GEOMETRY_STATE}
FORMATS = {"geopackage", "geoparquet", "parquet", "arrow_ipc_file"}


@dataclass(frozen=True)
class NativeReadLimits:
    max_objects: int = 4096
    batch_rows: int = 1024
    max_batch_bytes: int = 16 * 1024 * 1024
    max_row_group_bytes: int = 128 * 1024 * 1024
    max_metadata_bytes: int = 16 * 1024 * 1024
    max_record_bytes: int = 32 * 1024 * 1024
    max_output_bytes: int = 4 * 1024 * 1024 * 1024
    max_nested_values_per_record: int = 100_000


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()


def _digest(value: Any) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _identity(path: Path) -> tuple:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _write_json(path: Path, value: Any) -> None:
    write_json_atomic(path, value)


def _code_digest() -> str:
    return _digest({path.name: sha256_file(path) for path in (Path(__file__), Path(__file__).with_name("validation.py"), Path(__file__).with_name("evidence_io.py"))})


def reader_versions() -> dict:
    return {"fiona": fiona.__version__, "gdal": fiona.__gdal_version__, "pyarrow": pa.__version__,
            "shapely": shapely.__version__, "geos": shapely.geos_version_string, "pyproj": pyproj.__version__, "proj": pyproj.proj_version_str}


def _metadata(metadata: dict[bytes, bytes] | None) -> list[list[str]]:
    return [[base64.b64encode(key).decode(), base64.b64encode(value).decode()] for key, value in sorted((metadata or {}).items())]


def _scalar(value: Any) -> Any:
    """Canonical lossless primitives. Null stays null for keyed null detection."""
    if value is None:
        return None
    if isinstance(value, bool):
        return ["boolean", value]
    if isinstance(value, int):
        return ["integer", str(value)]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Non-finite floating values require an explicit future rule")
        return ["binary_float", value.hex()]
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Non-finite decimal")
        return ["decimal", str(value)]
    if isinstance(value, str):
        return ["string", value]
    if isinstance(value, bytes):
        return ["binary", base64.b64encode(value).decode()]
    raise ValueError(f"Unsupported native scalar: {type(value).__name__}")


def _arrow_scalar(value: pa.Scalar, budget: list[int], maximum: int) -> Any:
    budget[0] += 1
    if budget[0] > maximum:
        raise ValueError("Nested Arrow value count exceeds per-record budget")
    if not value.is_valid:
        return None
    if pa.types.is_struct(value.type):
        if any(not field.nullable and not value[field.name].is_valid for field in value.type):
            raise ValueError("Null value violates nested physical Arrow field nullability")
        return ["struct", [[field.name, _arrow_scalar(value[field.name], budget, maximum)] for field in value.type]]
    if pa.types.is_list(value.type) or pa.types.is_large_list(value.type) or pa.types.is_fixed_size_list(value.type):
        if not value.type.value_field.nullable and value.values.null_count:
            raise ValueError("Null value violates physical Arrow list-element nullability")
        return ["list", [_arrow_scalar(item, budget, maximum) for item in value.values]]
    if pa.types.is_map(value.type):
        if ((not value.type.key_field.nullable and value.values.field("key").null_count)
                or (not value.type.item_field.nullable and value.values.field("value").null_count)):
            raise ValueError("Null value violates physical Arrow map key/item nullability")
        return ["map", [[_arrow_scalar(item["key"], budget, maximum), _arrow_scalar(item["value"], budget, maximum)] for item in value.values]]
    if pa.types.is_temporal(value.type):
        # Arrow's raw integer ticks avoid datetime microsecond truncation and
        # preserve unit/timezone identity; no timezone/date normalization.
        return ["temporal", {"physical_type": str(value.type), "ticks": str(value.value)}]
    return _scalar(value.as_py())


def _check_arrow_type(kind: pa.DataType) -> None:
    if pa.types.is_struct(kind):
        if len({field.name for field in kind}) != kind.num_fields:
            raise ValueError("Duplicate nested Arrow field names")
        for field in kind:
            _check_arrow_type(field.type)
        return
    if pa.types.is_list(kind) or pa.types.is_large_list(kind) or pa.types.is_fixed_size_list(kind):
        _check_arrow_type(kind.value_type)
        return
    if pa.types.is_map(kind):
        _check_arrow_type(kind.key_type)
        _check_arrow_type(kind.item_type)
        return
    if not (pa.types.is_null(kind) or pa.types.is_boolean(kind) or pa.types.is_integer(kind)
            or pa.types.is_floating(kind) or pa.types.is_decimal(kind) or pa.types.is_temporal(kind)
            or pa.types.is_string(kind) or pa.types.is_large_string(kind)
            or pa.types.is_binary(kind) or pa.types.is_large_binary(kind) or pa.types.is_fixed_size_binary(kind)):
        raise ValueError(f"Unsupported Arrow physical type: {kind}")


def _arrow_field(field: pa.Field) -> dict:
    result = {"name": field.name, "physical_type": str(field.type), "nullable": field.nullable, "metadata": _metadata(field.metadata)}
    if pa.types.is_struct(field.type):
        result["children"] = [_arrow_field(child) for child in field.type]
    elif pa.types.is_list(field.type) or pa.types.is_large_list(field.type) or pa.types.is_fixed_size_list(field.type):
        result["children"] = [_arrow_field(field.type.value_field)]
    elif pa.types.is_map(field.type):
        result["children"] = [_arrow_field(field.type.key_field), _arrow_field(field.type.item_field)]
    return result


def _check_crs(observed: Any, policy: dict) -> None:
    if policy["geometry"]:
        if not observed or not policy.get("crs"):
            raise ValueError("Unknown native or policy CRS")
        if not CRS.from_user_input(observed).equals(CRS.from_user_input(policy["crs"])):
            raise ValueError("Native CRS differs from explicit comparison policy; no reprojection")
    elif observed is not None:
        raise ValueError("Unexpected spatial CRS under nonspatial policy")


def _geometry(wkb: bytes | None) -> tuple[dict | None, Any]:
    if wkb is None:
        return None, ["null"]
    geom = from_wkb(wkb, on_invalid="raise")
    if get_srid(geom):
        raise ValueError("Embedded EWKB SRID requires an explicit future physical-geometry rule")
    if has_m(geom):
        raise ValueError("Measured geometry is not supported losslessly by this adapter")
    if geom.is_empty:
        raise ValueError("Empty geometry requires an explicit future comparison rule")
    return mapping(geom), ["geometry", geom.geom_type, bool(geom.has_z)]


def _feature(properties: dict, geometry: dict | None, state: Any, feature_id: Any, partition: dict) -> dict:
    if RESERVED.intersection(properties):
        raise ValueError("Source field collides with reserved evidence-only namespace")
    return {"type": "Feature", "geometry": geometry, "properties": {
        **properties, FEATURE_ID: feature_id, PARTITION: ["partition", partition], GEOMETRY_STATE: state,
    }}


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _gpkg_geometry(blob: bytes | None, srs_id: int | None) -> bytes | None:
    if blob is None:
        return None
    if len(blob) < 8 or blob[:3] != b"GP\x00" or blob[3] & 0xE0:
        raise ValueError("Unsupported GeoPackage geometry header")
    flags = blob[3]
    envelope = (flags >> 1) & 7
    envelope_size = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get(envelope)
    if envelope_size is None or flags & 0x10:
        raise ValueError("Unsupported or empty GeoPackage geometry")
    stored_srs = struct.unpack("<i" if flags & 1 else ">i", blob[4:8])[0]
    if stored_srs != srs_id:
        raise ValueError("Per-feature GeoPackage CRS differs from layer CRS")
    return blob[8 + envelope_size:]


def _read_gpkg(path: Path, spec: dict, policy: dict, partition: dict, context: dict) -> Iterator[dict]:
    layer = spec.get("layer")
    if not isinstance(layer, str) or not layer:
        raise ValueError("GeoPackage requires an explicit layer; no first-layer selection")
    if any(Path(str(path) + suffix).exists() for suffix in ("-wal", "-shm", "-journal")):
        raise ValueError("GeoPackage must be an immutable checkpointed capture")
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)) as sql, fiona.open(path, layer=layer) as source:
        if source.driver != "GPKG":
            raise ValueError("Only native GeoPackage is supported by this Fiona adapter")
        fields = list(sql.execute(f"PRAGMA table_xinfo({_quoted(layer)})"))
        if not fields or any(field[6] for field in fields):
            raise ValueError("Missing schema or unsupported generated/hidden GeoPackage fields")
        primary = [field[1] for field in fields if field[5]]
        if len(primary) != 1:
            raise ValueError("GeoPackage requires one explicit physical primary key")
        fid = primary[0]
        geometry_info = list(sql.execute("SELECT column_name,srs_id,geometry_type_name,z,m FROM gpkg_geometry_columns WHERE table_name=?", (layer,)))
        if len(geometry_info) != 1 or not policy["geometry"]:
            raise ValueError("Only explicit single-geometry GeoPackage layers are supported")
        geometry_column, srs_id, *_ = geometry_info[0]
        if spec.get("geometry_column") != geometry_column:
            raise ValueError("Explicit GeoPackage geometry column differs from physical layer")
        if geometry_info[0][4] != 0:
            raise ValueError("Measured/optional-measure GeoPackage geometry is unsupported")
        _check_crs(source.crs_wkt, policy)
        names = [field[1] for field in fields]
        property_names = [name for name in names if name not in {fid, geometry_column}]
        if set(property_names) != set(source.schema["properties"]):
            raise ValueError("Fiona omitted or added physical GeoPackage fields")
        if RESERVED.intersection(property_names):
            raise ValueError("Reserved source field collision")
        descriptor = {"reader": "fiona+sqlite-raw", "driver": source.driver, "layer": layer,
                      "fiona_schema": to_dict(source.schema) if hasattr(source.schema, "__geo_interface__") else dict(source.schema),
                      "sqlite_columns": [list(field) for field in fields], "geometry": list(geometry_info[0]),
                      "crs_wkt": source.crs_wkt, "feature_id": {"driver_type": "string", "physical_column": fid}}
        context.update(schema=descriptor, columns=property_names,
                       nullable={field[1]: not bool(field[3]) for field in fields}, expected_rows=sql.execute(f"SELECT COUNT(*) FROM {_quoted(layer)}").fetchone()[0])
        sql.execute("PRAGMA temp_store=FILE")
        sql.execute("PRAGMA temp.cache_size=-8192")
        sql.execute("CREATE TEMP TABLE observed_source_ids (id TEXT PRIMARY KEY)")
        positions = {name: i for i, name in enumerate(names)}
        for feature in source:
            driver_id = feature.id
            if not isinstance(driver_id, str):
                raise ValueError("Unsupported Fiona intrinsic identifier type")
            rows = sql.execute(f"SELECT * FROM {_quoted(layer)} WHERE {_quoted(fid)}=?", (driver_id,)).fetchmany(2)
            if len(rows) != 1:
                raise ValueError("Fiona intrinsic ID does not resolve one physical source row")
            raw = rows[0]
            if str(raw[positions[fid]]) != driver_id:
                raise ValueError("Fiona intrinsic identifier differs from physical primary key")
            sql.execute("INSERT INTO observed_source_ids VALUES (?)", (driver_id,))
            wkb = _gpkg_geometry(raw[positions[geometry_column]], srs_id)
            geometry, state = _geometry(wkb)
            exposed_geometry = to_dict(feature.geometry) if feature.geometry is not None else None
            if _json_bytes(exposed_geometry) != _json_bytes(geometry):
                raise ValueError("Fiona geometry differs from original physical geometry")
            properties = {name: _scalar(raw[positions[name]]) for name in property_names}
            yield _feature(properties, geometry, state,
                           ["source_id", {"fiona": driver_id, "physical": _scalar(raw[positions[fid]])}], partition)


def _check_footer(path: Path, fmt: str, limits: NativeReadLimits) -> None:
    footer_size = 8 if fmt in {"parquet", "geoparquet"} else 10
    with path.open("rb") as source:
        if path.stat().st_size < footer_size:
            raise ValueError("Truncated native file")
        source.seek(-footer_size, 2)
        footer = source.read(footer_size)
    magic = b"PAR1" if footer_size == 8 else b"ARROW1"
    if footer[4:] != magic:
        raise ValueError("Invalid native file footer")
    length = int.from_bytes(footer[:4], "little")
    if length > limits.max_metadata_bytes or length + footer_size > path.stat().st_size:
        raise ValueError("Native footer exceeds metadata budget or file length")


def _read_arrow(path: Path, spec: dict, policy: dict, partition: dict, context: dict, limits: NativeReadLimits) -> Iterator[dict]:
    fmt = spec["format"]
    _check_footer(path, fmt, limits)
    with pa.memory_map(str(path), "r") as source:
        if fmt == "arrow_ipc_file":
            reader = ipc.open_file(source)
            schema = reader.schema
            batches = (reader.get_batch(index) for index in range(reader.num_record_batches))
            context["expected_rows"] = None
        else:
            reader = pq.ParquetFile(source, thrift_string_size_limit=limits.max_metadata_bytes,
                                    thrift_container_size_limit=limits.max_metadata_bytes)
            schema = reader.schema_arrow
            if any(reader.metadata.row_group(index).total_byte_size > limits.max_row_group_bytes for index in range(reader.metadata.num_row_groups)):
                raise ValueError("Parquet row group exceeds declared decompression budget")
            context["expected_rows"] = reader.metadata.num_rows
            batches = reader.iter_batches(batch_size=limits.batch_rows, use_threads=False)
        if len(schema.names) != len(set(schema.names)) or len(schema.serialize()) > limits.max_metadata_bytes:
            raise ValueError("Duplicate fields or oversized native schema")
        geometry_column = spec.get("geometry_column")
        metadata = schema.metadata or {}
        geometry_meta = None
        if policy["geometry"]:
            if not geometry_column or b"geo" not in metadata:
                raise ValueError("Spatial Arrow inputs require explicit geometry and GeoParquet metadata")
            geo = json.loads(metadata[b"geo"], object_pairs_hook=_no_duplicate_members)
            if geo.get("version") not in {"0.4.0", "1.0.0", "1.1.0"} or geo.get("primary_column") != geometry_column or set(geo.get("columns", {})) != {geometry_column}:
                raise ValueError("Unsupported version/multiple geometry columns or geometry selection")
            geometry_meta = geo["columns"][geometry_column]
            if geometry_meta.get("encoding") != "WKB":
                raise ValueError("Only lossless WKB geometry encoding is supported")
            _check_crs(geometry_meta.get("crs"), policy)
            if geometry_column not in schema.names or not pa.types.is_binary(schema.field(geometry_column).type):
                raise ValueError("GeoParquet geometry column must be physical binary WKB")
        elif geometry_column is not None or b"geo" in metadata:
            raise ValueError("Unexpected spatial input under nonspatial policy")
        for field in schema:
            _check_arrow_type(field.type)
        property_names = [name for name in schema.names if name != geometry_column]
        if RESERVED.intersection(property_names):
            raise ValueError("Reserved source field collision")
        context.update(schema={"reader": "arrow", "fields": [_arrow_field(field) for field in schema],
                               "metadata": _metadata(schema.metadata), "geometry_column": geometry_column, "feature_id": "absent"},
                       columns=property_names, nullable={field.name: field.nullable for field in schema})
        for batch in batches:
            if batch.nbytes > limits.max_batch_bytes:
                raise ValueError("Arrow decoded batch exceeds declared byte budget")
            if any(not field.nullable and batch.column(index).null_count for index, field in enumerate(schema)):
                raise ValueError("Null value violates physical Arrow field nullability")
            for index in range(batch.num_rows):
                budget = [0]
                properties = {name: _arrow_scalar(batch.column(schema.get_field_index(name))[index], budget, limits.max_nested_values_per_record) for name in property_names}
                value = batch.column(schema.get_field_index(geometry_column))[index].as_py() if geometry_column else None
                geometry, state = _geometry(value)
                declared_types = geometry_meta.get("geometry_types", geometry_meta.get("geometry_type")) if geometry_meta else None
                if isinstance(declared_types, str):
                    declared_types = [declared_types]
                actual_type = state[1] + (" Z" if state[2] else "") if geometry else None
                if geometry and (not isinstance(declared_types, list) or actual_type not in declared_types):
                    raise ValueError("Actual geometry differs from declared GeoParquet geometry types")
                yield _feature(properties, geometry, state, None, partition)


def _objects(spec: dict, limits: NativeReadLimits) -> list[tuple[dict, Path]]:
    if not isinstance(spec, dict) or spec.get("schema_version") != 1 or spec.get("format") not in FORMATS:
        raise ValueError("Unsupported native input manifest")
    allowed = {"schema_version", "format", "root", "objects", "layer", "geometry_column", "storage_prefix"}
    if set(spec) - allowed:
        raise ValueError("Unknown input parameters; no implicit filtering/projection/selection")
    storage_prefix = spec.get("storage_prefix", "")
    if not isinstance(storage_prefix, str) or (storage_prefix and (PurePosixPath(storage_prefix).is_absolute()
            or PurePosixPath(storage_prefix).as_posix() != storage_prefix or ".." in PurePosixPath(storage_prefix).parts)):
        raise ValueError("Unsafe explicit storage prefix")
    root = Path(spec["root"]).resolve()
    objects = spec.get("objects")
    if not isinstance(objects, list) or not 0 < len(objects) <= limits.max_objects:
        raise ValueError("Missing object roster or object-count budget exceeded")
    if spec["format"] == "geopackage" and len(objects) != 1:
        raise ValueError("GeoPackage adapter requires one explicit container/layer")
    selected = []
    keys = set()
    for obj in objects:
        if not isinstance(obj, dict):
            raise ValueError("Native object descriptor must be an object")
        if set(obj) - {"key", "sha256", "bytes", "rows", "partition"}:
            raise ValueError("Unknown native object parameters")
        key = obj["key"]
        if (type(obj.get("bytes")) is not int or obj["bytes"] < 0
                or not isinstance(obj.get("sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", obj["sha256"])
                or ("rows" in obj and (type(obj["rows"]) is not int or obj["rows"] < 0))):
            raise ValueError("Native object pins require exact SHA-256/length/nonnegative count")
        relative = PurePosixPath(key)
        if not isinstance(key, str) or relative.is_absolute() or relative.as_posix() != key or any(part in {".", ".."} for part in relative.parts) or key in keys:
            raise ValueError("Duplicate or unsafe native object key")
        path = root / key
        if path.is_symlink() or not path.resolve().is_relative_to(root) or not path.is_file():
            raise ValueError("Missing native object or symlink/root escape")
        if path.stat().st_size != obj["bytes"] or sha256_file(path) != obj["sha256"]:
            raise ValueError("Original native object checksum/length mismatch")
        partition = obj.get("partition", {})
        if not isinstance(partition, dict) or any(not isinstance(name, str) or type(value) not in {str, int, bool, type(None)} for name, value in partition.items()):
            raise ValueError("Unsupported partition lineage values")
        keys.add(key)
        selected.append((obj, path))
    suffixes = {"geopackage": {".gpkg"}, "geoparquet": {".parquet"}, "parquet": {".parquet"}, "arrow_ipc_file": {".arrow", ".feather"}}[spec["format"]]
    actual = set()
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in suffixes:
            actual.add(path.relative_to(root).as_posix())
            if len(actual) > limits.max_objects:
                raise ValueError("Native file roster exceeds budget")
    if actual != keys:
        raise ValueError("Native file roster incomplete or contains unpinned shards")
    return sorted(selected, key=lambda item: item[0]["key"])


def _safe_output(output_dir: Path, *specs: dict) -> None:
    # Reject before mkdir/receipt writes: captures are read-only even when a
    # caller accidentally places the evidence folder inside them.
    validate_evidence_directory(output_dir)
    for spec in specs:
        if isinstance(spec, dict) and isinstance(spec.get("root"), str) and output_dir.resolve().is_relative_to(Path(spec["root"]).resolve()):
            raise ValueError("Evidence output must be outside pinned native input root")


def normalize_native_input(spec: dict, policy: dict, output_dir: Path, *, limits: NativeReadLimits = NativeReadLimits()) -> dict:
    """Recompute a deterministic typed evidence stream from every pinned object."""
    _safe_output(output_dir, spec)
    output_dir.mkdir(parents=True, exist_ok=True)
    receipt_path = output_dir / "normalization_receipt.json"
    receipt = {"schema_version": 1, "adapter": ADAPTER, "status": "blocked", "reason_codes": ["normalization_not_complete"],
               "code_sha256": _code_digest(), "policy_sha256": _digest(policy), "policy": policy, "limits": asdict(limits),
               "reader_versions": reader_versions(), "objects": [], "rows": 0,
               "acceptance": "not_established_by_normalization"}
    _write_json(receipt_path, receipt)
    partial = output_dir / "normalized_records.jsonl.partial"
    lineage_partial = output_dir / "normalization_lineage.jsonl.partial"
    try:
        if any(type(value) is not int or value < 1 for value in asdict(limits).values()):
            raise ValueError("Native reader limits require positive integer budgets")
        if not isinstance(policy, dict) or policy.get("schema_version") != 1 or policy.get("status") not in {"proposed", "reviewed"} or type(policy.get("geometry")) is not bool:
            raise ValueError("Explicit proposed/reviewed schema/geometry policy required")
        if set(policy) - {"schema_version", "status", "geometry", "crs", "geometry_nullable", "id_fields", "coverage_fields", "allow_empty"}:
            raise ValueError("Unknown normalization policy parameters")
        for field in ("geometry_nullable", "allow_empty"):
            if field in policy and type(policy[field]) is not bool:
                raise ValueError("Native comparison flags must be explicit booleans")
        for field in ("id_fields", "coverage_fields"):
            value = policy.get(field)
            if not isinstance(value, list) or any(not isinstance(name, str) or not name for name in value) or len(value) != len(set(value)):
                raise ValueError("Native key/coverage policy requires unique explicit field names")
        if not policy["id_fields"] or (not policy["geometry"] and policy.get("crs") is not None):
            raise ValueError("Stable keys required; nonspatial inputs cannot declare a spatial CRS")
        if policy["geometry"]:
            CRS.from_user_input(policy["crs"])
        objects = _objects(spec, limits)
        if spec["format"] == "geoparquet" and not policy["geometry"]:
            raise ValueError("GeoParquet input requires an explicit spatial policy")
        initial_stats = {path: _identity(path) for _, path in objects}
        written = 0
        total_rows = 0
        reference_schema = None
        with new_evidence_stream(partial, binary=True) as output, new_evidence_stream(lineage_partial, binary=True) as lineage:
            for obj, path in objects:
                context: dict = {}
                rows = 0
                reader = (_read_gpkg(path, spec, policy, obj.get("partition", {}), context) if spec["format"] == "geopackage"
                          else _read_arrow(path, spec, policy, obj.get("partition", {}), context, limits))
                for record in reader:
                    if any(name not in record["properties"] for name in [*policy["id_fields"], *policy["coverage_fields"]]):
                        raise ValueError("Native key/coverage field absent; no generated replacement ID")
                    line = _json_bytes(record) + b"\n"
                    lineage_line = _json_bytes({"row": total_rows + 1, "object_key": obj["key"], "object_row": rows + 1,
                                               "feature_id": record["properties"][FEATURE_ID], "partition": obj.get("partition", {}),
                                               "keys": {name: record["properties"].get(name) for name in policy["id_fields"]},
                                               "record_sha256": hashlib.sha256(line).hexdigest()}) + b"\n"
                    if max(len(line), len(lineage_line)) > limits.max_record_bytes or written + len(line) + len(lineage_line) > limits.max_output_bytes:
                        raise ValueError("Normalized record/output byte budget exceeded")
                    output.write(line)
                    lineage.write(lineage_line)
                    rows += 1
                    total_rows += 1
                    written += len(line) + len(lineage_line)
                if rows != context.get("expected_rows", rows) and context.get("expected_rows") is not None:
                    raise ValueError("Native iterator does not match complete container row count")
                if "rows" in obj and rows != obj["rows"]:
                    raise ValueError("Native object row count differs from pinned expected count")
                if reference_schema is not None and context["schema"] != reference_schema:
                    raise ValueError("Partitioned native files have heterogeneous physical schemas")
                reference_schema = context["schema"]
                receipt.update(physical_schema=reference_schema, columns=context["columns"], nullable=context["nullable"])
                receipt["objects"].append({**obj, "rows_observed": rows, "physical_schema_sha256": _digest(reference_schema)})
                receipt["rows"] += rows
        if not receipt["rows"] and not policy.get("allow_empty", False):
            raise ValueError("Empty native population requires explicit policy")
        final_objects = _objects(spec, limits)
        if ([(obj, path) for obj, path in objects] != final_objects or any(_identity(path) != initial_stats[path] for _, path in objects)
                or _code_digest() != receipt["code_sha256"]):
            raise ValueError("Original native input changed during normalization")
        target = output_dir / "normalized_records.jsonl"
        lineage_target = output_dir / "normalization_lineage.jsonl"
        partial.replace(target)
        lineage_partial.replace(lineage_target)
        receipt.update(status="complete", reason_codes=[], normalized={"path": target.name, "sha256": sha256_file(target), "bytes": target.stat().st_size,
                                                                       "adapter": JSONL_ADAPTER}, parameters={key: value for key, value in spec.items() if key not in {"root", "objects"}})
        receipt["lineage"] = {"path": lineage_target.name, "sha256": sha256_file(lineage_target), "bytes": lineage_target.stat().st_size, "rows": total_rows}
    except (ValueError, OSError, KeyError, TypeError, OverflowError, RecursionError, sqlite3.Error, pa.ArrowException, ShapelyError, CRSError, fiona.errors.FionaError) as exc:
        receipt["status"] = "blocked"
        receipt["reason_codes"] = [f"normalization_incomplete:{type(exc).__name__}:{exc}"]
    receipt["receipt_id"] = _digest(receipt)
    _write_json(receipt_path, receipt)
    return receipt


def compare_native_inputs(baseline_spec: dict, candidate_spec: dict, policy: dict, output_dir: Path, *, baseline_reference: dict,
                          limits: NativeReadLimits = NativeReadLimits(), comparison_limits: ComparisonLimits = ComparisonLimits()) -> dict:
    """Normalize full pinned inputs, compare all rows and preserve schema drift."""
    _safe_output(output_dir, baseline_spec, candidate_spec)
    output_dir.mkdir(parents=True, exist_ok=True)
    blocked = {"schema_version": 1, "status": "blocked", "reason_codes": ["native_comparison_not_complete"],
               "acceptance": "not_established_by_comparison", "adapter": JSONL_ADAPTER}
    _write_json(output_dir / "baseline_comparison.json", blocked)
    receipts = {side: normalize_native_input(spec, policy, output_dir / side, limits=limits)
                for side, spec in (("baseline", baseline_spec), ("candidate", candidate_spec))}
    descriptors = {side: {"path": f"{side}/normalization_receipt.json", "sha256": sha256_file(output_dir / side / "normalization_receipt.json"),
                          "receipt": receipt} for side, receipt in receipts.items()}
    try:
        if any(receipt["status"] != "complete" for receipt in receipts.values()):
            raise ValueError("Native normalization incomplete; see pinned failure receipts")
        left, right = receipts["baseline"], receipts["candidate"]
        reference = dict(baseline_reference)
        derived_pin = {**left["normalized"], "derivation": {"source_sha256": sorted(obj["sha256"] for obj in left["objects"]),
                        "code_sha256": left["code_sha256"], "parameters": {"policy_sha256": left["policy_sha256"], "receipt_sha256": descriptors["baseline"]["sha256"]}}}
        if "comparison_input" not in reference:
            if reference.get("acceptance") != "unapproved_candidate" or reference.get("decision"):
                raise ValueError("Cannot create an accepted baseline input pin; explicit unapproved research reference required")
            reference["comparison_input"] = derived_pin
        elif reference["comparison_input"] != derived_pin:
            raise ValueError("Native recomputation differs from independently pinned baseline derivation")
        if sorted(obj["sha256"] for obj in reference.get("data_objects", [])) != derived_pin["derivation"]["source_sha256"]:
            raise ValueError("Native baseline objects differ from independently identified baseline objects")
        columns = left["columns"]
        contract = {"columns": {name: {"type": "array", "nullable": left["nullable"][name]} for name in columns},
                    "id_fields": policy["id_fields"], "coverage_fields": policy["coverage_fields"], "geometry": policy["geometry"],
                    "crs": policy.get("crs"), "geometry_nullable": policy.get("geometry_nullable", False), "allow_empty": policy.get("allow_empty", False)}
        contract["columns"].update({FEATURE_ID: {"type": "array", "nullable": True}, PARTITION: {"type": "array", "nullable": False}, GEOMETRY_STATE: {"type": "array", "nullable": False}})
        result = compare_full_rows(output_dir / "baseline" / left["normalized"]["path"], output_dir / "candidate" / right["normalized"]["path"],
                                   contract, output_dir, baseline_sha256=left["normalized"]["sha256"], candidate_sha256=right["normalized"]["sha256"],
                                   baseline_reference=reference, limits=comparison_limits)
        schema_equal = left["physical_schema"] == right["physical_schema"]
        result["physical_schema"] = {"equal": schema_equal, "baseline": left["physical_schema"], "candidate": right["physical_schema"]}
        if not schema_equal and result["status"] == "pass":
            result["status"] = "needs_decision"
            result["reason_codes"].append("physical_schema_or_source_id_representation_changed")
        result["native_inputs"] = descriptors
        result["verification_scope"] = "native_originals_recomputed"
    except (ValueError, OSError, KeyError, TypeError) as exc:
        result = {**blocked, "reason_codes": [f"native_comparison_incomplete:{type(exc).__name__}:{exc}"], "native_inputs": descriptors}
    _write_json(output_dir / "baseline_comparison.json", result)
    write_text_atomic(output_dir / "baseline_comparison.md", f"Native full comparison: **{result['status']}**. Acceptance remains separate.\n\nComplete normalization receipts and physical schemas are pinned in the JSON report.\n")
    return result


def _load_json(path: Path) -> dict:
    if path.stat().st_size > 8 * 1024 * 1024:
        raise ValueError("CLI manifest exceeds bounded metadata budget")
    return json.loads(path.read_text(), object_pairs_hook=_no_duplicate_members)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    actions = parser.add_subparsers(dest="action", required=True)
    normalize = actions.add_parser("normalize")
    normalize.add_argument("--spec", type=Path, required=True)
    compare = actions.add_parser("compare")
    compare.add_argument("--baseline-spec", type=Path, required=True)
    compare.add_argument("--candidate-spec", type=Path, required=True)
    compare.add_argument("--baseline-reference", type=Path, required=True)
    args = parser.parse_args()
    loaded_specs = []
    load_error = None
    try:
        for path in ([args.spec] if args.action == "normalize" else [args.baseline_spec, args.candidate_spec]):
            loaded_specs.append(_load_json(path))
    except (ValueError, OSError, KeyError, TypeError, RecursionError) as exc:
        load_error = exc
    try:
        _safe_output(args.output, *loaded_specs)
    except ValueError as exc:
        parser.exit(2, f"Unsafe evidence location; no output written: {exc}\n")
    args.output.mkdir(parents=True, exist_ok=True)
    result_path = args.output / ("normalization_receipt.json" if args.action == "normalize" else "baseline_comparison.json")
    result = {"status": "blocked", "acceptance": "not_established_by_comparison", "reason_codes": ["cli_inputs_not_loaded"]}
    _write_json(result_path, result)
    try:
        if load_error is not None:
            raise ValueError(f"Unreadable native input manifest: {load_error}")
        policy = _load_json(args.policy)
        if args.action == "normalize":
            result = normalize_native_input(loaded_specs[0], policy, args.output)
        else:
            result = compare_native_inputs(*loaded_specs, policy, args.output,
                                           baseline_reference=_load_json(args.baseline_reference))
    except (ValueError, OSError, KeyError, TypeError, RecursionError) as exc:
        result["reason_codes"] = [f"cli_incomplete:{type(exc).__name__}:{exc}"]
        _write_json(result_path, result)
    print(json.dumps({"status": result["status"], "output": str(args.output), "acceptance": result["acceptance"]}))
    raise SystemExit(0 if result["status"] in {"complete", "pass", "needs_decision"} else 2)


if __name__ == "__main__":
    main()

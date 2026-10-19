"""Bounded decoder for ArcGIS FeatureCollection PBF query responses.

The wire format follows Esri's published FeatureCollection protobuf schema.
Raw PBF pages remain the authoritative transport evidence; the ArcGIS JSON
objects returned here are a derived representation for the existing catalog
path and retain the service coordinate transform.
"""

from __future__ import annotations

import json
import struct
from decimal import Decimal
from typing import Any


_FIELD_TYPES = {
    0: "esriFieldTypeSmallInteger",
    1: "esriFieldTypeInteger",
    2: "esriFieldTypeSingle",
    3: "esriFieldTypeDouble",
    4: "esriFieldTypeString",
    5: "esriFieldTypeDate",
    6: "esriFieldTypeOID",
    7: "esriFieldTypeGeometry",
    8: "esriFieldTypeBlob",
    9: "esriFieldTypeRaster",
    10: "esriFieldTypeGUID",
    11: "esriFieldTypeGlobalID",
    12: "esriFieldTypeXML",
    13: "esriFieldTypeBigInteger",
    14: "esriFieldTypeDateOnly",
    15: "esriFieldTypeTimeOnly",
    16: "esriFieldTypeTimestampOffset",
}
_MAX_FIELDS = 256
_MAX_FEATURES = 2_000
_MAX_VERTICES = 50_000_000
_MAX_NESTING_BYTES = 32 * 1024 * 1024


class EsriPbfError(ValueError):
    """The response is malformed, incomplete, or outside supported limits."""


class DraftGeoParquetWriter:
    """Typed WKB GeoParquet writer retaining exact Esri declarations in metadata."""

    def __init__(self, target, field_declarations: list[dict[str, Any]], spatial_reference: dict[str, Any]):
        import pyarrow as pa
        import pyarrow.parquet as pq
        from pyproj import CRS

        self.pa = pa
        self.pq = pq
        self.declarations = field_declarations
        self.fields = [field for field in field_declarations if field.get("type") != "esriFieldTypeGeometry"]
        types = {
            "esriFieldTypeSmallInteger": pa.int16(),
            "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeOID": pa.int64(),
            "esriFieldTypeSingle": pa.float32(),
            "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeGUID": pa.string(),
            "esriFieldTypeGlobalID": pa.string(),
            "esriFieldTypeDate": pa.timestamp("ms", tz="UTC"),
        }
        if any(field.get("type") not in types for field in self.fields):
            raise EsriPbfError("unsupported_source_field_physical_type")
        fields = [pa.field(field["name"], types[field["type"]], nullable=bool(field.get("nullable", True)),
                           metadata={b"esri:field_type": str(field["type"]).encode()}) for field in self.fields]
        fields.append(pa.field("geometry", pa.binary(), nullable=False))
        try:
            crs = CRS.from_epsg(int(spatial_reference["latestWkid"]))
        except Exception as exc:
            raise EsriPbfError("unsupported_source_spatial_reference") from exc
        geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
            "encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"], "crs": crs.to_json_dict(), "edges": "planar"}}}
        metadata = {
            b"geo": json.dumps(geo, sort_keys=True, separators=(",", ":")).encode(),
            b"hifld:source_field_declarations": json.dumps(field_declarations, sort_keys=True, separators=(",", ":")).encode(),
            b"hifld:geometry_quantization": b"Decoded from Esri PBF transform; quantized source ordinates are retained in raw PBF captures.",
            b"hifld:draft": b"true",
        }
        self.schema = pa.schema(fields, metadata=metadata)
        self.writer = pq.ParquetWriter(target, self.schema, compression="zstd", version="2.6")
        self.row_count = 0
        self.vertex_count = 0

    def write_page(self, collection: dict[str, Any]) -> None:
        from datetime import datetime, timedelta, timezone
        import shapely
        from fiona.io import MemoryFile
        from shapely.geometry import shape

        pa = self.pa
        if collection.get("geometryType") != "esriGeometryPolygon":
            raise EsriPbfError("unsupported_geoparquet_geometry_type")
        columns = {field["name"]: [] for field in self.fields}
        geometries = []
        feature_set = {key: collection[key] for key in (
            "objectIdFieldName", "globalIdFieldName", "geometryType", "spatialReference", "fields", "features"
        ) if key in collection}
        encoded = json.dumps(feature_set, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
        with MemoryFile(encoded, ext=".json") as memory_file:
            with memory_file.open(driver="ESRIJSON") as esri_source:
                if len(esri_source) != len(collection["features"]):
                    raise EsriPbfError("esrijson_geometry_feature_count_mismatch")
                geometries_by_id = {}
                for esri_feature in esri_source:
                    if esri_feature.geometry is None:
                        raise EsriPbfError("esrijson_geometry_missing")
                    geometries_by_id[int(esri_feature.id)] = shapely.to_wkb(shape(esri_feature.geometry), byte_order=1)
        for feature in collection["features"]:
            attrs = feature["attributes"]
            object_id = attrs.get(collection["objectIdFieldName"])
            if type(object_id) is not int or object_id not in geometries_by_id:
                raise EsriPbfError("esrijson_geometry_id_mismatch")
            for field in self.fields:
                name, kind = field["name"], field["type"]
                value = attrs[name]
                if kind == "esriFieldTypeDate" and value is not None:
                    value = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=value)
                columns[name].append(value)
            self.vertex_count += sum(len(ring) for ring in feature["geometry"]["rings"])
            geometries.append(geometries_by_id[object_id])
        arrays = [pa.array(columns[field["name"]], type=self.schema.field(field["name"]).type) for field in self.fields]
        arrays.append(pa.array(geometries, type=pa.binary()))
        self.writer.write_table(pa.Table.from_arrays(arrays, schema=self.schema))
        self.row_count += len(geometries)

    def close(self) -> tuple[int, int]:
        self.writer.close()
        return self.row_count, self.vertex_count


def _varint(data: bytes, offset: int) -> tuple[int, int]:
    value = shift = 0
    while offset < len(data) and shift <= 70:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, offset
        shift += 7
    raise EsriPbfError("invalid_or_truncated_varint")


def _wire_fields(data: bytes) -> list[tuple[int, int, int | bytes]]:
    if len(data) > _MAX_NESTING_BYTES:
        raise EsriPbfError("protobuf_message_exceeds_size_limit")
    fields: list[tuple[int, int, int | bytes]] = []
    offset = 0
    while offset < len(data):
        key, offset = _varint(data, offset)
        number, wire_type = key >> 3, key & 7
        if number == 0:
            raise EsriPbfError("protobuf_field_number_zero")
        if wire_type == 0:
            value, offset = _varint(data, offset)
        elif wire_type == 1:
            end = offset + 8
            if end > len(data):
                raise EsriPbfError("truncated_fixed64")
            value, offset = data[offset:end], end
        elif wire_type == 2:
            size, offset = _varint(data, offset)
            end = offset + size
            if end > len(data):
                raise EsriPbfError("truncated_length_delimited_field")
            value, offset = data[offset:end], end
        elif wire_type == 5:
            end = offset + 4
            if end > len(data):
                raise EsriPbfError("truncated_fixed32")
            value, offset = data[offset:end], end
        else:
            raise EsriPbfError("unsupported_protobuf_wire_type")
        fields.append((number, wire_type, value))
    return fields


def _values(data: bytes, number: int) -> list[int | bytes]:
    return [value for field, _, value in _wire_fields(data) if field == number]


def _one(data: bytes, number: int, *, required: bool = False) -> int | bytes | None:
    found = _values(data, number)
    if len(found) > 1:
        raise EsriPbfError("duplicate_singular_protobuf_field")
    if not found:
        if required:
            raise EsriPbfError("missing_required_protobuf_field")
        return None
    return found[0]


def _packed_varints(data: bytes, *, signed: bool = False) -> list[int]:
    out: list[int] = []
    offset = 0
    while offset < len(data):
        value, offset = _varint(data, offset)
        out.append((value >> 1) ^ -(value & 1) if signed else value)
    return out


def _decode_value(data: bytes) -> tuple[str, Any]:
    selected: tuple[str, Any] | None = None
    for number, wire_type, value in _wire_fields(data):
        if number == 11:  # Value.index is advisory for sparse values.
            continue
        if selected is not None:
            raise EsriPbfError("multiple_value_oneof_members")
        if number == 1 and wire_type == 2:
            try:
                selected = ("string_value", value.decode("utf-8"))
            except UnicodeDecodeError as exc:
                raise EsriPbfError("invalid_utf8_string_value") from exc
        elif number == 2 and wire_type == 5:
            selected = ("float_value", struct.unpack("<f", value)[0])
        elif number == 3 and wire_type == 1:
            selected = ("double_value", struct.unpack("<d", value)[0])
        elif number == 4 and wire_type == 0:
            selected = ("sint_value", (value >> 1) ^ -(value & 1))
        elif number == 5 and wire_type == 0:
            selected = ("uint_value", value)
        elif number == 6 and wire_type == 0:
            selected = ("int64_value", value - (1 << 64) if value >= (1 << 63) else value)
        elif number == 7 and wire_type == 0:
            selected = ("uint64_value", value)
        elif number == 8 and wire_type == 0:
            selected = ("sint64_value", (value >> 1) ^ -(value & 1))
        elif number in (9, 10) and wire_type == 0:
            selected = ("bool_value" if number == 9 else "null_value", bool(value))
        else:
            raise EsriPbfError("unsupported_value_encoding")
    if selected is None:
        return "null_value", None
    if selected[0] == "null_value":
        return "null_value", None
    return selected


def _field(data: bytes) -> dict[str, Any]:
    result = {number: value for number, _, value in _wire_fields(data)}
    name = result.get(1)
    kind = result.get(2, 0)
    if not isinstance(name, bytes) or not isinstance(kind, int) or kind not in _FIELD_TYPES:
        raise EsriPbfError("invalid_field_declaration")
    try:
        decoded_name = name.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EsriPbfError("invalid_field_name") from exc
    return {"name": decoded_name, "type_code": kind, "type": _FIELD_TYPES[kind]}


def _transform(data: bytes) -> dict[str, Any]:
    raw: dict[int, int | bytes] = {n: v for n, _, v in _wire_fields(data)}
    scale_bytes, translate_bytes = raw.get(2), raw.get(3)
    if not isinstance(scale_bytes, bytes) or not isinstance(translate_bytes, bytes):
        raise EsriPbfError("missing_coordinate_transform")

    def doubles(message: bytes) -> dict[int, float]:
        out: dict[int, float] = {}
        for number, wire, value in _wire_fields(message):
            if wire != 1:
                raise EsriPbfError("invalid_coordinate_transform_value")
            out[number] = struct.unpack("<d", value)[0]
        return out

    origin = raw.get(1, 0)
    if origin not in (0, 1):
        raise EsriPbfError("unsupported_quantization_origin")
    scales, translates = doubles(scale_bytes), doubles(translate_bytes)
    result = {
        "quantizeOriginPosition": "upperLeft" if origin == 0 else "lowerLeft",
        "scale": scales,
        "translate": translates,
    }
    if 1 not in scales or 2 not in scales or 1 not in translates or 2 not in translates:
        raise EsriPbfError("incomplete_coordinate_transform")
    return result


def _geometry(data: bytes, transform: dict[str, Any]) -> tuple[dict[str, Any], int]:
    geometry_type = _one(data, 1, required=True)
    if geometry_type != 3:
        raise EsriPbfError("unsupported_geometry_type")
    lengths: list[int] = []
    coordinates: list[int] = []
    for number, wire, value in _wire_fields(data):
        if number in (2, 3):
            if wire != 2 or not isinstance(value, bytes):
                raise EsriPbfError("invalid_packed_geometry_array")
            (lengths if number == 2 else coordinates).extend(
                _packed_varints(value, signed=number == 3)
            )
    if len(lengths) > _MAX_VERTICES or sum(lengths) > _MAX_VERTICES:
        raise EsriPbfError("geometry_vertex_count_exceeds_limit")
    if sum(lengths) * 2 != len(coordinates):
        raise EsriPbfError("geometry_coordinate_count_mismatch")

    scale = transform["scale"]
    translate = transform["translate"]
    x_scale = Decimal(str(scale[1]))
    y_scale = Decimal(str(scale[2]))
    x_translate = Decimal(str(translate[1]))
    y_translate = Decimal(str(translate[2]))
    y_sign = -1 if transform["quantizeOriginPosition"] == "upperLeft" else 1
    rings: list[list[list[float]]] = []
    offset = 0
    for ring_length in lengths:
        ring: list[list[float]] = []
        x = y = 0
        for vertex in range(ring_length):
            dx, dy = coordinates[offset], coordinates[offset + 1]
            offset += 2
            if vertex == 0:
                x, y = dx, dy
            else:
                x += dx
                y += dy
            world_x = x_translate + Decimal(x) * x_scale
            world_y = y_translate + Decimal(y_sign * y) * y_scale
            ring.append([float(world_x), float(world_y)])
        rings.append(ring)
    return {"rings": rings}, sum(lengths)


def decode_feature_collection_pbf(data: bytes) -> dict[str, Any]:
    """Decode one PBF query page, rejecting unsupported or incomplete records."""
    if not isinstance(data, bytes) or not data:
        raise EsriPbfError("empty_pbf_response")
    top = _wire_fields(data)
    query_result = _one(bytes_value_fields(top, 2), 1, required=True)
    if not isinstance(query_result, bytes):
        raise EsriPbfError("invalid_feature_result")
    result_fields = _wire_fields(query_result)
    result = {number: value for number, _, value in result_fields}
    if result.get(7) != 3:
        raise EsriPbfError("source_layer_is_not_polygon")
    schema_raw = [value for number, wire, value in result_fields if number == 13 and wire == 2]
    if not schema_raw or len(schema_raw) > _MAX_FIELDS:
        raise EsriPbfError("missing_or_oversized_field_schema")
    schema = [_field(field_data) for field_data in schema_raw if isinstance(field_data, bytes)]
    if len(schema) != len(schema_raw) or len({field["name"] for field in schema}) != len(schema):
        raise EsriPbfError("duplicate_or_invalid_field_schema")
    object_id_name = result.get(1)
    global_id_name = result.get(3)
    spatial_raw = result.get(8)
    transform_raw = result.get(12)
    if not isinstance(object_id_name, bytes) or not isinstance(spatial_raw, bytes) or not isinstance(transform_raw, bytes):
        raise EsriPbfError("missing_source_identity_or_geometry_metadata")
    spatial = {n: v for n, _, v in _wire_fields(spatial_raw)}
    transform = _transform(transform_raw)
    fields = [{"name": f["name"], "type": f["type"]} for f in schema]
    feature_raw = [value for number, wire, value in result_fields if number == 15 and wire == 2]
    if not feature_raw or len(feature_raw) > _MAX_FEATURES:
        raise EsriPbfError("empty_or_oversized_feature_page")
    features = []
    total_vertices = 0
    for feature_data in feature_raw:
        if not isinstance(feature_data, bytes):
            raise EsriPbfError("invalid_feature_message")
        feature_fields = _wire_fields(feature_data)
        attributes_raw = [value for number, wire, value in feature_fields if number == 1 and wire == 2]
        geometry_raw = [value for number, wire, value in feature_fields if number == 2 and wire == 2]
        if len(attributes_raw) != len(schema) or len(geometry_raw) != 1:
            raise EsriPbfError("feature_attribute_or_geometry_roster_mismatch")
        attrs: dict[str, Any] = {}
        for declaration, value_data in zip(schema, attributes_raw):
            if not isinstance(value_data, bytes):
                raise EsriPbfError("invalid_feature_attribute")
            value_kind, value = _decode_value(value_data)
            if value_kind == "null_value":
                attrs[declaration["name"]] = None
                continue
            if declaration["type_code"] in (0, 1, 6) and value_kind not in ("sint_value", "uint_value", "int64_value", "uint64_value"):
                raise EsriPbfError("integer_field_value_type_mismatch")
            if declaration["type_code"] in (2, 3) and value_kind not in ("float_value", "double_value"):
                raise EsriPbfError("floating_field_value_type_mismatch")
            if declaration["type_code"] in (4, 10, 11, 12) and value_kind != "string_value":
                raise EsriPbfError("string_field_value_type_mismatch")
            if declaration["type_code"] == 5 and value_kind not in ("sint64_value", "int64_value"):
                raise EsriPbfError("date_field_value_type_mismatch")
            attrs[declaration["name"]] = value
        geometry, vertex_count = _geometry(geometry_raw[0], transform)
        total_vertices += vertex_count
        if total_vertices > _MAX_VERTICES:
            raise EsriPbfError("page_vertex_count_exceeds_limit")
        features.append({"attributes": attrs, "geometry": geometry})
    try:
        object_id_name = object_id_name.decode("utf-8")
        global_id_name = global_id_name.decode("utf-8") if isinstance(global_id_name, bytes) else ""
    except UnicodeDecodeError as exc:
        raise EsriPbfError("invalid_object_id_field_name") from exc
    if object_id_name not in {field["name"] for field in schema}:
        raise EsriPbfError("object_id_field_missing_from_schema")
    if result.get(9, 0):
        raise EsriPbfError("query_page_exceeded_transfer_limit")
    return {
        "objectIdFieldName": object_id_name,
        "globalIdFieldName": global_id_name,
        "geometryType": "esriGeometryPolygon",
        "spatialReference": {
            key: spatial[number]
            for number, key in ((1, "wkid"), (2, "latestWkid"), (3, "vcsWkid"), (4, "latestVcsWkid"), (5, "wkt"), (6, "wkt2"))
            if number in spatial and spatial[number] not in (0, b"")
        },
        "hasZ": bool(result.get(10, 0)),
        "hasM": bool(result.get(11, 0)),
        "fields": fields,
        "transform": transform,
        "features": features,
    }


def bytes_value_fields(fields: list[tuple[int, int, int | bytes]], number: int) -> bytes:
    matches = [value for field, wire, value in fields if field == number and wire == 2]
    if len(matches) != 1 or not isinstance(matches[0], bytes):
        raise EsriPbfError("missing_or_duplicate_query_result")
    return matches[0]

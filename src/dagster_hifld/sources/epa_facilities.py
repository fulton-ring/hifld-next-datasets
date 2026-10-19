"""EPA Locations MapServer profiles and native staging for Facilities layers.

Layer 1 is ordinary EPA Facilities and layer 0 is the distinct labels layer.
Both profiles preserve producer field declarations, source IDs and NAD83 Shape.
"""
from __future__ import annotations

import fcntl
from dataclasses import dataclass
from datetime import datetime, timezone
import importlib.metadata
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import stat
import subprocess
import tempfile
from uuid import uuid4
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from types import MappingProxyType
from typing import Mapping
from urllib.parse import urlencode
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file

SERVICE_URL = "https://geodata.epa.gov/arcgis/rest/services/OEI/EPA_Locations/MapServer"
SOURCE_CRS = 4269  # NAD83 source coordinates; the service's default display CRS is 3857.
DISPLAY_CRS = 3857


@dataclass(frozen=True)
class LayerProfile:
    """Immutable source-level layer identity and any verified physical schema."""

    layer_id: int
    name: str
    slug: str
    geometry_type: str | None
    source_crs: int
    geometry_keys: tuple[str, ...] = ("x", "y", "z")
    source_spatial_reference: Mapping[str, object] | None = None
    display_spatial_reference: Mapping[str, object] | None = None
    field_definitions: tuple[Mapping[str, object], ...] = ()
    page_size: int = 20
    count: int | None = None
    object_ids: tuple[int, ...] = ()
    source_record_sha256: str | None = None
    schema_verified: bool = False
    population_verified: bool = False


@dataclass(frozen=True)
class EpaFacilitiesProfile:
    service_url: str
    ordinary: LayerProfile
    labels: LayerProfile
    service_title: str
    publisher: str
    license_url: str
    release_date: str | None
    release_date_note: str


# Layer 0 metadata captured from the bounded #67 preflight.  Preserve the
# representation-rule coded domain exactly as declared by the producer.
_LABEL_FIELDS = (
    MappingProxyType({"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False, "domain": None}),
    MappingProxyType({"name": "Shape", "type": "esriFieldTypeGeometry", "nullable": True, "domain": None}),
    MappingProxyType({"name": "SHORTNAME", "type": "esriFieldTypeString", "length": 250, "nullable": True, "domain": None}),
    MappingProxyType({
        "name": "RuleID_1", "type": "esriFieldTypeInteger", "nullable": True,
        "domain": MappingProxyType({
            "type": "codedValue", "name": "EPA_Facilities_Rep_Rules",
            "description": "Representation rules",
            "codedValues": (
                (1, "Headquarters - Main"), (8, "Headquarters - Other"),
                (2, "Special Offices"), (5, "Research Labs"),
                (3, "Regional Offices"), (4, "Regional Satellite Offices"),
                (-1, "Free Representation"),
            ),
        }),
    }),
)

_RULE_DOMAIN = MappingProxyType({
    "type": "codedValue", "name": "EPA_Facilities_Rep_Rules",
    "description": "Representation rules",
    "codedValues": (
        MappingProxyType({"code": 1, "name": "Headquarters - Main"}),
        MappingProxyType({"code": 8, "name": "Headquarters - Other"}),
        MappingProxyType({"code": 2, "name": "Special Offices"}),
        MappingProxyType({"code": 5, "name": "Research Labs"}),
        MappingProxyType({"code": 3, "name": "Regional Offices"}),
        MappingProxyType({"code": 4, "name": "Regional Satellite Offices"}),
        MappingProxyType({"code": -1, "name": "Free Representation"}),
    ),
    "splitPolicy": "esriSPTDefaultValue", "mergePolicy": "esriMPTDefaultValue",
})

_LABEL_FIELDS = (
    MappingProxyType({"alias": "OID", "domain": None, "name": "OBJECTID", "nullable": False, "type": "esriFieldTypeOID"}),
    MappingProxyType({"alias": "Shape", "domain": None, "name": "Shape", "nullable": True, "type": "esriFieldTypeGeometry"}),
    MappingProxyType({"alias": "Name condensed", "domain": None, "length": 250, "name": "SHORTNAME", "nullable": True, "type": "esriFieldTypeString"}),
    MappingProxyType({"alias": "RuleID_1", "domain": _RULE_DOMAIN, "name": "RuleID_1", "nullable": True, "type": "esriFieldTypeInteger"}),
)

_FACILITY_FIELDS = (
    MappingProxyType({"alias": "OID", "domain": None, "name": "OBJECTID", "nullable": False, "type": "esriFieldTypeOID"}),
    MappingProxyType({"alias": "Shape", "domain": None, "name": "Shape", "nullable": True, "type": "esriFieldTypeGeometry"}),
    MappingProxyType({"alias": "Name", "domain": None, "length": 255, "name": "Name", "nullable": True, "type": "esriFieldTypeString"}),
    MappingProxyType({"alias": "Name condensed", "domain": None, "length": 250, "name": "SHORTNAME", "nullable": True, "type": "esriFieldTypeString"}),
    MappingProxyType({"alias": "POINT_X", "domain": None, "name": "POINT_X", "nullable": True, "type": "esriFieldTypeDouble"}),
    MappingProxyType({"alias": "POINT_Y", "domain": None, "name": "POINT_Y", "nullable": True, "type": "esriFieldTypeDouble"}),
    MappingProxyType({"alias": "Address", "domain": None, "length": 250, "name": "Address", "nullable": True, "type": "esriFieldTypeString"}),
    MappingProxyType({"alias": "City", "domain": None, "length": 50, "name": "City", "nullable": True, "type": "esriFieldTypeString"}),
    MappingProxyType({"alias": "State", "domain": None, "length": 50, "name": "State", "nullable": True, "type": "esriFieldTypeString"}),
    MappingProxyType({"alias": "Zipcode", "domain": None, "length": 50, "name": "Zip", "nullable": True, "type": "esriFieldTypeString"}),
    MappingProxyType({"alias": "Phone number", "domain": None, "length": 50, "name": "Phone", "nullable": True, "type": "esriFieldTypeString"}),
    MappingProxyType({"alias": "Web link", "domain": None, "length": 50, "name": "Website", "nullable": True, "type": "esriFieldTypeString"}),
    MappingProxyType({"alias": "RuleID_1", "domain": _RULE_DOMAIN, "name": "RuleID_1", "nullable": True, "type": "esriFieldTypeInteger"}),
)

_SOURCE_SPATIAL_REFERENCE = MappingProxyType({
    "falseM": -100000, "falseX": -400, "falseY": -400, "falseZ": -100000,
    "latestWkid": 4269, "mTolerance": 0.001, "mUnits": 10000,
    "wkid": 4269, "xyTolerance": 8.983152841195213e-09,
    "xyUnits": 999999999.9999999, "zTolerance": 0.001, "zUnits": 10000,
})
_DISPLAY_SPATIAL_REFERENCE = MappingProxyType({
    "falseM": -100000, "falseX": -22041257.77387803, "falseY": -30241100,
    "falseZ": -100000, "latestWkid": 3857, "mTolerance": 0.001,
    "mUnits": 10000, "wkid": 102100,
    "xyTolerance": 0.001, "xyUnits": 144148035.89861274,
    "zTolerance": 0.001, "zUnits": 10000,
})

EPA_FACILITIES_PROFILE = EpaFacilitiesProfile(
    service_url=SERVICE_URL,
    ordinary=LayerProfile(
        layer_id=1, name="EPA Facilities", slug="epa-facilities",
        geometry_type="esriGeometryPoint", source_crs=SOURCE_CRS,
        source_spatial_reference=_SOURCE_SPATIAL_REFERENCE,
        display_spatial_reference=_DISPLAY_SPATIAL_REFERENCE,
        field_definitions=_FACILITY_FIELDS, count=77,
        object_ids=tuple(range(1, 78)),
        source_record_sha256="6fc732c8e5c8afd84d6d85984e0e23c703ff1b51363a9285bcb9af9434b66bf8",
        schema_verified=True, population_verified=True,
    ),
    labels=LayerProfile(
        layer_id=0, name="EPA Facilities (Labels)", slug="epa-facilities-labels",
        geometry_type="esriGeometryPoint", source_crs=SOURCE_CRS,
        source_spatial_reference=_SOURCE_SPATIAL_REFERENCE,
        display_spatial_reference=_DISPLAY_SPATIAL_REFERENCE,
        field_definitions=_LABEL_FIELDS, count=77, object_ids=tuple(range(1, 78)),
        page_size=20,
        source_record_sha256="074a1648b1913e6d09fd2bd4322084d3a81e01bad87265884c628962addc037e",
        schema_verified=True, population_verified=True,
    ),
    service_title="EPA Facilities, US, 2012, US EPA, SEGS",
    publisher="U.S. Environmental Protection Agency, Office of Environmental Information",
    license_url="https://edg.epa.gov/EPA_Data_License.html",
    release_date=None,
    release_date_note=(
        "The 2012 service title and portal modification date do not establish a current source release date."
    ),
)


def layer_url(layer: LayerProfile, suffix: str = "") -> str:
    """Return a deterministic layer URL for metadata or query endpoints."""
    return f"{SERVICE_URL}/{layer.layer_id}{suffix}"


def query_url(layer: LayerProfile, **parameters: object) -> str:
    """Build an ArcGIS query URL while keeping caller parameter order stable."""
    return layer_url(layer, "/query") + "?" + urlencode(parameters)


def _canonical(value: object) -> bytes:
    def plain(item):
        if isinstance(item, Mapping):
            return {key: plain(value) for key, value in item.items()}
        if isinstance(item, tuple):
            return [plain(value) for value in item]
        if isinstance(item, list):
            return [plain(value) for value in item]
        return item
    return json.dumps(plain(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _validate_page_fields(page_fields: object, layer_fields: tuple[Mapping[str, object], ...]) -> None:
    if not isinstance(page_fields, list) or len(page_fields) != len([field for field in layer_fields if field["type"] != "esriFieldTypeGeometry"]):
        raise ValueError("page_field_schema_drift")
    expected = [field for field in layer_fields if field["type"] != "esriFieldTypeGeometry"]
    for actual, source in zip(page_fields, expected):
        if not isinstance(actual, dict):
            raise ValueError("page_field_schema_drift")
        for property_name in ("name", "type", "alias", "length"):
            if property_name in source and actual.get(property_name) != source[property_name]:
                raise ValueError("page_field_schema_drift:" + str(source["name"]))
        if "name" not in actual or "type" not in actual:
            raise ValueError("page_field_required_property_missing")
        for optional in ("nullable", "domain"):
            if optional in actual and actual[optional] != source.get(optional):
                raise ValueError("page_field_optional_property_drift:" + str(source["name"]))


def inspect_captures(captures: Mapping[str, tuple[bytes, Mapping[str, object]]], *, profile: LayerProfile = EPA_FACILITIES_PROFILE.ordinary) -> dict[str, object]:
    """Check a complete, bounded ArcGIS extraction against its frozen profile.

    Captures must contain decoded bodies paired with their HTTP receipts. The
    caller retains wire bodies independently through the common capture layer.
    """
    layer = profile
    if not layer.schema_verified or not layer.population_verified:
        raise ValueError("source schema and population pins are required")
    required = {"source_service", "layer_before", "layer_after", "count_before", "count_after", "ids_before", "ids_after"}
    if layer.page_size < 1:
        raise ValueError("invalid_profile_page_size")
    page_names = {f"page_{i:05d}" for i in range((layer.count + layer.page_size - 1) // layer.page_size)}
    if set(captures) != required | page_names:
        raise ValueError("incomplete_or_extra_capture_roster")
    expected_urls = _profile_urls(layer)

    def document(name: str) -> dict[str, object]:
        body, receipt = captures[name]
        size = receipt.get("bytes", receipt.get("decoded_bytes"))
        if receipt.get("complete_body") is not True or receipt.get("status") != 200 or size != len(body) or receipt.get("sha256") != hashlib.sha256(body).hexdigest():
            raise ValueError(f"invalid_original_receipt:{name}")
        if receipt.get("url") != expected_urls[name] or receipt.get("final_url") != expected_urls[name]:
            raise ValueError(f"capture_retrieval_url_drift:{name}")
        if receipt.get("independent_sink_readback_matches") is False or receipt.get("independent_decoded_readback_matches") is False or receipt.get("independent_wire_readback_matches") is False:
            raise ValueError(f"capture_independent_readback_failed:{name}")
        observed = receipt.get("observed_at", receipt.get("retrieved_at"))
        if not isinstance(observed, str) or datetime.fromisoformat(observed.replace("Z", "+00:00")).tzinfo is None:
            raise ValueError(f"capture_retrieval_timestamp_missing:{name}")
        value = json.loads(body)
        if not isinstance(value, dict) or "error" in value:
            raise ValueError(f"invalid_json_document:{name}")
        return value

    service = document("source_service")
    if not any(isinstance(item, dict) and item.get("id") == layer.layer_id and item.get("name") == layer.name for item in service.get("layers", [])):
        raise ValueError("exact_producer_layer_missing")
    before, after = document("layer_before"), document("layer_after")
    if _canonical(before) != _canonical(after):
        raise ValueError("source_metadata_changed_during_extraction")
    if before.get("id") != layer.layer_id or before.get("name") != layer.name or before.get("geometryType") != layer.geometry_type:
        raise ValueError("exact_layer_identity_drift")
    if _canonical(before.get("fields")) != _canonical([dict(field) for field in layer.field_definitions]):
        raise ValueError("source_physical_schema_or_domain_drift")
    if _canonical(before.get("sourceSpatialReference")) != _canonical(dict(layer.source_spatial_reference or {})):
        raise ValueError("native_source_crs_drift")
    if _canonical(before.get("extent", {}).get("spatialReference")) != _canonical(dict(layer.display_spatial_reference or {})):
        raise ValueError("display_crs_drift")
    count_values = [document(name).get("count") for name in ("count_before", "count_after")]
    if count_values != [layer.count, layer.count]:
        raise ValueError("complete_source_count_drift")
    ids_docs = [document(name) for name in ("ids_before", "ids_after")]
    for ids_doc in ids_docs:
        ids = ids_doc.get("objectIds")
        if ids_doc.get("objectIdFieldName") != "OBJECTID" or type(ids) is not list or any(type(i) is not int for i in ids) or sorted(ids) != list(layer.object_ids):
            raise ValueError("complete_original_ID_roster_drift")
    if ids_docs[0] != ids_docs[1]:
        raise ValueError("source_ID_census_changed")

    page_fields = None
    features = []
    for page_number in range(len(page_names)):
        page = document(f"page_{page_number:05d}")
        if page.get("geometryType") != layer.geometry_type or page.get("spatialReference") != {"wkid": layer.source_crs, "latestWkid": layer.source_crs}:
            raise ValueError("page_geometry_or_native_crs_drift")
        fields = page.get("fields")
        if not isinstance(fields, list):
            raise ValueError("page_field_definitions_missing")
        _validate_page_fields(fields, layer.field_definitions)
        if page_fields is not None and _canonical(fields) != _canonical(page_fields):
            raise ValueError("page_field_definitions_changed")
        page_fields = fields
        batch = page.get("features")
        expected_ids = layer.object_ids[page_number * layer.page_size:(page_number + 1) * layer.page_size]
        if not isinstance(batch, list) or len(batch) != len(expected_ids) or page.get("exceededTransferLimit") is True:
            raise ValueError("incomplete_or_limited_feature_page")
        if [f.get("attributes", {}).get("OBJECTID") for f in batch] != list(expected_ids):
            raise ValueError("page_ID_order_or_population_drift")
        features.extend(batch)

    for feature in features:
        attributes = feature.get("attributes")
        if not isinstance(attributes, dict) or set(attributes) != {field["name"] for field in layer.field_definitions if field["type"] != "esriFieldTypeGeometry"}:
            raise ValueError("source_attribute_roster_drift")
        for field in layer.field_definitions:
            if field["type"] == "esriFieldTypeGeometry":
                continue
            name, kind, value = field["name"], field["type"], attributes[field["name"]]
            if value is None:
                if field["nullable"] is not True:
                    raise ValueError(f"null_nonnullable_source_field:{name}")
            elif kind == "esriFieldTypeString":
                if type(value) is not str or len(value) > field["length"]:
                    raise ValueError(f"source_text_width_or_type_drift:{name}")
            elif kind == "esriFieldTypeOID":
                if type(value) is not int or not -(2**63) <= value < 2**63:
                    raise ValueError("source_OID_type_drift")
            elif kind == "esriFieldTypeInteger":
                if type(value) is not int or not -(2**31) <= value < 2**31:
                    raise ValueError(f"source_int32_type_drift:{name}")
                domain = field.get("domain")
                if isinstance(domain, Mapping) and domain.get("type") == "codedValue":
                    codes = {entry["code"] for entry in domain.get("codedValues", ())}
                    if value not in codes:
                        raise ValueError(f"source_coded_domain_drift:{name}")
            elif kind == "esriFieldTypeDouble":
                if type(value) not in (int, float) or not math.isfinite(value):
                    raise ValueError(f"source_binary64_type_drift:{name}")
            else:
                raise ValueError(f"unsupported_source_type:{name}")
        geometry = feature.get("geometry")
        if not isinstance(geometry, dict) or set(geometry) != set(layer.geometry_keys) or len(layer.geometry_keys) not in (2, 3):
            raise ValueError("source_native_point_geometry_drift")
        if any(type(geometry[k]) not in (int, float) or not math.isfinite(geometry[k]) for k in layer.geometry_keys):
            raise ValueError("source_coordinate_type_or_precision_drift")
        if ("POINT_X" in attributes and "POINT_Y" in attributes
                and (geometry["x"] != attributes["POINT_X"] or geometry["y"] != attributes["POINT_Y"])):
            raise ValueError("native_shape_and_coordinate_attribute_mismatch")
    record_hash = hashlib.sha256(_canonical(features)).hexdigest()
    if record_hash != layer.source_record_sha256:
        raise ValueError("source_record_or_native_geometry_drift_requires_review")
    return {
        "approved": False,
        "feature_count": len(features),
        "original_OBJECTIDs": list(layer.object_ids),
        "source_record_sha256": record_hash,
        "null_geometry_count": 0,
        "native_source_CRS": layer.source_crs,
        "display_CRS": DISPLAY_CRS,
        "field_count_including_geometry": len(layer.field_definitions),
        "field_names_including_geometry": [field["name"] for field in layer.field_definitions],
        "physical_type_mapping": {"RuleID_1": "int32, source esriFieldTypeInteger", "OBJECTID": "int64 candidate; producer OID bit width unspecified"},
        "geometry": "Original source x/y/z retained as Shape WKB; producer-native NAD83 coordinates, no reprojection or repair.",
        "producer_lineage": "EPA OEI describes the layer as the EPA facilities locations layer; no underlying facility registry lineage or current edition was established.",
        "rights_accepted": False,
        "source_release_accepted": False,
        "release_date_established": False,
        "mapping_approved": False,
    }


def decode_captures(captures: Mapping[str, tuple[bytes, Mapping[str, object]]], *, profile: LayerProfile = EPA_FACILITIES_PROFILE.ordinary) -> tuple[pa.Table, dict[str, object]]:
    """Decode all original typed attributes and source Shape coordinates."""
    audit = inspect_captures(captures, profile=profile)
    features = []
    for page_number in range((profile.count + profile.page_size - 1) // profile.page_size):
        features.extend(json.loads(captures[f"page_{page_number:05d}"][0]).get("features", []))
    definitions = [dict(field) for field in profile.field_definitions]
    physical_types = {
        "esriFieldTypeOID": pa.int64(),
        "esriFieldTypeInteger": pa.int32(),
        "esriFieldTypeDouble": pa.float64(),
        "esriFieldTypeString": pa.string(),
        "esriFieldTypeGeometry": pa.binary(),
    }
    rows = []
    for feature in features:
        row = dict(feature["attributes"])
        source_geometry = feature["geometry"]
        row["Shape"] = shapely.to_wkb(Point(*(source_geometry[key] for key in profile.geometry_keys)))
        rows.append(row)
    rows.sort(key=lambda row: row["OBJECTID"])
    schema = pa.schema([
        pa.field(
            field["name"], physical_types[field["type"]], nullable=field["nullable"],
            metadata={b"original_esri_field": _canonical(field)},
        ) for field in definitions
    ])
    table = pa.Table.from_pylist(rows, schema=schema)
    geometry_kinds = sorted({shapely.from_wkb(value).geom_type for value in table["Shape"].to_pylist() if value is not None})
    geo = {"version": "1.1.0", "primary_column": "Shape", "columns": {"Shape": {
        "encoding": "WKB", "geometry_types": geometry_kinds,
        "crs": pyproj.CRS.from_epsg(profile.source_crs).to_json_dict(),
    }}}
    table = table.replace_schema_metadata({b"geo": _canonical(geo)})
    return table, audit


def validate_native_table(table: pa.Table, *, profile: LayerProfile = EPA_FACILITIES_PROFILE.ordinary) -> None:
    """Read the written GeoParquet independently and retain exact schema/values."""
    if table.num_rows != profile.count or table.column_names != [field["name"] for field in profile.field_definitions]:
        raise ValueError("native_candidate_population_or_field_roster_drift")
    if table.schema.field("OBJECTID").type != pa.int64() or table.schema.field("RuleID_1").type != pa.int32():
        raise ValueError("native_integer_width_drift")
    if table.schema.field("Shape").type != pa.binary() or table["Shape"].null_count:
        raise ValueError("native_geometry_field_or_nullability_drift")


def _digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _write_capture_file(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError("capture_alias_or_special_file_refused")
        os.ftruncate(fd, 0)
        if os.write(fd, data) != len(data):
            raise OSError("short_capture_write")
        os.fsync(fd)
    finally:
        os.close(fd)
    if path.read_bytes() != data:
        raise ValueError("capture_readback_mismatch")


def _read_capture_file(path: Path, limit: int = 8_000_000) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > limit:
            raise ValueError("unsafe_or_oversized_capture_file")
        chunks = []
        total = 0
        while True:
            block = os.read(fd, min(1024 * 1024, limit + 1 - total))
            if not block:
                break
            chunks.append(block)
            total += len(block)
            if total > limit:
                raise ValueError("oversized_capture_file")
        after = os.fstat(fd)
    finally:
        os.close(fd)
    identity = lambda item: (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns, item.st_ctime_ns, item.st_nlink)
    data = b"".join(chunks)
    if len(data) != before.st_size or identity(before) != identity(after):
        raise ValueError("capture_file_changed_or_truncated")
    return data


def _capture_one(client: httpx.Client, url: str, directory: Path, name: str) -> tuple[bytes, dict[str, object]]:
    wire_path, decoded_path = directory / (name + ".wire"), directory / (name + ".body")
    wire_path.touch(mode=0o600, exist_ok=False)
    decoded_path.touch(mode=0o600, exist_ok=False)
    for path in (wire_path, decoded_path):
        if not stat.S_ISREG(path.lstat().st_mode) or path.lstat().st_nlink != 1:
            raise ValueError("capture_alias_or_special_file_refused")
    observed = datetime.now(timezone.utc).isoformat()

    def sink(path: Path):
        def retain(block: bytes) -> None:
            fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                if os.write(fd, block) != len(block):
                    raise OSError("short_capture_block_write")
                os.fsync(fd)
            finally:
                os.close(fd)
        return retain

    with client.stream("GET", url) as response:
        transfer = capture_http_response(
            response, max_wire_bytes=8_000_000, max_decoded_bytes=8_000_000,
            reserve=lambda progress: None, retain_wire=sink(wire_path), retain_decoded=sink(decoded_path),
        )
        receipt = {
            "url": url, "final_url": str(response.url), "status": response.status_code,
            "observed_at": observed, "complete_body": transfer.complete,
            "bytes": transfer.decoded_bytes_retained, "sha256": transfer.decoded_sha256,
            "wire_bytes": transfer.wire_bytes_retained, "wire_sha256": transfer.wire_sha256,
            "independent_sink_readback_matches": (
                wire_path.stat().st_size == transfer.wire_bytes_retained
                and _digest(wire_path.read_bytes()) == transfer.wire_sha256
                and decoded_path.stat().st_size == transfer.decoded_bytes_retained
                and _digest(decoded_path.read_bytes()) == transfer.decoded_sha256
            ),
        }
    raw = _canonical(receipt) + b"\n"
    _write_capture_file(directory / (name + ".receipt.json"), raw)
    return decoded_path.read_bytes(), receipt


def _profile_urls(profile: LayerProfile) -> dict[str, str]:
    root = layer_url(profile)
    base_query = {"where": "1=1"}
    ids = profile.object_ids
    names = {
        "source_service": SERVICE_URL + "?f=pjson",
        "layer_before": root + "?f=pjson",
        "count_before": root + "/query?" + urlencode({**base_query, "returnCountOnly": "true", "f": "json"}),
        "ids_before": root + "/query?" + urlencode({**base_query, "returnIdsOnly": "true", "f": "json"}),
        "layer_after": root + "?f=pjson",
        "count_after": root + "/query?" + urlencode({**base_query, "returnCountOnly": "true", "f": "json"}),
        "ids_after": root + "/query?" + urlencode({**base_query, "returnIdsOnly": "true", "f": "json"}),
    }
    for offset in range(0, len(ids), profile.page_size):
        page_ids = ids[offset:offset + profile.page_size]
        names[f"page_{offset // profile.page_size:05d}"] = root + "/query?" + urlencode({
            "objectIds": ",".join(map(str, page_ids)), "outFields": "*", "returnGeometry": "true",
            "returnZ": "true", "returnM": "true", "outSR": profile.source_crs,
            "orderByFields": "OBJECTID ASC", "f": "json",
        })
    return names


def _load_or_capture(profile: LayerProfile, storage: StagingStorageResource, capture_dir: str | None) -> tuple[dict[str, tuple[bytes, dict]], Path]:
    if capture_dir:
        root = Path(capture_dir).absolute()
    else:
        root = Path(storage.local_dir).absolute() / "_epa_facilities_captures" / profile.slug / uuid4().hex
    if ".." in root.parts or any(parent.is_symlink() for parent in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True, exist_ok=True)
    urls = _profile_urls(profile)
    captures = {}
    if capture_dir:
        for name, url in urls.items():
            body_path, receipt_path = root / (name + ".body"), root / (name + ".receipt.json")
            body = _read_capture_file(body_path)
            receipt = json.loads(_read_capture_file(receipt_path, 65536))
            wire_path = root / (name + ".wire")
            if not wire_path.exists():
                wire_path = root / name / "wire.body"
            wire = _read_capture_file(wire_path)
            if receipt.get("url") != url or receipt.get("final_url") != url:
                raise ValueError("capture_url_mismatch:" + name)
            if (receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _digest(wire)
                    or receipt.get("bytes", receipt.get("decoded_bytes")) != len(body)
                    or receipt.get("sha256") != _digest(body)):
                raise ValueError("capture_physical_readback_mismatch:" + name)
            captures[name] = (body, receipt)
    else:
        with httpx.Client(follow_redirects=False, timeout=60, headers={"Accept-Encoding": "identity"}) as client:
            for name, url in urls.items():
                captures[name] = _capture_one(client, url, root, name)
    return captures, root


def _processing_identity() -> dict[str, object]:
    project = Path(__file__).parents[3]
    files = {str(path.relative_to(project)): sha256_file(path) for path in sorted(project.joinpath("src/dagster_hifld").rglob("*.py"))}
    files.update({"pyproject.toml": sha256_file(project / "pyproject.toml"), "uv.lock": sha256_file(project / "uv.lock")})
    return {
        "code_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=project, text=True).strip(),
        "files": files, "python": platform.python_version(), "platform": platform.platform(),
        "distributions": [
            list(item)
            for item in sorted(
                (dist.metadata["Name"], dist.version)
                for dist in importlib.metadata.distributions()
            )
        ],
        "pyarrow": pa.__version__, "gdal": __import__("pyogrio").__gdal_version_string__,
        "geos": shapely.geos_version_string, "proj": pyproj.proj_version_str,
    }


def _immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    snapshot = storage.object_snapshot(key)
    if snapshot is None:
        storage.write_key_if_unchanged(key, body, None)
    elif storage.read_key(key) != body or storage.object_snapshot(key) != snapshot:
        raise ValueError("immutable_object_changed:" + key)


def _source_manifest(profile: LayerProfile, captures: Mapping[str, tuple[bytes, Mapping[str, object]]], identity: Mapping[str, object], parquet_body: bytes) -> dict[str, object]:
    objects = [{"path": f"geoparquet/{profile.slug}.parquet", "bytes": len(parquet_body), "sha256": _digest(parquet_body)}]
    inputs = []
    for name, (_body, receipt) in sorted(captures.items()):
        inputs.append({
            "input_id": f"epa-facilities-{profile.layer_id}-{name}",
            "producer": EPA_FACILITIES_PROFILE.publisher,
            "host": "geodata.epa.gov", "role": "producer_publication",
            "canonical_url": layer_url(profile), "retrieval_url": receipt.get("url"),
            "observed_final_url": receipt.get("final_url"), "retrieved_at": receipt.get("observed_at"),
            "http_status": receipt.get("status"), "bytes": receipt.get("bytes", receipt.get("decoded_bytes")),
            "sha256": receipt.get("sha256"), "license": EPA_FACILITIES_PROFILE.license_url,
            "attribution": "U.S. EPA Office of Environmental Information (OEI); rights acceptance remains pending.",
            "release_date": None,
            "release_date_unknown_reason": EPA_FACILITIES_PROFILE.release_date_note,
        })
    manifest = {
        "schema_version": 1, "approved": False,
        "title": profile.name, "description": "EPA OEI producer-hosted Facilities MapServer layer.",
        "publisher": EPA_FACILITIES_PROFILE.publisher, "agency": "Environmental Protection Agency",
        "source_url": layer_url(profile), "license": EPA_FACILITIES_PROFILE.license_url,
        "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"], "inputs": inputs,
        "transformations": [{"operation": "complete_native_source_capture", "parameters": {
            "layer": f"EPA_Locations/MapServer/{profile.layer_id}", "IDs": "Preserve original OBJECTID values and all source fields; no replacements.",
            "geometry": f"Preserve producer-native x/y/z in Shape WKB at EPSG:{profile.source_crs}; do not reproject or repair.",
            "OID_storage": "Source OID physical width unspecified; int64 candidate only.",
            "release": "Source release date, rights, schema/ID contract, mapping, and release acceptance remain pending.",
        }}],
        "lineage": "EPA OEI identifies this as the EPA Facilities point layer; the service title is historical and does not establish a current source edition.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Native source field order/types/nullability and NAD83 point geometry are pinned; publication mapping remains unapproved.",
            "objects": objects,
        }}},
        "unsupported_output_formats": {
            "geopackage": "Physical OID width and mapping contract remain unapproved.",
            "shapefile_zip": "Source field names/widths and null values cannot be represented losslessly without approval.",
            "pmtiles": "Projection, quantization and cartographic identifiers are not approved.",
        },
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    return manifest


def _cached_reuse(storage: StagingStorageResource, profile: LayerProfile, version: str, fingerprint: str,
                  prefix: str, identity: Mapping[str, object], current_table: pa.Table,
                  current_audit: Mapping[str, object], current_native_body: bytes) -> dict[str, object]:
    """Validate a deterministic prior candidate against its immutable first build."""
    state_key = f"_construction-state/{profile.slug}/{version}.json"
    sealed = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = sealed.get("payload")
    expected_keys = {"processing_identity", "manifest_sha256", "audit_sha256", "capture_receipts", "objects", "source_record_sha256", "schema_sha256"}
    if not isinstance(payload, dict) or set(payload) != expected_keys:
        raise ValueError("cached_first_construction_payload_invalid")
    if payload["processing_identity"] != identity or payload["source_record_sha256"] != current_audit["source_record_sha256"]:
        raise ValueError("cached_original_source_or_code_identity_drift")
    schema_sha = _digest(_canonical([dict(field) for field in profile.field_definitions]))
    if payload["schema_sha256"] != schema_sha:
        raise ValueError("cached_source_schema_pin_drift")
    descriptors = payload["objects"]
    if not isinstance(descriptors, dict):
        raise ValueError("cached_candidate_object_roster_invalid")
    ready_key = f"{prefix}/metadata/candidate_ready.json"
    expected_roster = {f"{prefix}/{name}" for name in descriptors} | {ready_key}
    if set(storage.list_keys(profile.slug, profile.slug, version)) != expected_roster:
        raise ValueError("cached_candidate_object_roster_changed")
    for relative, expected in descriptors.items():
        key = f"{prefix}/{relative}"
        snapshot = storage.object_snapshot(key)
        if (not isinstance(expected, dict) or snapshot is None or snapshot.size != expected.get("bytes")
                or storage.sha256_key(key) != expected.get("sha256") or storage.object_snapshot(key) != snapshot):
            raise ValueError("cached_candidate_object_changed:" + str(relative))
    ready = json.loads(storage.read_key(ready_key))
    if ready.get("approved") is not False or ready.get("version") != version or ready.get("objects") != descriptors:
        raise ValueError("cached_candidate_ready_marker_changed")
    if payload["objects"].get(f"geoparquet/{profile.slug}.parquet") != {"bytes": len(current_native_body), "sha256": _digest(current_native_body)}:
        raise ValueError("cached_native_candidate_source_drift")
    if pq.read_table(pa.BufferReader(storage.read_key(f"{prefix}/geoparquet/{profile.slug}.parquet"))).num_rows != current_table.num_rows:
        raise ValueError("cached_native_candidate_population_changed")
    raw_manifest = storage.read_key(f"{prefix}/metadata/source_manifest.json")
    raw_audit = storage.read_key(f"{prefix}/metadata/source_audit.json")
    raw_identity = storage.read_key(f"{prefix}/metadata/processing_identity.json")
    if (_digest(raw_manifest) != payload["manifest_sha256"] or _digest(raw_audit) != payload["audit_sha256"]
            or json.loads(raw_identity) != identity or json.loads(raw_audit).get("source_record_sha256") != current_audit["source_record_sha256"]):
        raise ValueError("cached_source_metadata_or_audit_changed")
    manifest = json.loads(raw_manifest)
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    receipts = payload["capture_receipts"]
    if not isinstance(receipts, dict):
        raise ValueError("cached_original_capture_receipts_missing")
    original_captures = {}
    for name, receipt in receipts.items():
        body = storage.read_key(f"{prefix}/raw/{name}.body")
        wire = storage.read_key(f"{prefix}/raw/{name}.wire")
        receipt_bytes = storage.read_key(f"{prefix}/raw/{name}.receipt.json")
        if _digest(receipt_bytes) != _digest(_canonical(receipt) + b"\n"):
            raise ValueError("cached_original_receipt_changed:" + name)
        if (receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _digest(wire)
                or receipt.get("bytes", receipt.get("decoded_bytes")) != len(body)
                or receipt.get("sha256") != _digest(body)):
            raise ValueError("cached_original_raw_capture_changed:" + name)
        original_captures[name] = (body, receipt)
    original_audit = inspect_captures(original_captures, profile=profile)
    if original_audit["source_record_sha256"] != current_audit["source_record_sha256"]:
        raise ValueError("cached_original_population_drift")
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {
        "dataset_slug": profile.slug, "file_slug": profile.slug, "version": version,
        "feature_count": current_table.num_rows, "source_null_geometry_count": current_audit["null_geometry_count"],
        "source_invalid_geometry_count": 0, "reused": True, "candidate_ready": ready_key,
        "source_release_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
    }


def stage_epa_facilities(
    storage,
    *,
    profile: LayerProfile = EPA_FACILITIES_PROFILE.ordinary,
    capture_dir: str | None = None,
):
    """Capture, verify and stage one EPA Facilities service layer as draft GeoParquet."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource):
        raise ValueError("explicit_local_staging_resource_required")
    if storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if profile is not EPA_FACILITIES_PROFILE.ordinary and profile is not EPA_FACILITIES_PROFILE.labels:
        raise ValueError("unrecognized_EPA_Facilities_profile")
    if profile.layer_id not in (0, 1) or profile.source_crs != SOURCE_CRS:
        raise ValueError("EPA_Facilities_profile_must_use_a_known_native_layer")
    if not profile.schema_verified or not profile.population_verified or profile.count is None or profile.source_record_sha256 is None:
        raise ValueError("complete_source_profile_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_path")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")

    captures: dict[str, tuple[bytes, dict]] = {}
    capture_root = None
    try:
        captures, capture_root = _load_or_capture(profile, storage, capture_dir)
        table, audit = decode_captures(captures, profile=profile)
    except Exception as exc:
        if capture_root is not None and captures:
            quarantine_key = f"_quarantine/{profile.slug}/{uuid4().hex}"
            for name, (body, receipt) in captures.items():
                _immutable(storage, quarantine_key + "/raw/" + name + ".body", body)
                wire_path = capture_root / (name + ".wire")
                if wire_path.exists():
                    _immutable(storage, quarantine_key + "/raw/" + name + ".wire", wire_path.read_bytes())
                _immutable(storage, quarantine_key + "/raw/" + name + ".receipt.json", _canonical(receipt) + b"\n")
            _immutable(storage, quarantine_key + "/reason.json", _canonical({"approved": False, "reason_type": type(exc).__name__, "reason": str(exc)}) + b"\n")
        raise

    identity = _processing_identity()
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    source_fingerprint = _digest(_canonical({"processing": identity, "source_record_sha256": audit["source_record_sha256"], "profile": profile.slug}))
    version = f"v0.0.0-draft-epa-facilities-{source_fingerprint[:16]}"
    prefix = f"{profile.slug}/{profile.slug}/{version}"

    with tempfile.TemporaryDirectory(prefix="epa-facilities-candidate-", dir=base) as temporary:
        candidate = Path(temporary)
        native = candidate / "geoparquet" / f"{profile.slug}.parquet"
        native.parent.mkdir(parents=True)
        pq.write_table(table, native, compression="zstd", version="2.6", write_statistics=True)
        readback = pq.read_table(native)
        if not readback.equals(table, check_metadata=True):
            raise ValueError("native_geoparquet_roundtrip_mismatch")
        validate_native_table(readback, profile=profile)
        native_body = native.read_bytes()
        state_key = f"_construction-state/{profile.slug}/{version}.json"
        existing_version_keys = storage.list_keys(profile.slug, profile.slug, version)
        if storage.object_snapshot(state_key) is not None:
            return _cached_reuse(storage, profile, version, source_fingerprint, prefix, identity, table, audit, native_body)
        if existing_version_keys:
            raise ValueError("partial_existing_candidate_requires_review")
        manifest = _source_manifest(profile, captures, identity, native_body)
        audit["source_null_geometry_count"] = int(readback["Shape"].null_count)
        audit["source_invalid_geometry_count"] = 0
        audit["source_null_geometry_OBJECTIDs"] = []
        audit["source_invalid_geometry_OBJECTIDs"] = []
        audit["approval_flags"] = {"source": False, "schema_contract": False, "ID_contract": False, "rights": False, "mapping": False, "release": False}
        payloads = {
            f"geoparquet/{profile.slug}.parquet": native_body,
            "metadata/source_manifest.json": _canonical(manifest) + b"\n",
            "metadata/source_audit.json": _canonical(audit) + b"\n",
            "metadata/processing_identity.json": _canonical(identity) + b"\n",
        }
        for name, (body, receipt) in captures.items():
            payloads[f"raw/{name}.body"] = body
            wire_path = capture_root / (name + ".wire")
            if not wire_path.exists():
                wire_path = capture_root / name / "wire.body"
            if not wire_path.exists():
                raise ValueError("retained_wire_capture_missing:" + name)
            payloads[f"raw/{name}.wire"] = wire_path.read_bytes()
            payloads[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
        object_descriptors = {name: {"bytes": len(body), "sha256": _digest(body)} for name, body in payloads.items()}
        ready = {
            "approved": False, "version": version, "source_release_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False,
            "feature_count": profile.count, "quality_passed": True,
            "source_null_geometry_count": 0, "source_invalid_geometry_count": 0,
            "processing_fingerprint": source_fingerprint, "objects": object_descriptors,
        }
        candidate_ready_path = candidate / "metadata/candidate_ready.json"
        candidate_ready_path.parent.mkdir(parents=True, exist_ok=True)
        _write_capture_file(candidate_ready_path, _canonical(ready) + b"\n")
        # Complete local artifacts and code identity are verified before the first-construction seal.
        for name, body in payloads.items():
            path = candidate / name
            path.parent.mkdir(parents=True, exist_ok=True)
            _write_capture_file(path, body)
            if not path.is_file() or path.read_bytes() != body:
                raise ValueError("private_candidate_readback_mismatch:" + name)
        if pq.read_table(native).num_rows != profile.count or _digest(native.read_bytes()) != object_descriptors[f"geoparquet/{profile.slug}.parquet"]["sha256"]:
            raise ValueError("private_native_candidate_identity_mismatch")
        bundle = canonical_construction_bundle(version, source_fingerprint, {
            "processing_identity": identity, "manifest_sha256": _digest(payloads["metadata/source_manifest.json"]),
            "audit_sha256": _digest(payloads["metadata/source_audit.json"]),
            "capture_receipts": {name: receipt for name, (_body, receipt) in captures.items()},
            "objects": object_descriptors,
            "source_record_sha256": audit["source_record_sha256"],
            "schema_sha256": _digest(_canonical([dict(field) for field in profile.field_definitions])),
        })
        sealed = seal_first_construction(storage, state_key, version, source_fingerprint, bundle)
        if sealed != bundle:
            raise ValueError("first_construction_seal_mismatch")
        for name, body in payloads.items():
            _immutable(storage, f"{prefix}/{name}", body)
        ready_key = f"{prefix}/metadata/candidate_ready.json"
        _immutable(storage, ready_key, _canonical(ready) + b"\n")
        expected_roster = {f"{prefix}/{name}" for name in payloads} | {ready_key}
        actual_roster = set(storage.list_keys(profile.slug, profile.slug, version))
        if actual_roster != expected_roster:
            raise ValueError("staged_candidate_object_roster_mismatch")
        for relative, descriptor in object_descriptors.items():
            key = f"{prefix}/{relative}"
            snapshot = storage.object_snapshot(key)
            if snapshot is None or snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] or storage.object_snapshot(key) != snapshot:
                raise ValueError("staged_candidate_object_readback_mismatch:" + relative)
    return {
        "dataset_slug": profile.slug, "file_slug": profile.slug, "version": version,
        "feature_count": profile.count, "source_null_geometry_count": 0,
        "source_invalid_geometry_count": 0, "reused": bool(existing_version_keys),
        "candidate_ready": ready_key, "source_release_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False,
    }

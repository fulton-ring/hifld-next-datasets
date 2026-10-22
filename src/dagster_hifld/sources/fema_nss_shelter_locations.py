"""Native, unapproved capture of FEMA layer 5 Shelter Locations; not a historic HIFLD reconstruction."""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import stat
from pathlib import Path
from contextlib import nullcontext
from urllib.parse import urlencode
from urllib.parse import urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import Point

from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.http_capture import validate_capture_roster

ITEM_ID = "9e731f5353f94ffba848d87d98fc064c"
PORTAL_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"
SERVICE_URL = "https://gis.fema.gov/arcgis/rest/services/NSS/FEMA_NSS/MapServer"
PORTAL_LAYER_URL = "https://gis.fema.gov/arcgis/rest/services/NSS/FEMA_NSS/FeatureServer/5"
LAYER_ID = 5
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Shelter Locations"
SERVICE_DESCRIPTION = "FEMA ESF#6 Shelter System (Formerly known as the FEMA National Shelter System)"
LAYER_DESCRIPTION = "This map layer turns on all shelter status types at the same time."
DATASET_SLUG = "national-shelter-system-facilities"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "Federal Emergency Management Agency (FEMA_MAC ArcGIS service)"
SCHEMA_SHA256 = "d2112bde2156c1a51a892365718c5bda6ff3f6e3ce0380259caece3f41df9684"
FIELD_DECLARATIONS = [{'name': 'shelter_id', 'type': 'esriFieldTypeInteger', 'alias': 'SHELTER_ID', 'length': None},
 {'name': 'shelter_name', 'type': 'esriFieldTypeString', 'alias': 'SHELTER_NAME', 'length': 255},
 {'name': 'address_1', 'type': 'esriFieldTypeString', 'alias': 'ADDRESS', 'length': 255},
 {'name': 'city', 'type': 'esriFieldTypeString', 'alias': 'CITY', 'length': 255},
 {'name': 'county_parish', 'type': 'esriFieldTypeString', 'alias': 'COUNTY_PARISH', 'length': 50},
 {'name': 'fips_code', 'type': 'esriFieldTypeString', 'alias': 'FIPS_CODE', 'length': 5},
 {'name': 'state', 'type': 'esriFieldTypeString', 'alias': 'STATE', 'length': 2},
 {'name': 'zip', 'type': 'esriFieldTypeString', 'alias': 'ZIP', 'length': 10},
 {'name': 'mail_addr_same_as_phys_addr',
  'type': 'esriFieldTypeString',
  'alias': 'MAIL_ADDR_SAME_AS_PHYS_ADDR',
  'length': 3},
 {'name': 'mailing_address_1', 'type': 'esriFieldTypeString', 'alias': 'MAILING_ADDRESS_1', 'length': 100},
 {'name': 'mailing_address_2', 'type': 'esriFieldTypeString', 'alias': 'MAILING_ADDRESS_2', 'length': 100},
 {'name': 'mailing_city', 'type': 'esriFieldTypeString', 'alias': 'MAILING_CITY', 'length': 28},
 {'name': 'mailing_county_parish',
  'type': 'esriFieldTypeString',
  'alias': 'MAILING_COUNTY_PARISH',
  'length': 50},
 {'name': 'mailing_state', 'type': 'esriFieldTypeString', 'alias': 'MAILING_STATE', 'length': 2},
 {'name': 'mailing_zip', 'type': 'esriFieldTypeString', 'alias': 'MAILING_ZIP', 'length': 5},
 {'name': 'facility_usage_code', 'type': 'esriFieldTypeString', 'alias': 'FACILITY_USAGE_CODE', 'length': 10},
 {'name': 'evacuation_capacity',
  'type': 'esriFieldTypeInteger',
  'alias': 'EVACUATION_CAPACITY',
  'length': None},
 {'name': 'post_impact_capacity',
  'type': 'esriFieldTypeInteger',
  'alias': 'POST_IMPACT_CAPACITY',
  'length': None},
 {'name': 'ada_compliant', 'type': 'esriFieldTypeString', 'alias': 'ADA_COMPLIANT', 'length': 3},
 {'name': 'wheelchair_accessible',
  'type': 'esriFieldTypeString',
  'alias': 'WHEELCHAIR_ACCESSIBLE',
  'length': 3},
 {'name': 'pet_accommodations_code',
  'type': 'esriFieldTypeString',
  'alias': 'PET_ACCOMMODATIONS_CODE',
  'length': 10},
 {'name': 'pet_accommodations_desc',
  'type': 'esriFieldTypeString',
  'alias': 'PET_ACCOMMODATIONS_DESC',
  'length': 200},
 {'name': 'generator_onsite', 'type': 'esriFieldTypeString', 'alias': 'GENERATOR_ONSITE', 'length': 3},
 {'name': 'self_sufficient_electricity',
  'type': 'esriFieldTypeString',
  'alias': 'SELF_SUFFICIENT_ELECTRICITY',
  'length': 3},
 {'name': 'latitude', 'type': 'esriFieldTypeDouble', 'alias': 'LATITUDE', 'length': None},
 {'name': 'longitude', 'type': 'esriFieldTypeDouble', 'alias': 'LONGITUDE', 'length': None},
 {'name': 'in_100_yr_floodplain',
  'type': 'esriFieldTypeString',
  'alias': 'IN_100_YR_FLOODPLAIN',
  'length': 3},
 {'name': 'in_500_yr_floodplain',
  'type': 'esriFieldTypeString',
  'alias': 'IN_500_YR_FLOODPLAIN',
  'length': 3},
 {'name': 'in_surge_slosh_area', 'type': 'esriFieldTypeString', 'alias': 'IN_SURGE_SLOSH_AREA', 'length': 3},
 {'name': 'pre_landfall_shelter',
  'type': 'esriFieldTypeString',
  'alias': 'PRE_LANDFALL_SHELTER',
  'length': 3},
 {'name': 'shelter_code', 'type': 'esriFieldTypeString', 'alias': 'SHELTER_CODE', 'length': 30},
 {'name': 'org_organization_id',
  'type': 'esriFieldTypeInteger',
  'alias': 'ORG_ORGANIZATION_ID',
  'length': None},
 {'name': 'org_organization_name',
  'type': 'esriFieldTypeString',
  'alias': 'ORG_ORGANIZATION_NAME',
  'length': 75},
 {'name': 'org_main_phone', 'type': 'esriFieldTypeString', 'alias': 'ORG_MAIN_PHONE', 'length': 10},
 {'name': 'org_fax', 'type': 'esriFieldTypeString', 'alias': 'ORG_FAX', 'length': 10},
 {'name': 'org_email', 'type': 'esriFieldTypeString', 'alias': 'ORG_EMAIL', 'length': 75},
 {'name': 'org_hotline_phone', 'type': 'esriFieldTypeString', 'alias': 'ORG_HOTLINE_PHONE', 'length': 10},
 {'name': 'org_other_phone', 'type': 'esriFieldTypeString', 'alias': 'ORG_OTHER_PHONE', 'length': 10},
 {'name': 'org_address', 'type': 'esriFieldTypeString', 'alias': 'ORG_ADDRESS', 'length': 38},
 {'name': 'org_city', 'type': 'esriFieldTypeString', 'alias': 'ORG_CITY', 'length': 28},
 {'name': 'org_state', 'type': 'esriFieldTypeString', 'alias': 'ORG_STATE', 'length': 2},
 {'name': 'org_zip', 'type': 'esriFieldTypeString', 'alias': 'ORG_ZIP', 'length': 5},
 {'name': 'org_poc_name', 'type': 'esriFieldTypeString', 'alias': 'ORG_POC_NAME', 'length': 75},
 {'name': 'org_poc_phone', 'type': 'esriFieldTypeString', 'alias': 'ORG_POC_PHONE', 'length': 10},
 {'name': 'org_poc_after_hours_phone',
  'type': 'esriFieldTypeString',
  'alias': 'ORG_POC_AFTER_HOURS_PHONE',
  'length': 10},
 {'name': 'org_poc_email', 'type': 'esriFieldTypeString', 'alias': 'ORG_POC_EMAIL', 'length': 75},
 {'name': 'org_hours_of_operation',
  'type': 'esriFieldTypeString',
  'alias': 'ORG_HOURS_OF_OPERATION',
  'length': 75},
 {'name': 'population_code', 'type': 'esriFieldTypeString', 'alias': 'POPULATION_CODE', 'length': 30},
 {'name': 'incident_id', 'type': 'esriFieldTypeInteger', 'alias': 'INCIDENT_ID', 'length': None},
 {'name': 'shelter_status_code', 'type': 'esriFieldTypeString', 'alias': 'SHELTER_STATUS_CODE', 'length': 10},
 {'name': 'shelter_open_date', 'type': 'esriFieldTypeDate', 'alias': 'SHELTER_OPEN_DATE', 'length': 8},
 {'name': 'shelter_closed_date', 'type': 'esriFieldTypeDate', 'alias': 'SHELTER_CLOSED_DATE', 'length': 8},
 {'name': 'reporting_period', 'type': 'esriFieldTypeString', 'alias': 'REPORTING_PERIOD', 'length': 10},
 {'name': 'general_population',
  'type': 'esriFieldTypeInteger',
  'alias': 'GENERAL_POPULATION',
  'length': None},
 {'name': 'medical_needs_population',
  'type': 'esriFieldTypeInteger',
  'alias': 'MEDICAL_NEEDS_POPULATION',
  'length': None},
 {'name': 'other_population', 'type': 'esriFieldTypeInteger', 'alias': 'OTHER_POPULATION', 'length': None},
 {'name': 'other_population_description',
  'type': 'esriFieldTypeString',
  'alias': 'OTHER_POPULATION_DESCRIPTION',
  'length': 200},
 {'name': 'total_population', 'type': 'esriFieldTypeInteger', 'alias': 'TOTAL_POPULATION', 'length': None},
 {'name': 'pet_population', 'type': 'esriFieldTypeInteger', 'alias': 'PET_POPULATION', 'length': None},
 {'name': 'incident_number', 'type': 'esriFieldTypeString', 'alias': 'INCIDENT_NUMBER', 'length': 255},
 {'name': 'incident_name', 'type': 'esriFieldTypeString', 'alias': 'INCIDENT_NAME', 'length': 250},
 {'name': 'incident_code', 'type': 'esriFieldTypeString', 'alias': 'INCIDENT_CODE', 'length': 10},
 {'name': 'objectid', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID', 'length': None},
 {'name': 'shape', 'type': 'esriFieldTypeGeometry', 'alias': 'Shape', 'length': None},
 {'name': 'score', 'type': 'esriFieldTypeSmallInteger', 'alias': 'SCORE', 'length': None},
 {'name': 'status', 'type': 'esriFieldTypeString', 'alias': 'STATUS', 'length': 5},
 {'name': 'match_type', 'type': 'esriFieldTypeString', 'alias': 'MATCH_TYPE', 'length': 5},
 {'name': 'loc_name', 'type': 'esriFieldTypeString', 'alias': 'LOC_NAME', 'length': 40},
 {'name': 'geox', 'type': 'esriFieldTypeDouble', 'alias': 'GEOX', 'length': None},
 {'name': 'geoy', 'type': 'esriFieldTypeDouble', 'alias': 'GEOY', 'length': None},
 {'name': 'facility_type', 'type': 'esriFieldTypeString', 'alias': 'FACILITY_TYPE', 'length': 20},
 {'name': 'subfacility_code', 'type': 'esriFieldTypeString', 'alias': 'SUBFACILITY_CODE', 'length': 20},
 {'name': 'data_source_id', 'type': 'esriFieldTypeInteger', 'alias': 'DATA_SOURCE_ID', 'length': None}]
PAGE_SIZE = 250
MAX_CAPTURE_BYTES = 256_000_000
MAX_RESPONSE_BYTES = 4_000_000
MIN_CAPTURE_BYTES = 1
DISK_FLOOR_BYTES = 400_000_000
STAGING_OVERHEAD_BYTES = 2_000_000
MAX_CAPTURE_MANIFEST_BYTES = 1_000_000
MAX_CAPTURE_REQUESTS = 512
MAX_CAPTURE_RECEIPT_BYTES = 64_000
MAX_CAPTURE_RECEIPT_TOTAL_BYTES = 4_000_000


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _validate_capture_limits(max_capture_bytes: int, max_response_bytes: int) -> None:
    if type(max_capture_bytes) is not int or not MIN_CAPTURE_BYTES <= max_capture_bytes <= MAX_CAPTURE_BYTES:
        raise ValueError("capture_budget_invalid")
    if type(max_response_bytes) is not int or not MIN_CAPTURE_BYTES <= max_response_bytes <= MAX_RESPONSE_BYTES:
        raise ValueError("response_budget_invalid")
    if max_response_bytes > max_capture_bytes:
        raise ValueError("response_budget_exceeds_capture_budget")


def _require_disk_space(path: Path, reserve_bytes: int = 0) -> None:
    if type(reserve_bytes) is not int or reserve_bytes < 0:
        raise ValueError("disk_reserve_invalid")
    if shutil.disk_usage(path).free < DISK_FLOOR_BYTES + reserve_bytes:
        raise ValueError("local_storage_floor")


def _read_bounded_capture_file(path: Path, max_bytes: int, *, expected_bytes: int | None = None) -> bytes:
    try:
        before = path.lstat()
    except OSError as exc:
        raise ValueError("retained_capture_artifact_unavailable") from exc
    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
            or path.is_symlink()):
        raise ValueError("retained_capture_artifact_type_invalid")
    if type(max_bytes) is not int or max_bytes < 0:
        raise ValueError("retained_capture_read_limit_invalid")
    if before.st_size > max_bytes:
        raise ValueError("retained_capture_artifact_oversized")
    if expected_bytes is not None and before.st_size != expected_bytes:
        raise ValueError("retained_capture_artifact_size_mismatch")
    read_limit = before.st_size if expected_bytes is None else expected_bytes
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        raise ValueError("retained_capture_artifact_type_invalid") from exc
    try:
        opened = os.fstat(descriptor)
        if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                or (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
                != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)):
            raise ValueError("retained_capture_artifact_changed_before_read")
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            data = stream.read(read_limit + 1)
        after = os.fstat(descriptor)
        if len(data) > read_limit:
            raise ValueError("retained_capture_artifact_oversized")
        if ((after.st_size, after.st_mtime_ns, after.st_nlink)
                != (opened.st_size, opened.st_mtime_ns, 1) or len(data) != opened.st_size):
            raise ValueError("retained_capture_artifact_changed_during_read")
        return data
    finally:
        os.close(descriptor)


def _capture_descriptors(root: Path, captures: dict[str, tuple[bytes, dict]]) -> dict[str, dict]:
    if not 1 <= len(captures) <= MAX_CAPTURE_REQUESTS:
        raise ValueError("capture_request_count_invalid")
    result = {}
    total_payload = total_receipts = 0
    for name, (body, receipt) in sorted(captures.items()):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("capture_name_invalid")
        if _read_bounded_capture_file(root / f"{name}.body", MAX_RESPONSE_BYTES) != body:
            raise ValueError("capture_body_file_mismatch:" + name)
        wire = _read_bounded_capture_file(root / f"{name}.wire", MAX_RESPONSE_BYTES)
        receipt_bytes = _read_bounded_capture_file(root / f"{name}.receipt.json", MAX_CAPTURE_RECEIPT_BYTES)
        if json.loads(receipt_bytes) != receipt:
            raise ValueError("capture_receipt_file_mismatch:" + name)
        if (receipt.get("method") != "GET" or receipt.get("status") != 200
                or receipt.get("complete_body") is not True or receipt.get("final_url") != receipt.get("url")
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt.get("error_code") is not None or receipt.get("error_type") is not None):
            raise ValueError("capture_receipt_invalid:" + name)
        result[name] = {
            "request_url": receipt.get("url"),
            "body": {"file": f"{name}.body", "bytes": len(body), "sha256": _sha(body)},
            "wire": {"file": f"{name}.wire", "bytes": len(wire), "sha256": _sha(wire)},
            "receipt": {"file": f"{name}.receipt.json", "bytes": len(receipt_bytes), "sha256": _sha(receipt_bytes)},
        }
        if len(body) + len(wire) > MAX_RESPONSE_BYTES:
            raise ValueError("capture_response_budget_exceeded:" + name)
        total_payload += len(body) + len(wire)
        total_receipts += len(receipt_bytes)
    if total_receipts > MAX_CAPTURE_RECEIPT_TOTAL_BYTES or total_payload > MAX_CAPTURE_BYTES:
        raise ValueError("capture_artifact_totals_exceeded")
    expected = {"capture_manifest.json"}
    for descriptor in result.values():
        expected.update(descriptor[kind]["file"] for kind in ("body", "wire", "receipt"))
    validate_capture_roster(root, expected - {"capture_manifest.json"})
    return result


def _write_capture_manifest(root: Path, captures: dict[str, tuple[bytes, dict]], retained_bytes: int,
                           capture_budget_bytes: int, response_budget_bytes: int) -> tuple[dict, str]:
    _require_disk_space(root, STAGING_OVERHEAD_BYTES)
    descriptors = _capture_descriptors(root, captures)
    manifest = {"schema_version": 1, "capture_complete": True,
                "capture_budget_bytes": capture_budget_bytes,
                "response_budget_bytes": response_budget_bytes,
                "capture_retained_wire_plus_decoded_bytes": retained_bytes,
                "captures": descriptors}
    encoded = _canonical(manifest) + b"\n"
    path = root / "capture_manifest.json"
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
    return manifest, _sha(encoded)


def _load_retained_capture(root: Path) -> tuple[dict, dict[str, tuple[bytes, dict]], str]:
    root = Path(root).absolute()
    if not root.is_dir() or root.is_symlink():
        raise ValueError("retained_capture_directory_invalid")
    manifest_path = root / "capture_manifest.json"
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_missing")
    encoded = _read_bounded_capture_file(manifest_path, MAX_CAPTURE_MANIFEST_BYTES)
    manifest = json.loads(encoded)
    if (not isinstance(manifest, dict) or set(manifest) != {
                "schema_version", "capture_complete", "capture_budget_bytes", "response_budget_bytes",
                "capture_retained_wire_plus_decoded_bytes", "captures"}
            or manifest.get("schema_version") != 1
            or manifest.get("capture_complete") is not True
            or not isinstance(manifest.get("captures"), dict)
            or type(manifest.get("capture_retained_wire_plus_decoded_bytes")) is not int
            or not MIN_CAPTURE_BYTES <= manifest["capture_retained_wire_plus_decoded_bytes"] <= MAX_CAPTURE_BYTES
            or type(manifest.get("capture_budget_bytes")) is not int
            or not manifest["capture_retained_wire_plus_decoded_bytes"] <= manifest["capture_budget_bytes"] <= MAX_CAPTURE_BYTES
            or type(manifest.get("response_budget_bytes")) is not int
            or not MIN_CAPTURE_BYTES <= manifest["response_budget_bytes"] <= MAX_RESPONSE_BYTES):
        raise ValueError("retained_capture_manifest_invalid")
    descriptors = manifest["captures"]
    if not 1 <= len(descriptors) <= MAX_CAPTURE_REQUESTS:
        raise ValueError("retained_capture_request_count_invalid")
    expected = {"capture_manifest.json"}
    descriptor_capture_total = descriptor_receipt_total = 0
    for name, descriptor in descriptors.items():
        if (not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", name)
                or not isinstance(descriptor, dict)
                or set(descriptor) != {"request_url", "body", "wire", "receipt"}):
            raise ValueError("retained_capture_manifest_invalid")
        for kind, suffix in (("body", ".body"), ("wire", ".wire"), ("receipt", ".receipt.json")):
            artifact = descriptor.get(kind)
            if (not isinstance(artifact, dict) or set(artifact) != {"file", "bytes", "sha256"}
                    or artifact.get("file") != name + suffix
                    or type(artifact.get("bytes")) is not int or artifact["bytes"] < 0
                    or not isinstance(artifact.get("sha256"), str)
                    or not re.fullmatch(r"[0-9a-f]{64}", artifact["sha256"])):
                raise ValueError("retained_capture_manifest_invalid")
            expected.add(artifact["file"])
        response_bytes = descriptor["body"]["bytes"] + descriptor["wire"]["bytes"]
        if response_bytes > manifest["response_budget_bytes"]:
            raise ValueError("retained_capture_response_budget_exceeded:" + name)
        descriptor_capture_total += response_bytes
        descriptor_receipt_total += descriptor["receipt"]["bytes"]
    if (descriptor_capture_total != manifest["capture_retained_wire_plus_decoded_bytes"]
            or descriptor_capture_total > manifest["capture_budget_bytes"]
            or descriptor_receipt_total > MAX_CAPTURE_RECEIPT_TOTAL_BYTES):
        raise ValueError("retained_capture_descriptor_totals_invalid")
    validate_capture_roster(root, expected)
    for name, descriptor in descriptors.items():
        for kind in ("body", "wire", "receipt"):
            artifact = descriptor[kind]
            max_size = (MAX_CAPTURE_RECEIPT_BYTES if kind == "receipt" else
                        min(MAX_RESPONSE_BYTES, manifest["response_budget_bytes"]))
            _read_bounded_capture_file(root / artifact["file"], max_size, expected_bytes=artifact["bytes"])
    captures: dict[str, tuple[bytes, dict]] = {}
    total = 0
    for name, descriptor in descriptors.items():
        artifacts = {kind: _read_bounded_capture_file(
                         root / descriptor[kind]["file"],
                         MAX_CAPTURE_RECEIPT_BYTES if kind == "receipt" else min(MAX_RESPONSE_BYTES, manifest["response_budget_bytes"]),
                         expected_bytes=descriptor[kind]["bytes"])
                     for kind in ("body", "wire", "receipt")}
        for kind, data in artifacts.items():
            recorded = descriptor[kind]
            if len(data) != recorded["bytes"] or _sha(data) != recorded["sha256"]:
                raise ValueError("retained_capture_artifact_mismatch:" + name + ":" + kind)
        body, wire, receipt_bytes = artifacts["body"], artifacts["wire"], artifacts["receipt"]
        receipt = json.loads(receipt_bytes)
        url = descriptor.get("request_url")
        if (not isinstance(url, str) or receipt.get("method") != "GET" or receipt.get("url") != url
                or receipt.get("final_url") != url or receipt.get("status") != 200
                or receipt.get("complete_body") is not True
                or type(receipt.get("bytes")) is not int or receipt["bytes"] != len(body)
                or receipt.get("sha256") != _sha(body)
                or type(receipt.get("wire_bytes")) is not int or receipt["wire_bytes"] != len(wire)
                or receipt.get("wire_sha256") != _sha(wire)
                or receipt.get("error_code") is not None or receipt.get("error_type") is not None):
            raise ValueError("retained_capture_receipt_invalid:" + name)
        captures[name] = (body, receipt)
        total += len(body) + len(wire)
    if total != manifest["capture_retained_wire_plus_decoded_bytes"]:
        raise ValueError("retained_capture_budget_total_mismatch")
    return manifest, captures, _sha(encoded)


def _field_contract(layer: dict) -> list[dict]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(f, dict) for f in fields):
        raise ValueError("source_fields_missing")
    plain = [{"name": f.get("name"), "type": f.get("type"), "alias": f.get("alias"),
              "length": f.get("length")} for f in fields]
    if plain != FIELD_DECLARATIONS or _sha(_canonical(plain)) != SCHEMA_SHA256:
        raise ValueError("source_schema_drift")
    oid_fields = [f["name"] for f in fields if f.get("type") == "esriFieldTypeOID"]
    if oid_fields != ["objectid"]:
        raise ValueError("source_oid_declaration_drift")
    attributes = [f for f in fields if f.get("type") != "esriFieldTypeGeometry"]
    if len(attributes) != 72:
        raise ValueError("source_attribute_field_count_drift")
    return attributes


def _arrow_type(field: dict) -> pa.DataType:
    kind = field["type"]
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeSingle": pa.float32(), "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64()}[kind]


def _native_parquet(fields: list[dict], features: list[dict]) -> bytes:
    cols = {f["name"]: pa.array([x["attributes"][f["name"]] for x in features], type=_arrow_type(f)) for f in fields}
    cols["geometry"] = pa.array([None if x["geometry"] is None else Point(x["geometry"]["x"], x["geometry"]["y"]).wkb for x in features], type=pa.binary())
    schema = pa.schema([pa.field(f["name"], _arrow_type(f), nullable=f.get("nullable", True)) for f in fields] + [pa.field("geometry", pa.binary(), nullable=True)])
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{"encoding":"WKB", "geometry_types":["Point"], "crs":pyproj.CRS.from_epsg(4326).to_json_dict(), "edges":"planar"}}}
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(cols, schema=schema.with_metadata({b"geo":_canonical(geo)})), sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def capture_fema_nss_shelter_locations(storage: StagingStorageResource, *, version: str,
                                capture_directory: str | None = None,
                                retained_capture_directory: str | None = None,
                                retained_capture_manifest_sha256: str | None = None,
                                max_capture_bytes: int = MAX_CAPTURE_BYTES,
                                max_response_bytes: int = MAX_RESPONSE_BYTES) -> dict:
    """Bracket a complete ID-paged capture and retain every source response."""
    _validate_capture_limits(max_capture_bytes, max_response_bytes)
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute(); base.mkdir(parents=True, exist_ok=True)
    if base.is_symlink() or not base.is_dir():
        raise ValueError("explicit_local_staging_directory_invalid")
    if retained_capture_directory and capture_directory:
        raise ValueError("live_and_retained_capture_directories_conflict")
    if retained_capture_directory and (not isinstance(retained_capture_manifest_sha256, str)
                                       or not re.fullmatch(r"[0-9a-f]{64}", retained_capture_manifest_sha256)):
        raise ValueError("retained_capture_manifest_digest_required")
    if not retained_capture_directory and retained_capture_manifest_sha256 is not None:
        raise ValueError("retained_capture_manifest_digest_without_directory")
    capture_root = (Path(retained_capture_directory).absolute() if retained_capture_directory else
                    Path(capture_directory or "/workspace/work/national-shelter-system-facilities/captures").absolute() / version)
    if retained_capture_directory:
        if not capture_root.is_dir() or capture_root.is_symlink():
            raise ValueError("retained_capture_directory_invalid")
    else:
        capture_root.mkdir(parents=True, exist_ok=True)
        if capture_root.is_symlink() or not capture_root.is_dir():
            raise ValueError("capture_directory_invalid")
    _require_disk_space(base)
    _require_disk_space(capture_root)
    captures: dict[str, tuple[bytes, dict]] = {}
    offline_manifest = offline_receipts = None
    offline_manifest_sha = None
    retained_bytes = 0
    if retained_capture_directory:
        offline_manifest, offline_receipts, offline_manifest_sha = _load_retained_capture(capture_root)
        if offline_manifest_sha != retained_capture_manifest_sha256:
            raise ValueError("retained_capture_manifest_digest_mismatch")
        if (offline_manifest["capture_budget_bytes"] != max_capture_bytes
                or offline_manifest["response_budget_bytes"] != max_response_bytes):
            raise ValueError("retained_capture_budget_config_mismatch")
        retained_bytes = offline_manifest["capture_retained_wire_plus_decoded_bytes"]
    elif any(capture_root.iterdir()):
        raise ValueError("capture_directory_not_empty")

    def reserve_capture(progress, response_used: list[int]) -> None:
        projected = retained_bytes + progress.reserve_bytes
        if projected > max_capture_bytes:
            raise ValueError("source_capture_budget_exceeded")
        _require_disk_space(capture_root, progress.reserve_bytes + STAGING_OVERHEAD_BYTES)
        _require_disk_space(base, projected + STAGING_OVERHEAD_BYTES)

    def before_retain(kind: str, data: bytes, response_used: list[int]) -> None:
        nonlocal retained_bytes
        if retained_bytes + len(data) > max_capture_bytes:
            raise ValueError("source_capture_budget_exceeded")
        if response_used[0] + len(data) > max_response_bytes:
            raise ValueError("source_response_budget_exceeded")
        _require_disk_space(capture_root, len(data) + STAGING_OVERHEAD_BYTES)
        _require_disk_space(base, retained_bytes + len(data) + STAGING_OVERHEAD_BYTES)
        response_used[0] += len(data)
        retained_bytes += len(data)

    def get(client, name, url, params=None):
        target = url + ("?" + urlencode(params) if params else "")
        nonlocal retained_bytes
        if offline_receipts is not None:
            if name not in offline_receipts or offline_receipts[name][1].get("url") != target:
                raise ValueError("retained_capture_request_mismatch:" + name)
            body, receipt = offline_receipts[name]
            wire_size = (capture_root / f"{name}.wire").stat().st_size
            if len(body) + wire_size > max_response_bytes:
                raise ValueError("retained_capture_response_budget_exceeded:" + name)
            captures[name] = (body, receipt)
        else:
            response_used = [0]
            try:
                body, receipt = capture_get(
                    client, target, capture_root, name, max_bytes=max_response_bytes,
                    reserve=lambda progress: reserve_capture(progress, response_used),
                    before_retain=lambda kind, data: before_retain(kind, data, response_used),
                )
            except Exception:
                receipt_path = capture_root / f"{name}.receipt.json"
                if receipt_path.is_file():
                    try:
                        receipt = json.loads(_read_bounded_capture_file(receipt_path, MAX_CAPTURE_RECEIPT_BYTES))
                        body_path = capture_root / f"{name}.body"
                        if isinstance(receipt, dict) and body_path.is_file():
                            captures[name] = (_read_bounded_capture_file(body_path, MAX_RESPONSE_BYTES), receipt)
                    except (OSError, ValueError, TypeError):
                        pass
                raise
            captures[name] = (body, receipt)
        data = json.loads(body)
        if not isinstance(data, dict) or "error" in data: raise ValueError("source_response_error:" + name)
        return data
    try:
        client_context = (nullcontext(None) if retained_capture_directory else
                          httpx.Client(follow_redirects=False, timeout=90, headers={"User-Agent":"hifld-next-datasets/fema-nss-shelter-locations"}))
        with client_context as client:
            portal_item = get(client, "portal_item", PORTAL_ITEM_URL)
            if (portal_item.get("id") != ITEM_ID or portal_item.get("owner") != "FEMA_MAC"
                    or portal_item.get("title") != "National Shelter System Facilities"
                    or portal_item.get("type") != "Feature Service"
                    or portal_item.get("url") != PORTAL_LAYER_URL or portal_item.get("access") != "public"):
                raise ValueError("source_portal_authority_or_publication_drift")
            service = get(client, "service_before", SERVICE_URL, {"f":"pjson"})
            layer = get(client, "layer_before", LAYER_URL, {"f":"pjson"})
            if (layer.get("name") != LAYER_NAME or layer.get("description") != LAYER_DESCRIPTION
                    or layer.get("geometryType") != "esriGeometryPoint"
                    or layer.get("definitionExpression") is not None
                    or service.get("description") != SERVICE_DESCRIPTION):
                raise ValueError("source_layer_identity_drift")
            fields = _field_contract(layer)
            has_z, has_m = layer.get("hasZ"), layer.get("hasM")
            if type(has_z) is not bool or type(has_m) is not bool:
                raise ValueError("source_geometry_dimensions_unpinned")
            if has_z or has_m:
                raise ValueError("source_geometry_dimensions_unsupported")
            sr = layer.get("extent", {}).get("spatialReference", {})
            if sr.get("latestWkid") != 4326: raise ValueError("source_crs_drift")
            count = get(client, "count_before", LAYER_URL+"/query", {"where":"1=1","returnCountOnly":"true","f":"json"}).get("count")
            ids_response = get(client, "ids_before", LAYER_URL+"/query", {"where":"1=1","returnIdsOnly":"true","f":"json"})
            oid_name = next(f["name"] for f in layer["fields"] if f.get("type") == "esriFieldTypeOID")
            ids = ids_response.get("objectIds")
            if ids_response.get("objectIdFieldName") != oid_name or type(count) is not int or count < 1 or not isinstance(ids,list) or len(ids)!=count or any(type(i) is not int for i in ids) or len(set(ids))!=count: raise ValueError("source_initial_id_roster_invalid")
            if 10 + math.ceil(count / PAGE_SIZE) > MAX_CAPTURE_REQUESTS:
                raise ValueError("source_capture_request_count_exceeded")
            seed = sorted(ids); features=[]
            for page_no, offset in enumerate(range(0,count,PAGE_SIZE)):
                expected_ids = seed[offset:offset + PAGE_SIZE]
                page = get(client, f"page_{page_no:03d}", LAYER_URL+"/query", {
                    "objectIds": ",".join(str(value) for value in expected_ids),
                    "outFields":"*", "returnGeometry":"true", "returnZ":"false",
                    "returnM":"false", "outSR":"4326", "resultRecordCount":PAGE_SIZE,
                    "f":"json"})
                rows=page.get("features")
                if not isinstance(rows,list) or len(rows)!=len(expected_ids):
                    raise ValueError("source_page_incomplete")
                if page.get("exceededTransferLimit") is True:
                    raise ValueError("source_page_exceeded_transfer_limit")
                page_sr=page.get("spatialReference",{})
                if not isinstance(page_sr,dict) or page_sr.get("latestWkid",page_sr.get("wkid"))!=4326:
                    raise ValueError("source_page_crs_drift")
                page_ids=[row.get("attributes",{}).get(oid_name) if isinstance(row,dict) else None for row in rows]
                if sorted(page_ids)!=expected_ids or len(set(page_ids))!=len(expected_ids):
                    raise ValueError("source_page_id_slice_mismatch")
                features.extend(rows)
            layer_after=get(client,"layer_after",LAYER_URL,{"f":"pjson"})
            service_after=get(client,"service_after",SERVICE_URL,{"f":"pjson"})
            count_after=get(client,"count_after",LAYER_URL+"/query",{"where":"1=1","returnCountOnly":"true","f":"json"}).get("count")
            ids_after_response=get(client,"ids_after",LAYER_URL+"/query",{"where":"1=1","returnIdsOnly":"true","f":"json"})
            ids_after=ids_after_response.get("objectIds")
            if (ids_after_response.get("objectIdFieldName") != oid_name or not isinstance(ids_after, list)
                    or len(ids_after) != count or any(type(i) is not int for i in ids_after)
                    or len(set(ids_after)) != count):
                raise ValueError("source_oid_property_drift")
        if layer_after != layer or service_after != service or count_after != count or sorted(ids_after or []) != seed: raise ValueError("source_changed_during_capture")
        names=[f["name"] for f in fields]; got=[]; expected_geometry_keys={"x","y"} | ({"z"} if has_z else set()) | ({"m"} if has_m else set())
        for item in features:
            a=item.get("attributes"); g=item.get("geometry")
            if not isinstance(a,dict) or list(a)!=names: raise ValueError("source_record_schema_drift")
            if g is not None:
                if not isinstance(g,dict) or set(g)!=expected_geometry_keys: raise ValueError("source_geometry_dimensions_drift")
                if not all(type(g[k]) in (int,float) and math.isfinite(g[k]) for k in expected_geometry_keys): raise ValueError("source_coordinate_invalid")
                if not -180 <= g["x"] <= 180 or not -90 <= g["y"] <= 90:
                    raise ValueError("source_coordinate_outside_epsg4326")
            got.append(a.get(oid_name))
        if len(features)!=count or sorted(got)!=seed or len(set(got))!=count: raise ValueError("source_feature_population_incomplete")
        features.sort(key=lambda x:x["attributes"][oid_name])
        if offline_receipts is not None and set(captures) != set(offline_receipts):
            raise ValueError("retained_capture_request_roster_mismatch")
    except Exception as exc:
        if captures: quarantine_incomplete_capture(storage,capture_root,DATASET_SLUG,version,captures,type(exc).__name__)
        raise
    if retained_capture_directory:
        capture_manifest, capture_manifest_sha = offline_manifest, offline_manifest_sha
    else:
        capture_manifest, capture_manifest_sha = _write_capture_manifest(
            capture_root, captures, retained_bytes, max_capture_bytes, max_response_bytes)
    if set(capture_manifest.get("captures", {})) != set(captures):
        raise ValueError("capture_manifest_request_roster_mismatch")
    record_sha=_sha(_canonical(features)); geom_sha=_sha(_canonical([[x["attributes"][oid_name],None if x["geometry"] is None else x["geometry"]] for x in features]))
    identity=processing_identity(Path(__file__).parents[3]); schema_sha=SCHEMA_SHA256
    native=_native_parquet(fields,features); version=content_derived_version(record_sha,geom_sha,schema_sha,identity); prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    null_geometry_count=sum(x["geometry"] is None for x in features)
    native_name=f"geoparquet/{DATASET_SLUG}.parquet"
    audit={"feature_count":count,"source_id_sha256":_sha(_canonical(seed)),"source_record_sha256":record_sha,"source_geometry_sha256":geom_sha,"source_schema_sha256":schema_sha,"source_crs_epsg":4326,"geometry_type":"Point","null_geometry_count":null_geometry_count,"all_ids_captured_once":True,"ids_equal_before_after":True,"portal_item_id":ITEM_ID,"capture_manifest_sha256":capture_manifest_sha,"capture_retained_wire_plus_decoded_bytes":capture_manifest["capture_retained_wire_plus_decoded_bytes"],"capture_budget_bytes":max_capture_bytes,"response_budget_bytes":max_response_bytes,"source_vintage":None,"source_vintage_note":"FEMA exposes no source data release timestamp; portal item modified time is not a row vintage."}
    manifest={"schema_version":1,"approved":False,"title":LAYER_NAME,"publisher":PUBLISHER,"agency":"Federal Emergency Management Agency","source_url":LAYER_URL,"source_release_date":None,"source_modified":None,"portal_item_modified_epoch_ms":portal_item["modified"],"portal_item_modified_is_source_release_date":False,"data_vintage_unknown":True,"contact_data_review_pending":True,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],"source_item_id":ITEM_ID,"source_vintage":None,"source_scope":"Current official FEMA MapServer layer 5 Shelter Locations (all shelter status types); not an asserted reconstruction of historical NSS/HIFLD coverage.","source_geographic_coverage_note":"The FEMA item description says continental United States and some territories; the service extent and observed count do not establish territory completeness.","operational_use_warning":"FEMA states not to use this layer to determine facility operational status during an active emergency.","archive_baseline_accepted":False,"historical_equivalence_accepted":False,"source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"related_layer_observation":{"layer_id":1,"name":"Closed Shelters","definition_expression":None,"observed_feature_count":71897,"observed_at_utc":"2026-10-22T04:31:32.641139+00:00","comparison_note":"Layer 1 is a distinct closed-shelters view and is not a partition or substitute. Its observed count differs from layer 5 count 71875; the relationship remains unresolved. Counts must not be summed."},"capture_manifest_sha256":capture_manifest_sha,"capture_retained_wire_plus_decoded_bytes":capture_manifest["capture_retained_wire_plus_decoded_bytes"],"capture_budget_bytes":max_capture_bytes,"response_budget_bytes":max_response_bytes,"inputs":[{"input_id":n,"producer":PUBLISHER,"host":urlsplit(r["url"]).hostname,"role":"producer_publication","canonical_url":PORTAL_ITEM_URL.split("?",1)[0] if n=="portal_item" else SERVICE_URL,"retrieval_url":r["url"],"observed_final_url":r["final_url"],"retrieved_at":r["observed_at"],"http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],"wire_bytes":r["wire_bytes"],"wire_sha256":r["wire_sha256"],"license":"Unreviewed; no license claim made.","attribution":"FEMA MAC ArcGIS item attribution has not been reviewed.","release_date":None,"release_date_unknown_reason":"The service and item metadata do not establish a FEMA producer release date."} for n,(_,r) in sorted(captures.items())],"transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{"layer_id":LAYER_ID,"fields":"all 72 non-geometry source fields retained in source order and declared physical types","geometry":"native point coordinates retained as WKB in EPSG:4326","capture_budget_bytes":max_capture_bytes,"response_budget_bytes":max_response_bytes,"source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"historical_equivalence_accepted":False,"archive_baseline_accepted":False,"mapping_approved":False,"release_accepted":False}}],"prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native Esri attributes, source IDs, and point coordinates are preserved without public mapping; approval gates remain open.","objects":[{"path":native_name,"bytes":len(native),"sha256":_sha(native)}]}}},"unsupported_output_formats":{"geopackage":"No approved source-to-public schema mapping.","pmtiles":"No approved display transformation."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    outputs={native_name:native,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n","metadata/capture_manifest.json":_canonical(capture_manifest)+b"\n"}
    for name,(body,receipt) in captures.items(): outputs.update({f"raw/{name}.body":body,f"raw/{name}.wire":_read_bounded_capture_file(capture_root/f"{name}.wire", MAX_RESPONSE_BYTES),f"raw/{name}.receipt.json":_canonical(receipt)+b"\n"})
    ready={"approved":False,"version":version,"objects":{native_name:{"bytes":len(native),"sha256":_sha(native)},"metadata/capture_manifest.json":{"bytes":len(outputs["metadata/capture_manifest.json"]),"sha256":_sha(outputs["metadata/capture_manifest.json"])},**{k:{"bytes":len(v),"sha256":_sha(v)} for k,v in outputs.items() if k.startswith("raw/")}},"source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"historical_equivalence_accepted":False,"archive_baseline_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"]=_canonical(ready)+b"\n"
    state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    fingerprint=_sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        sealed=json.loads(load_first_construction(storage,state_key,version,fingerprint))
        payload=sealed.get("payload")
        if not isinstance(payload,dict): raise ValueError("cached_first_candidate_payload_missing")
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,
            identity,record_sha,schema_sha,payload.get("objects",{}),payload.get("capture_receipts",{}))
        if set(payload["objects"])!=set(outputs): raise ValueError("cached_candidate_roster_changed")
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":count,
                "source_null_geometry_count":null_geometry_count,"candidate_ready":f"{prefix}/metadata/candidate_ready.json",
                "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
                "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,
                "release_accepted":False,"reused":True,"capture_directory":str(capture_root),
                "capture_manifest_sha256":capture_manifest_sha,"capture_retained_wire_plus_decoded_bytes":retained_bytes,
                "capture_budget_bytes":max_capture_bytes,"response_budget_bytes":max_response_bytes}
    _require_disk_space(base, sum(map(len, outputs.values())) + STAGING_OVERHEAD_BYTES)
    for name,body in outputs.items():
        key=f"{prefix}/{name}"
        write_immutable(storage,key,body)
        snapshot=storage.object_snapshot(key)
        if snapshot is None or snapshot.size!=len(body) or storage.sha256_key(key)!=_sha(body) or storage.object_snapshot(key)!=snapshot:
            raise ValueError("candidate_output_physical_readback_failed:"+name)
    descriptors={name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,
                         "source_record_sha256":record_sha,"source_schema_sha256":schema_sha,
                         "capture_receipts":{name:receipt for name,(_body,receipt) in captures.items()},
                         "objects":descriptors})
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":count,"source_null_geometry_count":null_geometry_count,"candidate_ready":f"{prefix}/metadata/candidate_ready.json","source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"capture_directory":str(capture_root),"capture_manifest_sha256":capture_manifest_sha,"capture_retained_wire_plus_decoded_bytes":retained_bytes,"capture_budget_bytes":max_capture_bytes,"response_budget_bytes":max_response_bytes}

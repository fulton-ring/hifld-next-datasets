"""USACE Offices candidate source; preserves exact public FeatureServer responses."""
from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import stat
import subprocess
import tempfile
import base64
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely.geometry import Point

from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file

SLUG = "us-army-corps-of-engineers-usace-offices"
SERVICE = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/usace_offices/FeatureServer"
BASE = SERVICE + "/0"
LANDING = "https://catalog.data.gov/dataset/us-army-corps-of-engineers-usace-offices"
CAPTURE_ENV = "HIFLD_USACE_OFFICES_CAPTURE_DIR"
MAX_COUNT = 10000
MAX_CAPTURE = 8 * 1024 * 1024
EXPECTED_FIELDS = (
    ("OBJECTID", "esriFieldTypeOID", False, None),
    ("CODE", "esriFieldTypeString", True, 3000),
    ("NAME", "esriFieldTypeString", True, 3000),
    ("HQ_DIVISION_OTHER", "esriFieldTypeString", True, 3000),
    ("DISTRICT_LAB", "esriFieldTypeString", True, 3000),
    ("PHYSICAL_ADDRESS", "esriFieldTypeString", True, 3000),
    ("LONGITUDE", "esriFieldTypeDouble", True, None),
    ("LATITUDE", "esriFieldTypeDouble", True, None),
    ("TIME_ZONE", "esriFieldTypeString", True, 3000),
    ("SE_ANNO_CAD_DATA", "esriFieldTypeBlob", True, None),
)
CRS_4326 = {
    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
    "area": "World.",
    "bbox": {"east_longitude": 180, "north_latitude": 90, "south_latitude": -90, "west_longitude": -180},
    "coordinate_system": {"axis": [
        {"abbreviation": "Lat", "direction": "north", "name": "Geodetic latitude", "unit": "degree"},
        {"abbreviation": "Lon", "direction": "east", "name": "Geodetic longitude", "unit": "degree"}], "subtype": "ellipsoidal"},
    "datum_ensemble": {"accuracy": "2.0", "ellipsoid": {"inverse_flattening": 298.257223563, "name": "WGS 84", "semi_major_axis": 6378137},
        "id": {"authority": "EPSG", "code": 6326}, "members": [
            {"id": {"authority": "EPSG", "code": 1166}, "name": "World Geodetic System 1984 (Transit)"},
            {"id": {"authority": "EPSG", "code": 1152}, "name": "World Geodetic System 1984 (G730)"},
            {"id": {"authority": "EPSG", "code": 1153}, "name": "World Geodetic System 1984 (G873)"},
            {"id": {"authority": "EPSG", "code": 1154}, "name": "World Geodetic System 1984 (G1150)"},
            {"id": {"authority": "EPSG", "code": 1155}, "name": "World Geodetic System 1984 (G1674)"},
            {"id": {"authority": "EPSG", "code": 1156}, "name": "World Geodetic System 1984 (G1762)"},
            {"id": {"authority": "EPSG", "code": 1309}, "name": "World Geodetic System 1984 (G2139)"},
            {"id": {"authority": "EPSG", "code": 1383}, "name": "World Geodetic System 1984 (G2296)"}],
        "name": "World Geodetic System 1984 ensemble"}, "id": {"authority": "EPSG", "code": 4326},
    "name": "WGS 84", "scope": "Horizontal component of 3D system.", "type": "GeographicCRS"}


class USACEOfficesSourceError(ValueError):
    """Fail-closed source, drift, or candidate error."""


def _json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode()


def _strict_json(body: bytes) -> dict:
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise USACEOfficesSourceError("duplicate_json_member")
            result[key] = value
        return result
    try:
        value = json.loads(body, object_pairs_hook=pairs, parse_constant=lambda _: (_ for _ in ()).throw(USACEOfficesSourceError("nonfinite_json")))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise USACEOfficesSourceError("invalid_source_json") from exc
    if not isinstance(value, dict) or "error" in value:
        raise USACEOfficesSourceError("source_json_error_or_nonobject")
    return value


def _capture(client: httpx.Client, url: str, directory: Path, name: str) -> tuple[bytes, dict]:
    wire_path, decoded_path = directory / f"{name}.wire", directory / f"{name}.decoded"
    if wire_path.exists() or decoded_path.exists():
        raise USACEOfficesSourceError("capture_name_already_exists")
    for path in (wire_path, decoded_path):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        os.close(fd)

    def sink(path: Path):
        def retain(block: bytes):
            fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
            try:
                if os.write(fd, block) != len(block):
                    raise OSError("short_capture_write")
                os.fsync(fd)
            finally:
                os.close(fd)
        return retain

    retrieved_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with client.stream("GET", url, headers={"User-Agent": "hifld-eoc-source-adapter/1.0", "Accept": "application/json"}) as response:
        headers = dict(response.headers)
        result = capture_http_response(response, max_wire_bytes=MAX_CAPTURE, max_decoded_bytes=MAX_CAPTURE,
            reserve=lambda progress: None, retain_wire=sink(wire_path), retain_decoded=sink(decoded_path))
    pending = {}
    for label in ("wire", "decoded"):
        block = getattr(result, f"pending_{label}")
        if block:
            pending_path = directory / f"{name}.pending-{label}"
            fd = os.open(pending_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            try:
                if os.write(fd, block) != len(block):
                    raise OSError("short_pending_capture_write")
                os.fsync(fd)
            finally:
                os.close(fd)
            if pending_path.read_bytes() != block:
                raise USACEOfficesSourceError("pending_capture_readback_mismatch")
            pending[label] = {"bytes": len(block), "sha256": hashlib.sha256(block).hexdigest(), "path": pending_path.name}
    wire, decoded = wire_path.read_bytes(), decoded_path.read_bytes()
    readback = (len(wire) == result.wire_bytes_retained and hashlib.sha256(wire).hexdigest() == result.wire_sha256
                and len(decoded) == result.decoded_bytes_retained and hashlib.sha256(decoded).hexdigest() == result.decoded_sha256)
    receipt = {"url": url, "status": result.status_code, "retrieved_at": retrieved_at, "headers": headers,
        "transfer": {k: v for k, v in vars(result).items() if k not in ("pending_wire", "pending_decoded")},
        "wire": {"bytes": len(wire), "sha256": hashlib.sha256(wire).hexdigest(), "path": wire_path.name},
        "decoded": {"bytes": len(decoded), "sha256": hashlib.sha256(decoded).hexdigest(), "path": decoded_path.name},
        "pending": pending, "independent_physical_readback_matches_acknowledged_sinks": readback}
    (directory / f"{name}.receipt.json").write_bytes(_json(receipt))
    if not result.complete or not readback:
        raise USACEOfficesSourceError("incomplete_or_unreadback_http_capture")
    if not 200 <= result.status_code < 300:
        raise USACEOfficesSourceError(f"http_status_{result.status_code}")
    return decoded, receipt


def _query_url(**params) -> str:
    return BASE + "/query?" + urlencode({"f": "json", **params})


def _field_signature(metadata: dict) -> tuple:
    fields = metadata.get("fields")
    if not isinstance(fields, list):
        raise USACEOfficesSourceError("source_field_declarations_missing")
    return tuple((f.get("name"), f.get("type"), f.get("alias"), f.get("nullable"), f.get("length")) for f in fields)


def _object_ids(body: bytes) -> tuple[str, list[int]]:
    value = _strict_json(body)
    field = value.get("objectIdFieldName")
    ids = value.get("objectIds")
    if not isinstance(field, str) or not isinstance(ids, list) or any(type(i) is not int for i in ids):
        raise USACEOfficesSourceError("source_object_id_list_missing_or_invalid")
    if len(ids) > MAX_COUNT or len(set(ids)) != len(ids):
        raise USACEOfficesSourceError("source_object_id_limit_or_duplicate")
    return field, sorted(ids)


def _validate_rows(features: list, field_names: tuple[str, ...], object_id: str, ids: list[int]) -> list[str]:
    errors = []
    seen = []
    for feature in features:
        if not isinstance(feature, dict) or set(feature) != {"attributes", "geometry"}:
            errors.append("feature_shape"); continue
        attrs = feature["attributes"]
        if not isinstance(attrs, dict) or tuple(attrs.keys()) != field_names:
            errors.append("source_attributes_do_not_match_physical_declarations"); continue
        oid = attrs.get(object_id)
        if type(oid) is not int:
            errors.append("invalid_object_id")
        else:
            seen.append(oid)
        geom = feature.get("geometry")
        if not isinstance(geom, dict) or set(geom) - {"x", "y", "spatialReference"} or type(geom.get("x")) not in (int, float) or type(geom.get("y")) not in (int, float):
            errors.append("missing_or_unexpected_native_point_geometry")
        elif "spatialReference" in geom:
            sr = geom["spatialReference"]
            if not isinstance(sr, dict) or sr.get("latestWkid", sr.get("wkid")) != 4326:
                errors.append("native_point_spatial_reference_drift")
        elif not math.isfinite(float(geom["x"])) or not math.isfinite(float(geom["y"])):
            errors.append("nonfinite_source_coordinates")
    if sorted(seen) != ids or len(seen) != len(set(seen)):
        errors.append("full_source_id_roster_mismatch")
    return sorted(set(errors))


def _table(metadata: dict, features: list) -> tuple[pa.Table, list[str]]:
    fields = metadata["fields"]
    map_types = {"esriFieldTypeOID": pa.int64(), "esriFieldTypeSmallInteger": pa.int16(),
                 "esriFieldTypeString": pa.string(), "esriFieldTypeDouble": pa.float64(),
                 # ArcGIS declares SE_ANNO_CAD_DATA as a nullable Blob. Keep
                 # that physical declaration even while every current value is null.
                 "esriFieldTypeBlob": pa.binary()}
    arrays, schema_fields, quality = [], [], []
    for field in fields:
        name, esri_type = field["name"], field["type"]
        if esri_type not in map_types:
            raise USACEOfficesSourceError("unmapped_source_field_type:" + str(name))
        declared_nullable = field.get("nullable", True)
        if type(declared_nullable) is not bool:
            raise USACEOfficesSourceError("source_nullable_declaration_invalid:" + str(name))
        values = [row["attributes"][name] for row in features]
        expected = map_types[esri_type]
        for value in values:
            if value is None:
                if not declared_nullable: quality.append("null_in_nonnullable_field:" + name)
            elif esri_type in ("esriFieldTypeOID", "esriFieldTypeSmallInteger") and type(value) is not int:
                raise USACEOfficesSourceError("source_value_type_mismatch:" + name)
            elif esri_type == "esriFieldTypeString" and type(value) is not str:
                raise USACEOfficesSourceError("source_value_type_mismatch:" + name)
            elif esri_type == "esriFieldTypeDouble" and (type(value) not in (int, float) or not math.isfinite(float(value))):
                raise USACEOfficesSourceError("source_value_type_mismatch:" + name)
            elif esri_type == "esriFieldTypeBlob":
                if type(value) is not str:
                    raise USACEOfficesSourceError("source_blob_wire_type_mismatch:" + name)
        if esri_type == "esriFieldTypeBlob":
            try:
                values = [None if value is None else base64.b64decode(value, validate=True) for value in values]
            except (ValueError, TypeError) as exc:
                raise USACEOfficesSourceError("source_blob_base64_decode_failure:" + name) from exc
        try:
            arrays.append(pa.array(values, type=expected, from_pandas=False))
        except (pa.ArrowInvalid, OverflowError) as exc:
            raise USACEOfficesSourceError("source_arrow_type_range_failure:" + name) from exc
        schema_fields.append(pa.field(name, expected, nullable=declared_nullable))
    wkb = []
    for row in features:
        geom = row.get("geometry")
        if geom is None:
            wkb.append(None); quality.append("null_source_geometry")
        elif not isinstance(geom, dict) or not {"x", "y"}.issubset(geom):
            wkb.append(None); quality.append("invalid_source_geometry")
        else:
            point = Point(float(geom["x"]), float(geom["y"]))
            if point.is_empty or not point.is_valid or not math.isfinite(point.x) or not math.isfinite(point.y):
                quality.append("invalid_source_geometry")
            if point.x == 0 and point.y == 0:
                quality.append("source_zero_coordinate")
            if point.x < -180 or point.x > 180 or point.y < -90 or point.y > 90:
                quality.append("coordinates_outside_epsg4326_domain")
            wkb.append(shapely.to_wkb(point, output_dimension=2, byte_order=1))
    arrays.append(pa.array(wkb, type=pa.binary(), from_pandas=False)); schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": CRS_4326}}}
    schema = pa.schema(schema_fields, metadata={b"geo": _json(geo).strip()})
    table = pa.Table.from_arrays(arrays, schema=schema)
    return table, sorted(set(quality))


def _schema_identity(metadata: dict) -> dict:
    return {"fields": metadata.get("fields"), "objectIdField": metadata.get("objectIdField"),
            "geometryType": metadata.get("geometryType"), "spatialReference": metadata.get("sourceSpatialReference") or metadata.get("spatialReference"),
            "maxRecordCount": metadata.get("maxRecordCount"), "supportsPagination": metadata.get("advancedQueryCapabilities", {}).get("supportsPagination")}


def _validate_layer_schema(metadata: dict) -> None:
    fields = metadata.get("fields")
    if type(fields) is not list:
        raise USACEOfficesSourceError("source_field_declarations_missing")
    actual = tuple((field.get("name"), field.get("type"), field.get("nullable"), field.get("length")) for field in fields)
    if actual != EXPECTED_FIELDS:
        raise USACEOfficesSourceError("source_field_schema_drift")
    sr = metadata.get("sourceSpatialReference") or metadata.get("spatialReference") or metadata.get("extent", {}).get("spatialReference")
    if not isinstance(sr, dict) or sr.get("latestWkid", sr.get("wkid")) != 4326:
        raise USACEOfficesSourceError("source_spatial_reference_drift")


def _source_contract(metadata: dict, count_before: int, count_after: int, before_ids: list[int], after_ids: list[int], features: list) -> dict:
    object_id = metadata.get("objectIdField")
    names = tuple(f["name"] for f in metadata.get("fields", []))
    if object_id not in names or metadata.get("geometryType") != "esriGeometryPoint":
        raise USACEOfficesSourceError("source_identity_or_geometry_contract_changed")
    quality = []
    if count_before != count_after or count_before != len(before_ids) or count_after != len(after_ids): quality.append("source_count_or_id_roster_changed_during_capture")
    if before_ids != after_ids: quality.append("source_id_roster_changed_during_capture")
    if len(features) != count_before: quality.append("source_full_feature_count_mismatch")
    if count_before > MAX_COUNT: quality.append("source_count_exceeds_adapter_limit")
    quality.extend(_validate_rows(features, names, object_id, before_ids))
    return {"count_before": count_before, "count_after": count_after, "feature_count": len(features),
            "object_id_field": object_id, "id_roster_sha256": hashlib.sha256(_json(before_ids)).hexdigest(),
            "id_roster_preserved": before_ids == after_ids, "source_release_date": None,
            "release_date_reason": "USACE layer has no editFieldsInfo/source timestamp; fetch time is retained separately",
            "quality_errors": sorted(set(quality))}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _code_identity() -> dict:
    root = _project_root()
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        commit = "unresolved"
    processing_paths = (
        "src/dagster_hifld/sources/usace_offices.py",
        "src/dagster_hifld/assets/usace_offices.py",
        "src/dagster_hifld/assets/ingest_registry.py",
        "src/dagster_hifld/checks/__init__.py",
        "src/dagster_hifld/construction_state.py",
        "src/dagster_hifld/construction_provenance.py",
        "src/dagster_hifld/http_capture.py",
        "src/dagster_hifld/resources.py",
        "src/dagster_hifld/validation.py",
        "src/dagster_hifld/definitions.py",
        "pyproject.toml",
        "uv.lock",
    )
    processing_files = {path: sha256_file(root / path) for path in processing_paths}
    validate_construction_code_identity(root, commit, processing_files)
    return {"commit": commit, "processing_files": processing_files,
            "python": platform.python_version(), "platform": platform.platform()}


def _write_private(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        if os.write(fd, body) != len(body):
            raise OSError("short_private_candidate_write")
        os.fsync(fd)
    finally:
        os.close(fd)
    if path.read_bytes() != body:
        raise USACEOfficesSourceError("private_candidate_readback_mismatch")


def _local_staging(storage: StagingStorageResource, capture_dir: str | None) -> Path:
    if isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise USACEOfficesSourceError("isolated_local_staging_required")
    root = Path(storage.local_dir).absolute()
    configured_capture = capture_dir or os.environ.get(CAPTURE_ENV)
    capture = Path(configured_capture).absolute() if configured_capture else root / "captures"
    for path in (root, capture):
        if path == Path(".").absolute():
            raise USACEOfficesSourceError("invalid_local_storage_path")
        for parent in [path, *path.parents]:
            if parent.exists() and parent.is_symlink():
                raise USACEOfficesSourceError("symlink_storage_path_refused")
    root.mkdir(parents=True, exist_ok=True); capture.mkdir(parents=True, exist_ok=True)
    return capture


def stage_usace_offices(storage: StagingStorageResource, *, version: str, capture_dir: str | None = None) -> dict:
    """Capture, double-check, retain and stage all rows as a native-typed draft."""
    capture = _local_staging(storage, capture_dir)
    version_prefix = f"{SLUG}/{SLUG}/{version}"
    ready_key = f"{version_prefix}/metadata/candidate_ready.json"
    if storage.object_snapshot(ready_key) is not None:
        ready = _strict_json(storage.read_key(ready_key))
        objects = ready.get("objects")
        if ready.get("version") != version or ready.get("approved") is not False or not isinstance(objects, dict):
            raise USACEOfficesSourceError("cached_candidate_ready_identity_invalid")
        for relative, expected in objects.items():
            key = f"{version_prefix}/{relative}"
            snapshot = storage.object_snapshot(key)
            if (not isinstance(expected, dict) or snapshot is None or snapshot.size != expected.get("bytes")
                    or storage.sha256_key(key) != expected.get("sha256") or storage.object_snapshot(key) != snapshot):
                raise USACEOfficesSourceError("cached_candidate_bytes_changed:" + str(relative))
        # A mutable READY marker is not sufficient authority for reuse. Recheck
        # the immutable first-construction record and its full raw/native roster.
        identity = _strict_json(storage.read_key(f"{version_prefix}/metadata/processing_identity.json"))
        validate_construction_code_identity(_project_root(), identity.get("commit"), identity.get("processing_files"))
        fingerprint = hashlib.sha256(_json(identity)).hexdigest()
        state_key = f"_construction-state/{SLUG}/{version}.json"
        sealed_bytes = load_first_construction(storage, state_key, version, fingerprint)
        sealed = json.loads(sealed_bytes)
        sealed_payload = sealed.get("payload", {})
        audit = _strict_json(storage.read_key(f"{version_prefix}/metadata/source_audit.json"))
        receipts = sealed_payload.get("capture_receipts", {})
        raw_roster_ok = isinstance(receipts, dict) and bool(receipts)
        if raw_roster_ok:
            for name, receipt in receipts.items():
                expected_names = (f"raw/{name}.wire", f"raw/{name}.decoded", f"raw/{name}.receipt.json")
                if any(item not in objects for item in expected_names) or not isinstance(receipt, dict):
                    raw_roster_ok = False
                    break
                decoded = receipt.get("decoded", {})
                wire = receipt.get("wire", {})
                if (objects[expected_names[0]].get("sha256") != wire.get("sha256")
                        or objects[expected_names[1]].get("sha256") != decoded.get("sha256")
                        or objects[expected_names[2]].get("sha256") != hashlib.sha256(_json(receipt)).hexdigest()):
                    raw_roster_ok = False
                    break
        if (sealed_payload.get("objects") != objects
                or sealed_payload.get("id_roster_sha256") != audit.get("id_roster_sha256")
                or not raw_roster_ok
                or f"geoparquet/{SLUG}.parquet" not in objects):
            raise USACEOfficesSourceError("cached_candidate_first_construction_or_raw_roster_mismatch")
        return {"version": version, "rows": ready.get("source_row_count"), "quality_passed": ready.get("quality_passed"),
            "quality_errors": ready.get("quality_errors", []), "source_release_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "candidate_ready": ready_key, "cached_reuse": True}
    if storage.list_keys(SLUG, SLUG, version):
        raise USACEOfficesSourceError("incomplete_existing_candidate_requires_review")
    run_capture = capture / version
    run_capture.mkdir(mode=0o700, exist_ok=False)
    captures: dict[str, tuple[bytes, dict]] = {}
    def get(client, name, url):
        body, receipt = _capture(client, url, run_capture, name)
        captures[name] = (body, receipt)
        return body

    with httpx.Client(timeout=45, follow_redirects=True, headers={"Accept-Encoding": "identity"}) as client:
        layer_before = _strict_json(get(client, "layer_before", BASE + "?f=pjson"))
        _validate_layer_schema(layer_before)
        count_before = _strict_json(get(client, "count_before", _query_url(where="1=1", returnCountOnly="true"))).get("count")
        ids_field_before, ids_before = _object_ids(get(client, "ids_before", _query_url(where="1=1", returnIdsOnly="true")))
        if type(count_before) is not int or count_before > MAX_COUNT or ids_field_before != layer_before.get("objectIdField"):
            raise USACEOfficesSourceError("initial_count_or_id_probe_invalid")
        features = []
        batch_size = min(500, max(1, int(layer_before.get("maxRecordCount", 500))))
        for index in range(0, len(ids_before), batch_size):
            page_ids = ids_before[index:index + batch_size]
            url = _query_url(objectIds=",".join(map(str, page_ids)), outFields="*", returnGeometry="true", returnZ="true", returnM="true", orderByFields=layer_before["objectIdField"])
            page = _strict_json(get(client, f"features_{index:05d}", url))
            if page.get("exceededTransferLimit") is True:
                raise USACEOfficesSourceError("source_page_exceeded_transfer_limit")
            if tuple(f.get("name") for f in page.get("fields", [])) != tuple(f.get("name") for f in layer_before.get("fields", [])):
                raise USACEOfficesSourceError("query_field_roster_changed")
            features.extend(page.get("features", []))
        layer_after = _strict_json(get(client, "layer_after", BASE + "?f=pjson"))
        _validate_layer_schema(layer_after)
        count_after = _strict_json(get(client, "count_after", _query_url(where="1=1", returnCountOnly="true"))).get("count")
        ids_field_after, ids_after = _object_ids(get(client, "ids_after", _query_url(where="1=1", returnIdsOnly="true")))

    schema_drift = _schema_identity(layer_before) != _schema_identity(layer_after)
    if schema_drift or ids_field_after != layer_after.get("objectIdField"):
        raise USACEOfficesSourceError("source_metadata_drift_during_capture")
    audit = _source_contract(layer_before, count_before, count_after, ids_before, ids_after, features)
    table, geometry_quality = _table(layer_before, features)
    audit["quality_errors"] = sorted(set(audit["quality_errors"] + geometry_quality))
    audit["source_schema_sha256"] = hashlib.sha256(_json(_schema_identity(layer_before))).hexdigest()
    audit["fields"] = _schema_identity(layer_before)["fields"]
    audit["native_geometry"] = {"geometry_type": "Point", "wkid": 4326, "extent": layer_before.get("extent"), "count": len(features)}
    audit["captured_at_utc"] = datetime.now(timezone.utc).isoformat()
    identity = _code_identity()
    fingerprint = hashlib.sha256(_json(identity)).hexdigest()
    with tempfile.TemporaryDirectory(prefix="eoc-candidate-", dir=capture) as temp:
        candidate = Path(temp)
        parquet = candidate / "geoparquet" / f"{SLUG}.parquet"
        parquet.parent.mkdir()
        pq.write_table(table, parquet, compression="zstd", version="2.6", write_statistics=True)
        readback = pq.read_table(parquet)
        if not readback.equals(table, check_metadata=True) or readback.num_rows != len(features):
            raise USACEOfficesSourceError("native_parquet_roundtrip_mismatch")
        objects = {}
        payloads = {
            f"geoparquet/{SLUG}.parquet": parquet.read_bytes(),
            "metadata/source_audit.json": _json(audit),
            "metadata/processing_identity.json": _json(identity),
        }
        for name, (body, receipt) in captures.items():
            payloads[f"raw/{name}.wire"] = (run_capture / f"{name}.wire").read_bytes()
            payloads[f"raw/{name}.decoded"] = body
            payloads[f"raw/{name}.receipt.json"] = _json(receipt)
        parquet_descriptor = {"path": f"geoparquet/{SLUG}.parquet", "bytes": len(payloads[f"geoparquet/{SLUG}.parquet"]), "sha256": hashlib.sha256(payloads[f"geoparquet/{SLUG}.parquet"]).hexdigest()}
        provenance_inputs = []
        for name, (_body, receipt) in sorted(captures.items()):
            decoded_info = receipt["decoded"]
            provenance_inputs.append({"input_id": "usace-offices-" + name, "producer": "U.S. Army Corps of Engineers",
                "host": "services7.arcgis.com", "canonical_url": BASE, "retrieval_url": receipt["url"],
                "retrieved_at": receipt["retrieved_at"], "sha256": decoded_info["sha256"], "bytes": decoded_info["bytes"],
                "license": "USACE layer does not state a license; no rights acceptance is inferred.",
                "attribution": "USACE metadata copyright text: USACE ArcGIS Online.", "role": "producer_publication", "http_status": receipt["status"],
                "release_date": None, "release_date_unknown_reason": "The USACE layer has no editFieldsInfo/source timestamp or producer release-date field."})
        manifest = {"schema_version": 1, "title": "USACE District, Division and Research Lab Locations", "description": layer_before.get("description", ""),
            "publisher": "U.S. Army Corps of Engineers", "agency": "U.S. Army Corps of Engineers",
            "source_url": BASE, "license": "USACE layer metadata does not state a license; no rights acceptance is inferred.",
            "source_modified": None, "date_issued": None, "date_modified": None,
            "processor": "HIFLD Next", "code_commit": identity["commit"],
            "inputs": provenance_inputs,
            "transformations": ["Preserve every USACE layer attribute in declared order; encode source x/y point as WKB without reprojection. Proposed and unapproved mapping: ArcGIS base64 Blob values become Arrow binary; exact source JSON is retained unchanged."],
            "lineage": "USACE ArcGIS Online organization-hosted candidate matching the HIFLD description; exact official publication delegation and DHS/ICPSR crosswalk remain unverified.",
            "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Native source types and WGS84 point geometry were explicitly staged; draft only, generic regeneration disabled.", "objects": [parquet_descriptor]}}}}
        payloads["metadata/source_manifest.json"] = _json(manifest)
        for name, body in payloads.items():
            if name != f"geoparquet/{SLUG}.parquet":
                _write_private(candidate / name, body)
            obj = {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
            objects[name] = obj
        ready = {"version": version, "approved": False, "source_release_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False, "publication_approved": False,
            "source_row_count": len(features), "quality_passed": not audit["quality_errors"], "quality_errors": audit["quality_errors"],
            "object_count": len(objects), "objects": objects}
        _write_private(candidate / "metadata/candidate_ready.json", _json(ready))
        # Complete private candidate and round-trip proof exist before first-construction sealing or staging.
        candidate_files = sorted(p.relative_to(candidate).as_posix() for p in candidate.rglob("*") if p.is_file())
        if len(candidate_files) != len(objects) + 1:
            raise USACEOfficesSourceError("private_candidate_roster_incomplete")
        for relative, expected in objects.items():
            private_bytes = (candidate / relative).read_bytes()
            if len(private_bytes) != expected["bytes"] or hashlib.sha256(private_bytes).hexdigest() != expected["sha256"]:
                raise USACEOfficesSourceError("private_candidate_object_readback_mismatch:" + relative)
        if (pq.read_table(parquet).num_rows != len(features)
                or hashlib.sha256(parquet.read_bytes()).hexdigest() != parquet_descriptor["sha256"]):
            raise USACEOfficesSourceError("private_native_candidate_identity_mismatch")
        bundle = canonical_construction_bundle(version, fingerprint, {"source_schema_sha256": audit["source_schema_sha256"],
            "id_roster_sha256": audit["id_roster_sha256"], "capture_receipts": {name: receipt for name, (_, receipt) in captures.items()}, "objects": objects})
        state_key = f"_construction-state/{SLUG}/{version}.json"
        sealed = seal_first_construction(storage, state_key, version, fingerprint, bundle)
        if sealed != bundle:
            original = json.loads(load_first_construction(storage, state_key, version, fingerprint))
            if original.get("payload", {}).get("id_roster_sha256") != audit["id_roster_sha256"]:
                raise USACEOfficesSourceError("first_construction_source_roster_changed")
        for name, body in payloads.items():
            key = f"{version_prefix}/{name}"
            snapshot = storage.object_snapshot(key)
            if snapshot is None:
                storage.write_key_if_unchanged(key, body, None)
            elif storage.read_key(key) != body or storage.object_snapshot(key) != snapshot:
                raise USACEOfficesSourceError("immutable_staged_object_changed:" + name)
        ready_key = f"{version_prefix}/metadata/candidate_ready.json"
        if storage.object_snapshot(ready_key) is None:
            storage.write_key_if_unchanged(ready_key, _json(ready), None)
        elif storage.read_key(ready_key) != _json(ready):
            raise USACEOfficesSourceError("candidate_ready_state_changed")
    return {"version": version, "rows": len(features), "quality_passed": not audit["quality_errors"],
        "quality_errors": audit["quality_errors"], "source_release_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "candidate_ready": f"{version_prefix}/metadata/candidate_ready.json", "raw_capture_dir": str(run_capture)}


def source_quality_result(storage: StagingStorageResource, version: str) -> tuple[bool, dict]:
    key = f"{SLUG}/{SLUG}/{version}/metadata/source_audit.json"
    audit = _strict_json(storage.read_key(key))
    errors = audit.get("quality_errors", [])
    return not errors, {"row_count": audit.get("feature_count"), "quality_errors": errors, "source_release_accepted": False}



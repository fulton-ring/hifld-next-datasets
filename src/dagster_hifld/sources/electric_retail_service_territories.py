"""Immutable preservation capture of the frozen 2025 HIFLD retail-territory copy."""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import MultiPolygon, Polygon
from shapely.wkb import dumps as to_wkb

from dagster_hifld.http_capture import CaptureProgress, capture_http_response
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import processing_identity, write_immutable

DATASET_SLUG = "electric-retail-service-territories"
FILE_SLUG = DATASET_SLUG
EXPECTED_COUNT = 2931
EXPECTED_ID_SHA256 = "e1038f98577ee9a103ce0d390e47ddb896ea1f4775ff89f90b6d7d264443e5e4"
EXPECTED_SCHEMA_SHA256 = "9a6ff3ef2ca1fe63f0dd39df8d69d8ff3568fca08db5be209c99df0d6202f69d"
PAGE_SIZE = 100
SOURCE_EDIT_EPOCH_MS = 1755786120272  # 2025-08-21; service snapshot/edit timestamp, not release date.
FEATURE_LAYER_URL = (
    "https://services3.arcgis.com/OYP7N6mAJJCyH6hd/arcgis/rest/services/"
    "Electric_Retail_Service_Territories_HIFLD/FeatureServer/0"
)
FEATURE_SERVICE_URL = FEATURE_LAYER_URL.rsplit("/0", 1)[0]
ITEM_ID = "597555ce8e4a4892a030784a7c657fdd"
ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=pjson"
OWNER_URL = "https://www.arcgis.com/sharing/rest/community/users/NBAM_Org?f=pjson"
EXPECTED_COPY_NOTICE = "downloaded from Homeland Infrastructure Foundation-Level Data (HIFLD) on 8/21/2025 and will not be updated"
EXPECTED_COPYRIGHT = "Oak Ridge National Laboratory (ORNL), DOE - Office of Cybersecurity, Energy Security, and Emergency Response (CESER)"
MAX_RESPONSE_BYTES = 64_000_000
MAX_CAPTURE_BYTES = 768_000_000
DISK_FLOOR_BYTES = 300_000_000
_STRING_TYPES = {"esriFieldTypeString", "esriFieldTypeGUID", "esriFieldTypeGlobalID"}


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                       allow_nan=False) + "\n").encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _safe_local_storage(storage: StagingStorageResource) -> Path:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    root = Path(storage.local_dir).absolute()
    if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
        raise ValueError("unsafe_local_staging_root")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _capture_request(client: httpx.Client | None, root: Path, name: str, method: str,
                     url: str, params: dict[str, object], budget: list[int],
                     replay: bool, captures: list[dict[str, object]]) -> tuple[dict, bytes, bytes, bytes | None, dict]:
    """Capture or exactly replay one request, including POST form bytes and partial failures."""
    body_path, wire_path = root / f"{name}.body", root / f"{name}.wire"
    receipt_path, request_path = root / f"{name}.receipt.json", root / f"{name}.request.body"
    expected_request = urlencode(params).encode("ascii") if method == "POST" else None
    expected_url = url + ("?" + urlencode(params) if method == "GET" and params else "")
    replay_paths = (body_path, wire_path, receipt_path, *((request_path,) if method == "POST" else ()))
    if replay and all(path.exists() for path in replay_paths):
        body, wire = body_path.read_bytes(), wire_path.read_bytes()
        receipt = json.loads(receipt_path.read_bytes())
        request = request_path.read_bytes() if method == "POST" else None
        if (receipt.get("method") != method or receipt.get("url") != expected_url
                or receipt.get("final_url") != expected_url or receipt.get("complete_body") is not True
                or receipt.get("status") != 200 or receipt.get("sha256") != _sha(body)
                or receipt.get("bytes") != len(body) or receipt.get("wire_sha256") != _sha(wire)
                or receipt.get("wire_bytes") != len(wire)
                or request != expected_request
                or receipt.get("request_body_sha256") != (_sha(request) if request is not None else None)
                or receipt.get("request_body_bytes") != (len(request) if request is not None else 0)):
            raise ValueError("retained_capture_request_or_receipt_mismatch:" + name)
        budget[0] += len(body) + len(wire)
        if budget[0] > MAX_CAPTURE_BYTES:
            raise ValueError("source_capture_budget_exceeded")
        value = json.loads(body)
        if not isinstance(value, dict) or "error" in value:
            raise ValueError("invalid_arcgis_response:" + name)
        captures.append({"name": name, "receipt": receipt})
        return value, body, wire, request, receipt

    if any(path.exists() for path in (body_path, wire_path, receipt_path, request_path)):
        raise ValueError("capture_target_already_exists:" + name)
    observed = datetime.now(timezone.utc).isoformat()
    wire_hash, body_hash = hashlib.sha256(), hashlib.sha256()
    wire_size = body_size = 0
    body_path.touch(); wire_path.touch()

    def reserve(progress: CaptureProgress) -> None:
        if budget[0] + progress.wire_bytes_retained + progress.decoded_bytes_retained + progress.reserve_bytes > MAX_CAPTURE_BYTES:
            raise ValueError("source_capture_budget_exceeded")
        if shutil.disk_usage(root).free < DISK_FLOOR_BYTES:
            raise ValueError("source_capture_disk_floor")

    def retain(path: Path, data: bytes, kind: str) -> None:
        nonlocal wire_size, body_size
        with path.open("ab") as stream:
            stream.write(data)
        if kind == "wire":
            wire_size += len(data); wire_hash.update(data)
        else:
            body_size += len(data); body_hash.update(data)

    final_url = None
    status = None
    complete = False
    error_code = "request_error"
    error_type = None
    encoding = None
    content_length = None
    request_bytes = expected_request or b""
    if expected_request is not None:
        request_path.write_bytes(expected_request)
    try:
        if client is None:
            raise ValueError("http_client_missing")
        with client.stream(method, url, params=params if method == "GET" and params else None,
                           data=params if method == "POST" else None,
                           headers={"Accept-Encoding": "identity"}) as response:
            result = capture_http_response(
                response, max_wire_bytes=MAX_RESPONSE_BYTES, max_decoded_bytes=MAX_RESPONSE_BYTES,
                reserve=reserve, retain_wire=lambda data: retain(wire_path, data, "wire"),
                retain_decoded=lambda data: retain(body_path, data, "body"))
            final_url, status, complete = str(response.url), result.status_code, result.complete
            error_code, error_type = result.error_code, result.error_type
            encoding, content_length = result.content_encoding, result.content_length
    except Exception as exc:
        error_type = type(exc).__name__
    budget[0] += wire_size + body_size
    receipt = {
        "method": method, "url": expected_url, "final_url": final_url,
        "request_body_bytes": len(request_bytes) if method == "POST" else 0,
        "request_body_sha256": _sha(request_bytes) if method == "POST" else None,
        "observed_at": observed, "status": status, "complete_body": complete,
        "wire_bytes": wire_size, "wire_sha256": wire_hash.hexdigest(),
        "bytes": body_size, "sha256": body_hash.hexdigest(),
        "content_encoding": encoding, "content_length": content_length,
        "error_code": error_code, "error_type": error_type,
    }
    receipt_path.write_bytes(_canonical(receipt))
    wire, body = wire_path.read_bytes(), body_path.read_bytes()
    if (len(wire) != wire_size or _sha(wire) != receipt["wire_sha256"]
            or len(body) != body_size or _sha(body) != receipt["sha256"]):
        raise ValueError("capture_readback_mismatch:" + name)
    captures.append({"name": name, "receipt": receipt})
    if not complete or status != 200 or final_url != expected_url:
        raise ValueError("incomplete_source_capture:" + name)
    value = json.loads(body)
    if not isinstance(value, dict) or "error" in value:
        raise ValueError("invalid_arcgis_response:" + name)
    return value, body, wire, expected_request, receipt


def _validate_crs(value: object, reason: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(reason)
    codes = [value[k] for k in ("wkid", "latestWkid") if k in value]
    if not codes or any(type(code) is not int or code not in (102100, 3857) for code in codes):
        raise ValueError(reason)


def _validate_source_item(item: dict, owner: dict) -> None:
    if (item.get("id") != ITEM_ID or item.get("owner") != "NBAM_Org"
            or item.get("title") != "Electric Retail Service Territories"
            or item.get("type") != "Feature Service" or item.get("access") != "public"
            or item.get("url") != FEATURE_SERVICE_URL or owner.get("username") != "NBAM_Org"
            or owner.get("id") != item.get("ownerId")):
        raise ValueError("source_distribution_item_identity_drift")
    if EXPECTED_COPY_NOTICE not in item.get("description", ""):
        raise ValueError("source_frozen_edition_notice_missing")
    if item.get("licenseInfo") != "None (Public Use). Users are advised to read the data set's metadata thoroughly to understand appropriate use and data limitations.":
        raise ValueError("source_distribution_rights_notice_drift")
    if item.get("accessInformation") != EXPECTED_COPYRIGHT:
        raise ValueError("source_provider_attribution_drift")


def _field_signature(fields: object) -> list[tuple[str, str, int | None]]:
    if not isinstance(fields, list) or not fields or any(not isinstance(field, dict) for field in fields):
        raise ValueError("source_schema_missing")
    result = []
    for field in fields:
        name, kind, length = field.get("name"), field.get("type"), field.get("length")
        if not isinstance(name, str) or not isinstance(kind, str) or (length is not None and type(length) is not int):
            raise ValueError("source_schema_invalid")
        result.append((name, kind, length))
    if len({field[0] for field in result}) != len(result):
        raise ValueError("source_schema_invalid")
    return result


def _validate_layer(layer: dict, expected_signature: list[tuple[str, str, int | None]] | None = None) -> list[tuple[str, str, int | None]]:
    if (layer.get("id") != 0 or layer.get("name") != "Electric_Retail_Service_Territories_HIFLD"
            or layer.get("geometryType") != "esriGeometryPolygon"
            or layer.get("objectIdField") != "OBJECTID"
            or layer.get("dataSourceType") not in (None, "")
            or layer.get("editingInfo", {}).get("dataLastEditDate") != SOURCE_EDIT_EPOCH_MS
            or layer.get("hasZ") is not False or layer.get("hasM") is not False):
        raise ValueError("source_layer_identity_or_edition_drift")
    if (not isinstance(layer.get("advancedQueryCapabilities"), dict)
            or layer["advancedQueryCapabilities"].get("supportsPagination") is not True):
        raise ValueError("source_pagination_contract_drift")
    _validate_crs(layer.get("spatialReference"), "source_crs_drift")
    _validate_crs(layer.get("extent", {}).get("spatialReference"), "source_extent_crs_drift")
    signature = _field_signature(layer.get("fields"))
    if expected_signature is not None and signature != expected_signature:
        raise ValueError("source_schema_drift")
    return signature


def _ring_area(ring: list[list[float]]) -> float:
    # Translate to the first vertex before shoelace accumulation to avoid
    # cancellation for small polygons at large projected coordinates.
    origin_x, origin_y = ring[0]
    return sum((ring[i][0] - origin_x) * (ring[(i + 1) % len(ring)][1] - origin_y)
               - (ring[(i + 1) % len(ring)][0] - origin_x) * (ring[i][1] - origin_y)
               for i in range(len(ring) - 1)) / 2


def _polygon_from_esri(geometry: dict):
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    exteriors, holes, collapsed = [], [], []
    for ring in rings:
        if (not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]
                or any(not isinstance(point, list) or len(point) != 2
                       or any(type(value) not in (int, float) or not math.isfinite(value) for value in point)
                       for point in ring)):
            raise ValueError("source_polygon_ring_invalid")
        area = _ring_area(ring)
        if area == 0:
            collapsed.append(ring)
            continue
        # Esri JSON specifies clockwise shells and counter-clockwise holes.
        (exteriors if area < 0 else holes).append(ring)
    if not exteriors and not collapsed:
        raise ValueError("source_polygon_shell_missing")
    shell_polys = [Polygon(ring) for ring in exteriors]
    assignments: list[list[list[list[float]]]] = [[] for _ in exteriors]
    for hole in holes:
        point = Polygon(hole).representative_point()
        containers = [i for i, shell in enumerate(shell_polys) if shell.contains(point)]
        if not containers:
            raise ValueError("source_polygon_orphan_hole")
        assignments[min(containers, key=lambda i: shell_polys[i].area)].append(hole)
    polygons = [Polygon(shell, assigned) for shell, assigned in zip(exteriors, assignments, strict=True)]
    # Keep collapsed source rings as explicit zero-area polygon parts. They make
    # the derived geometry invalid and fail quality checks; they are never repaired or dropped.
    polygons.extend(Polygon(ring) for ring in collapsed)
    result = polygons[0] if len(polygons) == 1 else MultiPolygon(polygons)
    if result.is_empty:
        raise ValueError("source_polygon_empty")
    return result


def _validate_feature(feature: object, expected_id: int, fields: list[tuple[str, str, int | None]]):
    if not isinstance(feature, dict):
        raise ValueError("source_feature_shape_invalid")
    attrs, geometry = feature.get("attributes"), feature.get("geometry")
    expected = {name for name, kind, _length in fields if kind != "esriFieldTypeGeometry"}
    if not isinstance(attrs, dict) or set(attrs) != expected:
        raise ValueError("source_attribute_roster_drift")
    if type(attrs.get("OBJECTID")) is not int or attrs["OBJECTID"] != expected_id:
        raise ValueError("source_feature_id_or_order_drift")
    for name, kind, _length in fields:
        value = attrs.get(name)
        if value is None:
            continue
        if kind in _STRING_TYPES and not isinstance(value, str):
            raise ValueError("source_attribute_type_drift:" + name)
        if kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger", "esriFieldTypeDate"} and type(value) is not int:
            raise ValueError("source_attribute_type_drift:" + name)
        if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"} and type(value) not in (int, float):
            raise ValueError("source_attribute_type_drift:" + name)
    if not isinstance(geometry, dict) or any(k in geometry for k in ("z", "m")):
        raise ValueError("source_polygon_geometry_invalid")
    _validate_crs(geometry.get("spatialReference", {"wkid": 102100}), "source_geometry_crs_drift")
    polygon = _polygon_from_esri(geometry)
    return attrs, polygon


def _arrow_type(kind: str):
    if kind in _STRING_TYPES:
        return pa.string()
    if kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger", "esriFieldTypeDate"}:
        return pa.int64()
    if kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
        return pa.float64()
    raise ValueError("unsupported_esri_field_type:" + kind)


def _make_geoparquet(features: list[tuple[dict, object]], fields: list[tuple[str, str, int | None]]) -> bytes:
    arrays = {}
    for name, kind, _length in fields:
        if kind == "esriFieldTypeGeometry":
            continue
        arrays[name] = pa.array([attrs[name] for attrs, _geometry in features], type=_arrow_type(kind))
    arrays["geometry"] = pa.array([to_wkb(geometry, byte_order=1, output_dimension=2)
                                   for _attrs, geometry in features], type=pa.binary())
    table = pa.table(arrays)
    metadata = dict(table.schema.metadata or {})
    geom_types = sorted({geometry.geom_type for _attrs, geometry in features})
    metadata[b"geo"] = _canonical({
        "version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": geom_types,
                                    "crs": pyproj.CRS.from_epsg(3857).to_json_dict()}},
    }).strip()
    sink = pa.BufferOutputStream()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", version="2.6")
    return sink.getvalue().to_pybytes()


def _persist_attempt(storage: StagingStorageResource, root: Path, dataset_slug: str,
                     version: str, files: list[dict[str, object]], reason: str | None = None) -> str:
    attempt = uuid.uuid4().hex
    base = f"_capture-attempts/{dataset_slug}/{attempt}"
    for name in sorted({str(item["name"]) for item in files}):
        for suffix in ("body", "wire", "request.body", "receipt.json"):
            path = root / f"{name}.{suffix}"
            if path.is_file():
                write_immutable(storage, f"{base}/raw/{name}.{suffix}", path.read_bytes())
    doc = {"dataset_slug": dataset_slug, "candidate_version": version or None,
           "captured_at": datetime.now(timezone.utc).isoformat(),
           "request_names": sorted({str(item["name"]) for item in files}),
           "receipts": {str(item["name"]): item["receipt"] for item in files},
           "error_code": reason, "approved": False}
    write_immutable(storage, f"{base}/metadata/capture_attempt.json", _canonical(doc))
    return base


def capture_electric_retail_service_territories(
    storage: StagingStorageResource, *, capture_directory: str | None = None,
    client: httpx.Client | None = None,
) -> dict[str, object]:
    """Capture and stage an immutable 2025 snapshot; this function is not a live refresh."""
    root_storage = _safe_local_storage(storage)
    if shutil.disk_usage(root_storage).free < DISK_FLOOR_BYTES:
        raise ValueError("staging_disk_floor_before_acquisition")
    root = Path(capture_directory).absolute() if capture_directory else Path("/tmp/hifld-63-captures") / uuid.uuid4().hex
    if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    replay = bool(capture_directory)
    if replay:
        if not root.is_dir() or not (root / "layer_before.body").is_file():
            raise ValueError("replay_capture_directory_missing")
    else:
        if root.exists():
            raise ValueError("capture_target_already_exists")
        root.mkdir(parents=True)
    captures: list[dict[str, object]] = []
    budget = [0]
    owned_client = client is None
    client = client or (httpx.Client(timeout=httpx.Timeout(90, connect=15), follow_redirects=False,
                                     headers={"User-Agent": "hifld-next-datasets/electric-retail-63-preservation"})
                       if not replay else httpx.Client(timeout=httpx.Timeout(90, connect=15), follow_redirects=False,
                                                       headers={"User-Agent": "hifld-next-datasets/electric-retail-63-preservation"}))
    candidate_version = ""
    try:
        item, body, wire, request_body, receipt = _capture_request(client, root, "source_item", "GET", ITEM_URL, {}, budget, replay, captures)
        owner, body, wire, request_body, receipt = _capture_request(client, root, "source_owner", "GET", OWNER_URL, {}, budget, replay, captures)
        _validate_source_item(item, owner)
        layer_params = {"f": "pjson"}
        layer, body, wire, request_body, receipt = _capture_request(client, root, "layer_before", "GET", FEATURE_LAYER_URL, layer_params, budget, replay, captures)
        fields = _validate_layer(layer)
        if _sha(_canonical(fields)) != EXPECTED_SCHEMA_SHA256:
            raise ValueError("source_schema_frozen_edition_drift")
        count_params = {"where": "1=1", "returnCountOnly": "true", "f": "json"}
        count_before, body, wire, request_body, receipt = _capture_request(client, root, "count_before", "GET", FEATURE_LAYER_URL + "/query", count_params, budget, replay, captures)
        id_params = {"where": "1=1", "returnIdsOnly": "true", "f": "json"}
        ids_before_doc, body, wire, request_body, receipt = _capture_request(client, root, "ids_before", "GET", FEATURE_LAYER_URL + "/query", id_params, budget, replay, captures)
        ids = ids_before_doc.get("objectIds")
        if (count_before.get("count") != EXPECTED_COUNT or not isinstance(ids, list)
                or any(type(value) is not int for value in ids)):
            raise ValueError("source_population_drift_before")
        ids = sorted(ids)
        if len(ids) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT:
            raise ValueError("source_id_roster_invalid")
        if _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
            raise ValueError("source_id_frozen_edition_drift")
        processed: list[tuple[dict, object]] = []
        for offset in range(0, len(ids), PAGE_SIZE):
            wanted = ids[offset:offset + PAGE_SIZE]
            name = f"page_{offset:06d}"
            params = {"objectIds": ",".join(map(str, wanted)), "outFields": "*",
                      "returnGeometry": "true", "outSR": "102100", "orderByFields": "OBJECTID ASC", "f": "json"}
            page, body, wire, request_body, receipt = _capture_request(
                client, root, name, "POST", FEATURE_LAYER_URL + "/query", params, budget, replay, captures)
            if (page.get("geometryType") != "esriGeometryPolygon"
                    or page.get("exceededTransferLimit") is True
                    or not isinstance(page.get("features"), list) or len(page["features"]) != len(wanted)):
                raise ValueError("source_page_population_drift")
            _validate_crs(page.get("spatialReference"), "source_page_crs_drift")
            page_signature = _field_signature(page.get("fields"))
            if page_signature != fields:
                raise ValueError("source_page_schema_drift")
            for feature, object_id in zip(page["features"], wanted, strict=True):
                processed.append(_validate_feature(feature, object_id, fields))
        ids_after_doc, body, wire, request_body, receipt = _capture_request(client, root, "ids_after", "GET", FEATURE_LAYER_URL + "/query", id_params, budget, replay, captures)
        count_after, body, wire, request_body, receipt = _capture_request(client, root, "count_after", "GET", FEATURE_LAYER_URL + "/query", count_params, budget, replay, captures)
        layer_after, body, wire, request_body, receipt = _capture_request(client, root, "layer_after", "GET", FEATURE_LAYER_URL, layer_params, budget, replay, captures)
        if (_validate_layer(layer_after, fields) != fields
                or count_after.get("count") != EXPECTED_COUNT
                or not isinstance(ids_after_doc.get("objectIds"), list)
                or any(type(value) is not int for value in ids_after_doc["objectIds"])
                or sorted(ids_after_doc["objectIds"]) != ids
                or count_before != count_after
                or [attrs["OBJECTID"] for attrs, _geometry in processed] != ids):
            raise ValueError("source_population_or_metadata_changed_during_capture")
        id_sha = _sha(_canonical(ids))
        schema_sha = _sha(_canonical(fields))
        record_sha = _sha(_canonical([attrs for attrs, _geometry in processed]))
        wkb_values = [to_wkb(geometry, byte_order=1, output_dimension=2) for _attrs, geometry in processed]
        geometry_hash = hashlib.sha256()
        for value in wkb_values:
            geometry_hash.update(len(value).to_bytes(8, "little"))
            geometry_hash.update(value)
        geometry_sha = geometry_hash.hexdigest()
        identity = processing_identity(Path(__file__).parents[3])
        fingerprint = _sha(_canonical({"record": record_sha, "geometry": geometry_sha,
                                       "schema": schema_sha, "source_edition": "2025-08-21",
                                       "processing_identity": identity}))
        candidate_version = "v" + fingerprint[:32]
        prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{candidate_version}"
        output = _make_geoparquet(processed, fields)
        source_manifest = {
            "schema_version": 1, "approved": False,
            "title": "Electric Retail Service Territories — frozen 2025 HIFLD snapshot",
            "publisher": "NTIA NBAM public ArcGIS distribution copy (ArcGIS owner account NBAM_Org)",
            "producer_credit": EXPECTED_COPYRIGHT,
            "source_url": FEATURE_LAYER_URL, "source_item_url": f"https://www.arcgis.com/home/item.html?id={ITEM_ID}",
            "source_archive_url": "https://www.datalumos.org/datalumos/project/239091/version/V2/view",
            "source_edition": "HIFLD snapshot downloaded by the public NBAM ArcGIS item on 2025-08-21",
            "source_modified": None,
            "source_modified_unknown_reason": "The frozen NBAM item records its download date, not an upstream producer release/effective date.",
            "source_coverage_scope": "All 2,931 object IDs observed in the frozen public NBAM copy; geographic coverage and equivalence to the HIFLD inventory remain unaccepted.",
            "source_feature_count": len(processed), "source_id_field": "OBJECTID",
            "source_id_sha256": id_sha, "source_record_sha256": record_sha,
            "source_geometry_sha256": geometry_sha, "source_schema_sha256": schema_sha,
            "source_fields": [{"name": name, "type": kind, "length": length} for name, kind, length in fields],
            "native_crs": "EPSG:3857", "geometry_type": "Polygon/MultiPolygon",
            "preservation": "All 36 ArcGIS attribute columns and OBJECTID values are retained with native EPSG:3857 coordinates; no reprojecting, field mapping, geometry repair, filtering, or row ID synthesis.",
            "rights_notice": item["licenseInfo"], "copyright_notice": item["accessInformation"],
            "source_acceptance": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False,
            "baseline": None, "release_accepted": False, "promotion_allowed": False,
            "processing_code": identity,
            "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
                "reason": "Source-faithful native-coordinate preservation candidate; frozen edition only; release not approved.",
                "objects": [{"path": "geoparquet/electric-retail-service-territories.parquet",
                             "sha256": _sha(output), "bytes": len(output)}]}}},
        }
        capture_names = [str(item["name"]) for item in captures]
        for capture_name in capture_names:
            for suffix in ("body", "wire", "request.body"):
                raw_path = root / f"{capture_name}.{suffix}"
                if raw_path.is_file():
                    write_immutable(storage, f"{prefix}/raw/{capture_name}.{suffix}", raw_path.read_bytes())
        write_immutable(storage, f"{prefix}/geoparquet/{FILE_SLUG}.parquet", output)
        write_immutable(storage, f"{prefix}/metadata/source_manifest.json", _canonical(source_manifest))
        attempt_prefix = _persist_attempt(storage, root, DATASET_SLUG, candidate_version, captures)
        native_capture = {
            "candidate_version": candidate_version, "source_edition": "2025-08-21",
            "source_item_id": ITEM_ID, "source_item_owner": "NBAM_Org",
            "source_feature_count": len(processed), "count_before": count_before["count"],
            "count_after": count_after["count"], "source_id_sha256": id_sha,
            "source_schema_sha256": schema_sha, "source_record_sha256": record_sha,
            "source_geometry_sha256": geometry_sha, "native_crs": "EPSG:3857",
            "source_edit_epoch_ms": SOURCE_EDIT_EPOCH_MS,
            "producer_release_date": None, "source_accepted": False,
            "rights_accepted": False, "coverage_accepted": False, "release_accepted": False,
        }
        write_immutable(storage, f"{prefix}/metadata/native_capture.json", _canonical(native_capture))
        output_path = f"{prefix}/geoparquet/{FILE_SLUG}.parquet"
        if storage.read_key(output_path) != output or storage.sha256_key(output_path) != _sha(output):
            raise ValueError("candidate_output_readback_failed")
        return {
            "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": candidate_version,
            "feature_count": len(processed), "source_population_complete": True,
            "source_id_sha256": id_sha, "source_schema_sha256": schema_sha,
            "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
            "null_geometry_count": 0,
            "invalid_geometry_count": sum(
                not geometry.is_valid or geometry.is_empty or geometry.area == 0
                for _attrs, geometry in processed
            ),
            "source_coverage_scope": source_manifest["source_coverage_scope"],
            "capture_attempt_manifest": attempt_prefix + "/metadata/capture_attempt.json",
            "native_crs": "EPSG:3857", "source_edition": "2025-08-21",
            "source_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False,
            "release_accepted": False,
        }
    except Exception as exc:
        if captures:
            try:
                _persist_attempt(storage, root, DATASET_SLUG, candidate_version, captures,
                                 "capture_or_candidate_failure:" + type(exc).__name__)
            except Exception:
                pass
        raise
    finally:
        if owned_client and client is not None:
            client.close()

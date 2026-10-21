"""Bounded native capture for USACE IENC depth areas.

This creates an unapproved source-faithful candidate from the official IENC
feature class. Each ArcGIS response is retained verbatim with its wire receipt.
"""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import struct
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from pyproj import CRS
from shapely.geometry import LinearRing, MultiPolygon, Polygon

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response, capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "depth-areas-usace-ienc-2"
FILE_SLUG = DATASET_SLUG
LAYER_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/DEPTH_AREA/MapServer/96"
SERVICE_URL = LAYER_URL.rsplit("/", 1)[0]
SERVICE_DIRECTORY_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson"
BTS_ITEM_ID = "7ec34864c8804468a9cf52d437fe4b6c"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
PUBLISHER = "U.S. Army Corps of Engineers, USACE IENC Feature Classes service"
EXPECTED_COUNT = 5778
EXPECTED_IDS = list(range(1, EXPECTED_COUNT + 1))
EXPECTED_ID_SHA256 = "b3f5daae6c2a30af51ca89fb68353fd6a5bf0e75b9133a594940ac430accc28e"
PAGE_SIZE = 200
PAGE_SIZE_AFTER_14 = 50
PAGE_SIZE_TRANSITION_ID = 2801
MAX_BODY_BYTES = 32_000_000
MAX_CAPTURE_BYTES = 1_200_000_000
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID"), ("SHAPE", "esriFieldTypeGeometry"),
    ("Depth_Area_Value_1", "esriFieldTypeString"),
    ("Depth_Area_Value_2", "esriFieldTypeString"),
    ("Source_Date", "esriFieldTypeString"),
    ("Source_Indication", "esriFieldTypeString"),
    ("Source_Dataset", "esriFieldTypeString"),
    ("Feature_ID", "esriFieldTypeString"),
]
EXPECTED_FIELD_DECLARATIONS = [
    {"name":name,"type":kind,"alias":name,"domain":None,
     **({"length":254} if kind == "esriFieldTypeString" else {})}
    for name,kind in EXPECTED_FIELDS
]
SOURCE_CRS = CRS.from_epsg(4326)


def _validate_spatial_reference(value: object, label: str, *, required: bool = True) -> bool:
    """Require every advertised ArcGIS CRS representation to agree with EPSG:4326."""
    if not isinstance(value, dict):
        raise ValueError(f"{label}_crs_drift")
    found = False
    for key in ("wkid", "latestWkid"):
        if key in value:
            wkid = value[key]
            if type(wkid) is not int or wkid != 4326:
                raise ValueError(f"{label}_crs_drift")
            found = True
    for key in ("wkt", "wkt2"):
        if key in value:
            wkt = value[key]
            if not isinstance(wkt, str) or not wkt.strip():
                raise ValueError(f"{label}_crs_drift")
            try:
                parsed = CRS.from_wkt(wkt)
            except Exception as exc:
                raise ValueError(f"{label}_crs_drift") from exc
            if not parsed.equals(SOURCE_CRS, ignore_axis_order=True):
                raise ValueError(f"{label}_crs_drift")
            found = True
    if required and not found:
        raise ValueError(f"{label}_crs_drift")
    return found


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _fetch(client: httpx.Client, url: str, root: Path, name: str, budget: list[int]):
    root.mkdir(parents=True, exist_ok=True)
    body_path, wire_path = root / f"{name}.body", root / f"{name}.wire"
    for path in (body_path, wire_path):
        path.unlink(missing_ok=True)
    hashes = {"body": hashlib.sha256(), "wire": hashlib.sha256()}
    sizes = {"body": 0, "wire": 0}
    budget_start=budget[0]
    observed = datetime.now(timezone.utc).isoformat()
    def retain(path, kind):
        def write(chunk):
            with path.open("ab") as stream:
                stream.write(chunk)
            hashes[kind].update(chunk); sizes[kind] += len(chunk)
            budget[0]=budget_start+sizes["wire"]+sizes["body"]
        return write
    def reserve(progress):
        if budget_start+progress.wire_bytes_retained+progress.decoded_bytes_retained+progress.reserve_bytes>MAX_CAPTURE_BYTES:
            raise ValueError("source_capture_budget_exceeded")
    response = None
    result = None
    final_url = url
    transport_error = None
    try:
        with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
            final_url = str(response.url)
            result = capture_http_response(response, max_wire_bytes=MAX_BODY_BYTES,
                max_decoded_bytes=MAX_BODY_BYTES, reserve=reserve,
                retain_wire=retain(wire_path, "wire"), retain_decoded=retain(body_path, "body"))
    except Exception as exc:
        transport_error = type(exc).__name__
    receipt = {"method":"GET", "url":url, "final_url":final_url, "observed_at":observed,
        "status":result.status_code if result is not None else (response.status_code if response is not None else None),
        "retry_after":response.headers.get("retry-after") if response is not None else None,
        "complete_body":bool(result is not None and result.complete and transport_error is None),
        "wire_bytes":sizes["wire"], "wire_sha256":hashes["wire"].hexdigest(),
        "bytes":sizes["body"], "sha256":hashes["body"].hexdigest(),
        "content_encoding":result.content_encoding if result is not None else None,
        "content_length":result.content_length if result is not None else None,
        "error_code":result.error_code if result is not None else ("transport_error" if transport_error else "capture_result_missing"),
        "error_type":result.error_type if result is not None else transport_error}
    (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    if transport_error:
        raise httpx.TransportError("capture_transport_error:" + transport_error)
    if result is None:
        raise ValueError("capture_result_missing")
    body, wire = body_path.read_bytes(), wire_path.read_bytes()
    if (not result.complete or result.status_code != 200 or final_url != url
        or len(body) != receipt["bytes"] or _sha(body) != receipt["sha256"]
        or len(wire) != receipt["wire_bytes"] or _sha(wire) != receipt["wire_sha256"]):
        raise ValueError("incomplete_source_capture:" + name)
    return body, receipt


def _fetch_retry(client: httpx.Client, url: str, root: Path, name: str, budget: list[int]):
    """Retry one transient transport/incomplete read, retaining every attempt."""
    last = None
    for attempt in range(2):
        attempt_name=f"{name}.attempt{attempt+1}"
        try:
            body,receipt=_fetch(client,url,root,attempt_name,budget)
            for suffix in ("body","wire","receipt.json"):
                (root/f"{attempt_name}.{suffix}").replace(root/f"{name}.{suffix}")
            return body,receipt
        except (httpx.TransportError,ValueError) as exc:
            last=exc
            if isinstance(exc,ValueError) and not str(exc).startswith("incomplete_source_capture:"):
                raise
            receipt_path = root / f"{attempt_name}.receipt.json"
            try:
                receipt = json.loads(receipt_path.read_bytes())
            except (OSError, ValueError):
                receipt = {}
            status = receipt.get("status")
            if status not in (None, 200, 429):
                raise
            delay = 0.25
            if status == 429:
                raw_delay = receipt.get("retry_after")
                try:
                    delay = float(raw_delay)
                except (TypeError, ValueError):
                    try:
                        retry_at = parsedate_to_datetime(str(raw_delay))
                        if retry_at.tzinfo is None:
                            retry_at = retry_at.replace(tzinfo=timezone.utc)
                        delay = max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
                    except (TypeError, ValueError, OverflowError):
                        delay = 1.0
                if not math.isfinite(delay) or delay < 0 or delay > 60:
                    raise ValueError("retry_after_exceeds_bounded_wait") from None
            time.sleep(delay)
    raise last


def _esri_polygon(geometry: dict):
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    has_z, has_m = geometry.get("hasZ", False), geometry.get("hasM", False)
    if type(has_z) is not bool or type(has_m) is not bool:
        raise ValueError("source_ordinate_flags_invalid")
    shells, holes = [], []
    dimension = None
    for coords in rings:
        if not isinstance(coords, list) or len(coords) < 4:
            raise ValueError("source_polygon_ring_invalid")
        if coords[0] != coords[-1]:
            raise ValueError("source_polygon_ring_not_closed")
        if any(not isinstance(point,list) for point in coords):
            raise ValueError("source_polygon_coordinate_invalid")
        dims={len(point) for point in coords}
        if len(dims)!=1:
            raise ValueError("source_polygon_ordinate_count_drift")
        point_dims=next(iter(dims))
        if dimension is None: dimension=point_dims
        if dimension!=point_dims: raise ValueError("source_polygon_ordinate_count_drift")
        expected_dims=2+int(has_z)+int(has_m)
        if point_dims!=expected_dims:
            raise ValueError("source_polygon_zm_flags_do_not_match_coordinates")
        xy=[point[:2] for point in coords]
        ring = LinearRing(xy)
        (holes if ring.is_ccw else shells).append([list(point) for point in coords])
    # ArcGIS encodes exterior rings clockwise and interior rings counter-clockwise.
    # Reject non-standard winding rather than repairing or guessing.
    shell_polys = [Polygon([point[:2] for point in ring]) for ring in shells]
    assignments = [[] for _ in shell_polys]
    for ring in holes:
        hole = Polygon([point[:2] for point in ring])
        owners = [i for i, shell in enumerate(shell_polys) if shell.contains(hole.representative_point())]
        if len(owners) != 1:
            raise ValueError("source_hole_ownership_ambiguous")
        assignments[owners[0]].append(ring)
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    dim_code={2:0,3:(1000 if has_z else 2000),4:3000}[dimension]
    def polygon_wkb(shell, hole_rings):
        out=bytearray(struct.pack("<BI",1,dim_code+3))
        out.extend(struct.pack("<I",1+len(hole_rings)))
        for ring in [shell,*hole_rings]:
            out.extend(struct.pack("<I",len(ring)))
            for point in ring: out.extend(struct.pack("<"+"d"*dimension,*point))
        return bytes(out)
    polygons=[polygon_wkb(shell,assignments[i]) for i,shell in enumerate(shells)]
    if len(polygons)==1: return polygons[0]
    multi=bytearray(struct.pack("<BI",1,dim_code+6));multi.extend(struct.pack("<I",len(polygons)))
    for polygon in polygons: multi.extend(polygon)
    return bytes(multi)


def _arrow_type(field: dict):
    return {"esriFieldTypeOID":pa.int64(), "esriFieldTypeInteger":pa.int32(),
            "esriFieldTypeSmallInteger":pa.int16(), "esriFieldTypeSingle":pa.float32(),
            "esriFieldTypeDouble":pa.float64(), "esriFieldTypeString":pa.string(),
            "esriFieldTypeDate":pa.int64(), "esriFieldTypeGeometry":pa.binary()}.get(field.get("type"), pa.string())


def _parquet_bytes(fields: list[dict], features: list[dict], wkid: int) -> bytes:
    properties = [f for f in fields if f["name"] != "SHAPE"]
    arrays = []
    for field in properties:
        name, dtype = field["name"], _arrow_type(field)
        values = []
        for feature in features:
            values.append(feature["attributes"][name])
        arrays.append(pa.array(values, type=dtype))
    geometries = [_esri_polygon(x["geometry"]) for x in features]
    arrays.append(pa.array(geometries, type=pa.binary()))
    names = [x["name"] for x in properties] + ["geometry"]
    table = pa.Table.from_arrays(arrays, names=names)
    crs = {"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json", "type":"GeographicCRS",
           "name":"WGS 84", "datum":{"type":"GeodeticReferenceFrame","name":"World Geodetic System 1984",
           "ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563},
           "id":{"authority":"EPSG","code":6326}},
           "coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"},{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"}]},
           "id":{"authority":"EPSG","code":wkid}}
    metadata = {b"geo":_canonical({"version":"1.1.0","primary_column":"geometry",
        "columns":{"geometry":{"encoding":"WKB","geometry_types":["MultiPolygon","Polygon"],"crs":crs}}})}
    sink = __import__("io").BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture_roster(ids: list[int]) -> list[tuple[str, str]]:
    urls = [("bts_item", BTS_ITEM_URL), ("service_directory_before", SERVICE_DIRECTORY_URL),
            ("service_before", SERVICE_URL + "?f=pjson"),
            ("layer_before", LAYER_URL + "?f=pjson"),
            ("count_before", _query({"where":"1=1","returnCountOnly":"true","f":"json"})),
            ("ids_before", _query({"where":"1=1","returnIdsOnly":"true","f":"json"}))]
    for index, page_ids in enumerate(_page_batches(ids)):
        urls.append((f"page_{index:03d}", _query({"objectIds":",".join(map(str,page_ids)),
            "outFields":"*", "returnGeometry":"true", "returnZ":"true", "returnM":"true",
            "returnTrueCurves":"true", "outSR":"4326", "f":"json"})))
    urls.extend([("layer_after", LAYER_URL + "?f=pjson"),
                 ("count_after", _query({"where":"1=1","returnCountOnly":"true","f":"json"})),
                 ("ids_after", _query({"where":"1=1","returnIdsOnly":"true","f":"json"})),
                 ("service_after", SERVICE_URL + "?f=pjson"),
                 ("service_directory_after", SERVICE_DIRECTORY_URL)])
    return urls


def _page_batches(ids: list[int]) -> list[list[int]]:
    """Keep already retained 200-ID pages and bound later large responses at 100 IDs."""
    batches=[]
    split_index=PAGE_SIZE_TRANSITION_ID-1
    for start in range(0,min(len(ids),split_index),PAGE_SIZE):
        batches.append(ids[start:start+PAGE_SIZE])
    for start in range(split_index,len(ids),PAGE_SIZE_AFTER_14):
        batches.append(ids[start:start+PAGE_SIZE_AFTER_14])
    return batches


def _load_replay(root: Path, name: str) -> tuple[bytes, dict]:
    paths=[root/f"{name}.body",root/f"{name}.wire",root/f"{name}.receipt.json"]
    if any(path.is_symlink() for path in paths): raise ValueError("retained_capture_symlink_rejected")
    body=paths[0].read_bytes(); wire=paths[1].read_bytes(); receipt=json.loads(paths[2].read_bytes())
    if (receipt.get("complete_body") is not True or receipt.get("status") != 200
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)):
        raise ValueError("retained_capture_receipt_invalid:" + name)
    return body, receipt


def _validate(captures: dict[str, tuple[bytes, dict]]):
    expected_names = {name for name,_url in _capture_roster(EXPECTED_IDS)}
    if not expected_names <= set(captures):
        raise ValueError("source_capture_roster_incomplete")
    docs = {name: json.loads(body) for name, (body, _r) in captures.items()}
    item = docs["bts_item"]
    if (item.get("id") != BTS_ITEM_ID or item.get("owner") != "USDOT_BTS"
            or item.get("type") != "PDF" or item.get("access") != "public"):
        raise ValueError("bts_ienc_item_identity_drift")
    item_text = " ".join(str(item.get(key, "")) for key in ("title", "description"))
    if ("Inland Electronic Navigational Charts" not in item_text
            or "updated bi-monthly" not in item_text
            or SERVICE_DIRECTORY_URL.split("?")[0] not in item_text
            or "USACE_IENC_Master_Service_gdb.zip" not in item_text):
        raise ValueError("bts_ienc_official_distribution_link_drift")
    directory = docs["service_directory_before"]
    if not any(service.get("name") == "IENC_Feature_Classes/DEPTH_AREA"
               and service.get("type") == "MapServer" for service in directory.get("services", [])):
        raise ValueError("official_ienc_service_directory_drift")
    if docs["service_directory_after"] != directory:
        raise ValueError("official_ienc_service_directory_changed_during_capture")
    service = docs["service_before"]
    if (service.get("mapName") != "DEPTH_AREA" or service.get("copyrightText") != "USACE IENC"
            or [(layer.get("id"), layer.get("name"), layer.get("geometryType")) for layer in service.get("layers", [])]
            != [(96, "DEPTH_AREA", "esriGeometryPolygon")]):
        raise ValueError("official_depth_area_service_drift")
    if docs["service_after"] != service:
        raise ValueError("source_service_changed_during_capture")
    layer = docs["layer_before"]
    if docs["layer_after"] != layer:
        raise ValueError("source_layer_changed_during_capture")
    fields = layer.get("fields")
    projected=[{key:f[key] for key in ("name","type","alias","length","domain","nullable") if key in f}
               for f in fields] if isinstance(fields,list) else None
    if projected != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if layer.get("name") != "DEPTH_AREA" or layer.get("id") != 96 or layer.get("geometryType") != "esriGeometryPolygon":
        raise ValueError("source_layer_identity_drift")
    if layer.get("maxRecordCount") != 2000:
        raise ValueError("source_spatial_reference_or_page_limit_drift")
    if layer.get("advancedQueryCapabilities", {}).get("supportsTrueCurve") is not True:
        raise ValueError("source_true_curve_capability_drift")
    _validate_spatial_reference(layer.get("sourceSpatialReference"), "source_layer")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer_spatial_reference")
    extent = layer.get("extent", {})
    if "spatialReference" in extent:
        _validate_spatial_reference(extent["spatialReference"], "source_layer_extent")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    before, after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if not isinstance(before,list) or before != after or before != EXPECTED_IDS or len(before) != EXPECTED_COUNT or len(set(before)) != EXPECTED_COUNT or _sha(_canonical(before)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_population_drift")
    features=[]
    pages=sorted(n for n in captures if n.startswith("page_"))
    batches=_page_batches(EXPECTED_IDS)
    if pages != [f"page_{index:03d}" for index in range(len(batches))]:
        raise ValueError("source_page_roster_incomplete")
    for index, name in enumerate(pages):
        page=docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncation")
        _validate_spatial_reference(page.get("spatialReference"), "source_page")
        rows=page.get("features")
        expected_ids = batches[index]
        if not isinstance(rows,list) or len(rows) != len(expected_ids):
            raise ValueError("source_page_population_incomplete:" + name)
        page_ids = [row.get("attributes", {}).get("OBJECTID") for row in rows
                    if isinstance(row, dict) and isinstance(row.get("attributes"), dict)]
        if page_ids != expected_ids:
            raise ValueError("source_page_id_population_drift:" + name)
        features.extend(rows)
    expected_attrs=[f["name"] for f in fields if f["name"]!="SHAPE"]
    got=[]
    feature_ids=[]
    geometry_valid=geometry_empty=0
    coordinate_dimensions=set()
    vertices=0
    for feature in features:
        attrs, geom=feature.get("attributes"), feature.get("geometry")
        if isinstance(geom,dict) and ("curveRings" in geom or "curvePaths" in geom):
            raise ValueError("source_true_curve_geometry_unsupported_without_lossless_encoding")
        if not isinstance(attrs,dict) or list(attrs)!=expected_attrs or not isinstance(geom,dict) or "rings" not in geom:
            raise ValueError("source_record_schema_or_geometry_drift")
        if set(geom)-{"rings","hasZ","hasM","spatialReference"}:
            raise ValueError("unexpected_source_geometry_ordinates")
        geometry_reference=geom.get("spatialReference")
        if geometry_reference is not None:
            _validate_spatial_reference(geometry_reference, "source_feature")
        for name,kind in EXPECTED_FIELDS:
            if name == "SHAPE":
                continue
            value=attrs.get(name)
            if value is None:
                continue
            if kind == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                raise ValueError("source_oid_value_type_drift")
            if kind == "esriFieldTypeInteger" and type(value) is not int:
                raise ValueError("source_integer_value_type_drift:" + name)
            if kind == "esriFieldTypeString" and not isinstance(value,str):
                raise ValueError("source_string_value_type_drift:" + name)
        oid=attrs.get("OBJECTID")
        feature_id=attrs.get("Feature_ID")
        if type(oid) is not int or (feature_id is not None and not isinstance(feature_id,str)):
            raise ValueError("source_native_identifier_missing_or_type_drift")
        got.append(oid); feature_ids.append(feature_id)
        for ring in geom.get("rings",[]):
            for point in ring:
                if (not isinstance(point,list) or len(point)<2
                        or any(type(value) not in (int,float) or not math.isfinite(value) for value in point)):
                    raise ValueError("source_polygon_ordinate_contract_drift")
                coordinate_dimensions.add(len(point))
                vertices += 1
        geometry=shapely.from_wkb(_esri_polygon(geom))
        geometry_empty += int(geometry.is_empty)
        geometry_valid += int(geometry.is_valid and not geometry.is_empty)
    if len(features)!=EXPECTED_COUNT or sorted(got)!=sorted(before) or len(set(got))!=EXPECTED_COUNT:
        raise ValueError("source_feature_population_incomplete")
    features.sort(key=lambda f:f["attributes"]["OBJECTID"])
    rows=[{"attributes":f["attributes"],"geometry":f["geometry"]} for f in features]
    record_sha=_sha(_canonical([f["attributes"] for f in features]))
    geometry_sha=_sha(_canonical([[f["attributes"]["OBJECTID"],f["geometry"]] for f in features]))
    schema_sha=_sha(_canonical(fields))
    return fields, features, {"source_record_sha256":record_sha,"source_geometry_sha256":geometry_sha,"source_schema_sha256":schema_sha,
        "source_id_sha256":_sha(_canonical(sorted(got))),"distinct_feature_id_count":len({value for value in feature_ids if value is not None}),
        "source_feature_count":len(features),"source_id_min":min(got),"source_id_max":max(got),
        "source_geometry_type":"esriGeometryPolygon","source_wkid":4326,
        "source_true_curve_capability":True,"true_curve_query_requested":True,
        "source_has_z":layer.get("hasZ") if type(layer.get("hasZ")) is bool else None,
        "source_has_m":layer.get("hasM") if type(layer.get("hasM")) is bool else None,
        "observed_coordinate_dimensions":sorted(coordinate_dimensions),
        "source_vertex_count":vertices,"geometry_valid_count":geometry_valid,
        "geometry_empty_count":geometry_empty,"geometry_invalid_count":len(features)-geometry_valid-geometry_empty,
        "source_coverage_scope":"Complete observed DEPTH_AREA layer in the official USACE IENC chart service; inland-chart coverage only, not a nationwide bathymetry inventory.",
        "drift_guard":"Official BTS IENC catalog link, IENC service-directory membership, service/layer identity, declared field names/types, true-curve capability and query request, advertised CRS, page response CRS and geometry, complete before/after ID roster, per-page IDs, source population, and transfer-limit status are checked."}


def _write_outputs(storage, prefix: str, outputs: dict[str,bytes]) -> dict[str,dict[str,object]]:
    descriptors={}
    for name,body in outputs.items():
        write_immutable(storage,f"{prefix}/{name}",body)
        snapshot=storage.object_snapshot(f"{prefix}/{name}")
        descriptor={"bytes":len(body),"sha256":_sha(body)}
        if snapshot is None or snapshot.size!=descriptor["bytes"] or storage.sha256_key(f"{prefix}/{name}")!=descriptor["sha256"] or storage.object_snapshot(f"{prefix}/{name}")!=snapshot:
            raise ValueError("candidate_object_readback_failed:"+name)
        descriptors[name]=descriptor
    return descriptors


def capture_usace_depth_areas(storage: StagingStorageResource, *, version: str,
                                 capture_directory: str | None = None) -> dict[str, object]:
    """Acquire a complete bounded source snapshot, or replay an explicit capture directory."""
    if not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    base=Path(storage.local_dir).absolute()
    if ".." in base.parts or any(p.is_symlink() for p in (base,*base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(base).free < 300_000_000: raise ValueError("disk_floor_before_acquisition")
    requested=Path(capture_directory).absolute() if capture_directory else None
    replay=bool(requested and (requested/"capture_manifest.json").exists())
    preexisting = bool(requested and not replay and any(requested.glob("*.body")))
    if replay:
        root=requested
    elif requested and preexisting:
        root=requested
    elif requested:
        root=requested/(version+"-"+uuid4().hex)
    else:
        root=Path("/tmp/hifld-native-reconstruction/54/captures")/(version+"-"+uuid4().hex)
    if ".." in root.parts or any(p.is_symlink() for p in (root,*root.parents)):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True,exist_ok=True)
    captures={}
    try:
        if replay:
            retained_manifest=json.loads((root/"capture_manifest.json").read_bytes())
            if retained_manifest.get("complete") is not True or not isinstance(retained_manifest.get("receipts"),dict):
                raise ValueError("retained_capture_manifest_invalid")
            roster=retained_manifest.get("artifact_roster")
            if not isinstance(roster,dict): raise ValueError("retained_capture_roster_manifest_missing")
            validate_capture_roster(root,set(roster)|{"capture_manifest.json"})
            if roster != capture_roster_digests(root,exclude={"capture_manifest.json"}):
                raise ValueError("retained_capture_artifact_digest_mismatch")
            ids_body,ids_receipt=_load_replay(root,"ids_before")
            ids=json.loads(ids_body).get("objectIds")
            if not isinstance(ids,list): raise ValueError("retained_id_roster_invalid")
            if ids != EXPECTED_IDS or _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
                raise ValueError("retained_id_population_drift")
            names=[name for name,_url in _capture_roster(ids)]
            if set(retained_manifest["receipts"])!=set(names):
                raise ValueError("retained_capture_roster_changed")
            captures={name:_load_replay(root,name) for name in names}
            if any(receipt!=retained_manifest["receipts"][name] for name,(_body,receipt) in captures.items()):
                raise ValueError("retained_capture_manifest_receipt_mismatch")
            expected_urls = dict(_capture_roster(ids))
            for name,(_body,receipt) in captures.items():
                if (receipt.get("method") != "GET" or receipt.get("url") != expected_urls[name]
                        or receipt.get("final_url") != expected_urls[name]):
                    raise ValueError("retained_capture_request_identity_mismatch:" + name)
            attempt_map=retained_manifest.get("attempts",{})
            if not isinstance(attempt_map,dict): raise ValueError("retained_capture_attempt_manifest_invalid")
            for name,receipt in attempt_map.items():
                if not isinstance(name,str) or Path(name).name!=name or "/" in name or "\\" in name:
                    raise ValueError("retained_capture_attempt_name_invalid")
                attempt_paths=[root/f"{name}.body",root/f"{name}.wire"]
                if any(path.is_symlink() for path in attempt_paths): raise ValueError("retained_capture_attempt_symlink_rejected")
                attempt_body,attempt_wire=attempt_paths[0].read_bytes(),attempt_paths[1].read_bytes()
                if (receipt.get("bytes")!=len(attempt_body) or receipt.get("sha256")!=_sha(attempt_body)
                    or receipt.get("wire_bytes")!=len(attempt_wire) or receipt.get("wire_sha256")!=_sha(attempt_wire)):
                    raise ValueError("retained_failed_attempt_bytes_changed:"+name)
        else:
            budget_root=requested if requested else root.parent
            existing_bytes=sum(p.stat().st_size for p in budget_root.rglob("*") if p.is_file() and p.suffix in {".body",".wire"})
            capture_budget=[existing_bytes]
            if existing_bytes>MAX_CAPTURE_BYTES: raise ValueError("source_capture_budget_exceeded_before_request")
            if preexisting:
                expected_urls=dict(_capture_roster(EXPECTED_IDS))
                expected_names=set(expected_urls)
                found={path.name.removesuffix(".body") for path in root.glob("*.body")}
                if not found or not found <= expected_names:
                    raise ValueError("partial_capture_name_roster_invalid")
                for name in found:
                    body,receipt=_load_replay(root,name)
                    if (receipt.get("method") != "GET" or receipt.get("url") != expected_urls[name]
                            or receipt.get("final_url") != expected_urls[name]):
                        raise ValueError("partial_capture_request_identity_mismatch:" + name)
                    captures[name]=(body,receipt)
                for name in found:
                    if not (root/f"{name}.wire").is_file() or not (root/f"{name}.receipt.json").is_file():
                        raise ValueError("partial_capture_artifact_trio_incomplete:" + name)
            with httpx.Client(follow_redirects=False,timeout=httpx.Timeout(45,connect=12),headers={"User-Agent":"hifld-next-datasets/usace-ienc-draft"}) as client:
                expected_urls=dict(_capture_roster(EXPECTED_IDS))
                if preexisting:
                    for suffix in ("body", "wire", "receipt.json"):
                        artifact_names={path.name.removesuffix("."+suffix) for path in root.glob("*."+suffix)}
                        if artifact_names != found:
                            raise ValueError("partial_capture_artifact_roster_mismatch")
                for name,url in _capture_roster(EXPECTED_IDS):
                    if name in captures:
                        continue
                    captures[name]=_fetch_retry(client,url,root,name,capture_budget)
                    if name.startswith("page_"):
                        time.sleep(0.25)
        fields,features,audit=_validate(captures)
    except Exception as exc:
        retained={}
        for receipt_path in sorted(root.glob("*.receipt.json")):
            try: retained[receipt_path.name.removesuffix(".receipt.json")]=json.loads(receipt_path.read_bytes())
            except (OSError,ValueError): retained[receipt_path.name]={"receipt_readback_failed":True}
        (root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":False,"error_code":type(exc).__name__,"receipts":retained})+b"\n")
        if captures: quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,captures,type(exc).__name__)
        raise

    identity=processing_identity(Path(__file__).parents[3])
    wkid=4326
    parquet=_parquet_bytes(fields,features,wkid)
    capture_sha=_sha(_canonical({name:_sha(body) for name,(body,_receipt) in sorted(captures.items())}))
    audit["source_capture_sha256"]=capture_sha
    version="v"+_sha(_canonical({"source_record_sha256":audit["source_record_sha256"],
        "source_geometry_sha256":audit["source_geometry_sha256"],"source_schema_sha256":audit["source_schema_sha256"],
        "source_capture_sha256":capture_sha,"processing_identity":identity}))[:32]
    prefix=candidate_prefix(DATASET_SLUG,FILE_SLUG,version)
    inputs=[{"input_id":name,
             "producer":"USDOT BTS public NTAD catalog" if name == "bts_item" else PUBLISHER,
             "host":urlsplit(receipt["url"]).hostname,
             "role":"official_distributor" if name == "bts_item" else "producer_publication",
             "canonical_url":BTS_ITEM_URL if name == "bts_item" else LAYER_URL,
             "retrieval_url":receipt["url"],"observed_final_url":receipt["final_url"],
             "retrieved_at":receipt["observed_at"],"http_status":receipt["status"],
             "bytes":receipt["bytes"],"sha256":receipt["sha256"],
             "wire_bytes":receipt["wire_bytes"],"wire_sha256":receipt["wire_sha256"],
             "license":"Unresolved; rights review pending.",
             "attribution":"USACE IENC; rights review pending.",
             "release_date":None,
             "release_date_unknown_reason":"The service response does not declare a dated release for this layer snapshot.",
             **({"distributor":"USDOT Bureau of Transportation Statistics (BTS)",
                 "delegation_evidence":{"bts_item_url":BTS_ITEM_URL,"bts_item_id":BTS_ITEM_ID,
                   "bts_item_owner":"USDOT_BTS","official_service_directory":SERVICE_DIRECTORY_URL,
                   "basis":"The captured public BTS item identifies the IENC program, says updates are bi-monthly, and links the USACE IENC Feature Classes service and master file geodatabase. This supports distribution lineage only; source edition, freshness, rights, and release acceptance remain unresolved."}}
                if name == "bts_item" else {})}
            for name,(_body,receipt) in sorted(captures.items())]
    field_mapping=[{"source_name":name,"source_esri_type":kind,
                    "output_name":"geometry" if name == "SHAPE" else name,
                    "output_arrow_type":str(_arrow_type({"type":kind})),
                    **({"geometry_encoding":"WKB"} if kind == "esriFieldTypeGeometry" else {})}
                   for name,kind in EXPECTED_FIELDS]
    manifest={"schema_version":1,"approved":False,"title":"Depth Areas (USACE IENC)","description":"Complete captured USACE IENC DEPTH_AREA feature class; unapproved faithful candidate. This inland chart layer is not a nationwide bathymetry or all-waterways inventory.","publisher":PUBLISHER,"agency":"U.S. Army Corps of Engineers","source_url":LAYER_URL,"license":None,"source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],"code_identity_scheme":identity["code_identity_scheme"],"inputs":inputs,"transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{"source_fields":"All declared attribute names, values and nulls are retained. JSON feature-response attribute names are matched to the declared layer names. Output Arrow representations follow the explicit per-field mapping below; physical type identity with the service is not asserted.","source_field_type_mapping":field_mapping,"source_ids":"OBJECTID and Feature_ID source values are retained; OBJECTID is the native row key. Feature_ID is retained as an attribute and is not used as a substitute row identifier.","geometry":"Polygon rings retained in captured raw Esri JSON; returnTrueCurves=true is requested because the source advertises true-curve support. Any curveRings response blocks candidate construction because this adapter does not losslessly encode curves. Linear-ring WKB candidate uses source EPSG:4326 coordinates; no reprojection or geometry repair.","candidate_status":"unapproved; source release, historical baseline, ID/schema/coverage and rights decisions remain pending"}}],"lineage":"BTS public PDF catalog item 7ec34864c8804468a9cf52d437fe4b6c identifies USACE/Army Geospatial Center as IENC producer, says the service is updated bi-monthly, and links the master geodatabase and feature-class service. This candidate is the complete observed DEPTH_AREA layer (ID 96) in the USACE IENC Feature Classes service. Neither the item-modified timestamp nor capture date establishes this layer's release date or freshness. Equivalence to the historical DataLumos archive remains unverified.","prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native layer attributes, IDs, and polygon coordinate sequences are retained as a candidate; schema and release decisions remain pending.","objects":[{"path":"geoparquet/depth-areas-usace-ienc-2.parquet","bytes":len(parquet),"sha256":_sha(parquet)}]}}},"unsupported_output_formats":{"geopackage":"No approved schema/compatibility contract.","pmtiles":"No display transformation approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"source_fields":fields,"official_layer_name":"DEPTH_AREA","source_publisher":"USACE IENC service","source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})
    attempts={}
    for receipt_path in sorted(root.glob("*.attempt*.receipt.json")):
        try: attempts[receipt_path.name.removesuffix(".receipt.json")]=json.loads(receipt_path.read_bytes())
        except (OSError,ValueError): attempts[receipt_path.name]={"receipt_readback_failed":True}
    (root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":True,"source_feature_count":audit["source_feature_count"],"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"receipts":{n:r for n,(_b,r) in sorted(captures.items())},"attempts":attempts,"artifact_roster":capture_roster_digests(root,exclude={"capture_manifest.json"})})+b"\n")
    outputs={"geoparquet/depth-areas-usace-ienc-2.parquet":parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"]=body; outputs[f"raw/{name}.wire"]=(root/f"{name}.wire").read_bytes(); outputs[f"raw/{name}.receipt.json"]=_canonical(receipt)+b"\n"
    ready={"approved":False,"version":version,"objects":{"geoparquet/depth-areas-usace-ienc-2.parquet":{"bytes":len(parquet),"sha256":_sha(parquet)}},"source_population_complete":True,"source_record_sha256":audit["source_record_sha256"],"source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"]=_canonical(ready)+b"\n"
    fingerprint=_sha(_canonical(identity)); state_key=f"_construction-state/{DATASET_SLUG}/{version}.json"
    receipts={n:r for n,(_b,r) in captures.items()}
    if storage.object_snapshot(state_key) is not None:
        bundle=json.loads(load_first_construction(storage,state_key,version,fingerprint))
        payload=bundle.get("payload")
        if not isinstance(payload,dict) or not isinstance(payload.get("capture_receipts"),dict):
            raise ValueError("cached_candidate_original_receipts_missing")
        original_receipts=payload["capture_receipts"]
        if set(original_receipts)!=set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name,(fresh_body,_receipt) in captures.items():
            retained=storage.read_key(f"{prefix}/raw/{name}.body")
            if retained!=fresh_body:
                raise ValueError("cached_source_bytes_changed:"+name)
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,
            fingerprint,identity,audit["source_record_sha256"],audit["source_schema_sha256"],
            payload.get("objects",{}),original_receipts)
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,
            "feature_count":audit["source_feature_count"],"source_population_complete":True,
            "source_id_sha256":audit["source_id_sha256"],"source_id_min":audit["source_id_min"],"source_id_max":audit["source_id_max"],
            "source_capture_sha256":audit["source_capture_sha256"],
            "source_schema_sha256":audit["source_schema_sha256"],"source_coverage_scope":audit["source_coverage_scope"],"drift_guard":audit["drift_guard"],
            "candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,
            "source_release_accepted":False,"source_freshness_accepted":False,
            "schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,
            "rights_accepted":False,"mapping_approved":False,"release_accepted":False,
            "capture_directory":str(root)}
    descriptors=_write_outputs(storage,prefix,outputs)
    from dagster_hifld.sources.epa_frs_common import seal_ready_candidate
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":receipts,"objects":descriptors})
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"],identity=identity)
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["source_feature_count"],"source_population_complete":True,"source_id_sha256":audit["source_id_sha256"],"source_id_min":audit["source_id_min"],"source_id_max":audit["source_id_max"],"source_capture_sha256":audit["source_capture_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"source_coverage_scope":audit["source_coverage_scope"],"drift_guard":audit["drift_guard"],"candidate_ready":prefix+"/metadata/candidate_ready.json","source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"capture_directory":str(root)}

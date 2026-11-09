"""Bounded native capture for USACE IENC Hulke Areas.

This creates an unapproved source-faithful candidate from the official IENC
feature class. Each ArcGIS response is retained verbatim with its wire receipt.
"""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import struct
from datetime import datetime, timezone
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

DATASET_SLUG = "hulke-areas-usace-ienc"
FILE_SLUG = DATASET_SLUG
LAYER_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/HULKES_AREA/MapServer/88"
SERVICE_URL = LAYER_URL.rsplit("/", 1)[0]
SERVICE_DIRECTORY_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson"
BTS_ITEM_ID = "7ec34864c8804468a9cf52d437fe4b6c"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
PUBLISHER = "US Army Corps of Engineers, Army Geospatial Center; official IENC Feature Classes service"
EXPECTED_COUNT = 45
EXPECTED_IDS = list(range(1, EXPECTED_COUNT + 1))
EXPECTED_ID_SHA256 = "ab6cf16b6160344f12d9a043415b4c216d7825c1578152f2904de281e82d22bc"
PAGE_SIZE = 250
MAX_BODY_BYTES = 1_500_000
MAX_CAPTURE_BYTES = 2_000_000
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID"), ("SHAPE", "esriFieldTypeGeometry"),
    ("Category_of_Hulke", "esriFieldTypeString"), ("Object_Name", "esriFieldTypeString"), ("Information", "esriFieldTypeString"),
    ("Source_Date", "esriFieldTypeString"), ("Source_Indication", "esriFieldTypeString"),
    ("Source_Dataset", "esriFieldTypeString"), ("Feature_ID", "esriFieldTypeString"),
]
EXPECTED_FIELD_DECLARATIONS = [
    {"name":name,"type":kind,"alias":name,"domain":None,
     **({"length":254} if kind == "esriFieldTypeString" else {})}
    for name,kind in EXPECTED_FIELDS
]
EXPECTED_LAYER_EXTENT = {
    "xmin": -95.91146229999998,
    "ymin": 31.55923000000007,
    "xmax": -80.01800609999998,
    "ymax": 43.04139410000005,
}
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
            "esriFieldTypeDate":pa.int64()}.get(field.get("type"), pa.string())


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
    urls = [("bts_item", BTS_ITEM_URL), ("service_directory", SERVICE_DIRECTORY_URL),
            ("service_before", SERVICE_URL + "?f=pjson"),
            ("layer_before", LAYER_URL + "?f=pjson"),
            ("count_before", _query({"where":"1=1","returnCountOnly":"true","f":"json"})),
            ("ids_before", _query({"where":"1=1","returnIdsOnly":"true","f":"json"}))]
    for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        page_ids = ids[start:start + PAGE_SIZE]
        urls.append((f"page_{index:03d}", _query({"objectIds":",".join(map(str,page_ids)),
            "outFields":"*", "returnGeometry":"true", "returnZ":"true", "returnM":"true",
            "outSR":"4326", "orderByFields":"OBJECTID ASC", "f":"json"})))
    urls.extend([("layer_after", LAYER_URL + "?f=pjson"),
                 ("count_after", _query({"where":"1=1","returnCountOnly":"true","f":"json"})),
                 ("ids_after", _query({"where":"1=1","returnIdsOnly":"true","f":"json"})),
                 ("service_after", SERVICE_URL + "?f=pjson")])
    return urls


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
    if not {"ids_seed", *[name for name,_url in _capture_roster(EXPECTED_IDS)]} <= set(captures):
        raise ValueError("source_capture_roster_incomplete")
    docs = {name: json.loads(body) for name, (body, _r) in captures.items()}
    item = docs["bts_item"]
    if (item.get("id") != BTS_ITEM_ID or item.get("owner") != "USDOT_BTS"
            or item.get("type") != "PDF" or item.get("access") != "public"):
        raise ValueError("bts_ienc_item_identity_drift")
    item_description = " ".join(str(item.get(key, "")) for key in ("title", "description"))
    if ("Inland Electronic Navigational Charts" not in item_description
            or "updated bi-monthly" not in item_description
            or SERVICE_DIRECTORY_URL.split("?")[0] not in item_description
            or "USACE_IENC_Master_Service_gdb.zip" not in item_description):
        raise ValueError("bts_ienc_official_distribution_link_drift")
    directory = docs["service_directory"]
    if not any(service.get("name") == "IENC_Feature_Classes/HULKES_AREA"
               and service.get("type") == "MapServer" for service in directory.get("services", [])):
        raise ValueError("official_ienc_service_directory_drift")
    service = docs["service_before"]
    if (service.get("mapName") != "HULKES_AREA" or service.get("copyrightText") != "USACE IENC"
            or [(layer.get("id"), layer.get("name"), layer.get("geometryType")) for layer in service.get("layers", [])]
            != [(88, "HULKES_AREA", "esriGeometryPolygon")]):
        raise ValueError("official_hulke_area_service_drift")
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
    if layer.get("name") != "HULKES_AREA" or layer.get("id") != 88 or layer.get("geometryType") != "esriGeometryPolygon":
        raise ValueError("source_layer_identity_drift")
    if (layer.get("spatialReference",{}).get("latestWkid") != 4326
            or layer.get("maxRecordCount") != 2000):
        raise ValueError("source_spatial_reference_or_page_limit_drift")
    if {key: layer.get("extent", {}).get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise ValueError("source_coverage_extent_drift")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent = layer.get("extent", {})
    if "spatialReference" in extent:
        _validate_spatial_reference(extent["spatialReference"], "source_layer_extent")
    if docs["count_before"].get("count") != EXPECTED_COUNT or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    before, after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if not isinstance(before,list) or before != after or before != EXPECTED_IDS or len(before) != EXPECTED_COUNT or len(set(before)) != EXPECTED_COUNT or _sha(_canonical(before)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_population_drift")
    if docs["ids_seed"].get("objectIds") != EXPECTED_IDS:
        raise ValueError("source_seed_id_population_drift")
    features=[]
    pages=sorted(n for n in captures if n.startswith("page_"))
    if len(pages) != (EXPECTED_COUNT + PAGE_SIZE - 1)//PAGE_SIZE:
        raise ValueError("source_page_roster_incomplete")
    for name in pages:
        page=docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncation")
        _validate_spatial_reference(page.get("spatialReference"), "source_page")
        rows=page.get("features")
        if not isinstance(rows,list): raise ValueError("source_page_features_missing")
        features.extend(rows)
    expected_attrs=[f["name"] for f in fields if f["name"]!="SHAPE"]
    got=[]
    feature_ids=[]
    geometry_valid=geometry_empty=0
    vertices=0
    for feature in features:
        attrs, geom=feature.get("attributes"), feature.get("geometry")
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
        if type(oid) is not int or oid <= 0:
            raise ValueError("source_native_identifier_missing_or_type_drift")
        if feature_id is not None and not isinstance(feature_id, str):
            raise ValueError("source_feature_id_value_type_drift")
        got.append(oid)
        if feature_id is not None:
            feature_ids.append(feature_id)
        for ring in geom.get("rings",[]):
            for point in ring:
                if (not isinstance(point,list) or len(point)<2
                        or any(type(value) not in (int,float) or not math.isfinite(value) for value in point)):
                    raise ValueError("source_polygon_ordinate_contract_drift")
                if not (EXPECTED_LAYER_EXTENT["xmin"] <= point[0] <= EXPECTED_LAYER_EXTENT["xmax"]
                        and EXPECTED_LAYER_EXTENT["ymin"] <= point[1] <= EXPECTED_LAYER_EXTENT["ymax"]):
                    raise ValueError("source_geometry_outside_declared_coverage")
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
        "source_id_sha256":_sha(_canonical(sorted(got))),
        "source_feature_count":len(features),"source_id_min":min(got),"source_id_max":max(got),
        "feature_id_distinct_count":len(set(feature_ids)),"feature_id_duplicate_count":len(feature_ids)-len(set(feature_ids)),
        "feature_id_null_count":sum(1 for feature in features if feature["attributes"].get("Feature_ID") is None),
        "source_geometry_type":"esriGeometryPolygon","source_wkid":4326,
        "source_has_z":bool(layer.get("hasZ")),"source_has_m":bool(layer.get("hasM")),
        "source_vertex_count":vertices,"geometry_valid_count":geometry_valid,
        "geometry_empty_count":geometry_empty,"geometry_invalid_count":len(features)-geometry_valid-geometry_empty,
        "source_coverage_scope":"Complete observed current official IENC HULKES_AREA feature class; inland chart coverage only, not a claim of all hulks nationwide.",
        "drift_guard":f"Current public BTS IENC item {BTS_ITEM_ID} distribution/delegation link; no prior-item succession is asserted. Official service-directory membership, service/layer identity, field names/types, CRS, pinned layer extent, page errors, and before/after exact ID roster are checked; duplicate Feature_ID values are preserved."}


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


def capture_usace_hulke_areas(storage: StagingStorageResource, *, version: str,
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
    if replay:
        root=requested
    elif requested:
        if any(requested.glob("*.body")) or any(requested.glob("*.wire")) or any(requested.glob("*.receipt.json")):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root=requested/(version+"-"+uuid4().hex)
    else:
        root=Path("/tmp/hifld-native-reconstruction/116/captures")/(version+"-"+uuid4().hex)
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
            if not isinstance(ids,list): raise ValueError("retained_id_seed_invalid")
            names=["ids_seed",*[name for name,_url in _capture_roster(ids)]]
            if set(retained_manifest["receipts"])!=set(names):
                raise ValueError("retained_capture_roster_changed")
            captures={name:_load_replay(root,name) for name in names}
            if any(receipt!=retained_manifest["receipts"][name] for name,(_body,receipt) in captures.items()):
                raise ValueError("retained_capture_manifest_receipt_mismatch")
            expected_urls = dict(_capture_roster(ids))
            expected_urls["ids_seed"] = _query({"where":"1=1","returnIdsOnly":"true","f":"json"})
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
            with httpx.Client(follow_redirects=False,timeout=httpx.Timeout(45,connect=12),headers={"User-Agent":"hifld-next-datasets/usace-ienc-draft"}) as client:
                seed_body,seed_receipt=_fetch_retry(client,_query({"where":"1=1","returnIdsOnly":"true","f":"json"}),root,"ids_seed",capture_budget)
                ids=json.loads(seed_body).get("objectIds")
                if ids != EXPECTED_IDS or _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
                    raise ValueError("source_seed_id_count_or_roster_drift")
                captures["ids_seed"]=(seed_body,seed_receipt)
                for name,url in _capture_roster(ids): captures[name]=_fetch_retry(client,url,root,name,capture_budget)
        fields,features,audit=_validate(captures)
    except Exception as exc:
        retained={}
        for receipt_path in sorted(root.glob("*.receipt.json")):
            try: retained[receipt_path.name.removesuffix(".receipt.json")]=json.loads(receipt_path.read_bytes())
            except (OSError,ValueError): retained[receipt_path.name]={"receipt_readback_failed":True}
        # An explicit replay is read-only evidence. A failed validation must not
        # rewrite a previously complete capture as a fresh, incomplete attempt.
        if not replay:
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
             "producer":"USDOT Bureau of Transportation Statistics (BTS)" if name == "bts_item" else PUBLISHER,
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
             "release_date_unknown_reason":"The BTS item describes bi-monthly updates but does not provide a release date for this extracted layer snapshot.",
             **({"distributor":"USACE IENC Feature Classes service",
                 "delegation_evidence":{"bts_item_url":BTS_ITEM_URL,
                 "bts_item_id":BTS_ITEM_ID,"bts_item_owner":"USDOT_BTS",
                 "official_service_directory":SERVICE_DIRECTORY_URL,
                 "linked_feature_classes_service":"https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes"}}
                if name == "bts_item" else {})}
            for name,(_body,receipt) in sorted(captures.items())]
    manifest={"schema_version":1,"approved":False,"title":"Hulke Areas (USACE IENC)","description":"Complete observed current official USACE IENC HULKES_AREA feature class; unapproved faithful candidate. Inland-chart coverage only; historical HIFLD equivalence and nationwide hulks completeness are not asserted.","publisher":PUBLISHER,"agency":"US Army Corps of Engineers, Army Geospatial Center","source_url":LAYER_URL,"license":None,"source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],"code_identity_scheme":identity["code_identity_scheme"],"inputs":inputs,"transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{"source_fields":"all declared attributes retained in source order and declared physical types","source_ids":"OBJECTID and nullable/duplicate-capable Feature_ID values retained unchanged","geometry":"Polygon rings retained in captured raw Esri JSON; WKB candidate at source EPSG:4326; no reprojection or geometry repair","candidate_status":"unapproved; BTS catalog-item succession, source release, historical baseline, ID/schema/coverage and rights decisions remain pending"}}],"lineage":f"Current public USDOT BTS IENC PDF item {BTS_ITEM_ID} links the official IENC Feature Classes service and master geodatabase. This candidate is the exact published HULKES_AREA layer; its relationship to any historical HIFLD release is unestablished.","prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native layer attributes, identifiers and unmodified polygon rings retained as a candidate; release and compatibility decisions remain pending.","objects":[{"path":"geoparquet/hulke-areas-usace-ienc.parquet","bytes":len(parquet),"sha256":_sha(parquet)}]}}},"unsupported_output_formats":{"geopackage":"No approved schema/compatibility contract.","pmtiles":"No display transformation approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"source_fields":fields,"official_layer_name":"HULKES_AREA","official_source_item":"USDOT_BTS " + BTS_ITEM_ID,"prior_source_item":None,"source_item_succession_accepted":False,"source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})
    attempts={}
    for receipt_path in sorted(root.glob("*.attempt*.receipt.json")):
        try: attempts[receipt_path.name.removesuffix(".receipt.json")]=json.loads(receipt_path.read_bytes())
        except (OSError,ValueError): attempts[receipt_path.name]={"receipt_readback_failed":True}
    if not replay:
        (root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":True,"source_feature_count":audit["source_feature_count"],"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"receipts":{n:r for n,(_b,r) in sorted(captures.items())},"attempts":attempts,"artifact_roster":capture_roster_digests(root,exclude={"capture_manifest.json"})})+b"\n")
    outputs={"geoparquet/hulke-areas-usace-ienc.parquet":parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"]=body; outputs[f"raw/{name}.wire"]=(root/f"{name}.wire").read_bytes(); outputs[f"raw/{name}.receipt.json"]=_canonical(receipt)+b"\n"
    ready={"approved":False,"version":version,"objects":{"geoparquet/hulke-areas-usace-ienc.parquet":{"bytes":len(parquet),"sha256":_sha(parquet)}},"source_population_complete":True,"source_record_sha256":audit["source_record_sha256"],"source_item_succession_accepted":False,"source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
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
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["source_feature_count"],"source_population_complete":True,"source_id_sha256":audit["source_id_sha256"],"source_id_min":audit["source_id_min"],"source_id_max":audit["source_id_max"],"source_schema_sha256":audit["source_schema_sha256"],"source_coverage_scope":audit["source_coverage_scope"],"drift_guard":audit["drift_guard"],"candidate_ready":prefix+"/metadata/candidate_ready.json","source_item_succession_accepted":False,"source_release_accepted":False,"source_freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"capture_directory":str(root)}

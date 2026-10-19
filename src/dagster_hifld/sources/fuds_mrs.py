"""Lossless native capture of USACE FUDS Munitions Response Sites."""
from __future__ import annotations

import hashlib
import json
import math
import struct
from pathlib import Path
from urllib.parse import urlencode, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import Polygon

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

SERVICE_URL = "https://geospatial.sec.usace.army.mil/server/rest/services/Military/FUDS_Data/MapServer"
LAYER_ID = 3
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "FUDS Munitions Response Site"
DATASET_SLUG = "formerly-used-defense-sites-public-mrs"
FILE_SLUG = DATASET_SLUG
PUBLISHER = "U.S. Army Corps of Engineers (USACE), Military/FUDS_Data MapServer"
EXPECTED_COUNT = 1682
EXPECTED_SCHEMA_SHA256 = "fdfa05fec5e9854080ca4d5f3adab00224a8c61dad140a5efbfe3199e59d2bc2"
PAGE_SIZE = 250
MAX_RESPONSE_BYTES = 32 * 1024 * 1024


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _plain_field(field: dict) -> dict:
    return {k: field[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in field}


def _arrow_type(field: dict) -> pa.DataType:
    return {
        "esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
        "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeDouble": pa.float64(),
        "esriFieldTypeSingle": pa.float32(), "esriFieldTypeString": pa.string(),
        "esriFieldTypeGUID": pa.string(), "esriFieldTypeGlobalID": pa.string(),
        "esriFieldTypeBlob": pa.binary(), "esriFieldTypeDate": pa.int64(),
    }[field["type"]]


def _ring_area(ring: list[list[float]]) -> float:
    x0, y0 = ring[0][:2]
    return math.fsum((ring[i][0]-x0)*(ring[i+1][1]-y0) - (ring[i+1][0]-x0)*(ring[i][1]-y0)
                     for i in range(len(ring)-1)) / 2


def _encode_polygon_z(rings: list[list[list[float]]]) -> bytes:
    """Encode XY/Z vertices as ISO WKB; M ordinates are retained separately."""
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    shells: list[tuple[list[list[float]], Polygon]] = []
    holes: list[tuple[list[list[float]], Polygon]] = []
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
            raise ValueError("source_polygon_ring_invalid")
        for point in ring:
            if (not isinstance(point, list) or len(point) != 4
                    or any(v is not None and (type(v) not in (int, float) or not math.isfinite(v)) for v in point)):
                raise ValueError("source_polygon_coordinate_invalid")
        if ring[0][2] is None:
            raise ValueError("source_z_ordinate_null")
        ring_polygon = Polygon([point[:2] for point in ring])
        if _ring_area(ring) < 0:
            shells.append((ring, ring_polygon))
        else:
            holes.append((ring, ring_polygon))
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    # Esri specifies clockwise exterior rings and counterclockwise holes.
    # Test full hole polygon containment so an inner island cannot steal a
    # hole merely because the hole's first vertex happens to fall inside it.
    assigned: dict[int, list] = {i: [] for i in range(len(shells))}
    for hole, hole_polygon in holes:
        parents = [(abs(_ring_area(shell)), i) for i, (shell, shell_polygon) in enumerate(shells)
                   if shell_polygon.contains(hole_polygon)]
        if not parents:
            raise ValueError("source_polygon_hole_without_containing_shell")
        assigned[min(parents)[1]].append(hole)
    le = "<"
    def ring_bytes(ring):
        return struct.pack(le + "I", len(ring)) + b"".join(
            struct.pack(le + "ddd", float(x), float(y), float(z)) for x, y, z, _m in ring)
    polygons = []
    for i, (shell, _shell_polygon) in enumerate(shells):
        all_rings = [shell, *assigned[i]]
        polygon = b"\x01" + struct.pack(le + "I", 1003) + struct.pack(le + "I", len(all_rings))
        polygon += b"".join(ring_bytes(r) for r in all_rings)
        polygons.append(polygon)
    return b"\x01" + struct.pack(le + "I", 1006) + struct.pack(le + "I", len(polygons)) + b"".join(polygons)


def _native_parquet(fields: list[dict], features: list[dict], feature_srs: dict[int, dict]) -> bytes:
    attrs = [f for f in fields if f["type"] != "esriFieldTypeGeometry"]
    columns = {f["name"]: pa.array([x["attributes"][f["name"]] for x in features], type=_arrow_type(f)) for f in attrs}
    columns["geometry"] = pa.array([
        None if feature.get("geometry") is None else _encode_polygon_z(feature["geometry"]["rings"])
        for feature in features
    ], type=pa.binary())
    vertex_type = pa.struct([
        pa.field("x", pa.float64(), nullable=False), pa.field("y", pa.float64(), nullable=False),
        pa.field("z", pa.float64(), nullable=False), pa.field("m", pa.float64(), nullable=True),
    ])
    source_geometry_type = pa.struct([
        pa.field("hasZ", pa.bool_(), nullable=False), pa.field("hasM", pa.bool_(), nullable=False),
        pa.field("spatialReference", pa.string(), nullable=False),
        pa.field("rings", pa.list_(pa.list_(vertex_type)), nullable=False),
    ])
    columns["source_geometry"] = pa.array([
        None if feature.get("geometry") is None else {
            "hasZ": feature["geometry"]["hasZ"], "hasM": feature["geometry"]["hasM"],
            "spatialReference": _canonical(feature_srs[feature["attributes"]["OBJECTID"]]).decode("utf-8"),
            "rings": [[{"x":p[0],"y":p[1],"z":p[2],"m":p[3]} for p in ring]
                      for ring in feature["geometry"]["rings"]],
        }
        for feature in features
    ], type=source_geometry_type)
    schema = pa.schema([pa.field(f["name"], _arrow_type(f), nullable=f.get("nullable", True),
                                  metadata={b"esri:source_field": _canonical(f)}) for f in attrs]
                       + [pa.field("geometry", pa.binary(), nullable=True),
                          pa.field("source_geometry", source_geometry_type, nullable=True)])
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["MultiPolygon Z"],
        "crs": pyproj.CRS.from_epsg(4326).to_json_dict(), "edges": "planar"}}}
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(columns, schema=schema.with_metadata({
        b"geo": _canonical(geo), b"hifld:source_esri_fields": _canonical(fields),
        b"hifld:source_geometry_encoding": b"source_geometry is ordered native Esri geometry flags, returned spatialReference, and ring vertices; M is nullable; primary WKB is derived XYZ only because GeoParquet does not support M"}))
                   , sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def capture_fuds_mrs(storage: StagingStorageResource, *, version: str,
                     capture_directory: str | None = None) -> dict:
    """Capture a bracketed complete population and stage an unapproved candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    root = Path(capture_directory or "/tmp/hifld-native-reconstruction/98/captures") / version
    root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict]] = {}
    feature_srs: dict[int, dict] = {}
    first_page_sr: dict | None = None

    def get(client, name, url, params=None):
        target = url + ("?" + urlencode(params) if params else "")
        body, receipt = capture_get(client, target, root, name, max_bytes=MAX_RESPONSE_BYTES)
        doc = json.loads(body)
        if not isinstance(doc, dict) or "error" in doc:
            raise ValueError("source_response_error:" + name)
        captures[name] = body, receipt
        return doc

    try:
        with httpx.Client(follow_redirects=False, timeout=120, headers={"User-Agent": "hifld-next-datasets/fuds-mrs"}) as client:
            service = get(client, "service_before", SERVICE_URL, {"f": "pjson"})
            layer = get(client, "layer_before", LAYER_URL, {"f": "pjson"})
            if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
                    or layer.get("geometryType") != "esriGeometryPolygon"):
                raise ValueError("source_layer_identity_drift")
            fields = layer.get("fields")
            if not isinstance(fields, list) or not fields or any(not isinstance(f, dict) for f in fields):
                raise ValueError("source_fields_missing")
            if any(f.get("name") in {"geometry", "source_geometry"} for f in fields):
                raise ValueError("source_auxiliary_column_name_collision")
            schema_sha = _sha(_canonical([_plain_field(f) for f in fields]))
            if schema_sha != EXPECTED_SCHEMA_SHA256:
                raise ValueError("source_schema_drift")
            sr = layer.get("extent", {}).get("spatialReference", {})
            if sr.get("latestWkid", sr.get("wkid")) != 4326 or layer.get("hasZ") is not True or layer.get("hasM") is not True:
                raise ValueError("source_spatial_dimensions_drift")
            caps = layer.get("advancedQueryCapabilities", {})
            if caps.get("supportsPagination") is not True or caps.get("supportsOrderBy") is not True:
                raise ValueError("source_ordered_pagination_not_supported")
            count = get(client, "count_before", LAYER_URL + "/query", {"where": "1=1", "returnCountOnly": "true", "f": "json"}).get("count")
            ids = get(client, "ids_before", LAYER_URL + "/query", {"where": "1=1", "returnIdsOnly": "true", "f": "json"}).get("objectIds")
            if (type(count) is not int or count != EXPECTED_COUNT or not isinstance(ids, list) or len(ids) != count
                    or any(type(i) is not int for i in ids) or len(set(ids)) != count):
                raise ValueError("source_initial_id_roster_invalid")
            seed = sorted(ids)
            features = []
            attr_names = [f["name"] for f in fields if f.get("type") != "esriFieldTypeGeometry"]
            for page_no, offset in enumerate(range(0, count, PAGE_SIZE)):
                page = get(client, f"page_{page_no:03d}", LAYER_URL + "/query", {
                    "where": "1=1", "outFields": "*", "returnGeometry": "true", "returnZ": "true", "returnM": "true",
                    "outSR": "4326", "orderByFields": "OBJECTID ASC", "resultOffset": offset,
                    "resultRecordCount": min(PAGE_SIZE, count-offset), "f": "json"})
                rows = page.get("features")
                expected = min(PAGE_SIZE, count-offset)
                if not isinstance(rows, list) or len(rows) != expected:
                    raise ValueError("source_page_incomplete")
                if page.get("exceededTransferLimit") is True and offset + expected >= count:
                    raise ValueError("source_final_page_exceeded_transfer_limit")
                page_sr = page.get("spatialReference", {})
                if page_sr.get("latestWkid", page_sr.get("wkid")) != 4326:
                    raise ValueError("source_page_crs_drift")
                if first_page_sr is None:
                    first_page_sr = page_sr
                elif page_sr != first_page_sr:
                    raise ValueError("source_page_spatial_reference_drift")
                for row in rows:
                    if not isinstance(row, dict) or not isinstance(row.get("attributes"), dict) or list(row["attributes"]) != attr_names:
                        raise ValueError("source_record_schema_drift")
                    geom = row.get("geometry")
                    if geom is not None and (geom.get("hasZ") is not True or geom.get("hasM") is not True):
                        raise ValueError("source_geometry_dimensions_drift")
                if [r["attributes"].get("OBJECTID") for r in rows] != seed[offset:offset+expected]:
                    raise ValueError("source_page_id_slice_mismatch")
                feature_srs.update({r["attributes"]["OBJECTID"]: page_sr for r in rows})
                features.extend(rows)
            layer_after = get(client, "layer_after", LAYER_URL, {"f": "pjson"})
            service_after = get(client, "service_after", SERVICE_URL, {"f": "pjson"})
            count_after = get(client, "count_after", LAYER_URL+"/query", {"where":"1=1","returnCountOnly":"true","f":"json"}).get("count")
            ids_after = get(client, "ids_after", LAYER_URL+"/query", {"where":"1=1","returnIdsOnly":"true","f":"json"}).get("objectIds")
        if layer != layer_after or service != service_after or count_after != count or sorted(ids_after or []) != seed:
            raise ValueError("source_changed_during_capture")
        if sorted(r["attributes"]["OBJECTID"] for r in features) != seed:
            raise ValueError("source_feature_population_incomplete")
        for row in features:
            geom = row.get("geometry")
            if geom is not None:
                _encode_polygon_z(geom.get("rings"))
        record_sha = _sha(_canonical([r["attributes"] for r in features]))
        geometry_sha = _sha(_canonical([[r["attributes"]["OBJECTID"], r.get("geometry")] for r in features]))
        identity = processing_identity(Path(__file__).parents[3])
        native = _native_parquet(fields, features, feature_srs)
        version = content_derived_version(record_sha, geometry_sha, schema_sha, identity)
        prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
        native_name = f"geoparquet/{DATASET_SLUG}.parquet"
        manifest = {
            "schema_version": 1, "processor": "HIFLD Next", "approved": False,
            "title": LAYER_NAME, "publisher": PUBLISHER, "agency": "U.S. Army Corps of Engineers",
            "source_url": LAYER_URL, "source_release_date": None, "source_release_date_unknown": True,
            "source_release_date_unknown_reason": "USACE service metadata does not identify a producer release date.",
            "source_release_date_unknown_reason": "USACE service metadata does not identify a producer release date.",
            "inventory_archive_reference": {"archive_id": "239739V1",
                "url": "https://www.datalumos.org/datalumos/project/239739/version/V1/view",
                "role": "HIFLD inventory archive reference; contents not retrieved or compared"},
            "feature_count": count, "native_id_column": "OBJECTID", "source_schema_sha256": schema_sha,
            "source_crs_epsg": 4326, "geometry_type": "Polygon", "geometry_dimensions": "ZM",
            "rights_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "mapping_approved": False, "release_accepted": False,
            "code_commit": identity["code_commit"], "inputs": [
                {"input_id": n, "producer": PUBLISHER, "host": urlsplit(r["url"]).hostname,
                 "role": "producer_publication", "canonical_url": LAYER_URL, "retrieval_url": r["url"],
                 "observed_final_url": r["final_url"], "retrieved_at": r["observed_at"],
                 "http_status": r["status"], "bytes": r["bytes"], "sha256": r["sha256"],
                 "license": "Unreviewed; no license claim made.",
                 "attribution": "USACE service attribution and terms have not been reviewed.",
                 "release_date": None, "release_date_unknown_reason": "USACE service metadata gives no producer release date."}
                for n, (_body, r) in sorted(captures.items())],
            "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate", "parameters": {
                "layer_id": LAYER_ID, "attributes": "all returned source attributes in source order and declared Arrow types",
                "geometry": "ISO WKB MultiPolygon Z is derived for GeoParquet compatibility; exact ordered XY/Z/M tuples with nullable M retained in source_geometry_vertices and raw Esri JSON",
                "null_geometry": "retained as nullable geometry; no feature dropped", "approval_gates_open": True}}],
            "prebuilt_output_policy": {"schema_version":1,"formats":{"geoparquet":{"reason":"Native fields, IDs, and polygon Z/M coordinates are retained in an unapproved candidate.","objects":[{"path":native_name,"bytes":len(native),"sha256":_sha(native)}]}}},
            "unapproved_candidate": True,
        }
        validate_release_provenance(manifest); validate_output_policy(manifest)
        outputs = {native_name:native, "metadata/source_manifest.json":_canonical(manifest)+b"\n",
                   "metadata/processing_identity.json":_canonical(identity)+b"\n",
                   "metadata/source_audit.json":_canonical({"feature_count":count,"source_ids":seed,
                       "all_ids_captured_once":True,"null_geometry_count":sum(r.get("geometry") is None for r in features),
                       "source_record_sha256":record_sha,"source_geometry_sha256":geometry_sha,
                       "source_schema_sha256":schema_sha,"source_zm_dimensions_preserved":True,
                       "source_returned_spatial_reference":first_page_sr,
                       "source_vertex_count":sum(len(ring) for r in features if r.get("geometry") for ring in r["geometry"]["rings"]),
                       "source_measure_null_count":sum(p[3] is None for r in features if r.get("geometry") for ring in r["geometry"]["rings"] for p in ring),
                       "source_measure_zero_count":sum(p[3] == 0 for r in features if r.get("geometry") for ring in r["geometry"]["rings"] for p in ring)} )+b"\n"}
        receipts = {n:r for n,(_b,r) in sorted(captures.items())}
        for name,(body,receipt) in captures.items():
            outputs[f"raw/{name}.body"] = body
            outputs[f"raw/{name}.wire"] = (root/f"{name}.wire").read_bytes()
            outputs[f"raw/{name}.receipt.json"] = _canonical(receipt)+b"\n"
        descriptors = {n:{"bytes":len(b),"sha256":_sha(b)} for n,b in outputs.items()}
        outputs["metadata/candidate_ready.json"] = _canonical({"approved":False,"version":version,"objects":descriptors,
            "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
            "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})+b"\n"
        state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
        fingerprint = _sha(_canonical(identity))
        if storage.object_snapshot(state_key) is not None:
            sealed = json.loads(load_first_construction(storage,state_key,version,fingerprint)); payload = sealed.get("payload",{})
            verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,
                identity,record_sha,schema_sha,payload.get("objects",{}),payload.get("capture_receipts",{}))
            if set(payload.get("objects",{})) != set(outputs): raise ValueError("cached_candidate_roster_changed")
            return {"version":version,"prefix":prefix,"feature_count":count,"candidate_approved":False,"reused":True}
        for name,body in outputs.items():
            write_immutable(storage,f"{prefix}/{name}",body)
            snap=storage.object_snapshot(f"{prefix}/{name}")
            if snap is None or snap.size != len(body) or storage.sha256_key(f"{prefix}/{name}") != _sha(body):
                raise ValueError("candidate_output_physical_readback_failed:"+name)
        seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,
            "source_record_sha256":record_sha,"source_schema_sha256":schema_sha,"capture_receipts":receipts,
            "objects":descriptors})
        validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
        return {"version":version,"prefix":prefix,"feature_count":count,"candidate_approved":False,
                "source_release_accepted":False,"freshness_accepted":False,"schema_contract_accepted":False,
                "id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    except Exception as exc:
        if captures:
            quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,captures,type(exc).__name__)
        raise

"""Draft capture adapter for the USFWS final critical habitat layer.

The producer service contains a separate proposed layer. This adapter reads
only layer 0, captures the service's Esri JSON responses verbatim, and keeps
all producer fields and polygon rings in an unapproved GeoParquet candidate.
"""
from __future__ import annotations

import hashlib
import json
import gzip
import io
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely
from shapely.geometry import MultiPolygon, Polygon

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, content_derived_version, processing_identity,
    write_immutable,
)
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "final-critical-habitat-features"
FILE_SLUG = DATASET_SLUG
ITEM_ID = "794de45b9d774d21aed3bf9b5313ee24"
SERVICE_URL = "https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/USFWS_Critical_Habitat/FeatureServer"
LAYER_ID = 0
LAYER_NAME = "Final Critical Habitat Features"
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "U.S. Fish and Wildlife Service (ArcGIS item owner aditya_sastry@fws.gov_fws)"
PAGE_SIZE = 50
MAX_RESPONSE_BYTES = 128 * 1024 * 1024
MAX_ISSUE_BYTES = 3 * 1024 * 1024 * 1024
ISSUE_EVIDENCE_ROOT = Path("/tmp/hifld-native-reconstruction/90")


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _tree_bytes(root: Path) -> int:
    """Count regular files without following links for the issue-wide cap."""
    if not root.exists():
        return 0
    total = 0
    for path in root.rglob("*"):
        if path.is_symlink():
            continue
        if path.is_file():
            total += path.stat().st_size
    return total


def _enforce_issue_budget(roots: tuple[Path, ...], reserve_bytes: int = 0) -> None:
    ordered = sorted(set(Path(root).resolve() for root in roots), key=lambda p: len(p.parts))
    unique_roots = tuple(root for i,root in enumerate(ordered)
                         if not any(parent == root or parent in root.parents for parent in ordered[:i]))
    if sum(_tree_bytes(root) for root in unique_roots) + reserve_bytes > MAX_ISSUE_BYTES:
        raise OSError("issue_90_disk_budget_exceeded")


def _framed_hash_update(digest, value: object) -> None:
    """Hash one canonical JSON value with an unambiguous length prefix."""
    encoded = _canonical(value)
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)


def _fetch(client: httpx.Client, url: str, capture_root: Path, name: str,
           budget_roots: tuple[Path, ...]) -> tuple[Path, dict[str, Any]]:
    """Bound decoded and exact-wire capture separately while accepting gzip."""
    wire_path, body_path = capture_root / f"{name}.wire", capture_root / f"{name}.body"
    wire_path.touch(exist_ok=True); body_path.touch(exist_ok=True)
    wire_hash, body_hash = hashlib.sha256(), hashlib.sha256()
    wire_size = body_size = 0
    observed = datetime.now(timezone.utc).isoformat()
    def retain(path: Path, digest, data: bytes, kind: str) -> None:
        nonlocal wire_size, body_size
        with path.open("ab") as stream: stream.write(data)
        digest.update(data)
        if kind == "wire": wire_size += len(data)
        else: body_size += len(data)
    with client.stream("GET",url,headers={"Accept-Encoding":"gzip"}) as response:
        for path in (wire_path,body_path): path.unlink(missing_ok=True)
        def reserve(progress):
            # Reserve wire and decoded bytes plus bounded decoder allocation
            # before every read/decompression. The cap is aggregate, not per URL.
            _enforce_issue_budget(budget_roots, progress.reserve_bytes)
        result = capture_http_response(response,max_wire_bytes=MAX_RESPONSE_BYTES,max_decoded_bytes=MAX_RESPONSE_BYTES,
            reserve=reserve,
            retain_wire=lambda data:retain(wire_path,wire_hash,data,"wire"),
            retain_decoded=lambda data:retain(body_path,body_hash,data,"body"))
        final_url = str(response.url)
    receipt={"method":"GET","url":url,"final_url":final_url,"observed_at":observed,"status":result.status_code,
        "complete_body":result.complete,"wire_bytes":wire_size,"wire_sha256":wire_hash.hexdigest(),
        "bytes":body_size,"sha256":body_hash.hexdigest(),"content_encoding":result.content_encoding,
        "content_length":result.content_length,"error_code":result.error_code}
    (capture_root/f"{name}.receipt.json").write_bytes(_canonical(receipt)+b"\n")
    body=body_path.read_bytes(); wire=wire_path.read_bytes()
    if (not result.complete or result.status_code != 200 or final_url != url or len(body) != body_size
            or _sha(body) != receipt["sha256"] or len(wire) != wire_size or _sha(wire) != receipt["wire_sha256"]):
        raise ValueError("incomplete_source_capture:"+name)
    return body_path,receipt


def _capture_body(value: bytes | Path) -> bytes:
    return value.read_bytes() if isinstance(value, Path) else value


def _verify_wire_replay(wire: bytes, receipt: dict[str, Any], name: str) -> None:
    if receipt.get("complete_body") is not True or receipt.get("status") != 200:
        raise ValueError("cached_receipt_not_complete:" + name)
    if type(receipt.get("bytes")) is not int or not 0 <= receipt["bytes"] <= MAX_RESPONSE_BYTES:
        raise ValueError("cached_receipt_decoded_size_invalid:" + name)
    if (type(receipt.get("wire_bytes")) is not int or receipt["wire_bytes"] < 0
            or receipt["wire_bytes"] > MAX_RESPONSE_BYTES or len(wire) != receipt["wire_bytes"]
            or _sha(wire) != receipt.get("wire_sha256")):
        raise ValueError("cached_wire_integrity_failed:" + name)
    decoded_size, decoded_sha = 0, hashlib.sha256()
    encoding = receipt.get("content_encoding")
    if encoding in (None, "", "identity"):
        iterator = (wire[i:i + 1024 * 1024] for i in range(0, len(wire), 1024 * 1024))
    elif encoding == "gzip":
        stream = gzip.GzipFile(fileobj=io.BytesIO(wire), mode="rb")
        iterator = iter(lambda: stream.read(1024 * 1024), b"")
    else:
        raise ValueError("cached_wire_encoding_unsupported:" + name)
    for block in iterator:
        decoded_size += len(block)
        if decoded_size > MAX_RESPONSE_BYTES or decoded_size > receipt["bytes"]:
            raise ValueError("cached_decoded_response_exceeds_receipt:" + name)
        decoded_sha.update(block)
    if (decoded_size != receipt["bytes"]
            or decoded_sha.hexdigest() != receipt.get("sha256")):
        raise ValueError("cached_decoded_integrity_failed:" + name)


def _plain_field(field: dict[str, Any]) -> dict[str, Any]:
    return {k: field[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in field}


def _schema(layer: dict[str, Any]) -> list[dict[str, Any]]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or not fields or any(not isinstance(x, dict) for x in fields):
        raise ValueError("source_field_roster_invalid")
    return [_plain_field(x) for x in fields]


def _urls(ids: list[int]) -> list[tuple[str, str]]:
    urls = [("portal_item", f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"),
            ("service_before", SERVICE_URL + "?f=pjson"), ("layer_before", LAYER_URL + "?f=pjson"),
            ("count_before", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
            ("ids_before", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"}))]
    for page, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        params = {"objectIds": ",".join(map(str, ids[start:start + PAGE_SIZE])), "outFields":"*",
                  "returnGeometry":"true", "returnTrueCurves":"true", "returnZ":"true", "returnM":"true",
                  "outSR":"3857", "orderByFields":"OBJECTID ASC", "f":"json"}
        urls.append((f"page_{page:04d}", LAYER_URL + "/query?" + urlencode(params)))
    urls += [("layer_after", LAYER_URL + "?f=pjson"), ("service_after", SERVICE_URL + "?f=pjson"),
             ("count_after", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
             ("ids_after", LAYER_URL + "/query?" + urlencode({"where":"1=1", "returnIdsOnly":"true", "f":"json"}))]
    return urls


def _esri_type(field: dict[str, Any]) -> pa.DataType:
    typ = field["type"]
    if typ in ("esriFieldTypeOID", "esriFieldTypeInteger"): return pa.int32()
    if typ == "esriFieldTypeSmallInteger": return pa.int16()
    if typ == "esriFieldTypeDouble": return pa.float64()
    if typ == "esriFieldTypeSingle": return pa.float32()
    if typ in ("esriFieldTypeString", "esriFieldTypeGUID", "esriFieldTypeGlobalID"): return pa.string()
    if typ == "esriFieldTypeDate": return pa.timestamp("ms", tz="UTC")
    raise ValueError("unsupported_source_field_type:" + str(typ))


def _ring_area(ring: list[list[float]]) -> float:
    # Translate to a local origin before shoelace accumulation. Global Web
    # Mercator coordinates can otherwise cancel small rings to a false zero.
    x0,y0=ring[0]
    return math.fsum((ring[i][0]-x0)*(ring[i+1][1]-y0)
                     -(ring[i+1][0]-x0)*(ring[i][1]-y0)
                     for i in range(len(ring)-1))/2


def _esri_polygon(geometry: dict[str, Any]) -> MultiPolygon:
    """Assign each hole to its nearest containing shell without ring repair."""
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    shells, holes = [], []
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
            raise ValueError("source_polygon_ring_invalid")
        polygon = Polygon(ring)
        if _ring_area(ring) < 0: shells.append((ring, polygon))
        else: holes.append((ring, polygon))
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    assigned: dict[int, list[list[list[float]]]] = {index: [] for index in range(len(shells))}
    for ring, hole in holes:
        # Esri permits islands (clockwise shells) inside counterclockwise
        # holes. A hole may therefore be contained by both its nearest island
        # and an outer shell; choose the smallest containing shell as parent.
        parents = [(shell_poly.area, index) for index, (_, shell_poly) in enumerate(shells)
                   if shell_poly.contains(hole)]
        if not parents:
            raise ValueError("source_polygon_hole_without_containing_shell")
        _, parent = min(parents)
        assigned[parent].append(ring)
    members = [Polygon(shell, assigned[index]) for index, (shell, _) in enumerate(shells)]
    return MultiPolygon(members)


def _feature_rows(captures: dict[str, tuple[bytes, dict[str, Any]]], fields: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # Keep only small control documents live. Page JSON is parsed, converted,
    # fingerprinted, and released one page at a time.
    docs = {name: json.loads(_capture_body(captures[name][0])) for name in (
        "service_before", "service_after", "layer_before", "layer_after",
        "count_before", "count_after", "ids_before", "ids_after", "ids_seed")}
    portal = json.loads(_capture_body(captures["portal_item"][0]))
    before, after = docs["layer_before"], docs["layer_after"]
    service_before, service_after = docs["service_before"], docs["service_after"]
    if portal.get("id") != ITEM_ID or portal.get("owner") != "aditya_sastry@fws.gov_fws" or portal.get("url") != SERVICE_URL:
        raise ValueError("official_fws_item_identity_changed")
    spatial = before.get("extent",{}).get("spatialReference",{})
    if (before.get("name") != LAYER_NAME or before.get("geometryType") != "esriGeometryPolygon"
            or spatial.get("latestWkid",spatial.get("wkid")) not in (3857,102100)
            or before.get("hasZ") is not False or before.get("hasM") is not False):
        raise ValueError("exact_final_polygon_layer_changed")
    if _schema(before) != fields or _schema(after) != fields:
        raise ValueError("source_schema_changed_during_capture")
    if before != after or service_before != service_after:
        raise ValueError("source_metadata_changed_during_capture")
    count0, count1 = docs["count_before"].get("count"), docs["count_after"].get("count")
    ids0, ids1 = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if not isinstance(ids0, list) or not isinstance(ids1, list) or sorted(ids0) != sorted(ids1) or count0 != count1 or count0 != len(ids0):
        raise ValueError("source_population_changed_or_count_mismatch")
    ids = sorted(ids0)
    if len(set(ids)) != len(ids): raise ValueError("declared_feature_key_not_unique")
    seed_ids = docs.get("ids_seed", {}).get("objectIds")
    if not isinstance(seed_ids, list) or sorted(seed_ids) != ids:
        raise ValueError("source_population_changed_before_capture")
    rows: list[dict[str, Any]] = []
    record_digest, geometry_digest = hashlib.sha256(), hashlib.sha256()
    names = [f["name"] for f in fields]
    key = "OBJECTID"
    seen_ids: list[int] = []
    null_geometries = invalid_geometries = 0
    for name in sorted(x for x in captures if x.startswith("page_")):
        doc = json.loads(_capture_body(captures[name][0]))
        if "error" in doc or doc.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncated")
        page_sr=doc.get("spatialReference",{})
        if page_sr.get("latestWkid",page_sr.get("wkid")) not in (3857,102100):
            raise ValueError("source_page_crs_changed")
        features = doc.get("features")
        if not isinstance(features, list): raise ValueError("source_page_features_missing")
        for row in features:
            attrs, geom = row.get("attributes"), row.get("geometry")
            if not isinstance(attrs, dict) or list(attrs) != names:
                raise ValueError("source_attribute_roster_or_order_changed")
            source_id = attrs.get(key)
            if not isinstance(source_id, int):
                raise ValueError("source_feature_key_invalid")
            seen_ids.append(source_id)
            _framed_hash_update(record_digest, row)
            _framed_hash_update(geometry_digest, [source_id, geom])
            native = _esri_polygon(geom) if isinstance(geom, dict) else None
            null_geometries += native is None
            invalid_geometries += native is not None and not native.is_valid
            row["_native_geometry"] = native
            # The lossless Esri JSON remains in the raw capture. Keep only the
            # native geometry and attributes in the candidate row tree.
            row.pop("geometry", None)
            rows.append(row)
        del features, doc
    if seen_ids != ids or len(rows) != count0:
        raise ValueError("source_page_feature_key_union_incomplete")
    record_sha = record_digest.hexdigest()
    geom_sha = geometry_digest.hexdigest()
    schema_sha = _sha(_canonical(fields))
    return rows, {"feature_count":count0, "source_ids_sha256":_sha(_canonical(ids)),
                  "source_record_sha256":record_sha, "source_geometry_sha256":geom_sha,
                  "source_schema_sha256":schema_sha, "source_id_field":key,
                  "source_unique_id_count":len(set(seen_ids)), "source_null_geometry_count":null_geometries,
                  "source_invalid_geometry_count":invalid_geometries, "source_fields":fields,
                  "source_crs":"EPSG:3857", "geometry_type":"Polygon",
                  "service_modified_is_not_release_date":True,
                  "baseline_feature_count":798,"baseline_comparison":"unresolved; current advertised population differs from pinned 2025 baseline"}


def _parquet(fields: list[dict[str, Any]], rows: list[dict[str, Any]]) -> bytes:
    columns = {f["name"]: pa.array([r["attributes"].get(f["name"]) for r in rows], type=_esri_type(f)) for f in fields}
    columns["geometry"] = pa.array([r["_native_geometry"].wkb if r["_native_geometry"] is not None else None for r in rows], type=pa.binary())
    schema = pa.schema([pa.field(f["name"], _esri_type(f), nullable=f.get("nullable", True)) for f in fields]
                       + [pa.field("geometry", pa.binary(), nullable=True)])
    geo = {"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":["MultiPolygon"],"crs":pyproj.CRS.from_epsg(3857).to_json_dict(),"edges":"planar"}}}
    sink = pa.BufferOutputStream(); pq.write_table(pa.Table.from_pydict(columns, schema=schema.with_metadata({b"geo":_canonical(geo)})), sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def capture_final_critical_habitat(storage: StagingStorageResource, *, version: str, capture_directory: str | None = None) -> dict[str, Any]:
    """Capture all stable IDs of the exact final polygon layer into local staging."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    root = Path(storage.local_dir).absolute(); root.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(root).free < 300_000_000: raise ValueError("disk_floor_before_acquisition")
    capture_root = Path(capture_directory or "/tmp/hifld-native-reconstruction/90/captures") / f"{version}-{uuid4().hex}"
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes | Path, dict[str, Any]]] = {}
    budget_roots = (ISSUE_EVIDENCE_ROOT, root)
    try:
        with httpx.Client(follow_redirects=False, timeout=90, headers={"User-Agent":"hifld-next-datasets/final-critical-habitat-draft"}) as client:
            seed, receipt = _fetch(client, LAYER_URL + "/query?" + urlencode({"where":"1=1","returnIdsOnly":"true","f":"json"}), capture_root, "ids_seed", budget_roots)
            ids = json.loads(_capture_body(seed)).get("objectIds")
            if not isinstance(ids, list) or not ids: raise ValueError("source_seed_ids_missing")
            ids = sorted(ids)
            captures["ids_seed"] = (seed, receipt)
            for name, url in _urls(ids):
                body, receipt = _fetch(client, url, capture_root, name, budget_roots)
                captures[name] = body, receipt
        fields = _schema(json.loads(_capture_body(captures["layer_before"][0])))
        rows, audit = _feature_rows(captures, fields)
    except Exception as exc:
        manifest = {"captured_at":datetime.now(timezone.utc).isoformat(),"complete":False,"error_code":type(exc).__name__,"receipts":{n:r for n,(_,r) in captures.items()}}
        (capture_root / "capture_manifest.json").write_bytes(_canonical(manifest)+b"\n")
        raise
    repo = Path(__file__).parents[3]
    identity = processing_identity(repo); identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    parquet = _parquet(fields, rows)
    relative = f"geoparquet/{DATASET_SLUG}.parquet"
    objects = {relative:{"bytes":len(parquet),"sha256":_sha(parquet)}}
    manifest = {"schema_version":1,"approved":False,"title":LAYER_NAME,
      "description":"FWS Final Critical Habitat polygon layer. The same service exposes proposed critical habitat separately as layer 2, which is excluded. Complete current release comparison remains unresolved.",
      "publisher":PUBLISHER,"agency":"U.S. Fish and Wildlife Service","source_url":LAYER_URL,
      "license":"unknown","source_modified":None,"date_issued":None,"date_modified":None,"processor":"HIFLD Next","code_commit":identity["code_commit"],
      "inputs":[{"input_id":n,"producer":PUBLISHER,"host":urlsplit(r["url"]).hostname,"role":"producer_publication","canonical_url":SERVICE_URL,"retrieval_url":r["url"],"observed_final_url":r["final_url"],"retrieved_at":r["observed_at"],"http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],"license":"unknown","attribution":"ArcGIS item copyrightText identifies U.S. Fish and Wildlife Service; license requires maintainer determination.","release_date":None,"release_date_unknown_reason":"The item and service expose no source release date; ArcGIS modified time is portal metadata only."} for n,(_,r) in sorted(captures.items())],
      "transformations":[{"operation":"Esri polygon rings to GeoParquet WKB","parameters":{"layer_id":0,"source_fields":"all fields preserved in producer order and declared physical types","source_ids":"OBJECTID preserved as key; every other ID field retained unchanged","geometry":"all source rings and coordinates represented in EPSG:3857; no dissolve, simplification, reprojection, filtering, or repair","candidate_status":"unapproved; baseline has 798 rows while live service advertises 803"}}],
      "lineage":"USFWS ArcGIS item 794de45b9d774d21aed3bf9b5313ee24 exposes layer 0 Final Critical Habitat Features and layer 2 Proposed Critical Habitat Features. This draft uses layer 0 only. Official distribution is indicated by the FWS-owned ArcGIS item and FWS copyright text; source release date and license remain unknown.",
      "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native candidate retains all fields, IDs and polygon boundaries; contract, baseline, rights and release approvals remain pending.","objects":[{"path":relative,**objects[relative]}]}}},"unsupported_output_formats":{"shapefile_zip":"No accepted schema/identifier mapping or packaging decision.","geopackage":"No accepted schema/identifier mapping or packaging decision.","pmtiles":"No generalized display product approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"portal_item_id":ITEM_ID,"portal_owner":"aditya_sastry@fws.gov_fws","official_layer_name":LAYER_NAME,"source_release_date":None,"license":"unknown","source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})
    candidate_version = content_derived_version(audit["source_record_sha256"],audit["source_geometry_sha256"],audit["source_schema_sha256"],identity)
    prefix = candidate_prefix(DATASET_SLUG,FILE_SLUG,candidate_version)
    receipts = {n:r for n,(_,r) in sorted(captures.items())}
    out = {relative:parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n","metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    out["metadata/source_capture_manifest.json"] = _canonical({"complete":True,"source_feature_count":audit["feature_count"],"receipts":receipts})+b"\n"
    out["metadata/candidate_ready.json"] = _canonical({"approved":False,"version":candidate_version,"objects":objects,"source_record_sha256":audit["source_record_sha256"],"source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})+b"\n"
    raw_descriptors: dict[str, dict[str, Any]] = {}
    for n,(_,r) in sorted(captures.items()):
        wire = (capture_root/f"{n}.wire").read_bytes()
        receipt_bytes = _canonical(r)+b"\n"
        raw_descriptors[f"raw/{n}.wire"] = {"bytes":len(wire),"sha256":_sha(wire)}
        raw_descriptors[f"raw/{n}.receipt.json"] = {"bytes":len(receipt_bytes),"sha256":_sha(receipt_bytes)}
    state = f"_construction-state/{DATASET_SLUG}/{candidate_version}.json"
    if storage.object_snapshot(state) is not None:
        sealed = json.loads(load_first_construction(storage,state,candidate_version,_sha(_canonical(identity))))
        first_payload = sealed.get("payload",{})
        first_receipts = first_payload.get("capture_receipts")
        first_objects = first_payload.get("objects")
        if (not isinstance(first_payload,dict)
                or set(first_payload) != {"processing_identity","source_record_sha256","source_schema_sha256","capture_receipts","objects"}
                or first_payload.get("processing_identity") != identity
                or not isinstance(first_receipts,dict) or not isinstance(first_objects,dict)
                or set(first_receipts) != set(captures)
                or set(first_objects) != set(out) | set(raw_descriptors)
                or first_payload.get("source_record_sha256") != audit["source_record_sha256"]
                or first_payload.get("source_schema_sha256") != audit["source_schema_sha256"]
                or first_objects.get(relative) != objects[relative]):
            raise ValueError("cached_first_candidate_source_or_native_object_changed")
        if any(not isinstance(descriptor,dict) or set(descriptor)!={"bytes","sha256"}
               or type(descriptor.get("bytes")) is not int or descriptor["bytes"] < 0
               or not isinstance(descriptor.get("sha256"),str) or len(descriptor["sha256"])!=64
               for descriptor in first_objects.values()):
            raise ValueError("cached_first_candidate_object_descriptors_invalid")
        # Source-specific wire-only receipts are lossless: verify the exact
        # encoded bytes and deterministically replay decoding against receipt.
        actual_keys=set(storage.list_prefix(prefix))
        expected_keys={f"{prefix}/{k}" for k in first_objects}
        derived={"metadata/quality_manifest.json","metadata/data_dictionary.json",
                 "metadata/resolved_provenance.json","metadata/source/quality_manifest.json",
                 "metadata/source/data_dictionary.json","metadata/source/source_manifest.json"}
        source_manifest=first_objects.get("metadata/source_manifest.json")
        if source_manifest:
            derived.add(f"metadata/source/version_{source_manifest['sha256']}_source_manifest.json")
        if not expected_keys <= actual_keys or actual_keys-expected_keys-{f"{prefix}/{x}" for x in derived}:
            raise ValueError("cached_candidate_storage_roster_changed")
        for name,descriptor in first_objects.items():
            key=f"{prefix}/{name}"; snapshot=storage.object_snapshot(key)
            if snapshot is None or descriptor != {"bytes":snapshot.size,"sha256":storage.sha256_key(key)} or storage.object_snapshot(key)!=snapshot:
                raise ValueError("cached_candidate_object_integrity_failed:"+name)
        validate_construction_code_identity(repo,identity["code_commit"],identity["files"])
        source_manifest=first_objects.get("metadata/source_manifest.json")
        for snapshot_name in ("metadata/source/source_manifest.json",
                              f"metadata/source/version_{source_manifest['sha256']}_source_manifest.json" if source_manifest else None):
            if snapshot_name is None:
                continue
            key=f"{prefix}/{snapshot_name}"
            if key in actual_keys:
                snapshot=storage.object_snapshot(key)
                if (snapshot is None or {"bytes":snapshot.size,"sha256":storage.sha256_key(key)} != source_manifest
                        or storage.object_snapshot(key) != snapshot):
                    raise ValueError("cached_catalog_source_snapshot_changed:"+snapshot_name)
        for n,r in first_receipts.items():
            if not isinstance(r,dict):
                raise ValueError("cached_receipt_invalid:"+n)
            wire=storage.read_key(f"{prefix}/raw/{n}.wire")
            _verify_wire_replay(wire,r,n)
            if storage.read_key(f"{prefix}/raw/{n}.receipt.json") != _canonical(r)+b"\n":
                raise ValueError("cached_original_capture_changed:"+n)
        observation_root=Path(capture_directory).absolute().parent if capture_directory else ISSUE_EVIDENCE_ROOT
        observation = observation_root / "observations" / f"{candidate_version}-{uuid4().hex}.json"
        observation.parent.mkdir(parents=True,exist_ok=True)
        observation.write_bytes(_canonical({"observed_at":"fresh-cache-replay","receipts":receipts})+b"\n")
        shutil.rmtree(capture_root)
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":candidate_version,
                "feature_count":audit["feature_count"],"source_null_geometry_count":audit["source_null_geometry_count"],
                "source_invalid_geometry_count":audit["source_invalid_geometry_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json",
                "reused":True,"source_release_accepted":False,"schema_contract_accepted":False,
                "id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,
                "mapping_approved":False,"release_accepted":False,"capture_directory":str(capture_root)}
    for name,body in out.items():
        _enforce_issue_budget(budget_roots,len(body))
        write_immutable(storage,f"{prefix}/{name}",body)
    for name,descriptor in raw_descriptors.items():
        if name.endswith(".wire"):
            body=(capture_root/name.removeprefix("raw/")).read_bytes()
        else:
            raw_name=name.removeprefix("raw/").removesuffix(".receipt.json")
            body=_canonical(captures[raw_name][1])+b"\n"
        _enforce_issue_budget(budget_roots,len(body))
        write_immutable(storage,f"{prefix}/{name}",body)
        raw_descriptors[name]=descriptor
        if name.endswith(".receipt.json"):
            # The exact wire is now staged with its receipt; discard only the
            # temporary decoded copy after the body's receipt hash was checked.
            raw_name=name.removeprefix("raw/").removesuffix(".receipt.json")
            body_path=capture_root/f"{raw_name}.body"
            if body_path.exists():
                decoded=body_path.read_bytes()
                receipt=captures[raw_name][1]
                if len(decoded)!=receipt["bytes"] or _sha(decoded)!=receipt["sha256"]:
                    raise ValueError("capture_decoded_body_changed:"+raw_name)
                body_path.unlink()
    descriptors = {n:{"bytes":len(b),"sha256":_sha(b)} for n,b in out.items()} | raw_descriptors
    for n,d in descriptors.items():
        key=f"{prefix}/{n}"; snap=storage.object_snapshot(key)
        if snap is None or snap.size != d["bytes"] or storage.sha256_key(key) != d["sha256"] or storage.object_snapshot(key) != snap: raise ValueError("candidate_readback_failed:"+n)
    fingerprint = _sha(_canonical(identity))
    seal_first_construction(storage,state,candidate_version,fingerprint,canonical_construction_bundle(candidate_version,fingerprint,{"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],"capture_receipts":receipts,"objects":descriptors}))
    validate_construction_code_identity(repo,identity["code_commit"],identity["files"])
    shutil.rmtree(capture_root)
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":candidate_version,"feature_count":audit["feature_count"],"source_null_geometry_count":audit["source_null_geometry_count"],"source_invalid_geometry_count":audit["source_invalid_geometry_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json","source_release_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False,"capture_directory":str(capture_root)}

"""Lossless, approval-gated capture of USACE's FUDS district layer."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import MultiPolygon, Polygon

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "formerly-used-defense-sites-district-boundaries"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://geospatial.sec.usace.army.mil/server/rest/services/Military/FUDS_Data/MapServer"
LAYER_ID = 9
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "FUDS Program District Boundaries"
EXPECTED_SCHEMA_SHA256 = "0b76c21c490ad121358884633e644a96775cdee16af97ce5fb29606c8e799b59"
EXPECTED_COUNT = 13
PAGE_SIZE = 50


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _field_contract(layer: dict) -> list[dict]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or not fields or any(not isinstance(f, dict) for f in fields):
        raise ValueError("source_fields_missing")
    # ArcGIS advertises SHAPE as a system field in layer metadata, while query
    # responses return its value in the separate geometry member. Pin it too.
    plain = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f}
             for f in fields]
    if _sha(_canonical(plain)) != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_drift")
    return [f for f in fields if f.get("name") != "SHAPE"]


def _arrow_type(field: dict) -> pa.DataType:
    return {
        "esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
        "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeDouble": pa.float64(),
        "esriFieldTypeSingle": pa.float32(), "esriFieldTypeString": pa.string(),
        "esriFieldTypeBlob": pa.binary(), "esriFieldTypeDate": pa.int64(),
    }[field["type"]]


def _ring_area(ring: list[list[float]]) -> float:
    x0, y0 = ring[0]
    return math.fsum((ring[i][0] - x0) * (ring[i + 1][1] - y0)
                     - (ring[i][1] - y0) * (ring[i + 1][0] - x0)
                     for i in range(len(ring) - 1)) / 2


def _esri_polygon(geometry: dict) -> MultiPolygon:
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        raise ValueError("source_polygon_rings_missing")
    shells, holes = [], []
    for ring in rings:
        if (not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]
                or any(not isinstance(p, list) or len(p) != 2 or
                       any(type(v) not in (int, float) or not math.isfinite(v) for v in p)
                       for p in ring)):
            raise ValueError("source_polygon_ring_or_dimensions_invalid")
        polygon = Polygon(ring)
        (shells if _ring_area(ring) < 0 else holes).append((ring, polygon))
    if not shells:
        raise ValueError("source_polygon_shell_missing")
    assigned: dict[int, list] = {i: [] for i in range(len(shells))}
    for ring, hole in holes:
        parents = [(shell.area, i) for i, (_, shell) in enumerate(shells) if shell.contains(hole)]
        if not parents:
            raise ValueError("source_polygon_hole_without_containing_shell")
        assigned[min(parents)[1]].append(ring)
    return MultiPolygon([Polygon(shell, assigned[i]) for i, (shell, _) in enumerate(shells)])


def _native_parquet(fields: list[dict], source_fields: list[dict], features: list[dict]) -> bytes:
    columns = {f["name"]: pa.array([x["attributes"][f["name"]] for x in features], type=_arrow_type(f))
               for f in fields}
    columns["geometry"] = pa.array([_esri_polygon(x["geometry"]).wkb for x in features], type=pa.binary())
    schema = pa.schema([pa.field(
        f["name"], _arrow_type(f), nullable=f.get("nullable", True),
        metadata={b"esri:source_field": _canonical(f)},
    ) for f in fields]
                       + [pa.field("geometry", pa.binary(), nullable=False)])
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["MultiPolygon"],
        "crs": pyproj.CRS.from_epsg(4326).to_json_dict(), "edges": "planar"}}}
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(columns, schema=schema.with_metadata({
        b"geo": _canonical(geo), b"hifld:source_esri_fields": _canonical(source_fields),
    })),
                   sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def capture_fuds_district_boundaries(storage: StagingStorageResource, *, version: str,
                                     capture_directory: str | None = None) -> dict:
    """Bracket a complete, ID-ordered native capture; retain exact response bytes."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    root = Path(capture_directory or "/tmp/hifld-native-reconstruction/94/captures") / version
    root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict]] = {}

    def get(client: httpx.Client, name: str, url: str, params: dict | None = None) -> dict:
        target = url + ("?" + urlencode(params) if params else "")
        body, receipt = capture_get(client, target, root, name)
        doc = json.loads(body)
        if not isinstance(doc, dict) or "error" in doc:
            raise ValueError("source_response_error:" + name)
        captures[name] = (body, receipt)
        return doc

    try:
        with httpx.Client(follow_redirects=False, timeout=120) as client:
            service = get(client, "service_before", SERVICE_URL, {"f": "pjson"})
            layer = get(client, "layer_before", LAYER_URL, {"f": "pjson"})
            if layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPolygon":
                raise ValueError("source_layer_identity_drift")
            fields = _field_contract(layer)
            caps = layer.get("advancedQueryCapabilities", {})
            if caps.get("supportsPagination") is not True or caps.get("supportsOrderBy") is not True:
                raise ValueError("source_ordered_pagination_not_supported")
            sr = layer.get("extent", {}).get("spatialReference", {})
            if sr.get("latestWkid", sr.get("wkid")) != 4326:
                raise ValueError("source_crs_drift")
            if layer.get("hasZ", False) is not False or layer.get("hasM", False) is not False:
                raise ValueError("source_geometry_dimensions_unpinned")
            count = get(client, "count_before", LAYER_URL + "/query",
                        {"where": "1=1", "returnCountOnly": "true", "f": "json"}).get("count")
            ids = get(client, "ids_before", LAYER_URL + "/query",
                      {"where": "1=1", "returnIdsOnly": "true", "f": "json"}).get("objectIds")
            if type(count) is not int or count != EXPECTED_COUNT or not isinstance(ids, list) or len(ids) != count:
                raise ValueError("source_initial_id_roster_invalid")
            if any(type(i) is not int for i in ids) or len(set(ids)) != count:
                raise ValueError("source_initial_id_roster_invalid")
            seed = sorted(ids)
            features = []
            for page_no, offset in enumerate(range(0, count, PAGE_SIZE)):
                page = get(client, f"page_{page_no:03d}", LAYER_URL + "/query", {
                    "where": "1=1", "outFields": "*", "returnGeometry": "true", "returnZ": "true",
                    "returnM": "true", "outSR": "4326", "orderByFields": "OBJECTID ASC",
                    "resultOffset": offset, "resultRecordCount": min(PAGE_SIZE, count - offset), "f": "json"})
                rows = page.get("features")
                expected = min(PAGE_SIZE, count - offset)
                if not isinstance(rows, list) or len(rows) != expected:
                    raise ValueError("source_page_incomplete")
                if page.get("exceededTransferLimit") is True and offset + expected >= count:
                    raise ValueError("source_final_page_exceeded_transfer_limit")
                page_sr = page.get("spatialReference", {})
                if page_sr.get("latestWkid", page_sr.get("wkid")) != 4326:
                    raise ValueError("source_page_crs_drift")
                names = [f["name"] for f in fields]
                if any(not isinstance(r, dict) or list(r.get("attributes", {})) != names for r in rows):
                    raise ValueError("source_record_schema_drift")
                if [r["attributes"].get("OBJECTID") for r in rows] != seed[offset:offset + expected]:
                    raise ValueError("source_page_id_slice_mismatch")
                features.extend(rows)
            layer_after = get(client, "layer_after", LAYER_URL, {"f": "pjson"})
            service_after = get(client, "service_after", SERVICE_URL, {"f": "pjson"})
            count_after = get(client, "count_after", LAYER_URL + "/query",
                              {"where": "1=1", "returnCountOnly": "true", "f": "json"}).get("count")
            ids_after = get(client, "ids_after", LAYER_URL + "/query",
                            {"where": "1=1", "returnIdsOnly": "true", "f": "json"}).get("objectIds")
        if layer_after != layer or service_after != service or count_after != count or sorted(ids_after or []) != seed:
            raise ValueError("source_changed_during_capture")
        for row in features:
            _esri_polygon(row.get("geometry", {}))
        if len(features) != count or sorted(r["attributes"]["OBJECTID"] for r in features) != seed:
            raise ValueError("source_feature_population_incomplete")
    except Exception as exc:
        if captures:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
        raise

    schema_sha = EXPECTED_SCHEMA_SHA256
    records_sha = _sha(_canonical([x["attributes"] for x in features]))
    geometry_sha = _sha(_canonical([[x["attributes"]["OBJECTID"], x["geometry"]] for x in features]))
    identity = processing_identity(Path(__file__).parents[3])
    native = _native_parquet(fields, layer["fields"], features)
    version = content_derived_version(records_sha, geometry_sha, schema_sha, identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    native_name = f"geoparquet/{DATASET_SLUG}.parquet"
    objects = {native_name: native}
    receipts = {n: r for n, (_, r) in sorted(captures.items())}
    inputs = []
    for name, receipt in sorted(receipts.items()):
        parsed = urlsplit(receipt["url"])
        base_url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
        inputs.append({
            "input_id": name, "producer": "U.S. Army Corps of Engineers (USACE)",
            "host": parsed.hostname, "role": "producer_publication",
            "canonical_url": base_url, "retrieval_url": receipt["url"],
            "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
            "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "license": "Unreviewed; no license claim made.",
            "attribution": "USACE service attribution and terms have not been reviewed.",
            "release_date": None,
            "release_date_unknown_reason": "USACE layer metadata does not identify a producer release date.",
        })
    manifest = {
        "schema_version": 1, "processor": "HIFLD Next", "approved": False, "title": LAYER_NAME,
        "publisher": "U.S. Army Corps of Engineers (USACE), Military/FUDS_Data MapServer",
        "agency": "U.S. Army Corps of Engineers", "source_url": LAYER_URL,
        "source_release_date": None, "source_release_date_unknown": True,
        "feature_count": count, "native_id_column": "OBJECTID", "source_schema_sha256": schema_sha,
        "source_crs_epsg": 4326, "geometry_type": "Polygon", "geometry_dimensions": "2D",
        "rights_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "mapping_approved": False, "release_accepted": False,
        "code_commit": identity["code_commit"], "inputs": inputs,
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID, "source_fields": "all query attributes in order",
                                            "geometry": "native 2D rings; no repair or simplification",
                                            "approval": False}}],
        "unsupported_output_formats": {
            "shapefile_zip": "No approved mapping for publishing the complete native USACE schema.",
            "geopackage": "No approved mapping for publishing the complete native USACE schema.",
            "pmtiles": "No approved display simplification or public field mapping.",
        },
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source attributes, OBJECTID, and 2D polygons are retained without a public mapping; approval remains open.",
            "objects": [{"path": native_name, "bytes": len(native), "sha256": _sha(native)}]}}},
        "unapproved_candidate": True,
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    outputs = {native_name: native,
               "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n",
               "metadata/source_audit.json": _canonical({
                   "feature_count": count, "source_ids": seed, "all_ids_captured_once": True,
                   "source_record_sha256": records_sha, "source_geometry_sha256": geometry_sha,
                   "source_schema_sha256": schema_sha,
               }) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    expected_objects = {k: {"bytes": len(v), "sha256": _sha(v)} for k, v in outputs.items()}
    outputs["metadata/candidate_ready.json"] = _canonical({
        "approved": False, "version": version, "objects": expected_objects,
        "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    }) + b"\n"
    expected_objects = {k: {"bytes": len(v), "sha256": _sha(v)} for k, v in outputs.items()}
    fingerprint = _sha(_canonical(identity))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload", {})
        old_objects = payload.get("objects")
        if (payload.get("processing_identity") != identity
                or payload.get("source_record_sha256") != records_sha
                or payload.get("source_schema_sha256") != schema_sha
                or not isinstance(old_objects, dict) or set(old_objects) != set(expected_objects)):
            raise ValueError("cached_first_candidate_identity_or_source_changed")
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                fingerprint, identity, records_sha, schema_sha,
                                old_objects, payload.get("capture_receipts", {}))
        for name, descriptor in old_objects.items():
            if name == native_name and descriptor != expected_objects[name]:
                raise ValueError("cached_native_candidate_changed")
        return {"version": version, "prefix": prefix, "feature_count": count,
                "native_id_column": "OBJECTID", "source_schema_sha256": schema_sha,
                "candidate_approved": False, "reused": True}
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
    seal_ready_candidate(storage, state_key, version, fingerprint, {
        "processing_identity": identity, "source_record_sha256": records_sha,
        "source_schema_sha256": schema_sha, "capture_receipts": receipts,
        "objects": expected_objects,
    })
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"version": version, "prefix": prefix, "feature_count": count,
            "native_id_column": "OBJECTID", "source_schema_sha256": schema_sha,
            "candidate_approved": False, "source_release_accepted": False,
            "freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}

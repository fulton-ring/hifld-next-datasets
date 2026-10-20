"""Bounded, fail-closed draft capture of the official USGS plate-boundary layer."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx

from dagster_hifld.resources import StagingStorageResource

DATASET_SLUG = FILE_SLUG = "plate-boundaries"
LAYER_URL = "https://earthquake.usgs.gov/arcgis/rest/services/eq/map_plateboundaries/MapServer/1"
QUERY_URL = LAYER_URL + "/query"
EXPECTED_COUNT = 1175
EXPECTED_ID_SHA256 = "16439bcc2d7b9a12e48447a351cd4d769a8e1ad0b4776ae59c2c4963b9b6b8e0"
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID", None),
    ("Shape", "esriFieldTypeGeometry", None),
    ("NAME", "esriFieldTypeString", 200),
    ("LABEL", "esriFieldTypeString", 50),
    ("Shape_Length", "esriFieldTypeDouble", None),
]
BATCH_SIZE = 100
EXPECTED_NULL_COUNTS = {"NAME": 3, "LABEL": 0, "Shape_Length": 0}
EXPECTED_EXTENT = {"xmin": -20037507.067161843, "ymin": -9921601.01373757,
                   "xmax": 20037507.067161843, "ymax": 22830719.838254996}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _get(client: httpx.Client, params: dict[str, object], raw: dict[str, bytes], receipts: dict[str, dict], name: str):
    url = QUERY_URL + "?" + urlencode(params)
    response = client.get(url)
    response.raise_for_status()
    body = response.content
    doc = json.loads(body)
    if not isinstance(doc, dict) or "error" in doc:
        raise ValueError("source_response_error:" + name)
    raw[name] = body
    receipts[name] = {"url": url, "status": response.status_code,
        "retrieved_at": datetime.now(timezone.utc).isoformat(), "bytes": len(body),
        "sha256": _sha(body), "content_type": response.headers.get("content-type")}
    return doc


def validate_capture(layer: dict, count_before: dict, ids_before: dict, pages: list[dict],
                     count_after: dict, ids_after: dict, layer_after: dict | None = None) -> tuple[list[dict], dict]:
    """Reject incomplete or schema/ID/geometry drift; never infer a replacement ID."""
    fields = layer.get("fields")
    actual = [(f.get("name"), f.get("type"), f.get("length")) for f in fields or []]
    if actual != EXPECTED_FIELDS or layer.get("geometryType") != "esriGeometryPolyline":
        raise ValueError("source_schema_or_geometry_drift")
    sr = layer.get("spatialReference", {})
    if sr.get("latestWkid", sr.get("wkid")) != 3857:
        raise ValueError("source_crs_drift")
    extent = layer.get("extent", {})
    if {key: extent.get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT:
        raise ValueError("source_coverage_extent_drift")
    if layer_after is not None and layer_after != layer:
        raise ValueError("source_layer_changed_during_capture")
    ids = ids_before.get("objectIds")
    if (not isinstance(ids, list) or any(type(v) is not int for v in ids)
            or len(ids) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT
            or _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA256):
        raise ValueError("source_id_roster_drift")
    if count_before.get("count") != EXPECTED_COUNT or count_after.get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    if ids_after.get("objectIds") != ids:
        raise ValueError("source_changed_during_capture")
    features = []
    seen = set()
    null_counts = {name: 0 for name in EXPECTED_NULL_COUNTS}
    for page in pages:
        if page.get("exceededTransferLimit") is True:
            raise ValueError("source_transfer_limit")
        rows = page.get("features")
        if not isinstance(rows, list) or not rows:
            raise ValueError("source_empty_or_invalid_page")
        for feature in rows:
            props = feature.get("properties")
            geom = feature.get("geometry")
            if not isinstance(props, dict) or set(props) != {f[0] for f in EXPECTED_FIELDS if f[1] != "esriFieldTypeGeometry"}:
                raise ValueError("source_attribute_roster_drift")
            oid = props.get("OBJECTID")
            if type(oid) is not int or oid not in ids or oid in seen:
                raise ValueError("source_feature_id_drift")
            for name in ("NAME", "LABEL"):
                value = props.get(name)
                if value is None:
                    null_counts[name] += 1
                elif not isinstance(value, str):
                    raise ValueError("source_attribute_value_type_drift")
            length = props.get("Shape_Length")
            if length is None:
                null_counts["Shape_Length"] += 1
            elif type(length) not in (int, float):
                raise ValueError("source_shape_length_type_drift")
            if not isinstance(geom, dict) or geom.get("type") not in {"MultiLineString", "LineString"}:
                raise ValueError("source_geometry_type_drift")
            coordinates = geom.get("coordinates")
            if not isinstance(coordinates, list) or not coordinates:
                raise ValueError("source_geometry_coordinates_missing")
            seen.add(oid)
            features.append(feature)
    if seen != set(ids) or len(features) != EXPECTED_COUNT:
        raise ValueError("source_feature_population_incomplete")
    if null_counts != EXPECTED_NULL_COUNTS:
        raise ValueError("source_nullability_drift")
    features.sort(key=lambda feature: feature["properties"]["OBJECTID"])
    return features, {"feature_count": len(features), "id_sha256": _sha(_canonical(sorted(seen))),
        "schema_sha256": _sha(_canonical(fields)), "id_min": min(seen), "id_max": max(seen),
        "native_crs": "EPSG:3857", "response_crs": "EPSG:4326", "geometry_type": "polyline",
        "source_extent": EXPECTED_EXTENT,
        "attribute_null_counts": null_counts,
        "source_population_complete": True}


def capture_plate_boundaries(storage: StagingStorageResource, *, version: str) -> dict[str, object]:
    """Capture to a candidate namespace only; accepted releases are never overwritten."""
    if not isinstance(storage, StagingStorageResource):
        raise ValueError("staging_storage_required")
    raw: dict[str, bytes] = {}
    receipts: dict[str, dict] = {}
    with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                      headers={"User-Agent": "hifld-next-datasets/plate-boundaries-draft"}) as client:
        layer_response = client.get(LAYER_URL + "?f=json")
        layer_response.raise_for_status()
        layer = layer_response.json()
        if "error" in layer:
            raise ValueError("source_layer_metadata_error")
        raw["layer"] = layer_response.content
        receipts["layer"] = {"url": LAYER_URL + "?f=json", "status": layer_response.status_code,
            "retrieved_at": datetime.now(timezone.utc).isoformat(), "bytes": len(layer_response.content),
            "sha256": _sha(layer_response.content)}
        count_before = _get(client, {"where": "1=1", "returnCountOnly": "true", "f": "json"}, raw, receipts, "count_before")
        ids_before = _get(client, {"where": "1=1", "returnIdsOnly": "true", "f": "json"}, raw, receipts, "ids_before")
        ids = ids_before.get("objectIds")
        if not isinstance(ids, list) or any(type(v) is not int for v in ids):
            raise ValueError("source_id_roster_unavailable")
        if (count_before.get("count") != EXPECTED_COUNT or len(ids) != EXPECTED_COUNT
                or len(set(ids)) != EXPECTED_COUNT
                or _sha(_canonical(sorted(ids))) != EXPECTED_ID_SHA256):
            raise ValueError("source_population_drift_before_geometry_capture")
        pages = []
        for index, start in enumerate(range(0, len(ids), BATCH_SIZE)):
            page = _get(client, {"objectIds": ",".join(map(str, ids[start:start+BATCH_SIZE])),
                "outFields": "*", "returnGeometry": "true", "outSR": "4326", "f": "geojson"},
                raw, receipts, f"page_{index:03d}")
            pages.append(page)
        count_after = _get(client, {"where": "1=1", "returnCountOnly": "true", "f": "json"}, raw, receipts, "count_after")
        ids_after = _get(client, {"where": "1=1", "returnIdsOnly": "true", "f": "json"}, raw, receipts, "ids_after")
        layer_after_response = client.get(LAYER_URL + "?f=json")
        layer_after_response.raise_for_status()
        layer_after = layer_after_response.json()
        raw["layer_after"] = layer_after_response.content
        receipts["layer_after"] = {"url": LAYER_URL + "?f=json", "status": layer_after_response.status_code,
            "retrieved_at": datetime.now(timezone.utc).isoformat(), "bytes": len(layer_after_response.content),
            "sha256": _sha(layer_after_response.content)}
    features, audit = validate_capture(layer, count_before, ids_before, pages, count_after, ids_after, layer_after)
    capture_evidence_sha256 = _sha(_canonical(receipts))
    audit["source_capture_evidence_sha256"] = capture_evidence_sha256
    audit["source_receipt_count"] = len(receipts)
    response = _canonical({"type": "FeatureCollection", "features": features}) + b"\n"
    if version in {"", ".", ".."} or "/" in version or "\\" in version:
        raise ValueError("unsafe_capture_version")
    # Keep unapproved data outside the version-discovery sensor's ingest tree.
    prefix = f"_candidates/{DATASET_SLUG}/{version}/"
    manifest = {"schema_version": 1, "approved": False, "publisher": "U.S. Geological Survey",
        "source_url": LAYER_URL, "release_date": None, "lineage": "USGS service metadata cites Bird (2003) and DeMets et al. (2010); 2024 HIFLD archive equivalence is unresolved.",
        "inputs": receipts, "candidate_only": True,
        "source_capture_evidence_sha256": capture_evidence_sha256,
        "pending_decisions": ["source vintage and archive relationship", "schema and ID contract", "baseline", "coverage", "rights and attribution", "release"]}
    outputs = {"geojson/plate-boundaries.geojson": response,
        "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/candidate_ready.json": _canonical({**audit, "approved": False, "release_accepted": False}) + b"\n"}
    outputs.update({f"raw/{name}.body": body for name, body in raw.items()})
    outputs.update({f"raw/{name}.receipt.json": _canonical(receipt) + b"\n" for name, receipt in receipts.items()})
    for path, body in outputs.items():
        storage.write_key(prefix + path, body)
    return {**audit, "version": version, "bytes": len(response), "sha256": _sha(response),
        "candidate_prefix": prefix, "raw_response_count": len(raw), "source_capture_evidence_sha256": capture_evidence_sha256,
        "release_accepted": False, "source_vintage_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "baseline_accepted": False, "coverage_accepted": False,
        "rights_accepted": False}

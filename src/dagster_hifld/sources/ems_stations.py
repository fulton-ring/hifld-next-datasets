"""Official USGS TNMCorps fire/EMS layer draft; unaccepted source lineage.

The USGS layer is live and is not asserted equivalent to the HIFLD/DataLumos
historical release. Raw paginated JSON is retained, as are native EPSG:3857 XY,
all wire attributes and the producer's field declaration document.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
from shapely.geometry import Point

from dagster_hifld.release_approval import verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

SLUG = "fire-and-emergency-medical-service-ems-stations"
URL = "https://carto.nationalmap.gov/arcgis/rest/services/structures/MapServer/51"
LANDING = "https://www.usgs.gov/core-science-systems/ngp/tnm-corps/fire-ems-station"
MAPSERVER = "https://carto.nationalmap.gov/arcgis/rest/services/structures/MapServer"
COUNT = 52010
PAGE_SIZE = 2000
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_CAPTURE_BYTES = 80_000_000
SCHEMA_SHA256 = "4ef3e6b267361951434b64e7d08cc839033707b6f288c1c1445545c6142c8abb"
IDS_SHA256 = "1c10df0117100ca8a5fe721034f741ddd46a56d28427a8dea732d8c6c3ed43dd"
ROWS_SHA256 = "6f4b0f5a7377e0d070ea45b78caa3e4e013707ddf409a71475308f513a5aa0a2"
EXPECTED_WIRE_FIELDS = (
    "objectid", "permanent_identifier", "source_featureid", "source_datasetid",
    "source_datadesc", "source_originator", "loaddate", "ftype", "fcode", "name",
    "pointlocationtype", "admintype", "addressbuildingname", "address", "city",
    "state", "zipcode", "gnis_id", "globalid",
)

class EMSQuarantineError(ValueError):
    """Incomplete, malformed, or drifted official layer response."""


def encode(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def canonical_hash(value: object) -> str:
    return digest(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())


def strict_json(raw: bytes) -> dict:
    def pairs(items):
        out = {}
        for key, val in items:
            if key in out:
                raise EMSQuarantineError(f"duplicate_json_member:{key}")
            out[key] = val
        return out
    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda _: (_ for _ in ()).throw(EMSQuarantineError("nonfinite_json")))
    if not isinstance(value, dict) or "error" in value:
        raise EMSQuarantineError("invalid_or_arcgis_error_json")
    return value


def require_local(storage: StagingStorageResource) -> None:
    if isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise EMSQuarantineError("draft_requires_explicit_isolated_local_staging")


def _url(name: str, offset: int | None = None) -> str:
    q = URL + "/query"
    if name == "layer": return URL + "?f=pjson"
    if name in {"count_before", "count_after"}: return q + "?where=1%3D1&returnCountOnly=true&f=json"
    if name in {"ids_before", "ids_after"}: return q + "?where=1%3D1&returnIdsOnly=true&f=json"
    if name.startswith("page_"):
        return q + f"?where=1%3D1&outFields=%2A&returnGeometry=true&outSR=3857&orderByFields=OBJECTID&resultOffset={offset}&resultRecordCount={PAGE_SIZE}&f=pjson"
    raise EMSQuarantineError("unknown_capture_name")


def _capture(capture_dir: Path | None) -> tuple[dict[str, bytes], list[dict]]:
    if capture_dir is not None:
        receipt_doc = json.loads((capture_dir / "receipts.json").read_text())
        if not isinstance(receipt_doc, list): raise EMSQuarantineError("capture_receipt_array_required")
        bodies, receipts = {}, []
        for receipt in receipt_doc:
            if not isinstance(receipt, dict) or not isinstance(receipt.get("capture"), str):
                raise EMSQuarantineError("capture_receipt_shape_invalid")
            name = receipt["capture"]
            if name in bodies: raise EMSQuarantineError("duplicate_capture_receipt")
            offset = int(name[5:]) * PAGE_SIZE if name.startswith("page_") and name[5:].isdigit() else None
            expected = urlsplit(_url(name, offset))
            raw = (capture_dir / (name + ".json")).read_bytes()
            observed_urls = [urlsplit(receipt.get(k, "")) for k in ("url", "final_url")]
            expected_query = sorted(parse_qsl(expected.query, keep_blank_values=True))
            if (type(receipt.get("http_status")) is not int or receipt["http_status"] != 200
                    or type(receipt.get("bytes")) is not int or receipt["bytes"] != len(raw)
                    or receipt.get("sha256") != digest(raw)
                    or any(u.scheme != "https" or u.netloc != expected.netloc or u.path != expected.path
                           or u.fragment or sorted(parse_qsl(u.query, keep_blank_values=True)) != expected_query
                           for u in observed_urls)):
                raise EMSQuarantineError(f"capture_receipt_mismatch:{name}")
            bodies[name] = raw; receipts.append(receipt)
        expected_names = {"layer", "count_before", "ids_before", "ids_after", "count_after"} | {f"page_{i:03d}" for i in range((COUNT + PAGE_SIZE - 1)//PAGE_SIZE)}
        if set(bodies) != expected_names or sum(map(len, bodies.values())) > MAX_CAPTURE_BYTES:
            raise EMSQuarantineError("capture_receipt_set_or_storage_budget_mismatch")
        return bodies, receipts
    bodies, receipts = {}, []
    names = ["layer", "count_before", "ids_before"]
    with httpx.Client(timeout=90, headers={"Accept-Encoding": "identity", "User-Agent": "HIFLD local draft source adapter"}, follow_redirects=False) as client:
        for name in names:
            _get(client, name, _url(name), bodies, receipts)
        count = strict_json(bodies["count_before"]).get("count")
        ids = strict_json(bodies["ids_before"]).get("objectIds")
        if type(count) is not int or not isinstance(ids, list) or len(ids) != count:
            raise EMSQuarantineError("preflight_count_and_id_mismatch")
        for i, start in enumerate(range(0, len(ids), PAGE_SIZE)):
            name = f"page_{i:03d}"
            _get(client, name, _url(name, start), bodies, receipts)
            if sum(map(len, bodies.values())) > MAX_CAPTURE_BYTES:
                raise EMSQuarantineError("capture_exceeds_storage_budget")
        for name in ("ids_after", "count_after"):
            _get(client, name, _url(name), bodies, receipts)
    return bodies, receipts


def _get(client, name: str, url: str, bodies: dict, receipts: list) -> None:
    started = datetime.now(timezone.utc).isoformat()
    with client.stream("GET", url) as response:
        if response.status_code != 200:
            raise EMSQuarantineError(f"http_status:{name}:{response.status_code}")
        chunks = bytearray()
        for chunk in response.iter_bytes(65536):
            chunks.extend(chunk)
            if len(chunks) > MAX_RESPONSE_BYTES:
                raise EMSQuarantineError(f"response_size_limit:{name}")
        raw = bytes(chunks)
        request_url, final_url = str(response.request.url), str(response.url)
        content_type = response.headers.get("content-type")
    strict_json(raw)
    bodies[name] = raw
    receipts.append({"capture": name, "url": request_url, "final_url": final_url,
        "http_status": response.status_code, "fetched_at": started, "bytes": len(raw), "sha256": digest(raw),
        "content_type": content_type})


def validate(bodies: dict[str, bytes]) -> tuple[list[dict], dict, dict]:
    layer = strict_json(bodies["layer"])
    before, after = strict_json(bodies["count_before"]), strict_json(bodies["count_after"])
    ids1 = strict_json(bodies["ids_before"]).get("objectIds")
    ids2 = strict_json(bodies["ids_after"]).get("objectIds")
    if layer.get("name") != "Fire Stations/EMS Stations" or layer.get("geometryType") != "esriGeometryPoint":
        raise EMSQuarantineError("layer_identity_or_geometry_type_drift")
    if layer.get("maxRecordCount") != PAGE_SIZE:
        raise EMSQuarantineError("max_record_count_drift")
    sr = layer.get("extent", {}).get("spatialReference", {})
    if sr.get("latestWkid", sr.get("wkid")) != 3857:
        raise EMSQuarantineError("source_crs_drift")
    fields = layer.get("fields")
    if not isinstance(fields, list) or canonical_hash(fields) != SCHEMA_SHA256:
        raise EMSQuarantineError("source_schema_drift")
    declared = {field["name"].casefold(): field["type"] for field in fields if isinstance(field, dict) and isinstance(field.get("name"), str)}
    if len(declared) != len(fields): raise EMSQuarantineError("source_duplicate_field_declaration")
    if type(before.get("count")) is not int or before["count"] != COUNT or type(after.get("count")) is not int or after["count"] != COUNT:
        raise EMSQuarantineError("source_count_drift")
    if (not isinstance(ids1, list) or not isinstance(ids2, list) or len(ids1) != COUNT or len(ids2) != COUNT
            or any(type(value) is not int for value in ids1 + ids2)):
        raise EMSQuarantineError("source_id_count_drift")
    ids1, ids2 = sorted(ids1), sorted(ids2)
    if ids1 != ids2 or len(set(ids1)) != COUNT or canonical_hash(ids1) != IDS_SHA256:
        raise EMSQuarantineError("source_id_drift")
    page_names = sorted(name for name in bodies if name.startswith("page_"))
    if page_names != [f"page_{i:03d}" for i in range((COUNT + PAGE_SIZE - 1)//PAGE_SIZE)]:
        raise EMSQuarantineError("source_page_set_incomplete")
    features = []
    for i, name in enumerate(page_names):
        body = strict_json(bodies[name]); rows = body.get("features")
        expected = min(PAGE_SIZE, COUNT - i*PAGE_SIZE)
        if not isinstance(rows, list) or len(rows) != expected:
            raise EMSQuarantineError(f"source_page_count_mismatch:{name}")
        if body.get("geometryType") != "esriGeometryPoint" or body.get("spatialReference", {}).get("latestWkid", body.get("spatialReference", {}).get("wkid")) != 3857:
            raise EMSQuarantineError(f"page_crs_or_geometry_drift:{name}")
        features.extend(rows)
    features.sort(key=lambda row: row.get("attributes", {}).get("objectid", -1))
    row_ids = [row.get("attributes", {}).get("objectid") for row in features]
    if row_ids != ids1: raise EMSQuarantineError("feature_ids_do_not_reconcile")
    if any(tuple(row.get("attributes", {}).keys()) != EXPECTED_WIRE_FIELDS for row in features):
        raise EMSQuarantineError("wire_attribute_schema_drift")
    permanent = [row["attributes"].get("permanent_identifier") for row in features]
    if any(not isinstance(v, str) or not v for v in permanent) or len(set(permanent)) != COUNT:
        raise EMSQuarantineError("permanent_identifier_missing_or_duplicate")
    for row in features:
        geom = row.get("geometry")
        if not isinstance(geom, dict) or set(geom) != {"x", "y"}:
            raise EMSQuarantineError("point_geometry_missing_or_unexpected")
        if any(type(geom[k]) not in (int, float) or not math.isfinite(geom[k]) for k in ("x", "y")):
            raise EMSQuarantineError("native_xy_invalid")
        attrs = row["attributes"]
        if attrs["ftype"] != 740 or attrs["fcode"] != 74026:
            raise EMSQuarantineError("layer_classification_drift")
        for key, val in attrs.items():
            kind = declared.get(key.casefold())
            if kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeDate"} and val is not None and type(val) is not int:
                raise EMSQuarantineError(f"attribute_type_drift:{key}")
            if kind in {"esriFieldTypeString", "esriFieldTypeGlobalID"} and val is not None and type(val) is not str:
                raise EMSQuarantineError(f"attribute_type_drift:{key}")
    row_identity = [{"attributes": row["attributes"], "geometry": row["geometry"]} for row in features]
    if canonical_hash(row_identity) != ROWS_SHA256:
        raise EMSQuarantineError("source_rows_or_geometry_drift")
    if len(features) != COUNT:
        raise EMSQuarantineError("source_feature_count_drift")
    quality = {"feature_count": COUNT, "ids_sha256": IDS_SHA256, "row_fingerprint": ROWS_SHA256,
        "native_crs": "EPSG:3857", "coordinates_transformed": False,
        "field_declarations": fields, "wire_attribute_names": list(EXPECTED_WIRE_FIELDS),
        "wire_name_observation": "ArcGIS layer metadata declares upper-case field names while returned attribute keys are lower-case, including for explicitly upper-case outFields. Raw responses and case-folded type reconciliation are retained; no attribute values are renamed or changed.",
        "unique_permanent_identifiers": True, "fcode_ftype": {"740/74026": COUNT}, "promotion_approved": False}
    return features, layer, quality


def _arrow(features: list[dict], layer: dict) -> pa.Table:
    by_name = {field["name"].casefold(): field["type"] for field in layer["fields"]}
    arrays, schema = [], []
    for name in EXPECTED_WIRE_FIELDS:
        kind = by_name[name]
        values = [feature["attributes"][name] for feature in features]
        if kind in {"esriFieldTypeOID", "esriFieldTypeDate"}: dtype = pa.int64()
        elif kind == "esriFieldTypeInteger": dtype = pa.int32()
        elif kind in {"esriFieldTypeString", "esriFieldTypeGlobalID"}: dtype = pa.string()
        else: raise EMSQuarantineError(f"unsupported_source_type:{name}:{kind}")
        if kind == "esriFieldTypeInteger" and any(v is not None and (type(v) is not int or not -(2**31) <= v < 2**31) for v in values):
            raise EMSQuarantineError(f"integer_range_loss:{name}")
        schema.append(pa.field(name, dtype, nullable=any(v is None for v in values)))
        arrays.append(pa.array(values, type=dtype))
    points = [Point(feature["geometry"]["x"], feature["geometry"]["y"]).wkb for feature in features]
    schema.append(pa.field("shape", pa.binary(), nullable=False)); arrays.append(pa.array(points, type=pa.binary()))
    metadata = {b"geo": json.dumps({"version":"1.1.0","primary_column":"shape","columns":{"shape":{"encoding":"WKB","geometry_types":["Point"],"crs":CRS.from_epsg(3857).to_json_dict()}}},sort_keys=True,separators=(",",":")).encode(),
        b"hifld:source_fields": encode(layer["fields"]), b"hifld:acceptance": b"unapproved_draft",
        b"hifld:coordinate_rule": b"Native service x/y retained exactly as returned in EPSG:3857; no reprojection or repair."}
    return pa.Table.from_arrays(arrays, schema=pa.schema(schema, metadata=metadata))


def _immutable(storage: StagingStorageResource, key: str, body: bytes) -> str:
    prior = storage.object_snapshot(key)
    if prior is None: storage.write_key_if_unchanged(key, body, None)
    elif storage.read_key(key) != body or storage.object_snapshot(key) != prior:
        raise EMSQuarantineError(f"immutable_object_conflict:{key}")
    return storage._ensure_prefixed(key)


def stage_candidate(storage: StagingStorageResource, *, capture_dir: Path | None = None, code_commit: str | None = None) -> dict:
    require_local(storage)
    bodies, receipts = _capture(capture_dir)
    features, layer, quality = validate(bodies)
    raw_hashes = {name: digest(raw) for name, raw in sorted(bodies.items())}
    raw_identity = canonical_hash(raw_hashes)
    processor = verifier_implementation_sha256()
    version = "v0.0.0-draft.usgs." + canonical_hash({"raw": raw_identity, "processor": processor, "retrieval_receipts": receipts})[:20]
    prefix = f"{SLUG}/{SLUG}/{version}"
    raw_keys = {}
    for name, raw in sorted(bodies.items()):
        raw_keys[name] = _immutable(storage, f"_raw-captures/{SLUG}/{digest(raw)}/{name}.json", raw)
    receipt_bytes = encode(receipts)
    _immutable(storage, f"{prefix}/raw/receipts/{digest(receipt_bytes)}.json", receipt_bytes)
    table = _arrow(features, layer)
    import io
    buffer = io.BytesIO(); pq.write_table(table, buffer, compression="zstd", use_dictionary=False, write_statistics=True, version="2.6")
    parquet = buffer.getvalue()
    candidate_key = _immutable(storage, f"{prefix}/geoparquet/{SLUG}.parquet", parquet)
    source_key = candidate_key
    manifest = {"schema_version":1,"processor":"HIFLD Next","title":"Fire and Emergency Medical Service (EMS) Stations — working draft",
        "publisher":"U.S. Geological Survey, The National Map Corps","agency":"U.S. Geological Survey",
        "lineage":"Official USGS The National Map Corps live Fire Stations/EMS Stations MapServer layer 51. This is a maintained service snapshot and is not asserted equivalent to the historical HIFLD/DataLumos release.",
        "source_url":LANDING,"documentation_url":LANDING,"live_source_layer":URL,"license":"Rights review pending; consult USGS terms and disclaimers.",
        "baseline_accepted":False,"contract_approved":False,"promotion_approved":False,"data_lumos_equivalence_claimed":False,
        "code_commit":code_commit or f"installed-package-sha256:{processor}","code_identity_scheme":"run-tag" if code_commit else "installed-package-sha256-v1",
        "inputs":[{"input_id":name,"producer":"U.S. Geological Survey, The National Map Corps","host":urlsplit(next(r["url"] for r in receipts if r["capture"]==name)).hostname,"canonical_url":LANDING,
            "retrieval_url":next(r["url"] for r in receipts if r["capture"]==name),"observed_final_url":next(r["final_url"] for r in receipts if r["capture"]==name),
            "retrieved_at":next(r["fetched_at"] for r in receipts if r["capture"]==name),"sha256":raw_hashes[name],"bytes":len(bodies[name]),"raw_key":raw_keys[name],
            "role":"producer_publication","license":"Rights review pending","release_date":None,
            "release_date_unknown_reason":"Live service load dates are record timestamps; no dataset release date or complete coverage assertion is declared.","attribution":"U.S. Geological Survey / The National Map Corps; final attribution review pending."} for name in sorted(bodies)],
        "transformations":[{"operation":"native ArcGIS point JSON to typed local candidate","description":"Retains every source attribute including OBJECTID, PERMANENT_IDENTIFIER, SOURCE_FEATUREID and GLOBALID; preserves raw pages and exact native EPSG:3857 x/y. No transform, coordinate repair, ID substitution, baseline acceptance, or DataLumos equivalence claim."}],
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native source attribute values, IDs and point XY are retained in a draft candidate; source contract and published mapping remain unapproved.","objects":[{"path":f"geoparquet/{SLUG}.parquet","sha256":digest(parquet),"bytes":len(parquet)}]}}},
        "unsupported_output_formats":{"shapefile_zip":"No accepted source/published schema mapping.","geopackage":"No accepted output mapping; only the native GeoParquet candidate is staged.","pmtiles":"Reprojection and quantization are unapproved."}}
    from dagster_hifld.release_approval import validate_release_provenance
    validate_release_provenance(manifest)
    manifest_body=encode(manifest)
    _immutable(storage,f"{prefix}/metadata/source_manifest.json",manifest_body)
    proposed_contract = {"schema_version":1,"status":"proposed_unaccepted","accepted":False,
        "producer":"U.S. Geological Survey, The National Map Corps","service_layer":URL,
        "historical_equivalence":"not asserted; DataLumos / ICPSR 239161 V1 is a distinct historical snapshot",
        "feature_count_snapshot":COUNT,"field_declarations":layer["fields"],
        "wire_attribute_names":list(EXPECTED_WIRE_FIELDS),"wire_field_case_observation":quality["wire_name_observation"],
        "observed_unique_identifier_fields":["OBJECTID","PERMANENT_IDENTIFIER"],
        "geometry":{"type":"Point","crs":"EPSG:3857","coordinate_handling":"retain source x/y values without repair or reprojection"},
        "classification_snapshot":{"FTYPE":740,"FCODE":74026},
        "source_scope_caveat":"USGS definition excludes ambulance-only stations not associated with a firefighting facility; historical HIFLD coverage comparison is pending.",
        "rights":"rights and attribution review pending","baseline":"historical source files unavailable; no accepted baseline",
        "release_date":"unknown; per-record LOADDATE is not a declared producer release date"}
    snapshot = {"schema_version":1,"status":"proposed_snapshot_not_accepted_baseline","feature_count":COUNT,
        "schema_sha256":SCHEMA_SHA256,"objectids_sha256":IDS_SHA256,"full_attributes_and_geometry_sha256":ROWS_SHA256,
        "native_crs":"EPSG:3857","retrieval_receipts_sha256":digest(receipt_bytes),"raw_identity":raw_identity}
    contract_body=encode(proposed_contract); snapshot_body=encode(snapshot)
    contract_key=_immutable(storage,f"{prefix}/metadata/proposed_source_contract.json",contract_body)
    snapshot_key=_immutable(storage,f"{prefix}/metadata/proposed_source_snapshot.json",snapshot_body)
    report={**quality,"status":"draft_candidate_unapproved","dataset_slug":SLUG,"file_slug":SLUG,"version":version,
        "raw_identity":raw_identity,"candidate_key":candidate_key,"candidate_sha256":digest(parquet),"catalog_source_key":source_key,
        "source_manifest_sha256":digest(manifest_body),"proposed_contract_key":contract_key,"proposed_snapshot_key":snapshot_key,"refresh_policy":"Pinned field/count/ID/full-row snapshot; any observed drift quarantines for review; this is a proposed service snapshot, not a release or HIFLD baseline."}
    _immutable(storage,f"{prefix}/metadata/ems_extraction.json",encode(report))
    return report

"""Complete native capture for the official USACE IENC dynamic buoy service."""
from __future__ import annotations

import json
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.usace_obstruction_lines import (
    _canonical, _capture_identity,
    _load_capture, _parquet_bytes, _sha, _validate_spatial_reference,
)

DATASET_SLUG = "dynamic-buoys-usace-ienc-"
FILE_SLUG = DATASET_SLUG
BTS_ITEM_ID = "7ec34864c8804468a9cf52d437fe4b6c"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
SERVICE_DIRECTORY_URL = "https://ienccloud.us/arcgis/rest/services/IENC?f=pjson"
SERVICE_URL = "https://ienccloud.us/arcgis/rest/services/IENC/USACE_Buoys_Service/MapServer"
LAYER_ID = 1
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "US Army Corps of Engineers, Army Geospatial Center; official IENC USACE_Buoys_Service"
EXPECTED_COUNT = 11770
EXPECTED_IDS = list(range(1, EXPECTED_COUNT + 1))
EXPECTED_ID_SHA256 = "98134da814a8d97f58419dda784224200e809ad3b12aadb959ed4421ecfcf55d"
PAGE_SIZE = 1000
CAPTURE_MAX_BYTES = 192_000_000
DEFAULT_RESPONSE_MAX_BYTES = 8_000_000
LAYER_RESPONSE_MAX_BYTES = 32_000_000
EXPECTED_FIELD_DECLARATIONS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
    {"name": "Buoy_Shape", "type": "esriFieldTypeString", "alias": "Buoy_Shape", "length": 20, "domain": None},
    {"name": "Category_of_Lateral_Mark", "type": "esriFieldTypeString", "alias": "Category_of_Lateral_Mark", "length": 40, "domain": None},
    {"name": "Colour", "type": "esriFieldTypeString", "alias": "Colour", "length": 254, "domain": None},
    {"name": "Information", "type": "esriFieldTypeString", "alias": "Information", "length": 500, "domain": None},
    {"name": "Scale_Minimum", "type": "esriFieldTypeInteger", "alias": "Scale_Minimum", "domain": None},
    {"name": "Source_Date", "type": "esriFieldTypeString", "alias": "Source_Date", "length": 254, "domain": None},
    {"name": "Source_Indication", "type": "esriFieldTypeString", "alias": "Source_Indication", "length": 254, "domain": None},
]
EXPECTED_EXTENT = {"xmin": -10656258.609005641, "ymin": 3471008.9161383947,
                   "xmax": -8868886.348526133, "ymax": 5619539.6660792325}
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELD_DECLARATIONS if field["name"] != "SHAPE"]
CAPTURE_NAMES = {"bts_item", "service_directory", "service_before", "layer_before", "count_before",
                 "ids_before", "layer_after", "count_after", "ids_after", "service_after",
                 *{f"page_{index:03d}" for index in range((EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE)}}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture_roster(ids: list[int]) -> list[tuple[str, str]]:
    urls = [("bts_item", BTS_ITEM_URL), ("service_directory", SERVICE_DIRECTORY_URL),
            ("service_before", SERVICE_URL + "?f=pjson"), ("layer_before", LAYER_URL + "?f=json"),
            ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
            ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}))]
    for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        page_ids = ids[start:start + PAGE_SIZE]
        urls.append((f"page_{index:03d}", _query({"objectIds": ",".join(map(str, page_ids)),
            "outFields": "*", "returnGeometry": "true", "returnZ": "true", "returnM": "true",
            "outSR": "4326", "returnTrueCurves": "true", "orderByFields": "OBJECTID ASC", "f": "json"})))
    urls.extend([("layer_after", LAYER_URL + "?f=json"),
                 ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                 ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                 ("service_after", SERVICE_URL + "?f=pjson")])
    return urls


def _capture(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict]:
    # Bound each response and the complete multi-page capture.
    limit = LAYER_RESPONSE_MAX_BYTES if name in {"layer_before", "layer_after"} else DEFAULT_RESPONSE_MAX_BYTES
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire")
                   for path in root.glob(suffix) if path.is_file())
    if retained + 2 * limit > CAPTURE_MAX_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")
    return capture_get(client, url, root, name, max_bytes=limit)


def _validate_capture_request_identity(name: str, receipt: dict, expected_url: str) -> None:
    if (receipt.get("method") != "GET" or receipt.get("url") != expected_url
            or receipt.get("final_url") != expected_url):
        raise ValueError("retained_capture_request_identity_mismatch:" + name)


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    manifest_path = root / "capture_manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("retained_capture_manifest_symlink_rejected")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("complete") is not True or not isinstance(manifest.get("receipts"), dict):
        raise ValueError("retained_capture_manifest_invalid")
    roster = manifest.get("artifact_roster")
    if not isinstance(roster, dict) or set(manifest["receipts"]) != CAPTURE_NAMES:
        raise ValueError("retained_capture_roster_or_names_invalid")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("retained_capture_artifact_digest_mismatch")
    expected_urls = dict(_capture_roster(EXPECTED_IDS))
    captures = {}
    for name in sorted(CAPTURE_NAMES):
        body, _wire, receipt = _load_capture(root, name)
        if receipt != manifest["receipts"].get(name):
            raise ValueError("retained_capture_manifest_receipt_mismatch:" + name)
        _validate_capture_request_identity(name, receipt, expected_urls[name])
        captures[name] = (body, receipt)
    return captures


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], list[dict], dict]:
    if set(captures) != CAPTURE_NAMES:
        raise ValueError("source_capture_name_roster_drift")
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    item = docs["bts_item"]
    if (item.get("id") != BTS_ITEM_ID or item.get("owner") != "USDOT_BTS"
            or item.get("type") != "PDF" or item.get("access") != "public"):
        raise ValueError("bts_ienc_item_identity_drift")
    text = " ".join(str(item.get(key, "")) for key in ("title", "description"))
    if ("Inland Electronic Navigational Charts" not in text
            or "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes" not in text
            or "USACE_IENC_Master_Service_gdb.zip" not in text):
        raise ValueError("bts_ienc_official_distribution_link_drift")
    directory = docs["service_directory"]
    if not any(service.get("name") == "IENC/USACE_Buoys_Service"
               and service.get("type") == "MapServer" for service in directory.get("services", [])):
        raise ValueError("official_ienc_service_directory_drift")
    service = docs["service_before"]
    advertised = [(layer.get("id"), layer.get("name"), layer.get("geometryType"))
                  for layer in service.get("layers", [])]
    if (service.get("mapName") != "3UABUOYS" or "USACE" not in service.get("copyrightText", "")
            or advertised != [(LAYER_ID, "3UABUOYS", "esriGeometryPoint")]):
        raise ValueError("official_dynamic_buoy_service_drift")
    layer = docs["layer_before"]
    if layer.get("fields") != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != "3UABUOYS"
            or layer.get("geometryType") != "esriGeometryPoint" or layer.get("maxRecordCount") != 2000
            or {key: layer.get("extent", {}).get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT):
        raise ValueError("source_layer_identity_extent_or_limit_drift")
    if layer.get("hasZ") not in (None, False) or layer.get("hasM") not in (None, False):
        raise ValueError("source_layer_dimension_contract_drift")
    _validate_spatial_reference(layer.get("sourceSpatialReference"), "source_layer_native")
    if layer.get("spatialReference", {}).get("latestWkid") != 3857:
        raise ValueError("source_layer_service_crs_drift")
    extent_reference = layer.get("extent", {}).get("spatialReference")
    if not isinstance(extent_reference, dict) or extent_reference.get("latestWkid") != 3857:
        raise ValueError("source_layer_extent_crs_drift")
    if docs["service_after"] != service or docs["layer_after"] != layer:
        raise ValueError("source_service_or_layer_changed_during_capture")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    ids_before, ids_after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (count_before != EXPECTED_COUNT or count_after != count_before or not isinstance(ids_before, list)
            or any(type(oid) is not int for oid in ids_before) or ids_before != EXPECTED_IDS
            or ids_after != ids_before or len(set(ids_before)) != EXPECTED_COUNT
            or _sha(_canonical(ids_before)) != EXPECTED_ID_SHA256):
        raise ValueError("source_count_or_id_population_drift")
    features=[]; geometry_valid=geometry_empty=0
    for name in sorted(n for n in captures if n.startswith("page_")):
        page=docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_transfer_limit")
        _validate_spatial_reference(page.get("spatialReference"), "source_"+name+"_page")
        rows=page.get("features"); expected_index=int(name[-3:])
        page_ids=EXPECTED_IDS[expected_index*PAGE_SIZE:(expected_index+1)*PAGE_SIZE]
        if not isinstance(rows,list) or len(rows)!=len(page_ids):
            raise ValueError("source_page_population_incomplete:"+name)
        observed=[]
        for feature in rows:
            attrs,geometry=feature.get("attributes"),feature.get("geometry")
            if not isinstance(attrs,dict) or len(attrs)!=len(ATTRIBUTE_FIELDS) or set(attrs)!={f["name"] for f in ATTRIBUTE_FIELDS}:
                raise ValueError("source_attribute_roster_drift")
            for field in ATTRIBUTE_FIELDS:
                value=attrs[field["name"]]; kind=field["type"]
                if value is None: continue
                if kind=="esriFieldTypeOID" and (type(value) is not int or value<=0): raise ValueError("source_oid_value_type_drift")
                if kind=="esriFieldTypeInteger" and type(value) is not int: raise ValueError("source_integer_value_type_drift:"+field["name"])
                if kind=="esriFieldTypeString" and not isinstance(value,str): raise ValueError("source_string_value_type_drift:"+field["name"])
            if isinstance(geometry, dict) and ("z" in geometry or "m" in geometry):
                raise ValueError("source_point_zm_dimensions_not_supported")
            if not isinstance(geometry,dict) or set(geometry)-{"x","y","spatialReference"} or type(geometry.get("x")) not in (float,int) or type(geometry.get("y")) not in (float,int):
                raise ValueError("source_point_geometry_structure_drift")
            if not math.isfinite(geometry["x"]) or not math.isfinite(geometry["y"]): raise ValueError("source_xy_ordinate_contract_drift")
            if geometry.get("spatialReference") is not None: _validate_spatial_reference(geometry["spatialReference"],"source_"+name+"_feature")
            oid=attrs.get("OBJECTID")
            if type(oid) is not int or oid not in page_ids: raise ValueError("source_feature_id_roster_drift")
            observed.append(oid); geometry_valid += 1
            feature["attributes"]={f["name"]:attrs[f["name"]] for f in ATTRIBUTE_FIELDS}
            features.append(feature)
        if sorted(observed)!=page_ids or len(set(observed))!=len(observed): raise ValueError("source_page_id_population_drift:"+name)
    ids=[r["attributes"]["OBJECTID"] for r in features]
    if sorted(ids)!=EXPECTED_IDS or len(ids)!=EXPECTED_COUNT or len(set(ids))!=EXPECTED_COUNT: raise ValueError("source_feature_id_population_drift")
    features.sort(key=lambda r:r["attributes"]["OBJECTID"])
    coordinates=[[r["attributes"]["OBJECTID"],r["geometry"]["x"],r["geometry"]["y"]] for r in features]
    bounds=[min(p[1] for p in coordinates),min(p[2] for p in coordinates),max(p[1] for p in coordinates),max(p[2] for p in coordinates)]
    audit={"source_feature_count":len(features),"source_id_sha256":_sha(_canonical(ids)),"source_id_min":min(ids),"source_id_max":max(ids),
      "source_schema_sha256":_sha(_canonical(layer["fields"])),"source_record_sha256":_sha(_canonical([r["attributes"] for r in features])),
      "source_geometry_sha256":_sha(_canonical(coordinates)),"source_geometry_type":"esriGeometryPoint","source_wkid":4326,
      "source_vertex_count":len(features),"geometry_valid_count":geometry_valid,"geometry_empty_count":geometry_empty,"geometry_invalid_count":0,
      "geometry_bounds":bounds,"source_extent":layer["extent"],
      "source_coverage_scope":"Complete current USACE_Buoys_Service layer (3UABUOYS) for inland IENC chart coverage. Its observed extent is regional and is not nationwide buoy coverage.",
      "source_dimension_evidence":{"layer_hasZ":layer.get("hasZ"),"layer_hasM":layer.get("hasM"),"page_requests_returnZ":True,"page_requests_returnM":True,"page_requests_returnTrueCurves":True,"observed_feature_geometry_keys":sorted(set().union(*(set(r["geometry"]) for r in features)))},
      "drift_guard":"Exact official IENC folder/service/layer roster, full ArcGIS field declarations/types, native source SR and response SR, count and before/after ID roster, per-page native XY and attribute checks, and explicit returnZ/returnM/returnTrueCurves requests with observed feature dimensions."}
    return layer["fields"],features,audit


def _esri_point_wkb(geometry: dict) -> bytes:
    import struct
    return struct.pack("<BIdd",1,1,float(geometry["x"]),float(geometry["y"]))


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID":pa.int64(),"esriFieldTypeInteger":pa.int32(),"esriFieldTypeString":pa.string()}[field["type"]]


def _parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    properties=[f for f in fields if f["name"]!="SHAPE"]
    arrays=[pa.array([r["attributes"][f["name"]] for r in features],type=_arrow_type(f)) for f in properties]
    arrays.append(pa.array([_esri_point_wkb(r["geometry"]) for r in features],type=pa.binary()))
    table=pa.Table.from_arrays(arrays,names=[f["name"] for f in properties]+["geometry"])
    crs={"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"GeographicCRS","name":"WGS 84",
      "datum":{"type":"GeodeticReferenceFrame","name":"World Geodetic System 1984","ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563},"id":{"authority":"EPSG","code":6326}},
      "coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"},{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"}],"id":{"authority":"EPSG","code":6422}},"id":{"authority":"EPSG","code":4326}}
    metadata={b"geo":_canonical({"version":"1.1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","geometry_types":["Point"],"crs":crs}}})}
    import io
    sink=io.BytesIO(); pq.write_table(table.replace_schema_metadata(metadata),sink,compression="zstd",use_dictionary=False)
    return sink.getvalue()


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    return _sha(_canonical(_capture_identity(captures, root)))


def capture_usace_dynamic_buoys(storage: StagingStorageResource, *, version: str,
                                  capture_directory: str | None = None) -> dict[str, object]:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if (not isinstance(version, str) or not version or version in {".", ".."}
            or Path(version).name != version or "/" in version or "\\" in version):
        raise ValueError("unsafe_capture_version")
    staging = Path(storage.local_dir).absolute()
    if ".." in staging.parts or any(path.is_symlink() for path in (staging, *staging.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    if replay:
        root = requested
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        captures = _read_replay(root)
    else:
        if requested and any(requested.glob("*.body")):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        root = ((requested / version / uuid4().hex) if requested else
                Path("/tmp/hifld-native-reconstruction/60/captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/usace-ienc-draft"}) as client:
                ids_body, ids_receipt = _capture(client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}), root, "ids_before")
                ids = json.loads(ids_body).get("objectIds")
                if (not isinstance(ids, list) or any(type(oid) is not int for oid in ids)
                        or ids != EXPECTED_IDS or len(set(ids)) != EXPECTED_COUNT):
                    raise ValueError("source_seed_id_population_drift")
                captures["ids_before"] = (ids_body, ids_receipt)
                roster = _capture_roster(ids)
                for name, url in roster:
                    if name == "ids_before":
                        continue
                    captures[name] = _capture(client, url, root, name)
            fields, features, audit = _validate(captures)
        except Exception as exc:
            receipts = {}
            for path in sorted(root.glob("*.receipt.json")):
                try:
                    receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
                except (OSError, ValueError):
                    receipts[path.name] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({"complete": False,
                "captured_at": datetime.now(timezone.utc).isoformat(), "error_code": type(exc).__name__,
                "receipts": receipts}) + b"\n")
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
            raise
        (root / "capture_manifest.json").write_bytes(_canonical({"complete": True,
            "captured_at": datetime.now(timezone.utc).isoformat(), "source_feature_count": audit["source_feature_count"],
            "source_record_sha256": audit["source_record_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
            "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        captures = _read_replay(root)

    fields, features, audit = _validate(captures)
    identity = processing_identity(Path(__file__).parents[3])
    capture_hash = _capture_identity_hash(captures, root)
    audit.update({"source_capture_evidence_sha256": capture_hash,
        "source_capture_evidence": _capture_identity(captures, root),
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "bts_item_succession_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False, "baseline_accepted": False})
    parquet = _parquet_bytes(fields, features)
    version = "v" + _sha(_canonical({"source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    inputs = [{"input_id": name, "producer": ("USDOT BTS NTAD contextual catalog item" if name == "bts_item" else PUBLISHER),
        "host": urlsplit(receipt["url"]).hostname, "role": "contextual_catalog_reference" if name == "bts_item" else "producer_publication",
        "canonical_url": BTS_ITEM_URL if name == "bts_item" else LAYER_URL, "retrieval_url": receipt["url"],
        "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
        "release_date": None, "release_date_unknown_reason": "The BTS item describes IENC update cadence but does not identify this layer's release date.",
        "license": "Unresolved; rights review pending.", "attribution": "USCG / USACE IENC; attribution and rights review pending."}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "Dynamic Buoys (USACE IENC)",
        "description": "Complete current official USACE IENC USACE_Buoys_Service/3UABUOYS layer; faithful regional candidate, not accepted for release.",
        "publisher": PUBLISHER, "agency": "US Army Corps of Engineers", "source_url": LAYER_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"],
        "bts_item_succession_accepted": False, "inputs": inputs,
        "transformations": [{"operation": "Esri native point XY to GeoParquet WKB candidate", "parameters": {
            "source_fields": "all declared attributes retained in source order and declared types",
            "source_ids": "OBJECTID and Feature_ID retained unchanged, including nulls", "source_crs": "EPSG:4326",
            "dimensions": "all page queries request returnZ=true and returnM=true; layer declarations are absent; observed feature geometries contain XY only",
            "geometry": "native XY coordinates preserved; no reprojection, repair, or simplification",
            "raw_evidence": "complete decoded response and wire bodies retained with checksummed receipts",
            "candidate_status": "unapproved; historical/schema/ID/coverage/rights/release decisions pending"}}],
        "lineage": f"The official IENC USACE_Buoys_Service metadata describes USACE-produced locations supplied by the US Coast Guard and weekly AGC updates. The BTS IENC catalog item is retained as contextual cadence evidence and is not treated as delegation for this endpoint. This candidate contains complete service layer {LAYER_ID} 3UABUOYS only.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "All ArcGIS-declared attributes, source IDs, and native XY points are retained in a typed candidate.",
            "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No format-specific mapping has been approved.",
            "shapefile_zip": "Source field names exceed the 10-character format limit; no faithful name mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest); provenance_projection(manifest)
    audit.update({"source_fields": fields, "official_layer_name": "3UABUOYS", "official_layer_id": LAYER_ID,
                  "contextual_bts_item": BTS_ITEM_ID, "source_population_complete": True})
    outputs = {output_path: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n", "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in sorted(captures.items()):
        _body, wire, _receipt = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body; outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {"approved": False, "version": version,
        "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True, "bts_item_succession_accepted": False,
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_release_accepted": False,
        "freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    fingerprint = _sha(_canonical({"identity": identity, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_capture_sha256": capture_hash}))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    if storage.object_snapshot(state_key) is not None:
        original = json.loads(load_first_construction(storage, state_key, version, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("capture_receipts"), dict):
            raise ValueError("cached_first_candidate_receipts_missing")
        original_receipts = original["capture_receipts"]
        if set(original_receipts) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name in captures:
            body, wire, _receipt = _load_capture(root, name)
            if (storage.read_key(f"{prefix}/raw/{name}.body") != body
                    or storage.read_key(f"{prefix}/raw/{name}.wire") != wire):
                raise ValueError("cached_source_evidence_changed:" + name)
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
            identity, audit["source_record_sha256"], audit["source_schema_sha256"],
            original.get("objects", {}), original_receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": 1, "source_id_max": EXPECTED_COUNT,
            "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_capture_evidence_sha256": capture_hash, "geometry_valid_count": audit["geometry_valid_count"],
            "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_empty_count": audit["geometry_empty_count"],
            "bts_item_succession_accepted": False, "source_release_accepted": False,
            "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}
    descriptors = {}
    for name, body in sorted(outputs.items()):
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        if snapshot is None or snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"]:
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = descriptor
    seal_ready_candidate(storage, state_key, version, fingerprint, {"processing_identity": identity,
        "source_record_sha256": audit["source_record_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": receipts, "objects": descriptors})
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
    )
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": EXPECTED_COUNT, "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_id_min": 1, "source_id_max": EXPECTED_COUNT,
        "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
        "source_capture_evidence_sha256": capture_hash, "geometry_valid_count": audit["geometry_valid_count"],
        "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_empty_count": audit["geometry_empty_count"],
        "bts_item_succession_accepted": False, "source_release_accepted": False,
        "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}

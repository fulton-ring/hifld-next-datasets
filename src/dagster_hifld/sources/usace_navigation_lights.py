"""Source-specific LIGHTS_POINT contract and native point encoder for USACE IENC."""
from __future__ import annotations

import json
import hashlib
import shutil
import struct
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from uuid import uuid4
from typing import Mapping

import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import httpx

from dagster_hifld.sources.usace_administrative_areas import (
    _arrow_type, _canonical, _fetch_retry, _load_replay, _sha, _write_outputs,
)
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction

DATASET_SLUG = "navigation-lights-usace-ienc"
FILE_SLUG = DATASET_SLUG
LAYER_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/LIGHTS_POINT/MapServer/16"
PUBLISHER = "US Army Corps of Engineers, Army Geospatial Center; official IENC Feature Classes service"
EXPECTED_COUNT = 8892
PAGE_SIZE = 250
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID"), ("SHAPE", "esriFieldTypeGeometry"),
    ("Category_of_Light", "esriFieldTypeString"), ("Color", "esriFieldTypeString"),
    ("Light_Characteristics", "esriFieldTypeString"), ("Signal_Group", "esriFieldTypeString"),
    ("Signal_Period", "esriFieldTypeString"), ("Multiplicity_of_Lights", "esriFieldTypeString"),
    ("Condition", "esriFieldTypeString"), ("Orientation", "esriFieldTypeString"),
    ("Object_Name", "esriFieldTypeString"), ("Information", "esriFieldTypeString"),
    ("Status", "esriFieldTypeString"), ("Scale_Minimum", "esriFieldTypeInteger"),
    ("Source_Date", "esriFieldTypeString"), ("Source_Indication", "esriFieldTypeString"),
    ("Source_Dataset", "esriFieldTypeString"), ("Height", "esriFieldTypeString"),
    ("Value_of_Nominal_Range", "esriFieldTypeString"), ("Feature_ID", "esriFieldTypeString"),
    ("Exhibition_Condition_of_Light", "esriFieldTypeString"),
]


def validate_light_snapshot(captures: Mapping[str, tuple[bytes, dict]]) -> tuple[list[dict], list[dict], dict]:
    """Validate exact layer, schema, count, IDs and all page geometries."""
    docs = {name: json.loads(body) for name, (body, _) in captures.items()}
    expected = {"layer_before", "count_before", "ids_seed", "ids_before", "layer_after", "count_after", "ids_after"}
    expected |= {f"page_{i:03d}" for i in range((EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE)}
    if set(docs) != expected:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    layer = docs["layer_before"]
    fields = layer.get("fields")
    declared = [(f.get("name"), f.get("type")) for f in fields] if isinstance(fields, list) else None
    if (layer.get("id") != 16 or layer.get("name") != "LIGHTS_POINT"
            or layer.get("geometryType") != "esriGeometryPoint" or declared != EXPECTED_FIELDS
            or layer.get("spatialReference", {}).get("latestWkid") != 4326
            or layer.get("maxRecordCount") != 2000):
        raise ValueError("source_layer_schema_or_spatial_contract_drift")
    if docs["layer_after"] != layer:
        raise ValueError("source_layer_metadata_changed_during_capture")
    if any(docs[name].get("count") != EXPECTED_COUNT for name in ("count_before", "count_after")):
        raise ValueError("source_count_drift")
    ids = docs["ids_before"].get("objectIds")
    if (not isinstance(ids, list) or len(ids) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT
            or ids != docs["ids_after"].get("objectIds") or ids != docs["ids_seed"].get("objectIds")):
        raise ValueError("source_id_population_changed_or_invalid")
    ids = sorted(ids)
    features = []
    for i in range((EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE):
        page = docs[f"page_{i:03d}"]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncated")
        if page.get("spatialReference", {}).get("latestWkid") != 4326:
            raise ValueError("source_page_spatial_reference_drift")
        rows = page.get("features")
        if not isinstance(rows, list):
            raise ValueError("source_page_features_missing")
        features.extend(rows)
    attr_names = [name for name, _kind in EXPECTED_FIELDS if name != "SHAPE"]
    got_ids = []
    geom_audit = []
    for feature in features:
        attrs, geom = feature.get("attributes"), feature.get("geometry")
        if (not isinstance(attrs, dict) or list(attrs) != attr_names
                or not isinstance(geom, dict) or set(geom) - {"x", "y", "z", "m", "spatialReference"}
                or not all(isinstance(geom.get(k), (int, float)) for k in ("x", "y"))):
            raise ValueError("source_record_schema_or_geometry_drift")
        got_ids.append(attrs.get("OBJECTID"))
        geom_audit.append([attrs["OBJECTID"], geom])
    if len(features) != EXPECTED_COUNT or sorted(got_ids) != ids or len(set(got_ids)) != EXPECTED_COUNT:
        raise ValueError("source_feature_population_incomplete")
    features.sort(key=lambda f: f["attributes"]["OBJECTID"])
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
        "source_id_min": min(ids), "source_id_max": max(ids),
        "source_record_sha256": _sha(_canonical([f["attributes"] for f in features])),
        "source_geometry_sha256": _sha(_canonical(sorted(geom_audit))),
        "source_schema_sha256": _sha(_canonical(fields)), "source_wkid": 4326,
        "source_geometry_type": "esriGeometryPoint", "null_geometry_count": 0,
        "source_coverage_scope": "Complete official USACE IENC LIGHTS_POINT feature class; coverage limited to the IENC charting program.",
        "drift_guard": "Exact layer identity, field names/types, CRS, count, all-row ID roster and before/after metadata are checked.",
    }
    return fields, features, audit


def lights_geoparquet(fields: list[dict], features: list[dict]) -> bytes:
    """Encode source attributes with declared types and point ordinates unchanged."""
    attr_fields = [f for f in fields if f["name"] != "SHAPE"]
    arrays = [pa.array([r["attributes"][f["name"]] for r in features], type=_arrow_type(f)) for f in attr_fields]
    geometry = []
    dimensions = set()
    for row in features:
        point = row["geometry"]
        if set(point) - {"x", "y", "z", "m", "spatialReference"} or not {"x", "y"} <= set(point):
            raise ValueError("source_point_ordinate_roster_invalid")
        has_z, has_m = "z" in point, "m" in point
        dimensions.add((has_z, has_m))
        if len(dimensions) > 1:
            raise ValueError("source_point_ordinate_roster_drift")
        ordinates = [point["x"], point["y"]]
        if has_z: ordinates.append(point["z"])
        if has_m: ordinates.append(point["m"])
        if any(not isinstance(value, (int, float)) for value in ordinates):
            raise ValueError("source_point_ordinate_invalid")
        type_code = 1 + (1000 if has_z and not has_m else 2000 if has_m and not has_z else 3000 if has_z and has_m else 0)
        geometry.append(struct.pack("<BI" + "d" * len(ordinates), 1, type_code, *ordinates))
    arrays.append(pa.array(geometry, type=pa.binary()))
    schema = pa.schema([pa.field(f["name"], _arrow_type(f), nullable=f.get("nullable", True)) for f in attr_fields]
                       + [pa.field("geometry", pa.binary(), nullable=False)])
    metadata = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{
        "encoding":"WKB", "geometry_types":["Point" + (" Z" if dimensions and next(iter(dimensions))[0] else "") + (" M" if dimensions and next(iter(dimensions))[1] else "")], "crs":pyproj.CRS.from_epsg(4326).to_json_dict(), "edges":"planar"}}}
    table = pa.Table.from_arrays(arrays, schema=schema.with_metadata({b"geo":_canonical(metadata)}))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    return sink.getvalue().to_pybytes()


def _query(params: dict) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _urls(ids: list[int]) -> list[tuple[str, str]]:
    urls = [("layer_before", LAYER_URL + "?f=pjson"),
            ("count_before", _query({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
            ("ids_before", _query({"where":"1=1", "returnIdsOnly":"true", "f":"json"}))]
    for i in range(0, len(ids), PAGE_SIZE):
        urls.append((f"page_{i // PAGE_SIZE:03d}", _query({
            "objectIds": ",".join(map(str, ids[i:i + PAGE_SIZE])), "outFields":"*",
            "returnGeometry":"true", "returnZ":"true", "returnM":"true", "outSR":"4326",
            "orderByFields":"OBJECTID ASC", "f":"json"})))
    urls.extend([("layer_after", LAYER_URL + "?f=pjson"),
                 ("count_after", _query({"where":"1=1", "returnCountOnly":"true", "f":"json"})),
                 ("ids_after", _query({"where":"1=1", "returnIdsOnly":"true", "f":"json"}))])
    return urls


def _record_incomplete(root: Path, storage: StagingStorageResource, version: str,
                       captures: dict, error: Exception) -> None:
    """Seal failed acquisition evidence and quarantine successfully captured responses."""
    receipts = {}
    for path in sorted(root.glob("*.receipt.json")):
        name = path.name.removesuffix(".receipt.json")
        try:
            receipts[name] = json.loads(path.read_bytes())
        except (OSError, ValueError):
            receipts[name] = {"receipt_readback_failed":True}
    manifest = {"complete":False,"failed_at":datetime.now(timezone.utc).isoformat(),
                "error_code":type(error).__name__,"receipts":receipts,
                "artifact_roster":capture_roster_digests(root,exclude={"capture_manifest.json"})}
    (root/"capture_manifest.json").write_bytes(_canonical(manifest)+b"\n")
    if captures:
        try:
            quarantine_incomplete_capture(storage,root,DATASET_SLUG,version,captures,type(error).__name__)
        except Exception:
            # The immutable evidence directory and failed manifest remain available
            # for recovery even if staging quarantine itself cannot be written.
            pass


def capture_usace_navigation_lights(storage: StagingStorageResource, *, version: str,
                                    capture_directory: str | None = None) -> dict:
    """Capture/replay one complete source population and stage an unapproved candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(x.is_symlink() for x in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    root = Path(capture_directory).absolute() if capture_directory else Path("/tmp/hifld-native-reconstruction/180/captures") / (version + "-" + uuid4().hex)
    if ".." in root.parts or any(x.is_symlink() for x in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    replay = (root / "capture_manifest.json").is_file()
    root.mkdir(parents=True, exist_ok=True)
    captures = {}
    if replay:
        manifest = json.loads((root / "capture_manifest.json").read_bytes())
        roster = manifest.get("artifact_roster")
        receipts = manifest.get("receipts")
        if manifest.get("complete") is not True or not isinstance(roster, dict) or not isinstance(receipts, dict):
            raise ValueError("retained_capture_manifest_invalid")
        validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
        if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
            raise ValueError("retained_capture_roster_digest_mismatch")
        seed, _ = _load_replay(root, "ids_seed")
        ids = json.loads(seed).get("objectIds")
        if not isinstance(ids, list): raise ValueError("retained_id_seed_invalid")
        names = ["ids_seed", "layer_before", "count_before", "ids_before",
                 *(f"page_{i:03d}" for i in range((len(ids)+PAGE_SIZE-1)//PAGE_SIZE)),
                 "layer_after", "count_after", "ids_after"]
        if set(receipts) != set(names): raise ValueError("retained_capture_roster_changed")
        captures = {name: _load_replay(root, name) for name in names}
        if any(receipt != receipts[name] for name, (_body, receipt) in captures.items()):
            raise ValueError("retained_capture_receipt_changed")
    else:
        if any(root.glob("*.body")) or any(root.glob("*.wire")) or any(root.glob("*.receipt.json")):
            raise ValueError("explicit_capture_directory_contains_unsealed_attempt")
        budget = [0]
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(60, connect=15),
                          headers={"User-Agent":"hifld-next-datasets/usace-ienc-draft"}) as client:
            try:
                seed, receipt = _fetch_retry(client, _query({"where":"1=1", "returnIdsOnly":"true", "f":"json"}), root, "ids_seed", budget)
                ids = json.loads(seed).get("objectIds")
                if not isinstance(ids, list) or len(ids) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT:
                    raise ValueError("source_seed_id_population_drift")
                captures["ids_seed"] = (seed, receipt)
                for name, url in _urls(ids):
                    captures[name] = _fetch_retry(client, url, root, name, budget)
            except Exception as exc:
                _record_incomplete(root,captures,version,exc)
                raise
    try:
        fields, features, audit = validate_light_snapshot(captures)
    except Exception as exc:
        if not replay:
            _record_incomplete(root,captures,version,exc)
        raise
    parquet = lights_geoparquet(fields, features)
    source_hash = _sha(_canonical({k: _sha(v[0]) for k, v in sorted(captures.items())}))
    identity = processing_identity(Path(__file__).parents[3])
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"], audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    audit.update({"source_capture_sha256":source_hash, "source_release_accepted":False,
                  "source_freshness_accepted":False, "schema_contract_accepted":False,
                  "id_contract_accepted":False, "coverage_accepted":False,
                  "rights_accepted":False, "mapping_approved":False, "release_accepted":False})
    manifest = {"schema_version":1, "approved":False, "title":"Navigation Lights (USACE IENC)",
        "description":"Complete native USACE IENC LIGHTS_POINT layer candidate. Source release date, baseline compatibility, downstream ID/schema contracts, broader coverage and rights require independent acceptance.",
        "publisher":PUBLISHER, "agency":"US Army Corps of Engineers", "source_url":LAYER_URL,
        "license":None, "source_modified":None, "date_issued":None, "date_modified":None,
        "processor":"HIFLD Next", "code_commit":identity["code_commit"],
        "inputs":[{"input_id":n,"producer":PUBLISHER,"host":"ienccloud.us","role":"producer_publication","canonical_url":LAYER_URL,"retrieval_url":r["url"],"observed_final_url":r["final_url"],"retrieved_at":r["observed_at"],"http_status":r["status"],"bytes":r["bytes"],"sha256":r["sha256"],"license":"Unresolved; rights review pending.","attribution":"USACE IENC; rights review pending.","release_date":None,"release_date_unknown_reason":"Official layer metadata does not establish the source release date."} for n,(_b,r) in sorted(captures.items())],
        "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{"attributes":"all declared attributes and physical types retained","ids":"OBJECTID and Feature_ID unchanged","geometry":"Point ordinates retained in source EPSG:4326; no repair or reprojection","candidate_status":"unapproved"}}],
        "lineage":"BTS-linked official USACE IENC Feature Classes MapServer; this layer is limited to IENC chart coverage.",
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Source attributes and Point ordinates retained as an unapproved native candidate.","objects":[{"path":"geoparquet/navigation-lights-usace-ienc.parquet","bytes":len(parquet),"sha256":_sha(parquet)}]}}},
        "unsupported_output_formats":{"geopackage":"Independent source, compatibility and rights review pending.","pmtiles":"No display transformation approved."}}
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    ready = {"approved":False,"version":version,"feature_count":len(features),"source_population_complete":True,
             "source_record_sha256":audit["source_record_sha256"],"source_id_sha256":audit["source_id_sha256"],
             "source_schema_sha256":audit["source_schema_sha256"],"source_release_accepted":False,
             "freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,
             "coverage_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs = {"geoparquet/navigation-lights-usace-ienc.parquet":parquet,
               "metadata/source_manifest.json":_canonical(manifest)+b"\n",
               "metadata/source_audit.json":_canonical(audit)+b"\n",
               "metadata/candidate_ready.json":_canonical(ready)+b"\n",
               "metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (root/f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt)+b"\n"
    descriptors = {name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}
    fingerprint = _sha(_canonical(identity))
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    receipts = {name:receipt for name,(_body,receipt) in captures.items()}
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage,state_key,version,fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload,dict) or not isinstance(payload.get("objects"),dict) or not isinstance(payload.get("capture_receipts"),dict):
            raise ValueError("cached_candidate_original_receipts_missing")
        original_receipts = payload["capture_receipts"]
        if set(original_receipts) != set(captures):
            raise ValueError("cached_candidate_capture_roster_changed")
        for name,(fresh_body,_receipt) in captures.items():
            if storage.read_key(f"{prefix}/raw/{name}.body") != fresh_body or storage.read_key(f"{prefix}/raw/{name}.wire") != (root/f"{name}.wire").read_bytes():
                raise ValueError("cached_source_bytes_changed:"+name)
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,
            identity,audit["source_record_sha256"],audit["source_schema_sha256"],payload["objects"],original_receipts)
        validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":len(features),
                "source_population_complete":True,"source_id_sha256":audit["source_id_sha256"],
                "source_id_min":audit["source_id_min"],"source_id_max":audit["source_id_max"],
                "source_schema_sha256":audit["source_schema_sha256"],"source_coverage_scope":audit["source_coverage_scope"],
                "drift_guard":audit["drift_guard"],"candidate_ready":prefix+"/metadata/candidate_ready.json",
                "capture_directory":str(root),"reused":True,"source_release_accepted":False,"source_freshness_accepted":False,
                "schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,
                "rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    _write_outputs(storage, prefix, outputs)
    seal_ready_candidate(storage,state_key,version,fingerprint,{"processing_identity":identity,
        "source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],
        "capture_receipts":receipts,"objects":descriptors})
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    (root/"capture_manifest.json").write_bytes(_canonical({"complete":True,"receipts":{n:r for n,(_b,r) in captures.items()},"artifact_roster":capture_roster_digests(root,exclude={"capture_manifest.json"})})+b"\n")
    return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":len(features),
            "source_population_complete":True,"source_id_sha256":audit["source_id_sha256"],
            "source_id_min":audit["source_id_min"],"source_id_max":audit["source_id_max"],
            "source_schema_sha256":audit["source_schema_sha256"],"source_coverage_scope":audit["source_coverage_scope"],
            "drift_guard":audit["drift_guard"],"candidate_ready":prefix+"/metadata/candidate_ready.json",
            "capture_directory":str(root),"source_release_accepted":False,"source_freshness_accepted":False,
            "schema_contract_accepted":False,"id_contract_accepted":False,"coverage_accepted":False,
            "rights_accepted":False,"mapping_approved":False,"release_accepted":False}

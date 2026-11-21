"""Bounded, source-faithful capture for the official USACE IENC terminal areas."""
from __future__ import annotations

import io
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
from pyproj import CRS
from shapely import from_wkb

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.esri_geometry import esri_polygon_wkb
from dagster_hifld.sources.capture_common import (
    candidate_prefix, processing_identity, quarantine_incomplete_capture,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
    validate_capture_request_identity,
)
from dagster_hifld.sources.arcgis_layer import (
    capture_witness,
    read_ienc_layer_replay,
    ienc_layer_capture_roster,
    _canonical, _capture, _capture_identity, _load_capture, _sha,
    _validate_spatial_reference,
)

DATASET_SLUG = "terminal-areas-usace-ienc"
FILE_SLUG = DATASET_SLUG
# BTS's earlier IENC MapServer item is no longer anonymously readable. Its
# public replacement is a PDF catalog record that still links the producer's
# Feature Classes service. Treat that catalog succession as unaccepted.
BTS_ITEM_ID = "7ec34864c8804468a9cf52d437fe4b6c"
PRIOR_BTS_ITEM_ID = "3607d485a9854d6fa458bf507058ceae"
BTS_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{BTS_ITEM_ID}?f=json"
SERVICE_DIRECTORY_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes?f=pjson"
SERVICE_URL = "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/TERMINAL_AREA/MapServer"
LAYER_ID = 80
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PUBLISHER = "US Army Corps of Engineers, Army Geospatial Center; official IENC Feature Classes service"
PAGE_SIZE = 250
MAX_CAPTURE_BYTES = 192_000_000
EXPECTED_FIELD_DECLARATIONS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
    {"name": "Category_of_Harbour_Facility", "type": "esriFieldTypeString", "alias": "Category_of_Harbour_Facility", "length": 254, "domain": None},
    {"name": "Transshipping_Goods", "type": "esriFieldTypeString", "alias": "Transshipping_Goods", "length": 254, "domain": None},
    {"name": "Object_Name", "type": "esriFieldTypeString", "alias": "Object_Name", "length": 254, "domain": None},
    {"name": "Information", "type": "esriFieldTypeString", "alias": "Information", "length": 254, "domain": None},
    {"name": "Scale_Minimum", "type": "esriFieldTypeInteger", "alias": "Scale_Minimum", "domain": None},
    {"name": "Source_Date", "type": "esriFieldTypeString", "alias": "Source_Date", "length": 254, "domain": None},
    {"name": "Source_Indication", "type": "esriFieldTypeString", "alias": "Source_Indication", "length": 254, "domain": None},
    {"name": "Source_Dataset", "type": "esriFieldTypeString", "alias": "Source_Dataset", "length": 254, "domain": None},
    {"name": "Feature_ID", "type": "esriFieldTypeString", "alias": "Feature_ID", "length": 254, "domain": None},
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELD_DECLARATIONS if field["name"] != "SHAPE"]
EXPECTED_EXTENT = {"xmin": -94.38060919999998, "ymin": 33.97956320000003,
                   "xmax": -91.36408549999999, "ymax": 35.46381110000004}
_BASE_CAPTURE_NAMES = {"bts_item", "service_directory", "service_before", "layer_before", "count_before", "ids_before", "layer_after", "count_after", "ids_after", "service_after"}


def _capture_names(ids: list[int]) -> set[str]:
    return set(_BASE_CAPTURE_NAMES) | {
        f"page_{index:03d}" for index in range((len(ids) + PAGE_SIZE - 1) // PAGE_SIZE)}



def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture_roster(ids: list[int]) -> list[tuple[str, str]]:
    return ienc_layer_capture_roster(
        ids,
        bts_item_url=BTS_ITEM_URL,
        service_directory_url=SERVICE_DIRECTORY_URL,
        service_url=SERVICE_URL,
        layer_url=LAYER_URL,
        page_size=PAGE_SIZE,
    )


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict]]:
    return read_ienc_layer_replay(
        root, capture_roster=_capture_roster,
        load_capture=_load_capture,
        validate_request_identity=validate_capture_request_identity)

def _esri_polygon_wkb(geometry: dict) -> bytes:
    """Encode native ArcGIS polygon rings through the shared GDAL converter."""
    return esri_polygon_wkb(geometry, allow_zm=False)


def _validate(captures: dict[str, tuple[bytes, dict]]) -> tuple[list[dict], list[dict], dict]:
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    ids_before = capture_witness(captures)
    if set(captures) != _capture_names(ids_before):
        raise ValueError("source_capture_name_roster_drift")
    item = docs["bts_item"]
    if (item.get("id") != BTS_ITEM_ID or item.get("owner") != "USDOT_BTS"
            or item.get("type") != "PDF"):
        raise ValueError("bts_ienc_item_identity_drift")
    text = " ".join(str(item.get(key, "")) for key in ("title", "description"))
    if ("Inland Electronic Navigational Charts" not in text
            or "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes" not in text
            or "USACE_IENC_Master_Service_gdb.zip" not in text):
        raise ValueError("bts_ienc_official_distribution_link_drift")
    directory = docs["service_directory"]
    if not any(service.get("name") == "IENC_Feature_Classes/TERMINAL_AREA"
               and service.get("type") == "MapServer" for service in directory.get("services", [])):
        raise ValueError("official_ienc_service_directory_drift")
    service = docs["service_before"]
    advertised = [(layer.get("id"), layer.get("name"), layer.get("geometryType"))
                  for layer in service.get("layers", [])]
    if (service.get("mapName") != "TERMINAL_AREA" or service.get("copyrightText") != "USACE IENC"
            or advertised != [(LAYER_ID, "TERMINAL_AREA", "esriGeometryPolygon")]):
        raise ValueError("official_terminal_area_service_drift")
    layer = docs["layer_before"]
    if layer.get("fields") != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != "TERMINAL_AREA"
            or layer.get("geometryType") != "esriGeometryPolygon" or layer.get("maxRecordCount") != 2000
            or {key: layer.get("extent", {}).get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT):
        raise ValueError("source_layer_identity_extent_or_limit_drift")
    if layer.get("hasZ") or layer.get("hasM"):
        raise ValueError("source_layer_dimensional_drift")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent_reference = layer.get("extent", {}).get("spatialReference")
    if extent_reference is not None:
        _validate_spatial_reference(extent_reference, "source_layer_extent")
    if docs["service_after"] != service or docs["layer_after"] != layer:
        raise ValueError("source_service_or_layer_changed_during_capture")
    features = []
    vertices = 0
    geometry_valid = geometry_empty = 0
    for name in sorted(n for n in captures if n.startswith("page_")):
        page = docs[name]
        if "error" in page or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_transfer_limit")
        if page.get("hasZ") or page.get("hasM"):
            raise ValueError("source_page_dimensional_drift")
        _validate_spatial_reference(page.get("spatialReference"), "source_" + name + "_page")
        rows = page.get("features")
        expected_index = int(name[-3:])
        expected_ids = ids_before[expected_index * PAGE_SIZE:(expected_index + 1) * PAGE_SIZE]
        if not isinstance(rows, list) or len(rows) != len(expected_ids):
            raise ValueError("source_page_population_incomplete:" + name)
        page_ids = []
        for feature in rows:
            attrs, geometry = feature.get("attributes"), feature.get("geometry")
            if (not isinstance(attrs, dict) or len(attrs) != len(ATTRIBUTE_FIELDS)
                    or set(attrs) != {field["name"] for field in ATTRIBUTE_FIELDS}):
                raise ValueError("source_attribute_roster_or_order_drift")
            for field in ATTRIBUTE_FIELDS:
                value = attrs[field["name"]]
                if value is None:
                    continue
                kind = field["type"]
                if kind == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                    raise ValueError("source_oid_value_type_drift")
                if kind == "esriFieldTypeInteger" and type(value) is not int:
                    raise ValueError("source_integer_value_type_drift:" + field["name"])
                if kind == "esriFieldTypeString" and not isinstance(value, str):
                    raise ValueError("source_string_value_type_drift:" + field["name"])
            if not isinstance(geometry, dict) or set(geometry) - {"rings", "hasZ", "hasM", "spatialReference"}:
                raise ValueError("source_polygon_geometry_structure_drift")
            if "rings" not in geometry:
                raise ValueError("source_polygon_rings_missing")
            if geometry.get("hasZ") or geometry.get("hasM"):
                raise ValueError("source_geometry_dimensional_drift")
            if geometry.get("spatialReference") is not None:
                _validate_spatial_reference(geometry.get("spatialReference"),
                                            "source_" + name + "_feature")
            rings = geometry.get("rings")
            if not isinstance(rings, list) or not rings:
                raise ValueError("source_polygon_rings_missing")
            for ring in rings:
                if not isinstance(ring, list) or len(ring) < 4:
                    raise ValueError("source_polygon_ring_invalid")
                if ring[0] != ring[-1]:
                    raise ValueError("source_polygon_ring_not_closed")
                for point in ring:
                    if (not isinstance(point, list) or len(point) != 2
                            or any(type(coord) not in (int, float) or not math.isfinite(coord) for coord in point)):
                        raise ValueError("source_xy_ordinate_contract_drift")
                    if not (EXPECTED_EXTENT["xmin"] <= point[0] <= EXPECTED_EXTENT["xmax"]
                            and EXPECTED_EXTENT["ymin"] <= point[1] <= EXPECTED_EXTENT["ymax"]):
                        raise ValueError("source_geometry_outside_declared_coverage")
                    vertices += 1
            oid = attrs.get("OBJECTID")
            if type(oid) is not int or oid not in expected_ids:
                raise ValueError("source_feature_id_roster_drift")
            page_ids.append(oid)
            geom = from_wkb(_esri_polygon_wkb(geometry))
            geometry_empty += int(geom.is_empty)
            geometry_valid += int(geom.is_valid and not geom.is_empty)
            feature["attributes"] = {field["name"]: attrs[field["name"]] for field in ATTRIBUTE_FIELDS}
            features.append(feature)
        if sorted(page_ids) != expected_ids or len(set(page_ids)) != len(page_ids):
            raise ValueError("source_page_id_population_drift:" + name)
    ids = [feature["attributes"]["OBJECTID"] for feature in features]
    if sorted(ids) != ids_before or len(features) != len(ids_before) or len(set(ids)) != len(ids_before):
        raise ValueError("source_feature_id_population_drift")
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    bounds = [from_wkb(_esri_polygon_wkb(row["geometry"])).bounds for row in features]
    feature_ids = [row["attributes"]["Feature_ID"] for row in features]
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
        "source_id_min": min(ids), "source_id_max": max(ids),
        "source_schema_sha256": _sha(_canonical(layer["fields"])),
        "source_record_sha256": _sha(_canonical([row["attributes"] for row in features])),
        "source_geometry_sha256": _sha(_canonical([[row["attributes"]["OBJECTID"], row["geometry"]] for row in features])),
        "source_geometry_type": "esriGeometryPolygon", "source_wkid": 4326,
        "source_vertex_count": vertices, "geometry_valid_count": geometry_valid,
        "geometry_empty_count": geometry_empty,
        "geometry_invalid_count": len(features) - geometry_valid - geometry_empty,
        "geometry_bounds": [min(b[0] for b in bounds), min(b[1] for b in bounds),
                            max(b[2] for b in bounds), max(b[3] for b in bounds)],
        "source_extent": layer["extent"],
        "feature_id_distinct_count": len(set(feature_ids)),
        "feature_id_duplicate_count": len(feature_ids) - len(set(feature_ids)),
        "source_coverage_scope": "Complete observed TERMINAL_AREA layer from the directly queried USACE IENC "
            "Feature Classes service; the service describes the features as areas containing terminal and/or "
            "transshipment facilities, and the inland chart extent is regional, not a nationwide terminal inventory.",
        "bts_catalog_item_id": BTS_ITEM_ID,
        "prior_bts_catalog_item_id": PRIOR_BTS_ITEM_ID,
        "bts_catalog_item_successor_review_pending": True,
        "drift_guard": "Current BTS PDF catalog link, exact service directory/service/layer roster, full field "
            "declarations/types, explicit EPSG:4326 layer and response-page SRs, explicit geometry SRs, XY-only "
            "layer/page/geometry dimensional flags, count and before/after ID roster, per-page IDs/attributes/"
            "transfer limits, ring closure/winding/hole ownership and declared-extent containment are checked; "
            "duplicate Feature_ID values are preserved.",
    }
    return layer["fields"], features, audit


def _arrow_type(field: dict) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeString": pa.string()}[field["type"]]


def _parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    properties = [field for field in fields if field["name"] != "SHAPE"]
    arrays = [pa.array([row["attributes"][field["name"]] for row in features], type=_arrow_type(field))
              for field in properties]
    arrays.append(pa.array([_esri_polygon_wkb(row["geometry"]) for row in features], type=pa.binary()))
    table = pa.Table.from_arrays(arrays, names=[field["name"] for field in properties] + ["geometry"])
    crs = CRS.from_epsg(4326).to_json_dict()
    metadata = {b"geo": _canonical({"version": "1.1.0", "primary_column": "geometry",
                                    "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["MultiPolygon", "Polygon"], "crs": crs}}})}
    sink = io.BytesIO()
    pq.write_table(table.replace_schema_metadata(metadata), sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def _capture_identity_hash(captures: dict[str, tuple[bytes, dict]], root: Path) -> str:
    return _sha(_canonical(_capture_identity(captures, root)))


def capture_terminal_areas_usace_ienc(storage: StagingStorageResource, *, version: str,
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
                default_capture_dir("hifld-native-reconstruction", "324", "captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=12),
                              headers={"User-Agent": "hifld-next-datasets/usace-ienc-draft"}) as client:
                ids_body, ids_receipt = _capture(client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}), root, "ids_before")
                ids = json.loads(ids_body).get("objectIds")
                if (not isinstance(ids, list) or not ids
                        or any(type(oid) is not int or oid <= 0 for oid in ids)
                        or len(set(ids)) != len(ids)):
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
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False, "baseline_accepted": False})
    parquet = _parquet_bytes(fields, features)
    version = "v" + _sha(_canonical({"source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    inputs = [{"input_id": name, "producer": ("USDOT BTS IENC NTAD item" if name == "bts_item" else PUBLISHER),
        "host": urlsplit(receipt["url"]).hostname, "role": "official_distributor" if name == "bts_item" else "producer_publication",
        "canonical_url": BTS_ITEM_URL if name == "bts_item" else LAYER_URL, "retrieval_url": receipt["url"],
        "observed_final_url": receipt["final_url"], "retrieved_at": receipt["observed_at"],
        "http_status": receipt["status"], "bytes": receipt["bytes"], "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
        "release_date": None, "release_date_unknown_reason": "The BTS catalog item describes IENC update cadence but does not identify this layer's release date; the item succession does not establish the captured layer's edition date.",
        "license": "Unresolved; rights review pending.", "attribution": "USACE IENC / BTS; attribution and rights review pending.",
        **({"distributor": "USACE IENC Feature Classes service", "delegation_evidence": {
            "bts_item_url": BTS_ITEM_URL, "bts_item_id": BTS_ITEM_ID, "bts_item_owner": "USDOT_BTS",
            "official_service_directory": SERVICE_DIRECTORY_URL,
            "linked_feature_classes_service": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes",
            "prior_bts_item_id": PRIOR_BTS_ITEM_ID,
            "succession_status": "unaccepted; the current public PDF catalog item links the producer service, while a prior draft cited a different BTS item; continuity of the catalog record and layer edition has not been approved",
            "basis": "The captured BTS PDF record identifies the IENC program, describes USACE/Army Geospatial Center updates, and explicitly links the USACE IENC Feature Classes service and master file geodatabase. The queried layer's own service metadata identifies copyright USACE IENC; the direct service response, not the PDF item, supplies the captured geometry and attributes."}}
           if name == "bts_item" else {})}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "Terminal Areas (USACE IENC)",
        "description": "Complete observed USACE IENC TERMINAL_AREA layer; catalog-item succession, layer edition, historical continuity, and release remain unaccepted.",
        "publisher": PUBLISHER, "agency": "US Army Corps of Engineers", "source_url": LAYER_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Esri XY Polygon to GeoParquet WKB candidate", "parameters": {
            "source_fields": "all declared attributes retained in source order and declared types",
            "source_ids": "OBJECTID and Feature_ID retained unchanged, including any duplicates",
            "source_crs": "EPSG:4326",
            "geometry": "2D ring order, winding and hole ownership preserved; no reprojection, repair, or simplification",
            "raw_evidence": "complete decoded response and wire bodies retained with checksummed receipts",
            "candidate_status": "unapproved; historical/schema/ID/coverage/rights/release decisions pending"}}],
        "lineage": f"Current public BTS IENC PDF item {BTS_ITEM_ID} links the USACE IENC Feature Classes service and master geodatabase. A prior draft cited BTS map-service item {PRIOR_BTS_ITEM_ID}; the relationship between the records and continuity of the layer edition remain unapproved. This candidate captures the complete directly queried service layer {LAYER_ID} TERMINAL_AREA only; the service describes the features as areas containing terminal and/or transshipment facilities. The historical HIFLD inventory row and its DataLumos archive (project 240682) are third-party rescue context and not pipeline inputs, so archive crosswalk identity and historical continuity are unverified. It makes no claim of nationwide terminal coverage.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Native attributes, IDs and 2D rings are retained in a typed candidate.",
            "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No format-specific mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest); provenance_projection(manifest)
    audit.update({"source_fields": fields, "official_layer_name": "TERMINAL_AREA", "official_layer_id": LAYER_ID,
                  "official_source_item": "USDOT_BTS " + BTS_ITEM_ID,
                  "prior_bts_catalog_item_id": PRIOR_BTS_ITEM_ID,
                  "bts_catalog_item_successor_review_pending": True,
                  "source_population_complete": True})
    outputs = {output_path: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n", "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in sorted(captures.items()):
        _body, wire, _receipt = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body; outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {"approved": False, "version": version,
        "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True, "source_record_sha256": audit["source_record_sha256"],
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
            "feature_count": len(features), "source_reported_count": len(features), "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": audit["source_id_min"], "source_id_max": audit["source_id_max"],
            "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_capture_evidence_sha256": capture_hash, "geometry_valid_count": audit["geometry_valid_count"],
            "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_empty_count": audit["geometry_empty_count"],
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
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
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": len(features), "source_reported_count": len(features), "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_id_min": audit["source_id_min"], "source_id_max": audit["source_id_max"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_coverage_scope": audit["source_coverage_scope"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
        "source_capture_evidence_sha256": capture_hash, "geometry_valid_count": audit["geometry_valid_count"],
        "geometry_invalid_count": audit["geometry_invalid_count"], "geometry_empty_count": audit["geometry_empty_count"],
        "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "capture_directory": str(root)}

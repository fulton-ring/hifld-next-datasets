"""Complete native capture of the ORNL day care centers layer distributed by EPA.

Oak Ridge National Laboratory's Geographic Information Science and Technology
Group produced the national day care center spatial database (Singh, Tuttle,
Bhaduri, ISPRS Int. J. Geo-Inf. 2015; DOI 10.3390/ijgi4031290; OSTI 1210152).
The U.S. EPA Office of Environmental Information distributes the 2015 ORNL
Freedom edition of that database as the Day Care Centers layer of its
OEI/ORNL_Education SEGS service. This draft captures that complete
EPA-distributed layer only. The retired HIFLD hub, the DHS/HIFLD data.gov
harvest record, and HIFLD rescue archives are not pipeline inputs. Edition,
source, baseline, rights and release acceptance all remain false.
"""
from __future__ import annotations

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
import pyproj

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
    _canonical, _capture_identity, _load_capture, _sha,
    _validate_capture_request_identity, _validate_spatial_reference,
)

DATASET_SLUG = "child-care-centers"
FILE_SLUG = DATASET_SLUG
SERVICE_DIRECTORY_URL = "https://geodata.epa.gov/arcgis/rest/services/OEI?f=json"
SERVICE_URL = "https://geodata.epa.gov/arcgis/rest/services/OEI/ORNL_Education/MapServer"
LAYER_ID = 9
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Day Care Centers"
PUBLISHER = ("US EPA Office of Environmental Information (SEGS web service); data produced by "
             "Oak Ridge National Laboratory, Geographic Information Science and Technology Group "
             "(HSIP Freedom Database)")
EXPECTED_COUNT = 115036
EXPECTED_IDS = list(range(1, EXPECTED_COUNT + 1))
EXPECTED_ID_SHA256 = "7d34949428113483d9ed904d5d75f0b13916117bdbbb54b9ee768a1ae1178dce"
PAGE_SIZE = 1000
MAX_RESPONSE_BYTES = 8_000_000
MAX_CAPTURE_BYTES = 512_000_000
EXPECTED_SERVICE_MAP_NAME = "Education, US, 2015, ORNL, SEGS"
EXPECTED_SERVICE_TITLE = "Education, US, 2015, ORNL Freedom, SEGS"
EXPECTED_SERVICE_COPYRIGHT = (
    "Map service: USEPA Office of Environmental Information (OEI). Data: Oak Ridge National "
    "Laboratory, Geographic Information Sciences and Technology Group/HSIP Freedom Database.")
EXPECTED_SERVICE_SUBJECT = (
    "This SEGS web service contains point layers depicting educational institutions in the US, "
    "from Oak Ridge National Laboratory.")
EXPECTED_SERVICE_LAYERS = [
    (0, "Colleges and Universities Labels (Labels)", "esriGeometryPoint"),
    (1, "Supplemental Colleges (labels)", "esriGeometryPoint"),
    (2, "Private School (Labels)", "esriGeometryPoint"),
    (3, "Public School (Labels)", "esriGeometryPoint"),
    (4, "Day Care Center (Labels)", "esriGeometryPoint"),
    (5, "Colleges and Universities", "esriGeometryPoint"),
    (6, "Supplemental Colleges", "esriGeometryPoint"),
    (7, "Private Schools", "esriGeometryPoint"),
    (8, "Public Schools", "esriGeometryPoint"),
    (9, "Day Care Centers", "esriGeometryPoint"),
]
EXPECTED_FIELD_DECLARATIONS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "nullable": False, "domain": None},
    {"name": "Shape", "type": "esriFieldTypeGeometry", "alias": "Shape", "nullable": True, "domain": None},
    {"name": "ID", "type": "esriFieldTypeString", "alias": "ID", "length": 50, "nullable": True, "domain": None},
    {"name": "NAME", "type": "esriFieldTypeString", "alias": "NAME", "length": 255, "nullable": True, "domain": None},
    {"name": "ADDRESS", "type": "esriFieldTypeString", "alias": "ADDRESS", "length": 255, "nullable": True, "domain": None},
    {"name": "CITY", "type": "esriFieldTypeString", "alias": "CITY", "length": 50, "nullable": True, "domain": None},
    {"name": "STATE", "type": "esriFieldTypeString", "alias": "STATE", "length": 50, "nullable": True, "domain": None},
    {"name": "ZIP", "type": "esriFieldTypeString", "alias": "ZIP", "length": 50, "nullable": True, "domain": None},
    {"name": "TYPE", "type": "esriFieldTypeString", "alias": "TYPE", "length": 50, "nullable": True, "domain": None},
    {"name": "SOURCE", "type": "esriFieldTypeString", "alias": "SOURCE", "length": 255, "nullable": True, "domain": None},
    {"name": "STATE_FIPS", "type": "esriFieldTypeString", "alias": "STATE_FIPS", "length": 50, "nullable": True, "domain": None},
    {"name": "CNTY_FIPS", "type": "esriFieldTypeString", "alias": "CNTY_FIPS", "length": 255, "nullable": True, "domain": None},
    {"name": "COUNTY", "type": "esriFieldTypeString", "alias": "COUNTY", "length": 50, "nullable": True, "domain": None},
    {"name": "ADDRESS2", "type": "esriFieldTypeString", "alias": "ADDRESS2", "length": 255, "nullable": True, "domain": None},
    {"name": "ZIP4", "type": "esriFieldTypeString", "alias": "ZIP4", "length": 50, "nullable": True, "domain": None},
    {"name": "TELEPHONE", "type": "esriFieldTypeString", "alias": "TELEPHONE", "length": 50, "nullable": True, "domain": None},
    {"name": "STATUS", "type": "esriFieldTypeString", "alias": "STATUS", "length": 50, "nullable": True, "domain": None},
    {"name": "POPULATION", "type": "esriFieldTypeInteger", "alias": "POPULATION", "nullable": True, "domain": None},
    {"name": "COUNTYFIPS", "type": "esriFieldTypeString", "alias": "COUNTYFIPS", "length": 50, "nullable": True, "domain": None},
    {"name": "COUNTRY", "type": "esriFieldTypeString", "alias": "COUNTRY", "length": 50, "nullable": True, "domain": None},
    {"name": "LATITUDE", "type": "esriFieldTypeSingle", "alias": "LATITUDE", "nullable": True, "domain": None},
    {"name": "LONGITUDE", "type": "esriFieldTypeSingle", "alias": "LONGITUDE", "nullable": True, "domain": None},
    {"name": "NAICS_CODE", "type": "esriFieldTypeString", "alias": "NAICS_CODE", "length": 50, "nullable": True, "domain": None},
    {"name": "NAICS_DESC", "type": "esriFieldTypeString", "alias": "NAICS_DESC", "length": 255, "nullable": True, "domain": None},
    {"name": "SOURCE_DATE", "type": "esriFieldTypeDate", "alias": "SOURCE_DATE", "length": 8, "nullable": True, "domain": None},
    {"name": "VAL_METHOD", "type": "esriFieldTypeString", "alias": "VAL_METHOD", "length": 50, "nullable": True, "domain": None},
    {"name": "VAL_DATE", "type": "esriFieldTypeDate", "alias": "VAL_DATE", "length": 8, "nullable": True, "domain": None},
    {"name": "WEBSITE", "type": "esriFieldTypeString", "alias": "WEBSITE", "length": 255, "nullable": True, "domain": None},
    {"name": "STATE_SUBTYPE", "type": "esriFieldTypeString", "alias": "STATE_SUBTYPE", "length": 50, "nullable": True, "domain": None},
]
ATTRIBUTE_FIELDS = [field for field in EXPECTED_FIELD_DECLARATIONS if field["name"] != "Shape"]
ATTRIBUTE_FIELD_NAMES = {field["name"] for field in ATTRIBUTE_FIELDS}
EXPECTED_EXTENT = {"xmin": -166.53141479199996, "ymin": 17.948361168000076,
                   "xmax": -65.43435807999998, "ymax": 67.25950000000006}
CAPTURE_NAMES = {"service_directory", "service_before", "layer_before", "count_before", "ids_before",
                 "layer_after", "count_after", "ids_after", "service_after",
                 *{f"page_{index:03d}" for index in range((EXPECTED_COUNT + PAGE_SIZE - 1) // PAGE_SIZE)}}
DELEGATION_EVIDENCE = {
    "epa_service_folder_url": SERVICE_DIRECTORY_URL,
    "epa_service_map_name": EXPECTED_SERVICE_MAP_NAME,
    "epa_service_title": EXPECTED_SERVICE_TITLE,
    "epa_service_copyright_text": EXPECTED_SERVICE_COPYRIGHT,
    "producer_publication": "OSTI 1210152; DOI 10.3390/ijgi4031290 (ORNL GIST Group day care spatial database method paper)",
    "basis": ("The EPA OEI SEGS service metadata identifies the data as Oak Ridge National "
              "Laboratory, Geographic Information Sciences and Technology Group/HSIP Freedom "
              "Database, procured for EPA through the Office of Environmental Information, with "
              "access and use constraints of none. Whether this EPA distribution is an accepted "
              "successor of the retired 2021-2022 HIFLD edition remains an unaccepted decision."),
}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _capture_roster(ids: list[int]) -> list[tuple[str, str]]:
    urls = [("service_directory", SERVICE_DIRECTORY_URL),
            ("service_before", SERVICE_URL + "?f=pjson"), ("layer_before", LAYER_URL + "?f=pjson"),
            ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
            ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}))]
    for index, start in enumerate(range(0, len(ids), PAGE_SIZE)):
        urls.append((f"page_{index:03d}", _query({"where": "1=1", "outFields": "*",
            "returnGeometry": "true", "outSR": "4326", "orderByFields": "OBJECTID ASC",
            "resultOffset": start, "resultRecordCount": PAGE_SIZE, "f": "json"})))
    urls.extend([("layer_after", LAYER_URL + "?f=pjson"),
                 ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                 ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                 ("service_after", SERVICE_URL + "?f=pjson")])
    return urls


def _bounded_capture(client: httpx.Client, url: str, root: Path, name: str) -> tuple[bytes, dict]:
    retained = sum(path.stat().st_size for suffix in ("*.body", "*.wire")
                   for path in root.glob(suffix) if path.is_file())
    if retained + 2 * MAX_RESPONSE_BYTES > MAX_CAPTURE_BYTES:
        raise ValueError("source_capture_budget_exceeded_before_request")
    return capture_get(client, url, root, name, max_bytes=MAX_RESPONSE_BYTES)


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


def _point_wkb(x: float, y: float) -> bytes:
    return b"\x01" + struct.pack("<I", 1) + struct.pack("<dd", x, y)


def _validate(captures: dict[str, tuple[bytes, dict]], *,
              expected_count: int | None = None,
              expected_id_sha256: str | None = None) -> tuple[list[dict], list[dict], dict]:
    """Validate a complete capture roster against pinned or injected expectations.

    Keyword defaults pin the production source population; tests inject a
    small synthetic population to exercise every drift branch offline.
    """
    if expected_count is None:
        expected_count = EXPECTED_COUNT
    if expected_id_sha256 is None:
        expected_id_sha256 = EXPECTED_ID_SHA256
    if type(expected_count) is not int or expected_count <= 0:
        raise ValueError("invalid_expected_population")
    expected_ids = list(range(1, expected_count + 1))
    page_count = (expected_count + PAGE_SIZE - 1) // PAGE_SIZE
    expected_names = ({"service_directory", "service_before", "layer_before", "count_before",
                       "ids_before", "layer_after", "count_after", "ids_after", "service_after"}
                      | {f"page_{index:03d}" for index in range(page_count)})
    if set(captures) != expected_names:
        raise ValueError("source_capture_name_roster_drift")
    docs = {name: json.loads(body) for name, (body, _receipt) in captures.items()}
    directory = docs["service_directory"]
    if not any(service.get("name") == "OEI/ORNL_Education" and service.get("type") == "MapServer"
               for service in directory.get("services", []) if isinstance(service, dict)):
        raise ValueError("epa_oeni_service_directory_drift")
    service = docs["service_before"]
    document_info = service.get("documentInfo", {})
    advertised = [(layer.get("id"), layer.get("name"), layer.get("geometryType"))
                  for layer in service.get("layers", []) if isinstance(layer, dict)]
    if (service.get("mapName") != EXPECTED_SERVICE_MAP_NAME
            or service.get("copyrightText") != EXPECTED_SERVICE_COPYRIGHT
            or document_info.get("Title") != EXPECTED_SERVICE_TITLE
            or document_info.get("Subject") != EXPECTED_SERVICE_SUBJECT
            or advertised != EXPECTED_SERVICE_LAYERS):
        raise ValueError("epa_ornl_education_service_identity_drift")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    layer = docs["layer_before"]
    if layer.get("fields") != EXPECTED_FIELD_DECLARATIONS:
        raise ValueError("source_field_schema_drift")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("maxRecordCount") != PAGE_SIZE
            or layer.get("advancedQueryCapabilities", {}).get("supportsOrderBy") is not True
            or layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True
            or {key: layer.get("extent", {}).get(key) for key in EXPECTED_EXTENT} != EXPECTED_EXTENT):
        raise ValueError("source_layer_identity_extent_or_limit_drift")
    if layer.get("hasZ") or layer.get("hasM"):
        raise ValueError("source_layer_dimensional_drift")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    extent_reference = layer.get("extent", {}).get("spatialReference")
    if extent_reference is not None:
        _validate_spatial_reference(extent_reference, "source_layer_extent")
    if docs["layer_after"] != layer:
        raise ValueError("source_layer_metadata_changed_during_capture")
    count_before, count_after = docs["count_before"].get("count"), docs["count_after"].get("count")
    ids_before, ids_after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (count_before != expected_count or count_after != count_before
            or not isinstance(ids_before, list) or not isinstance(ids_after, list)
            or any(type(oid) is not int for oid in ids_before) or any(type(oid) is not int for oid in ids_after)
            or ids_before != expected_ids or ids_after != ids_before
            or len(set(ids_before)) != expected_count
            or docs["ids_before"].get("objectIdFieldName") != "OBJECTID"
            or docs["ids_after"].get("objectIdFieldName") != "OBJECTID"
            or _sha(_canonical(sorted(ids_before))) != expected_id_sha256):
        raise ValueError("source_count_or_id_population_drift")
    features = []
    geometry_valid = 0
    geometry_records = []
    states = set()
    for page_index in range(page_count):
        name = f"page_{page_index:03d}"
        page = docs[name]
        if "error" in page:
            raise ValueError("source_page_error_or_transfer_limit:" + name)
        exceeded = page.get("exceededTransferLimit") is True
        final_page = page_index == page_count - 1
        if final_page and exceeded:
            # A transfer-limited final page means the population was not fully
            # transferred; a non-final full page legitimately carries the flag.
            raise ValueError("source_page_error_or_transfer_limit:" + name)
        if page.get("hasZ") or page.get("hasM"):
            raise ValueError("source_page_dimensional_drift:" + name)
        _validate_spatial_reference(page.get("spatialReference"), "source_" + name + "_page")
        rows = page.get("features")
        expected_slice = expected_ids[page_index * PAGE_SIZE:(page_index + 1) * PAGE_SIZE]
        if not isinstance(rows, list) or len(rows) != len(expected_slice):
            raise ValueError("source_page_population_incomplete:" + name)
        page_ids = []
        for feature in rows:
            attrs, geometry = feature.get("attributes"), feature.get("geometry")
            if (not isinstance(attrs, dict) or len(attrs) != len(ATTRIBUTE_FIELDS)
                    or set(attrs) != ATTRIBUTE_FIELD_NAMES):
                raise ValueError("source_attribute_roster_or_order_drift:" + name)
            for field in ATTRIBUTE_FIELDS:
                value = attrs[field["name"]]
                if value is None:
                    continue
                kind = field["type"]
                if kind == "esriFieldTypeOID" and (type(value) is not int or value <= 0):
                    raise ValueError("source_oid_value_type_drift:" + name)
                if kind == "esriFieldTypeInteger" and type(value) is not int:
                    raise ValueError("source_integer_value_type_drift:" + field["name"])
                if kind == "esriFieldTypeString" and not isinstance(value, str):
                    raise ValueError("source_string_value_type_drift:" + field["name"])
                if kind == "esriFieldTypeDate" and type(value) is not int:
                    raise ValueError("source_date_value_type_drift:" + field["name"])
                if kind == "esriFieldTypeSingle":
                    if (type(value) not in (int, float) or isinstance(value, bool)
                            or not math.isfinite(value)):
                        raise ValueError("source_single_value_type_drift:" + field["name"])
                    if struct.unpack("<f", struct.pack("<f", value))[0] != value:
                        raise ValueError("source_single_precision_loss:" + field["name"])
            if not isinstance(geometry, dict) or set(geometry) != {"x", "y"}:
                raise ValueError("source_point_geometry_structure_drift:" + name)
            x, y = geometry["x"], geometry["y"]
            for coordinate in (x, y):
                if (type(coordinate) not in (int, float) or isinstance(coordinate, bool)
                        or not math.isfinite(coordinate)):
                    raise ValueError("source_point_ordinate_type_drift:" + name)
            if not (EXPECTED_EXTENT["xmin"] <= x <= EXPECTED_EXTENT["xmax"]
                    and EXPECTED_EXTENT["ymin"] <= y <= EXPECTED_EXTENT["ymax"]):
                raise ValueError("source_geometry_outside_declared_coverage:" + name)
            oid = attrs.get("OBJECTID")
            if type(oid) is not int or oid not in expected_slice:
                raise ValueError("source_feature_id_roster_drift:" + name)
            page_ids.append(oid)
            geometry_records.append([oid, x, y])
            if attrs.get("STATE") is not None:
                states.add(attrs["STATE"])
            geometry_valid += 1
            feature["attributes"] = {field["name"]: attrs[field["name"]] for field in ATTRIBUTE_FIELDS}
            feature["geometry"] = {"x": x, "y": y}
            features.append(feature)
        if page_ids != expected_slice or len(set(page_ids)) != len(page_ids):
            raise ValueError("source_page_id_population_drift:" + name)
    ids = [feature["attributes"]["OBJECTID"] for feature in features]
    if ids != expected_ids or len(features) != expected_count or len(set(ids)) != expected_count:
        raise ValueError("source_feature_id_population_drift")
    record_sha256 = _sha(_canonical([feature["attributes"] for feature in features]))
    geometry_sha256 = _sha(_canonical(geometry_records))
    bounds = [min(record[1] for record in geometry_records), min(record[2] for record in geometry_records),
              max(record[1] for record in geometry_records), max(record[2] for record in geometry_records)]
    provider_ids = [feature["attributes"]["ID"] for feature in features]
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(expected_ids)),
        "source_id_min": expected_ids[0], "source_id_max": expected_ids[-1],
        "source_schema_sha256": _sha(_canonical(EXPECTED_FIELD_DECLARATIONS)),
        "source_record_sha256": record_sha256, "source_geometry_sha256": geometry_sha256,
        "source_geometry_type": "esriGeometryPoint", "source_wkid": 4326,
        "geometry_valid_count": geometry_valid, "geometry_empty_count": 0,
        "geometry_invalid_count": len(features) - geometry_valid,
        "geometry_bounds": bounds, "source_extent": layer["extent"],
        "state_distinct_count": len(states), "state_roster": sorted(states),
        "provider_id_distinct_count": len(set(provider_ids)),
        "provider_id_duplicate_count": len(provider_ids) - len(set(provider_ids)),
        "source_coverage_scope": (
            "Complete observed Day Care Centers layer (layer 9) of the EPA OEI SEGS "
            "OEI/ORNL_Education MapServer: the 2015 ORNL Freedom edition of the national day care "
            "centers spatial database produced by ORNL's Geographic Information Science and "
            "Technology Group, covering the 50 states, the District of Columbia and Puerto Rico. "
            "This is not the retired 2021-06-24 to 2022-03-01 HIFLD edition; edition and source "
            "succession remain unaccepted."),
        "drift_guard": (
            "Exact EPA service folder, service identity (map name, document title/subject, "
            "copyright text), ten-layer roster, full field declarations/types, layer identity, "
            "extent, record limit and pagination/orderBy capabilities, explicit EPSG:4326 "
            "layer/page spatial references, before and after count and OBJECTID rosters, per-page "
            "id slices, attribute types including float32 round-trip for single-precision columns "
            "and declared-extent containment are checked; the transfer-limit flag is rejected on "
            "the final page; duplicate provider ID values are preserved."),
    }
    return EXPECTED_FIELD_DECLARATIONS, features, audit


_ARROW_TYPES = {"esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
                "esriFieldTypeSingle": pa.float32(), "esriFieldTypeString": pa.string(),
                "esriFieldTypeDate": pa.timestamp("ms", tz="UTC")}


def _parquet_bytes(fields: list[dict], features: list[dict]) -> bytes:
    import io
    schema_fields, columns = [], {}
    for field in fields:
        name, kind = field["name"], field["type"]
        if kind == "esriFieldTypeGeometry":
            values = [_point_wkb(feature["geometry"]["x"], feature["geometry"]["y"])
                      for feature in features]
            arrow_type = pa.binary()
        else:
            values = [feature["attributes"][name] for feature in features]
            arrow_type = _ARROW_TYPES[kind]
        schema_fields.append(pa.field(name, arrow_type, nullable=True))
        columns[name] = pa.array(values, type=arrow_type)
    geo = {"version": "1.1.0", "primary_column": "Shape",
           "columns": {"Shape": {"encoding": "WKB", "geometry_types": ["Point"],
                                 "crs": pyproj.CRS.from_epsg(4326).to_json_dict(), "edges": "planar"}}}
    table = pa.Table.from_pydict(columns, schema=pa.schema(schema_fields).with_metadata({b"geo": _canonical(geo)}))
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    return sink.getvalue()


def capture_child_care_centers(storage: StagingStorageResource, *, version: str,
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
    if shutil.disk_usage(staging).free < MAX_CAPTURE_BYTES + 384_000_000:
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
                Path("/tmp/hifld-native-reconstruction/36/captures") / (version + "-" + uuid4().hex))
        if ".." in root.parts or any(path.is_symlink() for path in (root, *root.parents)):
            raise ValueError("unsafe_capture_directory")
        root.mkdir(parents=True, exist_ok=False)
        captures = {}
        try:
            with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(60, connect=15),
                              headers={"User-Agent": "hifld-next-datasets/child-care-centers-draft"}) as client:
                ids_body, ids_receipt = _bounded_capture(
                    client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}), root, "ids_before")
                ids = json.loads(ids_body).get("objectIds")
                if (not isinstance(ids, list) or any(type(oid) is not int for oid in ids)
                        or ids != EXPECTED_IDS or len(set(ids)) != EXPECTED_COUNT):
                    raise ValueError("source_seed_id_population_drift")
                captures["ids_before"] = (ids_body, ids_receipt)
                for name, url in _capture_roster(ids):
                    if name == "ids_before":
                        continue
                    captures[name] = _bounded_capture(client, url, root, name)
            fields, features, audit = _validate(captures)
        except Exception as exc:
            receipts = {}
            for path in sorted(root.glob("*.receipt.json")):
                try:
                    receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
                except (OSError, ValueError):
                    receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
            (root / "capture_manifest.json").write_bytes(_canonical({"complete": False,
                "captured_at": datetime.now(timezone.utc).isoformat(), "error_code": type(exc).__name__,
                "receipts": receipts}) + b"\n")
            if captures:
                quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
            raise
        (root / "capture_manifest.json").write_bytes(_canonical({"complete": True,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "source_feature_count": audit["source_feature_count"],
            "source_record_sha256": audit["source_record_sha256"],
            "source_geometry_sha256": audit["source_geometry_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")
        captures = _read_replay(root)

    fields, features, audit = _validate(captures)
    identity = processing_identity(Path(__file__).parents[3])
    capture_hash = _sha(_canonical(_capture_identity(captures, root)))
    audit.update({"source_capture_evidence_sha256": capture_hash,
        "source_capture_evidence": _capture_identity(captures, root),
        "source_receipts": {name: receipt for name, (_body, receipt) in sorted(captures.items())},
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
        "baseline_accepted": False})
    parquet = _parquet_bytes(fields, features)
    version = "v" + _sha(_canonical({"source_record_sha256": audit["source_record_sha256"],
        "source_geometry_sha256": audit["source_geometry_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash, "processing_identity": identity}))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    output_path = f"geoparquet/{FILE_SLUG}.parquet"
    receipts = {name: receipt for name, (_body, receipt) in sorted(captures.items())}
    inputs = [{"input_id": name,
        "producer": "Oak Ridge National Laboratory, Geographic Information Science and Technology Group (HSIP Freedom Database)",
        "host": urlsplit(receipt["url"]).hostname, "role": "official_distributor",
        "canonical_url": LAYER_URL if name.startswith("page_") or name.startswith("layer_") or
            name.startswith("count_") or name.startswith("ids_") else
            (SERVICE_URL if name.startswith("service_") else SERVICE_DIRECTORY_URL),
        "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
        "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
        "bytes": receipt["bytes"], "sha256": receipt["sha256"],
        "wire_bytes": receipt["wire_bytes"], "wire_sha256": receipt["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": ("The EPA SEGS service metadata labels this the 2015 ORNL "
                                        "Freedom edition but publishes no dated release for the layer."),
        "license": "Unresolved; the EPA service states no access or use constraints, but rights review for the ORNL-produced data is pending.",
        "attribution": "US EPA Office of Environmental Information (map service); Oak Ridge National Laboratory GIST Group (data); attribution and rights review pending.",
        "distributor": "US EPA Office of Environmental Information (SEGS)",
        "delegation_evidence": DELEGATION_EVIDENCE}
        for name, receipt in sorted(receipts.items())]
    manifest = {"schema_version": 1, "approved": False, "title": "Child Care Centers",
        "description": ("Complete observed Day Care Centers layer of the EPA OEI SEGS "
                        "OEI/ORNL_Education service: the 2015 ORNL Freedom edition of the national "
                        "day care centers spatial database produced by ORNL's GIST Group. Edition "
                        "succession, source, baseline, rights and release remain unaccepted."),
        "publisher": PUBLISHER,
        "agency": "U.S. Department of Energy / Oak Ridge National Laboratory (producer); U.S. Environmental Protection Agency (distributor)",
        "source_url": LAYER_URL, "license": None, "source_modified": None,
        "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"], "inputs": inputs,
        "transformations": [{"operation": "Esri point layer to GeoParquet WKB candidate", "parameters": {
            "source_fields": "all declared attributes retained in source order and declared types; SOURCE_DATE and VAL_DATE epochs are represented as UTC millisecond timestamps",
            "source_ids": "OBJECTID and the provider ID column retained unchanged, including duplicates",
            "source_crs": "EPSG:4326",
            "geometry": "source point x/y retained as WKB in EPSG:4326; no reprojection, repair, or relocation; the source LATITUDE/LONGITUDE geocode columns are preserved unchanged alongside the relocated on-entity geometry",
            "raw_evidence": "complete decoded response and wire bodies retained with checksummed receipts",
            "candidate_status": "unapproved; edition, source, baseline, schema/ID, rights and release decisions pending"}}],
        "lineage": ("ORNL's Geographic Information Science and Technology Group produced the national "
                    "day care centers spatial database (Singh, Tuttle, Bhaduri, ISPRS Int. J. Geo-Inf. "
                    "2015, DOI 10.3390/ijgi4031290; OSTI 1210152), which contributes to the DHS HSIP "
                    "program. The U.S. EPA Office of Environmental Information distributes the 2015 "
                    "ORNL Freedom edition as the Day Care Centers layer (layer 9) of its "
                    "OEI/ORNL_Education SEGS service, procured for EPA through OEI. This candidate "
                    "captures that complete EPA-distributed layer only. The historical HIFLD inventory "
                    "row's 2021-06-24 to 2022-03-01 edition is not captured: the HIFLD hub item is "
                    "retired (HTTP 404), the DHS/HIFLD data.gov harvest record "
                    "(MGMT-GMO-HIFLD-126087) exposes no distribution endpoint, and DataLumos/PASDA "
                    "HIFLD rescue copies are ineligible pipeline inputs under project policy. Whether "
                    "the retired HIFLD edition descends from this ORNL 2015 database, and whether the "
                    "EPA distribution is an accepted successor source, remain unaccepted decisions."),
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Native attributes, IDs and point geometry are retained in a typed candidate.",
            "objects": [{"path": output_path, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No format-specific field mapping has been approved.",
            "geopackage": "No format-specific mapping has been approved.",
            "pmtiles": "No display generalization or release policy has been approved."}}
    validate_release_provenance(manifest); validate_output_policy(manifest); provenance_projection(manifest)
    audit.update({"source_fields": fields, "official_layer_name": LAYER_NAME, "official_layer_id": LAYER_ID,
                  "official_service": SERVICE_URL, "official_distributor": "US EPA Office of Environmental Information (SEGS)",
                  "source_population_complete": True})
    outputs = {output_path: parquet, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in sorted(captures.items()):
        _body, wire, _receipt = _load_capture(root, name)
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = wire
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {"approved": False, "version": version,
        "objects": {output_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_population_complete": True, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"], "source_release_accepted": False,
        "freshness_accepted": False, "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    fingerprint = _sha(_canonical({"identity": identity,
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "source_capture_sha256": capture_hash}))
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
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
            fingerprint, identity, audit["source_record_sha256"], audit["source_schema_sha256"],
            original.get("objects", {}), original_receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_id_min": 1,
            "source_id_max": EXPECTED_COUNT, "source_schema_sha256": audit["source_schema_sha256"],
            "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
            "source_capture_evidence_sha256": capture_hash,
            "geometry_valid_count": audit["geometry_valid_count"],
            "geometry_invalid_count": audit["geometry_invalid_count"],
            "geometry_empty_count": audit["geometry_empty_count"],
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(root)}
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
        "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": receipts, "objects": descriptors})
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": EXPECTED_COUNT, "source_population_complete": True,
        "source_id_sha256": audit["source_id_sha256"], "source_id_min": 1,
        "source_id_max": EXPECTED_COUNT, "source_schema_sha256": audit["source_schema_sha256"],
        "source_coverage_scope": audit["source_coverage_scope"],
        "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
        "source_capture_evidence_sha256": capture_hash,
        "geometry_valid_count": audit["geometry_valid_count"],
        "geometry_invalid_count": audit["geometry_invalid_count"],
        "geometry_empty_count": audit["geometry_empty_count"],
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False, "capture_directory": str(root)}

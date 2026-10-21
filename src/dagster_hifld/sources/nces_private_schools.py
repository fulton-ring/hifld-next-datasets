"""Pinned official NCES EDGE 2023-24 private-school location snapshot."""
from __future__ import annotations

import hashlib
import io
import json
import math
import shutil
import struct
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import geopandas as gpd
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    capture_get,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = FILE_SLUG = "private-schools"
SOURCE_URL = "https://nces.ed.gov/programs/edge/data/EDGE_GEOCODE_PRIVATESCH_2324.zip"
SERVICE_URL = "https://nces.ed.gov/opengis/rest/services/K12_School_Locations/EDGE_GEOCODE_PRIVATESCH_2324/MapServer"
LAYER_URL = SERVICE_URL + "/0"
SOURCE_RELEASE = "2023-2024"
EXPECTED_LAYER = "EDGE_GEOCODE_PRIVATESCH_2324"
EXPECTED_ZIP_BYTES = 6_454_097
EXPECTED_ZIP_SHA256 = "94271f590036600c869e864467807b54eee75bf6fa98b1a850c289669116d073"
EXPECTED_COUNT = 22_510
EXPECTED_MEMBERS = {
    "EDGE_GEOCODE_PRIVATESCH_2324.shx",
    "EDGE_GEOCODE_PRIVATESCH_2324.cpg",
    "EDGE_GEOCODE_PRIVATESCH_2324.dbf",
    "EDGE_GEOCODE_PRIVATESCH_2324.prj",
    "EDGE_GEOCODE_PRIVATESCH_2324.sbn",
    "EDGE_GEOCODE_PRIVATESCH_2324.sbx",
    "EDGE_GEOCODE_PRIVATESCH_2324.shp",
    "EDGE_GEOCODE_PRIVATESCH_2324.shp.xml",
    "edge_geocode_privatesch_2324.sas7bdat",
    "EDGE_GEOCODE_PRIVATESCH_2324.xlsx",
}
EXPECTED_FIELDS = [
    ("PPIN", "C", 8, 0), ("NAME", "C", 50, 0), ("STREET", "C", 100, 0),
    ("CITY", "C", 30, 0), ("STATE", "C", 2, 0), ("ZIP", "C", 5, 0),
    ("STFIP", "C", 2, 0), ("CNTY", "C", 3, 0), ("NAMELSAD", "C", 100, 0),
    ("LOCALE", "C", 2, 0), ("LAT", "F", 19, 11), ("LON", "F", 19, 11),
    ("CBSA", "C", 5, 0), ("NMCBSA", "C", 100, 0), ("CBSATYPE", "C", 1, 0),
    ("CSA", "C", 3, 0), ("NMCSA", "C", 100, 0), ("CD", "C", 4, 0),
    ("SLDL", "C", 5, 0), ("SLDU", "C", 5, 0), ("SCHOOLYEAR", "C", 10, 0),
]
EXPECTED_BOUNDS = [-159.71300899999997, 19.490814000000057, -67.38873999999998, 64.87414300000006]
EXPECTED_SERVICE_FIELDS = [
    "OBJECTID", "Shape", "PPIN", "NAME", "STREET", "CITY", "STATE", "ZIP", "STFIP", "CNTY",
    "NMCNTY", "LOCALE", "LAT", "LON", "CBSA", "NMCBSA", "CBSATYPE", "CSA", "NMCSA",
    "CD", "SLDL", "SLDU", "SCHOOLYEAR",
]
EXPECTED_SERVICE_COUNT = 22_510
EXPECTED_SERVICE_IDS_SHA256 = "4b7180d4ff044d0c388f1c6b8d1f92441756884ac0808df09f85bd924fbc7f6b"
EXPECTED_SERVICE_WKID = 4269
EXPECTED_SOURCE_ID_SHA256 = "bfc3a232acd4e362f77888cd445f33ce2cf3577f0808f511944b9f53af4ecec1"
EXPECTED_SOURCE_RECORD_SHA256 = "6b4bba5a8cc6e2d4aecd56277d295739d281a94304da10af68a0a251f49ad415"
EXPECTED_SOURCE_GEOMETRY_SHA256 = "52d2d780268b0b96564ecf7e374fcb86f553b56b086842434e1c7a056afac503"
WGS84 = CRS.from_epsg(4326)
CAPTURE_URLS = {
    "service_before": SERVICE_URL + "?f=pjson",
    "layer_before": LAYER_URL + "?f=pjson",
    "count_before": LAYER_URL + "/query?where=1%3D1&returnCountOnly=true&f=json",
    "ids_before": LAYER_URL + "/query?where=1%3D1&returnIdsOnly=true&f=json",
    "page_first_probe": LAYER_URL + "/query?where=1%3D1&outFields=OBJECTID%2CPPIN&returnGeometry=false&orderByFields=OBJECTID&resultOffset=0&resultRecordCount=2000&f=json",
    "page_last_probe": LAYER_URL + "/query?where=1%3D1&outFields=OBJECTID%2CPPIN&returnGeometry=false&orderByFields=OBJECTID&resultOffset=22500&resultRecordCount=10&f=json",
    "official_zip": SOURCE_URL,
    "service_after": SERVICE_URL + "?f=pjson",
    "layer_after": LAYER_URL + "?f=pjson",
    "count_after": LAYER_URL + "/query?where=1%3D1&returnCountOnly=true&f=json",
    "ids_after": LAYER_URL + "/query?where=1%3D1&returnIdsOnly=true&f=json",
}
CAPTURE_NAMES = set(CAPTURE_URLS)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _field_schema(dbf: Path) -> list[tuple[str, str, int, int]]:
    raw = dbf.read_bytes()
    header_len = int.from_bytes(raw[8:10], "little")
    if header_len < 33 or header_len > len(raw):
        raise ValueError("source_dbf_header_invalid")
    fields = []
    offset = 32
    while offset < header_len and raw[offset] != 0x0D:
        descriptor = raw[offset:offset + 32]
        if len(descriptor) != 32:
            raise ValueError("source_dbf_descriptor_incomplete")
        fields.append((descriptor[:11].split(b"\0", 1)[0].decode("ascii"),
                       chr(descriptor[11]), descriptor[16], descriptor[17]))
        offset += 32
    if offset >= header_len or raw[offset] != 0x0D:
        raise ValueError("source_dbf_terminator_missing")
    count = int.from_bytes(raw[4:8], "little")
    if count != EXPECTED_COUNT:
        raise ValueError("source_dbf_record_count_drift")
    return fields


def _service_evidence(bodies: dict[str, bytes]) -> dict[str, object]:
    service_before = json.loads(bodies["service_before"])
    service_after = json.loads(bodies["service_after"])
    layer_before = json.loads(bodies["layer_before"])
    layer_after = json.loads(bodies["layer_after"])
    if service_before != service_after or layer_before != layer_after:
        raise ValueError("source_service_drift_during_capture")
    layers = service_before.get("layers", [])
    if not any(layer.get("id") == 0 and layer.get("name") == "Private School Locations 2023-24" for layer in layers):
        raise ValueError("source_service_layer_identity_drift")
    if layer_before.get("name") != "Private School Locations 2023-24" or layer_before.get("geometryType") != "esriGeometryPoint":
        raise ValueError("source_service_layer_metadata_drift")
    if int(layer_before.get("maxRecordCount", 0)) != 2000:
        raise ValueError("source_service_transfer_limit_drift")
    fields = [field.get("name") for field in layer_before.get("fields", [])]
    if fields != EXPECTED_SERVICE_FIELDS:
        raise ValueError("source_service_schema_drift")
    sr = layer_before.get("extent", {}).get("spatialReference", {})
    if int(sr.get("wkid", 0)) != EXPECTED_SERVICE_WKID or int(sr.get("latestWkid", 0)) != EXPECTED_SERVICE_WKID:
        raise ValueError("source_service_crs_drift")
    count_before = json.loads(bodies["count_before"])
    count_after = json.loads(bodies["count_after"])
    ids_before = json.loads(bodies["ids_before"])
    ids_after = json.loads(bodies["ids_after"])
    ids_a = ids_before.get("objectIds")
    ids_b = ids_after.get("objectIds")
    if count_before != {"count": EXPECTED_SERVICE_COUNT} or count_after != count_before:
        raise ValueError("source_service_count_drift")
    if ids_before.get("objectIdFieldName") != "OBJECTID" or ids_b != ids_a:
        raise ValueError("source_service_ids_drift_during_capture")
    if not isinstance(ids_a, list) or len(ids_a) != EXPECTED_SERVICE_COUNT or len(set(ids_a)) != EXPECTED_SERVICE_COUNT:
        raise ValueError("source_service_id_roster_incomplete")
    if sorted(ids_a) != list(range(1, EXPECTED_SERVICE_COUNT + 1)):
        raise ValueError("source_service_id_roster_drift")
    id_hash = _sha(_canonical(sorted(ids_a)))
    if EXPECTED_SERVICE_IDS_SHA256 and id_hash != EXPECTED_SERVICE_IDS_SHA256:
        raise ValueError("source_service_id_roster_drift")
    for name, first_id, expected_count in (("page_first_probe", 1, 2000), ("page_last_probe", 22501, 10)):
        page = json.loads(bodies[name])
        features = page.get("features")
        if not isinstance(features, list) or len(features) != expected_count:
            raise ValueError("source_service_pagination_probe_incomplete:" + name)
        page_ids = [feature.get("attributes", {}).get("OBJECTID") for feature in features]
        if page_ids != list(range(first_id, first_id + expected_count)):
            raise ValueError("source_service_pagination_probe_ids_drift:" + name)
        if name == "page_first_probe" and page.get("exceededTransferLimit") is not True:
            raise ValueError("source_service_pagination_limit_not_observed")
        if name == "page_last_probe" and page.get("exceededTransferLimit") not in (None, False):
            raise ValueError("source_service_pagination_last_page_incomplete")
    return {
        "service_name": service_before.get("mapName"),
        "service_layer_id": 0,
        "service_layer_name": layer_before.get("name"),
        "service_max_record_count": int(layer_before["maxRecordCount"]),
        "service_native_wkid": EXPECTED_SERVICE_WKID,
        "service_count": count_before["count"],
        "service_object_id_field": ids_before["objectIdFieldName"],
        "service_object_id_count": len(ids_a),
        "service_object_id_sha256": id_hash,
        "service_field_names": fields,
        "service_source_schema_diff": "The corroborating REST layer uses NMCNTY and EPSG:4269; the separately published NCES ZIP DBF uses NAMELSAD and its .prj declares EPSG:4326. These are preserved as distinct representations, not silently equated.",
        "service_metadata_sha256": _sha(bodies["layer_before"]),
    }


def _decode(body: bytes, tmp: Path) -> tuple[gpd.GeoDataFrame, dict[str, object]]:
    if len(body) != EXPECTED_ZIP_BYTES or _sha(body) != EXPECTED_ZIP_SHA256:
        raise ValueError("pinned_nces_2324_archive_drift")
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        infos = archive.infolist()
        names = [info.filename for info in infos]
        for info in infos:
            mode = info.external_attr >> 16
            parts = Path(info.filename).parts
            if (not info.filename or info.filename.startswith(("/", "\\")) or "\\" in info.filename
                    or ".." in parts or len(parts) != 1 or info.is_dir() or (mode & 0o170000) == 0o120000):
                raise ValueError("source_zip_member_path_rejected")
        if len(set(names)) != len(names) or set(names) != EXPECTED_MEMBERS:
            raise ValueError("source_zip_member_roster_drift")
        if sum(info.file_size for info in infos) > 40_000_000:
            raise ValueError("source_zip_uncompressed_size_drift")
        tmp.mkdir(parents=True, exist_ok=True)
        for name in EXPECTED_MEMBERS:
            if name.startswith(EXPECTED_LAYER + "."):
                with archive.open(name) as src, (tmp / name).open("xb") as dst:
                    shutil.copyfileobj(src, dst)
    shp = tmp / (EXPECTED_LAYER + ".shp")
    schema = _field_schema(tmp / (EXPECTED_LAYER + ".dbf"))
    if schema != EXPECTED_FIELDS:
        raise ValueError("source_dbf_schema_drift")
    import pyogrio
    info = pyogrio.read_info(shp)
    names = [field[0] for field in EXPECTED_FIELDS]
    if (info["layer_name"] != EXPECTED_LAYER or info["geometry_type"] != "Point"
            or info["crs"] != "EPSG:4326" or int(info["features"]) != EXPECTED_COUNT
            or list(info["fields"]) != names):
        raise ValueError("source_archive_layer_metadata_drift")
    frame = gpd.read_file(shp, engine="pyogrio")
    if (list(frame.columns) != names + ["geometry"] or len(frame) != EXPECTED_COUNT
            or frame.isna().any(axis=None) or frame.geometry.is_empty.any()
            or frame.geometry.geom_type.ne("Point").any()):
        raise ValueError("source_population_or_schema_drift")
    if not CRS.from_user_input(frame.crs).equals(WGS84, ignore_axis_order=True):
        raise ValueError("source_archive_crs_drift")
    if frame["PPIN"].duplicated().any() or not frame["PPIN"].astype(str).str.fullmatch(r"[A-Z0-9]{8}").all():
        raise ValueError("source_native_id_drift")
    if set(frame["SCHOOLYEAR"].unique()) != {SOURCE_RELEASE}:
        raise ValueError("source_period_drift")
    if (abs(frame.geometry.x.to_numpy() - frame["LON"].to_numpy()) > 1e-12).any():
        raise ValueError("source_longitude_attribute_drift")
    if (abs(frame.geometry.y.to_numpy() - frame["LAT"].to_numpy()) > 1e-12).any():
        raise ValueError("source_latitude_attribute_drift")
    bounds = list(map(float, frame.total_bounds))
    if bounds != EXPECTED_BOUNDS:
        raise ValueError("source_bounds_drift")
    attrs = [[value for value in row] for row in frame[names].itertuples(index=False, name=None)]
    ids = sorted(frame["PPIN"].astype(str).tolist())
    xy_x = frame.geometry.x.to_numpy()
    xy_y = frame.geometry.y.to_numpy()
    ppin_values = frame["PPIN"].astype(str).to_numpy()
    geometry = sorted([[str(ppin_values[i]), float(xy_x[i]), float(xy_y[i])]
                       for i in range(len(frame))], key=lambda row: row[0])
    id_sha, record_sha, geometry_sha = (_sha(_canonical(value)) for value in (ids, attrs, geometry))
    if EXPECTED_SOURCE_ID_SHA256 and id_sha != EXPECTED_SOURCE_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if EXPECTED_SOURCE_RECORD_SHA256 and record_sha != EXPECTED_SOURCE_RECORD_SHA256:
        raise ValueError("source_record_population_drift")
    if EXPECTED_SOURCE_GEOMETRY_SHA256 and geometry_sha != EXPECTED_SOURCE_GEOMETRY_SHA256:
        raise ValueError("source_geometry_population_drift")
    audit = {
        "source_feature_count": len(frame), "source_population_complete": True,
        "source_id_field": "PPIN", "source_id_unique": True,
        "source_id_min": min(ids), "source_id_max": max(ids), "source_id_sha256": id_sha,
        "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": _sha(_canonical(schema)), "source_dbf_schema": schema,
        "source_crs": "EPSG:4326", "source_geometry_type": "Point", "source_bounds": bounds,
        "source_period": SOURCE_RELEASE,
        "source_null_counts": {name: int(frame[name].isna().sum()) for name in names},
        "source_geometry_null_count": int(frame.geometry.isna().sum()),
        "source_geometry_empty_count": int(frame.geometry.is_empty.sum()),
        "source_coverage_scope": "NCES EDGE private-school point file for the 2023-2024 PSS school collection; nationwide U.S. source bounds, including Alaska and Hawaii. This is a changed successor candidate, not an accepted HIFLD compatibility claim.",
        "source_drift_guard": "Exact official ZIP bytes and member roster, all DBF field names/types/widths, full feature count, all PPIN identifiers, all attributes, native EPSG:4326 point XY, source survey year, and bounds.",
    }
    return frame, audit


def _point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, float(x), float(y))


def _parquet(frame: gpd.GeoDataFrame) -> bytes:
    arrays = []
    fields = []
    for name, kind, _width, _decimals in EXPECTED_FIELDS:
        if kind == "C":
            dtype = pa.string()
            values = [str(value) for value in frame[name].tolist()]
        else:
            dtype = pa.float64()
            values = [float(value) for value in frame[name].tolist()]
        arrays.append(pa.array(values, type=dtype, safe=True))
        fields.append(pa.field(name, dtype, nullable=True))
    geometries = [_point_wkb(x, y) for x, y in zip(frame.geometry.x, frame.geometry.y)]
    arrays.append(pa.array(geometries, type=pa.binary()))
    fields.append(pa.field("geometry", pa.binary(), nullable=False))
    geo_metadata = {
        "version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": WGS84.to_json_dict()}},
    }
    output = io.BytesIO()
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata={b"geo": _canonical(geo_metadata)}))
    pq.write_table(table, output, compression="zstd", use_dictionary=False)
    check = pq.read_table(pa.BufferReader(output.getvalue()))
    if check.num_rows != EXPECTED_COUNT:
        raise ValueError("candidate_row_count_readback_drift")
    for name, _kind, _width, _decimals in EXPECTED_FIELDS:
        expected = [str(value) if isinstance(value, str) else float(value) for value in frame[name].tolist()]
        if check[name].to_pylist() != expected:
            raise ValueError("candidate_attribute_readback_drift:" + name)
    if check["geometry"].to_pylist() != geometries:
        raise ValueError("candidate_geometry_readback_drift")
    return output.getvalue()


def _load_capture(root: Path) -> tuple[dict[str, bytes], dict[str, dict[str, object]]]:
    manifest_path = root / "capture_manifest.json"
    if root.is_symlink() or any(path.is_symlink() for path in root.parents) or manifest_path.is_symlink():
        raise ValueError("unsafe_capture_directory")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("complete") is not True or set(manifest.get("receipts", {})) != CAPTURE_NAMES:
        raise ValueError("source_capture_manifest_invalid")
    roster = manifest.get("artifact_roster")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if roster != capture_roster_digests(root, exclude={"capture_manifest.json"}):
        raise ValueError("source_capture_roster_digest_drift")
    bodies, receipts = {}, {}
    for name, url in CAPTURE_URLS.items():
        body_path, wire_path, receipt_path = (root / f"{name}.{suffix}" for suffix in ("body", "wire", "receipt.json"))
        if any(path.is_symlink() for path in (body_path, wire_path, receipt_path)):
            raise ValueError("capture_symlink_rejected")
        body, wire = body_path.read_bytes(), wire_path.read_bytes()
        receipt = json.loads(receipt_path.read_bytes())
        if (receipt.get("method") != "GET" or receipt.get("url") != url or receipt.get("final_url") != url
                or receipt.get("complete_body") is not True or receipt.get("status") != 200
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or receipt != manifest["receipts"][name]):
            raise ValueError("source_capture_receipt_invalid:" + name)
        bodies[name] = body
        receipts[name] = receipt
    return bodies, receipts


def _capture(root: Path, storage: StagingStorageResource, version: str) -> tuple[dict[str, bytes], dict[str, dict[str, object]]]:
    if root.exists():
        return _load_capture(root)
    if root.is_symlink() or any(path.is_symlink() for path in root.parents):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True, exist_ok=False)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(90, connect=20),
                          headers={"User-Agent": "hifld-next-datasets/private-schools-draft"}) as client:
            for name, url in CAPTURE_URLS.items():
                body, receipt = capture_get(client, url, root, name, max_bytes=30_000_000)
                captures[name] = (body, receipt)
                if receipt.get("method") != "GET" or receipt.get("url") != url or receipt.get("final_url") != url:
                    raise ValueError("source_request_identity_mismatch:" + name)
        _service_evidence({name: value[0] for name, value in captures.items()})
        with tempfile.TemporaryDirectory(dir=root) as directory:
            _decode(captures["official_zip"][0], Path(directory) / "shape")
        roster = capture_roster_digests(root, exclude={"capture_manifest.json"})
        manifest = {"schema_version": 1, "complete": True, "captured_at": datetime.now(timezone.utc).isoformat(),
                    "receipts": {name: receipt for name, (_body, receipt) in captures.items()}, "artifact_roster": roster}
        (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        return ({name: body for name, (body, _receipt) in captures.items()},
                {name: receipt for name, (_body, receipt) in captures.items()})
    except Exception as exc:
        for path in root.glob("*.receipt.json"):
            try:
                name = path.name.removesuffix(".receipt.json")
                receipt = json.loads(path.read_bytes())
                body_path = root / f"{name}.body"
                if name in CAPTURE_NAMES and body_path.exists() and (root / f"{name}.wire").exists():
                    captures.setdefault(name, (body_path.read_bytes(), receipt))
            except Exception:
                pass
        partial = {"schema_version": 1, "complete": False, "captured_at": datetime.now(timezone.utc).isoformat(),
                   "error_code": type(exc).__name__, "receipts": {name: receipt for name, (_body, receipt) in captures.items()}}
        (root / "capture_manifest.json").write_bytes(_canonical(partial) + b"\n")
        if captures:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
        raise


def capture_private_schools(storage: StagingStorageResource, *, version: str, capture_directory: str | None = None) -> dict[str, object]:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if not isinstance(version, str) or not version or Path(version).name != version or "/" in version or "\\" in version:
        raise ValueError("unsafe_capture_version")
    staging_root = Path(storage.local_dir).absolute()
    if ".." in staging_root.parts or any(path.is_symlink() for path in (staging_root, *staging_root.parents)):
        raise ValueError("unsafe_local_staging_root")
    staging_root.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(staging_root).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    root = requested or Path("/tmp/hifld-native-reconstruction/278/captures") / (version + "-" + uuid4().hex)
    bodies, receipts = _capture(root, storage, version)
    try:
        service = _service_evidence(bodies)
        with tempfile.TemporaryDirectory() as directory:
            frame, audit = _decode(bodies["official_zip"], Path(directory) / "shape")
            output = _parquet(frame)
    except Exception as exc:
        captures = {name: (bodies[name], receipts[name]) for name in receipts}
        quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
        raise
    identity = processing_identity(Path(__file__).parents[3])
    receipt_hashes = {name: receipt["sha256"] for name, receipt in receipts.items()}
    evidence_hash = _sha(_canonical({"receipt_hashes": receipt_hashes, "archive_sha256": EXPECTED_ZIP_SHA256}))
    audit.update(service)
    audit.update({
        "source_capture_evidence_sha256": evidence_hash,
        "source_receipt": receipts["official_zip"],
        "source_archive_bytes": len(bodies["official_zip"]),
        "source_archive_sha256": _sha(bodies["official_zip"]),
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "baseline_accepted": False,
    })
    candidate_version = "v" + _sha(_canonical({
        "archive_sha256": EXPECTED_ZIP_SHA256, "record_sha256": audit["source_record_sha256"],
        "geometry_sha256": audit["source_geometry_sha256"], "schema_sha256": audit["source_schema_sha256"],
        "processing_identity": identity,
    }))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, candidate_version)
    parquet_path = f"geoparquet/{FILE_SLUG}.parquet"
    input_record = {
        "input_id": "nces_edge_private_school_locations_2023_24", "producer": "National Center for Education Statistics (NCES EDGE)",
        "host": urlsplit(SOURCE_URL).hostname, "role": "producer_publication", "canonical_url": SOURCE_URL,
        "retrieval_url": SOURCE_URL, "observed_final_url": receipts["official_zip"]["final_url"],
        "retrieved_at": receipts["official_zip"]["observed_at"], "http_status": receipts["official_zip"]["status"],
        "bytes": receipts["official_zip"]["bytes"], "sha256": receipts["official_zip"]["sha256"],
        "wire_bytes": receipts["official_zip"]["wire_bytes"], "wire_sha256": receipts["official_zip"]["wire_sha256"],
        "release_date": None, "release_date_unknown_reason": "2023-2024 is the PSS collection period. NCES/Data.gov publication and update timestamps are catalog/transport observations, not independently identified producer release dates.",
        "license": "NCES source describes all file information as public domain; approval remains pending.",
        "attribution": "National Center for Education Statistics (NCES), Education Demographic and Geographic Estimates (EDGE); review pending",
    }
    source_manifest = {
        "schema_version": 1, "approved": False, "title": "Private School Locations (NCES EDGE, 2023-24)",
        "description": "Pinned official NCES EDGE shapefile package for the 2023-2024 Private School Universe Survey collection; changed-successor candidate, not a current HIFLD compatibility assertion.",
        "publisher": "National Center for Education Statistics (NCES)", "agency": "U.S. Department of Education",
        "source_url": SOURCE_URL, "license": None, "source_modified": None, "date_issued": None,
        "date_modified": SOURCE_RELEASE, "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"],
        "inputs": [input_record],
        "transformations": [{"operation": "NCES EDGE shapefile attributes and EPSG:4326 points to GeoParquet",
                              "parameters": {"source_fields": "all native DBF fields preserved in original order and types",
                                             "source_ids": "PPIN retained unchanged as an 8-character string; no synthetic identifier",
                                             "geometry": "native WGS84 XY preserved without reprojection",
                                             "source_period": SOURCE_RELEASE,
                                             "candidate_status": "historical survey cohort; compatibility, freshness and release unapproved"}}],
        "lineage": "NCES EDGE geocodes represent the 2023-2024 NCES Private School Survey school collection. The HIFLD inventory entry describes the 2021-2022 PSS cohort and attributes publishing to ORNL. A direct NCES 2021-2022 archive comparison found population and schema changes; the DataLumos HIFLD archive was not used as a substitute or approved baseline.",
        "related_official_service": {"url": SERVICE_URL, "layer_url": LAYER_URL, "service_layer_metadata_sha256": service["service_metadata_sha256"],
                                     "assessment": service["service_source_schema_diff"]},
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Preserves the complete source DBF schema, PPIN values and native point coordinates.",
                          "objects": [{"path": parquet_path, "bytes": len(output), "sha256": _sha(output)}]}}},
        "unsupported_output_formats": {"geopackage": "No approved field mapping or compatibility contract.",
                                        "pmtiles": "No approved display transformation or generalization."},
        "promotion_allowed": False, "source_accepted": False, "baseline": None,
    }
    validate_release_provenance(source_manifest)
    validate_output_policy(source_manifest)
    raw_outputs = {}
    for name, body in bodies.items():
        raw_outputs[f"raw/capture/{name}.body"] = body
        raw_outputs[f"raw/capture/{name}.wire"] = (root / f"{name}.wire").read_bytes()
        raw_outputs[f"raw/capture/{name}.receipt.json"] = _canonical(receipts[name]) + b"\n"
    outputs = {
        parquet_path: output,
        "metadata/source_manifest.json": _canonical(source_manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical({"approved": False, "version": candidate_version,
            "objects": {parquet_path: {"bytes": len(output), "sha256": _sha(output)}},
            "source_population_complete": True, "source_release_accepted": False, "freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
            "coverage_accepted": False, "release_accepted": False}) + b"\n",
        **raw_outputs,
    }
    state_key = f"_construction-state/{DATASET_SLUG}/{candidate_version}.json"
    fingerprint = _sha(_canonical({"identity": identity, "archive_sha256": EXPECTED_ZIP_SHA256,
                                  "record_sha256": audit["source_record_sha256"], "schema_sha256": audit["source_schema_sha256"]}))
    if storage.object_snapshot(state_key) is not None:
        original = json.loads(load_first_construction(storage, state_key, candidate_version, fingerprint)).get("payload")
        if not isinstance(original, dict) or not isinstance(original.get("objects"), dict):
            raise ValueError("cached_candidate_state_invalid")
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, candidate_version, fingerprint,
                                identity, audit["source_record_sha256"], audit["source_schema_sha256"],
                                original["objects"], original.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": candidate_version,
                "feature_count": EXPECTED_COUNT, "source_population_complete": True,
                "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_capture_evidence_sha256": evidence_hash, "capture_directory": str(root), "release_accepted": False}
    descriptors = {}
    for name, body in outputs.items():
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        snapshot = storage.object_snapshot(key)
        if snapshot is None or snapshot.size != len(body) or storage.sha256_key(key) != _sha(body):
            raise ValueError("candidate_object_readback_failed:" + name)
        descriptors[name] = {"bytes": len(body), "sha256": _sha(body)}
    seal_ready_candidate(storage, state_key, candidate_version, fingerprint, {
        "processing_identity": identity, "source_record_sha256": audit["source_record_sha256"],
        "source_schema_sha256": audit["source_schema_sha256"],
        "capture_receipts": {name: receipt for name, receipt in receipts.items()}, "objects": descriptors,
    })
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity
    )
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": candidate_version,
            "feature_count": EXPECTED_COUNT, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
            "source_record_sha256": audit["source_record_sha256"], "source_geometry_sha256": audit["source_geometry_sha256"],
            "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_capture_evidence_sha256": evidence_hash, "capture_directory": str(root),
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "release_accepted": False}

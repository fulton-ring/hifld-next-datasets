"""Candidate acquisition of the newest published NCES EDGE private schools."""
from __future__ import annotations

import io
import json
import shutil
import struct
import tempfile
import zipfile
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
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.capture_common import (
    candidate_prefix,
    capture_get,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha
from dagster_hifld.sources.nces_edge_common import (
    LANDING,
    NcesEdgeReleaseError,
    dbf_record_count,
    discover_release,
    release_from_url,
    shapefile_index_count,
    shapefile_record_count,
)

DATASET_SLUG = FILE_SLUG = "private-schools"
PRODUCT = "PRIVATESCH"
PUBLISHER = "National Center for Education Statistics (NCES), U.S. Department of Education"
EXPECTED_FIELDS = [
    ("PPIN", "C", 8, 0), ("NAME", "C", 50, 0), ("STREET", "C", 100, 0),
    ("CITY", "C", 30, 0), ("STATE", "C", 2, 0), ("ZIP", "C", 5, 0),
    ("STFIP", "C", 2, 0), ("CNTY", "C", 3, 0), ("NAMELSAD", "C", 100, 0),
    ("LOCALE", "C", 2, 0), ("LAT", "F", 19, 11), ("LON", "F", 19, 11),
    ("CBSA", "C", 5, 0), ("NMCBSA", "C", 100, 0), ("CBSATYPE", "C", 1, 0),
    ("CSA", "C", 3, 0), ("NMCSA", "C", 100, 0), ("CD", "C", 4, 0),
    ("SLDL", "C", 5, 0), ("SLDU", "C", 5, 0), ("SCHOOLYEAR", "C", 10, 0),
]
WGS84 = CRS.from_epsg(4326)
MAX_SOURCE_BYTES = 15_000_000
MAX_LANDING_BYTES = 2_000_000
MAX_UNCOMPRESSED_BYTES = 40_000_000
REPLAY_FILES = {"source.body", "source.wire", "source.receipt.json"}


def _members(stem: str) -> set[str]:
    return {
        f"{stem}.shx", f"{stem}.cpg", f"{stem}.dbf", f"{stem}.prj",
        f"{stem}.sbn", f"{stem}.sbx", f"{stem}.shp", f"{stem}.shp.xml",
        f"edge_geocode_{PRODUCT.lower()}_{stem.rsplit('_', 1)[-1]}.sas7bdat",
        f"{stem}.xlsx",
    }


def _field_schema(dbf: bytes) -> list[tuple[str, str, int, int]]:
    header_len = int.from_bytes(dbf[8:10], "little")
    if header_len < 33 or header_len > len(dbf):
        raise ValueError("nces_private_schools_dbf_header_invalid")
    fields = []
    offset = 32
    while offset < header_len and dbf[offset] != 0x0D:
        descriptor = dbf[offset:offset + 32]
        if len(descriptor) != 32:
            raise ValueError("nces_private_schools_dbf_descriptor_incomplete")
        fields.append((descriptor[:11].split(b"\0", 1)[0].decode("ascii"),
                       chr(descriptor[11]), descriptor[16], descriptor[17]))
        offset += 32
    if offset >= header_len or dbf[offset] != 0x0D:
        raise ValueError("nces_private_schools_dbf_terminator_missing")
    return fields


def _decode(body: bytes, tmp: Path, release: dict[str, object]) -> tuple[gpd.GeoDataFrame, dict[str, object]]:
    stem = str(release["stem"])
    members = _members(stem)
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        infos = archive.infolist()
        names = [info.filename for info in infos]
        for info in infos:
            mode = info.external_attr >> 16
            parts = Path(info.filename).parts
            if (not info.filename or info.filename.startswith(("/", "\\")) or "\\" in info.filename
                    or ".." in parts or len(parts) != 1 or info.is_dir() or (mode & 0o170000) == 0o120000):
                raise ValueError("nces_private_schools_zip_member_path_rejected")
        if len(set(names)) != len(names) or set(names) != members:
            raise ValueError("nces_private_schools_zip_member_roster_drift")
        if sum(info.file_size for info in infos) > MAX_UNCOMPRESSED_BYTES:
            raise ValueError("nces_private_schools_zip_uncompressed_size_drift")
        if archive.testzip() is not None:
            raise ValueError("nces_private_schools_zip_crc_failure")
        tmp.mkdir(parents=True, exist_ok=True)
        for name in members:
            if name.startswith(stem + "."):
                with archive.open(name) as src, (tmp / name).open("xb") as dst:
                    shutil.copyfileobj(src, dst)
        dbf = archive.read(f"{stem}.dbf")
        shx = archive.read(f"{stem}.shx")
        shp = archive.read(f"{stem}.shp")
    fields = _field_schema(dbf)
    if fields != EXPECTED_FIELDS:
        raise ValueError("nces_private_schools_dbf_schema_drift")
    dbf_count = dbf_record_count(dbf, "nces_private_schools")
    shx_count = shapefile_index_count(shx, "nces_private_schools")
    shp_count = shapefile_record_count(shp, "nces_private_schools")
    if not dbf_count == shx_count == shp_count:
        raise ValueError("nces_private_schools_source_record_count_disagreement")
    shp_path = tmp / f"{stem}.shp"
    import pyogrio
    info = pyogrio.read_info(shp_path)
    names = [field[0] for field in EXPECTED_FIELDS]
    if (info["layer_name"] != stem or info["geometry_type"] != "Point"
            or info["crs"] != "EPSG:4326" or int(info["features"]) != dbf_count
            or list(info["fields"]) != names):
        raise ValueError("nces_private_schools_archive_layer_metadata_drift")
    frame = gpd.read_file(shp_path, engine="pyogrio")
    if (list(frame.columns) != names + ["geometry"] or len(frame) != dbf_count
            or frame.isna().any(axis=None) or frame.geometry.is_empty.any()
            or frame.geometry.geom_type.ne("Point").any()):
        raise ValueError("nces_private_schools_population_or_schema_drift")
    if not CRS.from_user_input(frame.crs).equals(WGS84, ignore_axis_order=True):
        raise ValueError("nces_private_schools_archive_crs_drift")
    if frame["PPIN"].duplicated().any() or not frame["PPIN"].astype(str).str.fullmatch(r"[A-Z0-9]{8}").all():
        raise ValueError("nces_private_schools_native_id_drift")
    schoolyear = str(release["schoolyear"])
    if set(frame["SCHOOLYEAR"].unique()) != {schoolyear}:
        raise ValueError("nces_private_schools_period_drift")
    if (abs(frame.geometry.x.to_numpy() - frame["LON"].to_numpy()) > 1e-12).any():
        raise ValueError("nces_private_schools_longitude_attribute_drift")
    if (abs(frame.geometry.y.to_numpy() - frame["LAT"].to_numpy()) > 1e-12).any():
        raise ValueError("nces_private_schools_latitude_attribute_drift")
    bounds = list(map(float, frame.total_bounds))
    attrs = [[value for value in row] for row in frame[names].itertuples(index=False, name=None)]
    ids = sorted(frame["PPIN"].astype(str).tolist())
    xy_x = frame.geometry.x.to_numpy()
    xy_y = frame.geometry.y.to_numpy()
    ppin_values = frame["PPIN"].astype(str).to_numpy()
    geometry = sorted([[str(ppin_values[i]), float(xy_x[i]), float(xy_y[i])]
                       for i in range(len(frame))], key=lambda row: row[0])
    id_sha, record_sha, geometry_sha = (_sha(_canonical(value)) for value in (ids, attrs, geometry))
    audit = {
        "source_feature_count": len(frame), "source_population_complete": True,
        "source_id_field": "PPIN", "source_id_unique": True,
        "source_id_min": min(ids), "source_id_max": max(ids), "source_id_sha256": id_sha,
        "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
        "source_schema_sha256": _sha(_canonical(fields)), "source_dbf_schema": fields,
        "source_crs": "EPSG:4326", "source_geometry_type": "Point", "source_bounds": bounds,
        "source_period": schoolyear, "schoolyear": schoolyear,
        "publication_selection": release,
        "source_counts": {"dbf": dbf_count, "shx": shx_count, "shp": shp_count, "decoded": len(frame)},
        "source_null_counts": {name: int(frame[name].isna().sum()) for name in names},
        "source_geometry_null_count": int(frame.geometry.isna().sum()),
        "source_geometry_empty_count": int(frame.geometry.is_empty.sum()),
        "source_coverage_scope": (
            f"NCES EDGE private-school point file for the {schoolyear} PSS school collection; "
            "nationwide U.S. source bounds, including Alaska and Hawaii. This is a changed "
            "successor candidate, not an accepted HIFLD compatibility claim."
        ),
        "source_drift_guard": (
            "Official ZIP member roster, all DBF field names/types/widths, publisher-written "
            "DBF/SHX/SHP record counts, all PPIN identifiers, all attributes, native EPSG:4326 "
            "point XY, and source survey year."
        ),
    }
    return frame, audit


def _point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, float(x), float(y))


def _parquet(frame: gpd.GeoDataFrame, expected_count: int) -> bytes:
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
    if check.num_rows != expected_count:
        raise ValueError("nces_private_schools_candidate_row_count_readback_drift")
    for name, _kind, _width, _decimals in EXPECTED_FIELDS:
        expected = [str(value) if isinstance(value, str) else float(value) for value in frame[name].tolist()]
        if check[name].to_pylist() != expected:
            raise ValueError("nces_private_schools_candidate_attribute_readback_drift:" + name)
    if check["geometry"].to_pylist() != geometries:
        raise ValueError("nces_private_schools_candidate_geometry_readback_drift")
    return output.getvalue()


def _validate_receipt(receipt: dict[str, object], body: bytes, wire: bytes,
                      release: dict[str, object]) -> None:
    url = release["source_url"]
    if (receipt.get("method") != "GET" or receipt.get("url") != url
            or receipt.get("final_url") != url or receipt.get("status") != 200
            or receipt.get("complete_body") is not True
            or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
            or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
            or receipt.get("content_encoding") != "identity"):
        raise ValueError("nces_private_schools_capture_receipt_mismatch")


def _quarantine(storage: StagingStorageResource, root: Path, artifacts: dict[str, bytes],
                receipt: dict[str, object], version: str, reason: str) -> None:
    try:
        quarantine_incomplete_capture(storage, root, DATASET_SLUG, version,
                                      {"source": (artifacts["source.body"], receipt)}, reason)
    except Exception:
        pass


def _capture(root: Path, storage: StagingStorageResource,
             version: str) -> tuple[dict[str, bytes], dict[str, dict[str, object]], dict[str, object]]:
    if root.exists():
        if root.is_symlink() or {path.name for path in root.iterdir()} != REPLAY_FILES:
            raise ValueError("nces_private_schools_capture_roster_mismatch")
        paths = {name: root / name for name in REPLAY_FILES}
        if any(path.is_symlink() or not path.is_file() for path in paths.values()):
            raise ValueError("nces_private_schools_capture_nonregular_file")
        artifacts = {name: path.read_bytes() for name, path in paths.items()}
        raw_receipt = json.loads(artifacts["source.receipt.json"])
        url = raw_receipt.get("url") or raw_receipt.get("requested_url")
        try:
            release = release_from_url(str(url), PRODUCT)
        except NcesEdgeReleaseError as exc:
            raise ValueError("nces_private_schools_" + str(exc)) from exc
        _validate_receipt(raw_receipt, artifacts["source.body"], artifacts["source.wire"], release)
        return artifacts, {"source": raw_receipt}, release
    if root.is_symlink() or any(path.is_symlink() for path in root.parents):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True, exist_ok=False)
    artifacts: dict[str, bytes] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(90, connect=20),
                          headers={"User-Agent": "hifld-next-datasets/private-schools-draft"}) as client:
            landing, _ = capture_get(client, LANDING, root, "landing", max_bytes=MAX_LANDING_BYTES)
            try:
                release = discover_release(landing, PRODUCT)
            except NcesEdgeReleaseError as exc:
                raise ValueError("nces_private_schools_" + str(exc)) from exc
            _body, receipt = capture_get(client, str(release["source_url"]), root, "source", max_bytes=MAX_SOURCE_BYTES)
            landing_after, _ = capture_get(client, LANDING, root, "landing_after", max_bytes=MAX_LANDING_BYTES)
            try:
                if discover_release(landing_after, PRODUCT) != release:
                    raise ValueError("nces_private_schools_published_release_changed_during_capture")
            except NcesEdgeReleaseError as exc:
                raise ValueError("nces_private_schools_" + str(exc)) from exc
        artifacts = {name: (root / name).read_bytes() for name in REPLAY_FILES}
        for name in ("landing", "landing_after"):
            if (root / f"{name}.body").is_file():
                artifacts[f"{name}.body"] = (root / f"{name}.body").read_bytes()
                artifacts[f"{name}.wire"] = (root / f"{name}.wire").read_bytes()
                artifacts[f"{name}.receipt.json"] = (root / f"{name}.receipt.json").read_bytes()
        _validate_receipt(receipt, artifacts["source.body"], artifacts["source.wire"], release)
        return artifacts, {"source": receipt}, release
    except Exception as exc:
        if (root.is_dir() and not root.is_symlink()
                and all((root / name).is_file() and not (root / name).is_symlink()
                        for name in REPLAY_FILES)):
            try:
                artifacts = {name: (root / name).read_bytes() for name in REPLAY_FILES}
                receipt_raw = json.loads(artifacts["source.receipt.json"])
                _quarantine(storage, root, artifacts, receipt_raw, version, type(exc).__name__)
            except Exception:
                pass
        if isinstance(exc, ValueError):
            raise
        raise ValueError("nces_private_schools_capture_invalid") from exc


def capture_private_schools(storage: StagingStorageResource, *, version: str,
                            capture_directory: str | None = None) -> dict[str, object]:
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
    root = requested or default_capture_dir("hifld-native-reconstruction", "278", "captures") / (version + "-" + uuid4().hex)
    artifacts, receipts, release = _capture(root, storage, version)
    try:
        with tempfile.TemporaryDirectory() as directory:
            frame, audit = _decode(artifacts["source.body"], Path(directory) / "shape", release)
            output = _parquet(frame, audit["source_feature_count"])
    except Exception as exc:
        _quarantine(storage, root, artifacts, receipts["source"], version, type(exc).__name__)
        raise
    schoolyear = str(release["schoolyear"])
    source_url = str(release["source_url"])
    identity = processing_identity(Path(__file__).parents[3])
    capture_evidence = {
        "schema_version": 1,
        "complete": True,
        "source_url": source_url,
        "schoolyear": schoolyear,
        "publication_selection": {
            "schoolyear": schoolyear, "source_url": source_url, "stem": release["stem"],
        },
        "receipt": receipts["source"],
        "raw_artifacts": {
            name: {"bytes": len(artifacts[name]), "sha256": _sha(artifacts[name])}
            for name in sorted(REPLAY_FILES)
        },
    }
    capture_sha = _sha(_canonical(capture_evidence))
    audit.update({
        "source_capture_sha256": capture_sha,
        "source_capture_evidence_sha256": _sha(_canonical({
            "receipt_hashes": {"source": receipts["source"]["sha256"]},
            "archive_sha256": _sha(artifacts["source.body"]),
        })),
        "source_receipt": receipts["source"],
        "source_archive_bytes": len(artifacts["source.body"]),
        "source_archive_sha256": _sha(artifacts["source.body"]),
        "schoolyear": schoolyear,
        "publication_selection": release,
        "source_release_accepted": False, "source_freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False, "baseline_accepted": False,
    })
    candidate_version = "v" + _sha(_canonical({
        "archive_sha256": _sha(artifacts["source.body"]),
        "record_sha256": audit["source_record_sha256"],
        "geometry_sha256": audit["source_geometry_sha256"],
        "schema_sha256": audit["source_schema_sha256"],
        "capture_sha256": capture_sha,
        "processing_identity": identity,
    }))[:32]
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, candidate_version)
    parquet_path = f"geoparquet/{FILE_SLUG}.parquet"
    year_slug = schoolyear.replace("-", "_")
    input_record = {
        "input_id": f"nces_edge_private_school_locations_{year_slug}",
        "producer": PUBLISHER,
        "host": urlsplit(source_url).hostname, "role": "producer_publication", "canonical_url": source_url,
        "retrieval_url": source_url, "observed_final_url": receipts["source"]["final_url"],
        "retrieved_at": receipts["source"]["observed_at"], "http_status": receipts["source"]["status"],
        "bytes": receipts["source"]["bytes"], "sha256": receipts["source"]["sha256"],
        "wire_bytes": receipts["source"]["wire_bytes"], "wire_sha256": receipts["source"]["wire_sha256"],
        "release_date": None,
        "release_date_unknown_reason": (
            f"{schoolyear} is the PSS collection period. NCES/Data.gov publication and update "
            "timestamps are catalog/transport observations, not independently identified producer "
            "release dates."
        ),
        "license": "NCES source describes all file information as public domain; approval remains pending.",
        "attribution": "National Center for Education Statistics (NCES), Education Demographic and Geographic Estimates (EDGE); review pending",
    }
    source_manifest = {
        "schema_version": 1, "approved": False,
        "title": f"Private School Locations (NCES EDGE, {schoolyear})",
        "description": (
            f"Official NCES EDGE shapefile package for the {schoolyear} Private School Universe "
            "Survey collection, selected from the newest release named by the NCES SchoolLocations "
            "page; changed-successor candidate, not a current HIFLD compatibility assertion."
        ),
        "publisher": PUBLISHER, "agency": "U.S. Department of Education",
        "source_url": source_url, "license": None, "source_modified": None, "date_issued": None,
        "date_modified": schoolyear, "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "code_identity_scheme": identity["code_identity_scheme"],
        "inputs": [input_record],
        "transformations": [{"operation": "NCES EDGE shapefile attributes and EPSG:4326 points to GeoParquet",
                              "parameters": {"source_fields": "all native DBF fields preserved in original order and types",
                                             "source_ids": "PPIN retained unchanged as an 8-character string; no synthetic identifier",
                                             "geometry": "native WGS84 XY preserved without reprojection",
                                             "source_period": schoolyear,
                                             "completeness": (
                                                 f"All {audit['source_feature_count']:,} rows validated against "
                                                 "publisher-written DBF, SHX and SHP counts."
                                             ),
                                             "candidate_status": "latest published survey cohort; compatibility, freshness and release unapproved"}}],
        "lineage": (
            "NCES EDGE geocodes represent the NCES Private School Survey school collection. "
            f"This candidate is the newest private-school release named by the official NCES "
            f"SchoolLocations page at capture time ({schoolyear}). Historical HIFLD equivalence "
            "and a direct crosswalk to the archived cohort remain unverified."
        ),
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Preserves the complete source DBF schema, PPIN values and native point coordinates.",
                          "objects": [{"path": parquet_path, "bytes": len(output), "sha256": _sha(output)}]}}},
        "unsupported_output_formats": {"geopackage": "No approved field mapping or compatibility contract.",
                                        "pmtiles": "No approved display transformation or generalization."},
        "promotion_allowed": False, "source_accepted": False, "baseline": None,
    }
    validate_release_provenance(source_manifest)
    validate_output_policy(source_manifest)
    raw_outputs = {}
    for name in sorted(REPLAY_FILES):
        raw_outputs[f"raw/source.{name.split('.', 1)[1]}"] = artifacts[name]
    for name in ("landing", "landing_after"):
        if f"{name}.body" in artifacts:
            raw_outputs[f"raw/{name}.body"] = artifacts[f"{name}.body"]
            raw_outputs[f"raw/{name}.wire"] = artifacts[f"{name}.wire"]
            raw_outputs[f"raw/{name}.receipt.json"] = _canonical(json.loads(artifacts[f"{name}.receipt.json"])) + b"\n"
    outputs = {
        parquet_path: output,
        "metadata/source_manifest.json": _canonical(source_manifest) + b"\n",
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/candidate_ready.json": _canonical({"approved": False, "version": candidate_version,
            "objects": {parquet_path: {"bytes": len(output), "sha256": _sha(output)}},
            "feature_count": audit["source_feature_count"], "schoolyear": schoolyear,
            "source_url": source_url,
            "source_population_complete": True, "source_release_accepted": False, "freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
            "coverage_accepted": False, "release_accepted": False}) + b"\n",
        **raw_outputs,
    }
    state_key = f"_construction-state/{DATASET_SLUG}/{candidate_version}.json"
    fingerprint = _sha(_canonical({
        "identity": identity, "archive_sha256": _sha(artifacts["source.body"]),
        "record_sha256": audit["source_record_sha256"], "schema_sha256": audit["source_schema_sha256"],
        "capture_sha256": capture_sha,
    }))
    if storage.object_snapshot(state_key) is not None:
        original = json.loads(load_first_construction(storage, state_key, candidate_version, fingerprint)).get("payload")
        if (not isinstance(original, dict) or not isinstance(original.get("objects"), dict)
                or original.get("capture_receipts") != receipts):
            raise ValueError("cached_candidate_state_invalid")
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, candidate_version, fingerprint,
                                identity, audit["source_record_sha256"], audit["source_schema_sha256"],
                                original["objects"], receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": candidate_version,
                "feature_count": audit["source_feature_count"], "schoolyear": schoolyear,
                "source_url": source_url, "source_population_complete": True,
                "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_capture_evidence_sha256": audit["source_capture_evidence_sha256"],
                "capture_directory": str(root), "release_accepted": False}
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
        "capture_receipts": receipts, "objects": descriptors,
    })
    validate_construction_code_identity(
        Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity
    )
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": candidate_version,
            "feature_count": audit["source_feature_count"], "schoolyear": schoolyear,
            "source_url": source_url, "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
            "source_record_sha256": audit["source_record_sha256"], "source_geometry_sha256": audit["source_geometry_sha256"],
            "source_coverage_scope": audit["source_coverage_scope"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_capture_evidence_sha256": audit["source_capture_evidence_sha256"],
            "capture_directory": str(root),
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "coverage_accepted": False, "rights_accepted": False, "release_accepted": False}

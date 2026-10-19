"""Faithful candidate extractor for EIA's January 2020 natural gas pipeline ZIP.

The source is a native EIA shapefile archive. Its HTTP modification date is
2020-04-27 and the embedded layer name identifies a January 2020 edition. The
candidate remains unapproved; this adapter does not treat the static archive
as a current refresh feed.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import zipfile
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import fiona
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix,
    capture_get,
    content_derived_version,
    load_first_construction,
    processing_identity,
    quarantine_incomplete_capture,
    seal_ready_candidate,
    verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

DATASET_SLUG = "natural-gas-interstate-and-intrastate-pipelines"
FILE_SLUG = DATASET_SLUG
SOURCE_URL = "https://www.eia.gov/maps/map_data/NaturalGas_InterIntrastate_Pipelines_US_EIA.zip"
LAYER_NAME = "NaturalGas_Pipelines_US_202001"
SOURCE_UPDATE_CLAIM = "Updated January 2020"
PACKAGE_PUBLICATION_DATE = "2020-04-28"
EXPECTED_FEATURES = 32961
EXPECTED_FIELDS = ("TYPEPIPE", "Operator", "Status", "Shape_Leng")
EXPECTED_MEMBERS = {
    f"{LAYER_NAME}.cpg", f"{LAYER_NAME}.dbf", f"{LAYER_NAME}.prj",
    f"{LAYER_NAME}.sbn", f"{LAYER_NAME}.sbx", f"{LAYER_NAME}.shp",
    f"{LAYER_NAME}.shp.xml", f"{LAYER_NAME}.shx",
}
MAX_ARCHIVE_BYTES = 32_000_000
MAX_UNCOMPRESSED_BYTES = 64_000_000
CAPTURE_FILES = {"eia_source_zip.body", "eia_source_zip.wire", "eia_source_zip.receipt.json"}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _load_retained_capture(directory: Path) -> tuple[bytes, bytes, dict[str, object]]:
    """Load a sealed acquisition without changing its original receipt/time."""
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("retained_capture_directory_missing_or_unsafe")
    entries = list(directory.iterdir())
    if {p.name for p in entries} != CAPTURE_FILES or any(p.is_symlink() or not p.is_file() for p in entries):
        raise ValueError("retained_capture_roster_drift")
    body = (directory / "eia_source_zip.body").read_bytes()
    wire = (directory / "eia_source_zip.wire").read_bytes()
    receipt_bytes = (directory / "eia_source_zip.receipt.json").read_bytes()
    receipt = json.loads(receipt_bytes)
    keys = {"method", "url", "final_url", "observed_at", "status", "complete_body",
            "wire_bytes", "wire_sha256", "bytes", "sha256", "content_encoding",
            "content_length", "error_code"}
    if (not isinstance(receipt, dict) or set(receipt) != keys
            or receipt_bytes != _canonical(receipt) + b"\n"
            or receipt["method"] != "GET" or receipt["url"] != SOURCE_URL
            or receipt["final_url"] != SOURCE_URL or receipt["status"] != 200
            or receipt["complete_body"] is not True or receipt["error_code"] is not None
            or receipt["bytes"] != len(body) or receipt["sha256"] != _sha(body)
            or receipt["wire_bytes"] != len(wire) or receipt["wire_sha256"] != _sha(wire)):
        raise ValueError("retained_capture_receipt_integrity_failed")
    return body, wire, receipt


def _sealed_receipt_for_same_capture(old: Mapping[str, object], fresh: Mapping[str, object],
                                     body: bytes, wire: bytes) -> dict[str, object]:
    """Keep the original acquisition record after a verified identical recapture."""
    if (old.get("url") != SOURCE_URL or fresh.get("url") != SOURCE_URL
            or old.get("final_url") != fresh.get("final_url")
            or old.get("status") != 200 or fresh.get("status") != 200
            or old.get("complete_body") is not True or fresh.get("complete_body") is not True
            or old.get("bytes") != len(body) or fresh.get("bytes") != len(body)
            or old.get("sha256") != _sha(body) or fresh.get("sha256") != _sha(body)
            or old.get("wire_bytes") != len(wire) or fresh.get("wire_bytes") != len(wire)
            or old.get("wire_sha256") != _sha(wire) or fresh.get("wire_sha256") != _sha(wire)):
        raise ValueError("fresh_capture_differs_from_sealed_source")
    return dict(old)


def _validate_archive(path: Path) -> dict[str, object]:
    with zipfile.ZipFile(path) as archive:
        infos = archive.infolist()
        names = [item.filename for item in infos]
        if len(names) != len(set(names)) or set(names) != EXPECTED_MEMBERS:
            raise ValueError("source_archive_member_roster_drift")
        if sum(item.file_size for item in infos) > MAX_UNCOMPRESSED_BYTES:
            raise ValueError("source_archive_uncompressed_size_limit")
        if any(item.file_size > MAX_UNCOMPRESSED_BYTES for item in infos):
            raise ValueError("source_archive_member_size_limit")
        for item in infos:
            if item.filename.startswith(("/", "\\")) or ".." in Path(item.filename).parts:
                raise ValueError("unsafe_source_archive_member")
        layer_path = f"/vsizip/{path}/{LAYER_NAME}.shp"
        info = pyogrio.read_info(layer_path)
        with fiona.open(layer_path) as collection:
            dbf_schema = dict(collection.schema["properties"])
        fields = list(info["fields"])
        if fields != list(EXPECTED_FIELDS):
            raise ValueError("source_field_roster_drift")
        if (info["layer_name"] != LAYER_NAME or info["driver"] != "ESRI Shapefile"
                or info["geometry_type"] not in {"LineString", "Unknown"} or info["features"] != EXPECTED_FEATURES
                or pyproj.CRS.from_user_input(info["crs"]).to_epsg() != 4326):
            raise ValueError("source_archive_layer_profile_drift")
        return {
            "layer_name": info["layer_name"], "driver": info["driver"],
            "geometry_type": info["geometry_type"], "feature_count": info["features"],
            "fields": fields, "dtypes": [str(x) for x in info["dtypes"]],
            "ogr_types": [str(x) for x in info["ogr_types"]],
            "dbf_schema": dbf_schema,
            "crs_epsg": pyproj.CRS.from_user_input(info["crs"]).to_epsg(),
            "total_bounds": [float(x) for x in info["total_bounds"]],
            "dbf_date_last_update": info.get("layer_metadata", {}).get("DBF_DATE_LAST_UPDATE"),
            "archive_members": [{"name": i.filename, "bytes": i.file_size, "crc32": i.CRC}
                                for i in infos],
        }


def _table_from_frame(frame) -> tuple[pa.Table, dict[str, object]]:
    if list(frame.columns[:-1]) != list(EXPECTED_FIELDS):
        raise ValueError("source_table_roster_drift")
    geometries = list(frame.geometry)
    geometry_types = sorted({g.geom_type for g in geometries if g is not None})
    if not set(geometry_types) <= {"LineString", "MultiLineString"}:
        raise ValueError("source_geometry_type_drift")
    schema = pa.schema([
        pa.field("TYPEPIPE", pa.string(), nullable=True),
        pa.field("Operator", pa.string(), nullable=True),
        pa.field("Status", pa.string(), nullable=True),
        pa.field("Shape_Leng", pa.float64(), nullable=True),
        pa.field("geometry", pa.binary(), nullable=True),
    ])
    columns = {
        "TYPEPIPE": pa.array(frame["TYPEPIPE"].tolist(), type=pa.string()),
        "Operator": pa.array(frame["Operator"].tolist(), type=pa.string()),
        "Status": pa.array(frame["Status"].tolist(), type=pa.string()),
        "Shape_Leng": pa.array(frame["Shape_Leng"].tolist(), type=pa.float64()),
        "geometry": pa.array([shapely.to_wkb(g, output_dimension=2) if g is not None else None
                              for g in geometries], type=pa.binary()),
    }
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {
        "geometry": {"encoding": "WKB", "geometry_types": geometry_types,
                     "crs": pyproj.CRS.from_epsg(4326).to_json_dict()}
    }}
    table = pa.Table.from_pydict(columns, schema=schema.with_metadata({b"geo": _canonical(geo)}))
    digest = hashlib.sha256()
    geometry_digest = hashlib.sha256()
    null_geometry_count = empty_geometry_count = invalid_geometry_count = 0
    for i in range(len(frame)):
        row = {name: (None if frame.iloc[i][name] is None else frame.iloc[i][name])
               for name in EXPECTED_FIELDS}
        # Shapefile features have no declared business ID; preserve source order
        # and do not manufacture an ID column.
        digest.update(_canonical({"source_row": i, "attributes": row}))
        geometry = geometries[i]
        if geometry is None:
            null_geometry_count += 1
            digest.update(b"\x00NULL_GEOMETRY\x00")
            geometry_digest.update(b"\x00NULL_GEOMETRY\x00")
        else:
            if geometry.is_empty:
                empty_geometry_count += 1
            if not geometry.is_valid:
                invalid_geometry_count += 1
            geometry_wkb = shapely.to_wkb(geometry, output_dimension=2)
            digest.update(geometry_wkb)
            geometry_digest.update(geometry_wkb)
    audit = {
        "feature_count": len(frame), "source_fields": list(EXPECTED_FIELDS),
        "source_order_preserved": True, "source_id_field": None,
        "source_record_sha256": digest.hexdigest(),
        "source_geometry_sha256": geometry_digest.hexdigest(),
        "geometry_types": geometry_types, "crs_epsg": 4326,
        "null_geometry_count": null_geometry_count,
        "empty_geometry_count": empty_geometry_count,
        "invalid_geometry_count": invalid_geometry_count,
    }
    return table, audit


def _read_native(path: Path) -> tuple[pa.Table, dict[str, object]]:
    layer_path = f"/vsizip/{path}/{LAYER_NAME}.shp"
    frame = pyogrio.read_dataframe(layer_path, use_arrow=True)
    if len(frame) != EXPECTED_FEATURES:
        raise ValueError("source_feature_count_drift")
    return _table_from_frame(frame)


def _verify_reuse(storage: StagingStorageResource, prefix: str, state_key: str,
                  version: str, fingerprint: str, identity: Mapping[str, object],
                  audit: Mapping[str, object], expected_objects: Mapping[str, Mapping[str, object]],
                  receipts: Mapping[str, Mapping[str, object]]) -> dict[str, object]:
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                            fingerprint, identity, str(audit["source_record_sha256"]),
                            str(audit["source_schema_sha256"]), expected_objects, receipts)
    for name, descriptor in expected_objects.items():
        key = f"{prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if (snapshot is None or descriptor != {"bytes": snapshot.size, "sha256": storage.sha256_key(key)}
                or storage.object_snapshot(key) != snapshot):
            raise ValueError("cached_candidate_object_integrity_failed:" + name)
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": EXPECTED_FEATURES, "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
        "source_null_geometry_count": audit["null_geometry_count"],
        "source_empty_geometry_count": audit["empty_geometry_count"],
        "source_invalid_geometry_count": audit["invalid_geometry_count"],
        "reused": True, "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False,
        "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
    }


def capture_eia_natural_gas_pipelines(
    storage: StagingStorageResource, *, version: str,
    capture_directory: str | None = None,
) -> dict[str, object]:
    """Capture or replay a retained full official EIA ZIP as an unapproved candidate."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000:
        raise ValueError("disk_floor_before_acquisition")
    replay = Path(capture_directory).absolute() if capture_directory else None
    root = replay if replay else (Path("/tmp/hifld-native-reconstruction/171/captures") / f"{version}-{uuid4().hex}").absolute()
    if ".." in root.parts or any(parent.is_symlink() for parent in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    if not replay:
        root.mkdir(parents=True, exist_ok=False)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        if replay:
            body, wire, receipt = _load_retained_capture(root)
        else:
            with httpx.Client(follow_redirects=False, timeout=60,
                              headers={"User-Agent": "hifld-next-datasets/eia-natural-gas-pipelines-draft"}) as client:
                body, receipt = capture_get(client, SOURCE_URL, root, "eia_source_zip", max_bytes=MAX_ARCHIVE_BYTES)
            wire = (root / "eia_source_zip.wire").read_bytes()
        captures["eia_source_zip"] = (body, receipt)
        source_zip = Path(storage.local_dir).absolute() / f".eia-pipeline-{uuid4().hex}.zip"
        source_zip.write_bytes(body)
        archive_audit = _validate_archive(source_zip)
        table, audit = _read_native(source_zip)
        source_zip.unlink()
    except Exception as exc:
        if captures:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, version, captures, type(exc).__name__)
        raise

    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    parquet = sink.getvalue().to_pybytes()
    schema_sha = _sha(_canonical({"dbf_schema": archive_audit["dbf_schema"], "fields": [
        {"name": f.name, "type": str(f.type), "nullable": f.nullable}
        for f in table.schema if f.name != "geometry"
    ], "geometry_types": audit["geometry_types"], "crs_epsg": 4326}))
    audit.update({"source_schema_sha256": schema_sha, **archive_audit,
                  "source_zip_sha256": receipt["sha256"], "source_zip_bytes": receipt["bytes"],
                  "source_update_claim": SOURCE_UPDATE_CLAIM,
                  "package_publication_date": PACKAGE_PUBLICATION_DATE,
                  "freshness_accepted": False, "schema_contract_accepted": False,
                  "id_contract_accepted": False, "rights_accepted": False,
                  "mapping_approved": False, "release_accepted": False})
    identity = processing_identity(Path(__file__).parents[3])
    version = content_derived_version(_sha(_canonical({"records": audit["source_record_sha256"], "source_zip": receipt["sha256"]})),
                                      str(audit["source_geometry_sha256"]),
                                      schema_sha, identity)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    sealed_snapshot = storage.object_snapshot(state_key)
    if sealed_snapshot is not None:
        sealed_bundle = json.loads(load_first_construction(storage, state_key, version, _sha(_canonical(identity))))
        sealed_receipts = sealed_bundle.get("payload", {}).get("capture_receipts", {})
        sealed_receipt = sealed_receipts.get("eia_source_zip")
        if not isinstance(sealed_receipt, dict):
            raise ValueError("sealed_original_capture_receipt_missing")
        receipt = _sealed_receipt_for_same_capture(sealed_receipt, receipt, body, wire)
        captures["eia_source_zip"] = (body, receipt)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    zip_name = "raw/eia_source_zip.body"
    objects = {
        parquet_name: parquet,
        zip_name: body,
        "metadata/source_audit.json": _canonical(audit) + b"\n",
        "metadata/processing_identity.json": _canonical(identity) + b"\n",
        "metadata/source_manifest.json": _canonical({
            "schema_version": 1, "approved": False,
            "title": "Natural Gas Interstate and Intrastate Pipelines",
            "description": "EIA native pipeline shapefile archive with LineString/MultiLineString geometries, 40 null and one invalid geometry in the January 2020 edition. The archived edition is a candidate and has not been accepted as a current refresh source.",
            "publisher": "U.S. Energy Information Administration",
            "source_url": SOURCE_URL, "source_update_claim": SOURCE_UPDATE_CLAIM,
            "package_publication_date": PACKAGE_PUBLICATION_DATE,
            "source_modified": None, "source_modified_is_release_date": False,
            "processor": "HIFLD Next", "code_commit": identity["code_commit"],
            "lineage": "Embedded EIA shapefile metadata says the data were compiled by EIA from federal and state agencies and external sources including company web pages and industry press. The exact upstream records and any delegated source agreements remain unresolved.",
            "native_layer": LAYER_NAME, "source_id_field": None,
            "inputs": [{"input_id": name, "producer": "U.S. Energy Information Administration",
                        "host": urlsplit(rcpt["url"]).hostname, "role": "archive",
                        "canonical_url": SOURCE_URL, "retrieval_url": rcpt["url"],
                        "observed_final_url": rcpt["final_url"], "retrieved_at": rcpt["observed_at"],
                        "http_status": rcpt["status"], "bytes": rcpt["bytes"],
                        "sha256": rcpt["sha256"], "release_date": PACKAGE_PUBLICATION_DATE,
                        "release_date_note": "Embedded EIA XML metadata gives pubDate 2020-04-28 and description Updated January 2020. The release date does not establish currentness.",
                        "license": "https://www.eia.gov/about/copyrights_reuse.php",
                        "attribution": "Source: U.S. Energy Information Administration (April 28, 2020). EIA's reuse policy covers its own website products; rights in upstream third-party inputs remain pending review."}
                       for name, (_, rcpt) in captures.items()],
            "transformations": [{"operation": "native_shapefile_to_geoparquet_candidate",
                                 "parameters": {"attributes": "all four source DBF fields in source order",
                                                "geometry": "source LineString coordinate values, EPSG:4326, no reprojection or repair",
                                                "id": "source archive has no declared identifier; source row order retained without inventing an ID",
                                                "candidate_status": "unapproved; freshness, schema/ID, rights, mapping and release acceptance pending"}}],
            "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
                "reason": "Native source fields and geometry are retained; source edition and contracts are pending.",
                "objects": [{"path": parquet_name, "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
            "unsupported_output_formats": {"geopackage": "No approved source-to-public mapping.",
                                            "pmtiles": "No display transformation or generalized geometry approved."}
        }) + b"\n",
    }
    validate_release_provenance(json.loads(objects["metadata/source_manifest.json"]))
    validate_output_policy(json.loads(objects["metadata/source_manifest.json"]))
    descriptors = {name: {"bytes": len(payload), "sha256": _sha(payload)} for name, payload in objects.items()}
    ready = {"approved": False, "version": version, "objects": {
        parquet_name: descriptors[parquet_name]}, "source_record_sha256": audit["source_record_sha256"],
        "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False}
    outputs = {**objects, "metadata/candidate_ready.json": _canonical(ready) + b"\n",
               "raw/eia_source_zip.wire": wire,
               "raw/eia_source_zip.receipt.json": _canonical(receipt) + b"\n"}
    output_descriptors = {
        name: {"bytes": len(payload), "sha256": _sha(payload)}
        for name, payload in outputs.items()
    }
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        return _verify_reuse(storage, prefix, state_key, version, fingerprint, identity,
                             audit, output_descriptors, {"eia_source_zip": receipt})
    for name, payload in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", payload)
    for name, descriptor in {**descriptors, "metadata/candidate_ready.json": {
        "bytes": len(outputs["metadata/candidate_ready.json"]),
        "sha256": _sha(outputs["metadata/candidate_ready.json"])} ,
        "raw/eia_source_zip.wire": {"bytes": len(outputs["raw/eia_source_zip.wire"]),
                                     "sha256": _sha(outputs["raw/eia_source_zip.wire"])},
        "raw/eia_source_zip.receipt.json": {"bytes": len(outputs["raw/eia_source_zip.receipt.json"]),
                                            "sha256": _sha(outputs["raw/eia_source_zip.receipt.json"])}
    }.items():
        key = f"{prefix}/{name}"; snapshot = storage.object_snapshot(key)
        if snapshot is None or snapshot.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] or storage.object_snapshot(key) != snapshot:
            raise ValueError("candidate_output_physical_readback_failed:" + name)
    state_payload = {"processing_identity": identity, "source_record_sha256": audit["source_record_sha256"],
                     "source_schema_sha256": schema_sha, "capture_receipts": {"eia_source_zip": receipt},
                     "objects": output_descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, state_payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_FEATURES, "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
            "capture_directory": str(root),
            "source_null_geometry_count": audit["null_geometry_count"],
            "source_empty_geometry_count": audit["empty_geometry_count"],
            "source_invalid_geometry_count": audit["invalid_geometry_count"],
            "source_release_accepted": False,
            "freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False}

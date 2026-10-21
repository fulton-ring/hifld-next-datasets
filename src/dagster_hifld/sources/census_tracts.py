"""Candidate-only streaming extraction of the official 2024 Census Tract layer."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path, PurePosixPath

import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely

from dagster_hifld.provenance import validate_provenance
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import processing_identity as shared_processing_identity
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "census-tracts-2"
FILE_SLUG = DATASET_SLUG
LAYER = "Census_Tract"
GDB = "tlgdb_2024_a_us_substategeo.gdb"
SOURCE_URL = f"https://www2.census.gov/geo/tiger/TGRGDB24/{GDB}.zip"
SOURCE_SHA256 = "e2409cd4ddb1a9302b130096fbcf9925ca3a701f720b8991045602bcb99d6b35"
SOURCE_BYTES = 736_048_475
SOURCE_FEATURE_COUNT = 85_529
SOURCE_MEMBERS = 84
SOURCE_EXPANDED_BYTES = 1_050_730_729
SOURCE_CRS = "EPSG:4269"
SOURCE_GEOMETRY = "MultiPolygon"
RAW_KEY = f"_raw-captures/{DATASET_SLUG}/{SOURCE_SHA256}/original.gdb.zip"
RAW_RECEIPT_KEY = RAW_KEY + ".receipt.json"
OUTPUT_NAME = f"{DATASET_SLUG}.parquet"
READY_ROSTER = "metadata/ready_roster.json"
DOWNSTREAM_METADATA = frozenset({
    "metadata/quality_manifest.json",
    "metadata/data_dictionary.json",
    "metadata/resolved_provenance.json",
    "metadata/source/source_manifest.json",
    "metadata/source/data_dictionary.json",
    "metadata/source/quality_manifest.json",
})
EXPECTED_NAMES = ("OBJECTID", "GEOID", "GEOIDFQ", "NAMELSAD", "ALAND", "AWATER", "INTPTLAT", "INTPTLON", "SHAPE")
EXPECTED_TYPES = (pa.int64(), pa.string(), pa.string(), pa.string(), pa.float64(), pa.float64(), pa.string(), pa.string(), pa.binary())
EXPECTED_NATIVE_FIELDS = {
    "GEOID": ("esriFieldTypeString", "11", "true"), "GEOIDFQ": ("esriFieldTypeString", "20", "true"),
    "NAMELSAD": ("esriFieldTypeString", "20", "true"), "ALAND": ("esriFieldTypeDouble", "8", "true"),
    "AWATER": ("esriFieldTypeDouble", "8", "true"), "INTPTLAT": ("esriFieldTypeString", "11", "true"),
    "INTPTLON": ("esriFieldTypeString", "12", "true"), "OBJECTID": ("esriFieldTypeOID", "4", "false"),
    "SHAPE": ("esriFieldTypeGeometry", "0", "true"),
}
LICENSE = "https://www.census.gov/about/policies/quality/guidelines/copyright.html"
MIN_FREE_BYTES = 1_073_741_824


class CensusTractsError(ValueError):
    """A source, capture, or candidate invariant changed; nothing is accepted."""


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode()


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _safe_local_staging(storage: StagingStorageResource) -> Path:
    if storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise CensusTractsError("explicit_unprefixed_local_staging_required")
    root = Path(storage.local_dir).absolute()
    for path in [*reversed(root.parents), root]:
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError:
            continue
        if not stat.S_ISDIR(mode):
            raise CensusTractsError("aliased_local_staging_path")
    root.mkdir(parents=True, exist_ok=True)
    if any(path.is_symlink() or (path.is_file() and path.stat().st_nlink != 1) for path in root.rglob("*")):
        raise CensusTractsError("linked_local_staging_object")
    return root


def _validate_capture(source_path: Path, receipt_path: Path) -> dict[str, object]:
    if source_path.is_symlink() or receipt_path.is_symlink() or not source_path.is_file() or not receipt_path.is_file():
        raise CensusTractsError("retained_source_or_receipt_missing_or_aliased")
    try:
        receipt = json.loads(receipt_path.read_text("utf-8"))
    except (OSError, ValueError) as exc:
        raise CensusTractsError("retained_source_receipt_invalid") from exc
    if not isinstance(receipt, dict):
        raise CensusTractsError("retained_source_receipt_not_object")
    if (
        receipt.get("url") != SOURCE_URL or receipt.get("final_url") != SOURCE_URL
        or type(receipt.get("status")) is not int or receipt["status"] != 200
        or receipt.get("complete_body") is not True
        or receipt.get("bytes") != SOURCE_BYTES or receipt.get("sha256") != SOURCE_SHA256
        or receipt.get("wire_bytes_downloaded") != SOURCE_BYTES
        or receipt.get("requested_accept_encoding") != "identity"
        or source_path.stat().st_size != SOURCE_BYTES or sha256_file(source_path) != SOURCE_SHA256
    ):
        raise CensusTractsError("retained_source_receipt_or_archive_identity_drift")
    return receipt


def _copy_raw_once(root: Path, source_path: Path, receipt_path: Path) -> Path:
    target = root / RAW_KEY
    target.parent.mkdir(parents=True, exist_ok=True)
    receipt_target = root / RAW_RECEIPT_KEY
    if target.exists():
        if target.is_symlink() or target.stat().st_nlink != 1 or sha256_file(target) != SOURCE_SHA256:
            raise CensusTractsError("canonical_raw_archive_changed_or_aliased")
        if not receipt_target.is_file() or receipt_target.read_bytes() != receipt_path.read_bytes():
            raise CensusTractsError("canonical_raw_receipt_changed")
        return target
    if shutil.disk_usage(root).free < SOURCE_BYTES + MIN_FREE_BYTES:
        raise CensusTractsError("insufficient_space_before_canonical_raw_copy")
    partial = target.with_name("original.gdb.zip.partial")
    try:
        shutil.copyfile(source_path, partial)
        if partial.stat().st_size != SOURCE_BYTES or sha256_file(partial) != SOURCE_SHA256:
            raise CensusTractsError("canonical_raw_copy_hash_mismatch")
        os.replace(partial, target)
        receipt_target.write_bytes(receipt_path.read_bytes())
    finally:
        if partial.exists():
            partial.unlink()
    return target


def _parse_layer_definition(xml: str) -> dict[str, object]:
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        raise CensusTractsError("native_layer_definition_invalid_xml") from exc
    fields: dict[str, dict[str, str | None]] = {}
    for field in root.findall("./GPFieldInfoExs/GPFieldInfoEx"):
        name = field.findtext("Name")
        if not name or name in fields:
            raise CensusTractsError("native_layer_definition_duplicate_or_missing_field")
        fields[name] = {"FieldType": field.findtext("FieldType"), "Length": field.findtext("Length"),
                        "IsNullable": field.findtext("IsNullable")}
    if set(fields) != set(EXPECTED_NATIVE_FIELDS):
        raise CensusTractsError("native_layer_definition_field_roster_drift")
    for name, (field_type, length, nullable) in EXPECTED_NATIVE_FIELDS.items():
        item = fields[name]
        if (item["FieldType"] != field_type or item["Length"] != length
                or item["IsNullable"] != nullable):
            raise CensusTractsError("native_layer_definition_physical_type_or_width_drift:" + name)
    if root.findtext("OIDFieldName") != "OBJECTID" or root.findtext("HasZ") != "false" or root.findtext("HasM") != "false":
        raise CensusTractsError("native_layer_definition_oid_or_dimension_drift")
    if root.findtext("ShapeType") != "esriGeometryPolygon":
        raise CensusTractsError("native_layer_definition_shape_type_drift")
    return {"fields": fields, "source_shape_type": root.findtext("ShapeType"),
            "source_oid_field": root.findtext("OIDFieldName"), "source_oid_physical_type": fields["OBJECTID"],
            "has_z": False, "has_m": False, "xml": xml}


def _validate_archive(path: Path) -> dict[str, object]:
    import zipfile

    with zipfile.ZipFile(path) as archive:
        members = archive.infolist()
        if len(members) != SOURCE_MEMBERS or len({entry.filename for entry in members}) != SOURCE_MEMBERS:
            raise CensusTractsError("archive_member_roster_drift")
        if sum(entry.file_size for entry in members) != SOURCE_EXPANDED_BYTES:
            raise CensusTractsError("archive_expanded_size_drift")
        for entry in members:
            member = PurePosixPath(entry.filename)
            if (member.is_absolute() or ".." in member.parts or len(member.parts) != 2
                or member.parts[0] != GDB or entry.flag_bits & 1 or stat.S_ISLNK(entry.external_attr >> 16)):
                raise CensusTractsError("unsafe_archive_member")
    uri = f"/vsizip/{path}/{GDB}"
    _, definition_table = pyogrio.read_arrow(uri, sql="GetLayerDefinition " + LAYER)
    if definition_table.num_rows != 1 or "FIELD_1" not in definition_table.column_names:
        raise CensusTractsError("native_layer_definition_missing")
    native_definition = _parse_layer_definition(definition_table["FIELD_1"][0].as_py())
    info = pyogrio.read_info(uri, layer=LAYER)
    fields = list(info["fields"])
    types = list(info["dtypes"])
    expected_fields = list(EXPECTED_NAMES[1:-1])
    if (
        fields != expected_fields or types != ["object", "object", "object", "float64", "float64", "object", "object"]
        or info.get("crs") != SOURCE_CRS or info.get("geometry_type") != SOURCE_GEOMETRY
        or info.get("geometry_name") != "SHAPE" or info.get("fid_column") != "OBJECTID"
        or info.get("features") != SOURCE_FEATURE_COUNT
    ):
        raise CensusTractsError("source_layer_count_schema_or_crs_drift")
    return {"uri": uri, "info": info, "native_definition": native_definition}


def processing_identity() -> dict[str, object]:
    project_root = Path(__file__).resolve().parents[3]
    identity = shared_processing_identity(project_root)
    return identity


def _geo_schema(schema: pa.Schema) -> pa.Schema:
    geo = {
        "version": "1.1.0", "primary_column": "SHAPE",
        "columns": {"SHAPE": {
            "encoding": "WKB", "geometry_types": [SOURCE_GEOMETRY],
            "crs": pyproj.CRS.from_epsg(4269).to_json_dict(),
        }},
    }
    metadata = dict(schema.metadata or {})
    metadata[b"geo"] = _canonical(geo).rstrip(b"\n")
    return schema.with_metadata(metadata)


def _batch_digest(batch: pa.RecordBatch) -> bytes:
    return batch.replace_schema_metadata(None).serialize().to_pybytes()


def _stream_layer(uri: str, output_path: Path) -> dict[str, object]:
    info = pyogrio.read_info(uri, layer=LAYER)
    digest = hashlib.sha256()
    id_digest = hashlib.sha256()
    seen_oids: set[int] = set()
    seen_geoids: set[str] = set()
    state_counts: dict[str, int] = {}
    rows = 0
    invalid = empty = z_values = 0
    lower = [float("inf"), float("inf")]
    upper = [float("-inf"), float("-inf")]
    with pyogrio.open_arrow(uri, layer=LAYER, return_fids=True, batch_size=2048, use_pyarrow=True) as (metadata, reader):
        if metadata.get("crs") != SOURCE_CRS or metadata.get("geometry_type") != SOURCE_GEOMETRY or metadata.get("fid_column") != "OBJECTID":
            raise CensusTractsError("streamed_layer_metadata_drift")
        if tuple(reader.schema.names) != EXPECTED_NAMES or tuple(field.type for field in reader.schema) != EXPECTED_TYPES:
            raise CensusTractsError("streamed_arrow_schema_drift")
        schema = _geo_schema(reader.schema)
        source_arrow_schema = str(reader.schema)
        with pq.ParquetWriter(output_path, schema, compression="zstd", use_dictionary=True, write_statistics=True) as writer:
            for batch in reader:
                if tuple(batch.schema.names) != EXPECTED_NAMES or tuple(field.type for field in batch.schema) != EXPECTED_TYPES:
                    raise CensusTractsError("streamed_batch_schema_drift")
                if batch.num_rows == 0:
                    continue
                values = batch.to_pydict()
                oids = values["OBJECTID"]
                geoids = values["GEOID"]
                geoidfq = values["GEOIDFQ"]
                if any(value is None for column in values.values() for value in column):
                    raise CensusTractsError("source_null_value_detected")
                for oid, geoid, fq in zip(oids, geoids, geoidfq, strict=True):
                    if type(oid) is not int or oid <= 0 or oid in seen_oids:
                        raise CensusTractsError("native_objectid_missing_or_duplicate")
                    if not isinstance(geoid, str) or len(geoid) != 11 or not geoid.isdigit() or geoid in seen_geoids:
                        raise CensusTractsError("native_geoid_missing_or_duplicate")
                    if fq != "1400000US" + geoid:
                        raise CensusTractsError("literal_geoidfq_relation_drift")
                    seen_oids.add(oid); seen_geoids.add(geoid)
                    state_counts[geoid[:2]] = state_counts.get(geoid[:2], 0) + 1
                geometries = shapely.from_wkb(values["SHAPE"])
                invalid += int((~shapely.is_valid(geometries)).sum())
                empty += int(shapely.is_empty(geometries).sum())
                z_values += int(shapely.has_z(geometries).sum())
                if any(int(kind) != 6 for kind in shapely.get_type_id(geometries)):
                    raise CensusTractsError("native_geometry_type_drift")
                bounds = shapely.bounds(geometries)
                nonempty = ~shapely.is_empty(geometries)
                if nonempty.any():
                    bounds = bounds[nonempty]
                    lower[0] = min(lower[0], float(bounds[:, 0].min())); lower[1] = min(lower[1], float(bounds[:, 1].min()))
                    upper[0] = max(upper[0], float(bounds[:, 2].max())); upper[1] = max(upper[1], float(bounds[:, 3].max()))
                digest.update(_batch_digest(batch))
                rows += batch.num_rows
                writer.write_batch(batch)
                if rows % 8192 < batch.num_rows and shutil.disk_usage(output_path.parent).free < MIN_FREE_BYTES:
                    raise CensusTractsError("minimum_disk_reserve_reached_during_candidate_stream")
        if rows != SOURCE_FEATURE_COUNT or len(seen_oids) != SOURCE_FEATURE_COUNT or len(seen_geoids) != SOURCE_FEATURE_COUNT:
            raise CensusTractsError("source_layer_population_or_identifier_drift")
        id_digest.update("\n".join(str(value) for value in sorted(seen_oids)).encode())
        id_hash = id_digest.hexdigest()
    if not output_path.is_file() or output_path.stat().st_size == 0:
        raise CensusTractsError("native_parquet_missing_after_stream")
    output_digest = hashlib.sha256()
    output_rows = 0
    parquet = pq.ParquetFile(output_path)
    if tuple(parquet.schema_arrow.names) != EXPECTED_NAMES:
        raise CensusTractsError("native_parquet_schema_changed_after_write")
    for batch in parquet.iter_batches(batch_size=2048):
        output_digest.update(_batch_digest(batch))
        output_rows += batch.num_rows
    if output_rows != rows or output_digest.digest() != digest.digest():
        raise CensusTractsError("native_parquet_full_value_or_wkb_parity_failure")
    return {
        "feature_count": rows, "native_objectid_sha256": id_hash,
        "source_batch_sha256": digest.hexdigest(), "parquet_batch_sha256": output_digest.hexdigest(),
        "statefp_counts": dict(sorted(state_counts.items())), "invalid_geometries": invalid,
        "empty_geometries": empty, "z_geometries": z_values,
        "bounds": [lower[0], lower[1], upper[0], upper[1]] if lower[0] != float("inf") else None,
        "parquet_bytes": output_path.stat().st_size, "parquet_sha256": sha256_file(output_path),
        "crs": SOURCE_CRS, "geometry_type": SOURCE_GEOMETRY,
        "arrow_schema": source_arrow_schema,
    }


def _manifest(identity: dict[str, object], audit: dict[str, object], receipt: dict[str, object],
              native_definition: dict[str, object]) -> dict[str, object]:
    manifest = {
        "schema_version": 1, "processor": "HIFLD Next", "approved": False,
        "code_commit": identity["code_commit"], "code_identity_scheme": identity["code_identity_scheme"],
        "title": "Census Tracts — exact 2024 TIGER/Line source, unapproved candidate",
        "publisher": "U.S. Census Bureau", "agency": "U.S. Census Bureau",
        "source_url": SOURCE_URL, "license": LICENSE,
        "lineage": (
            "Direct extraction from the official U.S. Census Bureau TIGER/Line 2024 national substate "
            "geodatabase. This is a proposed source route for HIFLD inventory census-tracts-2; equivalence "
            "to the DataLumos archive or the DHS crosswalk entry has not been established."
        ),
        "processing_fingerprint": _sha(_canonical(identity)),
        "inputs": [{
            "input_id": "tiger2024-substategeo:" + SOURCE_SHA256,
            "producer": "U.S. Census Bureau", "host": "www2.census.gov", "role": "producer_publication",
            "canonical_url": SOURCE_URL, "retrieval_url": SOURCE_URL, "observed_final_url": SOURCE_URL,
            "retrieved_at": receipt["observed_at"], "http_status": 200,
            "sha256": SOURCE_SHA256, "bytes": SOURCE_BYTES, "license": LICENSE,
            "attribution": "U.S. Census Bureau, TIGER/Line 2024 substate geographic database.",
            "release_date_unknown_reason": "The archive is explicitly the 2024 edition; an exact producer publication day has not been independently established.",
        }],
        "transformations": [{
            "operation": "stream_exact_original_filegdb_layer_to_native_geoparquet",
            "parameters": {"layer": LAYER, "original_fid": "OBJECTID", "all_source_attributes": "preserved",
                "geometry": "EPSG:4269 MultiPolygon WKB exposed by GDAL Arrow; copied byte-for-byte to Parquet with no repair, reprojection or simplification",
                "FileGDB_declared_shape_type": native_definition["source_shape_type"],
                "source_native_OID": "OBJECTID esriFieldTypeOID Length4; GDAL Arrow exposes original values as int64, compatibility remains unapproved",
                "source_feature_count": SOURCE_FEATURE_COUNT, "source_archive_sha256": SOURCE_SHA256},
        }],
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Exact source physical attributes, original FID and WKB are retained; output capability does not grant source, baseline, rights, coverage, schema or release acceptance.",
            "objects": [{"path": f"geoparquet/{OUTPUT_NAME}", "bytes": audit["parquet_bytes"], "sha256": audit["parquet_sha256"]}],
        }}},
        "unsupported_output_formats": {
            "geopackage": "No separately approved conversion policy; keep the original FileGDB as the authoritative retained source.",
            "shapefile_zip": "DBF field width and source identifier compatibility are unapproved.",
            "pmtiles": "Reprojection and quantization would change native source coordinates; no visualization mapping is approved.",
        },
    }
    validate_provenance(manifest)
    return manifest


def _file_roster(root: Path, *, exclude: frozenset[str] = frozenset()) -> dict[str, dict[str, object]]:
    """Hash the complete regular-file tree without following aliases."""
    roster: dict[str, dict[str, object]] = {}
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root).as_posix()
        if path.is_symlink():
            raise CensusTractsError("candidate_contains_symlink")
        if path.is_dir():
            continue
        if not path.is_file() or path.stat().st_nlink != 1:
            raise CensusTractsError("candidate_contains_nonregular_or_linked_file")
        if rel in exclude:
            continue
        roster[rel] = {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
    return roster


def _check_file_roster(root: Path, expected: dict[str, dict[str, object]]) -> None:
    # Catalog writes these three derived sidecars after source materialization;
    # snapshot_source_metadata also pins the authored manifest and any existing
    # catalog documents under metadata/source/ before generating new sidecars.
    allowed = set(DOWNSTREAM_METADATA)
    source_manifest = root / "metadata/source_manifest.json"
    if source_manifest.is_file() and not source_manifest.is_symlink():
        digest = sha256_file(source_manifest)
        allowed.add(f"metadata/source/version_{digest}_source_manifest.json")
    if _file_roster(root, exclude=frozenset(allowed | {READY_ROSTER})) != expected:
        raise CensusTractsError("existing_candidate_file_roster_changed")


def _write_ready_roster(root: Path, *, identity: dict[str, object], fingerprint: str,
                        version: str, receipt: dict[str, object]) -> dict[str, object]:
    roster = _file_roster(root, exclude=frozenset({READY_ROSTER}))
    ready = {
        "schema_version": 1, "candidate_version": version,
        "source_archive_sha256": SOURCE_SHA256,
        "receipt_sha256": _sha(_canonical(receipt)),
        "source_schema_sha256": _sha(_canonical(EXPECTED_NAMES)),
        "processing_fingerprint": fingerprint,
        "code_identity": identity,
        "files": roster,
    }
    ready_path = root / READY_ROSTER
    ready_path.parent.mkdir(parents=True, exist_ok=True)
    ready_path.write_bytes(_canonical(ready))
    return ready


def _verify_ready_roster(root: Path, *, identity: dict[str, object], fingerprint: str,
                         version: str, receipt: dict[str, object],
                         native_definition: dict[str, object]) -> tuple[dict[str, object], dict[str, object]]:
    """Accept an existing candidate only when its complete immutable roster matches."""
    ready_path = root / READY_ROSTER
    if root.is_symlink() or not root.is_dir() or ready_path.is_symlink() or not ready_path.is_file():
        raise CensusTractsError("existing_candidate_ready_roster_missing_or_aliased")
    try:
        ready = json.loads(ready_path.read_text("utf-8"))
    except (OSError, ValueError) as exc:
        raise CensusTractsError("existing_candidate_ready_roster_invalid") from exc
    if not isinstance(ready, dict) or ready != {
        "schema_version": 1, "candidate_version": version,
        "source_archive_sha256": SOURCE_SHA256,
        "receipt_sha256": _sha(_canonical(receipt)),
        "source_schema_sha256": _sha(_canonical(EXPECTED_NAMES)),
        "processing_fingerprint": fingerprint,
        "code_identity": identity,
        "files": ready.get("files") if isinstance(ready, dict) else None,
    } or ready_path.read_bytes() != _canonical(ready):
        raise CensusTractsError("existing_candidate_identity_or_ready_roster_drift")
    expected_files = ready.get("files")
    if not isinstance(expected_files, dict) or set(expected_files) != {
        "geoparquet/" + OUTPUT_NAME,
        "metadata/source_manifest.json", "metadata/processing_identity.json",
        "metadata/source_capture.json", "metadata/source_audit.json",
        "metadata/baseline_comparison.json",
    }:
        raise CensusTractsError("existing_candidate_file_roster_incomplete")
    _check_file_roster(root, expected_files)

    def read_json(relative: str) -> dict[str, object]:
        try:
            value = json.loads((root / relative).read_text("utf-8"))
        except (OSError, ValueError) as exc:
            raise CensusTractsError("existing_candidate_metadata_invalid:" + relative) from exc
        if not isinstance(value, dict):
            raise CensusTractsError("existing_candidate_metadata_not_object:" + relative)
        return value

    if (root / "metadata/processing_identity.json").read_bytes() != _canonical(identity):
        raise CensusTractsError("existing_candidate_code_identity_drift")
    capture = read_json("metadata/source_capture.json")
    expected_capture = {"archive_key": RAW_KEY, "receipt_key": RAW_RECEIPT_KEY,
        "archive_sha256": SOURCE_SHA256, "archive_bytes": SOURCE_BYTES,
        "archive_members": SOURCE_MEMBERS, "archive_expanded_bytes": SOURCE_EXPANDED_BYTES,
        "source_url": SOURCE_URL}
    if capture != expected_capture:
        raise CensusTractsError("existing_candidate_source_capture_drift")
    baseline = read_json("metadata/baseline_comparison.json")
    if baseline != {"schema_version": 1, "approved": False, "status": "blocked",
                    "reason_codes": ["published_baseline_and_ID_schema_contract_acceptance_pending"]}:
        raise CensusTractsError("existing_candidate_baseline_metadata_drift")
    manifest = read_json("metadata/source_manifest.json")
    validate_provenance(manifest)
    inputs = manifest.get("inputs")
    transformations = manifest.get("transformations")
    if (manifest.get("approved") is not False or manifest.get("code_commit") != identity.get("code_commit")
        or manifest.get("code_identity_scheme") != identity.get("code_identity_scheme")
        or manifest.get("processing_fingerprint") != fingerprint
        or manifest.get("source_url") != SOURCE_URL or not isinstance(inputs, list) or len(inputs) != 1
        or not isinstance(inputs[0], dict) or inputs[0].get("sha256") != SOURCE_SHA256
        or inputs[0].get("bytes") != SOURCE_BYTES or inputs[0].get("canonical_url") != SOURCE_URL
        or inputs[0].get("retrieved_at") != receipt.get("observed_at")
        or not isinstance(transformations, list) or len(transformations) != 1
        or not isinstance(transformations[0], dict)
        or transformations[0].get("parameters", {}).get("source_archive_sha256") != SOURCE_SHA256):
        raise CensusTractsError("existing_candidate_provenance_identity_drift")
    audit = read_json("metadata/source_audit.json")
    output_record = expected_files["geoparquet/" + OUTPUT_NAME]
    if (audit.get("approved") is not False or audit.get("feature_count") != SOURCE_FEATURE_COUNT
        or audit.get("parquet_sha256") != output_record.get("sha256")
        or audit.get("parquet_bytes") != output_record.get("bytes")
        or audit.get("crs") != SOURCE_CRS or audit.get("geometry_type") != SOURCE_GEOMETRY
        or audit.get("native_layer_definition") != native_definition
        or not audit.get("native_objectid_sha256")
        or audit.get("source_batch_sha256") != audit.get("parquet_batch_sha256")
        or not isinstance(audit.get("statefp_counts"), dict)
        or sum(audit["statefp_counts"].values()) != SOURCE_FEATURE_COUNT):
        raise CensusTractsError("existing_candidate_source_audit_or_output_drift")
    parquet_path = root / "geoparquet" / OUTPUT_NAME
    parquet = pq.ParquetFile(parquet_path)
    if (parquet.metadata.num_rows != SOURCE_FEATURE_COUNT
        or tuple(parquet.schema_arrow.names) != EXPECTED_NAMES
        or sha256_file(parquet_path) != output_record["sha256"]):
        raise CensusTractsError("existing_candidate_native_output_changed")
    return audit, ready


def _candidate_result(version: str, identity: dict[str, object], audit: dict[str, object], *, reused: bool) -> dict[str, object]:
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit["feature_count"], "source_population_complete": True,
        "source_archive_bytes": SOURCE_BYTES, "source_archive_sha256": SOURCE_SHA256,
        "source_id_sha256": audit["native_objectid_sha256"], "source_schema_sha256": _sha(_canonical(EXPECTED_NAMES)),
        "native_parquet_sha256": audit["parquet_sha256"], "native_parquet_bytes": audit["parquet_bytes"],
        "native_value_parity": audit["source_batch_sha256"] == audit["parquet_batch_sha256"],
        "code_identity_scheme": identity["code_identity_scheme"], "code_commit": identity["code_commit"],
        "statefp_counts": audit["statefp_counts"], "candidate_reused": reused, "approved": False}


def stage_census_tracts(
    storage: StagingStorageResource, *, archive_path: str, receipt_path: str,
) -> dict[str, object]:
    """Copy one verified original archive, then stream full native layer to Parquet."""
    root = _safe_local_staging(storage)
    source = Path(archive_path).absolute(); receipt_file = Path(receipt_path).absolute()
    receipt = _validate_capture(source, receipt_file)
    archive = _copy_raw_once(root, source, receipt_file)
    if shutil.disk_usage(root).free < MIN_FREE_BYTES:
        raise CensusTractsError("insufficient_space_before_candidate_stream")
    archive_audit = _validate_archive(archive)
    identity = processing_identity()
    fingerprint = _sha(_canonical(identity))
    version = f"v0.0.0-draft-tiger2024-{SOURCE_SHA256[:12]}-{fingerprint[:16]}"
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    target = root / prefix
    if target.exists() or target.is_symlink():
        audit, _ = _verify_ready_roster(target, identity=identity, fingerprint=fingerprint, version=version,
                                        receipt=receipt, native_definition=archive_audit["native_definition"])
        return _candidate_result(version, identity, audit, reused=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".census-tracts-building-", dir=target.parent))
    try:
        parquet_path = temporary / "geoparquet" / OUTPUT_NAME
        parquet_path.parent.mkdir()
        result = _stream_layer(archive_audit["uri"], parquet_path)
        if shutil.disk_usage(root).free < MIN_FREE_BYTES:
            raise CensusTractsError("minimum_disk_reserve_breached_after_candidate_stream")
        manifest = _manifest(identity, result, receipt, archive_audit["native_definition"])
        objects = {
            "metadata/source_manifest.json": _canonical(manifest),
            "metadata/processing_identity.json": _canonical(identity),
            "metadata/source_capture.json": _canonical({"archive_key": RAW_KEY, "receipt_key": RAW_RECEIPT_KEY,
                "archive_sha256": SOURCE_SHA256, "archive_bytes": SOURCE_BYTES, "archive_members": SOURCE_MEMBERS,
                "archive_expanded_bytes": SOURCE_EXPANDED_BYTES, "source_url": SOURCE_URL}),
            "metadata/source_audit.json": _canonical({"approved": False, "layer": LAYER, **result,
                "native_layer_definition": archive_audit["native_definition"],
                "source_driver_geometry_type": archive_audit["info"]["geometry_type"],
                "FileGDB_declared_shape_type": archive_audit["native_definition"]["source_shape_type"],
                "source_oid_arrow_representation": "OBJECTID is physical esriFieldTypeOID Length4; GDAL Arrow emits int64 with every original value retained",
                "statefp_counts": result["statefp_counts"], "coverage_scope": "all rows returned by the exact Census_Tract layer in the pinned 2024 national substate GDB"}),
            "metadata/baseline_comparison.json": _canonical({"schema_version": 1, "approved": False, "status": "blocked",
                "reason_codes": ["published_baseline_and_ID_schema_contract_acceptance_pending"]}),
        }
        for name, body in objects.items():
            path = temporary / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        _write_ready_roster(temporary, identity=identity, fingerprint=fingerprint, version=version, receipt=receipt)
        os.replace(temporary, target)
    except BaseException as exc:
        quarantine = root / "_quarantine" / DATASET_SLUG / ("failed-" + fingerprint[:16])
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        if temporary.exists() and not quarantine.exists():
            os.replace(temporary, quarantine)
        try:
            (quarantine / "failure.json").write_bytes(_canonical({"approved": False, "failure": type(exc).__name__,
                "reason": str(exc), "archive_key": RAW_KEY, "source_sha256": SOURCE_SHA256}))
        except OSError:
            pass
        raise
    return _candidate_result(version, identity, result, reused=False)

"""Candidate-only streaming extraction of the current national Census Block_Group layer."""
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
from dagster_hifld.sources import census_latest
from dagster_hifld.sources.capture_common import (STAGED_RELEASE_DERIVATIVE_NAMES,
    processing_identity as shared_processing_identity, validate_staged_release_derivatives)
from dagster_hifld.sources.esri_common import sha as _sha
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "census-block-groups-3"
FILE_SLUG = DATASET_SLUG
LAYER = "Block_Group"
# The producer publishes one national substate-geographic FileGDB per vintage; the
# newest published edition is resolved at capture time, never a pinned TGRGDB year.
GEODATABASE_PRODUCT = "substategeo"
SOURCE_CRS = "EPSG:4269"
SOURCE_GEOMETRY = "MultiPolygon"
FEATURE_COUNT_RANGE = (200_000, 300_000)
MAX_ARCHIVE_MEMBERS = 200
MAX_UNCOMPRESSED_BYTES = 2_000_000_000
MAX_SOURCE_BYTES = 900_000_000
OUTPUT_NAME = f"{DATASET_SLUG}.parquet"
SOURCE_OWNED_FILES = {
    "geoparquet/" + OUTPUT_NAME,
    "metadata/source_manifest.json",
    "metadata/processing_identity.json",
    "metadata/source_capture.json",
    "metadata/source_audit.json",
    "metadata/baseline_comparison.json",
}
CATALOG_OWNED_METADATA = {
    "metadata/data_dictionary.json",
    "metadata/resolved_provenance.json",
    "metadata/quality_manifest.json",
    "metadata/source/source_manifest.json",
    "metadata/source/data_dictionary.json",
    "metadata/source/quality_manifest.json",
} | STAGED_RELEASE_DERIVATIVE_NAMES
EXPECTED_NAMES = ("OBJECTID", "GEOID", "GEOIDFQ", "NAMELSAD", "ALAND", "AWATER", "INTPTLAT", "INTPTLON", "SHAPE")
EXPECTED_TYPES = (pa.int64(), pa.string(), pa.string(), pa.string(), pa.float64(), pa.float64(), pa.string(), pa.string(), pa.binary())
EXPECTED_NATIVE_FIELDS = {
    "GEOID": ("esriFieldTypeString", "12"), "GEOIDFQ": ("esriFieldTypeString", "21"),
    "NAMELSAD": ("esriFieldTypeString", "13"), "ALAND": ("esriFieldTypeDouble", "8"),
    "AWATER": ("esriFieldTypeDouble", "8"), "INTPTLAT": ("esriFieldTypeString", "11"),
    "INTPTLON": ("esriFieldTypeString", "12"), "OBJECTID": ("esriFieldTypeOID", "4"),
    "SHAPE": ("esriFieldTypeGeometry", "0"),
}
LICENSE = "https://www.census.gov/about/policies/quality/guidelines/copyright.html"
MIN_FREE_BYTES = 1_073_741_824


class CensusBlockGroupsError(ValueError):
    """A source, capture, or candidate invariant changed; nothing is accepted."""


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode()


def _safe_local_staging(storage: StagingStorageResource) -> Path:
    if storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise CensusBlockGroupsError("explicit_unprefixed_local_staging_required")
    root = Path(storage.local_dir).absolute()
    for path in [*reversed(root.parents), root]:
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError:
            continue
        if not stat.S_ISDIR(mode):
            raise CensusBlockGroupsError("aliased_local_staging_path")
    root.mkdir(parents=True, exist_ok=True)
    if any(path.is_symlink() or (path.is_file() and path.stat().st_nlink != 1) for path in root.rglob("*")):
        raise CensusBlockGroupsError("linked_local_staging_object")
    return root


def _validate_capture(source_path: Path, receipt_path: Path) -> tuple[dict[str, object], census_latest.GeodatabaseRelease]:
    if source_path.is_symlink() or receipt_path.is_symlink() or not source_path.is_file() or not receipt_path.is_file():
        raise CensusBlockGroupsError("retained_source_or_receipt_missing_or_aliased")
    try:
        receipt = json.loads(receipt_path.read_text("utf-8"))
    except (OSError, ValueError) as exc:
        raise CensusBlockGroupsError("retained_source_receipt_invalid") from exc
    if not isinstance(receipt, dict):
        raise CensusBlockGroupsError("retained_source_receipt_not_object")
    try:
        release = census_latest.geodatabase_release_from_receipt(receipt, GEODATABASE_PRODUCT)
    except ValueError as exc:
        raise CensusBlockGroupsError("retained_source_receipt_not_current_substategeo") from exc
    archive_sha = sha256_file(source_path)
    archive_bytes = source_path.stat().st_size
    if (
        receipt.get("url") != release.source_url or receipt.get("final_url") != release.source_url
        or type(receipt.get("status")) is not int or receipt["status"] != 200
        or receipt.get("complete_body") is not True
        or receipt.get("bytes") != archive_bytes or receipt.get("sha256") != archive_sha
        or receipt.get("wire_bytes_downloaded") != archive_bytes
        or receipt.get("requested_accept_encoding") != "identity"
        or archive_bytes <= 0 or archive_bytes > MAX_SOURCE_BYTES
    ):
        raise CensusBlockGroupsError("retained_source_receipt_or_archive_identity_drift")
    return receipt, release


def _raw_keys(archive_sha: str) -> tuple[str, str]:
    raw_key = f"_raw-captures/{DATASET_SLUG}/{archive_sha}/original.gdb.zip"
    return raw_key, raw_key + ".receipt.json"


def _copy_raw_once(root: Path, source_path: Path, receipt_path: Path, receipt: dict[str, object]) -> Path:
    archive_sha = sha256_file(source_path)
    archive_bytes = source_path.stat().st_size
    raw_key, raw_receipt_key = _raw_keys(archive_sha)
    target = root / raw_key
    target.parent.mkdir(parents=True, exist_ok=True)
    receipt_target = root / raw_receipt_key
    if target.exists():
        if target.is_symlink() or target.stat().st_nlink != 1 or sha256_file(target) != archive_sha:
            raise CensusBlockGroupsError("canonical_raw_archive_changed_or_aliased")
        if not receipt_target.is_file() or receipt_target.read_bytes() != receipt_path.read_bytes():
            raise CensusBlockGroupsError("canonical_raw_receipt_changed")
        return target
    if shutil.disk_usage(root).free < archive_bytes + MIN_FREE_BYTES:
        raise CensusBlockGroupsError("insufficient_space_before_canonical_raw_copy")
    partial = target.with_name("original.gdb.zip.partial")
    try:
        shutil.copyfile(source_path, partial)
        if partial.stat().st_size != archive_bytes or sha256_file(partial) != archive_sha:
            raise CensusBlockGroupsError("canonical_raw_copy_hash_mismatch")
        os.replace(partial, target)
        receipt_target.write_bytes(_canonical(receipt))
    finally:
        if partial.exists():
            partial.unlink()
    return target


def _parse_layer_definition(xml: str) -> dict[str, object]:
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        raise CensusBlockGroupsError("native_layer_definition_invalid_xml") from exc
    fields: dict[str, dict[str, str | None]] = {}
    for field in root.findall("./GPFieldInfoExs/GPFieldInfoEx"):
        name = field.findtext("Name")
        if not name or name in fields:
            raise CensusBlockGroupsError("native_layer_definition_duplicate_or_missing_field")
        fields[name] = {"FieldType": field.findtext("FieldType"), "Length": field.findtext("Length"),
                        "IsNullable": field.findtext("IsNullable")}
    if set(fields) != set(EXPECTED_NATIVE_FIELDS):
        raise CensusBlockGroupsError("native_layer_definition_field_roster_drift")
    for name, (field_type, length) in EXPECTED_NATIVE_FIELDS.items():
        item = fields[name]
        if item["FieldType"] != field_type or item["Length"] != length:
            raise CensusBlockGroupsError("native_layer_definition_physical_type_or_width_drift:" + name)
    if root.findtext("OIDFieldName") != "OBJECTID" or root.findtext("HasZ") != "false" or root.findtext("HasM") != "false":
        raise CensusBlockGroupsError("native_layer_definition_oid_or_dimension_drift")
    if root.findtext("ShapeType") != "esriGeometryPolygon":
        raise CensusBlockGroupsError("native_layer_definition_shape_type_drift")
    return {"fields": fields, "source_shape_type": root.findtext("ShapeType"),
            "source_oid_field": root.findtext("OIDFieldName"), "source_oid_physical_type": fields["OBJECTID"],
            "has_z": False, "has_m": False, "xml": xml}


def _validate_archive(path: Path, release: census_latest.GeodatabaseRelease) -> dict[str, object]:
    import zipfile

    gdb_root = release.source_file[:-4]
    with zipfile.ZipFile(path) as archive:
        members = archive.infolist()
        if not members or len(members) > MAX_ARCHIVE_MEMBERS or len({entry.filename for entry in members}) != len(members):
            raise CensusBlockGroupsError("archive_member_roster_drift")
        expanded = sum(entry.file_size for entry in members)
        if expanded <= 0 or expanded > MAX_UNCOMPRESSED_BYTES:
            raise CensusBlockGroupsError("archive_expanded_size_drift")
        for entry in members:
            member = PurePosixPath(entry.filename)
            if (member.is_absolute() or ".." in member.parts or len(member.parts) != 2
                or member.parts[0] != gdb_root or entry.flag_bits & 1 or stat.S_ISLNK(entry.external_attr >> 16)):
                raise CensusBlockGroupsError("unsafe_archive_member")
    uri = f"/vsizip/{path}/{gdb_root}"
    _, definition_table = pyogrio.read_arrow(uri, sql="GetLayerDefinition " + LAYER)
    if definition_table.num_rows != 1 or "FIELD_1" not in definition_table.column_names:
        raise CensusBlockGroupsError("native_layer_definition_missing")
    native_definition = _parse_layer_definition(definition_table["FIELD_1"][0].as_py())
    info = pyogrio.read_info(uri, layer=LAYER)
    fields = list(info["fields"])
    types = list(info["dtypes"])
    expected_fields = list(EXPECTED_NAMES[1:-1])
    if (
        fields != expected_fields or types != ["object", "object", "object", "float64", "float64", "object", "object"]
        or info.get("crs") != SOURCE_CRS or info.get("geometry_type") != SOURCE_GEOMETRY
        or info.get("geometry_name") != "SHAPE" or info.get("fid_column") != "OBJECTID"
        or not FEATURE_COUNT_RANGE[0] <= info.get("features", 0) <= FEATURE_COUNT_RANGE[1]
    ):
        raise CensusBlockGroupsError("source_layer_count_schema_or_crs_drift")
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
            raise CensusBlockGroupsError("streamed_layer_metadata_drift")
        if tuple(reader.schema.names) != EXPECTED_NAMES or tuple(field.type for field in reader.schema) != EXPECTED_TYPES:
            raise CensusBlockGroupsError("streamed_arrow_schema_drift")
        schema = _geo_schema(reader.schema)
        source_arrow_schema = str(reader.schema)
        with pq.ParquetWriter(output_path, schema, compression="zstd", use_dictionary=True, write_statistics=True) as writer:
            for batch in reader:
                if tuple(batch.schema.names) != EXPECTED_NAMES or tuple(field.type for field in batch.schema) != EXPECTED_TYPES:
                    raise CensusBlockGroupsError("streamed_batch_schema_drift")
                if batch.num_rows == 0:
                    continue
                values = batch.to_pydict()
                oids = values["OBJECTID"]
                geoids = values["GEOID"]
                geoidfq = values["GEOIDFQ"]
                if any(value is None for column in values.values() for value in column):
                    raise CensusBlockGroupsError("source_null_value_detected")
                for oid, geoid, fq in zip(oids, geoids, geoidfq, strict=True):
                    if type(oid) is not int or oid <= 0 or oid in seen_oids:
                        raise CensusBlockGroupsError("native_objectid_missing_or_duplicate")
                    if not isinstance(geoid, str) or len(geoid) != 12 or not geoid.isdigit() or geoid in seen_geoids:
                        raise CensusBlockGroupsError("native_geoid_missing_or_duplicate")
                    if fq != "1500000US" + geoid:
                        raise CensusBlockGroupsError("literal_geoidfq_relation_drift")
                    seen_oids.add(oid); seen_geoids.add(geoid)
                    state_counts[geoid[:2]] = state_counts.get(geoid[:2], 0) + 1
                geometries = shapely.from_wkb(values["SHAPE"])
                invalid += int((~shapely.is_valid(geometries)).sum())
                empty += int(shapely.is_empty(geometries).sum())
                z_values += int(shapely.has_z(geometries).sum())
                if invalid or empty or z_values:
                    raise CensusBlockGroupsError("native_geometry_invalid_empty_or_unexpected_z")
                if any(int(kind) != 6 for kind in shapely.get_type_id(geometries)):
                    raise CensusBlockGroupsError("native_geometry_type_drift")
                bounds = shapely.bounds(geometries)
                lower[0] = min(lower[0], float(bounds[:, 0].min())); lower[1] = min(lower[1], float(bounds[:, 1].min()))
                upper[0] = max(upper[0], float(bounds[:, 2].max())); upper[1] = max(upper[1], float(bounds[:, 3].max()))
                digest.update(_batch_digest(batch))
                rows += batch.num_rows
                writer.write_batch(batch)
                if rows % 8192 < batch.num_rows and shutil.disk_usage(output_path.parent).free < MIN_FREE_BYTES:
                    raise CensusBlockGroupsError("minimum_disk_reserve_reached_during_candidate_stream")
        if (not FEATURE_COUNT_RANGE[0] <= rows <= FEATURE_COUNT_RANGE[1]
                or len(seen_oids) != rows or len(seen_geoids) != rows):
            raise CensusBlockGroupsError("source_layer_population_or_identifier_drift")
        id_digest.update("\n".join(str(value) for value in sorted(seen_oids)).encode())
        id_hash = id_digest.hexdigest()
    if not output_path.is_file() or output_path.stat().st_size == 0:
        raise CensusBlockGroupsError("native_parquet_missing_after_stream")
    output_digest = hashlib.sha256()
    output_rows = 0
    parquet = pq.ParquetFile(output_path)
    if tuple(parquet.schema_arrow.names) != EXPECTED_NAMES:
        raise CensusBlockGroupsError("native_parquet_schema_changed_after_write")
    for batch in parquet.iter_batches(batch_size=2048):
        output_digest.update(_batch_digest(batch))
        output_rows += batch.num_rows
    if output_rows != rows or output_digest.digest() != digest.digest():
        raise CensusBlockGroupsError("native_parquet_full_value_or_wkb_parity_failure")
    return {
        "feature_count": rows, "native_objectid_sha256": id_hash,
        "source_batch_sha256": digest.hexdigest(), "parquet_batch_sha256": output_digest.hexdigest(),
        "statefp_counts": dict(sorted(state_counts.items())), "invalid_geometries": invalid,
        "empty_geometries": empty, "z_geometries": z_values,
        "bounds": [lower[0], lower[1], upper[0], upper[1]],
        "parquet_bytes": output_path.stat().st_size, "parquet_sha256": sha256_file(output_path),
        "crs": SOURCE_CRS, "geometry_type": SOURCE_GEOMETRY,
        "arrow_schema": source_arrow_schema,
    }


def _manifest(identity: dict[str, object], audit: dict[str, object], receipt: dict[str, object],
              release: census_latest.GeodatabaseRelease, native_definition: dict[str, object]) -> dict[str, object]:
    archive_sha = str(receipt["sha256"])
    archive_bytes = int(receipt["bytes"])
    manifest = {
        "schema_version": 1, "processor": "HIFLD Next", "approved": False,
        "code_commit": identity["code_commit"], "code_identity_scheme": identity["code_identity_scheme"],
        "title": f"Census Block Groups — {release.vintage} TIGER/Line national substate GDB, unapproved candidate",
        "publisher": "U.S. Census Bureau", "agency": "U.S. Census Bureau",
        "source_url": release.source_url, "license": LICENSE,
        "source_release": release.as_metadata(),
        "processing_fingerprint": _sha(_canonical(identity)),
        "inputs": [{
            "input_id": f"tiger{release.vintage}-substategeo:" + archive_sha,
            "producer": "U.S. Census Bureau", "host": "www2.census.gov", "role": "producer_publication",
            "canonical_url": release.source_url, "retrieval_url": str(receipt["final_url"]),
            "observed_final_url": str(receipt["final_url"]),
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "sha256": archive_sha, "bytes": archive_bytes, "license": LICENSE,
            "attribution": f"U.S. Census Bureau, TIGER/Line {release.vintage} substate geographic database.",
            "release_date_unknown_reason": f"The producer identifies this as the {release.vintage} TIGER/Line edition; the directory listing and HTTP Last-Modified values are retained as discovery evidence and are not treated as the producer release date.",
        }],
        "transformations": [{
            "operation": "stream_exact_original_filegdb_layer_to_native_geoparquet",
            "parameters": {"layer": LAYER, "original_fid": "OBJECTID", "all_source_attributes": "preserved",
                "geometry": "EPSG:4269 MultiPolygon WKB exposed by GDAL Arrow; copied byte-for-byte to Parquet with no repair, reprojection or simplification",
                "FileGDB_declared_shape_type": native_definition["source_shape_type"],
                "source_native_OID": "OBJECTID esriFieldTypeOID Length4; GDAL Arrow exposes original values as int64, compatibility remains unapproved",
                "source_feature_count": audit["feature_count"], "source_archive_sha256": archive_sha},
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


def _read_candidate_file(target: Path, relative: str) -> bytes:
    path = target / relative
    if path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1:
        raise CensusBlockGroupsError("cached_candidate_source_file_missing_or_aliased:" + relative)
    return path.read_bytes()


def _reuse_existing_candidate(
    target: Path, *, identity: dict[str, object], receipt: dict[str, object],
    release: census_latest.GeodatabaseRelease, native_definition: dict[str, object],
    version: str, raw_key: str, raw_receipt_key: str, archive_sha: str, archive_bytes: int,
) -> dict[str, object]:
    """Reuse an exact immutable source candidate while ignoring catalog-owned metadata."""
    if target.is_symlink() or not target.is_dir():
        raise CensusBlockGroupsError("cached_candidate_not_a_real_directory")
    source_manifest_path = target / "metadata/source_manifest.json"
    if source_manifest_path.is_symlink() or not source_manifest_path.is_file():
        raise CensusBlockGroupsError("cached_candidate_source_roster_incomplete")
    manifest_bytes = _read_candidate_file(target, "metadata/source_manifest.json")
    snapshot_name = "metadata/source/version_" + hashlib.sha256(manifest_bytes).hexdigest() + "_source_manifest.json"
    allowed_catalog_metadata = CATALOG_OWNED_METADATA | {
        "metadata/source/" + Path(snapshot_name).name,
    }
    files = {path.relative_to(target).as_posix() for path in target.rglob("*") if path.is_file()}
    links = [path for path in target.rglob("*") if path.is_symlink()]
    if links or files - SOURCE_OWNED_FILES - allowed_catalog_metadata:
        raise CensusBlockGroupsError("cached_candidate_source_roster_drift")
    if not SOURCE_OWNED_FILES <= files:
        raise CensusBlockGroupsError("cached_candidate_source_roster_incomplete")
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    class CandidateStorage:
        def read_key(self, key): return (target / key.removeprefix(prefix + "/")).read_bytes()
    try: validate_staged_release_derivatives(CandidateStorage(), prefix, {f"{prefix}/{name}" for name in files})
    except (OSError,UnicodeError,ValueError,TypeError) as exc: raise CensusBlockGroupsError("invalid_staged_release_derivatives") from exc
    catalog_source_manifests = {
        "metadata/source/source_manifest.json",
        snapshot_name,
    }
    for relative in catalog_source_manifests & files:
        if _read_candidate_file(target, relative) != manifest_bytes:
            raise CensusBlockGroupsError("cached_candidate_catalog_source_manifest_snapshot_tampered")
    try:
        stored_identity = json.loads(_read_candidate_file(target, "metadata/processing_identity.json"))
        capture = json.loads(_read_candidate_file(target, "metadata/source_capture.json"))
        audit = json.loads(_read_candidate_file(target, "metadata/source_audit.json"))
        manifest = json.loads(manifest_bytes)
        baseline = json.loads(_read_candidate_file(target, "metadata/baseline_comparison.json"))
    except (OSError, ValueError) as exc:
        raise CensusBlockGroupsError("cached_candidate_source_metadata_invalid") from exc
    if stored_identity != identity:
        raise CensusBlockGroupsError("cached_candidate_processing_identity_drift")
    expected_capture = {
        "archive_key": raw_key, "receipt_key": raw_receipt_key,
        "archive_sha256": archive_sha, "archive_bytes": archive_bytes,
        "source_url": release.source_url, "source_release": release.as_metadata(),
    }
    if capture != expected_capture:
        raise CensusBlockGroupsError("cached_candidate_input_identity_drift")
    if (
        audit.get("approved") is not False or audit.get("layer") != LAYER
        or not FEATURE_COUNT_RANGE[0] <= audit.get("feature_count", -1) <= FEATURE_COUNT_RANGE[1]
        or audit.get("crs") != SOURCE_CRS or audit.get("geometry_type") != SOURCE_GEOMETRY
        or audit.get("invalid_geometries") != 0 or audit.get("empty_geometries") != 0
        or audit.get("z_geometries") != 0
        or audit.get("native_layer_definition") != native_definition
        or audit.get("source_driver_geometry_type") != SOURCE_GEOMETRY
        or audit.get("FileGDB_declared_shape_type") != native_definition.get("source_shape_type")
        or audit.get("source_batch_sha256") != audit.get("parquet_batch_sha256")
        or audit.get("source_oid_arrow_representation") !=
            "OBJECTID is physical esriFieldTypeOID Length4; GDAL Arrow emits int64 with every original value retained"
        or audit.get("coverage_scope") !=
            f"all rows returned by the exact Block_Group layer in the {release.vintage} national substate GDB"
    ):
        raise CensusBlockGroupsError("cached_candidate_source_audit_drift")
    parquet_relative = "geoparquet/" + OUTPUT_NAME
    parquet = target / parquet_relative
    if (
        parquet.is_symlink() or not parquet.is_file() or parquet.stat().st_nlink != 1
        or parquet.stat().st_size != audit.get("parquet_bytes")
        or sha256_file(parquet) != audit.get("parquet_sha256")
    ):
        raise CensusBlockGroupsError("cached_candidate_native_parquet_tampered")
    try:
        parquet_file = pq.ParquetFile(parquet)
        native_schema = parquet_file.schema_arrow
    except Exception as exc:
        raise CensusBlockGroupsError("cached_candidate_native_parquet_unreadable") from exc
    if tuple(native_schema.names) != EXPECTED_NAMES or tuple(field.type for field in native_schema) != EXPECTED_TYPES:
        raise CensusBlockGroupsError("cached_candidate_native_parquet_schema_drift")
    geo_metadata = json.loads((native_schema.metadata or {}).get(b"geo", b"{}"))
    shape = geo_metadata.get("columns", {}).get("SHAPE", {})
    crs = shape.get("crs", {})
    crs_id = crs.get("id", {}) if isinstance(crs, dict) else {}
    if (
        not FEATURE_COUNT_RANGE[0] <= parquet_file.metadata.num_rows <= FEATURE_COUNT_RANGE[1]
        or geo_metadata.get("primary_column") != "SHAPE"
        or shape.get("encoding") != "WKB" or shape.get("geometry_types") != [SOURCE_GEOMETRY]
        or crs_id.get("authority") != "EPSG" or str(crs_id.get("code")) != "4269"
    ):
        raise CensusBlockGroupsError("cached_candidate_native_parquet_population_or_geo_metadata_drift")
    if manifest != _manifest(identity, audit, receipt, release, native_definition):
        raise CensusBlockGroupsError("cached_candidate_source_manifest_drift")
    if baseline != {"schema_version": 1, "approved": False, "status": "blocked",
                    "reason_codes": ["published_baseline_and_ID_schema_contract_acceptance_pending"]}:
        raise CensusBlockGroupsError("cached_candidate_baseline_contract_drift")
    state_counts = audit.get("statefp_counts")
    if not isinstance(state_counts, dict) or not FEATURE_COUNT_RANGE[0] <= sum(state_counts.values()) <= FEATURE_COUNT_RANGE[1]:
        raise CensusBlockGroupsError("cached_candidate_coverage_summary_drift")
    return {
        "dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": audit.get("feature_count"), "source_population_complete": True,
        "source_archive_bytes": archive_bytes, "source_archive_sha256": archive_sha,
        "source_release_vintage": str(release.vintage),
        "source_id_sha256": audit.get("native_objectid_sha256"),
        "source_schema_sha256": _sha(_canonical(EXPECTED_NAMES)),
        "native_parquet_sha256": audit["parquet_sha256"],
        "native_parquet_bytes": audit["parquet_bytes"], "native_value_parity": True,
        "code_identity_scheme": identity["code_identity_scheme"], "code_commit": identity["code_commit"],
        "statefp_counts": state_counts, "approved": False, "reused": True,
    }


def stage_census_block_groups(
    storage: StagingStorageResource, *, archive_path: str, receipt_path: str,
) -> dict[str, object]:
    """Copy one verified original archive, then stream full native layer to Parquet."""
    root = _safe_local_staging(storage)
    source = Path(archive_path).absolute(); receipt_file = Path(receipt_path).absolute()
    receipt, release = _validate_capture(source, receipt_file)
    archive_sha = str(receipt["sha256"]); archive_bytes = int(receipt["bytes"])
    raw_key, raw_receipt_key = _raw_keys(archive_sha)
    archive = _copy_raw_once(root, source, receipt_file, receipt)
    if shutil.disk_usage(root).free < MIN_FREE_BYTES:
        raise CensusBlockGroupsError("insufficient_space_before_candidate_stream")
    archive_audit = _validate_archive(archive, release)
    identity = processing_identity()
    fingerprint = _sha(_canonical(identity))
    version = census_latest.release_version(str(release.vintage), archive_sha, identity["code_commit"])
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version}"
    target = root / prefix
    if target.exists() or target.is_symlink():
        return _reuse_existing_candidate(
            target, identity=identity, receipt=receipt, release=release,
            native_definition=archive_audit["native_definition"], version=version,
            raw_key=raw_key, raw_receipt_key=raw_receipt_key,
            archive_sha=archive_sha, archive_bytes=archive_bytes,
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".census-bg-building-", dir=target.parent))
    try:
        parquet_path = temporary / "geoparquet" / OUTPUT_NAME
        parquet_path.parent.mkdir()
        result = _stream_layer(archive_audit["uri"], parquet_path)
        if shutil.disk_usage(root).free < MIN_FREE_BYTES:
            raise CensusBlockGroupsError("minimum_disk_reserve_breached_after_candidate_stream")
        manifest = _manifest(identity, result, receipt, release, archive_audit["native_definition"])
        objects = {
            "metadata/source_manifest.json": _canonical(manifest),
            "metadata/processing_identity.json": _canonical(identity),
            "metadata/source_capture.json": _canonical({"archive_key": raw_key, "receipt_key": raw_receipt_key,
                "archive_sha256": archive_sha, "archive_bytes": archive_bytes,
                "source_url": release.source_url, "source_release": release.as_metadata()}),
            "metadata/source_audit.json": _canonical({"approved": False, "layer": LAYER, **result,
                "native_layer_definition": archive_audit["native_definition"],
                "source_driver_geometry_type": archive_audit["info"]["geometry_type"],
                "FileGDB_declared_shape_type": archive_audit["native_definition"]["source_shape_type"],
                "source_oid_arrow_representation": "OBJECTID is physical esriFieldTypeOID Length4; GDAL Arrow emits int64 with every original value retained",
                "statefp_counts": result["statefp_counts"],
                "coverage_scope": f"all rows returned by the exact Block_Group layer in the {release.vintage} national substate GDB"}),
            "metadata/baseline_comparison.json": _canonical({"schema_version": 1, "approved": False, "status": "blocked",
                "reason_codes": ["published_baseline_and_ID_schema_contract_acceptance_pending"]}),
        }
        for name, body in objects.items():
            path = temporary / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        os.replace(temporary, target)
    except BaseException as exc:
        quarantine = root / "_quarantine" / DATASET_SLUG / ("failed-" + fingerprint[:16])
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        if temporary.exists() and not quarantine.exists():
            os.replace(temporary, quarantine)
        try:
            (quarantine / "failure.json").write_bytes(_canonical({"approved": False, "failure": type(exc).__name__,
                "reason": str(exc), "archive_key": raw_key, "source_sha256": archive_sha}))
        except OSError:
            pass
        raise
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
        "feature_count": result["feature_count"], "source_population_complete": True,
        "source_archive_bytes": archive_bytes, "source_archive_sha256": archive_sha,
        "source_release_vintage": str(release.vintage),
        "source_id_sha256": result["native_objectid_sha256"], "source_schema_sha256": _sha(_canonical(EXPECTED_NAMES)),
        "native_parquet_sha256": result["parquet_sha256"], "native_parquet_bytes": result["parquet_bytes"],
        "native_value_parity": result["source_batch_sha256"] == result["parquet_batch_sha256"],
        "code_identity_scheme": identity["code_identity_scheme"], "code_commit": identity["code_commit"],
        "statefp_counts": result["statefp_counts"], "approved": False}

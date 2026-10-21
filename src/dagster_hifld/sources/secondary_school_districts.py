"""Source-faithful capture of the 2024 Census secondary school district layer.

This is a proposal for the HIFLD inventory entry. It captures the official
Census TIGER/Line File Geodatabase itself, then extracts one explicitly named
layer without changing its CRS, attributes, IDs, or coordinate values. It does
not establish equivalence with the historical DataLumos copy or approve release.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.sources.epa_frs_common import processing_identity

DATASET_SLUG = "secondary-school-districts"
FILE_SLUG = DATASET_SLUG
SOURCE_URL = "https://www2.census.gov/geo/tiger/TGRGDB24/tlgdb_2024_a_us_school.gdb.zip"
SOURCE_HOST = "www2.census.gov"
LAYER_NAME = "School_District_Secondary"
EXPECTED_COUNT = 481
MAX_ARCHIVE_BYTES = 128_000_000
MAX_UNCOMPRESSED_BYTES = 1_500_000_000
EXPECTED_SOURCE_FIELDS = [
    ("GEOID", pa.string()),
    ("GEOIDFQ", pa.string()),
    ("NAME", pa.string()),
    ("LOGRADE", pa.string()),
    ("HIGRADE", pa.string()),
    ("SDTYP", pa.string()),
    ("ALAND", pa.float64()),
    ("AWATER", pa.float64()),
    ("INTPTLAT", pa.string()),
    ("INTPTLON", pa.string()),
    ("SHAPE", pa.binary()),
]
EXPECTED_OUTPUT_FIELDS = ["OBJECTID", "GEOID", "GEOIDFQ", "NAME", "LOGRADE", "HIGRADE", "SDTYP", "ALAND", "AWATER", "INTPTLAT", "INTPTLON", "geometry"]


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _capture_archive(staging, version: str, client: httpx.Client) -> tuple[bytes, dict]:
    wire = bytearray()
    attempt_observed_at = datetime.now(timezone.utc).isoformat()
    transfer_receipt_written = False
    try:
        with client.stream("GET", SOURCE_URL, headers={"Accept-Encoding": "identity"}) as response:
            final_host = response.request.url.host
            result = capture_http_response(
                response,
                max_wire_bytes=MAX_ARCHIVE_BYTES,
                max_decoded_bytes=MAX_ARCHIVE_BYTES,
                reserve=lambda progress: _reserve(progress.wire_bytes_received, progress.reserve_bytes),
                retain_wire=wire.extend,
                # Census serves this file with identity encoding. Retaining the
                # wire bytes once avoids storing a second identical 80 MB blob.
                retain_decoded=lambda _block: None,
            )
            receipt = {
                "method": "GET", "requested_url": SOURCE_URL,
                "final_url": str(response.request.url), "final_host": final_host,
                "status": result.status_code,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "content_type": response.headers.get("content-type"),
                "last_modified": response.headers.get("last-modified"),
                "content_encoding": result.content_encoding,
                "content_length_header": result.content_length_header,
                "wire_bytes_received": result.wire_bytes_received,
                "wire_bytes_retained": result.wire_bytes_retained,
                "wire_sha256": result.wire_sha256,
                "decoded_bytes_produced": result.decoded_bytes_produced,
                "transfer_complete": result.complete,
                "transfer_error_code": result.error_code,
                "raw_archive_key": f"raw/{Path(SOURCE_URL).name}",
                "raw_sha256": _sha256(bytes(wire)),
                "release_accepted": False,
            }
            staging.write(DATASET_SLUG, FILE_SLUG, version, receipt["raw_archive_key"], bytes(wire))
            staging.write(DATASET_SLUG, FILE_SLUG, version, "raw/source_archive.receipt.json", _json_bytes(receipt))
            transfer_receipt_written = True
            if (
                not result.complete
                or not 200 <= result.status_code < 300
                or result.content_encoding != "identity"
                or result.decoded_bytes_produced != len(wire)
                or final_host.lower() != SOURCE_HOST
                or not str(response.headers.get("content-type", "")).lower().startswith("application/zip")
            ):
                raise ValueError("source_archive_transfer_drift")
            if len(wire) == 0 or len(wire) > MAX_ARCHIVE_BYTES:
                raise ValueError("source_archive_size_out_of_bounds")
            return bytes(wire), receipt
    except ValueError as exc:
        if not transfer_receipt_written:
            _retain_partial_transfer(staging, version, wire, attempt_observed_at, type(exc).__name__)
        raise
    except Exception as exc:
        _retain_partial_transfer(staging, version, wire, attempt_observed_at, type(exc).__name__)
        raise ValueError("source_archive_transport_error:" + type(exc).__name__) from exc


def _retain_partial_transfer(staging, version: str, wire: bytearray, observed_at: str, error_type: str) -> None:
    partial = bytes(wire)
    partial_key = "raw/source_archive.partial.zip" if partial else None
    if partial_key:
        staging.write(DATASET_SLUG, FILE_SLUG, version, partial_key, partial)
    staging.write(DATASET_SLUG, FILE_SLUG, version, "raw/source_archive.failure_receipt.json", _json_bytes({
        "method": "GET", "requested_url": SOURCE_URL,
        "observed_at": observed_at,
        "wire_bytes_retained": len(partial),
        "wire_sha256": _sha256(partial),
        "partial_wire_key": partial_key,
        "transfer_error_type": error_type, "complete": False,
    }))


def _reserve(current: int, upcoming: int) -> None:
    if current + upcoming > MAX_ARCHIVE_BYTES:
        raise ValueError("source_archive_capture_budget_exceeded")


def _validate_archive(archive: bytes) -> None:
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as source:
            entries = source.infolist()
            if not entries or sum(item.file_size for item in entries) > MAX_UNCOMPRESSED_BYTES:
                raise ValueError("source_archive_expansion_limit_exceeded")
            if any(item.file_size > MAX_UNCOMPRESSED_BYTES for item in entries):
                raise ValueError("source_archive_member_limit_exceeded")
            for item in entries:
                parts = Path(item.filename).parts
                if Path(item.filename).is_absolute() or ".." in parts or item.filename.startswith(("/", "\\")):
                    raise ValueError("source_archive_unsafe_member_path")
            gdb_roots = {"/".join(item.filename.split("/")[:1]) for item in entries if ".gdb/" in item.filename}
            if len(gdb_roots) != 1 or not next(iter(gdb_roots)).endswith(".gdb"):
                raise ValueError("source_archive_geodatabase_roster_drift")
            corrupt = source.testzip()
            if corrupt is not None:
                raise ValueError("source_archive_crc_failure")
    except zipfile.BadZipFile as exc:
        raise ValueError("source_archive_invalid_zip") from exc


def _native_table(archive_path: Path) -> tuple[pa.Table, list[int], dict]:
    try:
        layers = pyogrio.list_layers(archive_path)
        layer_names = [str(row[0]) for row in layers]
        if LAYER_NAME not in layer_names:
            raise ValueError("source_target_layer_missing")
        info = pyogrio.read_info(archive_path, layer=LAYER_NAME, force_feature_count=True)
        if info.get("features") != EXPECTED_COUNT or info.get("geometry_type") != "MultiPolygon":
            raise ValueError("source_layer_population_or_geometry_drift")
        try:
            crs = pyproj.CRS.from_user_input(info.get("crs"))
        except Exception as exc:
            raise ValueError("source_crs_drift") from exc
        if not crs.equals(pyproj.CRS.from_epsg(4269), ignore_axis_order=True):
            raise ValueError("source_crs_drift")
        metadata, table = pyogrio.raw.read_arrow(archive_path, layer=LAYER_NAME)
        if metadata.get("geometry_type") != "MultiPolygon" or not pyproj.CRS.from_user_input(metadata.get("crs")).equals(pyproj.CRS.from_epsg(4269), ignore_axis_order=True):
            raise ValueError("source_arrow_layer_metadata_drift")
        geometry_name = metadata.get("geometry_name")
        if geometry_name != "SHAPE":
            raise ValueError("source_geometry_column_drift")
        actual = [(field.name, field.type) for field in table.schema]
        if actual != EXPECTED_SOURCE_FIELDS or table.num_rows != EXPECTED_COUNT:
            raise ValueError("source_schema_or_feature_count_drift")
        fid_frame = pyogrio.read_dataframe(archive_path, layer=LAYER_NAME, fid_as_index=True, read_geometry=False, use_arrow=True)
        ids = [int(value) for value in fid_frame.index.tolist()]
        if len(ids) != EXPECTED_COUNT or len(set(ids)) != EXPECTED_COUNT or sorted(ids) != list(range(1, EXPECTED_COUNT + 1)):
            raise ValueError("source_native_id_population_drift")
        table_geoids = table.column("GEOID").to_pylist()
        if fid_frame["GEOID"].tolist() != table_geoids or len(set(table_geoids)) != EXPECTED_COUNT:
            raise ValueError("source_geoid_population_drift")
        return table, ids, {"info": info, "table_metadata": metadata, "layers": layer_names}
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("source_geodatabase_read_error:" + type(exc).__name__) from exc


def _build_geoparquet(table: pa.Table, ids: list[int]) -> tuple[bytes, dict]:
    geometry_values = table.column("SHAPE").to_pylist()
    if len(geometry_values) != EXPECTED_COUNT or any(not isinstance(value, bytes) for value in geometry_values):
        raise ValueError("source_geometry_population_drift")
    geometry_types: set[str] = set()
    xmin = ymin = float("inf")
    xmax = ymax = float("-inf")
    for raw in geometry_values:
        try:
            geometry = shapely.from_wkb(raw)
        except Exception as exc:
            raise ValueError("source_geometry_wkb_invalid") from exc
        if geometry.is_empty or geometry.geom_type != "MultiPolygon" or not geometry.is_valid:
            raise ValueError("source_geometry_type_or_validity_drift")
        bounds = geometry.bounds
        if len(bounds) != 4 or not (-180 <= bounds[0] <= bounds[2] <= 180 and -90 <= bounds[1] <= bounds[3] <= 90):
            raise ValueError("source_geometry_coordinate_bounds_drift")
        xmin, ymin = min(xmin, bounds[0]), min(ymin, bounds[1])
        xmax, ymax = max(xmax, bounds[2]), max(ymax, bounds[3])
        geometry_types.add(geometry.geom_type)
    if geometry_types != {"MultiPolygon"}:
        raise ValueError("source_geometry_type_drift")

    names = [name for name, _ in EXPECTED_SOURCE_FIELDS if name != "SHAPE"]
    columns = [pa.array(ids, type=pa.int64())]
    columns.extend(table.column(name) for name in names)
    columns.append(table.column("SHAPE"))
    output = pa.Table.from_arrays(columns, names=["OBJECTID", *names, "geometry"])
    geo = {
        "version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB", "geometry_types": ["MultiPolygon"],
            "crs": pyproj.CRS.from_epsg(4269).to_json_dict(),
            "bbox": [xmin, ymin, xmax, ymax],
        }},
    }
    schema_metadata = dict(output.schema.metadata or {})
    schema_metadata[b"geo"] = _json_bytes(geo).strip()
    output = output.replace_schema_metadata(schema_metadata)
    sink = pa.BufferOutputStream()
    pq.write_table(output, sink, compression="zstd", version="2.6")
    parquet = sink.getvalue().to_pybytes()
    _assert_full_parity(table, ids, parquet)
    return parquet, {"field_names": output.column_names, "arrow_schema": str(output.schema), "geometry_bbox": [xmin, ymin, xmax, ymax]}


def _assert_full_parity(source: pa.Table, ids: list[int], parquet: bytes) -> None:
    output = pq.read_table(pa.BufferReader(parquet))
    if output.num_rows != EXPECTED_COUNT or output.column_names != EXPECTED_OUTPUT_FIELDS:
        raise ValueError("native_output_population_or_field_roster_drift")
    if output.column("OBJECTID").to_pylist() != ids:
        raise ValueError("native_output_id_parity_drift")
    for source_name, output_name in [(name, name) for name, _ in EXPECTED_SOURCE_FIELDS if name != "SHAPE"]:
        if output.column(source_name).to_pylist() != source.column(source_name).to_pylist():
            raise ValueError("native_output_attribute_parity_drift:" + source_name)
    if output.column("geometry").to_pylist() != source.column("SHAPE").to_pylist():
        raise ValueError("native_output_geometry_parity_drift")
    if len(set(output.column("OBJECTID").to_pylist())) != EXPECTED_COUNT:
        raise ValueError("native_output_duplicate_ids")


def _source_records_sha256(table: pa.Table, ids: list[int]) -> str:
    records = []
    columns = {name: table.column(name).to_pylist() for name, _ in EXPECTED_SOURCE_FIELDS}
    for index, identity in enumerate(ids):
        records.append({"OBJECTID": identity, **{name: (columns[name][index].hex() if name == "SHAPE" else columns[name][index]) for name, _ in EXPECTED_SOURCE_FIELDS}})
    return _sha256(_json_bytes(records))


def _packaged_code_sha256() -> str:
    root = Path(__file__).parents[1]
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        relative, body = path.relative_to(root).as_posix().encode(), path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big") + relative + len(body).to_bytes(8, "big") + body)
    return digest.hexdigest()


def _capture(
    staging,
    version: str,
    client: httpx.Client | None = None,
    *,
    retained_archive: bytes | None = None,
    retained_receipt: dict | None = None,
    retained_receipt_bytes: bytes | None = None,
) -> dict:
    owned = client is None
    client = client or httpx.Client(timeout=120, follow_redirects=True)
    try:
        if retained_archive is None:
            archive, receipt = _capture_archive(staging, version, client)
            replayed = False
        else:
            if retained_receipt is None or retained_receipt_bytes is None:
                raise ValueError("retained_capture_receipt_missing")
            archive, receipt = retained_archive, retained_receipt
            if _sha256(archive) != receipt.get("raw_sha256") or len(archive) != receipt.get("wire_bytes_retained"):
                raise ValueError("retained_capture_archive_receipt_mismatch")
            # Copy the original evidence bytes verbatim. This path is for a
            # local replay of an already observed transfer; it makes no new
            # network request and does not invent a retrieval timestamp.
            staging.write(DATASET_SLUG, FILE_SLUG, version, receipt["raw_archive_key"], archive)
            staging.write(DATASET_SLUG, FILE_SLUG, version, "raw/source_archive.receipt.json", retained_receipt_bytes)
            replayed = True
        _validate_archive(archive)
        with tempfile.TemporaryDirectory(prefix="hifld-school-districts-") as temporary:
            path = Path(temporary) / Path(SOURCE_URL).name
            path.write_bytes(archive)
            table, ids, source_meta = _native_table(path)
        parquet, output_schema = _build_geoparquet(table, ids)
        parquet_sha = _sha256(parquet)
        output_key = f"geoparquet/{FILE_SLUG}.parquet"
        staging.write(DATASET_SLUG, FILE_SLUG, version, output_key, parquet)
        ids_sha = _sha256(_json_bytes(ids))
        records_sha = _source_records_sha256(table, ids)
        source_schema = [{"name": name, "type": str(kind)} for name, kind in EXPECTED_SOURCE_FIELDS]
        archive_sha = receipt["raw_sha256"]
        retrieved_at = receipt["observed_at"]
        identity = processing_identity(Path(__file__).parents[3])
        try:
            validate_construction_code_identity(
                Path(__file__).parents[3], identity["code_commit"], identity["files"], identity=identity,
            )
        except Exception as exc:
            raise ValueError("source_code_identity_invalid:" + type(exc).__name__) from exc
        supplied_claim = os.environ.get("HIFLD_CODE_COMMIT") or os.environ.get("GITHUB_SHA")
        manifest = {
            "schema_version": 1,
            "title": "Secondary School Districts",
            "description": "Draft extraction of the complete 2024 Census TIGER/Line School_District_Secondary layer. It is a proposed source route for HIFLD inventory issue #307; historical HIFLD archive equivalence and release acceptance remain unresolved.",
            "publisher": "U.S. Census Bureau",
            "agency": "National Center for Education Statistics (HIFLD inventory attribution; source producer is U.S. Census Bureau)",
            "source_url": SOURCE_URL,
            "processor": "HIFLD Next",
            "code_commit": identity["code_commit"],
            "processing_identity": identity,
            "supplied_code_commit_claim": supplied_claim,
            "inputs": [{
                "input_id": "census_tigerline_2024_school_geodatabase",
                "producer": "U.S. Census Bureau",
                "host": SOURCE_HOST,
                "role": "producer_publication",
                "canonical_url": SOURCE_URL,
                "retrieval_url": receipt["final_url"],
                "retrieved_at": retrieved_at,
                "sha256": archive_sha,
                "bytes": receipt["wire_bytes_received"],
                "http_status": receipt["status"],
                "release_date_unknown_reason": "No explicit release date was established from the archive; the HTTP Last-Modified value is retained in the receipt and is not treated as a producer release date.",
                "license": "Not adjudicated; source reuse terms and rights require maintainer review before release.",
                "attribution": "U.S. Census Bureau, TIGER/Line 2024; historical HIFLD attribution and reuse terms remain under review.",
            }],
            "transformations": [
                {"name": "extract_geodatabase_layer", "parameters": {"layer": LAYER_NAME, "field_subset": "none", "geometry_simplification": "none", "reprojection": "none"}},
                {"name": "write_geoparquet", "parameters": {"compression": "zstd", "geometry_encoding": "WKB", "native_crs": "EPSG:4269"}},
            ],
            "lineage": "Direct extraction from the official U.S. Census Bureau TIGER/Line 2024 nationwide school geodatabase. This is a source route for the HIFLD inventory's named layer, not proof of byte/row equivalence with its DataLumos archive or DHS crosswalk entry.",
            "prebuilt_output_policy": {
                "schema_version": 1,
                "formats": {"geoparquet": {
                    "reason": "Source-faithful preservation of all 2024 native layer attributes, FIDs, WKB geometries, and EPSG:4269 coordinates in one pinned object; this is a draft conversion only.",
                    "objects": [{"path": output_key, "sha256": parquet_sha, "bytes": len(parquet)}],
                }},
            },
        }
        staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/source_manifest.json", _json_bytes(manifest))
        ids_rows = [{"OBJECTID": identity, "GEOID": table.column("GEOID")[index].as_py()} for index, identity in enumerate(ids)]
        capture_manifest = {
            "source_url": SOURCE_URL, "source_host": SOURCE_HOST,
            "archive_sha256": archive_sha, "archive_bytes": len(archive),
            "archive_receipt": "raw/source_archive.receipt.json",
            "archive_last_modified": receipt.get("last_modified"),
            "archive_members_crc_checked": True,
            "source_layer": LAYER_NAME, "source_layer_roster": source_meta["layers"],
            "feature_count": len(ids), "expected_feature_count": EXPECTED_COUNT,
            "page_count": 1, "page_mode": "single_complete_file_geodatabase_layer",
            "page": {"index": 1, "source_ids_sha256": ids_sha, "feature_count": len(ids)},
            "id_field": "OBJECTID (native GDB FID)", "ids_sha256": ids_sha,
            "record_sha256": records_sha, "ids_and_geoids": ids_rows,
            "source_schema": source_schema, "output_schema": output_schema,
            "native_geometry_type": source_meta["info"]["geometry_type"],
            "native_crs": "EPSG:4269", "native_crs_wkt": pyproj.CRS.from_epsg(4269).to_wkt(),
            "source_row_output_full_parity": True, "output_sha256": parquet_sha,
            "output_bytes": len(parquet), "output_key": output_key,
            "code_identity": {
                "processing_identity": identity,
                "capture_module_sha256": _sha256(Path(__file__).read_bytes()),
                "runtime_package_source_sha256": _packaged_code_sha256(),
                "python": identity["python"],
            },
            "coverage_statement": "Full 481-feature population of the named 2024 Census source layer. Equivalence to HIFLD archive coverage remains unverified.",
            "capture_replay": {
                "mode": "retained_transfer_replay" if replayed else "live_http_capture",
                "original_retrieved_at": retrieved_at,
                "new_network_retrieval_performed": not replayed,
            },
            "refresh_semantics": "Each live run fetches a fresh official archive; prior captured source bytes and receipts remain at the versioned raw path.",
            "release_accepted": False,
            "rights_contract_approved": False,
        }
        staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/native_capture.json", _json_bytes(capture_manifest))
        return {
            "feature_count": EXPECTED_COUNT, "ids_sha256": ids_sha,
            "records_sha256": records_sha, "source_archive_sha256": archive_sha,
            "parquet_sha256": parquet_sha, "release_accepted": False,
        }
    except Exception as exc:
        staging.write(DATASET_SLUG, FILE_SLUG, version, "metadata/quarantine.json", _json_bytes({
            "status": "quarantined", "source_url": SOURCE_URL,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "failure_code": str(exc).split(":", 1)[0],
            "release_accepted": False,
        }))
        raise
    finally:
        if owned:
            client.close()


def capture(staging, version: str, client: httpx.Client | None = None) -> dict:
    """Capture the bounded source archive and retain failure evidence."""
    return _capture(staging, version, client)


def replay_retained_capture(staging, version: str, archive: bytes, receipt_bytes: bytes) -> dict:
    """Reprocess one retained transfer while preserving its original receipt."""
    try:
        receipt = json.loads(receipt_bytes)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("retained_capture_receipt_invalid") from exc
    return _capture(
        staging, version, retained_archive=archive, retained_receipt=receipt,
        retained_receipt_bytes=receipt_bytes,
    )

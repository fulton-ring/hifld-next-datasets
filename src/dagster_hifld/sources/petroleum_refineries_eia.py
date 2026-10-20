"""Pinned historical adapter for EIA's Jan. 1, 2021 refinery point layer.

This adapter reconstructs the identified 2021 archive as a reproducible draft.
It is not a currentness claim: a different upstream ZIP is refused pending a
new release review, and every source/contract/rights/release approval stays off.
"""
from __future__ import annotations

import hashlib
import json
import stat
import tempfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from urllib.parse import urlsplit
from zipfile import BadZipFile, ZipFile

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, content_derived_version, processing_identity,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)

DATASET_SLUG = "petroleum-refineries"
FILE_SLUG = DATASET_SLUG
SOURCE_URL = "https://www.eia.gov/maps/map_data/Petroleum_Refineries_US_EIA.zip"
SOURCE_ZIP_SHA256 = "08243aed0c1f3d5206435d4932fb4e94ead8fe1b8e8d4220a03f601a460f4909"
SOURCE_ZIP_BYTES = 37784
SOURCE_LAST_MODIFIED = "Thu, 03 Feb 2022 19:32:04 GMT"
SOURCE_PERIOD = "As of Jan. 1 2021"
SOURCE_VINTAGE = "2021-01-01"
SOURCE_MEMBERS = {
    "Petroleum_Refineries_US_2021.cpg", "Petroleum_Refineries_US_2021.dbf",
    "Petroleum_Refineries_US_2021.prj", "Petroleum_Refineries_US_2021.sbn",
    "Petroleum_Refineries_US_2021.sbx", "Petroleum_Refineries_US_2021.shp",
    "Petroleum_Refineries_US_2021.shp.xml", "Petroleum_Refineries_US_2021.shx",
}
MAX_SOURCE_BYTES = 2_000_000
SOURCE_FIELDS = [
    ("OBJECTID", "OFTInteger64", pa.int64()), ("site_id", "OFTInteger64", pa.int64()),
    ("Company", "OFTString", pa.string()), ("Corp", "OFTString", pa.string()),
    ("Site", "OFTString", pa.string()), ("State", "OFTString", pa.string()),
    ("PADD", "OFTInteger64", pa.int64()), ("AD_Mbpd", "OFTReal", pa.float64()),
    ("VDist_Mbpd", "OFTReal", pa.float64()), ("CaDis_Mbpd", "OFTReal", pa.float64()),
    ("HyCrk_Mbpd", "OFTReal", pa.float64()), ("VRedu_Mbpd", "OFTInteger64", pa.int64()),
    ("CaRef_Mbpd", "OFTReal", pa.float64()), ("Isal_Mbpd", "OFTReal", pa.float64()),
    ("HDS_Mbpd", "OFTReal", pa.float64()), ("Cokin_Mbpd", "OFTReal", pa.float64()),
    ("Asph_Mbpd", "OFTReal", pa.float64()), ("Source", "OFTString", pa.string()),
    ("Period_", "OFTString", pa.string()), ("Latitude", "OFTReal", pa.float64()),
    ("Longitude", "OFTReal", pa.float64()), ("Link_Impor", "OFTInteger64", pa.int64()),
]


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json(data: object) -> bytes:
    return json.dumps(data, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode() + b"\n"


def _capture(directory: Path | None) -> tuple[bytes, dict, Path]:
    root = directory or Path("/tmp/hifld-native-reconstruction/268/captures/2021-eia")
    if root.is_symlink() or any(parent.is_symlink() for parent in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    root.mkdir(parents=True, exist_ok=True)
    body_path, wire_path, receipt_path = (root / "source_zip.body", root / "source_zip.wire", root / "source_zip.receipt.json")
    manifest_path = root / "capture_manifest.json"
    expected_names = {"source_zip.body", "source_zip.wire", "source_zip.receipt.json", "capture_manifest.json"}
    if manifest_path.exists():
        if ({path.name for path in root.iterdir()} != expected_names
                or any(path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode) for path in root.iterdir())):
            raise ValueError("retained_source_capture_roster_invalid")
        manifest = json.loads(manifest_path.read_bytes())
        receipt = json.loads(receipt_path.read_bytes())
        body, wire = body_path.read_bytes(), wire_path.read_bytes()
        if (manifest.get("complete") is not True or manifest.get("receipts") != {"source_zip": receipt}
                or receipt.get("method") != "GET" or receipt.get("url") != SOURCE_URL or receipt.get("final_url") != SOURCE_URL
                or receipt.get("status") != 200 or receipt.get("complete_body") is not True
                or receipt.get("bytes") != len(body) or receipt.get("wire_bytes") != len(wire)
                or receipt.get("last_modified") != SOURCE_LAST_MODIFIED or len(body) != SOURCE_ZIP_BYTES or body != wire
                or _sha(body) != SOURCE_ZIP_SHA256 or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_sha256") != _sha(wire)):
            raise ValueError("retained_source_capture_invalid")
        return body, receipt, root
    observed = datetime.now(timezone.utc).isoformat()
    wire_hash, body_hash = hashlib.sha256(), hashlib.sha256()
    sizes = {"wire": 0, "body": 0}

    def retain(path: Path, kind: str, digest, block: bytes) -> None:
        with path.open("ab") as stream:
            stream.write(block)
        sizes[kind] += len(block)
        digest.update(block)

    for path in (wire_path, body_path):
        path.unlink(missing_ok=True)
    result = None
    headers = {}
    final_url = SOURCE_URL
    error_code = None
    try:
        with httpx.Client(timeout=90, follow_redirects=False, headers={"Accept-Encoding": "identity"}) as client:
            with client.stream("GET", SOURCE_URL) as response:
                headers = dict(response.headers)
                final_url = str(response.url)
                result = capture_http_response(
                    response, max_wire_bytes=MAX_SOURCE_BYTES, max_decoded_bytes=MAX_SOURCE_BYTES,
                    reserve=lambda _progress: None,
                    retain_wire=lambda block: retain(wire_path, "wire", wire_hash, block),
                    retain_decoded=lambda block: retain(body_path, "body", body_hash, block),
                )
    except (httpx.HTTPError, OSError) as exc:
        error_code = type(exc).__name__
    body = body_path.read_bytes() if body_path.exists() else b""
    wire = wire_path.read_bytes() if wire_path.exists() else b""
    receipt = {"method": "GET", "url": SOURCE_URL, "final_url": final_url, "observed_at": observed,
        "status": result.status_code if result else None, "complete_body": bool(result and result.complete),
        "bytes": sizes["body"], "sha256": body_hash.hexdigest(), "wire_bytes": sizes["wire"],
        "wire_sha256": wire_hash.hexdigest(), "last_modified": headers.get("last-modified"),
        "content_length": result.content_length if result else None,
        "content_encoding": result.content_encoding if result else None,
        "error_code": error_code or (result.error_code if result else "capture_failed")}
    receipt_path.write_bytes(_json(receipt))
    manifest = {"complete": False, "captured_at": observed, "receipts": {"source_zip": receipt}}
    manifest_path.write_bytes(_json(manifest))
    if (result is None or not result.complete or result.status_code != 200 or final_url != SOURCE_URL
            or receipt["bytes"] != len(body) or receipt["wire_bytes"] != len(wire)
            or receipt["sha256"] != _sha(body) or receipt["wire_sha256"] != _sha(wire)):
        raise ValueError("source_capture_incomplete_review_required")
    if len(body) != SOURCE_ZIP_BYTES or _sha(body) != SOURCE_ZIP_SHA256:
        raise ValueError("source_archive_changed_new_release_review_required")
    if headers.get("last-modified") != SOURCE_LAST_MODIFIED:
        raise ValueError("source_last_modified_changed_new_release_review_required")
    manifest["complete"] = True
    manifest_path.write_bytes(_json(manifest))
    return body, receipt, root


def _read_archive(body: bytes) -> tuple[list[dict], dict]:
    if len(body) != SOURCE_ZIP_BYTES or _sha(body) != SOURCE_ZIP_SHA256:
        raise ValueError("source_archive_changed_new_release_review_required")
    try:
        archive = ZipFile(BytesIO(body))
    except BadZipFile as exc:
        raise ValueError("source_archive_invalid_zip") from exc
    members = [info.filename for info in archive.infolist()]
    if len(members) != len(set(members)) or set(members) != SOURCE_MEMBERS or any(".." in Path(name).parts or name.startswith(("/", "\\")) for name in members):
        raise ValueError("source_archive_member_roster_drift")
    with tempfile.TemporaryDirectory(prefix="hifld-refineries-") as tmp:
        root = Path(tmp)
        for name in members:
            (root / name).write_bytes(archive.read(name))
        shp = root / "Petroleum_Refineries_US_2021.shp"
        info = pyogrio.read_info(shp)
        fields, ogr_types = [field[0] for field in SOURCE_FIELDS], [field[1] for field in SOURCE_FIELDS]
        if (info["features"] != 130 or info["geometry_type"] != "Point" or info["crs"] != "EPSG:4326"
                or list(info["fields"]) != fields or info["ogr_types"] != ogr_types):
            raise ValueError("source_population_schema_or_crs_changed_new_release_review_required")
        frame = pyogrio.read_dataframe(shp, use_arrow=True)
    if len(frame) != 130 or frame["OBJECTID"].isna().any() or frame["site_id"].isna().any():
        raise ValueError("source_native_ids_missing")
    if frame["OBJECTID"].duplicated().any() or frame["site_id"].duplicated().any():
        raise ValueError("source_native_ids_duplicated")
    if set(frame["Period_"].dropna().unique()) != {SOURCE_PERIOD}:
        raise ValueError("source_period_changed_new_release_review_required")
    records = []
    for row in frame.itertuples(index=False, name=None):
        attrs, geom = row[:-1], row[-1]
        if geom is None or geom.geom_type != "Point":
            raise ValueError("source_geometry_invalid")
        records.append({**dict(zip(fields, attrs)), "geometry_wkb": bytes(geom.wkb)})
    schema = [{"name": name, "source_type": ogr, "parquet_type": str(arrow), "nullable": True} for name, ogr, arrow in SOURCE_FIELDS]
    record_hash = _sha(_json([{**row, "geometry_wkb": row["geometry_wkb"].hex()} for row in records]))
    ids_hash = _sha(_json([[row["OBJECTID"], row["site_id"]] for row in records]))
    audit = {"feature_count": 130, "source_vintage": SOURCE_VINTAGE, "source_period": SOURCE_PERIOD,
             "source_schema": schema, "source_schema_sha256": _sha(_json(schema)), "source_record_sha256": record_hash,
             "source_ids_sha256": ids_hash, "native_id_fields": ["OBJECTID", "site_id"],
             "source_crs": "EPSG:4326", "geometry_type": "Point", "null_geometry_count": 0,
             "invalid_geometry_count": 0, "historical_only": True, "currentness_accepted": False}
    return records, audit


def _parquet(records: list[dict]) -> bytes:
    fields = [pa.field(name, arrow, nullable=True) for name, _ogr, arrow in SOURCE_FIELDS]
    fields.append(pa.field("geometry", pa.binary(), nullable=False))
    columns = {name: pa.array([record[name] for record in records], type=arrow) for name, _ogr, arrow in SOURCE_FIELDS}
    columns["geometry"] = pa.array([record["geometry_wkb"] for record in records], type=pa.binary())
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": pyproj.CRS.from_epsg(4326).to_json_dict(), "edges": "planar"}}}
    table = pa.Table.from_pydict(columns, schema=pa.schema(fields, metadata={b"geo": _json(geo)}))
    sink = pa.BufferOutputStream(); pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def capture_petroleum_refineries_eia(storage: StagingStorageResource, *, capture_directory: str | None = None) -> dict:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    body, receipt, root = _capture(Path(capture_directory).absolute() if capture_directory else None)
    records, audit = _read_archive(body)
    identity = processing_identity(Path(__file__).parents[3]); identity["gdal"] = pyogrio.__gdal_version_string__
    parquet = _parquet(records)
    version = content_derived_version(audit["source_record_sha256"], audit["source_ids_sha256"], audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    manifest = {"schema_version": 1, "approved": False, "title": "Petroleum Refineries (historical EIA 2021)",
        "description": "Historical EIA point product as of January 1, 2021. This pinned archive does not represent a current release.",
        "publisher": "U.S. Energy Information Administration", "agency": "U.S. Department of Energy",
        "source_url": SOURCE_URL, "date_issued": None, "date_modified": None, "source_modified": SOURCE_LAST_MODIFIED,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"], "lineage": "Official EIA ZIP contains the complete Petroleum_Refineries_US_2021 shapefile, with 130 points as of Jan. 1, 2021. Current EIA capacity tables are nonspatial and are not substituted or joined. No maintained current point-layer release has been established.",
        "inputs": [{"input_id": "source_zip", "producer": "U.S. Energy Information Administration", "host": urlsplit(SOURCE_URL).hostname,
            "role": "producer_publication", "canonical_url": SOURCE_URL, "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
            "retrieved_at": receipt["observed_at"], "http_status": 200, "bytes": len(body), "sha256": _sha(body),
            "release_date": None, "release_date_unknown_reason": "EIA archive describes data as of Jan. 1, 2021; archive Last-Modified is not asserted to be a release date.",
            "license": "Rights review pending.", "attribution": "U.S. Energy Information Administration (EIA), Petroleum Refineries US EIA ZIP."}],
        "transformations": [{"operation": "HISTORICAL_EIA_SHAPEFILE_TO_NATIVE_GEOPARQUET", "parameters": {"source_vintage": SOURCE_VINTAGE,
            "attributes": "preserve all 22 original shapefile attributes and source values", "identifiers": "preserve OBJECTID and site_id; no generated ID",
            "geometry": "preserve source point WKB in EPSG:4326", "freshness": "historical only; no currentness claim"}}],
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Faithful historical candidate preserving every source field, ID and geometry.",
            "objects": [{"path": f"geoparquet/{DATASET_SLUG}.parquet", "bytes": len(parquet), "sha256": _sha(parquet)}]}}},
        "unsupported_output_formats": {"geopackage": "No approved publication contract.", "pmtiles": "No approved display contract."}}
    validate_release_provenance(manifest)
    audit.update({"publisher": "EIA", "source_zip_sha256": _sha(body), "source_zip_bytes": len(body), "source_last_modified": receipt["last_modified"],
        "historical_only": True, "currentness_accepted": False, "source_release_accepted": False, "freshness_accepted": False,
        "schema_contract_accepted": False, "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False,
        "mapping_approved": False, "release_accepted": False})
    outputs = {f"geoparquet/{DATASET_SLUG}.parquet": parquet, "metadata/source_manifest.json": _json(manifest),
        "metadata/source_audit.json": _json(audit), "metadata/processing_identity.json": _json(identity),
        "raw/source_zip.body": body, "raw/source_zip.wire": (root / "source_zip.wire").read_bytes(),
        "raw/source_zip.receipt.json": _json(receipt)}
    ready = {"approved": False, "historical_only": True, "version": version,
        "objects": {f"geoparquet/{DATASET_SLUG}.parquet": {"bytes": len(parquet), "sha256": _sha(parquet)}},
        "source_release_accepted": False, "freshness_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False, "rights_accepted": False, "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _json(ready)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    identity_fingerprint = _sha(_json(identity))
    expected_objects = {name: {"bytes": len(content), "sha256": _sha(content)} for name, content in outputs.items()}
    if storage.object_snapshot(state_key) is not None:
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
            identity_fingerprint, identity, audit["source_record_sha256"], audit["source_schema_sha256"],
            expected_objects, {"source_zip": receipt})
    else:
        for name, content in outputs.items():
            write_immutable(storage, f"{prefix}/{name}", content)
        seal_ready_candidate(storage, state_key, version, identity_fingerprint, {"processing_identity": identity,
            "source_record_sha256": audit["source_record_sha256"], "source_schema_sha256": audit["source_schema_sha256"],
            "capture_receipts": {"source_zip": receipt}, "objects": expected_objects})
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version, "feature_count": len(records),
        "source_population_complete": True, "source_vintage": SOURCE_VINTAGE, "historical_only": True,
        "source_ids_sha256": audit["source_ids_sha256"], "source_record_sha256": audit["source_record_sha256"],
        "source_capture_evidence_sha256": _sha(body), "candidate_ready": f"{prefix}/metadata/candidate_ready.json",
        "release_accepted": False, "freshness_accepted": False, "capture_directory": str(root)}

"""Draft USGS National Map Governmental Unit Boundaries source capture.

Raw ArcGIS PBF pages are retained as the source evidence. GeoParquet is a
derived draft view; it does not carry the complete native ArcGIS field
declarations or remove the source PBF's coordinate quantization.
"""
import hashlib
import json
import os
import platform
import shutil
import stat
import subprocess
import tempfile
import fcntl
from collections import defaultdict
from contextlib import contextmanager
from email.utils import parsedate_to_datetime
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlsplit

import httpx
from dagster import (
    AssetCheckResult, AssetKey, AssetSelection, Config, DefaultScheduleStatus,
    ScheduleDefinition, asset, asset_check, define_asset_job,
)

from dagster_hifld.esri_pbf import DraftGeoParquetWriter, decode_feature_collection_pbf
from dagster_hifld.http_capture import CaptureProgress, capture_http_response
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, content_derived_version, processing_identity,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.validation import sha256_file

DATASET_SLUG = "national-forests"
FILE_SLUG = "national-forests"
SERVICE_URL = "https://carto.nationalmap.gov/arcgis/rest/services/govunits/MapServer"
LAYER_ID = 31
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
PAGE_SIZE = 10
MAX_PAGE_BYTES = 24_000_000


class NationalForestsConfig(Config):
    capture_directory: str = ""


def _get(client: httpx.Client, url: str, capture_path: Path, params: dict[str, object] | None = None) -> tuple[bytes, dict[str, object]]:
    retained_wire: list[bytes] = []
    retained_body: list[bytes] = []
    with client.stream("GET", url, params=params) as response:
        headers = {k.lower(): v for k, v in response.headers.items()}
        capture = capture_http_response(
            response, max_wire_bytes=MAX_PAGE_BYTES, max_decoded_bytes=MAX_PAGE_BYTES,
            reserve=lambda progress: _reserve_page(progress),
            retain_wire=retained_wire.append, retain_decoded=retained_body.append,
        )
        wire = b"".join(retained_wire) + capture.pending_wire
        body = b"".join(retained_body) + capture.pending_decoded
        original_receipt = response.extensions.get("national_forests_original_receipt")
        observed_at = None
        if isinstance(original_receipt, dict):
            original_headers = original_receipt.get("headers", {})
            if isinstance(original_headers, dict) and isinstance(original_headers.get("date"), str):
                observed_at = parsedate_to_datetime(original_headers["date"]).astimezone(timezone.utc).isoformat()
        receipt = {
            "url": str(response.request.url), "final_url": str(response.url),
            "observed_at": observed_at or datetime.now(timezone.utc).isoformat(), "status": capture.status_code,
            "headers": headers, "content_encoding": capture.content_encoding,
            "wire_bytes": capture.wire_bytes_retained, "wire_sha256": capture.wire_sha256,
            "bytes": capture.decoded_bytes_retained, "sha256": capture.decoded_sha256,
            "complete_body": capture.complete, "complete": capture.complete, "error_code": capture.error_code,
        }
        if isinstance(original_receipt, dict):
            receipt["original_receipt"] = original_receipt
    capture_path.parent.mkdir(parents=True, exist_ok=True)
    capture_path.write_bytes(body)
    capture_path.with_name(capture_path.name + ".wire").write_bytes(wire)
    capture_path.with_name(capture_path.name + ".receipt.json").write_text(
        json.dumps(receipt, sort_keys=True, indent=2) + "\n"
    )
    if (not capture.complete or capture.status_code != 200 or capture.content_encoding not in ("", "identity")
            or capture.wire_sha256 != capture.decoded_sha256):
        raise RuntimeError(f"national_forests_source_http_capture_incomplete:{capture.error_code or 'response_validation'}")
    if (len(wire) != capture.wire_bytes_retained or hashlib.sha256(wire).hexdigest() != capture.wire_sha256
            or len(body) != capture.decoded_bytes_retained or hashlib.sha256(body).hexdigest() != capture.decoded_sha256
            or capture_path.read_bytes() != body
            or capture_path.with_name(capture_path.name + ".wire").read_bytes() != wire):
        raise RuntimeError("national_forests_source_capture_readback_mismatch")
    return body, receipt


def _reserve_page(progress: CaptureProgress) -> None:
    if progress.wire_bytes_received + progress.reserve_bytes > MAX_PAGE_BYTES:
        raise RuntimeError("national_forests_source_page_exceeds_limit")


def _json(client: httpx.Client, url: str, capture_path: Path, params: dict[str, object] | None = None) -> tuple[dict, bytes, dict[str, object]]:
    body, receipt = _get(client, url, capture_path, params)
    try:
        parsed = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("national_forests_source_json_invalid") from exc
    if not isinstance(parsed, dict) or "error" in parsed:
        raise RuntimeError("national_forests_source_json_error")
    return parsed, body, receipt


def _retained_capture(directory: Path, relative: str) -> tuple[bytes, bytes, dict[str, object]]:
    """Load one prior body/wire/receipt without changing its observed time."""
    body_path = directory / relative
    receipt_path = body_path.with_name(body_path.name.removesuffix(".body") + ".receipt.json")
    wire_path = body_path.with_name(body_path.name.removesuffix(".body") + ".wire")
    body, wire = body_path.read_bytes(), wire_path.read_bytes()
    original = json.loads(receipt_path.read_text())
    if (not body or type(original) is not dict or original.get("status") != 200
            or original.get("complete") is False or original.get("complete_body") is False
            or original.get("bytes", len(body)) != len(body)
            or (original.get("sha256", original.get("decoded_sha256")) is not None and
                original.get("sha256", original.get("decoded_sha256")) != hashlib.sha256(body).hexdigest())
            or original.get("wire_bytes", len(wire)) != len(wire)
            or original.get("wire_sha256", hashlib.sha256(wire).hexdigest()) != hashlib.sha256(wire).hexdigest()):
        raise ValueError("national_forests_retained_capture_integrity_failed")
    headers = original.get("headers", {})
    observed = original.get("observed_at") or original.get("retrieved_at")
    if not observed:
        date_header = headers.get("date") if isinstance(headers, dict) else None
        if not isinstance(date_header, str):
            raise ValueError("national_forests_retained_capture_timestamp_missing")
        observed = parsedate_to_datetime(date_header).astimezone(timezone.utc).isoformat()
    url = original.get("url")
    if not isinstance(url, str):
        raise ValueError("national_forests_retained_capture_url_missing")
    receipt = {
        "url": url, "final_url": original.get("final_url", url), "observed_at": observed,
        "status": 200, "complete_body": True, "bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(), "wire_bytes": len(wire),
        "wire_sha256": hashlib.sha256(wire).hexdigest(),
        "content_encoding": original.get("content_encoding", headers.get("content-encoding", "identity")),
        "original_receipt": original,
    }
    return body, wire, receipt


class _HttpSource:
    def __init__(self, client: httpx.Client, capture_root: Path):
        self.client, self.root = client, capture_root

    def json(self, name: str, url: str, params: dict[str, object]) -> tuple[dict, bytes, bytes, dict[str, object]]:
        body, receipt = _get(self.client, url, self.root / f"{name}.body", params)
        parsed = json.loads(body)
        if not isinstance(parsed, dict) or "error" in parsed:
            raise ValueError("national_forests_source_json_invalid")
        wire = (self.root / f"{name}.body.wire").read_bytes()
        return parsed, body, wire, receipt

    def pbf(self, name: str, ids: list[int]) -> tuple[bytes, bytes, dict[str, object]]:
        params = {"objectIds": ",".join(map(str, ids)), "outFields": "*", "returnGeometry": "true", "f": "pbf"}
        body, receipt = _get(self.client, f"{LAYER_URL}/query", self.root / f"{name}.body", params)
        return body, (self.root / f"{name}.body.wire").read_bytes(), receipt


class _RetainedSource:
    def __init__(self, root: Path):
        self.root = root

    def json(self, name: str, url: str, params: dict[str, object]) -> tuple[dict, bytes, bytes, dict[str, object]]:
        body, wire, receipt = _retained_capture(self.root / "fidelity", f"{name}.body")
        parsed = json.loads(body)
        if not isinstance(parsed, dict) or "error" in parsed:
            raise ValueError("national_forests_retained_source_json_invalid")
        if not receipt["url"].startswith(url):
            raise ValueError("national_forests_retained_source_url_mismatch")
        return parsed, body, wire, receipt

    def pbf(self, name: str, ids: list[int]) -> tuple[bytes, bytes, dict[str, object]]:
        body, wire, receipt = _retained_capture(self.root / "pbf-fidelity", f"{name}.body")
        if not receipt["url"].startswith(f"{LAYER_URL}/query?"):
            raise ValueError("national_forests_retained_pbf_url_mismatch")
        return body, wire, receipt


def _retained_transport(root: Path) -> httpx.MockTransport:
    """Serve the exact retained capture set, preserving its original receipts."""
    fidelity, pbf_root = root / "fidelity", root / "pbf-fidelity"
    metadata_names = {"service": "service", "layer": "layer", "count": "count", "ids": "ids"}
    seen: dict[str, int] = defaultdict(int)
    pbf_by_ids = {}
    for path in pbf_root.glob("page_*.body"):
        receipt = json.loads(path.with_suffix(".receipt.json").read_text())
        pbf_by_ids[receipt["params"]["objectIds"]] = path.stem
    if len(pbf_by_ids) != 66:
        raise ValueError("national_forests_retained_pbf_page_roster_invalid")

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = dict(request.url.params)
        if params.get("f") == "pbf":
            ids = params.get("objectIds", "")
            name = pbf_by_ids.get(ids)
            if name is None:
                raise ValueError("national_forests_retained_pbf_request_unknown")
            capture_path = pbf_root / f"{name}.body"
        else:
            if path.endswith("/MapServer"):
                base = "service"
            elif path.endswith("/MapServer/31"):
                base = "layer"
            elif path.endswith("/MapServer/31/query") and params.get("returnCountOnly") == "true":
                base = "count"
            elif path.endswith("/MapServer/31/query") and params.get("returnIdsOnly") == "true":
                base = "ids"
            else:
                raise ValueError("national_forests_retained_request_unknown")
            index = seen[base]
            seen[base] += 1
            name = f"{base}_after" if index else base
            capture_path = fidelity / f"{name}.body"
        body, wire, receipt = _retained_capture(capture_path.parent, capture_path.name)
        headers = receipt.get("original_receipt", {}).get("headers", {})
        response = httpx.Response(
            200, headers={str(k): str(v) for k, v in headers.items()},
            stream=httpx.ByteStream(wire), request=request,
        )
        response.extensions["national_forests_original_receipt"] = receipt.get("original_receipt", {})
        return response

    return httpx.MockTransport(handler)


def _source_key(storage: StagingStorageResource, version: str, *parts: str) -> str:
    return storage.build_target_location(DATASET_SLUG, FILE_SLUG, version, "/".join(parts))


@asset(
    key=AssetKey(["ingest", DATASET_SLUG, FILE_SLUG]),
    group_name="usgs",
    compute_kind="http+pbf",
    description=(
        "Draft capture of USGS The National Map Governmental Unit Boundaries layer 31, "
        "National Forest. Source feature lineage declares PAD-US 4.1 and Forest Service "
        "administration; raw PBF, source metadata, and the PBF transform are retained. "
        "GeoParquet is a derived quantized view pending acceptance."
    ),
)
def usgs_national_forests(context, config: NationalForestsConfig, staging_storage: StagingStorageResource) -> dict:
    result = capture_national_forests(staging_storage, capture_directory=config.capture_directory or None)
    context.instance.add_dynamic_partitions(
        PUBLISH_PARTITION_NAME,
        [build_publish_partition_key(DATASET_SLUG, FILE_SLUG, result["version"])],
    )
    context.add_output_metadata(result)
    return result


def capture_national_forests(storage: StagingStorageResource, *, capture_directory: str | None = None) -> dict:
    """Capture, validate, and stage a complete local draft candidate.

    Remote/GCS storage is deliberately refused until the source, candidate
    contract, and release have the project's separate approvals.
    """
    if storage.bucket or not storage.use_local or not storage.local_dir:
        raise ValueError("national_forests_draft_requires_explicit_local_staging")
    root = Path(storage.local_dir).resolve()
    # Build out of the discoverable candidate tree. The final prefix is
    # content-derived after source brackets, IDs, schema and code are known.
    base = Path(tempfile.mkdtemp(prefix="national-forests-build-", dir="/tmp"))
    capture_root = base / "metadata" / "source_capture"
    pbf_root = capture_root / "pbf"
    receipt_root = capture_root / "receipts"
    pbf_root.mkdir(parents=True, exist_ok=True)
    receipt_root.mkdir(parents=True, exist_ok=True)
    headers = {"Accept-Encoding": "identity"}
    timeout = httpx.Timeout(90, connect=20)
    provenance_inputs: list[dict[str, object]] = []

    def record_input(input_id: str, receipt: dict[str, object]) -> None:
        retrieval_url = str(receipt["url"])
        provenance_inputs.append({
            "input_id": input_id, "producer": "U.S. Geological Survey (USGS)",
            "host": urlsplit(retrieval_url).hostname, "role": "producer_publication",
            "canonical_url": LAYER_URL, "retrieval_url": retrieval_url,
            "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
            "bytes": receipt["bytes"], "sha256": receipt["sha256"],
            "license": "Not specified in captured source metadata; rights acceptance pending",
            "attribution": "USGS The National Map; attribution and rights acceptance pending",
            "release_date": None,
            "release_date_unknown_reason": "Captured service metadata does not establish a producer release date for this feature set.",
        })

    transport = _retained_transport(Path(capture_directory)) if capture_directory else None
    with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True, transport=transport) as client:
        service_before, service_body, service_receipt = _json(client, SERVICE_URL, capture_root / "service_before.json", {"f": "pjson"})
        layer_before, layer_body, layer_receipt = _json(client, LAYER_URL, capture_root / "layer_before.json", {"f": "pjson"})
        count_before, count_body, count_receipt = _json(client, f"{LAYER_URL}/query", capture_root / "count_before.json", {"where": "1=1", "returnCountOnly": "true", "f": "json"})
        ids_before, ids_body, ids_receipt = _json(client, f"{LAYER_URL}/query", capture_root / "ids_before.json", {"where": "1=1", "returnIdsOnly": "true", "f": "json"})
        ids = ids_before.get("objectIds")
        if not isinstance(ids, list) or len(ids) != count_before.get("count") or len(ids) != 653:
            raise RuntimeError("national_forests_source_population_unexpected")
        if len(set(ids)) != len(ids) or any(type(x) is not int for x in ids):
            raise RuntimeError("national_forests_source_ids_invalid")
        ids = sorted(ids)
        if (layer_before.get("name") != "National Forest" or layer_before.get("geometryType") != "esriGeometryPolygon"
                or not any(field.get("name") == "objectid" and field.get("type") == "esriFieldTypeOID"
                           for field in layer_before.get("fields", []) if isinstance(field, dict))):
            raise RuntimeError("national_forests_source_layer_identity_drift")
        native_fields = layer_before.get("fields")
        if not isinstance(native_fields, list) or not all(isinstance(x, dict) for x in native_fields):
            raise RuntimeError("national_forests_source_field_schema_invalid")
        expected_pbf_fields = [
            {"name": f["name"], "type": f["type"]}
            for f in native_fields if f.get("type") != "esriFieldTypeGeometry"
        ]
        if len(expected_pbf_fields) != 19:
            raise RuntimeError("national_forests_source_field_roster_unexpected")

        for name, body, receipt in (
            ("service_before.json", service_body, service_receipt),
            ("layer_before.json", layer_body, layer_receipt),
            ("count_before.json", count_body, count_receipt),
            ("ids_before.json", ids_body, ids_receipt),
        ):
            (capture_root / name).write_bytes(body)
            (receipt_root / f"{name}.receipt.json").write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n")
            record_input(name.removesuffix(".json"), receipt)

        parquet_path = base / "geoparquet" / "national-forests.parquet"
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        partial = parquet_path.with_suffix(".parquet.partial")
        page_files: list[Path] = []
        all_seen: list[int] = []
        page_transforms: list[dict[str, object]] = []
        parquet_writer = None
        pages = [ids[i:i + PAGE_SIZE] for i in range(0, len(ids), PAGE_SIZE)]
        try:
            parquet_writer = DraftGeoParquetWriter(partial, native_fields, layer_before.get("extent", {}).get("spatialReference", {"latestWkid": 3857}))
            for index, page_ids in enumerate(pages):
                params = {"objectIds": ",".join(map(str, page_ids)), "outFields": "*", "returnGeometry": "true", "f": "pbf"}
                body, receipt = _get(client, f"{LAYER_URL}/query", pbf_root / f"page_{index:03d}.pbf", params)
                decoded = decode_feature_collection_pbf(body)
                if decoded["fields"] != expected_pbf_fields:
                    raise RuntimeError("national_forests_pbf_field_schema_drift")
                if decoded["spatialReference"].get("latestWkid") != layer_before.get("extent", {}).get("spatialReference", {}).get("latestWkid", 3857):
                    raise RuntimeError("national_forests_pbf_spatial_reference_drift")
                source_sr = layer_before.get("sourceSpatialReference", {})
                xy_units = source_sr.get("xyUnits")
                transform = decoded.get("transform", {})
                scales = transform.get("scale", {})
                translates = transform.get("translate", {})
                if (type(xy_units) not in (int, float) or xy_units <= 0
                        or scales.get(1) != 1 / xy_units or scales.get(2) != 1 / xy_units
                        or not all(isinstance(translates.get(axis), (int, float)) for axis in (1, 2))):
                    raise RuntimeError("national_forests_pbf_transform_source_grid_drift")
                page_transforms.append(transform)
                found_ids = [feature["attributes"].get("objectid") for feature in decoded["features"]]
                if sorted(found_ids) != sorted(page_ids):
                    raise RuntimeError("national_forests_pbf_page_id_mismatch")
                for feature in decoded["features"]:
                    attributes = feature["attributes"]
                    if (attributes.get("source_originator") != "U.S. Geological Survey (USGS)"
                            or attributes.get("source_datadesc") != "PAD-US 4.1"
                            or attributes.get("ownerormanagingagency") != 11):
                        raise RuntimeError("national_forests_feature_lineage_drift")
                all_seen.extend(found_ids)
                page_path = pbf_root / f"page_{index:03d}.pbf"
                page_files.append(page_path)
                receipt.update({"requested_ids": page_ids, "returned_ids": found_ids, "complete": True})
                (receipt_root / f"page_{index:03d}.receipt.json").write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n")
                record_input(f"pbf_page_{index:03d}", receipt)
                parquet_writer.write_page(decoded)
            row_count, total_vertices = parquet_writer.close()
            parquet_writer = None
            if len(all_seen) != len(ids) or len(set(all_seen)) != len(ids) or set(all_seen) != set(ids):
                raise RuntimeError("national_forests_full_population_coverage_failed")

            layer_after, layer_after_body, layer_after_receipt = _json(client, LAYER_URL, capture_root / "layer_after.json", {"f": "pjson"})
            service_after, service_after_body, service_after_receipt = _json(client, SERVICE_URL, capture_root / "service_after.json", {"f": "pjson"})
            count_after, count_after_body, count_after_receipt = _json(client, f"{LAYER_URL}/query", capture_root / "count_after.json", {"where": "1=1", "returnCountOnly": "true", "f": "json"})
            ids_after, ids_after_body, ids_after_receipt = _json(client, f"{LAYER_URL}/query", capture_root / "ids_after.json", {"where": "1=1", "returnIdsOnly": "true", "f": "json"})
            if (layer_after.get("fields") != native_fields or layer_after.get("name") != layer_before.get("name")
                    or count_after.get("count") != len(ids) or set(ids_after.get("objectIds", [])) != set(ids)):
                raise RuntimeError("national_forests_source_changed_during_capture")
            for name, body, receipt in (
                ("service_after.json", service_after_body, service_after_receipt),
                ("layer_after.json", layer_after_body, layer_after_receipt),
                ("count_after.json", count_after_body, count_after_receipt),
                ("ids_after.json", ids_after_body, ids_after_receipt),
            ):
                (capture_root / name).write_bytes(body)
                (receipt_root / f"{name}.receipt.json").write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n")
                record_input(name.removesuffix(".json"), receipt)
            if row_count != len(ids):
                raise RuntimeError("national_forests_geoparquet_row_count_mismatch")
            os.replace(partial, parquet_path)
        except Exception:
            if parquet_writer is not None:
                parquet_writer.close()
            partial.unlink(missing_ok=True)
            raise

    schema_record = {
        "draft": True, "source": LAYER_URL, "source_product": "USGS Governmental Unit Boundaries, National Forest layer 31",
        "source_feature_lineage": "PAD-US 4.1; source_originator is U.S. Geological Survey (USGS); ownerormanagingagency is Forest Service",
        "native_geometry_type": layer_before.get("geometryType"),
        "native_spatial_reference": layer_before.get("extent", {}).get("spatialReference"),
        "source_xy_units": layer_before.get("sourceSpatialReference", {}).get("xyUnits"),
        "pbf_page_transforms": page_transforms,
        "pbf_transform_note": "Exact transform fields are captured from each PBF page; PBF query transport is quantized per the ArcGIS protocol.",
        "geoparquet_is_derived_view": True,
        "field_declarations": native_fields,
        "population_count": len(ids), "unique_objectid_count": len(set(all_seen)),
        "decoded_vertex_count": total_vertices,
        "objectids_sha256": hashlib.sha256(",".join(map(str, sorted(ids))).encode()).hexdigest(),
    }
    (capture_root / "draft_schema_and_coverage.json").write_text(json.dumps(schema_record, sort_keys=True, indent=2) + "\n")
    parquet_bytes = parquet_path.stat().st_size
    parquet_digest = hashlib.sha256()
    with parquet_path.open("rb") as parquet_file:
        for block in iter(lambda: parquet_file.read(1024 * 1024), b""):
            parquet_digest.update(block)
    parquet_sha256 = parquet_digest.hexdigest()
    repo_root = Path(__file__).parents[3]
    identity = processing_identity(repo_root)
    schema_digest = hashlib.sha256(json.dumps(native_fields, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    record_digest = hashlib.sha256(json.dumps(
        {"objectids": sorted(ids), "lineage": "PAD-US 4.1", "fields": native_fields},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    geometry_digest = hashlib.sha256(json.dumps(
        [{"receipt": x["sha256"], "transform": t} for x, t in zip(
            [json.loads((receipt_root / f"page_{i:03d}.receipt.json").read_text()) for i in range(len(page_files))],
            page_transforms)], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    version = content_derived_version(record_digest, geometry_digest, schema_digest, identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    source_manifest = {
        "schema_version": 1,
        "approved": False,
        "title": "National Forests (USGS The National Map layer 31)",
        "description": "Unapproved draft candidate. Native Esri PBF pages and field declarations are retained as source evidence.",
        "publisher": "U.S. Geological Survey",
        "agency": "USGS",
        "source_url": LAYER_URL,
        "lineage": "USGS Governmental Unit Boundaries service; feature source fields identify PAD-US 4.1 and USGS; ownerormanagingagency identifies Forest Service.",
        "processor": "HIFLD Next",
        "code_commit": identity["code_commit"],
        "inputs": provenance_inputs,
        "transformations": ["Decoded official ArcGIS PBF quantization transform to WKB GeoParquet; no simplification or geometry repair."],
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Draft decoded representation with raw PBF inputs and exact source field declarations retained; quantization and field representation remain unapproved.",
            "objects": [{"path": "geoparquet/national-forests.parquet", "bytes": parquet_bytes, "sha256": parquet_sha256}],
        }}},
    }
    from dagster_hifld.release_approval import validate_release_provenance
    validate_release_provenance(source_manifest)
    (base / "metadata" / "source_manifest.json").write_text(json.dumps(source_manifest, sort_keys=True, indent=2) + "\n")
    # Publish every draft object immutably, retaining exact source response
    # bodies, wire bytes and original acquisition receipts under raw/.
    object_bytes: dict[str, bytes] = {}
    receipts: dict[str, dict[str, object]] = {}
    for item in provenance_inputs:
        name = str(item["input_id"])
        source_path = (pbf_root / f"page_{name.removeprefix('pbf_page_')}.pbf") if name.startswith("pbf_page_") else capture_root / f"{name}.json"
        receipt_path = receipt_root / (f"{source_path.stem}.receipt.json" if name.startswith("pbf_page_") else f"{source_path.name}.receipt.json")
        if not receipt_path.exists():
            receipt_path = receipt_root / f"{name}.receipt.json"
        receipt = json.loads(receipt_path.read_text())
        wire = source_path.with_name(source_path.name + ".wire").read_bytes()
        body = source_path.read_bytes()
        standardized = {"status": receipt["status"], "complete_body": receipt.get("complete_body", receipt.get("complete")) is True,
                        "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                        "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest(),
                        "observed_at": receipt["observed_at"], "original_receipt": receipt}
        receipts[name] = standardized
        object_bytes[f"raw/{name}.body"] = body
        object_bytes[f"raw/{name}.wire"] = wire
        object_bytes[f"raw/{name}.receipt.json"] = (json.dumps(standardized, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
    for path in base.rglob("*"):
        if path.is_file():
            object_bytes[path.relative_to(base).as_posix()] = path.read_bytes()
    expected_objects = {name: {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()} for name, body in object_bytes.items()}
    existing = storage.object_snapshot(state_key)
    if existing is not None:
        verify_cached_candidate(storage, repo_root, state_key, prefix, version, fingerprint, identity,
                                record_digest, schema_digest, expected_objects, receipts)
        shutil.rmtree(base, ignore_errors=True)
        return {"version": version, "feature_count": len(ids), "vertex_count": total_vertices,
                "raw_pbf_pages": len(page_files), "draft_geoparquet": storage.build_target_location(DATASET_SLUG, FILE_SLUG, version, "geoparquet/national-forests.parquet"), "source_brackets_stable": True, "approved_for_release": False, "reused": True}
    for name, body in object_bytes.items():
        write_immutable(storage, f"{prefix}/{name}", body)
    seal_ready_candidate(storage, state_key, version, fingerprint, {
        "processing_identity": identity, "source_record_sha256": record_digest,
        "source_schema_sha256": schema_digest, "capture_receipts": receipts, "objects": expected_objects,
    })
    validate_construction_code_identity(repo_root, identity["code_commit"], identity["files"])
    verify_cached_candidate(storage, repo_root, state_key, prefix, version, fingerprint, identity,
                            record_digest, schema_digest, expected_objects, receipts)
    shutil.rmtree(base, ignore_errors=True)
    return {"version": version, "feature_count": len(ids), "vertex_count": total_vertices,
            "raw_pbf_pages": len(page_files), "source_brackets_stable": True,
            "draft_geoparquet": storage.build_target_location(DATASET_SLUG, FILE_SLUG, version, "geoparquet/national-forests.parquet"), "approved_for_release": False}


@asset_check(asset=usgs_national_forests, blocking=True)
def usgs_national_forests_complete(candidate: dict) -> AssetCheckResult:
    passed = candidate.get("feature_count") == 653 and candidate.get("raw_pbf_pages") == 66
    return AssetCheckResult(passed=passed, metadata={
        "feature_count": candidate.get("feature_count", 0), "complete_population": passed,
        "source_drift_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "quantization_accepted": False,
        "rights_accepted": False, "release_accepted": False,
    })


@asset_check(asset=usgs_national_forests, blocking=True)
def usgs_national_forests_source_drift(candidate: dict) -> AssetCheckResult:
    stable = candidate.get("source_brackets_stable") is True
    return AssetCheckResult(passed=stable,
                            metadata={"source_brackets_stable": stable, "drift_gate_accepted": False, "release_accepted": False})


@asset_check(asset=usgs_national_forests, blocking=True)
def usgs_national_forests_release_refusal(candidate: dict) -> AssetCheckResult:
    return AssetCheckResult(passed=candidate.get("approved_for_release") is False,
                            metadata={"release_refused": candidate.get("approved_for_release") is False})


national_forests_draft_job = define_asset_job(
    "national_forests_draft_refresh", selection=AssetSelection.assets(usgs_national_forests),
)
national_forests_draft_schedule = ScheduleDefinition(
    name="national_forests_draft_refresh_schedule", job=national_forests_draft_job,
    cron_schedule="0 8 1 * *", execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.STOPPED,
)

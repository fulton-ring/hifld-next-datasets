"""Current GCCA warehouse-directory candidate for the HIFLD warehouse scope.

This is an explicitly changed successor candidate, not the inaccessible 2017
HIFLD archive. The complete current GCCA Warehouse Facility result set is
captured first; the U.S. projection is then derived from its native country
field. Raw FUSE response bodies and wire bytes are retained for every page.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import shutil
import struct
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, seal_first_construction
from dagster_hifld.http_capture import capture_http_response, capture_roster_digests, validate_capture_roster
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

DATASET_SLUG = FILE_SLUG = "public-refrigerated-warehouses-current-gcca"
DIRECTORY_URL = "https://www.gcca.org/directory/"
VIEW_URL = "https://fuse.fusesearch.app/api/1/views/gccageo.js?element_id=sjview_gccageo"
SERIES_URL = "https://fuse.fusesearch.app/api/1/series"
BASE_QUERY = r"sid:gcca\salesforce\directory iscompanypagevisible=true"
WAREHOUSE_QUERY = BASE_QUERY + ' custom_company_type="Warehouse Facility"'
PAGE_SIZE = 40
MAX_TOTAL = 20_000
MAX_PAGE_BYTES = 4_000_000
EXPECTED_FIELDS = ("city", "companyname", "country", "identifier", "location_geo", "parentid", "state")
WGS84 = CRS.from_epsg(4326)
_UNSAFE_KEY_RE = re.compile(r'(?i)(\bapi_key\s*:\s*["\'])[^"\']+(["\'])')


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _point_wkb(coordinates) -> bytes:
    return struct.pack("<BIdd", 1, 1, float(coordinates[0]), float(coordinates[1]))


def _strict_json(raw: bytes):
    def members(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError("source_duplicate_json_member")
            out[key] = value
        return out
    def invalid(value):
        raise ValueError("source_non_json_number")
    return json.loads(raw, object_pairs_hook=members, parse_constant=invalid)


def _decode_page(body: bytes) -> tuple[dict, list[dict]]:
    if len(body) < 8 or body[:4] != b"SJTS":
        raise ValueError("fuse_wire_magic_invalid")
    metadata_size = struct.unpack("<I", body[4:8])[0]
    cursor = 8 + metadata_size
    if cursor > len(body):
        raise ValueError("fuse_metadata_truncated")
    metadata = _strict_json(body[8:cursor])
    rows = []
    while cursor < len(body):
        if cursor + 4 > len(body):
            raise ValueError("fuse_frame_prefix_truncated")
        size = struct.unpack("<I", body[cursor:cursor + 4])[0]
        cursor += 4
        if size == 0 or cursor + size > len(body):
            raise ValueError("fuse_frame_body_truncated")
        row = _strict_json(body[cursor:cursor + size])
        cursor += size
        if not isinstance(row, dict) or not isinstance(row.get("fields"), dict):
            raise ValueError("fuse_record_shape_invalid")
        rows.append(row)
    if metadata.get("success") is not True or metadata.get("error") or metadata.get("validate_query", {}).get("type") != "structured":
        raise ValueError("fuse_search_rejected")
    if metadata.get("series") != len(rows):
        raise ValueError("fuse_series_count_mismatch")
    return metadata, rows


def _safe_capture_root(root: Path) -> None:
    if root.is_symlink() or any(parent.is_symlink() for parent in root.parents):
        raise ValueError("unsafe_capture_directory")


def _retain_response(client: httpx.Client, url: str, *, headers: dict[str, str],
                     params: dict[str, str] | None, root: Path, name: str,
                     max_bytes: int) -> tuple[bytes, bytes, dict[str, object]]:
    """Save decoded response and exact wire bytes with a credential-free receipt."""
    body_path, wire_path = root / f"{name}.body", root / f"{name}.wire"
    receipt_path = root / f"{name}.receipt.json"
    for path in (body_path, wire_path, receipt_path):
        if path.exists() or path.is_symlink():
            raise ValueError("capture_artifact_already_exists")
    body_hash, wire_hash = hashlib.sha256(), hashlib.sha256()
    body_bytes = wire_bytes = 0
    observed = datetime.now(timezone.utc).isoformat()

    def sink(path: Path, digest, data: bytes):
        nonlocal body_bytes, wire_bytes
        with path.open("ab") as stream:
            stream.write(data)
        digest.update(data)
        if path == body_path:
            body_bytes += len(data)
        else:
            wire_bytes += len(data)

    try:
        with client.stream("GET", url, params=params, headers={**headers, "Accept-Encoding": "identity"}) as response:
            result = capture_http_response(
                response, max_wire_bytes=max_bytes, max_decoded_bytes=max_bytes,
                reserve=lambda progress: None,
                retain_wire=lambda data: sink(wire_path, wire_hash, data),
                retain_decoded=lambda data: sink(body_path, body_hash, data),
            )
            final_url = str(response.url)
            status = response.status_code
            request_url = str(response.request.url)
    except httpx.HTTPError as exc:
        body_path.touch(exist_ok=True)
        wire_path.touch(exist_ok=True)
        receipt = {"method": "GET", "url": url, "final_url": None, "observed_at": observed,
                   "status": None, "complete_body": False, "bytes": body_bytes,
                   "sha256": body_hash.hexdigest(), "wire_bytes": wire_bytes,
                   "wire_sha256": wire_hash.hexdigest(), "error_type": type(exc).__name__}
        receipt_path.write_bytes(_canonical(receipt) + b"\n")
        raise ValueError("source_transport_failed:" + name) from None
    body_path.touch(exist_ok=True)
    wire_path.touch(exist_ok=True)
    receipt = {"method": "GET", "url": request_url, "final_url": final_url,
               "observed_at": observed, "status": status, "complete_body": result.complete,
               "bytes": body_bytes, "sha256": body_hash.hexdigest(),
               "wire_bytes": wire_bytes, "wire_sha256": wire_hash.hexdigest(),
               "content_encoding": result.content_encoding, "error_code": result.error_code}
    receipt_path.write_bytes(_canonical(receipt) + b"\n")
    body, wire = body_path.read_bytes(), wire_path.read_bytes()
    if (not result.complete or status != 200 or final_url != request_url
            or len(body) != body_bytes or _sha(body) != receipt["sha256"]
            or len(wire) != wire_bytes or _sha(wire) != receipt["wire_sha256"]):
        raise ValueError("source_response_incomplete:" + name)
    return body, wire, receipt


def _view_credentials(client: httpx.Client, root: Path) -> tuple[str, str, dict[str, object]]:
    """Fetch the public embed config; persist a redacted body and receipt."""
    response = client.get(VIEW_URL, headers={"Referer": DIRECTORY_URL, "Accept-Encoding": "identity"})
    if response.status_code != 200 or str(response.url) != VIEW_URL or len(response.content) > MAX_PAGE_BYTES:
        raise ValueError("gcca_public_embed_config_unavailable")
    text = response.text
    user = re.search(r'\buser\s*:\s*["\']([^"\']+)', text)
    key = re.search(r'\bapi_key\s*:\s*["\']([^"\']+)', text)
    view = re.search(r'\bvar\s+viewId\s*=\s*["\']([^"\']+)', text)
    if not user or not key or user.group(1) != "api.gcca" or not view or view.group(1) != "gccageo":
        raise ValueError("gcca_public_embed_identity_drift")
    redacted = _UNSAFE_KEY_RE.sub(r"\1[REDACTED]\2", text).encode()
    (root / "view-config.body").write_bytes(redacted)
    # The configuration contains a public embed key. Keep only a redacted copy;
    # the unredacted HTTP response stays in memory and is never written.
    receipt = {"method": "GET", "url": VIEW_URL, "final_url": VIEW_URL,
               "observed_at": datetime.now(timezone.utc).isoformat(), "status": 200,
               "complete_body": True, "bytes": len(redacted), "sha256": _sha(redacted),
               "wire_bytes": len(redacted), "wire_sha256": _sha(redacted),
               "credential_redaction": "api_key value removed before durable retention"}
    (root / "view-config.wire").write_bytes(redacted)
    (root / "view-config.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    return user.group(1), key.group(1), receipt


def _series_params(page: int) -> dict[str, str]:
    return {
        "scorefunction": "gcca", "max_facet_values": "1000", "per_page": str(PAGE_SIZE),
        "sort": "companyname asc, address asc, address_line_2 asc, city asc, state asc, postalcode asc, country asc",
        "default_fields": "companyname^10,description,address,address_line_2,custom_company_type,website,main_contact_firstname,main_contact_lastname,postal_code,value_added_services",
        "page": str(page), "facets": "custom_company_type,category,custom_value_added_services,country,state,doc_type",
        "fields": "location_geo,companyname,websiteurl,address,address_line_2,city,state,postalcode,country,identifier,parentid,eep_certification",
        "query": WAREHOUSE_QUERY, "uid": "hifld-next-datasets-draft",
        "location_type": "sjviews", "location": "gccageo",
    }


def _validate_and_project(rows: list[dict], source_total: int, facet_meta: dict) -> tuple[list[dict], dict]:
    if source_total != len(rows) or not 0 < source_total <= MAX_TOTAL:
        raise ValueError("warehouse_category_population_incomplete")
    ids, identifiers = [], []
    candidate = []
    field_names = set()
    fieldsets: dict[str, int] = {}
    geometry_null = geometry_invalid = 0
    for row in rows:
        series_id, fields = row.get("series_id"), row["fields"]
        if not isinstance(series_id, str) or not isinstance(fields, dict):
            raise ValueError("warehouse_record_identifier_missing")
        if not set(fields) <= set(EXPECTED_FIELDS) or not {"companyname", "country", "identifier"} <= set(fields):
            raise ValueError("warehouse_source_schema_drift")
        fieldset = ",".join(sorted(fields))
        fieldsets[fieldset] = fieldsets.get(fieldset, 0) + 1
        if not isinstance(fields.get("identifier"), str) or not fields["identifier"]:
            raise ValueError("warehouse_source_identifier_invalid")
        if series_id.rsplit("\\", 1)[-1] != fields["identifier"]:
            raise ValueError("warehouse_series_id_field_id_mismatch")
        for name, value in fields.items():
            if name != "location_geo" and value is not None and not isinstance(value, str):
                raise ValueError("warehouse_source_attribute_type_drift:" + name)
        ids.append(str(series_id)); identifiers.append(fields["identifier"])
        field_names.update(fields)
        if fields.get("country") == "United States":
            geo = fields.get("location_geo")
            if geo is None:
                geometry_null += 1
            elif (not isinstance(geo, dict) or geo.get("type") != "Point"
                  or not isinstance(geo.get("coordinates"), list) or len(geo["coordinates"]) != 2
                  or any(type(value) not in (int, float) or not math.isfinite(value) for value in geo["coordinates"])):
                geometry_invalid += 1
                raise ValueError("warehouse_native_geometry_invalid")
            candidate.append(row)
    if len(set(ids)) != len(ids) or len(set(identifiers)) != len(identifiers):
        raise ValueError("warehouse_source_ids_not_unique")
    country_facet = facet_meta.get("facets", {}).get("country", {})
    us_counts = [item.get("count") for item in country_facet.get("terms", [])
                 if item.get("term") == "United States"]
    if len(us_counts) != 1 or us_counts[0] != len(candidate):
        raise ValueError("warehouse_us_projection_does_not_match_source_facet")
    ids_sha = _sha(_canonical(sorted(ids)))
    candidate_ids = sorted(str(row["series_id"]) for row in candidate)
    audit = {
        "source_title": "GCCA Current Warehouse Facility Directory",
        "source_category_query": WAREHOUSE_QUERY,
        "source_population_count": len(rows), "source_population_complete": True,
        "source_series_id_sha256": ids_sha, "source_series_id_unique": True,
        "source_identifier_unique": True, "source_fields": sorted(field_names),
        "source_fieldset_counts": dict(sorted(fieldsets.items())),
        "source_schema_sha256": _sha(_canonical({"fields": sorted(field_names), "fieldset_counts": dict(sorted(fieldsets.items()))})),
        "us_projection_count": len(candidate), "us_projection_id_sha256": _sha(_canonical(candidate_ids)),
        "us_projection_rule": "Native country field exactly equals 'United States'; candidate rows retained without geometry repair or exclusion.",
        "us_geometry_null_count": geometry_null, "us_geometry_invalid_count": geometry_invalid,
        "source_coverage_scope": "Complete current GCCA Warehouse Facility category, followed by explicit United States country projection. Not the 2017 HIFLD edition and not an accepted historical equivalent.",
        "source_release_accepted": False, "source_freshness_accepted": False,
        "historical_2017_baseline_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "coverage_accepted": False,
        "rights_accepted": False, "release_accepted": False,
    }
    return candidate, audit


def _geo_parquet(rows: list[dict]) -> bytes:
    fields = [row["fields"] for row in rows]
    arrays = []
    schema_fields = []
    for name in EXPECTED_FIELDS:
        if name == "location_geo":
            values = [None if item.get(name) is None else _canonical(item[name]).decode() for item in fields]
            typ = pa.string()
        else:
            values = [item.get(name) for item in fields]
            typ = pa.string()
        arrays.append(pa.array(values, type=typ, safe=True))
        schema_fields.append(pa.field(name, typ, nullable=True))
    geometries = []
    for item in fields:
        geo = item.get("location_geo")
        geometries.append(None if geo is None else _point_wkb(geo["coordinates"]))
    arrays.append(pa.array(geometries, type=pa.binary(), safe=True))
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "crs": WGS84.to_json_dict()}}}
    sink = __import__("io").BytesIO()
    table = pa.Table.from_arrays(arrays, schema=pa.schema(schema_fields, metadata={b"geo": _canonical(geo)}))
    pq.write_table(table, sink, compression="zstd", use_dictionary=False)
    readback = pq.read_table(pa.BufferReader(sink.getvalue()))
    if readback.num_rows != len(rows) or readback.schema.names != list(EXPECTED_FIELDS) + ["geometry"]:
        raise ValueError("warehouse_candidate_readback_schema_drift")
    for column, expected in zip(EXPECTED_FIELDS, arrays):
        if readback[column].to_pylist() != expected.to_pylist():
            raise ValueError("warehouse_candidate_attribute_readback_drift:" + column)
    if readback["geometry"].to_pylist() != geometries:
        raise ValueError("warehouse_candidate_geometry_readback_drift")
    return sink.getvalue()


def _capture_live(root: Path) -> tuple[list[dict], int, dict, dict[str, dict[str, object]]]:
    if root.exists():
        if not root.is_dir() or any(root.iterdir()):
            raise ValueError("unsealed_capture_attempt_exists")
    else:
        root.mkdir(parents=True, exist_ok=False)
    captures: dict[str, dict[str, object]] = {}
    with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(45, connect=15),
                      headers={"User-Agent": "hifld-next-datasets/gcca-warehouse-draft"}) as client:
        user, key, view_receipt = _view_credentials(client, root)
        captures["view-config"] = view_receipt
        auth = "Basic " + base64.b64encode(f"{user}:{key}".encode()).decode()
        headers = {"sj-auth": auth, "sj-receive-format": "sjts", "Origin": "https://www.gcca.org",
                   "Referer": DIRECTORY_URL}
        rows, total, first_meta = [], None, None
        page = 1
        while True:
            name = f"series-{page:04d}"
            body, wire, receipt = _retain_response(client, SERIES_URL, headers=headers,
                params=_series_params(page), root=root, name=name, max_bytes=MAX_PAGE_BYTES)
            captures[name] = receipt
            meta, page_rows = _decode_page(body)
            page_total = meta.get("total")
            if type(page_total) is not int or page_total <= 0 or page_total > MAX_TOTAL:
                raise ValueError("warehouse_source_total_invalid")
            if total is None:
                total, first_meta = page_total, meta
            elif page_total != total:
                raise ValueError("warehouse_source_total_changed_during_capture")
            expected_rows = min(PAGE_SIZE, total - len(rows))
            if len(page_rows) != expected_rows:
                raise ValueError("warehouse_source_page_population_incomplete")
            rows.extend(page_rows)
            if len(rows) == total:
                break
            if len(rows) > total:
                raise ValueError("warehouse_source_population_exceeded_total")
            page += 1
        manifest = {"complete": True, "source_url": DIRECTORY_URL, "category_query": WAREHOUSE_QUERY,
                    "page_size": PAGE_SIZE, "advertised_total": total, "captured_rows": len(rows),
                    "captured_pages": page, "receipts": captures,
                    "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}
        (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
    return rows, total, first_meta, captures


def _load_replay(root: Path):
    manifest_path = root / "capture_manifest.json"
    if root.is_symlink() or not manifest_path.is_file():
        raise ValueError("warehouse_replay_manifest_missing")
    manifest = json.loads(manifest_path.read_bytes())
    roster = manifest.get("artifact_roster")
    validate_capture_roster(root, set(roster) | {"capture_manifest.json"})
    if capture_roster_digests(root, exclude={"capture_manifest.json"}) != roster:
        raise ValueError("warehouse_replay_roster_digest_drift")
    receipts = manifest.get("receipts")
    if not isinstance(receipts, dict) or "view-config" not in receipts:
        raise ValueError("warehouse_replay_receipt_roster_invalid")
    rows, total, first_meta = [], manifest.get("advertised_total"), None
    pages = manifest.get("captured_pages")
    if type(pages) is not int or pages < 1 or pages > MAX_TOTAL // PAGE_SIZE + 1:
        raise ValueError("warehouse_replay_page_count_invalid")
    expected_names = {"view-config"} | {f"series-{i:04d}" for i in range(1, pages + 1)}
    if set(receipts) != expected_names:
        raise ValueError("warehouse_replay_receipt_roster_drift")
    for page in range(1, pages + 1):
        name = f"series-{page:04d}"
        body = (root / f"{name}.body").read_bytes()
        wire = (root / f"{name}.wire").read_bytes()
        receipt = json.loads((root / f"{name}.receipt.json").read_bytes())
        if receipt != receipts[name] or receipt.get("status") != 200 or receipt.get("complete_body") is not True or receipt.get("final_url") != receipt.get("url") or len(body) != receipt.get("bytes") or _sha(body) != receipt.get("sha256") or len(wire) != receipt.get("wire_bytes") or _sha(wire) != receipt.get("wire_sha256"):
            raise ValueError("warehouse_replay_response_receipt_invalid:" + name)
        meta, page_rows = _decode_page(body)
        if meta.get("total") != total or len(page_rows) != min(PAGE_SIZE, total - len(rows)):
            raise ValueError("warehouse_replay_population_drift")
        first_meta = first_meta or meta
        rows.extend(page_rows)
    if len(rows) != total or manifest.get("captured_rows") != len(rows):
        raise ValueError("warehouse_replay_population_incomplete")
    return rows, total, first_meta, receipts


def capture_gcca_warehouses(storage: StagingStorageResource, *, version: str,
                            capture_directory: str | None = None) -> dict[str, object]:
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_isolated_local_staging_required")
    if not isinstance(version, str) or not version or Path(version).name != version or version in {".", ".."} or "/" in version or "\\" in version:
        raise ValueError("unsafe_candidate_version")
    stage = Path(storage.local_dir).absolute()
    if ".." in stage.parts or any(path.is_symlink() for path in (stage, *stage.parents)):
        raise ValueError("unsafe_local_staging_root")
    stage.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(stage).free < 300_000_000:
        raise ValueError("disk_floor_before_acquisition")
    requested = Path(capture_directory).absolute() if capture_directory else None
    replay = bool(requested and (requested / "capture_manifest.json").exists())
    root = requested if requested else Path("/tmp/hifld-native-reconstruction/280") / (version + "-" + uuid4().hex)
    _safe_capture_root(root)
    if replay:
        rows, total, facet_meta, receipts = _load_replay(root)
    else:
        if requested and any(requested.iterdir()):
            raise ValueError("unsealed_capture_attempt_exists")
        rows, total, facet_meta, receipts = _capture_live(root)
    candidate, audit = _validate_and_project(rows, total, facet_meta)
    identity_root = Path(__file__).parents[3]
    from dagster_hifld.sources.epa_frs_common import processing_identity, write_immutable
    identity = processing_identity(identity_root)
    parquet = _geo_parquet(candidate)
    audit["source_capture_evidence_sha256"] = _sha(_canonical({k: v for k, v in receipts.items()}))
    audit["processing_code_identity"] = identity["code_commit"]
    series_names = sorted(name for name in receipts if name.startswith("series-"))
    raw_body = b"".join((root / f"{name}.body").read_bytes() for name in series_names)
    first_receipt = receipts[series_names[0]]
    retrieval_url = first_receipt["url"]
    source_input = {
        "input_id": "gcca_public_directory_warehouse_facilities",
        "producer": "Global Cold Chain Alliance", "host": "fuse.fusesearch.app",
        "role": "producer_publication", "canonical_url": DIRECTORY_URL,
        "retrieval_url": retrieval_url, "retrieved_at": first_receipt["observed_at"],
        "sha256": _sha(raw_body), "bytes": len(raw_body),
        "source_population_count": total, "source_category_query": WAREHOUSE_QUERY,
        "source_capture_evidence_sha256": audit["source_capture_evidence_sha256"],
        "http_status": 200, "release_date": None,
        "release_date_unknown_reason": "The live directory exposes no immutable producer release identifier or issue date.",
        "license": "Reuse terms not accepted", "attribution": "GCCA directory; acceptance pending",
    }
    manifest = {
        "schema_version": 1, "approved": False,
        "title": "Public Refrigerated Warehouses (current GCCA changed-successor candidate)",
        "description": "Current GCCA Warehouse Facility directory, projected to records with native country exactly United States. This is not the inaccessible 2017 HIFLD edition; continuity, currentness, coverage equivalence, rights, and release remain unapproved.",
        "publisher": "Global Cold Chain Alliance", "agency": "Global Cold Chain Alliance",
        "source_url": DIRECTORY_URL, "source_modified": None, "date_issued": None,
        "date_modified": datetime.now(timezone.utc).date().isoformat(),
        "processor": "HIFLD Next", "processing_identity": identity,
        "code_commit": identity["code_commit"], "inputs": [source_input],
        "transformations": [{"operation": "Complete GCCA Warehouse Facility query followed by U.S. country projection to GeoParquet",
            "parameters": {"source_rows": total, "candidate_rows": len(candidate), "projection": "country == United States",
                           "native_attributes": "All observed FUSE result fields retained; location_geo retained as canonical JSON text and also represented as native EPSG:4326 WKB.",
                           "missing_geometry": "Retained as null geometry; no geocoding, repair, exclusion, or synthetic ID."}}],
        "lineage": "Changed-successor draft for the GCCA warehouse participant scope. The DataLumos-linked 2017 HIFLD archive remains inaccessible for byte-level baseline comparison; no historical continuity or coverage equivalence is claimed.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Native string attributes, source identifier, location_geo values, and available source point coordinates retained.", "objects": []}}},
        "promotion_allowed": False, "source_accepted": False, "baseline": None,
        "acceptance": {"source": False, "historical_2017_baseline": False, "currentness": False,
                       "schema_contract": False, "id_contract": False, "coverage": False,
                       "rights": False, "release": False},
    }
    version_candidate = "v" + _sha(_canonical({"ids": audit["us_projection_id_sha256"], "records": audit["source_capture_evidence_sha256"], "code": identity}))[:32]
    prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{version_candidate}"
    parquet_path = f"geoparquet/{FILE_SLUG}.parquet"
    manifest["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"] = [{"path": parquet_path, "bytes": len(parquet), "sha256": _sha(parquet)}]
    validate_release_provenance(manifest)
    ready = {"approved": False, "version": version_candidate,
             "objects": {parquet_path: {"bytes": len(parquet), "sha256": _sha(parquet)}},
             "source_population_complete": True, "complete_source_population_count": total,
             "projected_candidate_count": len(candidate), "quality_geometry_accepted": False,
             "source_release_accepted": False, "historical_2017_baseline_accepted": False,
             "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False,
             "coverage_accepted": False, "release_accepted": False}
    outputs = {parquet_path: parquet,
               "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n",
               "metadata/candidate_ready.json": _canonical(ready) + b"\n"}
    for name in sorted(root.iterdir()):
        if name.name == "capture_manifest.json":
            outputs[f"raw/{name.name}"] = name.read_bytes()
        elif name.is_file():
            outputs[f"raw/{name.name}"] = name.read_bytes()
    state = f"_construction-state/{DATASET_SLUG}/{version_candidate}.json"
    fingerprint = _sha(_canonical({"identity": identity, "evidence": audit["source_capture_evidence_sha256"], "ids": audit["us_projection_id_sha256"]}))
    for name, body in outputs.items():
        key = f"{prefix}/{name}"
        snapshot = storage.object_snapshot(key)
        if snapshot is None:
            storage.write_key_if_unchanged(key, body, None)
        elif storage.read_key(key) != body or storage.object_snapshot(key) != snapshot:
            raise ValueError("immutable_candidate_object_changed:" + name)
        snapshot = storage.object_snapshot(key)
        if snapshot is None or snapshot.size != len(body) or storage.sha256_key(key) != _sha(body):
            raise ValueError("candidate_object_readback_failed:" + name)
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
    bundle = canonical_construction_bundle(version_candidate, fingerprint,
        {"processing_identity": identity, "source_population_count": total,
         "candidate_ids_sha256": audit["us_projection_id_sha256"],
         "source_capture_evidence_sha256": audit["source_capture_evidence_sha256"],
         "objects": descriptors})
    if seal_first_construction(storage, state, version_candidate, fingerprint, bundle) != bundle:
        raise ValueError("warehouse_original_construction_state_changed")
    validate_construction_code_identity(identity_root, identity["code_commit"], identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version_candidate,
            "feature_count": len(candidate), "source_population_count": total,
            "source_population_complete": True, "source_id_sha256": audit["us_projection_id_sha256"],
            "source_schema_sha256": audit["source_schema_sha256"],
            "source_capture_evidence_sha256": audit["source_capture_evidence_sha256"],
            "missing_geometry_count": audit["us_geometry_null_count"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "capture_directory": str(root), "historical_2017_baseline_accepted": False,
            "source_release_accepted": False, "coverage_accepted": False, "rights_accepted": False,
            "release_accepted": False, "quality_geometry_accepted": audit["us_geometry_null_count"] == 0}

"""NOAA MarineCadastre Aids-to-Navigation source capture (unapproved draft).

The source is NOAA's public MarineCadastre download, curated by NOAA and
described as the USCG ATON network. The exact downloaded GeoPackage remains
the candidate payload; no published-schema mapping is performed here.
"""
from __future__ import annotations

import hashlib
import io
import json
import shutil
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely import wkb

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, content_derived_version, processing_identity,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)
from dagster_hifld.release_approval import validate_release_provenance

DATASET_SLUG = "nautical-navigation-aids-navaid-locations"
FILE_SLUG = DATASET_SLUG
PORTAL_ITEM_ID = "3b44085a647241509d2f9a9a370c2722"
PORTAL_ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{PORTAL_ITEM_ID}?f=json"
SERVICE_URL = "https://coast.noaa.gov/arcgis/rest/services/Hosted/AtoNs/FeatureServer"
LAYER_URL = SERVICE_URL + "/0"
COUNT_URL = LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnCountOnly": "true", "f": "json"})
IDS_URL = LAYER_URL + "/query?" + urlencode({"where": "1=1", "returnIdsOnly": "true", "f": "json"})
DOWNLOAD_URL = "https://marinecadastre.gov/downloads/data/mc/AtoN.zip"
INPORT_URL = "https://www.fisheries.noaa.gov/inport/item/56120"
EXPECTED_COUNT = 51443
EXPECTED_ARCHIVE_SHA256 = "88969da2b2ad814ddc848f62bdaeb99d78f2a55a0086523d796c4d17c1361ae5"
PUBLISHED_GCS_VERSION = "v1.0.0"
PUBLISHED_GCS_FIELDS = [
    ("fid", "INTEGER", "primary-key"), ("geom", "POINT", "geometry"),
    ("lightlistn", "REAL", "published"), ("signaltime", "TEXT", "published"),
    ("characteri", "TEXT", "published"), ("aidheight", "REAL", "published"),
    ("aidtype", "TEXT", "published"), ("aidrange", "TEXT", "published"),
    ("aidname", "TEXT", "published"), ("location", "TEXT", "published"),
    ("objectid", "INTEGER", "published-identifier"), ("structure", "TEXT", "published"),
    ("remarks", "TEXT", "published"), ("id", "INTEGER", "published-identifier"),
]
EXPECTED_FIELDS = [
    ("OBJECTID", "INTEGER", 1), ("spatial", "POINT", 0),
    ("lightListNumber", "DOUBLE", 0), ("aidName", "TEXT(96)", 0),
    ("longitude", "TEXT(16)", 0), ("latitude", "TEXT(16)", 0),
    ("characteristics", "TEXT(24)", 0), ("aidHeight", "SMALLINT", 0),
    ("aidRange", "SMALLINT", 0), ("structure", "TEXT(160)", 0),
    ("remark", "TEXT(512)", 0),
]
MAX_BODY_BYTES = 64 * 1024 * 1024
CAPTURE_URLS = {
    "portal_item": PORTAL_ITEM_URL, "service_before": SERVICE_URL + "?f=pjson",
    "layer_before": LAYER_URL + "?f=pjson", "count_before": COUNT_URL,
    "ids_before": IDS_URL, "official_download": DOWNLOAD_URL,
    "layer_after": LAYER_URL + "?f=pjson", "service_after": SERVICE_URL + "?f=pjson",
    "count_after": COUNT_URL, "ids_after": IDS_URL,
}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _replay_capture_bundle(root: Path) -> dict[str, tuple[bytes, dict[str, object]]]:
    """Load only a complete, hash-checked retained response bundle; never refetch."""
    manifest = json.loads((root / "capture_manifest.json").read_bytes())
    if manifest.get("schema_version") != 1 or set(manifest.get("captures", {})) != set(CAPTURE_URLS):
        raise ValueError("noaa_replay_manifest_roster_invalid")
    expected_files = {f"{name}.{suffix}" for name in CAPTURE_URLS for suffix in ("body", "wire", "receipt.json")}
    if any(p.is_symlink() for p in root.iterdir()):
        raise ValueError("noaa_replay_directory_contains_symlink")
    actual_files = {p.name for p in root.iterdir() if p.is_file()}
    if actual_files != expected_files | {"capture_manifest.json"}:
        raise ValueError("noaa_replay_directory_roster_invalid")
    captures = {}
    for name, url in CAPTURE_URLS.items():
        body = (root / f"{name}.body").read_bytes()
        wire = (root / f"{name}.wire").read_bytes()
        receipt = json.loads((root / f"{name}.receipt.json").read_bytes())
        if (receipt.get("method") != "GET" or receipt.get("url") != url
                or receipt.get("final_url") != url or receipt.get("status") != 200
                or receipt.get("complete_body") is not True or receipt.get("error_code") is not None
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)
                or receipt.get("wire_bytes") != len(wire) or receipt.get("wire_sha256") != _sha(wire)
                or manifest["captures"].get(name) != {"body_sha256": _sha(body), "wire_sha256": _sha(wire),
                    "receipt_sha256": _sha((root / f"{name}.receipt.json").read_bytes())}):
            raise ValueError("noaa_replay_capture_integrity_failed:" + name)
        captures[name] = (body, receipt)
    return captures


def _native_parquet(native: bytes) -> bytes:
    """Convert native fields without interpreting SQLite's OBJECTID as implicit FID."""
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".gpkg") as temp:
        temp.write(native); temp.flush()
        conn = sqlite3.connect(temp.name)
        try:
            rows = conn.execute('SELECT OBJECTID,spatial,lightListNumber,aidName,longitude,latitude,characteristics,aidHeight,aidRange,structure,remark FROM AtoN ORDER BY OBJECTID')
            columns = {"OBJECTID": [], "spatial": [], "lightListNumber": [], "aidName": [], "longitude": [], "latitude": [], "characteristics": [], "aidHeight": [], "aidRange": [], "structure": [], "remark": []}
            while batch := rows.fetchmany(1024):
                for row in batch:
                    for name, value in zip(columns, row):
                        if name == "spatial":
                            raw = value
                            if not isinstance(raw, bytes) or len(raw) < 9 or raw[:2] != b"GP" or raw[2] != 0:
                                raise ValueError("noaa_gpkg_geometry_header_invalid")
                            flags = raw[3]; envelope = (flags >> 1) & 7
                            sizes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}
                            if envelope not in sizes or len(raw) <= 8 + sizes[envelope]:
                                raise ValueError("noaa_gpkg_geometry_envelope_invalid")
                            geom = wkb.loads(raw[8 + sizes[envelope]:])
                            if geom.geom_type != "Point" or geom.is_empty:
                                raise ValueError("noaa_native_geometry_type_changed")
                            columns[name].append(raw[8 + sizes[envelope]:])
                        else: columns[name].append(value)
        finally: conn.close()
    types = {"OBJECTID": pa.int64(), "spatial": pa.binary(), "lightListNumber": pa.float64(),
             "aidName": pa.string(), "longitude": pa.string(), "latitude": pa.string(),
             "characteristics": pa.string(), "aidHeight": pa.int16(), "aidRange": pa.int16(),
             "structure": pa.string(), "remark": pa.string()}
    schema = pa.schema([pa.field(n, types[n], nullable=(n != "OBJECTID")) for n in columns])
    geo = {"version":"1.1.0", "primary_column":"spatial", "columns":{"spatial":{
        "encoding":"WKB", "geometry_types":["Point"], "crs":pyproj.CRS.from_epsg(4269).to_json_dict(), "edges":"planar"}}}
    metadata = {b"geo":_canonical(geo), b"hifld:source_schema":_canonical([{"name":n,"sqlite_type":t,"primary_key":bool(pk)} for n,t,pk in EXPECTED_FIELDS]),
                b"hifld:source_semantics":_canonical({"source":"AtoN.gpkg/AtoN", "columns":[n for n in columns], "geometry_srs_id":4269})}
    table = pa.Table.from_pydict({n:pa.array(v, type=types[n]) for n,v in columns.items()}, schema=schema.with_metadata(metadata))
    sink = pa.BufferOutputStream(); pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _capture(client: httpx.Client, url: str, directory: Path, name: str) -> tuple[bytes, dict[str, object]]:
    """Retain full wire and decoded response bytes plus the shared real receipt."""
    wire_path, body_path = directory / f"{name}.wire", directory / f"{name}.body"
    wire_path.unlink(missing_ok=True); body_path.unlink(missing_ok=True)
    digests = {"wire": hashlib.sha256(), "body": hashlib.sha256()}
    sizes = {"wire": 0, "body": 0}

    def retain(kind: str, block: bytes) -> None:
        with (wire_path if kind == "wire" else body_path).open("ab") as stream:
            stream.write(block)
        sizes[kind] += len(block); digests[kind].update(block)

    observed = datetime.now(timezone.utc).isoformat()
    with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
        result = capture_http_response(
            response, max_wire_bytes=MAX_BODY_BYTES, max_decoded_bytes=MAX_BODY_BYTES,
            reserve=lambda _progress: None,
            retain_wire=lambda b: retain("wire", b), retain_decoded=lambda b: retain("body", b),
        )
        final_url = str(response.url)
    receipt = {"method": "GET", "url": url, "final_url": final_url, "observed_at": observed,
               "status": result.status_code, "complete_body": result.complete,
               "wire_bytes": sizes["wire"], "wire_sha256": digests["wire"].hexdigest(),
               "bytes": sizes["body"], "sha256": digests["body"].hexdigest(),
               "content_encoding": result.content_encoding, "content_length": result.content_length,
               "error_code": result.error_code}
    (directory / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    body = body_path.read_bytes()
    if (not result.complete or result.status_code != 200 or final_url != url
            or len(body) != receipt["bytes"] or _sha(body) != receipt["sha256"]):
        raise ValueError("incomplete_noaa_capture:" + name)
    return body, receipt


def _validate(captures: dict[str, tuple[bytes, dict[str, object]]], gpkg: bytes,
              *, expected_archive_sha256: str | None = EXPECTED_ARCHIVE_SHA256) -> dict[str, object]:
    expected = {"portal_item", "service_before", "layer_before", "count_before", "ids_before",
                "official_download", "layer_after", "service_after", "count_after", "ids_after"}
    if set(captures) != expected:
        raise ValueError("noaa_capture_roster_incomplete_or_unexpected")
    if expected_archive_sha256 is not None and _sha(gpkg) != expected_archive_sha256:
        raise ValueError("noaa_source_archive_content_drift")
    docs = {name: json.loads(body) for name, (body, _) in captures.items() if name != "official_download"}
    portal = docs["portal_item"]
    if (portal.get("id") != PORTAL_ITEM_ID or portal.get("owner") != "marinecadastre_noaa"
            or portal.get("title") != "Aids to Navigation" or portal.get("url") != SERVICE_URL
            or portal.get("access") != "public"):
        raise ValueError("noaa_portal_identity_or_access_drift")
    description = portal.get("description", "")
    if DOWNLOAD_URL not in description or INPORT_URL not in description or "U.S. Coast Guard" not in description:
        raise ValueError("noaa_official_lineage_links_or_attribution_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if service.get("serviceItemId") != layer.get("serviceItemId") or not any(
            row.get("id") == 0 and row.get("name") == "Aids to Navigation"
            for row in service.get("layers", []) if isinstance(row, dict)):
        raise ValueError("noaa_exact_service_layer_identity_drift")
    fields = layer.get("fields")
    expected_service_fields = [
        ("objectid", "esriFieldTypeOID", 4), ("lightlistnumber", "esriFieldTypeDouble", None),
        ("aidname", "esriFieldTypeString", 96), ("longitude", "esriFieldTypeString", 16),
        ("latitude", "esriFieldTypeString", 16), ("characteristics", "esriFieldTypeString", 24),
        ("aidheight", "esriFieldTypeSmallInteger", None), ("aidrange", "esriFieldTypeSmallInteger", None),
        ("structure", "esriFieldTypeString", 160), ("remark", "esriFieldTypeString", 512),
    ]
    actual_service_fields = ([(x.get("name"), x.get("type"), x.get("length")) for x in fields]
                             if isinstance(fields, list) else None)
    if actual_service_fields != expected_service_fields:
        raise ValueError("noaa_service_field_roster_drift")
    if layer.get("geometryType") != "esriGeometryPoint" or layer.get("extent", {}).get("spatialReference", {}).get("latestWkid") != 3857:
        raise ValueError("noaa_service_geometry_or_crs_drift")
    if (docs["service_after"] != service or docs["layer_after"] != layer
            or docs["count_before"].get("count") != EXPECTED_COUNT
            or docs["count_after"].get("count") != EXPECTED_COUNT):
        raise ValueError("noaa_source_changed_or_count_drift_during_capture")
    before, after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (not isinstance(before, list) or before != after or len(before) != EXPECTED_COUNT
            or len(set(before)) != EXPECTED_COUNT):
        raise ValueError("noaa_service_id_bracket_incomplete_or_changed")
    if docs["ids_before"].get("exceededTransferLimit") or docs["ids_after"].get("exceededTransferLimit"):
        raise ValueError("noaa_service_id_roster_truncated")
    if not zipfile.is_zipfile(io.BytesIO(gpkg)):
        raise ValueError("noaa_official_archive_invalid_zip")
    with zipfile.ZipFile(io.BytesIO(gpkg)) as archive:
        names = archive.namelist()
        if names != ["AtoN.gpkg"]:
            raise ValueError("noaa_official_archive_roster_drift")
        native = archive.read("AtoN.gpkg")
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".gpkg") as temp:
        temp.write(native); temp.flush()
        conn = sqlite3.connect(temp.name)
        try:
            tables = conn.execute("SELECT table_name,srs_id,geometry_type_name FROM gpkg_geometry_columns").fetchall()
            table = conn.execute("SELECT table_name FROM gpkg_contents WHERE data_type='features'").fetchall()
            schema = conn.execute('PRAGMA table_info("AtoN")').fetchall()
            count = conn.execute('SELECT COUNT(*) FROM "AtoN"').fetchone()[0]
            ids = [r[0] for r in conn.execute('SELECT OBJECTID FROM "AtoN" ORDER BY OBJECTID')]
            null_geometry_count = conn.execute('SELECT COUNT(*) FROM "AtoN" WHERE spatial IS NULL').fetchone()[0]
            rtree_count, min_x, min_y, max_x, max_y = conn.execute(
                "SELECT count(*),min(minx),min(miny),max(maxx),max(maxy) FROM rtree_AtoN_spatial"
            ).fetchone()
            orphan_rtree_ids = conn.execute(
                'SELECT COUNT(*) FROM rtree_AtoN_spatial r LEFT JOIN "AtoN" a ON a.OBJECTID=r.id WHERE a.OBJECTID IS NULL'
            ).fetchone()[0]
            missing_rtree_ids = conn.execute(
                'SELECT COUNT(*) FROM "AtoN" a LEFT JOIN rtree_AtoN_spatial r ON r.id=a.OBJECTID WHERE r.id IS NULL'
            ).fetchone()[0]
        finally:
            conn.close()
    roster = [(r[1], r[2], r[3] or 0) for r in schema]
    if tables != [("AtoN", 4269, "POINT")] or table != [("AtoN",)] or roster != EXPECTED_FIELDS:
        raise ValueError("noaa_native_geopackage_schema_or_crs_drift")
    if (count != EXPECTED_COUNT or ids != sorted(before) or null_geometry_count
            or rtree_count != count or orphan_rtree_ids or missing_rtree_ids
            or None in (min_x, min_y, max_x, max_y)):
        raise ValueError("noaa_native_geopackage_population_or_id_drift")
    return {"feature_count": count, "source_ids_sha256": _sha(_canonical(ids)),
            "service_ids_sha256": _sha(_canonical(sorted(before))), "source_fields": [
                {"name": name, "sqlite_type": typ, "primary_key": bool(pk)} for name, typ, pk in EXPECTED_FIELDS
            ], "native_crs_epsg": 4269, "native_geometry_type": "Point",
            "native_null_geometry_count": null_geometry_count,
            "native_geometry_index_count": rtree_count,
            "native_geometry_index_orphan_ids": orphan_rtree_ids,
            "native_geometry_index_missing_ids": missing_rtree_ids,
            "native_bbox": [min_x, min_y, max_x, max_y],
            "source_archive_sha256": _sha(gpkg), "native_geopackage_sha256": _sha(native),
            "item_id": PORTAL_ITEM_ID, "portal_owner": portal["owner"],
            "service_item_id": service["serviceItemId"], "layer_id": 0,
            "source_release_date": None, "source_release_date_unknown_reason": "NOAA item and download timestamps are retrieval metadata, not a declared source release date",
            "published_candidate_mismatch": {"published_version": PUBLISHED_GCS_VERSION,
                "published_feature_count": 52471,
                "source_feature_count": count, "published_geometry_crs": 3857,
                "source_geometry_crs": 4269,
                "published_fields": [{"name": n, "sqlite_type": t, "role": role}
                                     for n, t, role in PUBLISHED_GCS_FIELDS],
                "published_repeated_id_values": 10,
                "published_repeated_id_field": "id",
                "published_duplicate_identity_rows": [[value, value, value + 10]
                                                       for value in range(1, 11)],
                "schema_mapping_approved": False}}


def capture_noaa_nautical_navaids(storage: StagingStorageResource, *, capture_directory: str | None = None) -> dict[str, object]:
    """Replay a retained full capture or acquire and stage an unapproved candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(p.is_symlink() for p in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000:
        raise ValueError("disk_floor_before_acquisition")
    replay = capture_directory is not None
    root = Path(capture_directory) if replay else Path("/tmp/hifld-native-reconstruction/176/captures") / uuid4().hex
    root = root.absolute()
    if ".." in root.parts or any(p.is_symlink() for p in (root, *root.parents)):
        raise ValueError("unsafe_capture_directory")
    if replay:
        if not root.is_dir() or root.is_symlink(): raise ValueError("noaa_replay_capture_directory_missing_or_unsafe")
    else: root.mkdir(parents=True, exist_ok=False)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        if replay:
            captures = _replay_capture_bundle(root)
        else:
            with httpx.Client(follow_redirects=False, timeout=60, headers={"User-Agent": "hifld-noaa-navaid-draft/1.0"}) as client:
                for name in list(CAPTURE_URLS)[:6]: captures[name] = _capture(client, CAPTURE_URLS[name], root, name)
                for name in list(CAPTURE_URLS)[6:]: captures[name] = _capture(client, CAPTURE_URLS[name], root, name)
            (root / "capture_manifest.json").write_bytes(_canonical({"schema_version":1,"captures":{
                name:{"body_sha256":_sha(body),"wire_sha256":receipt["wire_sha256"],
                      "receipt_sha256":_sha((root/f"{name}.receipt.json").read_bytes())}
                for name,(body,receipt) in captures.items()}})+b"\n")
        archive = captures["official_download"][0]
        with zipfile.ZipFile(io.BytesIO(archive)) as z: native = z.read("AtoN.gpkg")
        audit = _validate(captures, archive)
    except Exception as exc:
        _quarantine_captures(storage, root, captures, type(exc).__name__)
        raise
    identity = processing_identity(Path(__file__).parents[3])
    schema_sha = _sha(_canonical({"fields":EXPECTED_FIELDS,"semantics":"Native GPKG declaration; OBJECTID remains explicit; EPSG:4269 Point"}))
    record_sha = _sha(native)
    geometry_sha = _sha(_canonical({"native_geopackage_sha256":_sha(native),"srs_id":4269,"geometry":"Point"}))
    version = content_derived_version(record_sha, geometry_sha, schema_sha, identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    construction_fingerprint = _sha(_canonical(identity))
    parquet = _native_parquet(native)
    prior_bundle = None
    if storage.object_snapshot(state_key) is not None:
        prior_bundle = json.loads(load_first_construction(storage,state_key,version,construction_fingerprint))
        prior_payload = prior_bundle.get("payload")
        if (not isinstance(prior_payload,dict) or set(prior_payload)!={"processing_identity","source_record_sha256","source_schema_sha256","capture_receipts","objects"}
                or prior_payload.get("processing_identity")!=identity or prior_payload.get("source_record_sha256")!=record_sha
                or prior_payload.get("source_schema_sha256")!=schema_sha):
            raise ValueError("cached_first_candidate_identity_or_source_changed")
        for name,(body,receipt) in list(captures.items()):
            old=prior_payload["capture_receipts"].get(name)
            wire=(root/f"{name}.wire").read_bytes()
            if (not isinstance(old,dict) or body!=storage.read_key(f"{prefix}/raw/{name}.body")
                    or wire!=storage.read_key(f"{prefix}/raw/{name}.wire")
                    or old.get("bytes")!=len(body) or old.get("sha256")!=_sha(body)
                    or old.get("wire_bytes")!=len(wire) or old.get("wire_sha256")!=_sha(wire)):
                raise ValueError("cached_original_capture_changed:"+name)
            captures[name]=(body,old)
    capture_receipts = {name:receipt for name,(_,receipt) in captures.items()}
    inputs = []
    for name, (_, receipt) in sorted(captures.items()):
        inputs.append({"input_id":name,"producer":"NOAA MarineCadastre; underlying USCG direct producer lineage unconfirmed",
            "host":httpx.URL(receipt["url"]).host,"role":"official_distributor","distributor":"NOAA MarineCadastre",
            "delegation_evidence":{"url":PORTAL_ITEM_URL,"statement":"NOAA-owned public item links this exact download; formal delegation evidence pending"},
            "canonical_url":PORTAL_ITEM_URL,"retrieval_url":receipt["url"],"requested_url":receipt["url"],
            "observed_final_url":receipt["final_url"],"retrieved_at":receipt["observed_at"],"http_status":receipt["status"],
            "bytes":receipt["bytes"],"sha256":receipt["sha256"],"license":"No explicit license stated by the NOAA item; rights review pending",
            "attribution":"NOAA MarineCadastre item ‘Aids to Navigation’; USCG responsibility described by item, exact producer lineage unresolved",
            "release_date":None,"release_date_unknown_reason":"NOAA item and download timestamps are retrieval metadata, not a declared source release date"})
    manifest = {"schema_version": 1, "approved": False, "source_url": DOWNLOAD_URL,
        "publisher": "NOAA MarineCadastre (USCG ATON network)", "producer": "U.S. Coast Guard (responsibility stated by NOAA item; direct producer lineage not independently confirmed)",
        "official_portal_item": PORTAL_ITEM_ID, "official_service_url": LAYER_URL,
        "metadata_url": INPORT_URL, "source_release_date": None,
        "lineage": "Public NOAA ArcGIS item owned by marinecadastre_noaa links this exact MarineCadastre archive and InPort 56120. The item says the USCG is responsible for ensuring the ATON network is up to date and functioning properly, and NOAA Marine Cadastre curates the item. InPort metadata was HTTP 500 during this capture.",
        "inputs": inputs, "candidate_payload": "Native GeoPackage retained byte-for-byte; typed GeoParquet preserves all declared native fields and Point WKB, with explicit OBJECTID",
        "published_gcs_compatibility": "unresolved: published collection has a different feature count, CRS and field schema; candidate remains blocked",
        "processor":"HIFLD Next","code_commit":identity["code_commit"],"transformations":[{"operation":"native_geopackage_to_typed_geoparquet","parameters":{"all_native_fields":"preserved in declared order and physical types","OBJECTID":"explicit source column","geometry":"native EPSG:4269 Point WKB, no reprojection or repair","approvals":"all pending"}}],
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Typed native columns and explicit OBJECTID avoid SQLite FID loss; approvals remain pending.","objects":[{"path":f"geoparquet/{DATASET_SLUG}.parquet","bytes":len(parquet),"sha256":_sha(parquet)}]}}},
        "unsupported_output_formats":{"geopackage":"Native original retained under raw/official_native; automatic conversion withheld to preserve SQLite OBJECTID semantics.","pmtiles":"No styling/generalization decision approved."},
        "approvals": {"source": False, "baseline": False, "contract": False,
            "rights": False, "geometry": False, "coverage": False, "release": False}}
    validate_release_provenance(manifest)
    audit.update({"source_archive_bytes": len(archive), "native_geopackage_bytes": len(native),
                  "source_release_accepted": False, "schema_contract_accepted": False,
                  "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
                  "release_accepted": False})
    outputs = {"raw/official_native/AtoN.gpkg": native,
               f"geoparquet/{DATASET_SLUG}.parquet":parquet,
               "raw/source_archive.zip": archive,
               "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    ready = {"approved": False, "version": version, "objects": {
        "raw/official_native/AtoN.gpkg": {"bytes": len(native), "sha256": audit["native_geopackage_sha256"]},
        f"geoparquet/{DATASET_SLUG}.parquet":{"bytes":len(parquet),"sha256":_sha(parquet)}},
        "source_release_accepted": False, "schema_contract_accepted": False,
        "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
        "release_accepted": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    for path, body in outputs.items(): write_immutable(storage, f"{prefix}/{path}", body)
    for path, body in outputs.items():
        key = f"{prefix}/{path}"; snap = storage.object_snapshot(key)
        if snap is None or snap.size != len(body) or storage.sha256_key(key) != _sha(body) or storage.object_snapshot(key) != snap:
            raise ValueError("noaa_candidate_readback_failed:" + path)
    payload = {"source_record_sha256":record_sha,"source_schema_sha256":schema_sha,"processing_identity":identity,
               "capture_receipts":capture_receipts,
               "objects":{name:{"bytes":len(body),"sha256":_sha(body)} for name,body in outputs.items()}}
    fingerprint = construction_fingerprint
    expected_objects = payload["objects"]
    if storage.object_snapshot(state_key) is not None:
        verify_cached_candidate(storage,Path(__file__).parents[3],state_key,prefix,version,fingerprint,identity,
                                record_sha,schema_sha,expected_objects,capture_receipts)
        return {"dataset_slug":DATASET_SLUG,"file_slug":FILE_SLUG,"version":version,"feature_count":audit["feature_count"],
                "candidate_ready":prefix+"/metadata/candidate_ready.json","reused":True,
                "published_baseline_compatible":False,"source_release_accepted":False,
                "schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    if storage.object_snapshot(state_key) is None:
        seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    else:
        prior = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        if prior.get("payload") != payload: raise ValueError("noaa_first_candidate_changed")
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["feature_count"], "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "native_geopackage_sha256": audit["native_geopackage_sha256"],
            "published_baseline_compatible": False, "source_release_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(root),"reused":False}


def _quarantine_captures(storage: StagingStorageResource, root: Path, captures: dict[str, tuple[bytes, dict[str, object]]], reason: str) -> None:
    """Keep every complete acquired response if a later check rejects the batch."""
    key_root = f"_quarantine/{DATASET_SLUG}/{uuid4().hex}"
    manifest = {"complete": False, "error_code": reason, "captures": {}}
    names = {path.name.removesuffix(".body") for path in root.glob("*.body")}
    names.update(path.name.removesuffix(".wire") for path in root.glob("*.wire"))
    names.update(path.name.removesuffix(".receipt.json") for path in root.glob("*.receipt.json"))
    names.update(captures)
    for name in sorted(names):
        evidence = {}
        for suffix in ("body", "wire", "receipt.json"):
            path = root / f"{name}.{suffix}"
            if path.is_file():
                evidence[suffix] = path.read_bytes()
                write_immutable(storage, f"{key_root}/raw/{name}.{suffix}", evidence[suffix])
        receipt = captures.get(name, (b"", {}))[1]
        if "receipt.json" in evidence:
            try: receipt = json.loads(evidence["receipt.json"])
            except (ValueError, UnicodeError): receipt = {"receipt_readback_failed": True}
        manifest["captures"][name] = receipt
    write_immutable(storage, f"{key_root}/capture_manifest.json", _canonical(manifest) + b"\n")

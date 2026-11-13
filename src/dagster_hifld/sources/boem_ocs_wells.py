"""BOEM/BSEE national MMC cadastre well-point candidate from gis.boem.gov.

The HIFLD inventory entry describes BOEM's Geospatial Services Division
"Regional Map Service" cadastre layers; the currently published national
release of that product is the Marine Mineral Cadastre service on BOEM's
own ArcGIS Server (``gis.boem.gov``), whose service description and BOEM
GSD contact match the inventory text. Its "OCS Oil and Natural Gas Wells"
layer (id 1) is queried in bounded, OBJECTID-pinned pages against the
full ID roster. Source JSON is retained byte-for-byte; a GeoParquet
candidate keeps all thirteen native attributes and the producer's
EPSG:4269 point coordinates without reprojection. This is an unapproved
draft candidate: source release date, schema/ID contracts, rights and
release acceptance remain pending.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import shapely

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, capture_get, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)

DATASET_SLUG = "outer-continental-shelf-ocs-oil-and-natural-gas-wells"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://gis.boem.gov/server/rest/services/BOEM_BSEE/MMC_Layers/FeatureServer"
LAYER_ID = 1
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "OCS Oil and Natural Gas Wells"
AGENCY = "Bureau of Ocean Energy Management"
PUBLISHER = "Bureau of Ocean Energy Management, Office of Strategic Resources, Geospatial Services Division"
HOST = "gis.boem.gov"
PAGE_SIZE = 10000
MAX_BODY_BYTES = 64_000_000
EXPECTED_FIELDS = [
    ("COMPLETION", "esriFieldTypeDate", True),
    ("Y", "esriFieldTypeDouble", True),
    ("X", "esriFieldTypeDouble", True),
    ("OBJECTID", "esriFieldTypeOID", False),
    ("API_NUMBER", "esriFieldTypeString", True),
    ("OPERATOR", "esriFieldTypeString", True),
    ("WELL_NAME", "esriFieldTypeString", True),
    ("SPUD_DATE", "esriFieldTypeString", True),
    ("TYPE_CODE", "esriFieldTypeString", True),
    ("TYPE_CODE_DESC", "esriFieldTypeString", True),
    ("STATUS", "esriFieldTypeString", True),
    ("DEPTH", "esriFieldTypeString", True),
    ("STATUS_DESCRIPTION", "esriFieldTypeString", True),
]
STRING_FIELDS = ["API_NUMBER", "OPERATOR", "WELL_NAME", "SPUD_DATE", "TYPE_CODE",
                 "TYPE_CODE_DESC", "STATUS", "DEPTH", "STATUS_DESCRIPTION"]
FIELD_NAMES = [field[0] for field in EXPECTED_FIELDS]


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _query(**params: object) -> str:
    return LAYER_URL + "/query?" + urlencode({"f": "json", **params})


def _input_fingerprint(captures: Mapping[str, tuple[bytes, Mapping[str, object]]], identity: Mapping[str, object]) -> str:
    """Hash exact source body/wire evidence and code identity; timestamps are excluded."""
    return _sha(_canonical({"bodies": {name: {"bytes": len(body), "sha256": _sha(body),
                                              "wire_bytes": receipt.get("wire_bytes"),
                                              "wire_sha256": receipt.get("wire_sha256")}
                            for name, (body, receipt) in sorted(captures.items())},
                            "processing_identity": identity}))


def _validate_source_spatial_reference(value: object, label: str) -> None:
    """Accept only EPSG:4269 and its ArcGIS 4269 alias, including WKT evidence."""
    if not isinstance(value, dict):
        raise ValueError(f"{label}_spatial_reference_missing")
    identifiers = [value.get(name) for name in ("wkid", "latestWkid") if value.get(name) is not None]
    if identifiers and any(type(code) is not int or code != 4269 for code in identifiers):
        raise ValueError(f"{label}_spatial_reference_drift")
    if not identifiers and not any(value.get(name) for name in ("wkt", "wkt2")):
        raise ValueError(f"{label}_spatial_reference_missing")
    for name in ("wkt", "wkt2"):
        wkt = value.get(name)
        if wkt:
            try:
                if not pyproj.CRS.from_user_input(wkt).equals(pyproj.CRS.from_epsg(4269)):
                    raise ValueError(f"{label}_spatial_reference_drift")
            except ValueError:
                raise
            except Exception as exc:
                raise ValueError(f"{label}_spatial_reference_invalid") from exc


def _capture_names(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query(where="1=1", returnCountOnly="true")),
        ("ids_before", _query(where="1=1", returnIdsOnly="true")),
    ]
    # The server drops very long objectIds lists, so pages walk the sorted
    # roster in OBJECTID order with resultOffset/resultRecordCount pagination.
    for page in range(0, (len(ids) + PAGE_SIZE - 1) // PAGE_SIZE):
        urls.append((f"page_{page:03d}", _query(
            where="1=1", outFields="*", returnGeometry="true", outSR="4269",
            orderByFields="OBJECTID ASC", resultOffset=page * PAGE_SIZE,
            resultRecordCount=PAGE_SIZE)))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"), ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", _query(where="1=1", returnCountOnly="true")),
        ("ids_after", _query(where="1=1", returnIdsOnly="true")),
    ])
    return urls


def _load_offline(root: Path) -> dict[str, tuple[bytes, dict[str, object]]]:
    summary = json.loads((root / "capture-summary.json").read_bytes())
    if summary.get("complete") is not True:
        raise ValueError("offline_native_capture_incomplete")
    captures = {}
    for receipt_path in sorted(root.glob("*.receipt.json")):
        receipt = json.loads(receipt_path.read_bytes())
        name = receipt_path.name.removesuffix(".receipt.json")
        body = (root / f"{name}.body").read_bytes()
        if (receipt.get("complete") is not True or receipt.get("status") != 200
                or receipt.get("bytes") != len(body) or receipt.get("sha256") != _sha(body)):
            raise ValueError("offline_capture_body_receipt_mismatch:" + name)
        # The preserved one-shot capture was recorded with identity encoding;
        # reconstruct the shared receipt shape without changing observation time.
        receipt.update({"complete_body": True, "wire_bytes": len(body), "wire_sha256": _sha(body),
                        "content_encoding": "identity", "error_code": None})
        captures[name] = (body, receipt)
    return captures


def _acquire(root: Path, storage: StagingStorageResource) -> dict[str, tuple[bytes, dict[str, object]]]:
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=60,
                          headers={"User-Agent": "hifld-next-datasets/boem-ocs-wells-draft"}) as client:
            seed, receipt = capture_get(client, _query(where="1=1", returnIdsOnly="true"), root,
                                        "ids_before", max_bytes=MAX_BODY_BYTES)
            captures["ids_before"] = (seed, receipt)
            ids = json.loads(seed).get("objectIds")
            if not isinstance(ids, list) or not ids or any(type(i) is not int for i in ids) or len(ids) != len(set(ids)):
                raise ValueError("source_seed_id_roster_invalid")
            urls = [(n, u) for n, u in _capture_names(ids) if n != "ids_before"]
            for name, url in urls:
                body, receipt = capture_get(client, url, root, name, max_bytes=MAX_BODY_BYTES)
                captures[name] = (body, receipt)
        return captures
    except Exception as exc:
        if captures:
            quarantine_incomplete_capture(storage, root, DATASET_SLUG, "incomplete", captures, type(exc).__name__)
        raise


def _profile_fields(layer: Mapping[str, object]) -> list[dict[str, object]]:
    fields = layer.get("fields")
    if not isinstance(fields, list) or any(not isinstance(x, dict) for x in fields):
        raise ValueError("source_field_roster_missing")
    simple = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f} for f in fields]
    if [(f.get("name"), f.get("type"), f.get("nullable")) for f in simple] != EXPECTED_FIELDS:
        raise ValueError("source_field_schema_drift")
    return simple


def _validate(captures: Mapping[str, tuple[bytes, Mapping[str, object]]]) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    docs = {name: json.loads(body) for name, (body, _) in captures.items()}
    ids_before, ids_after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (not isinstance(ids_before, list) or not ids_before or sorted(ids_before) != sorted(ids_after or [])
            or len(ids_before) != len(set(ids_before))):
        raise ValueError("source_id_population_changed_during_capture")
    count = len(ids_before)
    if docs["count_before"].get("count") != count or docs["count_after"].get("count") != count:
        raise ValueError("source_count_bracket_mismatch")
    service, layer = docs["service_before"], docs["layer_before"]
    if layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPoint":
        raise ValueError("source_layer_identity_or_geometry_drift")
    if not any(item.get("id") == LAYER_ID and item.get("name") == LAYER_NAME for item in service.get("layers", []) if isinstance(item, dict)):
        raise ValueError("source_layer_not_in_service_roster")
    description = service.get("serviceDescription", "")
    if "Bureau of Ocean Energy Management" not in description or "Bureau of Safety and Environmental Enforcement" not in description \
            or "mappingboundarybranch@boem.gov" not in description:
        raise ValueError("producer_service_description_drift")
    if layer.get("objectIdField") != "OBJECTID" or layer.get("hasZ") is not False or layer.get("hasM") is not False:
        raise ValueError("source_spatial_contract_drift")
    if layer.get("maxRecordCount") != 10000:
        raise ValueError("source_transfer_limit_drift")
    extent = layer.get("extent", {})
    _validate_source_spatial_reference(extent.get("spatialReference"), "source_layer")
    _validate_source_spatial_reference(layer.get("sourceSpatialReference") if layer.get("sourceSpatialReference") is not None else service.get("spatialReference"), "source_service")
    fields = _profile_fields(layer)
    if fields != _profile_fields(docs["layer_after"]):
        raise ValueError("source_schema_changed_during_capture")
    if docs["service_before"] != docs["service_after"] or layer != docs["layer_after"]:
        raise ValueError("source_metadata_changed_during_capture")
    pages = sorted(name for name in docs if name.startswith("page_"))
    if len(pages) != (count + PAGE_SIZE - 1) // PAGE_SIZE:
        raise ValueError("source_page_roster_incomplete")
    features = []
    for index, name in enumerate(pages):
        page = docs[name]
        rows = page.get("features")
        if not isinstance(rows, list) or len(rows) > PAGE_SIZE:
            raise ValueError("source_page_error_or_truncation")
        # A page below the record cap must not claim more rows exist; only a
        # completely full page may report exceededTransferLimit.
        if page.get("exceededTransferLimit") is True and len(rows) != PAGE_SIZE:
            raise ValueError("source_page_error_or_truncation")
        _validate_source_spatial_reference(page.get("spatialReference"), "source_page")
        features.extend(rows)
    feature_ids = [f.get("attributes", {}).get("OBJECTID") for f in features if isinstance(f, dict) and isinstance(f.get("attributes"), dict)]
    if (len(features) != count or len(feature_ids) != count or any(type(value) is not int for value in feature_ids)
            or sorted(feature_ids) != sorted(ids_before)):
        raise ValueError("source_feature_population_incomplete")
    if feature_ids != sorted(feature_ids) or len(set(feature_ids)) != count:
        raise ValueError("source_page_order_drift")
    for feature in features:
        attrs, geom = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs, dict) or set(attrs) != set(FIELD_NAMES):
            raise ValueError("source_feature_field_roster_drift")
        if not isinstance(geom, dict) or set(geom) - {"x", "y", "spatialReference"}:
            raise ValueError("source_point_geometry_invalid")
        if feature.get("spatialReference") is not None:
            _validate_source_spatial_reference(feature["spatialReference"], "source_feature")
        if geom.get("spatialReference") is not None:
            _validate_source_spatial_reference(geom["spatialReference"], "source_geometry")
        x, y = geom.get("x"), geom.get("y")
        if any(type(v) not in (int, float) or v != v or v in (float("inf"), float("-inf")) for v in (x, y)) \
                or not (-180 <= x <= 180 and -90 <= y <= 90):
            raise ValueError("source_point_coordinates_invalid")
        if type(attrs["OBJECTID"]) is not int:
            raise ValueError("source_native_identifier_invalid")
        api_number = attrs["API_NUMBER"]
        if not isinstance(api_number, str) or not api_number.strip():
            raise ValueError("source_api_number_missing")
        completion = attrs["COMPLETION"]
        if completion is not None and (type(completion) is not int or completion < 0):
            raise ValueError("source_completion_date_invalid")
        xy_attrs = (attrs["X"], attrs["Y"])
        if any(v is not None and (type(v) not in (int, float) or v != v) for v in xy_attrs):
            raise ValueError("source_xy_attribute_invalid")
        for name in STRING_FIELDS[1:]:
            value = attrs[name]
            if value is not None and not isinstance(value, str):
                raise ValueError("source_string_field_type_drift:" + name)
    features.sort(key=lambda f: f["attributes"]["OBJECTID"])
    nulls = {}
    for name in FIELD_NAMES:
        nulls[name] = sum(1 for f in features if f["attributes"][name] is None)
    api_numbers = [f["attributes"]["API_NUMBER"] for f in features]
    duplicate_api_numbers = len(api_numbers) - len(set(api_numbers))
    audit = {"feature_count": count, "unique_objectid_count": len(set(feature_ids)),
             "unique_api_number_count": len(set(api_numbers)),
             "duplicate_api_number_count": duplicate_api_numbers,
             "null_counts_by_field": nulls,
             "geometry_bounds": [
                 min(f["geometry"]["x"] for f in features), min(f["geometry"]["y"] for f in features),
                 max(f["geometry"]["x"] for f in features), max(f["geometry"]["y"] for f in features)],
             "source_id_sha256": _sha(_canonical(sorted(ids_before))),
             "source_record_sha256": _sha(_canonical(features)),
             "source_schema_sha256": _sha(_canonical(fields)),
             "source_crs_epsg": 4269, "native_geometry_type": "Point", "native_dimension": 2,
             "null_geometry_count": 0,
             "source_release_date": None,
             "service_current_version": service.get("currentVersion"),
             "producer_owner": None,
             "source_fields": fields,
             "release_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "freshness_accepted": False, "rights_accepted": False,
             "source_authority_accepted": False, "mapping_approved": False}
    return layer, fields, features, audit


def _geoparquet(features: list[dict[str, object]], fields: list[dict[str, object]], root: Path) -> bytes:
    if [field["name"] for field in fields] != FIELD_NAMES:
        raise ValueError("source_field_order_drift")
    schema = pa.schema([
        pa.field("COMPLETION", pa.date32(), nullable=True),
        pa.field("Y", pa.float64(), nullable=True),
        pa.field("X", pa.float64(), nullable=True),
        pa.field("OBJECTID", pa.int64(), nullable=False),
        *[pa.field(name, pa.string(), nullable=True) for name in STRING_FIELDS],
        pa.field("geometry", pa.binary(), nullable=False),
    ])
    columns = {
        "COMPLETION": pa.array(
            [None if f["attributes"]["COMPLETION"] is None
             else datetime.fromtimestamp(f["attributes"]["COMPLETION"] / 1000, tz=timezone.utc).date()
             for f in features], type=pa.date32()),
        "Y": pa.array([f["attributes"]["Y"] for f in features], type=pa.float64()),
        "X": pa.array([f["attributes"]["X"] for f in features], type=pa.float64()),
        "OBJECTID": pa.array([f["attributes"]["OBJECTID"] for f in features], type=pa.int64()),
        **{name: pa.array([f["attributes"][name] for f in features], type=pa.string()) for name in STRING_FIELDS},
        "geometry": pa.array([shapely.geometry.Point(f["geometry"]["x"], f["geometry"]["y"]).wkb
                              for f in features], type=pa.binary()),
    }
    geo = {"version": "1.1.0", "primary_column": "geometry",
           "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"],
                                    "crs": pyproj.CRS.from_epsg(4269).to_json_dict(), "edges": "planar"}}}
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(columns, schema=schema.with_metadata({b"geo": _canonical(geo)})),
                   sink, compression="zstd")
    body = sink.getvalue().to_pybytes()
    (root / "candidate-verification.json").write_bytes(_canonical({
        "rows": len(features),
        "objectid_first": features[0]["attributes"]["OBJECTID"],
        "objectid_last": features[-1]["attributes"]["OBJECTID"],
        "readback_columns": schema.names}) + b"\n")
    return body


def capture_boem_ocs_wells(storage: StagingStorageResource, *, version: str = "draft",
                           capture_directory: str | None = None, offline_capture_directory: str | None = None) -> dict[str, object]:
    """Capture an unapproved national well-point candidate; explicit offline replay is supported."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(p.is_symlink() for p in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    if shutil.disk_usage(base).free < 256_000_000:
        raise ValueError("unsafe_or_low_space_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    capture_parent = Path(capture_directory).absolute() if capture_directory else Path(tempfile.gettempdir()) / "boem-ocs-well-captures"
    capture_root = capture_parent / f"{version}-{uuid4().hex}"
    if ".." in capture_root.parts or any(p.is_symlink() for p in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures = _load_offline(Path(offline_capture_directory)) if offline_capture_directory else _acquire(capture_root, storage)
    layer, fields, features, audit = _validate(captures)
    identity = processing_identity(Path(__file__).parents[3])
    fingerprint = _input_fingerprint(captures, identity)
    version = "v" + fingerprint[:32]
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, _sha(_canonical(identity))))
        payload = bundle.get("payload", {})
        receipts = payload.get("capture_receipts", {})
        input_key = f"{prefix}/metadata/source_inputs.json"
        if (payload.get("processing_identity") != identity or "objects" not in payload
                or storage.read_key(input_key) != _canonical({"sha256": fingerprint}) + b"\n"):
            raise ValueError("cached_first_candidate_identity_changed")
        # Same content version can be observed on a later refresh. Keep the first
        # sealed receipts and manifest verbatim instead of rewriting timestamps.
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                _sha(_canonical(identity)), identity, audit["source_record_sha256"],
                                audit["source_schema_sha256"], payload["objects"], receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": audit["feature_count"], "candidate_ready": prefix + "/metadata/candidate_ready.json",
                "unique_api_number_count": audit["unique_api_number_count"],
                "source_id_sha256": audit["source_id_sha256"],
                "reused": True, "source_release_accepted": False, "schema_contract_accepted": False,
                "id_contract_accepted": False, "freshness_accepted": False, "rights_accepted": False,
                "source_authority_accepted": False, "mapping_approved": False, "release_accepted": False}

    parquet_body = _geoparquet(features, fields, capture_root)
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_name: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    inputs = [{"input_id": n, "producer": PUBLISHER, "canonical_url": SERVICE_URL,
               "host": urlsplit(receipt["url"]).hostname,
               "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
               "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
               "bytes": receipt["bytes"], "sha256": receipt["sha256"],
               "wire_bytes": receipt.get("wire_bytes"), "wire_sha256": receipt.get("wire_sha256"),
               "license": "https://www.usa.gov/publicdomain/label/1.0/",
               "attribution": "U.S. Government work (BOEM/BSEE cadastre, 30 CFR 256.8); rights acceptance remains false.",
               "role": "producer_publication",
               "release_date": None,
               "release_date_unknown_reason": "The cadastre service updates wells continuously (layer description: daily); it publishes no dated source release, and service metadata timestamps are observations, not release dates."}
              for n, (_, receipt) in sorted(captures.items())]
    manifest = {"schema_version": 1, "approved": False,
                "title": "Outer Continental Shelf (OCS) Oil and Natural Gas Wells",
                "description": "National-scale point locations of exploration and development wells drilled in federal OCS waters, published by BOEM's Geospatial Services Division Marine Mineral Cadastre service; the well database is maintained by BSEE. Not for official or legal use per the service description.",
                "agency": AGENCY, "publisher": PUBLISHER,
                "source_host_first_party": True,
                "source_authority_note": ("gis.boem.gov is BOEM's own ArcGIS Server host (no ArcGIS Online "
                                          "hosting account involved); the service description and BOEM Geospatial "
                                          "Services Division contact match the HIFLD inventory entry. Source "
                                          "authority, freshness and rights acceptance remain false pending "
                                          "maintainer approval."),
                "source_authority_accepted": False,
                "source_url": LAYER_URL, "source_release_date": None,
                "processor": "HIFLD Next", "code_commit": identity["code_commit"],
                "inputs": inputs,
                "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                                     "parameters": {"layer_id": LAYER_ID,
                                                    "source_fields": "all thirteen native fields in producer order; OBJECTID, API_NUMBER, attribute X/Y, dates-as-strings and epoch-ms COMPLETION retain source meaning",
                                                    "geometry": "Esri point x/y read directly into WKB; EPSG:4269 retained without reprojection, repair or simplification",
                                                    "release_status": "unapproved draft"}}],
                "lineage": "National Scale Marine Mineral Cadastre Map Server served from BOEM's own gis.boem.gov ArcGIS Server by BOEM's Office of Strategic Resources, Geospatial Services Division (service contact matches the HIFLD inventory entry); wells data within it are maintained by BSEE. Crosswalk-classified as a maintained producer REST publication, not a mirror or archive copy.",
                "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Source attributes and EPSG:4269 point geometry retained for an unapproved candidate.", "objects": [{"path": parquet_name, **source_objects[parquet_name]}]}}},
                "unsupported_output_formats": {
                    "shapefile_zip": "No approved published schema mapping; release remains blocked.",
                    "geopackage": "No approved published schema mapping; release remains blocked.",
                    "pmtiles": "No approved display transformation or generalized geometry.",
                }}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"source_service_url": SERVICE_URL, "source_layer_id": LAYER_ID,
                  "source_layer_name": LAYER_NAME, "source_inputs_sha256": fingerprint,
                  "source_release_accepted": False, "id_contract_accepted": False,
                  "freshness_accepted": False, "rights_accepted": False,
                  "source_authority_accepted": False, "schema_contract_accepted": False,
                  "release_accepted": False})
    outputs = {parquet_name: parquet_body,
               "metadata/source_inputs.json": _canonical({"sha256": fingerprint}) + b"\n",
               "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes() if (capture_root / f"{name}.wire").exists() else body
        outputs[f"raw/{name}.receipt.json"] = _canonical(dict(receipt)) + b"\n"
    ready = {"approved": False, "version": version, "objects": source_objects,
             "source_record_sha256": audit["source_record_sha256"], "release_accepted": False,
             "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False,
             "source_authority_accepted": False, "mapping_approved": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready) + b"\n"
    for name, body in outputs.items():
        write_immutable(storage, f"{prefix}/{name}", body)
    objects = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
    for name, descriptor in objects.items():
        key = f"{prefix}/{name}"; snap = storage.object_snapshot(key)
        if snap is None or descriptor != {"bytes": snap.size, "sha256": storage.sha256_key(key)} or storage.object_snapshot(key) != snap:
            raise ValueError("candidate_physical_readback_failed:" + name)
    payload = {"processing_identity": identity, "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": {n: r for n, (_, r) in sorted(captures.items())}, "objects": objects}
    seal_ready_candidate(storage, state_key, version, _sha(_canonical(identity)), payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": audit["feature_count"], "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "unique_api_number_count": audit["unique_api_number_count"],
            "duplicate_api_number_count": audit["duplicate_api_number_count"],
            "source_id_sha256": audit["source_id_sha256"],
            "source_release_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "freshness_accepted": False, "rights_accepted": False,
            "source_authority_accepted": False, "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(capture_root)}

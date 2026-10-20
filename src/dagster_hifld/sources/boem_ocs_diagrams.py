"""BOEM OCS protraction-diagram footprint candidate from the national GeoSEAS layer.

BOEM's older Data Center ProtractionPolygons downloads describe the New Orleans
Office (Gulf only) and are not a substitute for this national inventory layer.
The selected BOEM_GeoSEAS FeatureServer layer exposes the national OPD footprint
and is queried in bounded, OBJECTID-pinned pages. Its source JSON is retained
byte-for-byte; a GeoParquet candidate keeps all nine source attributes and the
producer's EPSG:3857 polygon coordinates. This is a draft, not legal boundary
geometry or release acceptance.
"""
from __future__ import annotations

import hashlib
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
import geopandas as gpd

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

DATASET_SLUG = "outer-continental-shelf-ocs-diagrams-maps"
FILE_SLUG = DATASET_SLUG
ITEM_ID = "db04973405494cf18acb3135ab6b3326"
ITEM_URL = f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"
SERVICE_URL = "https://services7.arcgis.com/G5Ma95RzqJRPKsWL/arcgis/rest/services/OCS_Protraction_Polygons___1st_Division/FeatureServer"
LAYER_ID = 11
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "OCS Protraction Polygons - 1st Division"
PRODUCER = "Bureau of Ocean Energy Management (BOEM_GeoSEAS)"
REGIONS = {"A": "Atlantic OCS Region", "G": "Gulf of America OCS Region", "P": "Pacific OCS Region", "Y": "Alaska OCS Region"}
PAGE_SIZE = 50
MAX_BODY_BYTES = 32_000_000
EXPECTED_FIELDS = [
    ("OBJECTID", "esriFieldTypeOID", False),
    ("RGN_CODE", "esriFieldTypeString", False),
    ("PROT_NUM", "esriFieldTypeString", False),
    ("PROT_NAME", "esriFieldTypeString", False),
    ("PROT_APRV_DATE", "esriFieldTypeDate", True),
    ("REMARK", "esriFieldTypeString", True),
    ("FED_REG_DATE", "esriFieldTypeDate", True),
    ("Shape__Area", "esriFieldTypeDouble", True),
    ("Shape__Length", "esriFieldTypeDouble", True),
]


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


def _capture_names(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        ("portal_item", ITEM_URL), ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query(where="1=1", returnCountOnly="true")),
        ("ids_before", _query(where="1=1", returnIdsOnly="true")),
    ]
    for offset in range(0, len(ids), PAGE_SIZE):
        part = ids[offset:offset + PAGE_SIZE]
        urls.append((f"page_{offset // PAGE_SIZE:03d}", _query(
            objectIds=",".join(map(str, part)), outFields="*", returnGeometry="true",
            returnZ="true", returnM="true", outSR="3857", orderByFields="OBJECTID ASC")))
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
                          headers={"User-Agent": "hifld-next-datasets/boem-ocs-diagrams-draft"}) as client:
            seed, receipt = capture_get(client, _query(where="1=1", returnIdsOnly="true"), root, "ids_before", max_bytes=MAX_BODY_BYTES)
            captures["ids_before"] = (seed, receipt)
            ids = json.loads(seed).get("objectIds")
            if not isinstance(ids, list) or not ids or any(type(i) is not int for i in ids) or len(ids) != len(set(ids)):
                raise ValueError("source_seed_id_roster_invalid")
            # Capture the pre-count and metadata after acquiring the ID roster.
            urls = [
                ("portal_item", ITEM_URL), ("service_before", SERVICE_URL + "?f=pjson"),
                ("layer_before", LAYER_URL + "?f=pjson"),
                ("count_before", _query(where="1=1", returnCountOnly="true")),
                *[(n, u) for n, u in _capture_names(ids) if n not in {"portal_item", "service_before", "layer_before", "count_before", "ids_before"}],
            ]
            for name, url in urls:
                if name == "ids_before":
                    continue
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


def _validate_source_spatial_reference(value: object, label: str) -> None:
    """Accept only EPSG:3857 and its ArcGIS 102100 alias, including WKT evidence."""
    from pyproj import CRS

    if not isinstance(value, dict):
        raise ValueError(f"{label}_spatial_reference_missing")
    identifiers = [value.get(name) for name in ("wkid", "latestWkid") if value.get(name) is not None]
    if identifiers and any(type(code) is not int or code not in {3857, 102100} for code in identifiers):
        raise ValueError(f"{label}_spatial_reference_drift")
    if not identifiers and not any(value.get(name) for name in ("wkt", "wkt2")):
        raise ValueError(f"{label}_spatial_reference_missing")
    for name in ("wkt", "wkt2"):
        wkt = value.get(name)
        if wkt:
            try:
                if not CRS.from_user_input(wkt).equals(CRS.from_epsg(3857)):
                    raise ValueError(f"{label}_spatial_reference_drift")
            except ValueError:
                raise
            except Exception as exc:
                raise ValueError(f"{label}_spatial_reference_invalid") from exc


def _validate(captures: Mapping[str, tuple[bytes, Mapping[str, object]]]) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    docs = {name: json.loads(body) for name, (body, _) in captures.items()}
    ids_before, ids_after = docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds")
    if (not isinstance(ids_before, list) or not ids_before or sorted(ids_before) != sorted(ids_after or [])
            or len(ids_before) != len(set(ids_before))):
        raise ValueError("source_id_population_changed_during_capture")
    count = len(ids_before)
    if docs["count_before"].get("count") != count or docs["count_after"].get("count") != count:
        raise ValueError("source_count_bracket_mismatch")
    portal, service, layer = docs["portal_item"], docs["service_before"], docs["layer_before"]
    if (portal.get("id") != ITEM_ID or portal.get("owner") != "BOEM_GeoSEAS" or portal.get("access") != "public"
            or portal.get("url") != SERVICE_URL or service.get("serviceItemId") != ITEM_ID):
        raise ValueError("producer_item_identity_or_access_drift")
    if (not any(item.get("id") == LAYER_ID and item.get("name") == LAYER_NAME for item in service.get("layers", []) if isinstance(item, dict))
            or layer.get("name") != LAYER_NAME or layer.get("geometryType") != "esriGeometryPolygon"):
        raise ValueError("source_layer_identity_or_geometry_drift")
    spatial = layer.get("extent", {}).get("spatialReference", {})
    _validate_source_spatial_reference(spatial, "source_layer")
    _validate_source_spatial_reference(layer.get("spatialReference"), "source_layer")
    if layer.get("hasZ") is not False or layer.get("hasM") is not False:
        raise ValueError("source_spatial_contract_drift")
    fields = _profile_fields(layer)
    if fields != _profile_fields(docs["layer_after"]):
        raise ValueError("source_schema_changed_during_capture")
    if docs["service_before"] != docs["service_after"] or layer != docs["layer_after"]:
        raise ValueError("source_metadata_changed_during_capture")
    pages = sorted(name for name in docs if name.startswith("page_"))
    if len(pages) != (count + PAGE_SIZE - 1) // PAGE_SIZE:
        raise ValueError("source_page_roster_incomplete")
    features = []
    for name in pages:
        page = docs[name]
        rows = page.get("features")
        if not isinstance(rows, list) or page.get("exceededTransferLimit") is True:
            raise ValueError("source_page_error_or_truncation")
        _validate_source_spatial_reference(page.get("spatialReference"), "source_page")
        features.extend(rows)
    feature_ids = [f.get("attributes", {}).get("OBJECTID") for f in features if isinstance(f, dict) and isinstance(f.get("attributes"), dict)]
    if (len(features) != count or len(feature_ids) != count or any(type(value) is not int for value in feature_ids)
            or sorted(feature_ids) != sorted(ids_before)):
        raise ValueError("source_feature_population_incomplete")
    field_names = [field["name"] for field in fields]
    regions: dict[str, int] = {}
    for feature in features:
        attrs, geom = feature.get("attributes"), feature.get("geometry")
        if not isinstance(attrs, dict) or set(attrs) != set(field_names) or not isinstance(geom, dict) or not isinstance(geom.get("rings"), list) or not geom["rings"]:
            raise ValueError("source_feature_field_or_geometry_drift")
        if feature.get("spatialReference") is not None:
            _validate_source_spatial_reference(feature["spatialReference"], "source_feature")
        if geom.get("spatialReference") is not None:
            _validate_source_spatial_reference(geom["spatialReference"], "source_geometry")
        if type(attrs.get("OBJECTID")) is not int or attrs["RGN_CODE"] not in REGIONS:
            raise ValueError("source_native_identifier_or_region_invalid")
        if not isinstance(attrs["PROT_NUM"], str) or not attrs["PROT_NUM"]:
            raise ValueError("source_protraction_number_invalid")
        for ring in geom["rings"]:
            if (not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]
                    or any(not isinstance(pt, list) or len(pt) != 2 or any(type(v) not in (int, float) for v in pt) for pt in ring)):
                raise ValueError("source_polygon_coordinates_invalid")
        regions[attrs["RGN_CODE"]] = regions.get(attrs["RGN_CODE"], 0) + 1
    if set(regions) != set(REGIONS):
        raise ValueError("source_missing_required_ocs_region")
    features.sort(key=lambda f: f["attributes"]["OBJECTID"])
    records_sha = _sha(_canonical(features))
    id_sha = _sha(_canonical(sorted(ids_before)))
    schema_sha = _sha(_canonical(fields))
    geometry_sha = _sha(_canonical([[f["attributes"]["OBJECTID"], f["geometry"]] for f in features]))
    audit = {"feature_count": count, "unique_objectid_count": count, "unique_prot_num_count": len({f['attributes']['PROT_NUM'] for f in features}),
             "region_counts": dict(sorted(regions.items())), "source_id_sha256": id_sha,
             "source_record_sha256": records_sha, "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": schema_sha, "source_crs_epsg": 3857,
             "native_geometry_type": "Polygon", "native_dimension": 2, "null_geometry_count": 0,
             "source_release_date": None, "service_modified_epoch_ms": service.get("documentInfo", {}).get("dateLastUpdated"),
             "service_modified_is_source_release_date": False,
             "producer_item_id": ITEM_ID, "producer_owner": portal["owner"], "source_fields": fields,
             "required_regions": REGIONS, "release_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "freshness_accepted": False, "rights_accepted": False,
             "mapping_approved": False}
    return layer, fields, features, audit


def _geoparquet(captures: Mapping[str, tuple[bytes, Mapping[str, object]]], root: Path) -> bytes:
    pages = sorted(name for name in captures if name.startswith("page_"))
    source = root / "combined.esrijson"
    docs = [json.loads(captures[name][0]) for name in pages]
    first = docs[0]
    combined = {k: first[k] for k in ("geometryType", "spatialReference", "fields") if k in first}
    combined["features"] = [feature for doc in docs for feature in doc["features"]]
    source.write_bytes(_canonical(combined))
    frame = pyogrio.read_dataframe(source, driver="ESRIJSON")
    if frame.crs is None or pyproj.CRS.from_user_input(frame.crs) != pyproj.CRS.from_epsg(3857):
        raise ValueError("extracted_crs_mismatch")
    if frame.geometry.isna().any() or frame.geometry.is_empty.any() or not frame.geometry.is_valid.all():
        raise ValueError("extracted_geometry_invalid")
    expected = [field[0] for field in EXPECTED_FIELDS]
    if frame.columns.tolist() != [*expected, "geometry"]:
        raise ValueError("extracted_attribute_roster_mismatch")
    out = root / "candidate.parquet"
    frame.to_parquet(out, index=False, compression="zstd")
    return out.read_bytes()


def capture_boem_ocs_diagrams(storage: StagingStorageResource, *, version: str = "draft",
                              capture_directory: str | None = None, offline_capture_directory: str | None = None) -> dict[str, object]:
    """Capture an unapproved national candidate; explicit offline replay is supported."""
    if (not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource)
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix):
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(p.is_symlink() for p in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    capture_parent = Path(capture_directory).absolute() if capture_directory else Path(tempfile.gettempdir()) / "boem-ocs-captures"
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
                or storage.read_key(input_key) != _canonical({"sha256": fingerprint})+b"\n"):
            raise ValueError("cached_first_candidate_identity_changed")
        # Same content version can be observed on a later refresh. Keep the first
        # sealed receipts and manifest verbatim instead of rewriting timestamps.
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                _sha(_canonical(identity)), identity, audit["source_record_sha256"],
                                audit["source_schema_sha256"], payload["objects"], receipts)
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": audit["feature_count"], "candidate_ready": prefix + "/metadata/candidate_ready.json",
                "region_counts": audit["region_counts"], "source_id_sha256": audit["source_id_sha256"],
                "reused": True, "source_release_accepted": False, "schema_contract_accepted": False,
                "id_contract_accepted": False, "freshness_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False}

    parquet_body = _geoparquet(captures, capture_root)
    parquet_name = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {parquet_name: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    inputs = [{"input_id": n, "producer": PRODUCER, "canonical_url": ITEM_URL if n == "portal_item" else SERVICE_URL,
               "host": urlsplit(receipt["url"]).hostname,
               "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
               "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
               "bytes": receipt["bytes"], "sha256": receipt["sha256"],
               "wire_bytes": receipt.get("wire_bytes"), "wire_sha256": receipt.get("wire_sha256"),
               "license": "https://www.usa.gov/publicdomain/label/1.0/",
               "attribution": "BOEM_GeoSEAS item metadata declares U.S. Government public domain; rights acceptance remains false.",
               "role": "producer_publication",
               "release_date": None, "release_date_unknown_reason": "BOEM service metadata has no dated national source release; ArcGIS modified times are observations, not release dates."}
              for n, (_, receipt) in sorted(captures.items())]
    manifest = {"schema_version": 1, "approved": False,
                "title": "Outer Continental Shelf (OCS) Protraction Diagrams & Leasing Maps",
                "description": "National-scale BOEM OPD footprint polygons, covering Atlantic, Gulf, Pacific and Alaska. Cartographic footprint only; official/legal boundaries are the current BOEM paper/PDF OPD/LM products.",
                "agency": "Bureau of Ocean Energy Management", "publisher": PRODUCER,
                "source_url": LAYER_URL, "source_release_date": None,
                "processor": "HIFLD Next", "code_commit": identity["code_commit"],
                "inputs": inputs,
                "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                                     "parameters": {"layer_id": LAYER_ID, "source_fields": "all nine native fields in producer order; OID and dates retain source meaning", "geometry": "Esri polygon rings read by GDAL; EPSG:3857 retained without reprojection, repair or simplification", "regions": REGIONS, "release_status": "unapproved draft"}}],
                "lineage": "BOEM_GeoSEAS public hosted feature service in BOEM Offshore Marine Cadastre Data Collection (30 CFR 556.8, 585.205, 581.15).",
                "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {"reason": "Source attributes and EPSG:3857 geometry retained for an unapproved candidate.", "objects": [{"path": parquet_name, **source_objects[parquet_name]}]}}},
                "unsupported_output_formats": {
                    "shapefile_zip": "No approved published schema mapping; release remains blocked.",
                    "geopackage": "No approved published schema mapping; release remains blocked.",
                    "pmtiles": "No approved display transformation or generalized geometry.",
                }}
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"source_item_id": ITEM_ID, "source_layer_id": LAYER_ID, "source_layer_name": LAYER_NAME,
                  "source_inputs_sha256": fingerprint, "source_release_accepted": False,
                  "id_contract_accepted": False, "freshness_accepted": False, "rights_accepted": False,
                  "schema_contract_accepted": False, "release_accepted": False})
    outputs = {parquet_name: parquet_body,
               "metadata/source_inputs.json": _canonical({"sha256": fingerprint})+b"\n",
               "metadata/source_manifest.json": _canonical(manifest)+b"\n",
               "metadata/source_audit.json": _canonical(audit)+b"\n",
               "metadata/processing_identity.json": _canonical(identity)+b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes() if (capture_root / f"{name}.wire").exists() else body
        outputs[f"raw/{name}.receipt.json"] = _canonical(dict(receipt))+b"\n"
    ready = {"approved": False, "version": version, "objects": source_objects,
             "source_record_sha256": audit["source_record_sha256"], "release_accepted": False,
             "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready)+b"\n"
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
            "region_counts": audit["region_counts"], "source_id_sha256": audit["source_id_sha256"],
            "source_release_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "freshness_accepted": False, "rights_accepted": False,
            "mapping_approved": False, "release_accepted": False, "capture_directory": str(capture_root)}

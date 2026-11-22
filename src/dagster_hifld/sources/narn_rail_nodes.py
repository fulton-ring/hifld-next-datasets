"""Lossless, approval-gated capture of the FRA North American Rail Network Nodes.

The Federal Railroad Administration (FRA) develops and maintains the North
American Rail Network (NARN) and publishes it through the National
Transportation Atlas Database (NTAD) hosted by the Bureau of Transportation
Statistics (BTS): FRA's own "Maps - Geographic Information System" page
(https://railroads.dot.gov/rail-network-development/maps-and-data/maps-geographic-information-system/maps-geographic,
page footer "Last updated: Thursday, September 4, 2025"; content re-observed
2026-11-13) states "FRA publishes geospatial data through the National
Transportation Atlas Database (NTAD) hosted by the Bureau of Transportation
Statistics (BTS)" and lists "Rail Points -
https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_North_American_Rail_Network_Nodes/FeatureServer/0"
as the exact REST endpoint used here. That FeatureServer is the BTS-hosted view
over FRA's NARN nodes (ArcGIS item 54f40ddee0844fb285dafb13a6b2f623 owned by
``USDOT_BTS`` in the BTS ArcGIS Online organization ``xOi1kZaI0eWDREZv``,
serviceItemId matching, access public, U.S. public domain license). This is the
producing agency's explicitly delegated official distribution; the BTS NTAD
publication is the current, maintained channel (portal description: "was
updated on October 19, 2026 from the Federal Railroad Administration (FRA)";
layer ``editingInfo.dataLastEditDate`` is captured as vintage evidence).
Anonymous direct fetches of the FRA HTML page return HTTP 403 (observed
2026-11-13), so that page is delegation evidence recorded here, never a
runtime input. The HIFLD Open inventory lists this dataset as
``north-american-rail-network-nodes`` (agency: Department of Transportation,
publisher: FRA); its DataLumos project 240894 archive copy is identification
context only, never a pipeline input.

The layer is captured as ordered-offset pages (the service reports
``maxRecordCount`` 2000 and advertises pagination plus ordering) of the sorted
OBJECTID roster: every accepted page must return exactly its expected sorted
id slice, the accepted pages must tile the complete roster without gaps or
overlaps, and the before/after count/id/metadata brackets plus full-record and
geometry digests prove that nothing was skipped, duplicated, or reordered.
Schema, IDs, geometry, coordinates and every attribute value are preserved
unchanged, including leading zeros in the STFIPS/CTYFIPS/STCYFIPS strings and
the producer's nulls.
"""
from __future__ import annotations

from functools import partial
import hashlib
import json
import math
import re
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
from shapely.geometry import Point

from dagster_hifld.http_capture import capture_retention_budget
from dagster_hifld.sources import esri_geometry as crs_validation
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.sources.arcgis_capture import RetainedCaptureView
from dagster_hifld.sources.arcgis_adaptive import SliceCaptureSpec, capture_offset_pages, capture_offset_documents
from dagster_hifld.sources.arcgis_layer import (
    capture_witness, offset_layer_capture_roster, read_adaptive_offset_replay,
    capture_with_retry as _capture_with_retry, producer_error_doc as _producer_error_doc,
)
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.capture_common import (
    candidate_prefix, content_derived_version,
    processing_identity, quarantine_incomplete_capture, seal_ready_candidate,
    verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "north-american-rail-network-nodes"
FILE_SLUG = DATASET_SLUG
ITEM_ID = "54f40ddee0844fb285dafb13a6b2f623"
ITEM_ORG_ID = "xOi1kZaI0eWDREZv"
SERVICE_URL = ("https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
               "NTAD_North_American_Rail_Network_Nodes/FeatureServer")
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "North American Rail Network Nodes"
PUBLISHER = ("Federal Railroad Administration (FRA), published through the USDOT Bureau "
             "of Transportation Statistics (BTS) National Transportation Atlas Database (NTAD)")
AGENCY = "Department of Transportation"
SOURCE_CRS_WKID = 4326
FRA_GIS_PAGE_URL = ("https://railroads.dot.gov/rail-network-development/maps-and-data/"
                    "maps-geographic-information-system/maps-geographic")
# Initial page width, capped by the producer's maxRecordCount (2000). Any page
# whose response the producer refuses or drops mid-stream is bisected until the
# producer serves both halves; see the module docstring.
PAGE_START_RECORDS = 2000
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MAX_CAPTURE_TOTAL_BYTES = 512 * 1024 * 1024
MIN_FREE_BYTES = 4_000_000_000
# Every request retries a bounded number of times on transient producer error
# documents or incomplete transport captures; a retry re-issues the exact same
# request, and completeness is still enforced by the captured before/after id
# rosters, counts, exact page tiling, and full-record digests, so retries can
# never mask drift. Pages wider than one record that keep failing are bisected;
# a single-record page that keeps failing fails the capture explicitly.
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
PAGE_PROBE_ATTEMPTS = 2
PAGE_PROBE_BASE_DELAY_SECONDS = 5.0
EXPECTED_SCHEMA_SHA256 = "ae7b8bf0a22ba6b0f7150fa32a315f677fa651230d38450e37a9ad902df10040"
EXPECTED_NULL_GEOMETRY_COUNT = 0
EXPECTED_INVALID_GEOMETRY_COUNT = 0
EXPECTED_FRANODEID_NULL_COUNT = 0
EXPECTED_FRANODEID_DUPLICATE_COUNT = 0
EXPECTED_FIELD_NAMES = ["OBJECTID", "FRANODEID", "COUNTRY", "STATE", "STFIPS", "CTYFIPS",
                        "STCYFIPS", "FRADISTRCT", "PASSNGR", "PASSNGRSTN", "BNDRY"]
EXPECTED_FIELD_TYPES = ["esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeString",
                        "esriFieldTypeString", "esriFieldTypeString", "esriFieldTypeString",
                        "esriFieldTypeString", "esriFieldTypeSmallInteger", "esriFieldTypeString",
                        "esriFieldTypeString", "esriFieldTypeSmallInteger"]
EXPECTED_SERVICE_LAYER_ROSTER = [(LAYER_ID, LAYER_NAME, "Feature Layer")]


def _list_sha(items: list) -> str:
    """Streamed sha256 of the canonical JSON encoding of ``items``.

    Byte-identical to ``_sha(_canonical(items))`` without materializing the
    complete document twice.
    """
    digest = hashlib.sha256()
    digest.update(b"[")
    for index, item in enumerate(items):
        if index:
            digest.update(b",")
        digest.update(_canonical(item))
    digest.update(b"]")
    return digest.hexdigest()


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain")
            if key in field}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


_PAGE_NAME_PATTERN = re.compile(r"page_([0-9]{6})_([0-9]{5})$")


def _page_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


def _parse_page_name(name: str) -> tuple[int, int]:
    match = _PAGE_NAME_PATTERN.fullmatch(name)
    if match is None:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    return int(match.group(1)), int(match.group(2))


def _page_query(offset: int, count: int) -> str:
    return _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                   "outSR": "4326", "orderByFields": "OBJECTID ASC", "resultOffset": offset,
                   "resultRecordCount": count, "f": "json"})


def _capture_roster(total: int):
    return offset_layer_capture_roster(total, layer_url=LAYER_URL, service_url=SERVICE_URL,
        page_size=PAGE_START_RECORDS,
        extra_documents=(("portal_item", f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"),),
        page_params={"outSR": "4326"},
        page_namer=lambda _index, offset: _page_name(offset, min(PAGE_START_RECORDS, total - offset)),
        after_order=("layer_after", "service_after", "count_after", "ids_after"))


def _doc_urls() -> list[tuple[str, str]]:
    return [(name, url) for name, url in _capture_roster(0)
            if name == 'portal_item' or name.endswith('_before')]


def _after_doc_urls() -> list[tuple[str, str]]:
    return [(name, url) for name, url in _capture_roster(0) if name.endswith('_after')]


_validate_spatial_reference = partial(
    crs_validation.validate_advertised_wkids,
    native_wkid=SOURCE_CRS_WKID)


def _collect_page(offset: int, count: int, total_count: int, page: object, ids: list[int],
                  features: list[dict[str, object]]) -> None:
    if not isinstance(page, dict) or "error" in page:
        raise ValueError("source_page_error")
    _validate_spatial_reference(page.get("spatialReference"), f"source_page_{offset:06d}")
    rows = page.get("features")
    if not isinstance(rows, list):
        raise ValueError("source_page_missing_features")
    expected = ids[offset:offset + count]
    actual = [row.get("attributes", {}).get("OBJECTID") for row in rows if isinstance(row, dict)]
    if actual != expected or len(rows) != len(expected):
        raise ValueError("source_page_id_slice_incomplete_or_reordered")
    if page.get("exceededTransferLimit") is True and offset + count >= total_count:
        raise ValueError("source_page_unexpected_exceeded_transfer_limit")
    features.extend(rows)


def _classify_geometries(features: list[dict[str, object]], attr_names: list[str]):
    """Point geometry audit: [OBJECTID, x, y] triples (or [OBJECTID, None]).

    The producer publishes plain EPSG:4326 point geometries; a geometry that is
    not exactly an ``x``/``y`` pair of finite numbers is counted as invalid and
    the capture fails closed (never repaired or coerced).
    """
    invalid_geometry = 0
    null_geometry = 0
    geometry_records: list[list] = []
    xmin = ymin = xmax = ymax = None
    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict) or list(attrs) != attr_names:
            raise ValueError("source_record_attribute_schema_drift")
        geom = feature.get("geometry")
        oid = attrs["OBJECTID"]
        if geom is None:
            null_geometry += 1
            geometry_records.append([oid, None])
            continue
        if not isinstance(geom, dict) or set(geom) != {"x", "y"} \
                or not all(type(geom[key]) in (int, float) for key in ("x", "y")):
            invalid_geometry += 1
            geometry_records.append([oid, "INVALID", geom])
            continue
        x, y = geom["x"], geom["y"]
        if not (-180 <= x <= 180 and -90 <= y <= 90):
            raise ValueError("source_coordinate_range_drift")
        if xmin is None or x < xmin:
            xmin = x
        if xmax is None or x > xmax:
            xmax = x
        if ymin is None or y < ymin:
            ymin = y
        if ymax is None or y > ymax:
            ymax = y
        geometry_records.append([oid, x, y])
    observed_bounds = None if xmin is None else {"xmin": xmin, "ymin": ymin,
                                                 "xmax": xmax, "ymax": ymax}
    return geometry_records, null_geometry, invalid_geometry, observed_bounds


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]):
    docs = {name: json.loads(body) for name, (body, _) in captures.items()
            if not name.startswith("page_")}
    page_names = sorted(name for name in captures if name.startswith("page_"))
    pages = [_parse_page_name(name) for name in page_names]
    position = 0
    for offset, count in pages:
        if offset != position:
            raise ValueError("source_page_tiling_incomplete_or_overlapped")
        position += count
    source_count = docs["count_before"].get("count")
    if position != source_count:
        raise ValueError("source_page_tiling_incomplete_or_overlapped")
    if set(captures) != set(docs) | {*page_names}:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    portal = docs["portal_item"]
    if (not isinstance(portal, dict) or portal.get("id") != ITEM_ID
            or portal.get("owner") != "USDOT_BTS" or portal.get("orgId") != ITEM_ORG_ID
            or portal.get("title") != LAYER_NAME or portal.get("access") != "public"
            or portal.get("type") != "Feature Service" or portal.get("url") != SERVICE_URL):
        raise ValueError("source_portal_item_identity_drift")
    description = str(portal.get("description", ""))
    if ("Federal Railroad Administration (FRA)" not in description
            or "National Transportation Atlas Database (NTAD)" not in description
            or "North American Rail Network" not in description):
        raise ValueError("official_FRA_NTAD_relationship_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if not isinstance(service, dict) or service.get("serviceItemId") != ITEM_ID:
        raise ValueError("official_BTS_narn_rail_nodes_service_identity_drift")
    service_layers = service.get("layers")
    roster = [(x.get("id"), x.get("name"), x.get("type")) for x in service_layers
              if isinstance(x, dict)] if isinstance(service_layers, list) else None
    if roster != EXPECTED_SERVICE_LAYER_ROSTER:
        raise ValueError("official_BTS_narn_rail_nodes_service_identity_drift")
    if (not isinstance(layer, dict) or layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("objectIdField") != "OBJECTID" or layer.get("hasZ") is not False
            or layer.get("hasM") is not False):
        raise ValueError("exact_BTS_narn_rail_nodes_layer_identity_drift")
    capabilities = layer.get("advancedQueryCapabilities")
    if not (isinstance(capabilities, dict) and capabilities.get("supportsPagination") is True
            and capabilities.get("supportsOrderBy") is True):
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != 2000:
        raise ValueError("source_record_limit_drift")
    extent = layer.get("extent", {})
    _validate_spatial_reference(extent.get("spatialReference"), "source_layer_extent")
    declared_extent = {key: extent.get(key) for key in ('xmin', 'ymin', 'xmax', 'ymax')}
    if (any(type(value) not in (int, float) or not math.isfinite(value) for value in declared_extent.values())
            or declared_extent['xmin'] > declared_extent['xmax'] or declared_extent['ymin'] > declared_extent['ymax']
            or extent != docs['layer_after'].get('extent')):
        raise ValueError("source_layer_extent_drift")
    fields_value = layer.get("fields")
    if not isinstance(fields_value, list) or any(not isinstance(x, dict) for x in fields_value):
        raise ValueError("source_field_roster_drift")
    fields = [_plain_field(x) for x in fields_value]
    schema_sha = _sha(_canonical(fields))
    if schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_contract_drift")
    if ([f.get("name") for f in fields] != EXPECTED_FIELD_NAMES
            or [f.get("type") for f in fields] != EXPECTED_FIELD_TYPES):
        raise ValueError("source_schema_roster_drift")
    if docs["layer_after"].get("fields") is None \
            or [_plain_field(x) for x in docs["layer_after"]["fields"]] != fields:
        raise ValueError("source_field_schema_changed_during_capture")
    for name in ("layer_before", "layer_after"):
        if docs[name] != layer:
            raise ValueError("source_layer_metadata_changed_during_capture")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID"
           for name in ("ids_seed", "ids_before", "ids_after")):
        raise ValueError("source_id_field_changed")
    ids = capture_witness(captures)
    seed = docs["ids_seed"].get("objectIds")
    if not isinstance(seed, list) or sorted(seed) != ids:
        raise ValueError("source_id_population_drift")

    features: list[dict[str, object]] = []
    for (offset, count), name in zip(pages, page_names):
        _collect_page(offset, count, len(ids), json.loads(captures[name][0]), ids, features)
    attr_names = [field["name"] for field in fields]
    geometry_records, null_geometry, invalid_geometry, bounds = _classify_geometries(
        features, attr_names)
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    if (null_geometry != EXPECTED_NULL_GEOMETRY_COUNT
            or invalid_geometry != EXPECTED_INVALID_GEOMETRY_COUNT):
        raise ValueError("source_geometry_population_drift")
    record_sha = _list_sha(features)
    geometry_sha = _list_sha(geometry_records)
    franodeids = [row["attributes"].get("FRANODEID") for row in features]
    franodeid_nulls = sum(1 for value in franodeids if value is None)
    franodeid_duplicates = len(franodeids) - len({value for value in franodeids
                                                  if value is not None})
    if franodeid_nulls != EXPECTED_FRANODEID_NULL_COUNT:
        raise ValueError("source_feature_id_null_drift")
    if franodeid_duplicates != EXPECTED_FRANODEID_DUPLICATE_COUNT:
        raise ValueError("source_feature_id_duplicate_drift")
    audit = {"source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": schema_sha, "source_crs_wkid": SOURCE_CRS_WKID,
             "source_geometry_type": "esriGeometryPoint",
             "null_geometry_count": null_geometry,
             "invalid_geometry_count": invalid_geometry,
             "observed_bounds": bounds, "declared_layer_extent": declared_extent,
             "unique_object_id_count": len(ids), "unique_franodeid_count":
                 len({value for value in franodeids if value is not None}),
             "franodeid_null_count": franodeid_nulls,
             "franodeid_duplicate_count": franodeid_duplicates,
             "source_page_count": len(pages), "source_release_date": None,
             "source_release_date_unknown_reason":
                 "The BTS NTAD service publishes no per-snapshot release date for this layer; "
                 "the portal description update date and the layer editingInfo timestamps are "
                 "retained as separate vintage evidence, not as an accepted release date."}
    return layer, fields_value, {"features": features, "audit": audit, "ids": ids}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeInteger": pa.int32(),
            "esriFieldTypeSmallInteger": pa.int16(),
            "esriFieldTypeDouble": pa.float64(),
            "esriFieldTypeSingle": pa.float32(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDate": pa.int64(),
            "esriFieldTypeGUID": pa.string(),
            "esriFieldTypeGlobalID": pa.string()}[str(field["type"])]


def _parquet_bytes(fields: list[dict[str, object]], features: list[dict[str, object]]) -> bytes:
    columns = {}
    schema_fields = []
    for field in fields:
        name = field["name"]
        typ = _arrow_type(field)
        values = [row["attributes"].get(name) for row in features]
        columns[name] = pa.array(values, type=typ)
        schema_fields.append(pa.field(name, typ, nullable=True))
    columns["geometry"] = pa.array([
        None if row.get("geometry") is None else Point(row["geometry"]["x"], row["geometry"]["y"]).wkb
        for row in features
    ], type=pa.binary())
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["Point"],
        "crs": pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict(), "edges": "planar"}}}
    table = pa.Table.from_pydict(columns, schema=pa.schema(schema_fields).with_metadata(
        {b"geo": _canonical(geo)}))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _code_identity() -> dict[str, object]:
    return processing_identity(Path(__file__).parents[3])


def _slice_capture_spec() -> SliceCaptureSpec:
    return SliceCaptureSpec(
        byte_budget=MAX_CAPTURE_TOTAL_BYTES,
        page_name=_page_name, page_url=_page_query,
        capture=partial(_capture_with_retry, max_bytes=MAX_CAPTURE_BYTES), producer_error=_producer_error_doc,
        page_attempts=PAGE_PROBE_ATTEMPTS, page_delay=PAGE_PROBE_BASE_DELAY_SECONDS,
        doc_attempts=DOC_CAPTURE_ATTEMPTS, doc_delay=DOC_CAPTURE_BASE_DELAY_SECONDS,
    )


def _read_replay(root: Path) -> dict[str, tuple[bytes, dict[str, object]]]:
    urls = dict(_doc_urls() + _after_doc_urls())
    urls['ids_seed'] = _query({'where': '1=1', 'returnIdsOnly': 'true', 'f': 'json'})
    return read_adaptive_offset_replay(root, document_urls=urls, page_size=PAGE_START_RECORDS,
        page_name=_page_name, parse_page_name=_parse_page_name, page_query=_page_query,
        max_capture_bytes=MAX_CAPTURE_BYTES, max_capture_total_bytes=MAX_CAPTURE_TOTAL_BYTES,
        doc_capture_attempts=DOC_CAPTURE_ATTEMPTS, page_probe_attempts=PAGE_PROBE_ATTEMPTS)


def capture_narn_rail_nodes(storage: StagingStorageResource, *, version: str,
                            capture_directory: str | None = None,
                            offline_capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved NARN Rail Nodes candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"
                          for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise ValueError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else default_capture_dir("hifld-native-reconstruction", "248", "captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures = RetainedCaptureView(capture_root)
    try:
        if offline_capture_directory:
            offline = Path(offline_capture_directory).absolute()
            _read_replay(offline)
            for path in offline.iterdir():
                shutil.copyfile(path, capture_root / path.name)
            captures = _read_replay(capture_root)
        else:
            with capture_retention_budget(capture_root, MAX_CAPTURE_TOTAL_BYTES), httpx.Client(follow_redirects=False, timeout=180,
                              headers={"User-Agent": "hifld-next-datasets/narn-rail-nodes-draft"}) as client:
                seed_body, seed_receipt = _capture_with_retry(
                    client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                    capture_root, "ids_seed", max_bytes=MAX_CAPTURE_BYTES,
                    attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
                seed = json.loads(seed_body).get("objectIds")
                if (not isinstance(seed, list) or not seed
                        or any(type(value) is not int for value in seed) or len(set(seed)) != len(seed)):
                    raise ValueError("source_seed_id_population_drift")
                seed_count = len(seed)
                captures["ids_seed"] = (seed_body, seed_receipt)
                captured_total = seed_receipt["bytes"] + seed_receipt["wire_bytes"]
                captured_total = capture_offset_documents(_slice_capture_spec(), client, capture_root, _doc_urls(), captures,
                    retained_bytes=captured_total, max_total_bytes=MAX_CAPTURE_TOTAL_BYTES)
                layer_doc = json.loads(captures["layer_before"][0])
                if layer_doc.get("maxRecordCount") != 2000 or PAGE_START_RECORDS > layer_doc["maxRecordCount"]:
                    raise ValueError("source_record_limit_drift")
                captures.update(capture_offset_pages(_slice_capture_spec(), client, capture_root, seed_count, page_size=PAGE_START_RECORDS))
                captured_total += sum(receipt["bytes"] + receipt["wire_bytes"]
                                      for _, receipt in _page_receipts(captures))
                if captured_total > MAX_CAPTURE_TOTAL_BYTES:
                    raise ValueError("source_capture_exceeds_scratch_budget")
                captured_total = capture_offset_documents(_slice_capture_spec(), client, capture_root,
                    _after_doc_urls(), captures, retained_bytes=captured_total,
                    max_total_bytes=MAX_CAPTURE_TOTAL_BYTES)
                capture_witness(captures)
        layer, source_fields, extracted = _validate_captures(captures)
    except Exception as exc:
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try:
                receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError):
                receipts[path.name.removesuffix(".receipt.json")] = {"receipt_readback_failed": True}
        (capture_root / "capture_manifest.json").write_bytes(
            _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
                        "error_code": type(exc).__name__, "receipts": receipts}) + b"\n")
        retained = RetainedCaptureView(capture_root, captures)
        for receipt_path in capture_root.glob("*.receipt.json"):
            name = receipt_path.name.removesuffix(".receipt.json")
            if name in retained:
                continue
            try:
                retained[name] = ((capture_root / f"{name}.body").read_bytes(),
                                  json.loads(receipt_path.read_bytes()))
            except (OSError, ValueError):
                continue
        if retained:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version, retained,
                                          type(exc).__name__)
        raise
    identity = _code_identity()
    parquet_body = _parquet_bytes(source_fields, extracted["features"])
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    portal_doc = json.loads(captures["portal_item"][0])
    service_doc = json.loads(captures["service_before"][0])
    layer_doc = json.loads(captures["layer_before"][0])
    vintage_evidence = {
        "portal_description": portal_doc.get("description"),
        "portal_item_created_epoch_ms": portal_doc.get("created"),
        "portal_item_modified_epoch_ms": portal_doc.get("modified"),
        "service_editing_info": service_doc.get("editingInfo"),
        "layer_editing_info": layer_doc.get("editingInfo"),
        "fra_gis_page_delegation": (
            "FRA's own Maps - Geographic Information System page states FRA publishes geospatial "
            "data through the NTAD hosted by BTS and lists this exact FeatureServer URL as "
            "'Rail Points'; the page is delegation evidence (anonymous direct fetch returns 403), "
            "not a runtime input."),
        "release_date_selected": None,
        "freshness_accepted": False,
        "assessment": (
            "Portal description dates and ArcGIS item/service/layer editing metadata are "
            "preserved separately; none is treated as an independently verified source release "
            "date. BTS publishes this NTAD layer on a quarterly maintenance cadence."),
    }
    manifest = {
        "schema_version": 1, "approved": False, "title": f"{LAYER_NAME} (FRA NARN)",
        "description": "Federal Railroad Administration (FRA) North American Rail Network (NARN) "
                       "nodes, captured completely from the FRA-delegated BTS NTAD FeatureServer "
                       "publication (NTAD item 54f40ddee0844fb285dafb13a6b2f623, owner USDOT_BTS). "
                       "Complete source attributes and EPSG:4326 point geometry are represented in "
                       "the candidate. Source release date/freshness, rights, public schema/ID "
                       "contract, and release acceptance remain unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": AGENCY, "source_url": LAYER_URL,
        "license": "https://www.usa.gov/publicdomain/label/1.0/",
        "source_modified": None, "date_issued": None, "date_modified": None,
        "source_vintage_evidence": vintage_evidence,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER,
                    "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication",
                    "canonical_url": (f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"
                                      if name == "portal_item" else LAYER_URL),
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason": "The NTAD publication carries no per-snapshot "
                                                   "release date; portal and service edit "
                                                   "timestamps are retained as separate vintage "
                                                   "evidence.",
                    "license": "https://www.usa.gov/publicdomain/label/1.0/",
                    "attribution": "BTS NTAD; item metadata states the work is U.S. public domain; "
                                   "rights review remains pending."}
                   for name, (_, receipt) in sorted(captures.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "source_fields": "all eleven source fields retained in "
                                                             "producer order and physical types "
                                                             "(OBJECTID, FRANODEID, COUNTRY, STATE, "
                                                             "STFIPS, CTYFIPS, STCYFIPS, FRADISTRCT, "
                                                             "PASSNGR, PASSNGRSTN, BNDRY)",
                                            "source_ids": "Preserve producer OBJECTID and FRA "
                                                          "FRANODEID values unchanged (both fully "
                                                          "populated and unique in the captured "
                                                          "roster); no substitute IDs, casts, "
                                                          "trimming, or joins",
                                            "geometry": "Source EPSG:4326 point x/y retained as WKB "
                                                        "without reprojection or repair; null source "
                                                        "geometry retained as null",
                                            "candidate_status": "unapproved; publication blocked "
                                                                "pending freshness, schema/ID "
                                                                "contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "FRA develops and maintains the North American Rail Network (NARN) nodes and "
                   "publishes it through the National Transportation Atlas Database (NTAD) hosted "
                   "by BTS: FRA's own Maps - Geographic Information System page "
                   "(" + FRA_GIS_PAGE_URL + ") lists "
                   "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
                   "NTAD_North_American_Rail_Network_Nodes/FeatureServer/0 as the Rail Points REST "
                   "endpoint used here. The captured ArcGIS item is owned by USDOT_BTS in the BTS "
                   "organization, its serviceItemId matches, access is public, and the item "
                   "license states U.S. public domain. The HIFLD Open inventory lists this dataset "
                   "as north-american-rail-network-nodes (agency DOT, publisher FRA); its DataLumos "
                   "project 240894 archive copy and modification date are identification and "
                   "comparison context only, never pipeline inputs. The data dictionary DOI "
                   "10.21949/1529070 cited by the producer is reference documentation.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls, and point "
                      "geometry retained in the producer CRS; all release and compatibility "
                      "approvals remain pending.",
            "objects": [{"path": rel, **source_objects[rel]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit = {**extracted["audit"], "source_fields": [_plain_field(f) for f in source_fields],
             "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID, "service_url": SERVICE_URL,
             "arcgis_item_id": ITEM_ID, "arcgis_item_owner": portal_doc.get("owner"),
             "source_relationship": "FRA-maintained NARN nodes, BTS NTAD delegated publication",
             "source_vintage_evidence": vintage_evidence, "release_date_unknown": True,
             "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
             "release_accepted": False}
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    (capture_root / "capture_manifest.json").write_bytes(
        _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
                    "source_feature_count": len(extracted["features"]),
                    "source_record_sha256": audit["source_record_sha256"],
                    "source_geometry_sha256": audit["source_geometry_sha256"],
                    "artifact_roster": capture_roster_digests(capture_root, exclude={'capture_manifest.json'}),
                    "receipts": {name: receipt for name, (_, receipt) in sorted(captures.items())}}) + b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs = {rel: parquet_body, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    outputs["metadata/candidate_ready.json"] = _canonical(
        {"approved": False, "version": version, "objects": source_objects,
         "source_record_sha256": audit["source_record_sha256"], "freshness_accepted": False,
         "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
         "mapping_approved": False, "release_accepted": False}) + b"\n"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("cached_first_candidate_invalid")
        objects = payload.get("objects", {})
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version,
                                fingerprint, identity, audit["source_record_sha256"],
                                audit["source_schema_sha256"], objects,
                                payload.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": len(extracted["features"]),
                "source_reported_count": len(extracted["ids"]),
                "source_population_complete": True,
                "source_id_sha256": audit["source_id_sha256"],
                "source_null_geometry_count": audit["null_geometry_count"],
                "source_invalid_geometry_count": audit["invalid_geometry_count"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
    if storage.list_prefix(prefix):
        raise ValueError('unsealed_candidate_prefix:' + prefix)
    for relative, body in outputs.items():
        write_immutable(storage, f"{prefix}/{relative}", body)
    descriptors = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in outputs.items()}
    for name, descriptor in descriptors.items():
        key = f"{prefix}/{name}"
        snap = storage.object_snapshot(key)
        if snap is None or snap.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] \
                or storage.object_snapshot(key) != snap:
            raise ValueError("candidate_output_physical_readback_failed:" + name)
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": {name: receipt for name, (_, receipt) in captures.items()},
               "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"],
                                        identity["files"], identity=identity)
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": len(extracted["features"]),
            "source_reported_count": len(extracted["ids"]),
            "source_population_complete": True,
            "source_id_sha256": audit["source_id_sha256"],
            "source_null_geometry_count": audit["null_geometry_count"],
            "source_invalid_geometry_count": audit["invalid_geometry_count"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(capture_root)}


def _page_receipts(captures: dict[str, tuple[bytes, dict[str, object]]]):
    for name, (_, receipt) in sorted(captures.items()):
        if name.startswith("page_"):
            yield name, receipt

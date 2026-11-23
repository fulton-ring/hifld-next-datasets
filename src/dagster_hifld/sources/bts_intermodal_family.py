"""Shared capture, page planning, validation and candidate staging for the
BTS NTAD Intermodal Freight Facilities point layers.

The air-to-truck, marine roll-on/roll-off and rail TOFC/COFC FeatureServer
layers (services.arcgis.com/xOi1kZaI0eWDREZv/.../NTAD_Intermodal_Freight_Facilities_<X>/
FeatureServer/0, all owned by USDOT_BTS) were byte-copy adapters differing only
in per-layer pins; each frozen spec below holds those pins, and the capture,
ordered-offset/bounded-ObjectID page planning, source validation and unapproved
candidate construction are implemented once here. The thin dataset modules
(``bts_air_truck``, ``bts_marine_roro``, ``bts_rail_tofc_cofc``) keep the
public ``capture_bts_<x>`` entry point and the constants their asset modules
import.

``query_url``, ``object_id_chunks``, ``page_plan`` and
``verify_first_candidate`` are shared with ``sources/epa_frs_tri.py`` (4269
outSR), which keeps its own validation because its portal, pin and CRS checks
differ. ``bts_pipeline_terminals`` (open issue #411) and
``national_transit_map_routes`` (different query shape: no outSR/returnM, page
count cap) keep their local planner copies until separate consolidation.

Every candidate stays unapproved: the runtime approval flags remain false and
the draft schedules stopped.
"""
from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
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

from dagster_hifld.http_capture import CaptureDocuments
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.arcgis_capture import capture_layer, RetainedCaptureView
from dagster_hifld.sources.arcgis_capture_sequence import LayerCaptureFetch
from dagster_hifld.sources.capture_common import (
    candidate_prefix, content_derived_version, processing_identity,
    quarantine_incomplete_capture, verify_cached_candidate,
    seal_candidate_objects,
    write_candidate_outputs,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha


@dataclass(frozen=True)
class BtsIntermodalSpec:
    """The per-layer pins one Intermodal Freight Facilities adapter supplies."""

    dataset_slug: str
    file_slug: str
    publisher: str
    item_id: str
    service_url: str
    layer_id: int
    layer_name: str
    page_size: int
    max_body_bytes: int
    max_capture_total_bytes: int
    max_object_id_url_length: int
    native_wkid: int
    expected_source_schema_sha256: str
    expected_source_id_sha256: str
    expected_source_count: int
    expected_field_names: tuple
    expected_field_types: tuple
    description_token: str
    layer_identity_error: str
    user_agent: str
    capture_root_id: str
    source_fields_text: str
    datalumos_project: int
    include_source_vintage_evidence: bool
    release_date_unknown_reason: str

    @property
    def layer_url(self) -> str:
        return f"{self.service_url}/{self.layer_id}"


AIR_TO_TRUCK_SPEC = BtsIntermodalSpec(
    dataset_slug="intermodal-freight-facilities-air-to-truck",
    file_slug="intermodal-freight-facilities-air-to-truck",
    publisher="U.S. Department of Transportation, Bureau of Transportation Statistics (USDOT_BTS)",
    item_id="e73c3b5df80f42e79e2212a478392544",
    service_url="https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Intermodal_Freight_Facilities_Air_to_Truck/FeatureServer",
    layer_id=0,
    layer_name="Intermodal Freight Facilities Air-to-Truck",
    page_size=1000,
    max_body_bytes=8_000_000,
    max_capture_total_bytes=480_000_000,
    max_object_id_url_length=1800,
    native_wkid=4326,
    expected_source_schema_sha256="5b5a5c0d95f05121b8175395ef3377ba4858fa5a1433238b0bb64a774cb71d69",
    expected_source_id_sha256="e7d57716b91b78ea204a1bb5ebd57b0ee17353f71f9824addcd18c22061919db",
    expected_source_count=404,
    expected_field_names=("OBJECTID", "LOCID", "FACILITY_C", "DATE", "EST_AREA"),
    expected_field_types=("esriFieldTypeOID", "esriFieldTypeString", "esriFieldTypeString",
                          "esriFieldTypeString", "esriFieldTypeDouble"),
    description_token="Air-to-Truck",
    layer_identity_error="exact_BTS_air_to_truck_layer_identity_drift",
    user_agent="hifld-next-datasets/bts-air-to-truck-draft",
    capture_root_id="126",
    source_fields_text="all five source fields retained with declared source types; DATE is the source string field",
    datalumos_project=239366,
    include_source_vintage_evidence=False,
    release_date_unknown_reason="ArcGIS modified timestamp is not a source release date.",
)

MARINE_RORO_SPEC = BtsIntermodalSpec(
    dataset_slug="intermodal-freight-facilities-marine-roll-on-roll-off",
    file_slug="intermodal-freight-facilities-marine-roll-on-roll-off",
    publisher="U.S. Department of Transportation, Bureau of Transportation Statistics (USDOT_BTS)",
    item_id="ff5f787f987c4686b3a79d3dc5b3daa2",
    service_url="https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Intermodal_Freight_Facilities_Marine_Roll_on_Roll_off/FeatureServer",
    layer_id=0,
    layer_name="Intermodal Freight Facilities Marine Roll-on/Roll-off",
    page_size=1000,
    max_body_bytes=1_000_000,
    max_capture_total_bytes=2_000_000,
    max_object_id_url_length=1800,
    native_wkid=4326,
    expected_source_schema_sha256="8080a33e136b55a194e8786dae203069fd44c02f6bb4ca8501db5a893c495972",
    expected_source_id_sha256="865f85261b03d18843adc4a68e28bb003422ff44d6d6f6f8b8f3835eb126e5a3",
    expected_source_count=84,
    expected_field_names=("OBJECTID", "PORT", "TERMINAL", "OPERATED_BY", "TERM_ADDRESS", "CITY",
                          "STATE", "ZIP_CODE", "RAIL_CO", "EQUIPMENT", "ACCESS_RD", "LAT", "LON",
                          "NAV_UNIT_ID"),
    expected_field_types=("esriFieldTypeOID", *("esriFieldTypeString",) * 10,
                          "esriFieldTypeDouble", "esriFieldTypeDouble", "esriFieldTypeString"),
    description_token="Marine Roll-on/Roll-off",
    layer_identity_error="exact_BTS_marine_roll_on_roll_off_layer_identity_drift",
    user_agent="hifld-next-datasets/bts-marine-roro-draft",
    capture_root_id="127",
    source_fields_text="all fourteen source fields retained with declared source types",
    datalumos_project=239382,
    include_source_vintage_evidence=False,
    release_date_unknown_reason="ArcGIS modified timestamp is not a source release date.",
)

RAIL_TOFC_COFC_SPEC = BtsIntermodalSpec(
    dataset_slug="intermodal-freight-facilities-rail-tofc-cofc",
    file_slug="intermodal-freight-facilities-rail-tofc-cofc",
    publisher="U.S. Department of Transportation, Bureau of Transportation Statistics (USDOT_BTS)",
    item_id="e6332597fcd546d3899f60ec9b9157b9",
    service_url="https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Intermodal_Freight_Facilities_Rail_TOFC_COFC/FeatureServer",
    layer_id=0,
    layer_name="Intermodal Freight Facilities Rail TOFC/COFC",
    page_size=1000,
    max_body_bytes=1_000_000,
    max_capture_total_bytes=2_000_000,
    max_object_id_url_length=1800,
    native_wkid=4326,
    expected_source_schema_sha256="2d38415a5d7ff8ee1d6eee5123ecb70303904a51afcd7e43ef1500b4cf9e6b0f",
    expected_source_id_sha256="d3cf1ddd67aa002b0a1409cd70096c18e40cb24a3ecb0609603d6b82903ad228",
    expected_source_count=241,
    expected_field_names=("OBJECTID", "TERMINAL", "TERM_ADDRESS", "CITY", "STATE", "ZIP_CODE",
                          "PORT", "RAIL_CO", "EQUIPMENT", "ACCESS_RD", "SPLC", "LAT", "LON"),
    expected_field_types=("esriFieldTypeOID", *("esriFieldTypeString",) * 10,
                          "esriFieldTypeDouble", "esriFieldTypeDouble"),
    description_token="Rail TOFC/COFC",
    layer_identity_error="exact_BTS_rail_tofc_cofc_layer_identity_drift",
    user_agent="hifld-next-datasets/bts-rail-tofc-cofc-draft",
    capture_root_id="127",
    source_fields_text="all thirteen source fields retained with declared source types",
    datalumos_project=239441,
    include_source_vintage_evidence=True,
    release_date_unknown_reason=("Portal description and ArcGIS item/service/layer editing "
                                 "timestamps are retained separately; none is independently "
                                 "verified as a source release date."),
)


def plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def arrow_type(field: Mapping[str, object]) -> pa.DataType:
    # This is a proposed native-preservation encoding. Esri dates remain their
    # source epoch-millisecond integers; review can approve a richer mapping.
    # Unlike esri_common.arrow_type this map fails closed on unknown types.
    return {
        "esriFieldTypeOID": pa.int64(), "esriFieldTypeInteger": pa.int32(),
        "esriFieldTypeSmallInteger": pa.int16(), "esriFieldTypeDouble": pa.float64(),
        "esriFieldTypeSingle": pa.float32(), "esriFieldTypeString": pa.string(),
        "esriFieldTypeDate": pa.int64(), "esriFieldTypeGUID": pa.string(),
        "esriFieldTypeGlobalID": pa.string(),
    }[str(field["type"])]


def query_url(layer_url: str, **params: object) -> str:
    return layer_url + "/query?" + urlencode(params)


def source_metadata_urls(spec: BtsIntermodalSpec) -> list[tuple[str, str]]:
    return [
        ("portal_item", f"https://www.arcgis.com/sharing/rest/content/items/{spec.item_id}?f=json"),
        ("service_before", spec.service_url + "?f=pjson"),
        ("layer_before", spec.layer_url + "?f=pjson"),
        ("count_before", query_url(spec.layer_url, where="1=1", returnCountOnly="true", f="json")),
        ("ids_before", query_url(spec.layer_url, where="1=1", returnIdsOnly="true", f="json")),
    ]


def source_after_urls(spec: BtsIntermodalSpec) -> list[tuple[str, str]]:
    return [
        ("layer_after", spec.layer_url + "?f=pjson"),
        ("service_after", spec.service_url + "?f=pjson"),
        ("count_after", query_url(spec.layer_url, where="1=1", returnCountOnly="true", f="json")),
        ("ids_after", query_url(spec.layer_url, where="1=1", returnIdsOnly="true", f="json")),
    ]


def supports_ordered_pagination(layer: Mapping[str, object]) -> bool:
    capabilities = layer.get("advancedQueryCapabilities")
    return (isinstance(capabilities, dict)
            and capabilities.get("supportsPagination") is True
            and capabilities.get("supportsOrderBy") is True)


def object_id_chunks(ids: list[int], *, layer_url: str, out_sr: int,
                     max_url_length: int) -> list[list[int]]:
    """Split fallback ObjectID GET requests below the server URL limit."""
    chunks: list[list[int]] = []
    current: list[int] = []
    for object_id in ids:
        candidate = [*current, object_id]
        params = {"objectIds": ",".join(map(str, candidate)), "outFields": "*",
                  "returnGeometry": "true", "returnZ": "true", "returnM": "true",
                  "outSR": str(out_sr), "orderByFields": "OBJECTID ASC", "f": "json"}
        if len(query_url(layer_url, **params)) >= max_url_length:
            if not current:
                raise ValueError("single_object_id_exceeds_get_url_limit")
            chunks.append(current)
            current = [object_id]
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def page_plan(ids: list[int], layer: Mapping[str, object], *, layer_url: str, out_sr: int,
              page_size: int, max_url_length: int) -> tuple[str, list[tuple[str, str, list[int]]]]:
    """Plan deterministic source pages with expected ordered IDs."""
    ordered_ids = sorted(ids)
    pages: list[tuple[str, str, list[int]]] = []
    if supports_ordered_pagination(layer):
        mode = "ordered_offset"
        for offset in range(0, len(ordered_ids), page_size):
            page_ids = ordered_ids[offset:offset + page_size]
            url = query_url(layer_url, where="1=1", outFields="*", returnGeometry="true", returnZ="true",
                returnM="true", outSR=str(out_sr), orderByFields="OBJECTID ASC",
                resultOffset=offset, resultRecordCount=page_size, f="json")
            if len(url) >= max_url_length:
                raise ValueError("pagination_get_url_exceeds_safe_limit")
            pages.append((f"page_{len(pages):03d}", url, page_ids))
    else:
        mode = "bounded_object_ids"
        for page_ids in object_id_chunks(ordered_ids, layer_url=layer_url, out_sr=out_sr,
                                         max_url_length=max_url_length):
            params = {"objectIds": ",".join(map(str, page_ids)), "outFields": "*",
                      "returnGeometry": "true", "returnZ": "true", "returnM": "true",
                      "outSR": str(out_sr), "orderByFields": "OBJECTID ASC", "f": "json"}
            url = query_url(layer_url, **params)
            if len(url) >= max_url_length:
                raise ValueError("object_id_get_url_exceeds_safe_limit")
            pages.append((f"page_{len(pages):03d}", url, page_ids))
    if not pages:
        raise ValueError("source_page_plan_empty")
    return mode, pages


def validate(spec: BtsIntermodalSpec,
             captures: dict[str, tuple[bytes, dict[str, object]]]) -> tuple[dict, list[dict], list[dict], dict]:
    docs = CaptureDocuments(captures)
    fields = docs["layer_before"].get("fields")
    if not isinstance(fields, list) or not fields or any(not isinstance(f, dict) for f in fields):
        raise ValueError("source_field_roster_missing")
    plain_fields = [plain_field(f) for f in fields]
    schema_sha = _sha(_canonical(plain_fields))
    if schema_sha != spec.expected_source_schema_sha256:
        raise ValueError("source_schema_drift")
    portal = docs["portal_item"]
    if (portal.get("id") != spec.item_id or portal.get("owner") != "USDOT_BTS"
            or portal.get("title") != spec.layer_name
            or portal.get("url") != spec.service_url or portal.get("access") != "public"):
        raise ValueError("source_item_identity_or_access_drift")
    desc = str(portal.get("description", ""))
    if ("Bureau of Transportation Statistics (BTS)" not in desc
            or "National Transportation Atlas Database (NTAD)" not in desc
            or spec.description_token not in desc):
        raise ValueError("official_BTS_NTAD_relationship_or_coverage_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if (service.get("serviceItemId") != spec.item_id
            or not any(x.get("id") == spec.layer_id and x.get("name") == spec.layer_name
                       for x in service.get("layers", []) if isinstance(x, dict))
            or layer.get("name") != spec.layer_name or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("objectIdField") != "OBJECTID" or layer.get("hasZ") is not False
            or layer.get("hasM") is not False):
        raise ValueError(spec.layer_identity_error)
    if layer.get("extent", {}).get("spatialReference", {}).get("latestWkid") != spec.native_wkid:
        raise ValueError("source_native_crs_drift")
    if (len(fields) != len(spec.expected_field_names)
            or [f.get("name") for f in fields] != list(spec.expected_field_names)
            or [f.get("type") for f in fields] != list(spec.expected_field_types)):
        raise ValueError("source_schema_roster_drift")
    if docs["layer_before"] != docs["layer_after"] or docs["service_before"] != docs["service_after"]:
        raise ValueError("source_metadata_changed_during_capture")
    if [plain_field(f) for f in docs["layer_after"].get("fields", [])] != plain_fields:
        raise ValueError("source_field_schema_changed_during_capture")
    counts = (docs["count_before"].get("count"), docs["count_after"].get("count"))
    ids = (docs["ids_before"].get("objectIds"), docs["ids_after"].get("objectIds"))
    if (type(counts[0]) is not int or counts[0] != spec.expected_source_count
            or type(counts[1]) is not int or counts[1] != counts[0]
            or not isinstance(ids[0], list) or ids[0] != ids[1] or len(ids[0]) != counts[0]
            or any(type(value) is not int for value in ids[0])):
        raise ValueError("source_count_or_id_roster_drift")
    got_ids = sorted(ids[0])
    if len(set(got_ids)) != counts[0]:
        raise ValueError("source_id_roster_duplicate")
    if _sha(_canonical(got_ids)) != spec.expected_source_id_sha256:
        raise ValueError("source_id_roster_drift")
    seed = docs["ids_seed"].get("objectIds")
    if not isinstance(seed, list) or any(type(value) is not int for value in seed) or sorted(seed) != got_ids:
        raise ValueError("source_id_roster_changed_before_capture")
    page_names = sorted(name for name in captures if name.startswith("page_"))
    pagination_mode, pages = page_plan(got_ids, layer, layer_url=spec.layer_url,
                                       out_sr=spec.native_wkid, page_size=spec.page_size,
                                       max_url_length=spec.max_object_id_url_length)
    expected_pages = [name for name, _url, _ids in pages]
    if page_names != expected_pages:
        raise ValueError("source_page_roster_incomplete")
    features = []
    for page_index, (name, expected_url, expected_page_ids) in enumerate(pages):
        page = docs[name]
        receipt = captures[name][1]
        if receipt.get("url") != expected_url:
            raise ValueError("source_page_query_plan_drift")
        if pagination_mode == "bounded_object_ids" and len(expected_url) >= spec.max_object_id_url_length:
            raise ValueError("source_page_get_url_exceeds_safe_limit")
        if "error" in page:
            raise ValueError("source_page_error_or_truncated")
        # The BTS layer sets exceededTransferLimit on a full offset page when
        # additional ordered rows remain. The independently pinned ID roster
        # and exact requested slice prove page completeness; a partial or
        # unexpectedly flagged final page still fails closed.
        if (page.get("exceededTransferLimit") is True
                and (pagination_mode != "ordered_offset" or len(expected_page_ids) != spec.page_size
                     or page_index == len(pages) - 1)):
            raise ValueError("source_page_error_or_truncated")
        if page.get("spatialReference", {}).get("latestWkid") != spec.native_wkid:
            raise ValueError("source_page_crs_drift")
        rows = page.get("features")
        if not isinstance(rows, list):
            raise ValueError("source_page_missing_features")
        page_ids = [row.get("attributes", {}).get("OBJECTID") for row in rows
                    if isinstance(row, dict) and isinstance(row.get("attributes"), dict)]
        if page_ids != expected_page_ids:
            raise ValueError("source_page_ids_do_not_match_requested_slice")
        features.extend(rows)
    page_ids = [f.get("attributes", {}).get("OBJECTID") for f in features]
    if (len(features) != counts[0] or any(type(value) is not int for value in page_ids)
            or sorted(page_ids) != got_ids or len(set(page_ids)) != counts[0]):
        raise ValueError("source_feature_population_incomplete")
    field_names = [f["name"] for f in fields]
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    geom_repr = []
    null_geometry_count = 0
    invalid_geometry_count = 0
    for row in features:
        attrs, geom = row.get("attributes"), row.get("geometry")
        if not isinstance(attrs, dict) or set(attrs) != set(field_names):
            raise ValueError("source_attribute_roster_drift")
        if geom is None:
            null_geometry_count += 1
            geom_repr.append([attrs["OBJECTID"], None])
            continue
        if not isinstance(geom, dict) or set(geom) != {"x", "y"}:
            raise ValueError("source_point_geometry_roster_drift")
        if not all(type(geom[k]) in (int, float) for k in ("x", "y")):
            raise ValueError("source_point_coordinate_invalid")
        if not (-180 <= geom["x"] <= 180 and -90 <= geom["y"] <= 90):
            invalid_geometry_count += 1
        geom_repr.append([attrs["OBJECTID"], geom["x"], geom["y"]])
    audit = {
        "source_feature_count": len(features), "source_id_sha256": _sha(_canonical(got_ids)),
        "source_population_complete": True,
        "source_record_sha256": _sha(_canonical(features)),
        "source_geometry_sha256": _sha(_canonical(geom_repr)), "source_schema_sha256": schema_sha,
        "source_fields": plain_fields, "source_crs_wkid": spec.native_wkid,
        "source_geometry_type": "esriGeometryPoint", "source_null_geometry_count": null_geometry_count,
        "source_invalid_geometry_count": invalid_geometry_count,
        "pagination_mode":pagination_mode,"page_count":len(pages),"page_size":spec.page_size,
        "proposed_physical_field_mappings":[{"field":field["name"],"source_type":field["type"],
            "proposed_arrow_type":str(arrow_type(field)),"approved":False} for field in plain_fields],
    }
    return layer, plain_fields, features, audit


def parquet_bytes(fields: list[dict], features: list[dict], wkid: int) -> bytes:
    arrays = {}
    schema_fields = []
    for field in fields:
        name = field["name"]
        kind = arrow_type(field)
        arrays[name] = pa.array([row["attributes"][name] for row in features], type=kind)
        schema_fields.append(pa.field(name, kind, nullable=bool(field.get("nullable", True))))
    arrays["geometry"] = pa.array([
        None if row["geometry"] is None else Point(row["geometry"]["x"], row["geometry"]["y"]).wkb
        for row in features
    ], type=pa.binary())
    schema_fields.append(pa.field("geometry", pa.binary(), nullable=True))
    geo = {"version":"1.1.0", "primary_column":"geometry", "columns":{"geometry":{
        "encoding":"WKB", "geometry_types":["Point"], "crs":pyproj.CRS.from_epsg(wkid).to_json_dict(), "edges":"planar",
    }}}
    schema = pa.schema(schema_fields, metadata={b"geo":_canonical(geo)})
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pydict(arrays, schema=schema), sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def verify_first_candidate(dataset_slug: str, file_slug: str, storage: StagingStorageResource,
                           prefix: str, state_key: str, version: str, fingerprint: str,
                           identity: Mapping[str, object], audit: Mapping[str, object],
                           expected_names: set[str]) -> dict:
    bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
    payload = bundle.get("payload")
    if (not isinstance(payload, dict) or set(payload) != {"processing_identity", "source_record_sha256",
            "source_schema_sha256", "capture_receipts", "objects"}
            or payload["processing_identity"] != identity
            or payload["source_record_sha256"] != audit["source_record_sha256"]
            or payload["source_schema_sha256"] != audit["source_schema_sha256"]):
        raise ValueError("cached_first_candidate_identity_or_source_changed")
    verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint,
        identity, audit["source_record_sha256"], audit["source_schema_sha256"], payload["objects"], payload["capture_receipts"])
    if set(payload["objects"]) != expected_names:
        raise ValueError("cached_candidate_roster_changed")
    return {"dataset_slug":dataset_slug,"file_slug":file_slug,"version":version,
        "feature_count":audit["source_feature_count"],"source_population_complete":True,
        "source_null_geometry_count":audit["source_null_geometry_count"],
        "source_invalid_geometry_count":audit["source_invalid_geometry_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json",
        "reused":True,"source_release_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,
        "mapping_approved":False,"release_accepted":False}


def capture_bts_intermodal(spec: BtsIntermodalSpec, storage: StagingStorageResource, *,
                           capture_directory: str | None = None) -> dict:
    """Acquire, validate and stage a complete unapproved BTS candidate."""
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < 256_000_000:
        raise ValueError("disk_floor_before_acquisition")
    capture_base = Path(capture_directory) if capture_directory else default_capture_dir("hifld-native-reconstruction", spec.capture_root_id, "captures")
    capture_token = uuid4().hex
    capture_root = (capture_base / f"capture-{capture_token}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures = RetainedCaptureView(capture_root)
    cursor = LayerCaptureFetch(max_body_bytes=spec.max_body_bytes, max_capture_bytes=spec.max_capture_total_bytes)
    cursor.current_name = "ids_seed"
    cursor.current_url = query_url(spec.layer_url, where="1=1", returnIdsOnly="true", f="json")
    try:
        with httpx.Client(follow_redirects=False, timeout=90, headers={"User-Agent": spec.user_agent}) as client:
            def validate_seed(body):
                seed = json.loads(body).get("objectIds")
                if not isinstance(seed, list) or not seed or any(type(value) is not int for value in seed):
                    raise ValueError("source_seed_id_roster_missing_or_invalid")
                return seed

            def roster(seed):
                layer_before = json.loads(captures["layer_before"][0])
                _pagination_mode, pages = page_plan(seed, layer_before, layer_url=spec.layer_url,
                    out_sr=spec.native_wkid, page_size=spec.page_size,
                    max_url_length=spec.max_object_id_url_length)
                return [(name, url) for name, url, _page_ids in pages] + source_after_urls(spec)

            def on_validation_error(captured):
                return validate(spec, captured)

            capture_layer(spec.layer_url, capture_root, spec.max_capture_total_bytes, spec.page_size,
                client=client, captures=captures, seed_name="ids_seed", seed_url=cursor.current_url,
                validate_seed=validate_seed, after_seed_documents=source_metadata_urls(spec),
                roster=roster, fetch=cursor.fetch, on_validation_error=on_validation_error)
        layer, fields, features, audit = validate(spec, captures)
    except Exception as exc:
        error_code = str(exc) if isinstance(exc, ValueError) and str(exc).replace("_", "").isalnum() else type(exc).__name__
        receipts = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            try: receipts[path.name.removesuffix(".receipt.json")] = json.loads(path.read_bytes())
            except (ValueError, OSError): receipts[path.name] = {"receipt_readback_failed":True}
        # The shared capture helper may fail during a stream before it can
        # return a receipt. Preserve every partial body/wire file with a
        # clearly incomplete receipt so a later operator can inspect it.
        partial_names = {p.name.removesuffix(".wire") for p in capture_root.glob("*.wire")}
        partial_names.update(p.name.removesuffix(".body") for p in capture_root.glob("*.body"))
        for name in sorted(partial_names - captures.keys()):
            body_path, wire_path = capture_root/f"{name}.body", capture_root/f"{name}.wire"
            body = body_path.read_bytes() if body_path.exists() else b""
            wire = wire_path.read_bytes() if wire_path.exists() else b""
            receipt = receipts.get(name)
            if not isinstance(receipt, dict):
                receipt = {"method":"GET","url":cursor.current_url if name == cursor.current_name else None,
                    "final_url":None,"observed_at":datetime.now(timezone.utc).isoformat(),"status":None,
                    "complete_body":False,"wire_bytes":len(wire),"wire_sha256":_sha(wire),
                    "bytes":len(body),"sha256":_sha(body),"content_encoding":None,
                    "content_length":None,"error_code":error_code}
            receipts[name] = receipt
            captures[name] = (body, receipt)
        (capture_root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":False,"error_code":error_code,"receipts":receipts})+b"\n")
        if captures:
            quarantine_incomplete_capture(storage, capture_root, spec.dataset_slug, capture_token, captures, error_code)
        raise
    identity = processing_identity(Path(__file__).parents[3])
    parquet = parquet_bytes(fields, features, spec.native_wkid)
    native_path = f"geoparquet/{spec.dataset_slug}.parquet"
    source_objects = {native_path:{"bytes":len(parquet),"sha256":_sha(parquet)}}
    vintage_evidence = None
    if spec.include_source_vintage_evidence:
        portal_doc = json.loads(captures["portal_item"][0])
        service_doc = json.loads(captures["service_before"][0])
        layer_doc = json.loads(captures["layer_before"][0])
        vintage_evidence = {
            "portal_description": portal_doc.get("description"),
            "portal_item_created_epoch_ms": portal_doc.get("created"),
            "portal_item_modified_epoch_ms": portal_doc.get("modified"),
            "service_editing_info": service_doc.get("editingInfo"),
            "layer_editing_info": layer_doc.get("editingInfo"),
            "release_date_selected": None,
            "freshness_accepted": False,
            "assessment": (
                "Portal description dates and ArcGIS item/service/layer editing metadata are "
                "preserved separately; none is treated as an independently verified source release date."
            ),
        }
    manifest = {
        "schema_version":1,"approved":False,"title":spec.layer_name,
        "description":f"BTS NTAD Intermodal Freight Facilities {spec.description_token} point layer. Schema, ID, freshness and release approvals remain pending.",
        "publisher":spec.publisher,"agency":"Department of Transportation","source_url":spec.layer_url,
        "license":"https://www.usa.gov/publicdomain/label/1.0/","source_modified":None,"date_issued":None,
        "date_modified":None,
    }
    if vintage_evidence is not None:
        manifest["source_vintage_evidence"] = vintage_evidence
    manifest.update({
        "processor":"HIFLD Next","code_commit":identity["code_commit"],
        "inputs":[{"input_id":name,"producer":spec.publisher,"host":urlsplit(receipt["url"]).hostname,
            "role":"producer_publication","canonical_url":f"https://www.arcgis.com/sharing/rest/content/items/{spec.item_id}?f=json" if name.startswith("portal") else spec.layer_url,
            "retrieval_url":receipt["url"],"observed_final_url":receipt["final_url"],"retrieved_at":receipt["observed_at"],
            "http_status":receipt["status"],"bytes":receipt["bytes"],"sha256":receipt["sha256"],
            "license":"https://www.usa.gov/publicdomain/label/1.0/","attribution":"BTS NTAD; producer metadata states U.S. public domain; rights review remains pending.",
            "release_date":None,"release_date_unknown_reason":spec.release_date_unknown_reason}
            for name,(_,receipt) in ((name, captures[name]) for name in sorted(captures))],
        "transformations":[{"operation":"native_esri_json_to_geoparquet_candidate","parameters":{
            "feature_layer_id":spec.layer_id,"source_fields":spec.source_fields_text,
            "source_ids":"OBJECTID preserved unchanged; source ID roster is pinned before and after capture","geometry":"source point x/y encoded as WKB at EPSG:4326 without reprojection or repair",
            "physical_mapping":"Per-field source-to-Arrow proposals are recorded in source_audit.json; OID widening to int64 preserves values, and all mapping approvals remain false.",
            "candidate_status":"unapproved; production publication prohibited pending source freshness, schema/ID contract, rights and mapping acceptance"}}],
        "lineage":f"USDOT_BTS item {spec.item_id} exposes the exact BTS-maintained NTAD FeatureServer layer 0. HIFLD inventory lineage points to DataLumos project {spec.datalumos_project}, version V1; archive files are not used as the ingest source.",
        "prebuilt_output_policy":{"schema_version":1,"formats":{"geoparquet":{"reason":"Native Esri fields, IDs, source point geometry and coordinates retained; approvals pending.","objects":[{"path":native_path,**source_objects[native_path]}]}}},
        "unsupported_output_formats":{"geopackage":"No approved source-to-public schema mapping.","pmtiles":"No display transformation or generalized geometry approved."},
    })
    validate_release_provenance(manifest); validate_output_policy(manifest)
    audit.update({"official_layer_name":spec.layer_name,"arcgis_item_id":spec.item_id,
        "source_relationship":f"BTS-maintained NTAD {spec.description_token} layer",
        "physical_mapping_approved":False,"freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False})
    if vintage_evidence is not None:
        audit["source_vintage_evidence"] = vintage_evidence
    version = content_derived_version(audit["source_record_sha256"],audit["source_geometry_sha256"],audit["source_schema_sha256"],identity)
    prefix = candidate_prefix(spec.dataset_slug,spec.file_slug,version)
    (capture_root/"capture_manifest.json").write_bytes(_canonical({"captured_at":datetime.now(timezone.utc).isoformat(),"complete":True,
        "source_feature_count":audit["source_feature_count"],"source_record_sha256":audit["source_record_sha256"],"source_geometry_sha256":audit["source_geometry_sha256"],
        "receipts":{name:receipt for name,(_,receipt) in ((name, captures[name]) for name in sorted(captures))}})+b"\n")
    outputs = {native_path:parquet,"metadata/source_manifest.json":_canonical(manifest)+b"\n",
        "metadata/source_audit.json":_canonical(audit)+b"\n","metadata/processing_identity.json":_canonical(identity)+b"\n"}
    for name,(body,receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root/f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt)+b"\n"
    ready = {"approved":False,"version":version,"objects":source_objects,"source_record_sha256":audit["source_record_sha256"],
        "freshness_accepted":False,"schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,"mapping_approved":False,"release_accepted":False}
    outputs["metadata/candidate_ready.json"] = _canonical(ready)+b"\n"
    state_key = f"_construction-state/{spec.dataset_slug}/{version}.json"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        return verify_first_candidate(spec.dataset_slug, spec.file_slug, storage, prefix, state_key,
                                      version, fingerprint, identity, audit, set(outputs))
    descriptors = write_candidate_outputs(
        storage, prefix, outputs, readback_order="after-all", error_prefix='candidate_output_physical_readback_failed:')
    payload={"processing_identity":identity,"source_record_sha256":audit["source_record_sha256"],"source_schema_sha256":audit["source_schema_sha256"],
        "capture_receipts":{name:receipt for name,(_,receipt) in captures.items()},"objects":descriptors}
    seal_candidate_objects(
        storage, state_key, version,
        payload['processing_identity'], payload['capture_receipts'], payload['objects'],
        source_record_sha256=payload['source_record_sha256'],
        source_schema_sha256=payload['source_schema_sha256'],
        fingerprint=fingerprint)
    validate_construction_code_identity(Path(__file__).parents[3],identity["code_commit"],identity["files"])
    return {"dataset_slug":spec.dataset_slug,"file_slug":spec.file_slug,"version":version,"feature_count":audit["source_feature_count"],"source_population_complete":True,
        "source_null_geometry_count":audit["source_null_geometry_count"],
        "source_invalid_geometry_count":audit["source_invalid_geometry_count"],"candidate_ready":prefix+"/metadata/candidate_ready.json",
        "capture_directory":str(capture_root),"source_release_accepted":False,"schema_contract_accepted":False,
        "id_contract_accepted":False,"freshness_accepted":False,"rights_accepted":False,"release_accepted":False}

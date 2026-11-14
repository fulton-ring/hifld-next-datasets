"""Lossless, approval-gated capture of the FHWA TMAS Stations NTAD layer.

The Federal Highway Administration (FHWA) Travel Monitoring Analysis System
(TMAS) Stations dataset is published through the USDOT Bureau of
Transportation Statistics (BTS) National Transportation Atlas Database (NTAD):
the BTS ArcGIS item a016bbaee4794daca873135a5cf4164d (owner ``USDOT_BTS`` in
the BTS organization ``xOi1kZaI0eWDREZv``, serviceItemId matching, access
public) is the delegated official NTAD FeatureServer publication of the
producer dataset; its item metadata states the work is a U.S. government work
not protected by copyright under 17 U.S.C. § 101, available for unrestricted
public use. The DHS HIFLD Open crosswalk (row "Travel Monitoring Analysis
System (TMAS) Stations", Agency Department of Transportation, DOI
10.3886/E241367V1) points the retired HIFLD Open geoplatform item
f4e62fe944cb489ab5131c25105348fe at exactly this BTS-hosted service
(https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/
NTAD_Travel_Monitoring_Analysis_System_Stations/FeatureServer/0). The
producer item description records the NTAD edition vintage ("compiled on
December 31, 2025 and was published on March 23, 2026 from the Federal
Highway Administration (FHWA)"; the item was last edited 2026-04-17) and
cites the FHWA data dictionary DOI 10.21949/zxzj-hb56; those dates are
retained as separate vintage evidence, never an accepted release date. The
HIFLD Open inventory lists this dataset as
travel-monitoring-analysis-system-tmas-stations (publisher FHWA); its
DataLumos project 240593 archive copy is identification context only, never
a pipeline input.

The layer is captured as ordered-offset pages (the layer reports
``maxRecordCount`` 2000 and advertises pagination plus ordering) of the
sorted OBJECTID roster: every accepted page must return exactly its expected
sorted id slice, the accepted pages must tile the complete roster without
gaps or overlaps, and the before/after count/id/metadata brackets plus
full-record and geometry digests prove that nothing was skipped, duplicated,
or reordered. Schema, IDs, geometry and every attribute value are preserved
unchanged. The layer's Station_Id attribute repeats across rows (the
captured 2026-04-17 edition holds 6213 distinct values across 8060 non-null
rows, 1847 extra occurrences); duplicates are preserved and counted, never
dropped or failed — the row identity contract is the strictly validated
OBJECTID roster. Producer point geometries carry the service's full double
precision while the latitude/longitude attributes carry six decimal digits;
every geometry must agree with its own attributes within 1e-8 degrees
(observed worst deviation 4.5e-10) and the exact served x/y values are
digest-pinned.
"""
from __future__ import annotations

import hashlib
import json
import math
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

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    capture_get, candidate_prefix, content_derived_version,
    processing_identity, quarantine_incomplete_capture, seal_ready_candidate,
    verify_cached_candidate, write_immutable,
)

DATASET_SLUG = "travel-monitoring-analysis-system-tmas-stations"
FILE_SLUG = DATASET_SLUG
ITEM_ID = "a016bbaee4794daca873135a5cf4164d"
ITEM_ORG_ID = "xOi1kZaI0eWDREZv"
SERVICE_URL = ("https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
               "NTAD_Travel_Monitoring_Analysis_System_Stations/FeatureServer")
LAYER_ID = 0
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "Travel Monitoring Analysis System Stations"
PUBLISHER = ("Federal Highway Administration (FHWA), published through the USDOT Bureau "
             "of Transportation Statistics (BTS) National Transportation Atlas Database (NTAD)")
AGENCY = "Department of Transportation"
SOURCE_CRS_WKID = 4326
DATA_DICTIONARY_DOI = "https://doi.org/10.21949/zxzj-hb56"
PAGE_START_RECORDS = 2000
EXPECTED_MAX_RECORD_COUNT = 2000
MAX_CAPTURE_BYTES = 8_000_000
MAX_CAPTURE_TOTAL_BYTES = 128_000_000
MIN_FREE_BYTES = 1_000_000_000
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
PAGE_CAPTURE_ATTEMPTS = 4
PAGE_CAPTURE_BASE_DELAY_SECONDS = 5.0
# Pins below are the observed complete-layer population contract, established
# by the dated complete capture 2026-11-14 (5 accepted pages tiling the sorted
# OBJECTID roster without gaps or overlaps; every accepted page returned
# exactly its expected sorted-OBJECTID slice; observed point bounds equal the
# layer's declared extent exactly; no null attributes or geometries).
EXPECTED_COUNT = 8060
EXPECTED_ID_SHA256 = "20afcf25e15dcad068e0d22d37bd4e44c81a30b2631609f02fc22b1d8fcc8981"
EXPECTED_SCHEMA_SHA256 = "e6bdc07f0f5aa48b243d14d04c69c9c38a8d334df1d01e4801af56532493c7af"
EXPECTED_RECORD_SHA256 = "523ef2ffadc96c1cec4724bf5a46c36ec99a626358fd8ca062cff8f643b01dc0"
EXPECTED_GEOMETRY_SHA256 = "663800e79cff6e91111808c5f59aec9e839263576172c898e23bf0ccc2deaca6"
EXPECTED_LAYER_EXTENT = {"xmin": -165.392939999978, "ymin": 17.97165599982555,
                         "xmax": -65.77957099982048, "ymax": 64.97835000000852}
EXPECTED_OBSERVED_BOUNDS = {"xmin": -165.392939999978, "ymin": 17.97165599982555,
                            "xmax": -65.77957099982048, "ymax": 64.97835000000852}
EXPECTED_NULL_GEOMETRY_COUNT = 0
EXPECTED_INVALID_GEOMETRY_COUNT = 0
EXPECTED_FIELD_NAMES = ["OBJECTID", "Station_Id", "functional_class", "state", "latitude", "longitude"]
EXPECTED_FIELD_TYPES = ["esriFieldTypeOID", "esriFieldTypeString", "esriFieldTypeString",
                        "esriFieldTypeString", "esriFieldTypeDouble", "esriFieldTypeDouble"]
EXPECTED_SERVICE_LAYER_ROSTER = [(LAYER_ID, LAYER_NAME, "Feature Layer")]
# Producer fields are declared nullable but the captured edition has no nulls
# anywhere; a null in any field is population drift that fails closed.
EXPECTED_NULL_COUNTS = {"OBJECTID": 0, "Station_Id": 0, "functional_class": 0,
                        "state": 0, "latitude": 0, "longitude": 0}
# Station_Id repeats across station-period rows (6213 distinct values across
# 8060 rows, 1847 extra occurrences, up to 24 copies); duplicates are
# preserved and counted, never dropped.
EXPECTED_STATION_ID_NULL_COUNT = 0
EXPECTED_STATION_ID_UNIQUE_COUNT = 6213
EXPECTED_STATION_ID_DUPLICATE_COUNT = 1847
# Geometry/attribute agreement: producer point geometry equals its own
# latitude/longitude attributes within this tolerance (observed worst
# deviation 4.5e-10 degrees); the exact served x/y values are separately
# digest-pinned.
GEOMETRY_ATTRIBUTE_TOLERANCE_DEGREES = 1e-8
EXPECTED_GEOMETRY_ATTRIBUTE_MISMATCH_COUNT = 0
# Observed 2026-11-14: 52 jurisdiction codes (50 states + DC + PR; the item
# description advertises "all 50 states plus DC") and 14 functional-class
# route-type codes.
EXPECTED_STATES = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI',
                   'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN',
                   'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH',
                   'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT',
                   'WA', 'WI', 'WV', 'WY']
EXPECTED_FUNCTIONAL_CLASSES = {'1R', '1U', '2R', '2U', '3R', '3U', '4R', '4U',
                               '5R', '5U', '6R', '6U', '7R', '7U'}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _list_sha(items: list) -> str:
    """Streamed sha256 of the canonical JSON encoding of ``items``."""
    digest = hashlib.sha256()
    digest.update(b"[")
    for index, item in enumerate(items):
        if index:
            digest.update(b",")
        digest.update(_canonical(item))
    digest.update(b"]")
    return digest.hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain")
            if key in field}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _page_name(offset: int, count: int) -> str:
    return f"page_{offset:06d}_{count:05d}"


def _parse_page_name(name: str) -> tuple[int, int]:
    stem = name[5:]
    offset, count = stem.split("_", 1)
    return int(offset), int(count)


def _page_query(offset: int, count: int) -> str:
    return _query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
                   "outSR": "4326", "orderByFields": "OBJECTID ASC", "resultOffset": offset,
                   "resultRecordCount": count, "f": "json"})


def _doc_urls() -> list[tuple[str, str]]:
    return [
        ("portal_item", f"https://www.arcgis.com/sharing/rest/content/items/{ITEM_ID}?f=json"),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]


def _after_doc_urls() -> list[tuple[str, str]]:
    return [
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]


def _capture_names(page_roster: list[tuple[int, int]] | None = None) -> set[str]:
    names = {"ids_seed", *(name for name, _url in _doc_urls()),
             *(name for name, _url in _after_doc_urls())}
    if page_roster is None:
        page_roster = [(offset, min(PAGE_START_RECORDS, EXPECTED_COUNT - offset))
                       for offset in range(0, EXPECTED_COUNT, PAGE_START_RECORDS)]
    return names | {_page_name(offset, count) for offset, count in page_roster}


def _validate_spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    found = False
    for key in ("wkid", "latestWkid"):
        wkid = value.get(key)
        if wkid is not None:
            if wkid != SOURCE_CRS_WKID:
                raise ValueError(label + "_crs_drift")
            found = True
    if not found:
        raise ValueError(label + "_crs_drift")


def _producer_error_doc(body: bytes) -> bool:
    if not body.lstrip().startswith(b"{") or len(body) > 65536:
        return False
    try:
        doc = json.loads(body)
    except ValueError:
        return False
    return isinstance(doc, dict) and "error" in doc


def _preserve_failed_attempt(capture_root: Path, name: str, attempt: int) -> None:
    for suffix in (".body", ".wire", ".receipt.json"):
        path = capture_root / f"{name}{suffix}"
        if path.exists():
            path.rename(capture_root / f"{name}.attempt-{attempt:02d}{suffix}")


def _retained_capture_bytes(capture_root: Path) -> int:
    return sum(path.stat().st_size for path in capture_root.iterdir()
               if path.is_file() and (path.name.endswith(".body") or path.name.endswith(".wire")))


def _capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                        max_bytes: int, attempts: int, base_delay: float) -> tuple[bytes, dict[str, object]]:
    """Capture one producer request, retrying bounded times on transient failures.

    Retries cover incomplete transport captures and the producer's transient
    error documents; failed attempts are preserved on disk under
    ``<name>.attempt-NN`` evidence names and never enter the capture roster.
    Every retry re-issues the exact same bounded request, and completeness is
    enforced by the pinned before/after rosters, counts, page tiling and full
    record/geometry digests, so retries can never mask drift.
    """
    for attempt in range(1, attempts + 1):
        if _retained_capture_bytes(capture_root) + 2 * max_bytes > MAX_CAPTURE_TOTAL_BYTES:
            raise ValueError("source_capture_budget_exceeded_before_request")
        try:
            body, receipt = capture_get(client, url, capture_root, name, max_bytes=max_bytes)
        except ValueError as exc:
            if attempt >= attempts or not str(exc).startswith(f"incomplete_source_capture:{name}"):
                raise
            _preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        if _producer_error_doc(body) and attempt < attempts:
            _preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        return body, receipt
    raise ValueError(f"incomplete_source_capture:{name}")


def _collect_page(offset: int, count: int, page: object, ids: list[int],
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
    if page.get("exceededTransferLimit") is True and offset + count >= EXPECTED_COUNT:
        raise ValueError("source_page_unexpected_exceeded_transfer_limit")
    features.extend(rows)


def _validate_captures(captures: dict[str, tuple[bytes, dict[str, object]]]):
    if set(captures) != _capture_names():
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    docs = {name: json.loads(body) for name, (body, _) in captures.items()
            if not name.startswith("page_")}
    page_names = sorted(name for name in captures if name.startswith("page_"))
    pages = [_parse_page_name(name) for name in page_names]
    position = 0
    for offset, count in pages:
        if offset != position:
            raise ValueError("source_page_tiling_incomplete_or_overlapped")
        position += count
    if position != EXPECTED_COUNT:
        raise ValueError("source_page_tiling_incomplete_or_overlapped")
    portal = docs["portal_item"]
    if (not isinstance(portal, dict) or portal.get("id") != ITEM_ID
            or portal.get("owner") != "USDOT_BTS" or portal.get("orgId") != ITEM_ORG_ID
            or portal.get("title") != LAYER_NAME or portal.get("access") != "public"
            or portal.get("type") != "Feature Service"):
        raise ValueError("source_portal_item_identity_drift")
    description = str(portal.get("description", ""))
    if ("Federal Highway Administration (FHWA)" not in description
            or "National Transportation Atlas Database (NTAD)" not in description
            or "Travel Monitoring Analysis System" not in description):
        raise ValueError("official_FHWA_NTAD_relationship_drift")
    service, layer = docs["service_before"], docs["layer_before"]
    if not isinstance(service, dict) or service.get("serviceItemId") != ITEM_ID:
        raise ValueError("official_BTS_tmas_stations_service_identity_drift")
    service_layers = service.get("layers")
    roster = [(x.get("id"), x.get("name"), x.get("type")) for x in service_layers
              if isinstance(x, dict)] if isinstance(service_layers, list) else None
    if roster != EXPECTED_SERVICE_LAYER_ROSTER:
        raise ValueError("official_BTS_tmas_stations_service_identity_drift")
    if (not isinstance(layer, dict) or layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPoint"
            or layer.get("objectIdField") != "OBJECTID" or layer.get("hasZ") is not False
            or layer.get("hasM") is not False):
        raise ValueError("exact_BTS_tmas_stations_layer_identity_drift")
    capabilities = layer.get("advancedQueryCapabilities")
    if not (isinstance(capabilities, dict) and capabilities.get("supportsPagination") is True
            and capabilities.get("supportsOrderBy") is True):
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != EXPECTED_MAX_RECORD_COUNT:
        raise ValueError("source_record_limit_drift")
    extent = layer.get("extent", {})
    _validate_spatial_reference(extent.get("spatialReference"), "source_layer_extent")
    _validate_spatial_reference(layer.get("spatialReference"), "source_layer")
    if {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
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
    if docs["count_before"].get("count") != EXPECTED_COUNT \
            or docs["count_after"].get("count") != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    if docs["count_before"] != docs["count_after"]:
        raise ValueError("source_count_changed_during_capture")
    rosters = [docs[name].get("objectIds") for name in ("ids_seed", "ids_before", "ids_after")]
    if any(not isinstance(roster, list) or len(roster) != EXPECTED_COUNT
           or any(type(value) is not int for value in roster) or len(set(roster)) != EXPECTED_COUNT
           for roster in rosters):
        raise ValueError("source_id_population_drift")
    if any(sorted(roster) != sorted(rosters[0]) for roster in rosters[1:]):
        raise ValueError("source_id_population_drift")
    ids = sorted(rosters[0])
    if _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID"
           for name in ("ids_seed", "ids_before", "ids_after")):
        raise ValueError("source_id_field_changed")

    features: list[dict[str, object]] = []
    for (offset, count), name in zip(pages, page_names):
        _collect_page(offset, count, json.loads(captures[name][0]), ids, features)
    attr_names = list(EXPECTED_FIELD_NAMES)
    null_counts = {name: 0 for name in EXPECTED_FIELD_NAMES}
    states: set[str] = set()
    functional_classes: set[str] = set()
    station_ids: list[str | None] = []
    geometry_attribute_mismatches = 0
    geometry_records: list[list] = []
    invalid_geometry = 0
    null_geometry = 0
    xmin = ymin = xmax = ymax = None
    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict) or list(attrs) != attr_names:
            raise ValueError("source_record_attribute_schema_drift")
        oid = attrs["OBJECTID"]
        if type(oid) is not int or oid <= 0:
            raise ValueError("source_oid_value_type_drift")
        for name in ("Station_Id", "functional_class", "state"):
            value = attrs[name]
            if value is None:
                null_counts[name] += 1
            elif not isinstance(value, str):
                raise ValueError("source_string_value_type_drift:" + name)
            if len(value) > 8000:
                raise ValueError("source_string_length_drift:" + name)
        for name in ("latitude", "longitude"):
            value = attrs[name]
            if value is None:
                null_counts[name] += 1
            elif type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError("source_double_value_type_drift:" + name)
        if attrs["state"] not in EXPECTED_STATES:
            raise ValueError("source_state_value_contract_drift")
        if attrs["functional_class"] not in EXPECTED_FUNCTIONAL_CLASSES:
            raise ValueError("source_functional_class_value_contract_drift")
        states.add(attrs["state"])
        functional_classes.add(attrs["functional_class"])
        station_ids.append(attrs["Station_Id"])
        geom = feature.get("geometry")
        if geom is None:
            null_geometry += 1
            geometry_records.append([oid, None])
            continue
        if (not isinstance(geom, dict) or set(geom) != {"x", "y"}
                or not all(type(geom[key]) in (int, float) and math.isfinite(geom[key])
                           for key in ("x", "y"))):
            invalid_geometry += 1
            geometry_records.append([oid, "INVALID"])
            continue
        x, y = geom["x"], geom["y"]
        if not (-180 <= x <= 180 and -90 <= y <= 90):
            raise ValueError("source_coordinate_range_drift")
        if (attrs["latitude"] is not None and attrs["longitude"] is not None
                and (abs(x - attrs["longitude"]) > GEOMETRY_ATTRIBUTE_TOLERANCE_DEGREES
                     or abs(y - attrs["latitude"]) > GEOMETRY_ATTRIBUTE_TOLERANCE_DEGREES)):
            geometry_attribute_mismatches += 1
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
    features.sort(key=lambda row: row["attributes"]["OBJECTID"])
    if null_counts != EXPECTED_NULL_COUNTS:
        raise ValueError("source_null_population_drift")
    if states != set(EXPECTED_STATES):
        raise ValueError("source_state_value_contract_drift")
    if functional_classes != EXPECTED_FUNCTIONAL_CLASSES:
        raise ValueError("source_functional_class_value_contract_drift")
    station_id_nulls = sum(1 for value in station_ids if value is None)
    station_id_uniques = len({value for value in station_ids if value is not None})
    station_id_duplicates = len(station_ids) - station_id_uniques - station_id_nulls
    if (station_id_nulls != EXPECTED_STATION_ID_NULL_COUNT
            or station_id_uniques != EXPECTED_STATION_ID_UNIQUE_COUNT
            or station_id_duplicates != EXPECTED_STATION_ID_DUPLICATE_COUNT):
        raise ValueError("source_station_id_population_drift")
    if (geometry_attribute_mismatches != EXPECTED_GEOMETRY_ATTRIBUTE_MISMATCH_COUNT
            or null_geometry != EXPECTED_NULL_GEOMETRY_COUNT
            or invalid_geometry != EXPECTED_INVALID_GEOMETRY_COUNT):
        raise ValueError("source_geometry_population_drift")
    if EXPECTED_OBSERVED_BOUNDS is not None and observed_bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    record_sha = _list_sha(features)
    if record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    geometry_sha = _list_sha(geometry_records)
    if geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    audit = {"source_feature_count": len(features), "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": record_sha, "source_geometry_sha256": geometry_sha,
             "source_schema_sha256": schema_sha, "source_crs_wkid": SOURCE_CRS_WKID,
             "source_geometry_type": "esriGeometryPoint",
             "null_geometry_count": null_geometry, "invalid_geometry_count": invalid_geometry,
             "observed_bounds": observed_bounds, "declared_layer_extent": EXPECTED_LAYER_EXTENT,
             "unique_object_id_count": len(ids),
             "source_null_counts": null_counts,
             "unique_station_id_count": station_id_uniques,
             "station_id_duplicate_count": station_id_duplicates,
             "station_id_null_count": station_id_nulls,
             "observed_state_count": len(states),
             "observed_functional_class_count": len(functional_classes),
             "geometry_attribute_mismatch_count": geometry_attribute_mismatches,
             "geometry_attribute_tolerance_degrees": GEOMETRY_ATTRIBUTE_TOLERANCE_DEGREES,
             "source_page_count": len(pages), "source_release_date": None,
             "source_release_date_unknown_reason":
                 "The BTS NTAD service publishes no per-snapshot release date for this layer; "
                 "the portal description records the NTAD edition vintage (compiled December "
                 "31, 2025; published March 23, 2026 from FHWA) and the item and layer edit "
                 "timestamps are retained as separate vintage evidence, not as an accepted "
                 "release date."}
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
        columns[name] = pa.array([row["attributes"].get(name) for row in features], type=typ)
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


_EVIDENCE_SUFFIXES = (".receipt.json", ".body", ".wire")


def _capture_evidence(capture_root: Path,
                      captures: dict[str, tuple[bytes, dict[str, object]]]
                      ) -> dict[str, dict[str, object]]:
    """Collect hash-pinned failed-attempt evidence for sealing.

    Retry responses never enter the accepted capture roster; at seal time
    every body/wire/receipt still on disk under a non-accepted ``.attempt-NN``
    name is returned so failed producer responses are staged separately from
    the accepted captures, with each retained receipt re-verified against its
    own bytes. Anything else in the capture directory fails closed.
    """
    evidence: dict[str, dict[str, object]] = {}
    for path in sorted(capture_root.iterdir(), key=lambda item: item.name):
        if path.is_symlink() or not path.is_file():
            raise ValueError("unsafe_capture_evidence_file:" + path.name)
        suffix = next((s for s in _EVIDENCE_SUFFIXES if path.name.endswith(s)), None)
        if suffix is None:
            continue
        stem = path.name[:-len(suffix)]
        if stem in captures:
            continue
        if ".attempt-" not in stem:
            raise ValueError("unsafe_capture_evidence_file:" + path.name)
        entry = evidence.setdefault(stem, {"kind": "failed_attempt", "files": {}})
        entry["files"][suffix] = path.read_bytes()
    for stem, entry in sorted(evidence.items()):
        files = entry["files"]
        receipt_body = files.get(".receipt.json")
        if receipt_body is None:
            raise ValueError("capture_evidence_receipt_missing:" + stem)
        try:
            receipt = json.loads(receipt_body)
        except ValueError:
            receipt = None
        if not isinstance(receipt, dict):
            raise ValueError("capture_evidence_receipt_invalid:" + stem)
        for suffix, size_key, digest_key in ((".body", "bytes", "sha256"),
                                             (".wire", "wire_bytes", "wire_sha256")):
            body = files.get(suffix)
            if body is None:
                raise ValueError("capture_evidence_bytes_missing:" + stem + suffix)
            if isinstance(receipt.get(size_key), int) and receipt[size_key] != len(body):
                raise ValueError("capture_evidence_receipt_mismatch:" + stem + suffix)
            if isinstance(receipt.get(digest_key), str) and receipt[digest_key] != _sha(body):
                raise ValueError("capture_evidence_receipt_mismatch:" + stem + suffix)
    return evidence


def capture_tmas_stations(storage: StagingStorageResource, *, version: str,
                          capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and stage a complete unapproved TMAS stations candidate."""
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
    attempt_root = Path(capture_directory) if capture_directory else Path(
        "/tmp/hifld-native-reconstruction/331/captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    captures: dict[str, tuple[bytes, dict[str, object]]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/tmas-stations-draft"}) as client:
            seed_body, seed_receipt = _capture_with_retry(
                client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                capture_root, "ids_seed", max_bytes=MAX_CAPTURE_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            seed = json.loads(seed_body).get("objectIds")
            if (not isinstance(seed, list) or len(seed) != EXPECTED_COUNT
                    or any(type(value) is not int for value in seed) or len(set(seed)) != EXPECTED_COUNT):
                raise ValueError("source_seed_id_population_drift")
            captures["ids_seed"] = (seed_body, seed_receipt)
            for name, url in _doc_urls():
                captures[name] = _capture_with_retry(client, url, capture_root, name,
                                                     max_bytes=MAX_CAPTURE_BYTES,
                                                     attempts=DOC_CAPTURE_ATTEMPTS,
                                                     base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            layer_doc = json.loads(captures["layer_before"][0])
            if layer_doc.get("maxRecordCount") != EXPECTED_MAX_RECORD_COUNT \
                    or PAGE_START_RECORDS > layer_doc["maxRecordCount"]:
                raise ValueError("source_record_limit_drift")
            for offset in range(0, EXPECTED_COUNT, PAGE_START_RECORDS):
                count = min(PAGE_START_RECORDS, EXPECTED_COUNT - offset)
                name = _page_name(offset, count)
                captures[name] = _capture_with_retry(client, _page_query(offset, count),
                                                     capture_root, name,
                                                     max_bytes=MAX_CAPTURE_BYTES,
                                                     attempts=PAGE_CAPTURE_ATTEMPTS,
                                                     base_delay=PAGE_CAPTURE_BASE_DELAY_SECONDS)
            for name, url in _after_doc_urls():
                captures[name] = _capture_with_retry(client, url, capture_root, name,
                                                     max_bytes=MAX_CAPTURE_BYTES,
                                                     attempts=DOC_CAPTURE_ATTEMPTS,
                                                     base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
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
        retained = dict(captures)
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
    layer_doc = json.loads(captures["layer_before"][0])
    vintage_evidence = {
        "portal_description": portal_doc.get("description"),
        "portal_item_created_epoch_ms": portal_doc.get("created"),
        "portal_item_modified_epoch_ms": portal_doc.get("modified"),
        "layer_editing_info": layer_doc.get("editingInfo"),
        "data_dictionary": DATA_DICTIONARY_DOI,
        "release_date_selected": None,
        "freshness_accepted": False,
        "assessment": (
            "The producer item description records the NTAD edition vintage (compiled "
            "December 31, 2025; published March 23, 2026 from FHWA) and the item/layer edit "
            "timestamps are preserved separately; none is treated as an independently "
            "verified source release date. FHWA receives station data monthly and NTAD "
            "republishes the product."),
    }
    manifest = {
        "schema_version": 1, "approved": False, "title": f"{LAYER_NAME} (FHWA TMAS)",
        "description": "Federal Highway Administration (FHWA) Travel Monitoring Analysis "
                       "System (TMAS) Stations from the producer's NTAD publication, captured "
                       "completely from the delegated BTS NTAD FeatureServer (NTAD item "
                       "a016bbaee4794daca873135a5cf4164d, owner USDOT_BTS). Complete source "
                       "attributes and EPSG:4326 point geometry are represented in the "
                       "candidate. Source release date/freshness, public schema/ID contract, "
                       "and release acceptance remain unresolved and unapproved.",
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
                                                   "release date; the portal description edition "
                                                   "dates and edit timestamps are retained as "
                                                   "separate vintage evidence.",
                    "license": "https://www.usa.gov/publicdomain/label/1.0/",
                    "attribution": "BTS NTAD; item metadata states the work is a U.S. "
                                   "government work not protected by copyright, available for "
                                   "unrestricted public use; rights review remains pending."}
                   for name, (_, receipt) in sorted(captures.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "source_fields": "all six source fields retained in "
                                                             "producer order and physical types "
                                                             "(OBJECTID, Station_Id, "
                                                             "functional_class, state, latitude, "
                                                             "longitude)",
                                            "source_ids": "Preserve producer OBJECTID and "
                                                          "Station_Id values unchanged "
                                                          "(Station_Id repeats across rows: 6213 "
                                                          "distinct values, 1847 extra "
                                                          "occurrences in the captured roster; "
                                                          "duplicates preserved, never dropped); "
                                                          "no substitute IDs, casts or joins",
                                            "geometry": "Source EPSG:4326 point x/y retained as "
                                                        "WKB without reprojection or repair; "
                                                        "every geometry agrees with its own "
                                                        "latitude/longitude attributes within "
                                                        "1e-8 degrees; null source geometry "
                                                        "retained as null",
                                            "candidate_status": "unapproved; publication blocked "
                                                                "pending freshness, schema/ID "
                                                                "contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "FHWA's Travel Monitoring Analysis System (TMAS) Stations dataset is "
                   "published through the USDOT Bureau of Transportation Statistics (BTS) "
                   "National Transportation Atlas Database (NTAD). The DHS HIFLD Open "
                   "crosswalk (DOI 10.3886/E241367V1) points the retired HIFLD Open item "
                   "f4e62fe944cb489ab5131c25105348fe at exactly this BTS-hosted service. The "
                   "captured ArcGIS item is owned by USDOT_BTS in the BTS organization, its "
                   "serviceItemId matches the service, access is public, and the item "
                   "metadata states the work is a U.S. government work not protected by "
                   "copyright, available for unrestricted public use. The item description "
                   "records the NTAD edition vintage (compiled December 31, 2025; published "
                   "March 23, 2026 from FHWA) and cites the FHWA data dictionary "
                   "https://doi.org/10.21949/zxzj-hb56. The HIFLD Open inventory lists this "
                   "dataset as travel-monitoring-analysis-system-tmas-stations (publisher "
                   "FHWA); its DataLumos project 240593 archive copy and modification date "
                   "are identification and comparison context only, never pipeline inputs.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values and point geometry retained "
                      "in the producer CRS; all release and compatibility approvals remain "
                      "pending.",
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
             "source_relationship": "FHWA TMAS stations, BTS NTAD delegated publication",
             "source_vintage_evidence": vintage_evidence, "release_date_unknown": True,
             "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
             "release_accepted": False}
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    evidence = _capture_evidence(capture_root, captures)
    (capture_root / "capture_manifest.json").write_bytes(
        _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
                    "source_feature_count": EXPECTED_COUNT,
                    "source_record_sha256": audit["source_record_sha256"],
                    "source_geometry_sha256": audit["source_geometry_sha256"],
                    "receipts": {name: receipt for name, (_, receipt) in sorted(captures.items())},
                    "evidence": {stem: {"kind": entry["kind"],
                                        "files": {suffix: {"bytes": len(body), "sha256": _sha(body)}
                                                  for suffix, body in sorted(entry["files"].items())}}
                                 for stem, entry in sorted(evidence.items())}}) + b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    outputs = {rel: parquet_body, "metadata/source_manifest.json": _canonical(manifest) + b"\n",
               "metadata/source_audit.json": _canonical(audit) + b"\n",
               "metadata/processing_identity.json": _canonical(identity) + b"\n"}
    for name, (body, receipt) in captures.items():
        outputs[f"raw/{name}.body"] = body
        outputs[f"raw/{name}.wire"] = (capture_root / f"{name}.wire").read_bytes()
        outputs[f"raw/{name}.receipt.json"] = _canonical(receipt) + b"\n"
    for stem, entry in sorted(evidence.items()):
        for suffix, body in sorted(entry["files"].items()):
            outputs[f"raw/evidence/{stem}{suffix}"] = body
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
                "feature_count": EXPECTED_COUNT,
                "source_null_geometry_count": audit["null_geometry_count"],
                "source_invalid_geometry_count": audit["invalid_geometry_count"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False,
                "rights_accepted": False, "mapping_approved": False, "release_accepted": False}
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
            "feature_count": EXPECTED_COUNT, "source_null_geometry_count": audit["null_geometry_count"],
            "source_invalid_geometry_count": audit["invalid_geometry_count"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": False,
            "source_release_accepted": False, "source_freshness_accepted": False,
            "schema_contract_accepted": False, "id_contract_accepted": False,
            "rights_accepted": False, "mapping_approved": False, "release_accepted": False,
            "capture_directory": str(capture_root)}

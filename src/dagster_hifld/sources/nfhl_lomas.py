"""Lossless, approval-gated capture of the FEMA NFHL "LOMAs" point layer.

FEMA's National Flood Hazard Layer (NFHL) MapServer (service ``public/NFHL``,
documented by FEMA's MSC "GIS Web Services for the FEMA National Flood Hazard
Layer" page at the exact REST URL used here) exposes layer 34 "LOMAs" as the
producer's own publication of this HIFLD inventory dataset
(``nfhl/lomas-1``, DataLumos 239978 historical archive copy used for
identification only). The layer is continuously updated and publishes no
per-snapshot release date; source freshness, rights, and public-schema
compatibility are not accepted by this draft.

The producer serializes the Esri date field DATEENDED as an epoch-millisecond
integer and carries the LAT/LON doubles verbatim; both are retained verbatim
in the candidate.

Validation and candidate extraction stream: every page body is decoded once
from the attempt directory and the full-population record/geometry digests and
the GeoParquet candidate are built one feature at a time, so peak memory stays
bounded by the page size plus the metadata, not by the population.

Until a dated complete capture establishes the population pins below, the
module runs in live mode: acquisition and validation complete, the computed
population contract is written to ``population_pins.json`` in the attempt
directory, and the run fails closed with
``source_population_pins_not_established`` without staging anything. With
pins set, the module reproduces every pin and stages the unapproved candidate
exactly like its NFHL siblings.

The complete 543,656-feature layer was captured 2026-11-16 as 272 full-width
2,000-record pages of the sorted OBJECTID roster with unchanged before/after
count/id/metadata brackets, zero null-geometry rows and zero string-"NaN"
ordinate rows; the observed bounds are within the declared layer extent.
"""
from __future__ import annotations

import hashlib
import json
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
import shapely
from shapely.geometry import Point

from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.release_approval import validate_release_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource, default_capture_dir
from dagster_hifld.sources.capture_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    quarantine_incomplete_capture, seal_ready_candidate, verify_cached_candidate,
    write_immutable,
)
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

DATASET_SLUG = "nfhl-lomas"
FILE_SLUG = DATASET_SLUG
SERVICE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
SERVICE_DOC_URL = "https://hazards.fema.gov/femaportal/resources/flood_map_svc.htm"
LAYER_ID = 34
LAYER_URL = f"{SERVICE_URL}/{LAYER_ID}"
LAYER_NAME = "LOMAs"
PUBLISHER = "Federal Emergency Management Agency (FEMA)"
SOURCE_CRS_WKID = 4269
PAGE_SIZE = 2000
MAX_CAPTURE_BYTES = 64 * 1024 * 1024
MIN_FREE_BYTES = 3_000_000_000
# The producer's MapServer intermittently answers valid requests with complete
# HTTP 200 Esri error documents or transport failures (observed 2026-11-12
# while probing this layer; identical requests succeed moments later). Every
# request retries a bounded number of times for that transient mode; a retry
# re-issues the exact same request, and completeness is still enforced by the
# pinned before/after id rosters, counts, exact page id slices, and
# full-record digests, so retries can never mask drift.
DOC_CAPTURE_ATTEMPTS = 4
DOC_CAPTURE_BASE_DELAY_SECONDS = 10.0
PAGE_CAPTURE_ATTEMPTS = 3
PAGE_CAPTURE_BASE_DELAY_SECONDS = 5.0
# Pins below are the observed 2026-11-16 complete-layer population contract,
# established by a dated live-mode capture of all 272 full-width roster pages
# (before/after count/id/metadata brackets unchanged during capture); the
# module's own capture must reproduce every pin. The service layer roster is
# the same cheap independently re-verifiable service contract. The live-mode
# path (pins unset, contract written to ``population_pins.json``, nothing
# staged) remains available for future re-establishment.
EXPECTED_COUNT = 543656
EXPECTED_ID_SHA256 = "4082f46fb9f9c445f31422d654f828c729d3933dc3e54fc8e9abc0f18796102c"
EXPECTED_RECORD_SHA256 = "1c13fa6af7b42a694f8ec23ad762f06014e3e327bcf007b2c88c888cb39ae211"
EXPECTED_GEOMETRY_SHA256 = "53ee687bad81d2b0f0840bf4493288e7954024fb7a48996cad483f40e43c7dd2"
EXPECTED_SCHEMA_SHA256 = "328e606a5d54ce05e781cd0e7b9045912fbd1a2252fc871b9092a131ce37964d"
EXPECTED_NULL_GEOMETRY_COUNT = 0
EXPECTED_NAN_GEOMETRY_COUNT = 0
EXPECTED_OBSERVED_BOUNDS = {"xmin": -161.79400000032945, "ymin": 13.26000000001352,
                            "xmax": 144.78699999988265, "ymax": 64.95900000019401}
EXPECTED_LAYER_EXTENT = {"xmin": -161.79400323620945, "ymin": 13.259999734813519,
                         "xmax": 144.78700289562266, "ymax": 64.95900129937401}
EXPECTED_SERVICE_LAYER_ROSTER = [
    (0, "NFHL Availability", "Feature Layer"), (3, "FIRM Panels", "Feature Layer"),
    (1, "LOMRs", "Feature Layer"), (34, "LOMAs", "Feature Layer"),
    (22, "Political Jurisdictions", "Feature Layer"), (17, "Profile Baselines", "Feature Layer"),
    (20, "Water Lines", "Feature Layer"), (14, "Cross-Sections", "Feature Layer"),
    (16, "Base Flood Elevations", "Feature Layer"), (23, "Levees", "Feature Layer"),
    (29, "Seclusion Boundaries", "Feature Layer"), (15, "Coastal Transects", "Feature Layer"),
    (18, "Transect Baselines", "Feature Layer"), (24, "General Structures", "Feature Layer"),
    (7, "River Mile Markers", "Feature Layer"), (32, "Water Areas", "Feature Layer"),
    (5, "PLSS", "Feature Layer"), (19, "Limit of Moderate Wave Action", "Feature Layer"),
    (27, "Flood Hazard Boundaries", "Feature Layer"), (28, "Flood Hazard Zones", "Feature Layer"),
    (25, "Primary Frontal Dunes", "Feature Layer"), (4, "Base Index", "Feature Layer"),
    (6, "Topographic Low Confidence Areas", "Feature Layer"), (8, "Datum Conversion Points", "Feature Layer"),
    (9, "Coastal Gages", "Feature Layer"), (10, "Gages", "Feature Layer"),
    (11, "Nodes", "Feature Layer"), (12, "High Water Marks", "Feature Layer"),
    (13, "Station Start Points", "Feature Layer"), (26, "Hydrologic Reaches", "Feature Layer"),
    (30, "Alluvial Fans", "Feature Layer"), (31, "Subbasins", "Feature Layer"),
]


class _PinsPendingError(Exception):
    """Raised when a live-mode capture completes but population pins are unset."""


def _stable_layer(doc: object) -> object:
    """Layer document without the producer's volatile display symbology.

    The layer JSON's ``drawingInfo`` (renderer/labeling symbology) changed
    between the 2026-11-12 before/after layer documents of one unchanged
    capture window (observed on the live LOMAs layer); it is cosmetic display
    state, not data. Every data-bearing part of the layer contract — fields,
    geometry type, extent, count, pagination — is still compared, and the
    published schema/ID contract remains pinned separately, so a symbology
    edit cannot mask real drift.
    """
    if not isinstance(doc, dict):
        return doc
    stable = dict(doc)
    stable.pop("drawingInfo", None)
    return stable


def _plain_field(field: Mapping[str, object]) -> dict[str, object]:
    return {key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain") if key in field}


def _query(params: dict[str, object]) -> str:
    return LAYER_URL + "/query?" + urlencode(params)


def _feature_urls(ids: list[int]) -> list[tuple[str, str]]:
    urls = [
        ("service_doc", SERVICE_DOC_URL),
        ("service_before", SERVICE_URL + "?f=pjson"),
        ("layer_before", LAYER_URL + "?f=pjson"),
        ("count_before", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_before", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ]
    for offset in range(0, len(ids), PAGE_SIZE):
        params = {"where": "1=1", "outFields": "*", "returnGeometry": "true",
                  "orderByFields": "OBJECTID ASC", "resultOffset": offset,
                  "resultRecordCount": PAGE_SIZE, "f": "json"}
        urls.append((f"page_{offset // PAGE_SIZE:03d}", _query(params)))
    urls.extend([
        ("layer_after", LAYER_URL + "?f=pjson"),
        ("service_after", SERVICE_URL + "?f=pjson"),
        ("count_after", _query({"where": "1=1", "returnCountOnly": "true", "f": "json"})),
        ("ids_after", _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
    ])
    return urls


def _validate_spatial_reference(value: object, label: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(label + "_crs_missing")
    for key in ("wkid", "latestWkid"):
        wkid = value.get(key)
        if wkid is not None and wkid != SOURCE_CRS_WKID:
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


def _capture_with_retry(client: httpx.Client, url: str, capture_root: Path, name: str, *,
                        max_bytes: int, attempts: int, base_delay: float) -> tuple[bytes, dict[str, object]]:
    """Capture one producer request, retrying bounded times on transient failures.

    Retries cover the producer's transient complete-200 Esri error documents and
    incomplete transport captures; failed attempts are preserved on disk under
    ``<name>.attempt-NN`` evidence names and never enter the capture roster. The
    final attempt's body is always returned as captured, so a persistent
    producer failure surfaces to the caller unchanged.
    """
    for attempt in range(1, attempts + 1):
        try:
            body, receipt = capture_get(client, url, capture_root, name, max_bytes=max_bytes)
        except ValueError as exc:
            if attempt >= attempts or not str(exc).startswith("incomplete_source_capture:"):
                raise
            _preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        if _producer_error_doc(body) and attempt < attempts:
            _preserve_failed_attempt(capture_root, name, attempt)
            time.sleep(base_delay * (3 ** (attempt - 1)))
            continue
        return body, receipt
    raise AssertionError("unreachable_retry_state")


def _read_capture_body(capture_root: Path, name: str) -> bytes:
    return (capture_root / f"{name}.body").read_bytes()


def _read_capture_doc(capture_root: Path, name: str) -> object:
    return json.loads(_read_capture_body(capture_root, name))


def _validate_service_brackets(docs: dict[str, object], count: int) -> \
        tuple[dict[str, object], list, list[int], list[str], str]:
    """Validate the service/layer/count/roster bracket documents of one attempt.

    Returns the layer document, the field roster, the sorted OBJECTID population,
    the exact attribute key order every record must carry, and the field-schema
    digest. Roster documents are checked one at a time so no two id populations
    are held simultaneously; each must carry the same unique integer population
    as the sorted ``ids_before`` roster.
    """
    service, layer = docs["service_before"], docs["layer_before"]
    if not isinstance(service, dict) or service.get("mapName") != "NFHL":
        raise ValueError("official_fema_nfhl_service_identity_drift")
    service_layers = service.get("layers")
    roster = [(x.get("id"), x.get("name"), x.get("type")) for x in service_layers if isinstance(x, dict)] \
        if isinstance(service_layers, list) else None
    if roster != EXPECTED_SERVICE_LAYER_ROSTER:
        raise ValueError("fema_nfhl_service_layer_roster_drift")
    if not any(x.get("id") == LAYER_ID and x.get("name") == LAYER_NAME and x.get("type") == "Feature Layer"
               for x in service_layers):
        raise ValueError("exact_nfhl_lomas_layer_missing")
    if (layer.get("id") != LAYER_ID or layer.get("name") != LAYER_NAME
            or layer.get("type") != "Feature Layer" or layer.get("geometryType") != "esriGeometryPoint"):
        raise ValueError("exact_nfhl_lomas_layer_metadata_drift")
    if layer.get("hasZ") is True or layer.get("hasM") is True:
        raise ValueError("source_z_or_m_geometry_unsupported")
    if layer.get("advancedQueryCapabilities", {}).get("supportsPagination") is not True:
        raise ValueError("source_pagination_capability_changed")
    if layer.get("maxRecordCount") != 2000:
        raise ValueError("source_record_limit_drift")
    _validate_spatial_reference(layer.get("sourceSpatialReference"), "source_layer")
    extent = layer.get("extent", {})
    _validate_spatial_reference(extent.get("spatialReference"), "source_layer_extent")
    if EXPECTED_LAYER_EXTENT is not None \
            and {key: extent.get(key) for key in EXPECTED_LAYER_EXTENT} != EXPECTED_LAYER_EXTENT:
        raise ValueError("source_layer_extent_drift")
    fields_value = layer.get("fields")
    if not isinstance(fields_value, list) or any(not isinstance(x, dict) for x in fields_value):
        raise ValueError("source_field_roster_drift")
    fields = [_plain_field(x) for x in fields_value]
    schema_sha = _sha(_canonical(fields))
    if EXPECTED_SCHEMA_SHA256 is not None and schema_sha != EXPECTED_SCHEMA_SHA256:
        raise ValueError("source_schema_contract_drift")
    if docs["layer_after"].get("fields") is None or [_plain_field(x) for x in docs["layer_after"]["fields"]] != fields:
        raise ValueError("source_field_schema_changed_during_capture")
    for name in ("layer_before", "layer_after"):
        if _stable_layer(docs[name]) != _stable_layer(layer):
            raise ValueError("source_layer_metadata_changed_during_capture")
    if docs["service_after"] != service:
        raise ValueError("source_service_metadata_changed_during_capture")
    if (docs["count_before"].get("count") != count
            or docs["count_after"].get("count") != count):
        raise ValueError("source_count_drift")
    if EXPECTED_COUNT is not None and count != EXPECTED_COUNT:
        raise ValueError("source_count_drift")
    base = docs["ids_before"].get("objectIds")
    if (not isinstance(base, list) or len(base) != count
            or any(type(value) is not int for value in base) or len(set(base)) != count):
        raise ValueError("source_id_population_drift")
    ids = sorted(base)
    for name in ("ids_seed", "ids_after"):
        roster_ids = docs[name].get("objectIds")
        if (not isinstance(roster_ids, list) or len(roster_ids) != count
                or any(type(value) is not int for value in roster_ids) or len(set(roster_ids)) != count
                or sorted(roster_ids) != ids):
            raise ValueError("source_id_population_drift")
    if EXPECTED_ID_SHA256 is not None and _sha(_canonical(ids)) != EXPECTED_ID_SHA256:
        raise ValueError("source_id_roster_drift")
    if any(docs[name].get("objectIdFieldName") != "OBJECTID" for name in ("ids_before", "ids_after", "ids_seed")):
        raise ValueError("source_id_field_changed")
    if docs["count_before"] != docs["count_after"]:
        raise ValueError("source_count_changed_during_capture")
    attr_names = [field["name"] for field in fields if field["name"] != "SHAPE"]
    return layer, fields_value, ids, attr_names, schema_sha


def _grow_bounds(bounds: dict[str, float] | None, x: float, y: float) -> dict[str, float]:
    if bounds is None:
        return {"xmin": x, "ymin": y, "xmax": x, "ymax": y}
    if x < bounds["xmin"]:
        bounds["xmin"] = x
    if x > bounds["xmax"]:
        bounds["xmax"] = x
    if y < bounds["ymin"]:
        bounds["ymin"] = y
    if y > bounds["ymax"]:
        bounds["ymax"] = y
    return bounds


class _CandidateParquetWriter:
    """Write the candidate GeoParquet incrementally, one row group per page.

    The schema, field order, physical Arrow types and GeoParquet metadata match
    the producer's contract exactly; only the on-disk row-group layout differs
    from a single-shot write, and no row of the population is retained after its
    row group is emitted.
    """

    def __init__(self, path: Path, fields_value: list) -> None:
        fields = [_plain_field(field) for field in fields_value]
        self._types: dict[str, pa.DataType] = {}
        schema_fields = []
        for field in fields:
            name = field["name"]
            typ = pa.binary() if name == "SHAPE" else _arrow_type(field)
            self._types[name] = typ
            schema_fields.append(pa.field(name, typ, nullable=True))
        geo = {"version": "1.1.0", "primary_column": "SHAPE", "columns": {"SHAPE": {
            "encoding": "WKB", "geometry_types": ["Point"],
            "crs": pyproj.CRS.from_epsg(SOURCE_CRS_WKID).to_json_dict(), "edges": "planar"}}}
        self._schema = pa.schema(schema_fields).with_metadata({b"geo": _canonical(geo)})
        self._writer = pq.ParquetWriter(str(path), self._schema, compression="zstd")

    def add_page(self, rows: list) -> None:
        columns = {}
        for name, typ in self._types.items():
            if name == "SHAPE":
                values = [Point(row["geometry"]["x"], row["geometry"]["y"]).wkb
                          if _usable_point(row.get("geometry")) else None for row in rows]
            else:
                values = [row["attributes"].get(name) for row in rows]
            columns[name] = pa.array(values, type=typ)
        self._writer.write_table(pa.Table.from_pydict(columns, schema=self._schema))

    def close(self) -> None:
        self._writer.close()


def _stream_validate_pages(pages, *, count: int, ids: list[int], attr_names: list[str],
                           writer: _CandidateParquetWriter | None) -> dict[str, object]:
    """Stream-validate every page document and hash the full populations incrementally.

    Every page's ids must equal its ascending roster slice, so features arrive in
    ascending OBJECTID order — the exact order the full-population canonical JSON
    digests are computed over — and both digests plus the GeoParquet candidate can
    be built one feature at a time without retaining the population in memory.
    """
    record_digest, geometry_digest = hashlib.sha256(), hashlib.sha256()
    record_digest.update(b"[")
    geometry_digest.update(b"[")
    separator = b""
    null_geometry = 0
    nan_geometry = 0
    bounds: dict[str, float] | None = None
    seen = 0
    for page_idx, offset in enumerate(range(0, count, PAGE_SIZE)):
        page = next(pages)
        if not isinstance(page, dict) or "error" in page:
            raise ValueError("source_page_error")
        _validate_spatial_reference(page.get("spatialReference"), f"source_page_{page_idx:03d}")
        rows = page.get("features")
        if not isinstance(rows, list):
            raise ValueError("source_page_missing_features")
        expected = ids[offset:offset + PAGE_SIZE]
        actual = [r.get("attributes", {}).get("OBJECTID") for r in rows if isinstance(r, dict)]
        full_nonfinal = len(expected) == PAGE_SIZE and offset + PAGE_SIZE < count
        exceeded = page.get("exceededTransferLimit") is True
        if actual != expected or len(rows) != len(expected):
            raise ValueError("source_page_id_slice_incomplete_or_reordered")
        if exceeded and not full_nonfinal:
            raise ValueError("source_page_unexpected_exceeded_transfer_limit")
        for feature in rows:
            attrs = feature.get("attributes")
            if not isinstance(attrs, dict) or list(attrs) != attr_names:
                raise ValueError("source_record_attribute_schema_drift")
            oid = attrs["OBJECTID"]
            geom = feature.get("geometry")
            if geom is None:
                null_geometry += 1
                geometry_record = [oid, None, None]
            elif isinstance(geom, dict) and set(geom) == {"x", "y"} \
                    and all(isinstance(geom[k], str) and geom[k] == "NaN" for k in ("x", "y")):
                # FEMA publishes one coastal gage row with Esri string-"NaN" ordinates;
                # the values are retained verbatim in the record/raw evidence and gated
                # as unusable geometry here, never silently coerced to a location.
                nan_geometry += 1
                geometry_record = [oid, geom["x"], geom["y"]]
            elif isinstance(geom, dict) and set(geom) == {"x", "y"} \
                    and all(type(geom[k]) in (int, float) for k in ("x", "y")):
                x, y = geom["x"], geom["y"]
                if not (-180 <= x <= 180 and -90 <= y <= 90):
                    raise ValueError("source_coordinate_range_drift")
                geometry_record = [oid, x, y]
                bounds = _grow_bounds(bounds, x, y)
            else:
                raise ValueError("source_point_geometry_invalid")
            element = _canonical(feature)
            record_digest.update(separator)
            record_digest.update(element)
            geometry_digest.update(separator)
            geometry_digest.update(_canonical(geometry_record))
            separator = b","
            seen += 1
        if writer is not None:
            writer.add_page(rows)
    record_digest.update(b"]")
    geometry_digest.update(b"]")
    record_sha = record_digest.hexdigest()
    geometry_sha = geometry_digest.hexdigest()
    if EXPECTED_GEOMETRY_SHA256 is not None and geometry_sha != EXPECTED_GEOMETRY_SHA256:
        raise ValueError("source_geometry_or_coverage_drift")
    if EXPECTED_RECORD_SHA256 is not None and record_sha != EXPECTED_RECORD_SHA256:
        raise ValueError("source_record_value_drift")
    if EXPECTED_OBSERVED_BOUNDS is not None and bounds != EXPECTED_OBSERVED_BOUNDS:
        raise ValueError("source_observed_coverage_drift")
    return {"source_feature_count": seen, "source_record_sha256": record_sha,
            "source_geometry_sha256": geometry_sha, "null_geometry_count": null_geometry,
            "nan_string_geometry_count": nan_geometry, "observed_bounds": bounds}


def _validate_and_extract(capture_root: Path, receipts: Mapping[str, Mapping[str, object]], *,
                          parquet_path: Path | None) -> tuple[dict, list, dict[str, object]]:
    """Stream-validate the captured attempt against the population contract.

    Every capture body is decoded once from the attempt directory; no capture
    body, decoded page or full-population structure is retained in memory. With
    ``parquet_path`` set, the candidate GeoParquet is written row group by row
    group while the pages stream through validation; in live mode (pins unset)
    it stays None and the candidate is not built.
    """
    docs = {name: _read_capture_doc(capture_root, name) for name in
            ("service_before", "layer_before", "count_before", "count_after", "layer_after", "service_after",
             "ids_before", "ids_seed", "ids_after")}
    count = docs["count_before"].get("count") if isinstance(docs["count_before"], dict) else None
    if not isinstance(count, int) or count < 0:
        raise ValueError("source_count_drift")
    page_count = (count + PAGE_SIZE - 1) // PAGE_SIZE
    expected_names = {"service_doc", "service_before", "layer_before", "count_before", "ids_before", "ids_seed",
                      "layer_after", "service_after", "count_after", "ids_after",
                      *(f"page_{i:03d}" for i in range(page_count))}
    if set(receipts) != expected_names:
        raise ValueError("source_capture_roster_incomplete_or_unexpected")
    if SERVICE_URL.encode("utf-8") not in _read_capture_body(capture_root, "service_doc"):
        raise ValueError("fema_service_documentation_link_drift")
    layer, fields_value, ids, attr_names, schema_sha = _validate_service_brackets(docs, count)
    writer = _CandidateParquetWriter(parquet_path, fields_value) if parquet_path is not None else None
    try:
        tail = _stream_validate_pages(
            (_read_capture_doc(capture_root, f"page_{page_idx:03d}") for page_idx in range(page_count)),
            count=count, ids=ids, attr_names=attr_names, writer=writer)
    finally:
        if writer is not None:
            writer.close()
    audit = {"source_feature_count": tail["source_feature_count"], "source_id_sha256": _sha(_canonical(ids)),
             "source_record_sha256": tail["source_record_sha256"],
             "source_geometry_sha256": tail["source_geometry_sha256"],
             "source_schema_sha256": schema_sha, "source_crs_epsg": SOURCE_CRS_WKID,
             "geometry_type": "Point", "null_geometry_count": tail["null_geometry_count"],
             "nan_string_geometry_count": tail["nan_string_geometry_count"],
             "observed_bounds": tail["observed_bounds"], "declared_layer_extent": EXPECTED_LAYER_EXTENT,
             "unique_source_id_count": len(ids),
             "source_release_date": None,
             "source_release_date_unknown_reason":
                 "FEMA updates the NFHL continuously and publishes no release date for this layer; "
                 "fetch time and the DataLumos archive modification date (2024-10-31) are not release dates."}
    contract = {}
    if EXPECTED_COUNT is None:
        contract = {"expected_count": tail["source_feature_count"], "expected_id_sha256": _sha(_canonical(ids)),
                    "expected_record_sha256": tail["source_record_sha256"],
                    "expected_geometry_sha256": tail["source_geometry_sha256"],
                    "expected_schema_sha256": schema_sha,
                    "expected_observed_bounds": tail["observed_bounds"],
                    "expected_null_geometry_count": tail["null_geometry_count"],
                    "expected_nan_geometry_count": tail["nan_string_geometry_count"],
                    "expected_layer_extent": {key: layer.get("extent", {}).get(key) for key in
                                              ("xmin", "ymin", "xmax", "ymax")}}
    return layer, fields_value, {"audit": audit, "ids": ids, "contract": contract}


def _arrow_type(field: Mapping[str, object]) -> pa.DataType:
    return {"esriFieldTypeOID": pa.int64(),
            "esriFieldTypeGlobalID": pa.string(),
            "esriFieldTypeString": pa.string(),
            "esriFieldTypeDouble": pa.float64(),
            # The producer serializes the Esri date fields START_PD/END_PD as
            # epoch-millisecond integers in its own JSON; they are kept verbatim
            # as int64, never reinterpreted or reformatted.
            "esriFieldTypeDate": pa.int64(),
            "esriFieldTypeInteger": pa.int64()}[str(field["type"])]


def _usable_point(geom: object) -> bool:
    return isinstance(geom, dict) and type(geom.get("x")) in (int, float) and type(geom.get("y")) in (int, float)


class _RetainedCaptureEvidence(Mapping[str, tuple[bytes, dict[str, object]]]):
    """Disk-backed capture-evidence view for bounded-memory quarantine.

    Quarantine iterates the mapping once, so each body is read from the attempt
    directory only when its turn comes and is dropped before the next name is
    written; no full attempt is ever held in memory.
    """

    def __init__(self, capture_root: Path, receipts: dict[str, dict[str, object]]) -> None:
        self._capture_root = capture_root
        self._receipts = receipts

    def __len__(self) -> int:
        return len(self._receipts)

    def __iter__(self):
        return iter(self._receipts)

    def __getitem__(self, name: str) -> tuple[bytes, dict[str, object]]:
        return (self._capture_root / f"{name}.body").read_bytes(), self._receipts[name]


def _code_identity() -> dict[str, object]:
    identity = processing_identity(Path(__file__).parents[3])
    identity["gdal"] = __import__("pyogrio").__gdal_version_string__
    return identity


def capture_nfhl_lomas(storage: StagingStorageResource, *, version: str,
                               capture_directory: str | None = None) -> dict[str, object]:
    """Capture, verify, and (with pins set) stage the NFHL LOMAs candidate.

    With population pins unset, the capture completes in live mode: the
    computed population contract is written to ``population_pins.json`` in the
    attempt directory and the run fails closed without staging anything.
    """
    if not isinstance(storage, StagingStorageResource) or isinstance(storage, PublishedStorageResource) \
            or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise ValueError("explicit_local_staging_resource_required")
    if not version or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_" for c in version):
        raise ValueError("invalid_version")
    base = Path(storage.local_dir).absolute()
    if ".." in base.parts or any(parent.is_symlink() for parent in (base, *base.parents)):
        raise ValueError("unsafe_local_staging_root")
    base.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(base).free < MIN_FREE_BYTES:
        raise ValueError("disk_floor_before_acquisition")
    attempt_root = Path(capture_directory) if capture_directory else default_capture_dir("hifld-native-reconstruction", "201", "captures")
    capture_root = (attempt_root / f"{version}-{uuid4().hex}").absolute()
    if ".." in capture_root.parts or any(parent.is_symlink() for parent in (capture_root, *capture_root.parents)):
        raise ValueError("unsafe_capture_directory")
    capture_root.mkdir(parents=True, exist_ok=True)
    receipts: dict[str, dict[str, object]] = {}
    try:
        with httpx.Client(follow_redirects=False, timeout=180,
                          headers={"User-Agent": "hifld-next-datasets/nfhl-lomas-draft"}) as client:
            seed_body, seed_receipt = _capture_with_retry(
                client, _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                capture_root, "ids_seed", max_bytes=MAX_CAPTURE_BYTES,
                attempts=DOC_CAPTURE_ATTEMPTS, base_delay=DOC_CAPTURE_BASE_DELAY_SECONDS)
            seed = json.loads(seed_body).get("objectIds")
            if (not isinstance(seed, list) or any(type(value) is not int for value in seed)
                    or len(set(seed)) != len(seed)):
                raise ValueError("source_seed_id_population_drift")
            receipts["ids_seed"] = seed_receipt
            for name, url in _feature_urls(sorted(seed)):
                _, receipt = _capture_with_retry(
                    client, url, capture_root, name, max_bytes=MAX_CAPTURE_BYTES,
                    attempts=PAGE_CAPTURE_ATTEMPTS if name.startswith("page_") else DOC_CAPTURE_ATTEMPTS,
                    base_delay=PAGE_CAPTURE_BASE_DELAY_SECONDS if name.startswith("page_")
                    else DOC_CAPTURE_BASE_DELAY_SECONDS)
                # Capture bodies stay on disk in the attempt directory; only the
                # small hash-pinned receipts are kept in memory and streamed back
                # during validation, so the population is never held.
                receipts[name] = receipt
        parquet_path = None if EXPECTED_COUNT is None else capture_root / "candidate.parquet"
        layer, source_fields, extracted = _validate_and_extract(capture_root, receipts, parquet_path=parquet_path)
    except Exception as exc:
        receipts_all: dict[str, dict[str, object]] = {}
        quarantinable: dict[str, dict[str, object]] = {}
        for path in sorted(capture_root.glob("*.receipt.json")):
            name = path.name.removesuffix(".receipt.json")
            try:
                receipt = json.loads(path.read_bytes())
            except (ValueError, OSError):
                receipts_all[name] = {"receipt_readback_failed": True}
                continue
            receipts_all[name] = receipt
            if (capture_root / f"{name}.body").exists():
                quarantinable[name] = receipt
        (capture_root / "capture_manifest.json").write_bytes(
            _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": False,
                        "error_code": type(exc).__name__, "receipts": receipts_all}) + b"\n")
        if quarantinable:
            quarantine_incomplete_capture(storage, capture_root, DATASET_SLUG, version,
                                          _RetainedCaptureEvidence(capture_root, quarantinable),
                                          type(exc).__name__)
        raise
    if EXPECTED_COUNT is None:
        (capture_root / "population_pins.json").write_bytes(_canonical(
            {"established_by": "live-mode complete capture (pins unset at capture time)",
             "captured_at": datetime.now(timezone.utc).isoformat(),
             "layer_id": LAYER_ID, "layer_name": LAYER_NAME, **extracted["contract"]}) + b"\n")
        (capture_root / "capture_manifest.json").write_bytes(_canonical(
            {"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True, "staged": False,
             "reason_code": "source_population_pins_not_established",
             "source_feature_count": extracted["audit"]["source_feature_count"]}) + b"\n")
        raise _PinsPendingError("source_population_pins_not_established")
    identity = _code_identity()
    parquet_body = parquet_path.read_bytes()
    rel = f"geoparquet/{DATASET_SLUG}.parquet"
    source_objects = {rel: {"bytes": len(parquet_body), "sha256": _sha(parquet_body)}}
    manifest = {
        "schema_version": 1, "approved": False, "title": "LOMAs (FEMA NFHL)",
        "description": "FEMA National Flood Hazard Layer (NFHL) MapServer layer 34 'LOMAs', captured "
                       "completely from the producer's own REST publication. Complete source attributes and NAD83 "
                       "point geometry are represented in the candidate. The NFHL is continuously updated and "
                       "publishes no per-snapshot release date; freshness, rights, public schema/ID contract, and "
                       "release acceptance remain unresolved and unapproved.",
        "publisher": PUBLISHER, "agency": "Federal Emergency Management Agency", "source_url": LAYER_URL,
        "license": None, "source_modified": None, "date_issued": None, "date_modified": None,
        "processor": "HIFLD Next", "code_commit": identity["code_commit"],
        "inputs": [{"input_id": name, "producer": PUBLISHER, "host": urlsplit(receipt["url"]).hostname,
                    "role": "producer_publication",
                    "canonical_url": SERVICE_DOC_URL if name == "service_doc" else LAYER_URL,
                    "retrieval_url": receipt["url"], "observed_final_url": receipt["final_url"],
                    "retrieved_at": receipt["observed_at"], "http_status": receipt["status"],
                    "bytes": receipt["bytes"], "sha256": receipt["sha256"],
                    "release_date": None,
                    "release_date_unknown_reason": "The NFHL is continuously updated; the service and its "
                                                   "documentation page publish no per-snapshot release date.",
                    "license": "Not stated by the captured endpoints; rights review pending.",
                    "attribution": "FEMA National Flood Hazard Layer; formal attribution terms require maintainer "
                                   "review."}
                   for name, receipt in sorted(receipts.items())],
        "transformations": [{"operation": "native_esri_json_to_geoparquet_candidate",
                             "parameters": {"layer_id": LAYER_ID,
                                            "source_fields": "all producer fields retained in producer order and "
                                                             "physical types, including the producer's DATEENDED "
                                                             "Esri date field serialized as an epoch-millisecond "
                                                             "integer and the LAT/LON doubles verbatim; SHAPE is "
                                                             "the original geometry field represented as WKB",
                                            "source_ids": "Preserve producer OBJECTID, CASENUMBER, CID, PDFHYPERLINKID, "
                                                          "GFID and GlobalID values unchanged; no substitute IDs, "
                                                          "casts, trimming, or joins",
                                            "geometry": "Source NAD83 (EPSG:4269) point x/y retained as WKB without "
                                                        "projection; null source geometry retained as null; any "
                                                        "producer string-'NaN' ordinate rows are retained as "
                                                        "records with null candidate geometry, raw evidence intact",
                                            "candidate_status": "unapproved; publication blocked pending freshness, "
                                                                "schema/ID contract, attribution and release "
                                                                "acceptance"}}],
        "lineage": "FEMA publishes the NFHL as its own geospatial database (fema.gov/flood-maps/national-flood-hazard-"
                   "layer). FEMA's MSC page 'GIS Web Services for the FEMA National Flood Hazard Layer (NFHL)' "
                   "documents this exact REST URL (https://hazards.fema.gov/arcgis/rest/services/public/NFHL/"
                   "MapServer) as the ArcGIS REST service for effective NFHL data; its HTML is captured as "
                   "'service_doc' evidence. This candidate is exactly layer 34 'LOMAs' of that producer "
                   "service. The HIFLD Open inventory lists this dataset as nfhl/lomas-1 with DataLumos "
                   "project 239978; that ICPSR archive copy and the 2024-10-31 archive modification date are "
                   "identification and comparison context only, never pipeline inputs.",
        "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
            "reason": "Complete native source fields, IDs, values, physical nulls, and geometry retained in the "
                      "producer CRS; all release and compatibility approvals remain pending.",
            "objects": [{"path": rel, **source_objects[rel]}]}}},
        "unsupported_output_formats": {"shapefile_zip": "No approved source-to-public Shapefile field mapping.",
                                       "geopackage": "No approved source-to-public schema mapping.",
                                       "pmtiles": "No display transformation or generalized geometry approved."},
    }
    validate_release_provenance(manifest)
    validate_output_policy(manifest)
    audit = {**extracted["audit"], "source_fields": [_plain_field(f) for f in source_fields],
             "official_layer_name": LAYER_NAME, "layer_id": LAYER_ID, "service_url": SERVICE_URL,
             "release_date_unknown": True, "freshness_accepted": False, "schema_contract_accepted": False,
             "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
             "release_accepted": False}
    version = content_derived_version(audit["source_record_sha256"], audit["source_geometry_sha256"],
                                      audit["source_schema_sha256"], identity)
    prefix = candidate_prefix(DATASET_SLUG, FILE_SLUG, version)
    (capture_root / "capture_manifest.json").write_bytes(
        _canonical({"captured_at": datetime.now(timezone.utc).isoformat(), "complete": True,
                    "source_feature_count": EXPECTED_COUNT,
                    "source_record_sha256": audit["source_record_sha256"],
                    "source_geometry_sha256": audit["source_geometry_sha256"],
                    "receipts": dict(sorted(receipts.items()))}) + b"\n")
    state_key = f"_construction-state/{DATASET_SLUG}/{version}.json"
    fingerprint = _sha(_canonical(identity))
    if storage.object_snapshot(state_key) is not None:
        bundle = json.loads(load_first_construction(storage, state_key, version, fingerprint))
        payload = bundle.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("cached_first_candidate_invalid")
        objects = payload.get("objects", {})
        verify_cached_candidate(storage, Path(__file__).parents[3], state_key, prefix, version, fingerprint, identity,
                                audit["source_record_sha256"], audit["source_schema_sha256"], objects,
                                payload.get("capture_receipts", {}))
        return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
                "feature_count": EXPECTED_COUNT, "source_null_geometry_count": audit["null_geometry_count"],
                "source_nan_string_geometry_count": audit["nan_string_geometry_count"],
                "candidate_ready": prefix + "/metadata/candidate_ready.json", "reused": True,
                "source_release_accepted": False, "source_freshness_accepted": False,
                "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
                "mapping_approved": False, "release_accepted": False}
    descriptors: dict[str, dict[str, object]] = {}

    def _stage(name: str, body: bytes) -> None:
        key = f"{prefix}/{name}"
        write_immutable(storage, key, body)
        descriptor = {"bytes": len(body), "sha256": _sha(body)}
        snap = storage.object_snapshot(key)
        if snap is None or snap.size != descriptor["bytes"] or storage.sha256_key(key) != descriptor["sha256"] \
                or storage.object_snapshot(key) != snap:
            raise ValueError("candidate_output_physical_readback_failed:" + name)
        descriptors[name] = descriptor

    _stage(rel, parquet_body)
    _stage("metadata/source_manifest.json", _canonical(manifest) + b"\n")
    _stage("metadata/source_audit.json", _canonical(audit) + b"\n")
    _stage("metadata/processing_identity.json", _canonical(identity) + b"\n")
    for name in sorted(receipts):
        _stage(f"raw/{name}.body", _read_capture_body(capture_root, name))
        _stage(f"raw/{name}.wire", (capture_root / f"{name}.wire").read_bytes())
        _stage(f"raw/{name}.receipt.json", _canonical(receipts[name]) + b"\n")
    _stage("metadata/candidate_ready.json", _canonical(
        {"approved": False, "version": version, "objects": source_objects,
         "source_record_sha256": audit["source_record_sha256"], "freshness_accepted": False,
         "schema_contract_accepted": False, "id_contract_accepted": False, "rights_accepted": False,
         "mapping_approved": False, "release_accepted": False}) + b"\n")
    payload = {"processing_identity": identity,
               "source_record_sha256": audit["source_record_sha256"],
               "source_schema_sha256": audit["source_schema_sha256"],
               "capture_receipts": receipts,
               "objects": descriptors}
    seal_ready_candidate(storage, state_key, version, fingerprint, payload)
    validate_construction_code_identity(Path(__file__).parents[3], identity["code_commit"], identity["files"])
    return {"dataset_slug": DATASET_SLUG, "file_slug": FILE_SLUG, "version": version,
            "feature_count": EXPECTED_COUNT, "source_null_geometry_count": audit["null_geometry_count"],
            "source_nan_string_geometry_count": audit["nan_string_geometry_count"],
            "candidate_ready": prefix + "/metadata/candidate_ready.json",
            "source_release_accepted": False, "source_freshness_accepted": False, "schema_contract_accepted": False,
            "id_contract_accepted": False, "rights_accepted": False, "mapping_approved": False,
            "release_accepted": False, "capture_directory": str(capture_root)}

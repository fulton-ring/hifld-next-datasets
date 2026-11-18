from __future__ import annotations

import contextlib
import hashlib
import json
import shutil
import struct
import sys
import tempfile
import tracemalloc
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from shapely import from_wkb

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.wbd_10_digit_hu import (
    wbd_10_digit_hu, wbd_10_digit_hu_complete, wbd_10_digit_hu_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import wbd_10_digit_hu as source
from dagster_hifld.sources.esri_geometry import to_wkb
from dagster_hifld.sources.arcgis_layer import _canonical

_TEST_COUNT = 5
_TEST_PAGE_SIZE = 2
_TEST_PAGE_COUNT = 3
_TEST_ROSTER = [11, 12, 501, 100_003, 200_004]
_SR = {"wkid": 102100, "latestWkid": 3857}
_SERVICE_DOC = ("<html>USGS National Hydrography products: WBD data downloads and services "
                "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer "
                "https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/"
                "Hydrography/WBD/National/</html>")

_GATE_FEATURES = {501: "source_polygon_ring_degenerate",
                  100_003: "source_polygon_ring_self_intersecting",
                  200_004: "source_polygon_assembly_invalid"}


@contextlib.contextmanager
def patches(module, **values):
    with contextlib.ExitStack() as stack:
        for name, value in values.items():
            stack.enter_context(patch.object(module, name, value))
        yield


@contextlib.contextmanager
def relaxed_pins(**extra):
    scale = {
        "EXPECTED_COUNT": _TEST_COUNT, "PAGE_SIZE": _TEST_PAGE_SIZE,
        "PAGE_COUNT": _TEST_PAGE_COUNT,
        "EXPECTED_ID_MIN": min(_TEST_ROSTER), "EXPECTED_ID_MAX": max(_TEST_ROSTER),
        "EXPECTED_ID_SHA256": "", "EXPECTED_RECORD_SHA256": "",
        "EXPECTED_GEOMETRY_SHA256": "", "EXPECTED_NULL_GEOMETRY_COUNT": None,
        "EXPECTED_UNUSABLE_GEOMETRY_COUNTS": None,
    }
    scale.update(extra)
    with patches(source, **scale):
        yield


def _shell(oid: int) -> list[list[float]]:
    x0 = -13131705.0 + (oid % 7) * 9000.0
    y0 = 3984685.0 + ((oid // 7) % 900) * 7000.0
    return [[x0, y0], [x0, y0 + 4000.0], [x0 + 5000.0, y0 + 4000.0],
            [x0 + 5000.0, y0], [x0, y0]]


def _hole(shell: list[list[float]]) -> list[list[float]]:
    x0, y0 = shell[0]
    x1, y1 = shell[2]
    return [[x0 + 1000.0, y0 + 1000.0], [x1 - 1000.0, y0 + 1000.0],
            [x1 - 1000.0, y1 - 1000.0], [x0 + 1000.0, y1 - 1000.0],
            [x0 + 1000.0, y0 + 1000.0]]


def _bowtie(oid: int) -> list[list[float]]:
    x0 = -13131705.0 + (oid % 7) * 9000.0
    y0 = 3984685.0 + ((oid // 7) % 900) * 7000.0
    return [[x0, y0], [x0 + 2000.0, y0], [x0, y0 + 4000.0],
            [x0 + 3000.0, y0 + 4000.0], [x0, y0]]


def _overlapping_shells(oid: int) -> list[list[float]]:
    x0 = -13131705.0 + (oid % 7) * 9000.0
    y0 = 3984685.0 + ((oid // 7) % 900) * 7000.0
    first = [[x0, y0], [x0, y0 + 5000.0], [x0 + 5000.0, y0 + 5000.0],
             [x0 + 5000.0, y0], [x0, y0]]
    second = [[x0 + 2500.0, y0 + 1000.0], [x0 + 2500.0, y0 + 6000.0],
              [x0 + 7000.0, y0 + 6000.0], [x0 + 7000.0, y0 + 1000.0],
              [x0 + 2500.0, y0 + 1000.0]]
    return [first, second]


def _feature_rings(oid: int) -> list[list[list[float]]]:
    if _GATE_FEATURES.get(oid) == "source_polygon_ring_degenerate":
        return [[_shell(oid)[0]] * 4]
    if _GATE_FEATURES.get(oid) == "source_polygon_ring_self_intersecting":
        return [_bowtie(oid)]
    if _GATE_FEATURES.get(oid) == "source_polygon_assembly_invalid":
        return _overlapping_shells(oid)
    if oid == 11:
        return [_shell(oid), _hole(_shell(oid))]
    if oid == 12:
        return [_shell(oid), _shell(oid + 100_000)]
    return [_shell(oid)]


def _attributes(oid: int) -> dict:
    attrs = {}
    for field in source.ATTRIBUTE_FIELDS:
        name, kind = field["name"], field["type"]
        if name == "objectid":
            attrs[name] = oid
        elif name == "huc10":
            attrs[name] = f"{oid % 100_000:010d}"
        elif kind == "esriFieldTypeDate":
            attrs[name] = 1_700_000_000_000 + oid
        elif kind == "esriFieldTypeDouble":
            attrs[name] = float(oid) * 10.0
        elif name == "tnmid":
            attrs[name] = "{%08X-4B8E-9E52-388A49AC0A23}" % (oid & 0xFFFFFFFF)
        elif name == "globalid":
            attrs[name] = "{%08X-E29C-11E2-8094-0021280458E6}" % (oid & 0xFFFFFFFF)
        else:
            attrs[name] = f"{name}-{oid}"
    if oid not in _GATE_FEATURES:
        decoded = from_wkb(to_wkb({"rings": _feature_rings(oid)},
                                  "esriGeometryPolygon", has_z=False, has_m=False))
        attrs["shape_Area"] = decoded.area
        attrs["shape_Length"] = decoded.length
    else:
        attrs["shape_Area"] = 12.5
        attrs["shape_Length"] = 340.0
    return attrs


def _documents() -> dict:
    service = {"currentVersion": 11.3, "mapName": source.MAP_NAME,
               "copyrightText": source.EXPECTED_COPYRIGHT_TEXT,
               "capabilities": "Map,Query,Data", "maxRecordCount": 2000,
               "units": "esriMeters", "tables": [], "spatialReference": dict(_SR),
               "fullExtent": {**source.EXPECTED_EXTENT, "spatialReference": dict(_SR)},
               "layers": [{"id": layer_id, "name": name, "parentLayerId": -1,
                           "subLayerIds": None, "type": "Feature Layer"}
                          for layer_id, name in source.EXPECTED_SERVICE_LAYERS]}
    layer = {"id": source.LAYER_ID, "name": source.LAYER_NAME, "type": "Feature Layer",
             "geometryType": "esriGeometryPolygon", "maxRecordCount": 2000,
             "copyrightText": "U.S. Geological Survey", "displayField": "NAME",
             "geometryField": {"name": "shape"},
             "geometryProperties": {"shapeAreaFieldName": "shape_Area",
                                    "shapeLengthFieldName": "shape_Length",
                                    "units": "esriMeters", "mapUnits": {"uwkid": 9001}},
             "typeIdField": None, "isDataVersioned": False, "isDataArchived": False,
             "extent": {**source.EXPECTED_EXTENT, "spatialReference": dict(_SR)},
             "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": dict(_SR), "spatialReference": None,
             "fields": json.loads(json.dumps(source.EXPECTED_LAYER_FIELDS))}
    features = {}
    for oid in _TEST_ROSTER:
        features[oid] = {"attributes": _attributes(oid), "geometry": {"rings": _feature_rings(oid)}}
    pages = {}
    for index in range(_TEST_PAGE_COUNT):
        page_ids = _TEST_ROSTER[index * _TEST_PAGE_SIZE:(index + 1) * _TEST_PAGE_SIZE]
        pages[f"page_{index:04d}"] = {
            "displayFieldName": "NAME",
            "exceededTransferLimit": index < _TEST_PAGE_COUNT - 1,
            "features": [features[oid] for oid in page_ids],
            "fieldAliases": {field["name"]: field["alias"]
                             for field in source.EXPECTED_PAGE_FIELD_DECLARATIONS},
            "fields": json.loads(json.dumps(source.EXPECTED_PAGE_FIELD_DECLARATIONS)),
            "geometryType": "esriGeometryPolygon",
            "spatialReference": dict(_SR),
        }
    return {
        "service_doc": _SERVICE_DOC,
        "service_before": service, "layer_before": layer,
        "count_before": {"count": _TEST_COUNT},
        "ids_before": {"objectIdFieldName": "objectid", "objectIds": list(_TEST_ROSTER)},
        "service_after": json.loads(json.dumps(service)),
        "layer_after": json.loads(json.dumps(layer)),
        "count_after": {"count": _TEST_COUNT},
        "ids_after": {"objectIdFieldName": "objectid", "objectIds": list(_TEST_ROSTER)},
        **pages,
    }


def _digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


_PBF_SCALE = 0.0001
_PBF_TRANSLATE = (-20037700.0, -30241100.0)


def _varint(value: int) -> bytes:
    if value < 0:
        value += 1 << 64
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _zigzag(value: int) -> int:
    return (value << 1) ^ (value >> 63) if value >= 0 else ((-value) << 1) - 1


def _field(number: int, wire: int, payload: bytes) -> bytes:
    return _varint((number << 3) | wire) + payload


def _message(number: int, payload: bytes) -> bytes:
    return _field(number, 2, _varint(len(payload)) + payload)


def _pbf_value_message(value, kind: str) -> bytes:
    if value is None:
        return _field(10, 0, _varint(1))
    if kind in {"esriFieldTypeString", "esriFieldTypeGlobalID"}:
        encoded = value.encode()
        return _field(1, 2, _varint(len(encoded)) + encoded)
    if kind == "esriFieldTypeDate":
        return _field(3, 1, struct.pack("<d", float(value)))
    if kind == "esriFieldTypeDouble":
        return _field(3, 1, struct.pack("<d", float(value)))
    return _field(5, 0, _varint(value))


def _pbf_geometry_message(rings: list[list[list[float]]]) -> bytes:
    lengths, coords = [], []
    for ring in rings:
        lengths.append(len(ring))
        previous = None
        for point in ring:
            x_int = round((point[0] - _PBF_TRANSLATE[0]) / _PBF_SCALE)
            y_int = round((_PBF_TRANSLATE[1] - point[1]) / _PBF_SCALE)
            if previous is None:
                dx, dy = x_int, y_int
            else:
                dx, dy = x_int - previous[0], y_int - previous[1]
            previous = (x_int, y_int)
            coords.append(_varint(_zigzag(dx)))
            coords.append(_varint(_zigzag(dy)))
    lengths_bytes = b"".join(_varint(length) for length in lengths)
    coords_bytes = b"".join(coords)
    payload = (_field(1, 0, _varint(3))
               + _field(2, 2, _varint(len(lengths_bytes)) + lengths_bytes)
               + _field(3, 2, _varint(len(coords_bytes)) + coords_bytes))
    return _message(2, payload)


def _pbf_field_messages() -> bytes:
    type_map = {"esriFieldTypeOID": 6, "esriFieldTypeString": 4,
                "esriFieldTypeDate": 5, "esriFieldTypeDouble": 3,
                "esriFieldTypeGlobalID": 11}
    out = []
    for field in source.EXPECTED_PAGE_FIELD_DECLARATIONS:
        name = field["name"].encode()
        alias = field["alias"].encode()
        out.append(_message(13, _field(1, 2, _varint(len(name)) + name)
                            + _field(2, 0, _varint(type_map[field["type"]]))
                            + _field(3, 2, _varint(len(alias)) + alias)))
    return b"".join(out)


def _pbf_transform_message(scale=_PBF_SCALE, translate=_PBF_TRANSLATE) -> bytes:
    scales = b"".join(_field(number, 1, struct.pack("<d", value))
                      for number, value in enumerate((scale, scale, scale, scale), start=1))
    translates = b"".join(_field(number, 1, struct.pack("<d", value))
                          for number, value in enumerate(
                              (translate[0], translate[1], -100000.0, -100000.0), start=1))
    return _message(12, _message(2, scales) + _message(3, translates))


def _encode_pbf_window(ids: list[int], *, exceeded: bool | None = None,
                       rings_by_oid: dict[int, list] | None = None,
                       attributes_by_oid: dict[int, dict] | None = None,
                       transform: bytes | None = None,
                       field_messages: bytes | None = None,
                       geometry_type: int = 3,
                       geometry_type_declaration: bool = True,
                       spatial_reference_declaration: bool = True,
                       feature_message_hook=None,
                       result_hook=None,
                       value_message_hook=None) -> bytes:
    """Encode one protobuf FeatureCollectionPBuffer window in the producer's
    observed wire shape (upperLeft default quantization, per-ring deltas)."""
    rings_by_oid = rings_by_oid or {}
    attributes_by_oid = attributes_by_oid or {}
    feature_payloads = []
    for oid in ids:
        attrs = attributes_by_oid.get(oid) or _attributes(oid)

        def value_bytes(field, value, oid=oid):
            if value_message_hook is not None:
                return value_message_hook(oid, field, value)
            return _message(1, _pbf_value_message(value, field["type"]))

        values = b"".join(value_bytes(field, attrs[field["name"]])
                          for field in source.EXPECTED_PAGE_FIELD_DECLARATIONS)
        rings = rings_by_oid.get(oid, _feature_rings(oid))
        payload = values + _pbf_geometry_message(rings)
        if feature_message_hook is not None:
            payload = feature_message_hook(oid, payload)
        feature_payloads.append(_message(15, payload))
    result = (_field(1, 2, _varint(8) + b"objectid")
              + _field(3, 2, _varint(8) + b"globalid")
              + (_field(7, 0, _varint(geometry_type))
                 if geometry_type_declaration else b"")
              + (_message(8, _field(1, 0, _varint(102100)) + _field(2, 0, _varint(3857)))
                 if spatial_reference_declaration else b"")
              + (_field(9, 0, _varint(1)) if exceeded else b"")
              + (transform or _pbf_transform_message())
              + (field_messages or _pbf_field_messages())
              + b"".join(feature_payloads))
    if result_hook is not None:
        result = result_hook(result)
    return _message(2, _message(1, result))


def _encode(document) -> bytes:
    if isinstance(document, bytes):
        return document
    return json.dumps(document, ensure_ascii=False, allow_nan=False).encode()


def _write_capture_root(root: Path, documents: dict) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    roster = documents["ids_before"]["objectIds"]
    expected_urls = dict(source._capture_roster(roster))
    for name, document in documents.items():
        body = _encode(document)
        wire = b"wire:" + body[:32]
        receipt = {"method": "GET", "url": expected_urls[name], "final_url": expected_urls[name],
                   "observed_at": "2026-11-14T12:00:00.000000Z", "status": 200,
                   "complete_body": True, "bytes": len(body), "sha256": _digest(body),
                   "wire_bytes": len(wire), "wire_sha256": _digest(wire),
                   "content_encoding": None, "content_length": None,
                   "error_code": None, "error_type": None}
        (root / f"{name}.body").write_bytes(body)
        (root / f"{name}.wire").write_bytes(wire)
        (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    return root


class Wbd10DigitHuTests(unittest.TestCase):
    def _capture_root(self, documents: dict) -> Path:
        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-capture-")
        self.addCleanup(temporary.cleanup)
        with relaxed_pins():
            return _write_capture_root(Path(temporary.name) / "captures", documents)

    def test_pinned_source_constants_match_verified_evidence(self):
        self.assertEqual(source.DATASET_SLUG, "wbd-10-digit-hu-watershed")
        self.assertEqual(source.SERVICE_URL,
                         "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer")
        self.assertEqual(source.LAYER_URL, source.SERVICE_URL + "/5")
        self.assertEqual(source.LAYER_NAME, "10-digit HU (Watershed)")
        self.assertEqual(source.MAP_NAME, "wbd")
        self.assertEqual(source.EXPECTED_COUNT, 18_911)
        self.assertEqual(source.PAGE_SIZE, 100)
        self.assertEqual(source.DECLARED_MAX_RECORD_COUNT, 2000)
        self.assertEqual((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE,
                         source.PAGE_COUNT)
        self.assertEqual(source.SERVICE_DOC_URL,
                         "https://www.usgs.gov/national-hydrography/access-national-hydrography-products")
        self.assertEqual(source.EXPECTED_COPYRIGHT_TEXT,
                         "USGS WBD - Watershed Boundary Dataset. Data refreshed October, 2026.")
        self.assertEqual(source.EXPECTED_EXTENT["xmin"], -19951755.682)
        self.assertEqual(source.EXPECTED_EXTENT["xmax"], 20021551.7474)
        self.assertEqual([entry for entry in source.EXPECTED_SERVICE_LAYERS if entry[0] == 5],
                         [(5, "10-digit HU (Watershed)")])
        self.assertEqual(source.NATIVE_WKID, 102100)
        self.assertEqual(source.SOURCE_CRS_WKID, 3857)
        self.assertEqual([field["name"] for field in source.EXPECTED_PAGE_FIELD_DECLARATIONS],
                         ["objectid", "tnmid", "metasourceid", "sourcedatadesc",
                          "sourceoriginator", "sourcefeatureid", "loaddate",
                          "referencegnis_ids", "areaacres", "areasqkm", "states", "huc10",
                          "name", "hutype", "humod", "globalid", "shape_Length", "shape_Area"])
        self.assertEqual(len(source.EXPECTED_LAYER_FIELDS), 19)
        self.assertEqual(source.EXPECTED_LAYER_FIELDS[15]["name"], "shape")
        self.assertEqual(source.EXPECTED_LAYER_FIELDS[15]["type"], "esriFieldTypeGeometry")
        self.assertEqual(source.PUBLISHER,
                         "U.S. Geological Survey, National Geospatial Technical Operations Center")

    def test_source_manifest_descriptions_identify_this_inventory(self):
        descriptions = source._source_contract_descriptions()
        served_attributes = sum(1 for field in source.EXPECTED_LAYER_FIELDS
                                if field["type"] != "esriFieldTypeGeometry")
        self.assertEqual(len(source.ATTRIBUTE_FIELDS), served_attributes)
        self.assertIn(f"all {served_attributes} served attributes",
                      descriptions["source_fields"])
        self.assertNotIn("21 served attributes", descriptions["source_fields"])
        self.assertIn("inventory entry wbd/10-digit-hu-watershed", descriptions["lineage"])
        self.assertNotIn("subwatershed", descriptions["lineage"])
        self.assertIn("OBJECTID/huc10/TNMID", descriptions["prebuilt_reason"])
        self.assertNotIn("HUC12", descriptions["prebuilt_reason"])
        self.assertEqual([field["name"] for field in source.EXPECTED_PAGE_FIELD_DECLARATIONS
                          if field["name"].startswith("huc")], ["huc10"])

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(wbd_10_digit_hu, hifld_ingest_assets)
        self.assertEqual(wbd_10_digit_hu_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = wbd_10_digit_hu_complete({"feature_count": source.EXPECTED_COUNT,
                                          "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = wbd_10_digit_hu_complete({"feature_count": source.EXPECTED_COUNT - 1,
                                           "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_cover_roster_exactly_once(self):
        with relaxed_pins():
            urls = dict(source._capture_roster(list(_TEST_ROSTER)))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         ["page_0000", "page_0001", "page_0002"])
        self.assertIn("objectid%3E0", urls["page_0000"])
        self.assertIn("objectid%3E12", urls["page_0001"])
        self.assertIn("returnZ=true", urls["page_0000"])
        self.assertIn("returnM=true", urls["page_0000"])
        self.assertIn("orderByFields=objectid+ASC", urls["page_0000"])
        self.assertIn("resultRecordCount=100", source._page_url(0))
        self.assertEqual(urls["ids_before"], source._ids_url())
        self.assertEqual(urls["service_doc"], source.SERVICE_DOC_URL)

    def test_complete_streaming_population_and_baselines_are_validated(self):
        root = self._capture_root(_documents())
        with relaxed_pins():
            audit, derived = source._validate_stream(root, parquet_sink=None)
        self.assertEqual(audit["source_feature_count"], len(_TEST_ROSTER))
        self.assertEqual(audit["source_id_min"], min(_TEST_ROSTER))
        self.assertEqual(audit["source_id_max"], max(_TEST_ROSTER))
        self.assertEqual(audit["source_id_sha256"], _digest(_canonical(list(_TEST_ROSTER))))
        self.assertEqual(audit["null_geometry_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], len(_TEST_ROSTER))
        self.assertEqual(audit["unusable_geometry_counts"], {})
        self.assertEqual(sorted(audit["ogc_invalid_geometry_sample_objectids"]),
                         sorted(_GATE_FEATURES))
        self.assertEqual(audit["source_crs"], "EPSG:3857")
        self.assertLess(audit["geometry_bounds"][0], -13_100_000)
        self.assertEqual(sorted(derived["geometry_types"]), ["MultiPolygon", "Polygon"])

    def test_streaming_digests_match_the_pinned_scheme(self):
        root = self._capture_root(_documents())
        with relaxed_pins():
            audit, _derived = source._validate_stream(root, parquet_sink=None)
        expected_record = hashlib.sha256()
        expected_geometry = hashlib.sha256()
        for oid in _TEST_ROSTER:
            expected_record.update(_canonical(_attributes(oid)) + source.DIGEST_SEPARATOR)
            # The geometry baseline digests the structurally validated served
            # rings of every row, including rows that gate to null geometry.
            expected_geometry.update(
                _canonical([oid, _feature_rings(oid)]) + source.DIGEST_SEPARATOR)
        self.assertEqual(audit["source_record_sha256"], expected_record.hexdigest())
        self.assertEqual(audit["source_geometry_sha256"], expected_geometry.hexdigest())

    def test_pinned_baselines_and_gate_counts_are_enforced_once_pinned(self):
        documents = _documents()
        root = self._capture_root(documents)
        with relaxed_pins():
            audit, _derived = source._validate_stream(root, parquet_sink=None)
        gate_counts = audit["unusable_geometry_counts"]
        roster_digest = _digest(_canonical(list(_TEST_ROSTER)))
        with relaxed_pins(EXPECTED_ID_SHA256=roster_digest,
                          EXPECTED_RECORD_SHA256=audit["source_record_sha256"],
                          EXPECTED_GEOMETRY_SHA256=audit["source_geometry_sha256"],
                          EXPECTED_NULL_GEOMETRY_COUNT=audit["null_geometry_count"],
                          EXPECTED_UNUSABLE_GEOMETRY_COUNTS=gate_counts):
            source._validate_stream(root, parquet_sink=None)
            with self.assertRaisesRegex(ValueError, "source_id_roster_baseline_drift"):
                with relaxed_pins(EXPECTED_ID_SHA256=_digest(_canonical(
                        list(reversed(_TEST_ROSTER))))):
                    source._validate_stream(root, parquet_sink=None)
            mutated = json.loads(json.dumps(documents))
            mutated["page_0000"]["features"][0]["attributes"]["name"] = "Renamed HU"
            with self.assertRaisesRegex(ValueError, "source_record_baseline_drift"):
                source._validate_stream(self._capture_root(mutated), parquet_sink=None)
            mutated = json.loads(json.dumps(documents))
            ring = mutated["page_0000"]["features"][0]["geometry"]["rings"][0]
            ring.insert(2, [ring[1][0] + (ring[2][0] - ring[1][0]) / 2, ring[1][1]])
            with self.assertRaisesRegex(ValueError, "source_geometry_baseline_drift"):
                source._validate_stream(self._capture_root(mutated), parquet_sink=None)
            # A served-ring mutation inside a row that still gates to null
            # geometry (gate mode and gate counts unchanged, record values
            # unchanged) must also be detected: the geometry baseline digests
            # every row's served rings before the gate decision.
            mutated = json.loads(json.dumps(documents))
            gated = next(feature for feature in mutated["page_0001"]["features"]
                         if feature["attributes"]["objectid"] == 501)
            gated["geometry"]["rings"][0][1] = [
                gated["geometry"]["rings"][0][1][0] + 25.0, gated["geometry"]["rings"][0][1][1]]
            with self.assertRaisesRegex(ValueError, "source_geometry_baseline_drift"):
                source._validate_stream(self._capture_root(mutated), parquet_sink=None)
            with self.assertRaisesRegex(ValueError, "source_null_geometry_count_drift"):
                with relaxed_pins(EXPECTED_NULL_GEOMETRY_COUNT=1,
                                  EXPECTED_UNUSABLE_GEOMETRY_COUNTS=gate_counts):
                    source._validate_stream(root, parquet_sink=None)

    def test_wkb_preserves_ring_structure_and_declared_types(self):
        def converted(rings):
            return to_wkb({"rings": rings}, "esriGeometryPolygon", has_z=False, has_m=False)

        single = converted(_feature_rings(11)[:1])
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 3))
        with_hole = converted([_shell(2), _hole(_shell(2))])
        self.assertEqual(struct.unpack("<BI", with_hole[:5]), (1, 3))
        self.assertEqual(len(from_wkb(with_hole).interiors), 1)
        multi = converted([_shell(3), _shell(4)])
        self.assertEqual(struct.unpack("<BI", multi[:5]), (1, 6))
        self.assertEqual(len(from_wkb(multi).geoms), 2)
        # GDAL assembles a lone CCW producer ring into a valid polygon and
        # retains zero-area, self-intersecting and overlapping-ring defects
        # verbatim; none is refused or repaired (issue #997 shared converter).
        self.assertTrue(from_wkb(converted([_hole(_shell(1))])).is_valid)
        for rings in ([[_shell(1)[0]] * 4], [_bowtie(1)], _overlapping_shells(1)):
            self.assertFalse(from_wkb(converted(rings)).is_valid)

    def test_replay_rejects_roster_drift_and_honors_retained_receipts(self):
        documents = _documents()
        roster = list(_TEST_ROSTER)
        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-replay-")
        self.addCleanup(temporary.cleanup)
        with relaxed_pins():
            root = _write_capture_root(Path(temporary.name) / "captures", documents)
        manifest = {"complete": True, "captured_at": "2026-11-14T12:00:00.000000Z",
                    "receipts": {}, "artifact_roster": {}}
        for name in sorted(documents):
            body = (root / f"{name}.body").read_bytes()
            wire = (root / f"{name}.wire").read_bytes()
            receipt = json.loads((root / f"{name}.receipt.json").read_bytes())
            receipt_body = (root / f"{name}.receipt.json").read_bytes()
            manifest["receipts"][name] = receipt
            manifest["artifact_roster"][f"{name}.body"] = {
                "bytes": len(body), "sha256": _digest(body)}
            manifest["artifact_roster"][f"{name}.wire"] = {
                "bytes": len(wire), "sha256": _digest(wire)}
            manifest["artifact_roster"][f"{name}.receipt.json"] = {
                "bytes": len(receipt_body), "sha256": _digest(receipt_body)}
        (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        with relaxed_pins(EXPECTED_ID_SHA256=_digest(_canonical(roster)),
                          DISK_FLOOR_BYTES=1):
            captures, replay_parts = source._read_replay(root)
            self.assertEqual(set(captures), set(documents))
            self.assertEqual(replay_parts, {})
            audit, _derived = source._validate_stream(root, parquet_sink=None)
            self.assertEqual(audit["source_id_sha256"], _digest(_canonical(roster)))
        mutated = json.loads(json.dumps(documents))
        mutated["ids_after"]["objectIds"] = [11, 12, 999, 100_003, 200_004]
        with relaxed_pins():
            with self.assertRaisesRegex(ValueError, "source_id_roster_changed_during_capture"):
                source._validate_stream(self._capture_root(mutated), parquet_sink=None)

    def test_population_schema_transfer_crs_alias_and_parity_drift_fail_closed(self):
        documents = _documents()
        cases = [
            ("page error", lambda d: d["page_0000"].update(error={"code": 500}),
             "source_page_error"),
            ("page extra member", lambda d: d["page_0000"].update(extraKey=1),
             "source_page_unexpected_member"),
            ("page display field", lambda d: d["page_0000"].update(displayFieldName="NAME2"),
             "source_page_identity_drift"),
            ("transfer flag", lambda d: d["page_0000"].update(exceededTransferLimit="yes"),
             "source_page_transfer_flag_drift"),
            ("page population", lambda d: d["page_0000"]["features"].pop(),
             "source_page_population_incomplete"),
            ("transfer limit on final page", lambda d: d["page_0002"].update(exceededTransferLimit=True),
             "source_page_transfer_limit_on_final_page"),
            ("page crs", lambda d: d["page_0000"].update(spatialReference={"wkid": 4326}),
             "source_page_0000_page_crs_drift"),
            ("page alias", lambda d: d["page_0000"].update(
                fieldAliases={**d["page_0000"]["fieldAliases"], "huc10": "HUC"}),
             "source_page_alias_drift"),
            ("page field drift", lambda d: d["page_0000"]["fields"][1].update(name="renamed"),
             "source_page_physical_schema_changed"),
            ("page geometry type", lambda d: d["page_0000"].update(geometryType="esriGeometryPoint"),
             "source_page_identity_drift"),
            ("attribute order", lambda d: d["page_0000"]["features"][0]["attributes"].pop("name"),
             "source_attribute_roster_or_order_drift"),
            ("extra attribute", lambda d: d["page_0000"]["features"][0]["attributes"].update(EXTRA="x"),
             "source_attribute_roster_or_order_drift"),
            ("oid value", lambda d: d["page_0000"]["features"][0]["attributes"].update(objectid=0),
             "source_oid_value_type_drift"),
            ("oid roster", lambda d: d["page_0000"]["features"][0]["attributes"].update(objectid=999),
             "source_feature_id_roster_drift"),
            ("id order", lambda d: d["page_0000"]["features"].reverse(),
             "source_feature_ids_not_strictly_increasing"),
            ("string value", lambda d: d["page_0000"]["features"][0]["attributes"].update(name=5),
             "source_string_value_type_drift:name"),
            ("date value", lambda d: d["page_0000"]["features"][0]["attributes"].update(loaddate="now"),
             "source_date_value_type_drift:loaddate"),
            ("double value", lambda d: d["page_0000"]["features"][0]["attributes"].update(areaacres="x"),
             "source_double_value_type_drift:areaacres"),
            ("area parity", lambda d: d["page_0000"]["features"][0]["attributes"].update(shape_Area=9e9),
             "source_shape_Area_parity_drift"),
            ("length parity", lambda d: d["page_0000"]["features"][0]["attributes"].update(shape_Length=1.0),
             "source_shape_Length_parity_drift"),
            ("outside coverage", lambda d: d["page_0000"]["features"][1]["geometry"]["rings"][1].__setitem__(
                2, [22_000_000.0, 3_984_685.0]), "source_geometry_outside_declared_coverage"),
            ("geometry extra key", lambda d: d["page_0000"]["features"][0]["geometry"].update(z=1),
             "source_polygon_geometry_structure_drift"),
            ("geometry z flag", lambda d: d["page_0000"]["features"][0]["geometry"].update(hasZ=True),
             "source_geometry_dimensional_drift"),
            ("open ring", lambda d: d["page_0000"]["features"][0]["geometry"]["rings"][0].pop(),
             "source_polygon_ring_not_closed"),
            ("short ring", lambda d: d["page_0000"]["features"][0]["geometry"]["rings"].__setitem__(
                0, [[0.0, 0.0], [1.0, 1.0]]), "source_polygon_ring_invalid"),
            ("schema", lambda d: d["layer_before"]["fields"][1].update(name="renamed"),
             "source_field_schema_drift"),
            ("layer identity", lambda d: d["layer_before"].update(name="Other"),
             "source_layer_identity_or_extent_drift"),
            ("layer extent", lambda d: d["layer_before"]["extent"].update(xmax=0.0),
             "source_layer_identity_or_extent_drift"),
            ("layer type field", lambda d: d["layer_before"].update(typeIdField="hutype"),
             "source_layer_identity_or_extent_drift"),
            ("layer geometry props", lambda d: d["layer_before"]["geometryProperties"].update(units="esriFeet"),
             "source_layer_identity_or_extent_drift"),
            ("layer crs present", lambda d: d["layer_before"].update(spatialReference={"wkid": 4326}),
             "source_layer_crs_drift"),
            ("layer versioned", lambda d: d["layer_before"].update(isDataVersioned=True),
             "source_layer_identity_or_extent_drift"),
            ("count before", lambda d: d["count_before"].update(count=_TEST_COUNT - 1),
             "source_count_population_drift:before"),
            ("ids before", lambda d: d["ids_before"].update(objectIds=_TEST_ROSTER[:-1]),
             "source_id_population_drift:before"),
            ("ids changed mid capture", lambda d: d["ids_after"].update(objectIds=[11, 12, 999, 100_003, 200_004]),
             "source_id_roster_changed_during_capture"),
            ("service identity", lambda d: d["service_before"].update(copyrightText="changed"),
             "service_before_service_identity_drift"),
            ("service extent", lambda d: d["service_before"]["fullExtent"].update(xmin=0.0),
             "service_before_service_extent_drift"),
            ("service roster", lambda d: d["service_before"]["layers"].pop(),
             "service_before_service_layer_roster_drift"),
            ("service changed during capture", lambda d: d["service_after"].update(mapName="other"),
             "source_service_or_layer_changed_during_capture"),
            ("service doc drift", lambda d: d.update(service_doc="<html>unrelated</html>"),
             "usgs_service_documentation_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(documents))
                mutate(docs)
                with relaxed_pins():
                    with self.assertRaisesRegex(ValueError, expected):
                        source._validate_stream(self._capture_root(docs), parquet_sink=None)

    def test_native_parquet_round_trips_native_schema_and_wkb(self):
        documents = _documents()
        root = self._capture_root(documents)
        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-parquet-")
        self.addCleanup(temporary.cleanup)
        sink = Path(temporary.name) / "wbd-10-digit-hu.parquet"
        with relaxed_pins():
            audit, derived = source._validate_stream(root, parquet_sink=sink)
        self.assertTrue(sink.exists())
        table = pq.read_table(sink)
        self.assertEqual(table.num_rows, len(_TEST_ROSTER))
        self.assertEqual(table.schema.metadata[b"geo"], _canonical({
            "version": "1.1.0", "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB",
                                     "geometry_types": ["Polygon", "MultiPolygon"],
                                     "crs": source._EPSG3857_PROJJSON, "edges": "planar"}}}))
        self.assertEqual(table.column("objectid").to_pylist(), _TEST_ROSTER)
        for position, oid in enumerate(_TEST_ROSTER):
            expected = _attributes(oid)
            for field in source.ATTRIBUTE_FIELDS:
                name = field["name"]
                self.assertEqual(table.column(name)[position].as_py(), expected[name])
            wkb = table.column("geometry")[position].as_py()
            decoded = from_wkb(wkb)
            reference = from_wkb(to_wkb({"rings": _feature_rings(oid)},
                                  "esriGeometryPolygon", has_z=False, has_m=False))
            self.assertEqual(decoded.area, reference.area)
            self.assertEqual(decoded.length, reference.length)
        self.assertEqual(derived["parquet_path"], sink)

    def test_row_groups_are_budgeted_by_served_geometry_bytes(self):
        documents = _documents()
        root = self._capture_root(documents)
        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-budget-")
        self.addCleanup(temporary.cleanup)
        sink = Path(temporary.name) / "wbd-10-digit-hu.parquet"
        with relaxed_pins(ROW_GROUP_GEOMETRY_BUDGET_BYTES=1):
            audit, derived = source._validate_stream(root, parquet_sink=sink)
        self.assertTrue(sink.exists())
        parquet = pq.ParquetFile(sink)
        # With a one-byte budget every served (non-null) geometry forms its
        # own row group; consecutive null-geometry rows carry no bytes and may
        # share one. The population and digests are unchanged by the
        # row-group structure.
        non_null_groups = 0
        for group in range(parquet.metadata.num_row_groups):
            column = parquet.read_row_group(group, columns=["geometry"]).column("geometry")
            non_null = len(column) - column.null_count
            self.assertLessEqual(non_null, 1)
            if non_null:
                non_null_groups += 1
        self.assertEqual(non_null_groups, len(_TEST_ROSTER))
        self.assertEqual(parquet.metadata.num_rows, len(_TEST_ROSTER))
        table = pq.read_table(sink)
        self.assertEqual(table.column("objectid").to_pylist(), _TEST_ROSTER)

    def test_refusing_window_bisects_and_replays_with_retained_refusal_artifacts(self):
        documents = _documents()
        roster = list(_TEST_ROSTER)
        refusal = {"error": {"code": 500, "message": "Error performing query operation",
                             "details": []}}

        def part_page(ids: list[int], exceeded: bool) -> dict:
            return {"displayFieldName": "NAME", "exceededTransferLimit": exceeded,
                    "features": [{"attributes": _attributes(oid),
                                  "geometry": {"rings": _feature_rings(oid)}} for oid in ids],
                    "fieldAliases": {field["name"]: field["alias"]
                                     for field in source.EXPECTED_PAGE_FIELD_DECLARATIONS},
                    "fields": json.loads(json.dumps(source.EXPECTED_PAGE_FIELD_DECLARATIONS)),
                    "geometryType": "esriGeometryPolygon", "spatialReference": dict(_SR)}

        # Page 0001 covers roster[2:4] = [501, 100_003]; with the bisection
        # floor relaxed to 1 the refusing 2-id window splits into 1-id leaves.
        part_a, part_b = part_page([501], True), part_page([100_003], False)
        with relaxed_pins():
            roster_urls = dict(source._capture_roster(roster))
            nominal_url = roster_urls["page_0001"]
            url_a = source._window_url(roster, 1, "a")
            url_b = source._window_url(roster, 1, "b")
        self.assertIn("resultRecordCount=1", url_a)
        self.assertIn("resultRecordCount=1", url_b)
        self.assertNotEqual(url_a, url_b)

        def scripted_capture(script: dict):
            calls: list[str] = []

            def fake_capture(_client, url, root, name, max_bytes=0):
                calls.append(name)
                body = _encode(script[name])
                wire = b"wire:" + body[:32]
                receipt = {"method": "GET", "url": url, "final_url": url,
                           "observed_at": "2026-11-14T12:00:00.000000Z", "status": 200,
                           "complete_body": True, "bytes": len(body), "sha256": _digest(body),
                           "wire_bytes": len(wire), "wire_sha256": _digest(wire),
                           "content_encoding": None, "content_length": None,
                           "error_code": None, "error_type": None}
                (root / f"{name}.body").write_bytes(body)
                (root / f"{name}.wire").write_bytes(wire)
                (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
                return body, receipt

            return calls, fake_capture

        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-bisect-")
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / "captures"
        root.mkdir(parents=True)
        with relaxed_pins():
            calls, fake_capture = scripted_capture({
                "page_0001": refusal, "page_0001a": part_a, "page_0001b": part_b})
            with patches(source, _capture=fake_capture, MIN_WINDOW_IDS=1):
                parts, refusals = source._capture_page_window(None, roster, 1, root)
        self.assertEqual(calls, ["page_0001", "page_0001a", "page_0001b"])
        self.assertEqual(set(parts), {"page_0001_refusal", "page_0001a", "page_0001b"})
        self.assertEqual(refusals, ["page_0001_refusal"])
        self.assertTrue((root / "page_0001_refusal.body").exists())
        self.assertFalse((root / "page_0001.body").exists())
        with relaxed_pins():
            with self.assertRaisesRegex(ValueError, "source_page_window_refused_after_bisection"):
                calls, fake_capture = scripted_capture({
                    "page_0001": refusal, "page_0001a": refusal, "page_0001a_pbf": refusal,
                    "page_0001b": part_b})
                with patches(source, _capture=fake_capture, MIN_WINDOW_IDS=1):
                    (root / "refused").mkdir()
                    source._capture_page_window(None, roster, 1, root / "refused")
        self.assertTrue((root / "refused" / "page_0001a_pbf_refusal.body").exists())

        # A bisected capture replays: the nominal slot holds no artifact, the
        # refusal evidence is bound to the nominal GET URL and the two half
        # parts reassemble the exact same validated population.
        replay_documents = {**{name: document for name, document in documents.items()
                               if name != "page_0001"},
                            "page_0001_refusal": refusal, "page_0001a": part_a,
                            "page_0001b": part_b}
        slot_urls = {}
        with relaxed_pins():
            roster_urls = dict(source._capture_roster(roster))
        for name in (*source.METADATA_NAMES, "page_0000", "page_0002"):
            slot_urls[name] = roster_urls[name]
        slot_urls["page_0001_refusal"] = roster_urls["page_0001"]
        slot_urls["page_0001a"], slot_urls["page_0001b"] = url_a, url_b
        replay_root = self._capture_root_with_urls(replay_documents, slot_urls)
        manifest = {"complete": True, "captured_at": "2026-11-14T12:00:00.000000Z",
                    "receipts": {name: json.loads(
                        (replay_root / f"{name}.receipt.json").read_bytes())
                        for name in sorted(replay_documents)},
                    "artifact_roster": source.capture_roster_digests(
                        replay_root, exclude={"capture_manifest.json"}),
                    "page_parts": {"page_0001": ["page_0001a", "page_0001b"]},
                    "page_refusals": ["page_0001_refusal"]}
        (replay_root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        with relaxed_pins(EXPECTED_ID_SHA256=_digest(_canonical(roster)), DISK_FLOOR_BYTES=1):
            captures, replay_parts = source._read_replay(replay_root)
            self.assertEqual(set(captures), set(replay_documents))
            self.assertEqual(replay_parts, {"page_0001": ["page_0001a", "page_0001b"]})
            audit, _derived = source._validate_stream(replay_root, parquet_sink=None,
                                                      page_parts=replay_parts)
        reference_root = self._capture_root(documents)
        with relaxed_pins():
            reference_audit, _unused = source._validate_stream(reference_root,
                                                               parquet_sink=None)
        for key in ("source_record_sha256", "source_geometry_sha256",
                    "source_id_sha256", "source_vertex_count"):
            self.assertEqual(audit[key], reference_audit[key])
        self.assertEqual(audit["page_parts"], {"page_0001": ["page_0001a", "page_0001b"]})

        incomplete = json.loads(json.dumps(manifest))
        incomplete["receipts"].pop("page_0001_refusal")
        with relaxed_pins():
            with self.assertRaisesRegex(ValueError, "retained_capture_roster_or_names_invalid"):
                source._read_replay(self._root_with_manifest(replay_root, incomplete))

    def test_floor_refused_window_is_rescued_by_protobuf_serializer(self):
        documents = _documents()
        roster = list(_TEST_ROSTER)
        refusal = {"error": {"code": 500, "message": "Error performing query operation",
                             "details": []}}

        def part_page(ids: list[int], exceeded: bool) -> dict:
            return {"displayFieldName": "NAME", "exceededTransferLimit": exceeded,
                    "features": [{"attributes": _attributes(oid),
                                  "geometry": {"rings": _feature_rings(oid)}} for oid in ids],
                    "fieldAliases": {field["name"]: field["alias"]
                                     for field in source.EXPECTED_PAGE_FIELD_DECLARATIONS},
                    "fields": json.loads(json.dumps(source.EXPECTED_PAGE_FIELD_DECLARATIONS)),
                    "geometryType": "esriGeometryPolygon", "spatialReference": dict(_SR)}

        # Page 0001 covers roster[2:4] = [501, 100_003]. Its first leaf [501]
        # refuses the JSON serializer at the bisection floor and is rescued by
        # the producer's protobuf serializer for the same window.
        part_b = part_page([100_003], False)
        pbf_leaf = _encode_pbf_window([501], exceeded=True)
        with relaxed_pins():
            roster_urls = dict(source._capture_roster(roster))
            url_a = source._window_url(roster, 1, "a")
            pbf_url_a = source._window_url(roster, 1, "a", fmt="pbf")
            url_b = source._window_url(roster, 1, "b")
        self.assertIn("resultRecordCount=1", pbf_url_a)
        self.assertIn("f=pbf", pbf_url_a)
        self.assertNotEqual(url_a, pbf_url_a)

        def scripted_capture(script: dict):
            calls: list[str] = []

            def fake_capture(_client, url, root, name, max_bytes=0):
                calls.append(name)
                body = _encode(script[name])
                wire = b"wire:" + body[:32]
                receipt = {"method": "GET", "url": url, "final_url": url,
                           "observed_at": "2026-11-14T12:00:00.000000Z", "status": 200,
                           "complete_body": True, "bytes": len(body), "sha256": _digest(body),
                           "wire_bytes": len(wire), "wire_sha256": _digest(wire),
                           "content_encoding": None, "content_length": None,
                           "error_code": None, "error_type": None}
                (root / f"{name}.body").write_bytes(body)
                (root / f"{name}.wire").write_bytes(wire)
                (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
                return body, receipt

            return calls, fake_capture

        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-pbf-")
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / "captures"
        root.mkdir(parents=True)
        with relaxed_pins():
            calls, fake_capture = scripted_capture({
                "page_0001": refusal, "page_0001a": refusal, "page_0001a_pbf": pbf_leaf,
                "page_0001b": part_b})
            with patches(source, _capture=fake_capture, MIN_WINDOW_IDS=1):
                parts, refusals = source._capture_page_window(None, roster, 1, root)
        self.assertEqual(calls, ["page_0001", "page_0001a", "page_0001a_pbf", "page_0001b"])
        self.assertEqual(set(parts), {"page_0001_refusal", "page_0001a_refusal",
                                      "page_0001a_pbf", "page_0001b"})
        self.assertEqual(refusals, ["page_0001_refusal", "page_0001a_refusal"])
        self.assertTrue((root / "page_0001a_refusal.body").exists())
        self.assertTrue((root / "page_0001a_pbf.body").exists())
        self.assertFalse((root / "page_0001a.body").exists())

        # The rescued capture replays and validates to the exact same
        # population digests as the all-JSON reference capture: the protobuf
        # rescue quantizes onto the pinned grid without changing any served
        # value for this population.
        replay_documents = {**{name: document for name, document in documents.items()
                               if name != "page_0001"},
                            "page_0001_refusal": refusal, "page_0001a_refusal": refusal,
                            "page_0001a_pbf": pbf_leaf, "page_0001b": part_b}
        slot_urls = {}
        for name in (*source.METADATA_NAMES, "page_0000", "page_0002"):
            slot_urls[name] = roster_urls[name]
        slot_urls["page_0001_refusal"] = roster_urls["page_0001"]
        slot_urls["page_0001a_refusal"] = url_a
        slot_urls["page_0001a_pbf"] = pbf_url_a
        slot_urls["page_0001b"] = url_b
        replay_root = self._capture_root_with_urls(replay_documents, slot_urls)
        manifest = {"complete": True, "captured_at": "2026-11-14T12:00:00.000000Z",
                    "receipts": {name: json.loads(
                        (replay_root / f"{name}.receipt.json").read_bytes())
                        for name in sorted(replay_documents)},
                    "artifact_roster": source.capture_roster_digests(
                        replay_root, exclude={"capture_manifest.json"}),
                    "page_parts": {"page_0001": ["page_0001a_pbf", "page_0001b"]},
                    "page_refusals": ["page_0001_refusal", "page_0001a_refusal"]}
        (replay_root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        with relaxed_pins(EXPECTED_ID_SHA256=_digest(_canonical(roster)), DISK_FLOOR_BYTES=1):
            captures, replay_parts = source._read_replay(replay_root)
            self.assertEqual(set(captures), set(replay_documents))
            self.assertEqual(replay_parts, {"page_0001": ["page_0001a_pbf", "page_0001b"]})
            audit, _derived = source._validate_stream(replay_root, parquet_sink=None,
                                                      page_parts=replay_parts)
        reference_root = self._capture_root(documents)
        with relaxed_pins():
            reference_audit, _unused = source._validate_stream(reference_root,
                                                               parquet_sink=None)
        for key in ("source_record_sha256", "source_geometry_sha256",
                    "source_id_sha256", "source_vertex_count",
                    "null_geometry_count", "unusable_geometry_counts"):
            self.assertEqual(audit[key], reference_audit[key], key)
        self.assertEqual(audit["page_parts"],
                         {"page_0001": ["page_0001a_pbf", "page_0001b"]})

    def test_protobuf_window_identity_and_schema_are_validated(self):
        ids = [501, 100_003]
        with relaxed_pins():
            valid = _encode_pbf_window(ids, exceeded=True)
            self.assertTrue(source._is_pbf_feature_result(valid))
            self.assertTrue(source._is_pbf_refusal(b'{"error": {"code": 500}}'))
            cases = {
                "transform drift":
                    ("source_pbf_transform_drift",
                     _encode_pbf_window(ids, transform=_pbf_transform_message(scale=0.001)), ids),
                "wrong geometry type enum":
                    ("source_pbf_page_identity_drift",
                     _encode_pbf_window(ids, geometry_type=5), ids),
                "field schema drift":
                    ("source_pbf_field_schema_drift",
                     _encode_pbf_window(ids, field_messages=_pbf_field_messages()
                                        .replace(b"\x10\x06", b"\x10\x04", 1)), ids),
                "truncated page":
                    ("source_pbf_page_structure_drift", valid[:-3], ids),
                "unknown result field":
                    ("source_pbf_page_identity_drift",
                     _encode_pbf_window(
                         ids, result_hook=lambda result: result + b"\x50\x01"), ids),
                "missing geometry type declaration":
                    ("source_pbf_page_identity_drift",
                     _encode_pbf_window(ids, geometry_type_declaration=False), ids),
                "missing spatial reference declaration":
                    ("source_pbf_page_identity_drift",
                     _encode_pbf_window(ids, spatial_reference_declaration=False), ids),
                "duplicated geometry type declaration":
                    ("source_pbf_page_identity_drift",
                     _encode_pbf_window(
                         ids, result_hook=lambda result:
                         result + _field(7, 0, _varint(3))), ids),
                "duplicated spatial reference declaration":
                    ("source_pbf_page_identity_drift",
                     _encode_pbf_window(
                         ids, result_hook=lambda result:
                         result + _message(8, _field(1, 0, _varint(102100))
                                           + _field(2, 0, _varint(3857)))), ids),
                "date value type drift":
                    ("source_date_value_type_drift",
                     _encode_pbf_window([501], attributes_by_oid={
                         501: {**_attributes(501), "loaddate": 1700000000050.5}}), [501]),
                "attribute value drift":
                    ("source_pbf_attribute_value_drift",
                     _encode_pbf_window([501], value_message_hook=lambda oid, field, value:
                         _message(1, b"\x19" + struct.pack("<d", 1.5))
                         if field["name"] == "huc10" else
                         _message(1, _pbf_value_message(value, field["type"]))), [501]),
            }
            for label, (expected_code, body, case_ids) in cases.items():
                with self.subTest(label=label):
                    with self.assertRaisesRegex(ValueError, expected_code):
                        source._pbf_feature_result(body, "page_0001a_pbf", case_ids)
            with self.assertRaisesRegex(ValueError, "source_pbf_page_population_incomplete"):
                source._pbf_feature_result(valid, "page_0001a_pbf", [501])

    def test_protobuf_rescue_part_requires_refused_leaf_on_replay(self):
        documents = _documents()
        roster = list(_TEST_ROSTER)
        refusal = {"error": {"code": 500, "message": "Error performing query operation",
                             "details": []}}
        pbf_leaf = _encode_pbf_window([501], exceeded=True)

        def part_page(ids: list[int], exceeded: bool) -> dict:
            return {"displayFieldName": "NAME", "exceededTransferLimit": exceeded,
                    "features": [{"attributes": _attributes(oid),
                                  "geometry": {"rings": _feature_rings(oid)}} for oid in ids],
                    "fieldAliases": {field["name"]: field["alias"]
                                     for field in source.EXPECTED_PAGE_FIELD_DECLARATIONS},
                    "fields": json.loads(json.dumps(source.EXPECTED_PAGE_FIELD_DECLARATIONS)),
                    "geometryType": "esriGeometryPolygon", "spatialReference": dict(_SR)}

        replay_documents = {**{name: document for name, document in documents.items()
                               if name != "page_0001"},
                            "page_0001_refusal": refusal, "page_0001a_pbf": pbf_leaf,
                            "page_0001b": part_page([100_003], False)}
        slot_urls = {}
        with relaxed_pins():
            roster_urls = dict(source._capture_roster(roster))
            for name in (*source.METADATA_NAMES, "page_0000", "page_0002"):
                slot_urls[name] = roster_urls[name]
            slot_urls["page_0001_refusal"] = roster_urls["page_0001"]
            slot_urls["page_0001a_pbf"] = source._window_url(roster, 1, "a", fmt="pbf")
            slot_urls["page_0001b"] = source._window_url(roster, 1, "b")
        replay_root = self._capture_root_with_urls(replay_documents, slot_urls)
        manifest = {"complete": True, "captured_at": "2026-11-14T12:00:00.000000Z",
                    "receipts": {name: json.loads(
                        (replay_root / f"{name}.receipt.json").read_bytes())
                        for name in sorted(replay_documents)},
                    "artifact_roster": source.capture_roster_digests(
                        replay_root, exclude={"capture_manifest.json"}),
                    "page_parts": {"page_0001": ["page_0001a_pbf", "page_0001b"]},
                    "page_refusals": ["page_0001_refusal"]}
        (replay_root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        with relaxed_pins(EXPECTED_ID_SHA256=_digest(_canonical(roster)), DISK_FLOOR_BYTES=1):
            with self.assertRaisesRegex(ValueError, "retained_capture_refusal_slot_invalid"):
                source._read_replay(replay_root)

    def _capture_root_with_urls(self, documents: dict, slot_urls: dict) -> Path:
        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-slots-")
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / "captures"
        root.mkdir(parents=True)
        for name, document in documents.items():
            body = _encode(document)
            wire = b"wire:" + body[:32]
            url = slot_urls[name]
            receipt = {"method": "GET", "url": url, "final_url": url,
                       "observed_at": "2026-11-14T12:00:00.000000Z", "status": 200,
                       "complete_body": True, "bytes": len(body), "sha256": _digest(body),
                       "wire_bytes": len(wire), "wire_sha256": _digest(wire),
                       "content_encoding": None, "content_length": None,
                       "error_code": None, "error_type": None}
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(wire)
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        return root

    def _root_with_manifest(self, source_root: Path, manifest: dict) -> Path:
        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-manifest-")
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name) / "captures"
        shutil.copytree(source_root, root)
        (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        return root

    def test_replay_memory_is_bounded_by_single_artifacts_not_population(self):
        """Replay retains paths/receipts/digests as descriptors and loads each
        body only while validating it: doubling the retained page count must
        not multiply the peak, whereas population-wide body retention would
        scale it with the whole source."""
        module = sys.modules[__name__]
        pad = 1_000_000

        def replay_peak(page_count: int) -> int:
            roster = list(range(11, 11 + 2 * page_count))
            with tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-mem-") as tmp, \
                    patch.object(module, "_TEST_ROSTER", roster), \
                    patch.object(module, "_TEST_COUNT", len(roster)), \
                    patch.object(module, "_TEST_PAGE_COUNT", page_count):
                documents = _documents()
                for index in range(page_count):
                    documents[f"page_{index:04d}"]["pad"] = "p" * pad
                with relaxed_pins():
                    root = _write_capture_root(Path(tmp) / "captures", documents)
                manifest = {"complete": True, "captured_at": "2026-11-14T12:00:00.000000Z",
                            "receipts": {name: json.loads(
                                (root / f"{name}.receipt.json").read_bytes())
                                for name in sorted(documents)},
                            "artifact_roster": source.capture_roster_digests(
                                root, exclude={"capture_manifest.json"}),
                            "page_parts": {}, "page_refusals": []}
                (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
                tracemalloc.start()
                try:
                    with relaxed_pins(EXPECTED_ID_SHA256=_digest(_canonical(roster)),
                                      DISK_FLOOR_BYTES=1):
                        source._read_replay(root)
                finally:
                    _current, peak = tracemalloc.get_traced_memory()
                    tracemalloc.stop()
                return peak

        small, large = replay_peak(4), replay_peak(8)
        self.assertLess(large, small * 1.5)

    def test_capture_window_memory_is_bounded_by_single_artifacts_not_population(self):
        """Window capture writes bodies to disk and retains only receipts, so
        doubling the number of bisection leaves leaves the peak flat, where
        population-wide body retention would double it with the leaf count."""
        body_size = 1_000_000

        def walk_peak(leaf_floor: int) -> int:
            roster = [11_000 + number for number in range(100)]
            refusal = json.dumps({"error": {"code": 500,
                                            "message": "Error performing query operation",
                                            "details": []}}).encode()

            def fake_capture(_client, url, root, name, max_bytes=0):
                if len(source._window_slice(roster, 0, name[len("page_0000"):])) > leaf_floor:
                    body = refusal
                else:
                    # Served leaves carry one megabyte of body bytes; the
                    # walk must retain only the receipt.
                    body = json.dumps(
                        {"pad": "p" * (body_size - 40), "features": []}).encode()
                wire = b"wire:" + body[:32]
                receipt = {"method": "GET", "url": url, "final_url": url,
                           "observed_at": "2026-11-14T12:00:00.000000Z", "status": 200,
                           "complete_body": True, "bytes": len(body), "sha256": _digest(body),
                           "wire_bytes": len(wire), "wire_sha256": _digest(wire),
                           "content_encoding": None, "content_length": None,
                           "error_code": None, "error_type": None}
                (root / f"{name}.body").write_bytes(body)
                (root / f"{name}.wire").write_bytes(wire)
                (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
                return body, receipt

            with tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-cmem-") as tmp:
                root = Path(tmp) / "captures"
                root.mkdir(parents=True)
                tracemalloc.start()
                try:
                    with relaxed_pins():
                        with patches(source, _capture=fake_capture, MIN_WINDOW_IDS=leaf_floor,
                                     PAGE_SIZE=100):
                            parts, refusals = source._capture_page_window(None, roster, 0, root)
                finally:
                    _current, peak = tracemalloc.get_traced_memory()
                    tracemalloc.stop()
            served = [name for name in parts if not name.endswith("_refusal")]
            self.assertEqual(len(served), 16 if leaf_floor == 10 else 32)
            return peak

        small, large = walk_peak(10), walk_peak(5)
        self.assertLess(large, small + 2 * body_size)

    def test_unsafe_capture_version_is_refused(self):
        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-version-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True,
                                         local_dir=str(Path(temporary.name) / "stage"))
        with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
            source.capture_wbd_10_digit_hu(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

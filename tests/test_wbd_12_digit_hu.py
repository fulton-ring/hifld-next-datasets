from __future__ import annotations

import contextlib
import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from shapely import from_wkb

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.wbd_12_digit_hu import (
    wbd_12_digit_hu, wbd_12_digit_hu_complete, wbd_12_digit_hu_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import wbd_12_digit_hu as source
from dagster_hifld.sources.usace_obstruction_lines import _canonical

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
        elif name == "huc12":
            attrs[name] = f"{oid % 100_000:012d}"
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
        decoded = from_wkb(source._assemble_geometry(_feature_rings(oid)))
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
             "sourceSpatialReference": dict(_SR), "spatialReference": dict(_SR),
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


class Wbd12DigitHuTests(unittest.TestCase):
    def _capture_root(self, documents: dict) -> Path:
        temporary = tempfile.TemporaryDirectory(prefix="wbd-12-digit-hu-capture-")
        self.addCleanup(temporary.cleanup)
        with relaxed_pins():
            return _write_capture_root(Path(temporary.name) / "captures", documents)

    def test_pinned_source_constants_match_verified_evidence(self):
        self.assertEqual(source.DATASET_SLUG, "wbd-12-digit-hu-subwatershed")
        self.assertEqual(source.SERVICE_URL,
                         "https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer")
        self.assertEqual(source.LAYER_URL, source.SERVICE_URL + "/6")
        self.assertEqual(source.LAYER_NAME, "12-digit HU (Subwatershed)")
        self.assertEqual(source.MAP_NAME, "wbd")
        self.assertEqual(source.EXPECTED_COUNT, 103_068)
        self.assertEqual(source.PAGE_SIZE, 500)
        self.assertEqual(source.DECLARED_MAX_RECORD_COUNT, 2000)
        self.assertEqual((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE,
                         source.PAGE_COUNT)
        self.assertEqual(source.SERVICE_DOC_URL,
                         "https://www.usgs.gov/national-hydrography/access-national-hydrography-products")
        self.assertEqual(source.EXPECTED_COPYRIGHT_TEXT,
                         "USGS WBD - Watershed Boundary Dataset. Data refreshed October, 2026.")
        self.assertEqual(source.EXPECTED_EXTENT["xmin"], -19951755.682)
        self.assertEqual(source.EXPECTED_EXTENT["xmax"], 20021551.7474)
        self.assertEqual([entry for entry in source.EXPECTED_SERVICE_LAYERS if entry[0] == 6],
                         [(6, "12-digit HU (Subwatershed)")])
        self.assertEqual(source.NATIVE_WKID, 102100)
        self.assertEqual(source.SOURCE_CRS_WKID, 3857)
        self.assertEqual([field["name"] for field in source.EXPECTED_PAGE_FIELD_DECLARATIONS],
                         ["objectid", "tnmid", "metasourceid", "sourcedatadesc",
                          "sourceoriginator", "sourcefeatureid", "loaddate",
                          "referencegnis_ids", "areaacres", "areasqkm", "states", "huc12",
                          "name", "hutype", "humod", "tohuc", "noncontributingareaacres",
                          "noncontributingareasqkm", "globalid", "shape_Length", "shape_Area"])
        self.assertEqual(len(source.EXPECTED_LAYER_FIELDS), 22)
        self.assertEqual(source.EXPECTED_LAYER_FIELDS[18]["name"], "shape")
        self.assertEqual(source.EXPECTED_LAYER_FIELDS[18]["type"], "esriFieldTypeGeometry")
        self.assertEqual(source.PUBLISHER,
                         "U.S. Geological Survey, National Geospatial Technical Operations Center")

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(wbd_12_digit_hu, hifld_ingest_assets)
        self.assertEqual(wbd_12_digit_hu_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = wbd_12_digit_hu_complete({"feature_count": source.EXPECTED_COUNT,
                                          "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = wbd_12_digit_hu_complete({"feature_count": source.EXPECTED_COUNT - 1,
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
        self.assertIn("resultRecordCount=500", source._page_url(0))
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
        self.assertEqual(audit["null_geometry_count"], len(_GATE_FEATURES))
        self.assertEqual(audit["geometry_valid_count"], len(_TEST_ROSTER) - len(_GATE_FEATURES))
        self.assertEqual(audit["unusable_geometry_counts"], dict(sorted(
            (mode, sum(1 for value in _GATE_FEATURES.values() if value == mode))
            for mode in set(_GATE_FEATURES.values()))))
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
            if oid not in _GATE_FEATURES:
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
            with self.assertRaisesRegex(ValueError, "source_null_geometry_count_drift"):
                with relaxed_pins(EXPECTED_NULL_GEOMETRY_COUNT=1,
                                  EXPECTED_UNUSABLE_GEOMETRY_COUNTS=gate_counts):
                    source._validate_stream(root, parquet_sink=None)
            with self.assertRaisesRegex(ValueError, "source_unusable_geometry_counts_drift"):
                with relaxed_pins(EXPECTED_NULL_GEOMETRY_COUNT=3,
                                  EXPECTED_UNUSABLE_GEOMETRY_COUNTS={
                                      **gate_counts, "source_polygon_ring_degenerate": 2}):
                    source._validate_stream(root, parquet_sink=None)

    def test_wkb_preserves_ring_structure_and_declared_types(self):
        single = source._assemble_geometry(_feature_rings(11)[:1])
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 3))
        with_hole = source._assemble_geometry([_shell(2), _hole(_shell(2))])
        self.assertEqual(struct.unpack("<BI", with_hole[:5]), (1, 3))
        self.assertEqual(len(from_wkb(with_hole).interiors), 1)
        multi = source._assemble_geometry([_shell(3), _shell(4)])
        self.assertEqual(struct.unpack("<BI", multi[:5]), (1, 6))
        self.assertEqual(len(from_wkb(multi).geoms), 2)
        for rings, mode in (
                ([_hole(_shell(1))], "source_polygon_exterior_missing"),
                ([[_shell(1)[0]] * 4], "source_polygon_ring_degenerate"),
                ([_bowtie(1)], "source_polygon_ring_self_intersecting"),
                (_overlapping_shells(1), "source_polygon_assembly_invalid")):
            with self.assertRaisesRegex(Exception, mode):
                source._assemble_geometry(rings)
        self.assertIn("source_polygon_hole_unassigned", source.GATE_MODES)

    def test_replay_rejects_roster_drift_and_honors_retained_receipts(self):
        documents = _documents()
        roster = list(_TEST_ROSTER)
        temporary = tempfile.TemporaryDirectory(prefix="wbd-12-digit-hu-replay-")
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
            captures = source._read_replay(root)
            self.assertEqual(set(captures), set(documents))
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
                fieldAliases={**d["page_0000"]["fieldAliases"], "huc12": "HUC"}),
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
        temporary = tempfile.TemporaryDirectory(prefix="wbd-12-digit-hu-parquet-")
        self.addCleanup(temporary.cleanup)
        sink = Path(temporary.name) / "wbd-12-digit-hu.parquet"
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
            if oid in _GATE_FEATURES:
                self.assertIsNone(wkb)
            else:
                decoded = from_wkb(wkb)
                reference = from_wkb(source._assemble_geometry(_feature_rings(oid)))
                self.assertEqual(decoded.area, reference.area)
                self.assertEqual(decoded.length, reference.length)
        self.assertEqual(derived["parquet_path"], sink)

    def test_unsafe_capture_version_is_refused(self):
        temporary = tempfile.TemporaryDirectory(prefix="wbd-12-digit-hu-version-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True,
                                         local_dir=str(Path(temporary.name) / "stage"))
        with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
            source.capture_wbd_12_digit_hu(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

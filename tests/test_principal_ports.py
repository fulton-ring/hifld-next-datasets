import copy
import gzip
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely import from_wkb

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.principal_ports import (
    principal_ports_draft_schedule,
    principal_ports_source_complete,
    principal_ports_usace,
)
from dagster_hifld.sources import principal_ports as source
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.capture_completeness import approved_baseline_drift

COUNT = 150
IDS = list(range(1, COUNT + 1))
EXTENT = {"xmin": -18565289.717, "ymin": 1993523.1207999997,
          "xmax": -7181517.771299999, "ymax": 8737821.747599997}
REAL_CLIENT = httpx.Client


def _clockwise(x0, y0, x1, y1):
    return [[x0, y0], [x0, y1], [x1, y1], [x1, y0], [x0, y0]]


def _counterclockwise(x0, y0, x1, y1):
    return [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]


def _captures(ids=None, extent=None):
    extent = dict(EXTENT if extent is None else extent)
    ids = list(IDS if ids is None else ids)
    fields = source.EXPECTED_FIELD_DECLARATIONS
    attributes = []
    geometry = {"rings": [_clockwise(extent["xmin"], extent["ymin"], extent["xmax"], extent["ymax"])]}
    for object_id in ids:
        row = {
            "OBJECTID": object_id,
            "RANK": float(object_id),
            "PORT": float(1000 + object_id),
            "TYPE": "Coastal",
            "PORTNAME": f"Port {object_id}",
            "TOTAL": float(object_id * 10),
            "DOMESTIC": float(object_id * 2),
            "FOREIGN_": float(object_id * 8),
            "IMPORTS": float(object_id * 3),
            "EXPORTS": float(object_id * 5),
            "Shape__Area": float(object_id * 100),
            "Shape__Length": float(object_id * 20),
        }
        attributes.append(row)
    layer = {
        "name": "Principal_Ports",
        "id": 0,
        "serviceItemId": source.BTS_ITEM_ID,
        "geometryType": "esriGeometryPolygon",
        "objectIdField": "OBJECTID",
        "maxRecordCount": 2000,
        "hasZ": False,
        "hasM": False,
        "extent": {**extent, "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
        "spatialReference": {"wkid": 102100, "latestWkid": 3857},
        "fields": copy.deepcopy(fields),
    }
    service = {
        "serviceItemId": source.BTS_ITEM_ID,
        "maxRecordCount": 2000,
        "copyrightText": "U.S. Army Corps of Engineers",
        "serviceDescription": "Principal Ports for CY 2023",
        "spatialReference": {"wkid": 102100, "latestWkid": 3857},
    }
    item = {
        "id": source.BTS_ITEM_ID,
        "owner": "usace_iwr_cac",
        "access": "public",
        "url": source.SERVICE_URL,
        "title": "Principal Ports",
        "description": "USACE ports for CY 2023",
        "snippet": "USACE ports for CY 2023",
    }
    page = {
        "geometryType": "esriGeometryPolygon",
        "spatialReference": {"wkid": 102100, "latestWkid": 3857},
        "features": [{"attributes": row, "geometry": geometry} for row in attributes],
    }
    body_docs = {
        "data_gov_landing": (
            f'"publisher": "United States Army Corp of Engineers (USACE)" '
            f'{source.DATA_GOV_RESOURCE_DOI} {source.BTS_CATALOG_DOI} CY 2023 '
            "https://transtats.bts.gov/NTADmetadata/USDOT_BTS_NTAD_Principal_Ports.xml"
        ).encode(),
        "bts_metadata": (
            f"{source.DATA_GOV_RESOURCE_DOI} CY 2023 United States Army Corp of Engineers (USACE) "
            "Waterborne Commerce Statistics Center"
        ).encode(),
        "bts_item": json.dumps(item).encode(),
        "service_before": json.dumps(service).encode(),
        "layer_before": json.dumps(layer).encode(),
        "count_before": json.dumps({"count": len(ids)}).encode(),
        "ids_before": json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": ids}).encode(),
        "page_000": json.dumps(page).encode(),
        "layer_after": json.dumps(layer).encode(),
        "count_after": json.dumps({"count": len(ids)}).encode(),
        "ids_after": json.dumps({"objectIdFieldName": "OBJECTID", "objectIds": ids}).encode(),
    }
    for index, start in enumerate(range(0, len(ids), source.PAGE_SIZE)):
        body_docs[f"page_{index:03d}"] = json.dumps({
            **page, "features": page["features"][start:start + source.PAGE_SIZE]}).encode()
    return {name: (body, {}) for name, body in body_docs.items()}


def _client_factory(captures, encoding="identity", fail_page=False):
    ids = json.loads(captures["ids_before"][0])["objectIds"]
    bodies = {url: captures[name][0] for name, url in source._capture_urls(ids)}

    def handler(request):
        if fail_page and "objectIds=" in str(request.url):
            return httpx.Response(503, stream=httpx.ByteStream(b"unavailable"))
        body = bodies[str(request.url)]
        wire = gzip.compress(body, mtime=0) if encoding == "gzip" else body
        return httpx.Response(200, headers={"Content-Encoding": encoding, "Content-Length": str(len(wire))},
                              stream=httpx.ByteStream(wire))

    return lambda *args, **kwargs: REAL_CLIENT(*args, **kwargs, transport=httpx.MockTransport(handler))


class PrincipalPortsTests(unittest.TestCase):
    def test_changed_population_and_extent_use_publisher_witnesses(self):
        extent = {**EXTENT, "xmax": EXTENT["xmax"] + 20}
        for ids in ([7, 21, 90], list(range(1, 1002))):
            with self.subTest(count=len(ids)):
                _fields, features, audit = source._validate(_captures(ids, extent))
                self.assertEqual([f["attributes"]["OBJECTID"] for f in features], ids)
                self.assertEqual(audit["source_coverage_extent"], extent)
                self.assertTrue(audit["source_population_complete"])

    def test_pages_must_match_each_requested_id_slice(self):
        captures = _captures(range(1, 1002))
        pages = [json.loads(captures[name][0]) for name in ("page_000", "page_001")]
        pages[0]["features"][-1], pages[1]["features"][0] = (
            pages[1]["features"][0], pages[0]["features"][-1])
        for name, page in zip(("page_000", "page_001"), pages):
            captures[name] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete"):
            source._validate(captures)

    def test_page_roster_transfer_crs_and_witness_failures_refuse(self):
        for label in ("missing", "extra", "swapped", "duplicate", "transfer", "crs", "torn", "empty"):
            with self.subTest(label=label):
                captures = _captures(list(range(1, 1002)))
                if label == "missing":
                    captures.pop("page_001")
                elif label == "extra":
                    captures["page_002"] = captures["page_001"]
                elif label == "swapped":
                    captures["page_000"], captures["page_001"] = captures["page_001"], captures["page_000"]
                elif label == "duplicate":
                    _mutate_json(captures, "page_001", lambda d: d["features"][0]["attributes"].update(OBJECTID=1))
                elif label == "transfer":
                    _mutate_json(captures, "page_001", lambda d: d.update(exceededTransferLimit=True))
                elif label == "crs":
                    _mutate_json(captures, "page_001", lambda d: d.update(spatialReference={"wkid": 4326}))
                elif label == "torn":
                    _mutate_json(captures, "count_after", lambda d: d.update(count=1000))
                else:
                    captures = _captures([])
                with self.assertRaises(ValueError):
                    source._validate(captures)

    def test_dynamic_extent_remains_finite_ordered_crs_bound_and_contains_geometry(self):
        captures = _captures([7, 21])
        for name in ("layer_before", "layer_after"):
            _mutate_json(captures, name, lambda d: d["extent"].update(xmax=EXTENT["xmax"] + 20))
        _fields, _features, audit = source._validate(captures)
        self.assertEqual(audit["source_coverage_extent"], EXTENT)
        self.assertEqual(audit["source_declared_extent"]["xmax"], EXTENT["xmax"] + 20)
        for label in ("nonfinite", "boolean", "inverted", "crs", "outside", "torn"):
            with self.subTest(label=label):
                captures = _captures([7, 21])
                for name in ("layer_before", "layer_after"):
                    if label in ("nonfinite", "boolean", "inverted", "outside"):
                        value = {"nonfinite": float("inf"), "boolean": True,
                                 "inverted": EXTENT["xmin"] - 1, "outside": EXTENT["xmax"] - 1}[label]
                        _mutate_json(captures, name, lambda d: d["extent"].update(xmax=value))
                    elif label == "crs":
                        _mutate_json(captures, name, lambda d: d["extent"].update(spatialReference={"wkid": 4326}))
                if label == "torn":
                    _mutate_json(captures, "layer_after", lambda d: d["extent"].update(xmax=0))
                with self.assertRaises(ValueError):
                    source._validate(captures)

    def test_real_sealing_replay_refresh_and_changed_population_preserve_originals(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(source, "PAGE_SIZE", 2):
            storage = StagingStorageResource(use_local=True, local_dir=tmp + "/stage", prefix="")
            extent = {**EXTENT, "xmax": EXTENT["xmax"] + 20}
            captures = _captures([7, 21, 90], extent)
            _mutate_json(captures, "page_000", lambda d: d["features"][0]["attributes"].update(
                RANK=None, PORT=1007.25, TOTAL=42.125, TYPE=None))
            with patch.object(source.httpx, "Client", side_effect=_client_factory(captures)):
                first = source.capture_principal_ports(storage, version="first", capture_directory=tmp + "/first")
            self.assertEqual(first["feature_count"], 3)
            self.assertEqual(first["source_coverage_extent"], extent)
            prefix = first["candidate_ready"].removesuffix("/metadata/candidate_ready.json")
            table = pq.read_table(pa.BufferReader(storage.read_key(prefix + "/geoparquet/principal-ports.parquet")))
            self.assertEqual(table["OBJECTID"].to_pylist(), [7, 21, 90])
            self.assertEqual(table["PORT"].to_pylist(), [1007.25, 1021.0, 1090.0])
            self.assertEqual(table["RANK"].to_pylist(), [None, 21.0, 90.0])
            self.assertEqual(table["TOTAL"][0].as_py(), 42.125)
            self.assertIsNone(table["TYPE"][0].as_py())
            self.assertEqual(table["PORT"].type, pa.float64())
            self.assertEqual(table["geometry"][0].as_py(), source._feature_geometry_wkb(
                json.loads(captures["page_000"][0])["features"][0]["geometry"]))
            self.assertEqual(json.loads(table.schema.metadata[b"geo"])["columns"]["geometry"]["crs"],
                             source.SOURCE_CRS.to_json_dict())
            originals = {key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}
            with patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                replay = source.capture_principal_ports(storage, version="replay", capture_directory=tmp + "/first")
            self.assertTrue(replay["reused"])
            self.assertEqual(replay["source_coverage_extent"], extent)
            with patch.object(source.httpx, "Client", side_effect=_client_factory(captures)):
                refresh = source.capture_principal_ports(storage, version="refresh", capture_directory=tmp + "/refresh")
            self.assertEqual(refresh["version"], first["version"])
            self.assertTrue(refresh["reused"])
            self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}, originals)
            with patch.object(source.httpx, "Client", side_effect=_client_factory(_captures([7, 90]))):
                changed = source.capture_principal_ports(storage, version="changed", capture_directory=tmp + "/changed")
            self.assertNotEqual(changed["version"], first["version"])
            self.assertEqual(changed["feature_count"], 2)
            for key, original in originals.items():
                self.assertEqual((storage.read_key(key), storage.object_snapshot(key)), original)
            self.assertEqual(approved_baseline_drift(first).metadata["comparison_status"].value, "skipped_no_approved_baseline")
            baseline = {**first, "release_accepted": True}
            drift = approved_baseline_drift(changed, baseline)
            self.assertFalse(drift.passed)
            self.assertEqual(drift.severity.value, "WARN")
            parquet_key = prefix + "/geoparquet/principal-ports.parquet"
            storage.write_key(parquet_key, b"corrupt")
            tampered = {key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}
            with patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                with self.assertRaisesRegex(ValueError, "cached_candidate_object_integrity_failed"):
                    source.capture_principal_ports(storage, version="corrupt", capture_directory=tmp + "/first")
            self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}, tampered)

    def test_replay_binds_identity_and_gzip_wire_to_decoded_body(self):
        for encoding in ("identity", "gzip"):
            with self.subTest(encoding=encoding), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / "capture"
                storage = StagingStorageResource(use_local=True, local_dir=tmp + "/stage", prefix="")
                with patch.object(source.httpx, "Client", side_effect=_client_factory(_captures([7]), encoding)):
                    source._read_capture(root, storage, "wire")
                with patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                    self.assertEqual(len(source._read_capture(root, storage, "honest")), 11)
                body = (root / "page_000.body").read_bytes().replace(b"Port 7", b"Port X")
                (root / "page_000.body").write_bytes(body)
                receipt = json.loads((root / "page_000.receipt.json").read_bytes())
                receipt.update(bytes=len(body), sha256=source._sha(body))
                (root / "page_000.receipt.json").write_bytes(json.dumps(receipt).encode())
                manifest = json.loads((root / "capture_manifest.json").read_bytes())
                manifest["receipts"]["page_000"] = receipt
                manifest["artifact_roster"] = source.capture_roster_digests(root, exclude={"capture_manifest.json"})
                (root / "capture_manifest.json").write_bytes(json.dumps(manifest).encode())
                before = {p.name: p.read_bytes() for p in root.iterdir()}
                with self.assertRaisesRegex(ValueError, "retained_capture_wire_body_mismatch"):
                    source._read_replay(root)
                self.assertEqual({p.name: p.read_bytes() for p in root.iterdir()}, before)

    def test_replay_preflights_every_file_and_combined_budget_before_reads(self):
        for name in ("capture_manifest.json", "ids_before.body", "page_001.wire", "page_000.receipt.json", "failed.wire"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / name).write_bytes(b"x" * 21)
                with patch.object(source, "PAGE_MAX_BYTES", 20), patch.object(Path, "read_bytes", side_effect=AssertionError("read before preflight")):
                    with self.assertRaisesRegex(ValueError, "retained_capture_response_budget_exceeded"):
                        source._read_replay(root)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "capture_manifest.json").write_bytes(b"x" * 20)
            (root / "ids_before.body").write_bytes(b"x" * 20)
            with patch.object(source, "CAPTURE_MAX_BYTES", 30), patch.object(Path, "read_bytes", side_effect=AssertionError("read before preflight")):
                with self.assertRaisesRegex(ValueError, "retained_capture_total_budget_exceeded"):
                    source._read_replay(root)
            (root / "directory.body").mkdir()
            with self.assertRaisesRegex(ValueError, "capture_roster_nonregular_path"):
                source._read_replay(root)

    def test_failed_acquisition_keeps_manifest_and_quarantine_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            storage = StagingStorageResource(use_local=True, local_dir=tmp + "/stage", prefix="")
            with patch.object(source.httpx, "Client", side_effect=_client_factory(_captures([7]), fail_page=True)):
                with self.assertRaises(ValueError):
                    source._read_capture(root, storage, "failed")
            self.assertFalse(json.loads((root / "capture_manifest.json").read_bytes())["complete"])
            self.assertEqual((root / "page_000.wire").read_bytes(), b"unavailable")
            self.assertTrue(any("quarantine" in key for key in storage.list_prefix("")))
            with patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                with self.assertRaises(ValueError):
                    source._read_capture(root, storage, "replay-failed")

    def test_asset_is_registered_and_scheduled_refresh_is_stopped(self):
        self.assertIn(principal_ports_usace, hifld_ingest_assets)
        self.assertEqual(principal_ports_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        result = principal_ports_source_complete({
            "feature_count": 150, "source_reported_count": COUNT,
            "source_population_complete": True,
            "source_invalid_geometry_count_without_repair": 8,
        })
        self.assertTrue(result.passed)
        self.assertFalse(result.metadata["release_accepted"].value)
        self.assertEqual(result.metadata["unrepaired_invalid_source_geometries"].value, 8)

    def test_esri_multipart_rings_are_preserved_as_native_xy_without_repair(self):
        outer = _clockwise(0, 0, 10, 10)
        hole = _counterclockwise(2, 2, 3, 3)
        second_shell = _clockwise(20, 20, 30, 30)
        parsed = from_wkb(source._feature_geometry_wkb({"rings": [outer, hole, second_shell]}))
        self.assertEqual(parsed.geom_type, "MultiPolygon")
        self.assertEqual(len(parsed.geoms), 2)
        self.assertEqual(list(parsed.geoms[0].exterior.coords), [tuple(p) for p in outer])
        self.assertEqual(list(parsed.geoms[0].interiors[0].coords), [tuple(p) for p in hole])
        self.assertEqual(list(parsed.geoms[1].exterior.coords), [tuple(p) for p in second_shell])

    def test_polygon_rejects_non_xy_nonfinite_open_and_contradictory_crs(self):
        ring = _clockwise(0, 0, 1, 1)
        cases = [
            ({"rings": [[*ring[:-1], [1, 1, 7], ring[-1]]]}, "source_polygon_coordinate_not_finite_xy"),
            ({"rings": [[*ring[:-1], [float("nan"), 0], ring[-1]]]}, "source_polygon_coordinate_not_finite_xy"),
            ({"rings": [ring[:-1]]}, "source_polygon_ring_invalid_or_open"),
            ({"rings": [ring], "hasM": True}, "source_polygon_not_xy"),
            ({"rings": [ring], "spatialReference": {"wkid": 4326}}, "feature_geometry_crs_drift"),
        ]
        for geometry, reason in cases:
            with self.subTest(reason=reason), self.assertRaisesRegex(ValueError, reason):
                source._feature_geometry_wkb(geometry)

    def test_parquet_preserves_native_field_types_values_and_epsg3857(self):
        captures = _captures()
        fields, features, _audit = source._validate(captures)
        table = pq.read_table(pa.BufferReader(source._parquet_bytes(fields, features[:2])))
        self.assertEqual(table.column_names, [field["name"] for field in fields] + ["geometry"])
        self.assertEqual(table.schema.field("PORT").type, pa.float64())
        self.assertEqual(table["PORT"].to_pylist(), [1001.0, 1002.0])
        self.assertEqual(table["OBJECTID"].to_pylist(), [1, 2])
        metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(metadata["columns"]["geometry"]["geometry_types"],
                         ["MultiPolygon", "Polygon"])
        self.assertEqual(metadata["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 3857})

    def test_complete_extract_validates_count_identity_fields_and_port_roster(self):
        captures = _captures()
        _fields, features, audit = source._validate(captures)
        self.assertEqual(len(features), 150)
        self.assertTrue(audit["source_population_complete"])
        self.assertEqual(audit["source_id_min"], 1001.0)
        self.assertEqual(audit["source_polygon_vertex_count"], 750)
        self.assertEqual(audit["source_wkid"], 3857)
        self.assertEqual(audit["source_invalid_geometry_count_without_repair"], 0)

    def test_schema_count_id_and_transfer_limit_drift_are_rejected(self):
        mutations = [
            ("field type", lambda c: _mutate_json(c, "layer_before", lambda d: d["fields"][2].update(type="esriFieldTypeString")), "source_field_schema_drift"),
            ("count", lambda c: _mutate_json(c, "count_after", lambda d: d.update(count=149)), "source_count_or_id_roster_drift"),
            ("ID roster", lambda c: _mutate_json(c, "ids_after", lambda d: d["objectIds"].pop()), "source_count_or_id_roster_drift"),
            ("transfer limit", lambda c: _mutate_json(c, "page_000", lambda d: d.update(exceededTransferLimit=True)), "source_page_error_or_transfer_limit"),
        ]
        for name, mutate, reason in mutations:
            with self.subTest(name=name):
                captures = _captures()
                mutate(captures)
                with self.assertRaisesRegex(ValueError, reason):
                    source._validate(captures)


def _mutate_json(captures, name, mutator):
    body, receipt = captures[name]
    document = json.loads(body)
    mutator(document)
    captures[name] = (json.dumps(document).encode(), receipt)


if __name__ == "__main__":
    unittest.main()

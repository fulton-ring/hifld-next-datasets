"""Source contract tests for the complete official IENC LOCK_GATE_AREA layer."""

from __future__ import annotations

import copy
import json
import os
import shutil
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely import from_wkb, get_coordinates

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.publish import _copy_version_files
from dagster_hifld.assets.usace_lock_gate_areas import (
    usace_lock_gate_areas_complete,
    usace_lock_gate_areas_draft_job,
    usace_lock_gate_areas_draft_schedule,
    usace_lock_gate_areas_usace_ienc,
)
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.definitions import defs
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.release_approval import ReleaseApprovalError
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import usace_lock_gate_areas as source
from dagster_hifld.sources.epa_frs_common import _canonical, processing_identity


def _ring() -> list[list[float]]:
    extent = source.EXPECTED_EXTENT
    # Clockwise exterior ring touching the pinned coverage corners.
    return [
        [extent["xmin"], extent["ymin"]], [extent["xmin"], extent["ymax"]],
        [extent["xmax"], extent["ymax"]], [extent["xmax"], extent["ymin"]],
        [extent["xmin"], extent["ymin"]],
    ]


def _attributes(oid: int) -> dict:
    attributes = {field["name"]: None for field in source.ATTRIBUTE_FIELDS}
    attributes.update({
        "OBJECTID": oid, "Category_of_Gate": "Lock Gate",
        "Horizontal_Clearance": "17.1", "Vertical_Clearance": "",
        "Object_Name": "", "Information": "", "Scale_Minimum": 22000,
        "Source_Date": "20140805", "Source_Indication": "US,U3,SURVY",
        "Source_Dataset": "U37AG001.000", "Feature_ID": f"U3 {oid:010d}",
    })
    return attributes


def _docs() -> dict[str, dict]:
    object_ids = list(source.EXPECTED_IDS)
    crs = {"wkid": 4326, "latestWkid": 4326}
    service = {"mapName": "LOCK_GATE_AREA", "copyrightText": "USACE IENC",
               "spatialReference": crs}
    layer = {
        "id": 78, "name": "LOCK_GATE_AREA", "geometryType": "esriGeometryPolygon",
        "spatialReference": crs,
        "extent": {**source.EXPECTED_EXTENT, "spatialReference": crs},
        "maxRecordCount": 2000,
        "fields": copy.deepcopy(source.EXPECTED_FIELD_DECLARATIONS),
    }
    features = [{"attributes": _attributes(oid), "geometry": {"rings": [_ring()]}}
                for oid in object_ids]
    return {
        "bts_item": {
            "id": source.BTS_ITEM_ID, "owner": "USDOT_BTS",
            "title": "Inland Electronic Navigational Charts",
            "description": (
                "The Inland Electronic Navigational Charts (IENC) dataset is updated "
                "bi-monthly by USACE. Services: "
                "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes and "
                "geodatabase USACE_IENC_Master_Service_gdb.zip."
            ),
        },
        "service_directory": {"services": [
            {"name": "IENC_Feature_Classes/LOCK_GATE_AREA", "type": "MapServer"}]},
        "service_before": copy.deepcopy(service),
        "layer_before": layer,
        "count_before": {"count": source.EXPECTED_COUNT},
        "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": object_ids},
        "page_000": {"geometryType": "esriGeometryPolygon", "spatialReference": crs,
                     "features": features},
        "layer_after": copy.deepcopy(layer),
        "count_after": {"count": source.EXPECTED_COUNT},
        "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": object_ids},
    }


def _body(doc: dict) -> bytes:
    """Encode a capture body the way the service emits it: natural key order."""
    return json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _captures(docs: dict[str, dict]) -> dict[str, tuple[bytes, dict]]:
    return {name: (_body(doc), {"complete_body": True, "status": 200})
            for name, doc in docs.items()}


def _fixture_pins(docs: dict[str, dict]) -> dict[str, str]:
    """Snapshot pins derived from the synthetic fixture, standing in for the pins
    established from the actual live capture that the module freezes."""
    features = sorted(docs["page_000"]["features"],
                     key=lambda feature: feature["attributes"]["OBJECTID"])
    return {
        "EXPECTED_RECORD_SHA256": source._sha(source._canonical(
            [feature["attributes"] for feature in features])),
        "EXPECTED_GEOMETRY_SHA256": source._sha(source._canonical(
            [[feature["attributes"]["OBJECTID"], feature["geometry"]] for feature in features])),
        "EXPECTED_SCHEMA_SHA256": source._sha(source._canonical(docs["layer_before"]["fields"])),
    }


def _sealed_capture_directory(root: Path, docs: dict[str, dict]) -> None:
    expected_urls = dict(source._capture_urls(source.EXPECTED_IDS))
    receipts = {}
    for name, url in expected_urls.items():
        body = _body(docs[name])
        (root / f"{name}.body").write_bytes(body)
        (root / f"{name}.wire").write_bytes(body)
        receipt = {"method": "GET", "url": url, "final_url": url,
                   "observed_at": "2026-11-09T00:00:00Z", "status": 200,
                   "complete_body": True, "wire_bytes": len(body),
                   "wire_sha256": source._sha(body), "bytes": len(body),
                   "sha256": source._sha(body), "content_encoding": "identity",
                   "content_length": len(body), "error_code": None}
        (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        receipts[name] = receipt
    (root / "capture_manifest.json").write_bytes(_canonical({
        "complete": True, "receipts": receipts,
        "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"}),
    }) + b"\n")


class _FakeClient:
    """Minimal httpx.Client stand-in replaying canned bodies per exact URL."""

    def __init__(self, bodies: dict[str, bytes]):
        self._bodies = bodies

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def stream(self, method, url, **kwargs):
        body = self._bodies[url]
        request = httpx.Request(method, url)
        response = httpx.Response(200, stream=httpx.ByteStream(body), request=request)

        class _StreamContext:
            def __enter__(self):
                return response

            def __exit__(self, *exc):
                response.close()

        return _StreamContext()


class RegistrationGateTests(unittest.TestCase):
    def test_asset_is_registered_and_refresh_is_stopped_with_acceptance_false(self):
        self.assertIn(usace_lock_gate_areas_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_lock_gate_areas_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        passed = usace_lock_gate_areas_complete(
            {"feature_count": source.EXPECTED_COUNT, "source_population_complete": True})
        self.assertTrue(passed.passed)
        self.assertFalse(passed.metadata["release_accepted"].value)
        self.assertFalse(passed.metadata["rights_accepted"].value)
        self.assertFalse(passed.metadata["mapping_approved"].value)
        failed = usace_lock_gate_areas_complete(
            {"feature_count": source.EXPECTED_COUNT - 1, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_asset_check_job_and_schedule_are_registered_in_definitions(self):
        registered = defs.get_repository_def()
        self.assertIn(usace_lock_gate_areas_usace_ienc.key,
                      registered.asset_graph.get_all_asset_keys())
        self.assertIn("usace_lock_gate_areas_complete",
                      {key.name for key in registered.asset_checks_defs_by_key})
        self.assertIn(usace_lock_gate_areas_draft_job.name,
                      {job.name for job in registered.get_all_jobs()})
        self.assertIn(usace_lock_gate_areas_draft_schedule.name,
                      {schedule.name for schedule in registered.schedule_defs})


class LiveCapturePinTests(unittest.TestCase):
    def test_pins_record_the_verified_live_capture(self):
        self.assertEqual(source.EXPECTED_COUNT, 94)
        self.assertEqual(source.EXPECTED_IDS, list(range(1, 95)))
        self.assertEqual(source.EXPECTED_ID_SHA256,
                         "0e23b6e4dfbf50733e1b02554bf2e094265eaa4482c886717fbfb47cd042c0e2")
        self.assertEqual(source.EXPECTED_RECORD_SHA256,
                         "d4bdbc39a0857e00a62460ab2b64162a8850c4835c7a234313c0e12e6a31dcf1")
        self.assertEqual(source.EXPECTED_GEOMETRY_SHA256,
                         "aed206fac00c1fc7a2e753e96abb24241200c6bd0f03a0372dda226671683bfa")
        self.assertEqual(source.EXPECTED_SCHEMA_SHA256,
                         "1c15259a627ba5a989d34c88f147216630867d88713a984aea385d6034d18992")
        self.assertTrue(source.LAYER_URL.endswith("LOCK_GATE_AREA/MapServer/78"))
        self.assertEqual(source.EXPECTED_CAPTURE_NAMES, {
            "bts_item", "service_directory", "service_before", "layer_before", "count_before",
            "ids_before", "page_000", "layer_after", "count_after", "ids_after"})


class BtsSourceChainTests(unittest.TestCase):
    def test_pins_current_item_and_never_requests_prior_denied_item(self):
        self.assertEqual(source.BTS_ITEM_ID, "7ec34864c8804468a9cf52d437fe4b6c")
        self.assertEqual(source.PRIOR_BTS_ITEM_ID, "3607d485a9854d6fa458bf507058ceae")
        self.assertEqual(
            source.BTS_ITEM_URL,
            "https://www.arcgis.com/sharing/rest/content/items/"
            "7ec34864c8804468a9cf52d437fe4b6c?f=json",
        )
        requested = [url for _name, url in source._capture_urls(source.EXPECTED_IDS)]
        self.assertIn(source.BTS_ITEM_URL, requested)
        self.assertTrue(all(source.PRIOR_BTS_ITEM_ID not in url for url in requested))

    def test_denial_body_and_identity_drift_fail_closed(self):
        denial = {"error": {"code": 403, "messageCode": "GWM_0003",
                            "message": "Access denied", "details": []}}
        valid = _docs()["bts_item"]
        cases = [
            ("anonymous arcgis denial body", denial),
            ("prior denied item id", {**valid, "id": source.PRIOR_BTS_ITEM_ID}),
            ("foreign owner", {**valid, "owner": "SOMEONE_ELSE"}),
        ]
        for label, doc in cases:
            with self.subTest(label=label):
                body = _canonical(doc)
                with self.assertRaisesRegex(ValueError, "bts_ienc_item_identity_drift"):
                    source._validate({"bts_item": (body, {"complete_body": True, "status": 200})})

    def test_delegation_marker_drift_fails_closed(self):
        valid = _docs()["bts_item"]
        cases = [
            ("missing ienccloud service link", {**valid, "description": (
                "Inland Electronic Navigational Charts master geodatabase "
                "USACE_IENC_Master_Service_gdb.zip.")}),
            ("missing master gdb link", {**valid, "description": (
                "Inland Electronic Navigational Charts web services "
                "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes.")}),
            ("missing ienc program title", {**valid, "title": "Unrelated item",
                                            "description": (
                                                "Web services https://ienccloud.us/arcgis/rest/services/"
                                                "IENC_Feature_Classes and USACE_IENC_Master_Service_gdb.zip.")}),
        ]
        for label, doc in cases:
            with self.subTest(label=label):
                body = _canonical(doc)
                with self.assertRaisesRegex(ValueError, "bts_ienc_official_distribution_link_drift"):
                    source._validate({"bts_item": (body, {"complete_body": True, "status": 200})})


class DriftRefusalTests(unittest.TestCase):
    def _drift(self, mutation) -> str:
        docs = _docs()
        mutation(docs)
        with self.assertRaises(ValueError) as raised:
            source._validate(_captures(docs))
        return str(raised.exception)

    def test_count_and_id_roster_drift_fail_closed(self):
        self.assertEqual(
            self._drift(lambda d: d["count_after"].update(count=93)),
            "source_count_drift")
        self.assertEqual(
            self._drift(lambda d: d["ids_after"].update(
                objectIds=list(range(1, source.EXPECTED_COUNT)) + [95])),
            "source_id_population_drift")
        self.assertEqual(
            self._drift(lambda d: d["ids_before"].update(
                objectIdFieldName="ROWID")),
            "source_id_population_drift")
        self.assertEqual(
            self._drift(lambda d: d["ids_before"].update(objectIds=[2, *list(range(3, 96))])),
            "source_id_population_drift")

    def test_schema_and_service_identity_drift_fail_closed(self):
        def rename_field(docs):
            docs["layer_before"]["fields"][2]["name"] = "Changed"
        self.assertEqual(self._drift(rename_field), "source_field_schema_drift")

        def add_field(docs):
            docs["layer_before"]["fields"].append(
                {"name": "NEW_FIELD", "type": "esriFieldTypeString",
                 "alias": "NEW_FIELD", "length": 254, "domain": None})
        self.assertEqual(self._drift(add_field), "source_field_schema_drift")

        self.assertEqual(
            self._drift(lambda d: d["service_before"].update(mapName="SOMETHING_ELSE")),
            "source_service_identity_drift")
        self.assertEqual(
            self._drift(lambda d: d["service_directory"].update(services=[])),
            "official_ienc_service_directory_drift")

    def test_layer_identity_extent_and_page_limit_drift_fail_closed(self):
        self.assertEqual(
            self._drift(lambda d: d["layer_before"].update(id=79)),
            "source_layer_geometry_extent_or_page_limit_drift")
        self.assertEqual(
            self._drift(lambda d: d["layer_before"]["extent"].update(xmax=-70.0)),
            "source_layer_geometry_extent_or_page_limit_drift")
        self.assertEqual(
            self._drift(lambda d: d["layer_before"].update(maxRecordCount=1000)),
            "source_layer_geometry_extent_or_page_limit_drift")

    def test_page_and_feature_population_drift_fail_closed(self):
        self.assertEqual(
            self._drift(lambda d: d["page_000"].update(exceededTransferLimit=True)),
            "source_page_error_or_transfer_limit")
        self.assertEqual(
            self._drift(lambda d: d["page_000"].update(features=d["page_000"]["features"][1:])),
            "source_feature_population_incomplete")
        self.assertEqual(
            self._drift(lambda d: d["page_000"]["features"][0]["attributes"].update(Extra="x")),
            "source_record_attribute_schema_drift")
        self.assertEqual(
            self._drift(lambda d: d["page_000"]["features"][0]["attributes"].pop("Information")),
            "source_record_attribute_schema_drift")
        self.assertEqual(
            self._drift(lambda d: d["layer_after"].update(currentVersion="9.9")),
            "source_layer_changed_during_capture")

    def test_crs_drift_fails_closed_at_every_boundary(self):
        cases = [
            (lambda d: d["layer_before"].update(
                spatialReference={"wkid": 3857, "latestWkid": 4326}),
             "source_layer_crs_drift"),
            (lambda d: d["layer_before"]["extent"].update(spatialReference={"wkid": 4269}),
             "source_layer_extent_crs_drift"),
            (lambda d: d["page_000"].update(
                spatialReference={"wkid": 3857, "latestWkid": 3857}),
             "page_000_spatial_reference_crs_drift"),
            (lambda d: d["page_000"]["features"][0]["geometry"].update(
                spatialReference={"wkid": 3857, "latestWkid": 4326}),
             "feature_geometry_crs_drift"),
        ]
        for mutation, label in cases:
            with self.subTest(label=label):
                self.assertEqual(self._drift(mutation), label)

    def test_geometry_outside_pinned_coverage_fails_closed(self):
        def move_vertex(docs):
            ring = docs["page_000"]["features"][0]["geometry"]["rings"][0]
            ring[0] = [ring[0][0] - 10.0, ring[0][1]]
        self.assertEqual(self._drift(move_vertex), "source_geometry_coverage_extent_drift")

    def test_snapshot_drift_against_live_pins_fails_closed(self):
        docs = _docs()
        captures = _captures(docs)
        with self.assertRaisesRegex(ValueError, "source_record_snapshot_drift"):
            source._validate(captures)


class SyntheticCompleteCaptureTests(unittest.TestCase):
    def test_complete_capture_validates_and_audits_population(self):
        docs = _docs()
        with patch.multiple(source, **_fixture_pins(docs)):
            fields, features, audit = source._validate(_captures(docs))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), source.EXPECTED_COUNT)
        self.assertEqual(audit["source_feature_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], source.EXPECTED_COUNT)
        self.assertEqual(audit["source_feature_id_unique_count"], source.EXPECTED_COUNT)
        self.assertEqual(audit["source_feature_id_duplicate_count"], 0)
        self.assertEqual(audit["source_polygon_vertex_count"], source.EXPECTED_COUNT * 5)
        self.assertEqual(audit["source_ring_count_histogram"], {"1": source.EXPECTED_COUNT})
        self.assertEqual(audit["source_coverage_extent"], source.EXPECTED_EXTENT)
        self.assertFalse(audit["bts_item_succession_claimed"])
        self.assertEqual(audit["prior_bts_item_id"], source.PRIOR_BTS_ITEM_ID)

    def test_duplicate_and_nullable_feature_ids_are_preserved_without_substitutes(self):
        docs = _docs()
        duplicate = docs["page_000"]["features"][1]["attributes"]["Feature_ID"]
        docs["page_000"]["features"][0]["attributes"]["Feature_ID"] = duplicate
        docs["page_000"]["features"][2]["attributes"]["Feature_ID"] = None
        with patch.multiple(source, **_fixture_pins(docs)):
            _fields, features, audit = source._validate(_captures(docs))
        self.assertEqual(audit["source_feature_id_unique_count"], source.EXPECTED_COUNT - 2)
        self.assertEqual(audit["source_feature_id_duplicate_count"], 2)
        self.assertEqual(features[1]["attributes"]["Feature_ID"],
                         features[0]["attributes"]["Feature_ID"])
        self.assertIsNone(features[2]["attributes"]["Feature_ID"])


class GeometryContractTests(unittest.TestCase):
    def test_native_polygon_wkb_preserves_ring_and_vertex_order(self):
        exterior = [[0, 0], [0, 4], [4, 4], [4, 0], [0, 0]]
        interior = [[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]]
        wkb = source._polygon_wkb({"rings": [exterior, interior]})
        self.assertEqual(struct.unpack_from("<I", wkb, 5)[0], 2)
        polygon = from_wkb(wkb)
        self.assertEqual(get_coordinates(polygon.exterior).tolist(), exterior)
        self.assertEqual(get_coordinates(polygon.interiors[0]).tolist(), interior)

    def test_polygon_rejects_non_xy_nonfinite_open_and_rewound_rings(self):
        ring = [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]
        cases = [
            ({"rings": [[*ring[:-1], [0, 0, 5], ring[-1]]]},
             "source_polygon_coordinate_not_finite_xy"),
            ({"rings": [[*ring[:-1], [float("nan"), 0], ring[-1]]]},
             "source_polygon_coordinate_not_finite_xy"),
            ({"rings": [ring[:-1]]}, "source_polygon_ring_invalid_or_open"),
            ({"rings": [list(reversed(ring))]}, "source_exterior_ring_winding_drift"),
            ({"rings": [ring], "hasM": True}, "source_polygon_not_xy"),
        ]
        for geometry, error in cases:
            with self.subTest(error=error):
                with self.assertRaisesRegex(ValueError, error):
                    source._polygon_wkb(geometry)


class ParquetCandidateTests(unittest.TestCase):
    def test_parquet_retains_all_attributes_ids_and_polygon_wkb(self):
        first = {field["name"]: None for field in source.ATTRIBUTE_FIELDS}
        first.update(OBJECTID=1, Feature_ID="duplicate", Category_of_Gate="Lock Gate",
                     Scale_Minimum=22000)
        second = dict(first, OBJECTID=2)
        third = dict(first, OBJECTID=3, Feature_ID=None)
        ring = [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]
        features = [
            {"attributes": first, "geometry": {"rings": [ring]}},
            {"attributes": second, "geometry": {"rings": [ring]}},
            {"attributes": third, "geometry": {"rings": [ring]}},
        ]
        data = source._parquet_bytes(source.EXPECTED_FIELD_DECLARATIONS, features)
        table = pq.read_table(pa.BufferReader(data))
        self.assertEqual(table.column_names,
                         [field["name"] for field in source.ATTRIBUTE_FIELDS] + ["geometry"])
        self.assertEqual(table["OBJECTID"].to_pylist(), [1, 2, 3])
        self.assertEqual(table["Feature_ID"].to_pylist(), ["duplicate", "duplicate", None])
        self.assertEqual(table["Category_of_Gate"].to_pylist(),
                         ["Lock Gate", "Lock Gate", "Lock Gate"])
        self.assertEqual(str(table.schema.field("OBJECTID").type), "int64")
        self.assertEqual(str(table.schema.field("Scale_Minimum").type), "int32")
        self.assertEqual(str(table.schema.field("Feature_ID").type), "string")
        self.assertEqual(table["geometry"][0].as_py(), source._polygon_wkb({"rings": [ring]}))
        geo = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Polygon"])


class CaptureFunctionTests(unittest.TestCase):
    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_usace_lock_gate_areas(storage, version="../outside")

    def test_count_drift_quarantines_evidence_with_incomplete_manifest(self):
        docs = _docs()
        docs["count_after"]["count"] = source.EXPECTED_COUNT - 1
        bodies = {url: _body(docs[name])
                  for name, url in source._capture_urls(source.EXPECTED_IDS)}
        with tempfile.TemporaryDirectory() as tmp:
            capture_root = Path(tmp) / "captures"
            capture_root.mkdir()
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            with patch.object(source.shutil, "disk_usage") as usage, \
                    patch.object(source.httpx, "Client",
                                 lambda **kwargs: _FakeClient(bodies)):
                usage.return_value.free = 1_000_000_000
                with self.assertRaisesRegex(ValueError, "source_count_drift"):
                    source.capture_usace_lock_gate_areas(
                        storage, version="draft", capture_directory=str(capture_root))
            manifest = json.loads((capture_root / "capture_manifest.json").read_bytes())
            self.assertIs(manifest["complete"], False)
            self.assertEqual(manifest["error_code"], "ValueError")
            self.assertEqual(set(manifest["receipts"]), source.EXPECTED_CAPTURE_NAMES)
            quarantined = [key for key in storage.list_prefix()
                           if key.startswith(f"_quarantine/{source.DATASET_SLUG}/")]
            self.assertTrue(quarantined)
            base = sorted(key for key in quarantined if key.endswith("metadata/failure.json"))[0]
            base = base[: -len("metadata/failure.json")]
            failure = json.loads(storage.read_key(base + "metadata/failure.json"))
            self.assertEqual(failure["reason_code"], "ValueError")
            self.assertEqual(sorted(failure["captured_names"]),
                             sorted(source.EXPECTED_CAPTURE_NAMES))
            for name in source.EXPECTED_CAPTURE_NAMES:
                self.assertEqual(storage.read_key(f"{base}raw/{name}.body"), _body(docs[name]))
            for key in quarantined:
                self.assertFalse(key.startswith(f"{source.DATASET_SLUG}/"))

    def test_sealed_replay_rejects_re_pinned_wrong_origin_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            _sealed_capture_directory(root, _docs())
            receipt = json.loads((root / "ids_before.receipt.json").read_bytes())
            for field in ("url", "final_url"):
                receipt[field] = receipt[field].replace("ienccloud.us", "attacker.example")
            (root / "ids_before.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            manifest["receipts"]["ids_before"] = receipt
            manifest["artifact_roster"] = capture_roster_digests(
                root, exclude={"capture_manifest.json"})
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
            with self.assertRaisesRegex(ValueError,
                                        "retained_capture_request_identity_mismatch:ids_before"):
                source._read_replay(root)

    def test_fresh_capture_reuses_first_candidate_and_staged_evidence(self):
        docs = _docs()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            _sealed_capture_directory(root, docs)
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            pins = _fixture_pins(docs)
            with patch.multiple(source, **pins), \
                    patch.object(source.shutil, "disk_usage") as usage, \
                    patch.object(source, "validate_construction_code_identity") as identity_check, \
                    patch("dagster_hifld.sources.epa_frs_common."
                          "validate_construction_code_identity"):
                usage.return_value.free = 1_000_000_000
                first = source.capture_usace_lock_gate_areas(
                    storage, version="draft", capture_directory=str(root))
                identity_check.assert_called_once()
                args, kwargs = identity_check.call_args
                self.assertEqual(args[1], kwargs["identity"]["code_commit"])
                self.assertEqual(args[2], kwargs["identity"]["files"])
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
                manifest_before = storage.read_key(f"{prefix}/metadata/source_manifest.json")
                audit_before = storage.read_key(f"{prefix}/metadata/source_audit.json")
                receipts_before = {
                    name: storage.read_key(f"{prefix}/raw/{name}.receipt.json")
                    for name in source.EXPECTED_CAPTURE_NAMES}
                for name, receipt in json.loads(
                        (root / "capture_manifest.json").read_bytes())["receipts"].items():
                    receipt["observed_at"] = "2026-11-10T00:00:00Z"
                    (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
                capture_manifest = json.loads((root / "capture_manifest.json").read_bytes())
                capture_manifest["receipts"] = {
                    name: json.loads((root / f"{name}.receipt.json").read_bytes())
                    for name in capture_manifest["receipts"]}
                capture_manifest["artifact_roster"] = capture_roster_digests(
                    root, exclude={"capture_manifest.json"})
                (root / "capture_manifest.json").write_bytes(_canonical(capture_manifest) + b"\n")
                second = source.capture_usace_lock_gate_areas(
                    storage, version="draft", capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(second["feature_count"], source.EXPECTED_COUNT)
            self.assertEqual(second["source_population_complete"], True)
            self.assertFalse(second["release_accepted"])
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_manifest.json"),
                             manifest_before)
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_audit.json"),
                             audit_before)
            self.assertEqual({
                name: storage.read_key(f"{prefix}/raw/{name}.receipt.json")
                for name in source.EXPECTED_CAPTURE_NAMES}, receipts_before)


class PromotionRefusalTests(unittest.TestCase):
    def test_missing_approval_preserves_populated_accepted_inventory(self):
        with tempfile.TemporaryDirectory() as staging_dir, \
                tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          f"geoparquet/{source.FILE_SLUG}.parquet", b"candidate")
            accepted_key = published.write(
                source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                f"geoparquet/{source.FILE_SLUG}.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError,
                                            "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, source.DATASET_SLUG,
                                        source.FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)


class PackagedIdentityTests(unittest.TestCase):
    def test_packaged_code_identity_validates_without_git_metadata(self):
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            packaged_root = Path(tmp) / "runtime"
            packaged_root.mkdir()
            shutil.copytree(repo_root / "src/dagster_hifld", packaged_root / "src/dagster_hifld")
            for name in ("pyproject.toml", "uv.lock"):
                shutil.copy2(repo_root / name, packaged_root / name)
            self.assertFalse((packaged_root / ".git").exists())
            identity = processing_identity(packaged_root)
            self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
            self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
            validate_construction_code_identity(
                packaged_root, identity["code_commit"], identity["files"], identity=identity,
            )


if __name__ == "__main__":
    unittest.main()

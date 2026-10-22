"""Native preservation and completeness checks for the official IENC LATERAL_BUOY_POINT layer."""
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pyarrow
import pyarrow.parquet as pq
from shapely import from_wkb
from dagster import DefaultScheduleStatus

from dagster_hifld.sources import usace_lateral_buoys as source
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_lateral_buoys import (
    usace_lateral_buoys_complete, usace_lateral_buoys_draft_schedule, usace_lateral_buoys_usace_ienc,
)
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import _canonical


def _capture_docs(count=source.EXPECTED_COUNT):
    layer = {
        "id": source.LAYER_ID, "name": "LATERAL_BUOY_POINT", "geometryType": "esriGeometryPoint",
        "maxRecordCount": 2000, "fields": source.EXPECTED_FIELD_DECLARATIONS,
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
    }
    service = {"mapName": "LATERAL_BUOY_POINT", "copyrightText": "USACE IENC",
               "layers": [{"id": 15, "name": "LATERAL_BUOY_POINT", "geometryType": "esriGeometryPoint"}]}
    ids = list(range(1, count + 1))
    docs = {
        "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF", "access": "public",
                     "title": "Inland Electronic Navigational Charts", "description": "Official link https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/LATERAL_BUOY_POINT", "type": "MapServer"}]},
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "count_before": {"count": count}, "count_after": {"count": count},
        "ids_before": {"objectIds": ids}, "ids_after": {"objectIds": ids},
    }
    for page_idx in range((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
        page_ids = ids[page_idx * source.PAGE_SIZE:(page_idx + 1) * source.PAGE_SIZE]
        rows = []
        for oid in page_ids:
            rows.append({"attributes": {"OBJECTID": oid, "Category_of_Lateral_Mark": "Starboard Hand Lateral Mark",
                "Buoy_Shape": "Nun", "Color": "Red", "Object_Name": None, "Color_Pattern": None,
                "Status": None, "Information": None, "Scale_Minimum": 60000, "Source_Date": "20210303",
                "Source_Indication": "US,U3,LIGHT", "Source_Dataset": "U37AG001.000",
                "Feature_ID": "U3 0000003676 00001"},
                "geometry": {"x": -90.0 + oid / 10000, "y": 38.0 + oid / 10000,
                             "spatialReference": {"wkid": 4326, "latestWkid": 4326}}})
        docs[f"page_{page_idx:03d}"] = {"features": rows, "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
    return {name: (json.dumps(doc).encode(), {}) for name, doc in docs.items()}


class LateralBuoysTests(unittest.TestCase):
    def test_registered_stopped_draft_and_unapproved_release_gate(self):
        self.assertIn(usace_lateral_buoys_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_lateral_buoys_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        repository = defs.get_repository_def()
        self.assertIn(usace_lateral_buoys_draft_schedule.name, {item.name for item in defs.schedules})
        self.assertIn(usace_lateral_buoys_draft_schedule.name, {item.name for item in repository.schedule_defs})
        self.assertIn("usace_lateral_buoys_draft_refresh", {item.name for item in defs.jobs})
        self.assertEqual(
            set(repository.get_job("usace_lateral_buoys_draft_refresh").asset_layer.executable_asset_keys),
            {next(iter(usace_lateral_buoys_usace_ienc.keys))},
        )
        check_keys = {key.to_user_string() for key in defs.resolve_asset_graph().asset_check_keys}
        self.assertIn(
            "ingest/lateral-buoys-usace-ienc/lateral-buoys-usace-ienc:usace_lateral_buoys_complete",
            check_keys,
        )
        self.assertEqual(
            repository.get_schedule_def(usace_lateral_buoys_draft_schedule.name).default_status,
            DefaultScheduleStatus.STOPPED,
        )
        check = usace_lateral_buoys_complete({"feature_count": source.EXPECTED_COUNT, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["bts_item_succession_accepted"].value)
        self.assertFalse(check.metadata["coverage_accepted"].value)

    def test_capture_uses_strict_response_and_total_byte_guards(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            staging = Path(tmp) / "staging"
            staging.mkdir()
            with patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=1_000_000_000)), \
                    patch.object(source, "capture_get", return_value=(b"ok", {})) as capture_get:
                source._capture(object(), "https://example.test/layer", root, "page_000", staging=staging)
                self.assertEqual(capture_get.call_args.kwargs["max_bytes"], source.MAX_RESPONSE_BYTES)
                reserve = capture_get.call_args.kwargs["reserve"]
                before_retain = capture_get.call_args.kwargs["before_retain"]
                self.assertTrue(callable(reserve))
                self.assertTrue(callable(before_retain))
                with self.assertRaisesRegex(ValueError, "source_response_budget_exceeded:wire"):
                    before_retain("wire", b"x" * (source.MAX_RESPONSE_BYTES + 1))
                (root / "previous.body").write_bytes(b"x" * (source.MAX_CAPTURE_BYTES - 500_000))
                with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded"):
                    before_retain("body", b"x" * 500_001)
                with self.assertRaisesRegex(ValueError, "capture_budget_cannot_reserve_inflight_block"):
                    reserve(SimpleNamespace(reserve_bytes=source.CAPTURE_INFLIGHT_RESERVE_BYTES))

    def test_oversized_delivered_block_is_not_written_and_receipt_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            staging = Path(tmp) / "staging"
            staging.mkdir()
            payload = b"x" * (source.MAX_RESPONSE_BYTES + 1)

            def handler(request):
                return httpx.Response(200, stream=httpx.ByteStream(payload), request=request)

            with httpx.Client(transport=httpx.MockTransport(handler)) as client, \
                    patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=1_000_000_000)):
                with self.assertRaisesRegex(ValueError, "incomplete_source_capture:page_000"):
                    source._capture(client, "https://example.test/layer", root, "page_000", staging=staging)
            receipt = json.loads((root / "page_000.receipt.json").read_bytes())
            self.assertFalse(receipt["complete_body"])
            self.assertEqual(receipt["wire_bytes"], 0)
            self.assertEqual((root / "page_000.wire").stat().st_size, 0)
            self.assertEqual((root / "page_000.body").stat().st_size, 0)

    def test_replay_budget_rejects_oversized_artifacts_before_readback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            oversized = root / "page_000.body"
            with oversized.open("wb") as artifact:
                artifact.truncate(source.MAX_RESPONSE_BYTES + 1)
            with self.assertRaisesRegex(ValueError, "retained_capture_exceeds_response_byte_budget"):
                source._read_replay(root)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            for index in range(5):
                artifact = root / f"page_{index:03d}.body"
                with artifact.open("wb") as payload:
                    payload.truncate(source.MAX_RESPONSE_BYTES)
            with self.assertRaisesRegex(ValueError, "retained_capture_exceeds_total_byte_budget"):
                source._read_replay(root)

    def test_retained_capture_is_bound_to_exact_named_get_request(self):
        name, url = source._capture_roster(list(range(1, source.EXPECTED_COUNT + 1)))[0]
        receipt = {"method": "GET", "url": url, "final_url": url}
        source._validate_capture_request_identity(name, receipt, url)
        receipt["final_url"] = "https://example.invalid/wrong"
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
            source._validate_capture_request_identity(name, receipt, url)

    def test_complete_population_native_geometry_and_declared_parquet_types(self):
        fields, features, audit = source._validate(_capture_docs())
        self.assertEqual(len(features), source.EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], source._sha(source._canonical(list(range(1, source.EXPECTED_COUNT + 1)))))
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], source.EXPECTED_COUNT)
        parquet = source._parquet_bytes(fields, features)
        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual([f.name for f in table.schema], [f["name"] for f in source.ATTRIBUTE_FIELDS] + ["geometry"])
        self.assertTrue(table.schema.field("Category_of_Lateral_Mark").type.equals(pyarrow.string()))
        self.assertTrue(table.schema.field("Scale_Minimum").type.equals(pyarrow.int32()))
        geometry = from_wkb(table["geometry"][0].as_py())
        self.assertEqual(geometry.geom_type, "Point")
        self.assertEqual(geometry.x, features[0]["geometry"]["x"])
        self.assertEqual(geometry.y, features[0]["geometry"]["y"])

    def test_rejects_schema_and_population_drift(self):
        captures = _capture_docs()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"] = layer["fields"][:-1]
        captures["layer_before"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate(captures)
        captures = _capture_docs()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"] = [{**field, "length": 300} if field["name"] == "Color" else field
                           for field in layer["fields"]]
        captures["layer_before"] = (json.dumps(layer).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"].pop()
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
            source._validate(captures)
        captures = _capture_docs()
        ids = json.loads(captures["ids_before"][0])
        ids["objectIds"] = ids["objectIds"][:-1]
        captures["ids_before"] = (json.dumps(ids).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_count_or_id_population_drift"):
            source._validate(captures)

    def test_rejects_id_value_type_or_roster_loss(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["OBJECTID"] = "1"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_oid_value_type_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["Feature_ID"] = None
        captures["page_000"] = (json.dumps(page).encode(), {})
        fields, features, audit = source._validate(captures)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], None)
        self.assertEqual(audit["geometry_valid_count"], source.EXPECTED_COUNT)

    def test_rejects_geometry_or_declared_type_loss(self):
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["x"] = "-90"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_point_geometry_structure_drift"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["Scale_Minimum"] = "60000"
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_integer_value_type_drift:Scale_Minimum"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["z"] = 0.0
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_point_zm_dimensions_not_supported"):
            source._validate(captures)
        captures = _capture_docs()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["x"] = source.EXPECTED_EXTENT["xmax"] + 0.1
        captures["page_000"] = (json.dumps(page).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_point_outside_declared_extent"):
            source._validate(captures)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_usace_lateral_buoys(storage, version="../outside")

    def _capture_directory(self, root: Path):
        docs = {name: json.loads(body) for name, (body, _receipt) in _capture_docs().items()}
        receipts = {}
        expected_urls = dict(source._capture_roster(list(range(1, source.EXPECTED_COUNT + 1))))
        for name, doc in docs.items():
            body = _canonical(doc)
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            receipt = {"method": "GET", "url": expected_urls[name], "final_url": expected_urls[name],
                       "observed_at": "2026-10-22T00:00:00Z", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": source._sha(body), "bytes": len(body),
                       "sha256": source._sha(body), "content_encoding": "identity",
                       "content_length": len(body), "error_code": None}
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            receipts[name] = receipt
        (root / "capture_manifest.json").write_bytes(_canonical({"complete": True, "receipts": receipts,
            "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}) + b"\n")

    def test_incomplete_capture_is_quarantined_with_retained_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir(parents=True)
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))

            def fake_capture(_client, _url, capture_root, name, *, staging):
                body = _canonical({"objectIds": [1, 2, 3]} if name == "ids_before" else {})
                (capture_root / f"{name}.body").write_bytes(body)
                (capture_root / f"{name}.wire").write_bytes(body)
                receipt = {"method": "GET", "url": _url, "final_url": _url, "status": 200,
                           "complete_body": True, "bytes": len(body), "sha256": source._sha(body),
                           "wire_bytes": len(body), "wire_sha256": source._sha(body)}
                (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
                return body, receipt

            with patch.object(source.httpx, "Client"), patch.object(source, "_capture", fake_capture), \
                    patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=1_000_000_000)):
                with self.assertRaisesRegex(ValueError, "source_seed_id_population_drift"):
                    source.capture_usace_lateral_buoys(storage, version="draft", capture_directory=str(root))
            quarantine_roots = list((Path(tmp) / "staging" / "_quarantine" / source.DATASET_SLUG).iterdir())
            self.assertEqual(len(quarantine_roots), 1)
            failure = json.loads(storage.read_key(f"{quarantine_roots[0].relative_to(Path(tmp) / 'staging')}/metadata/failure.json"))
            self.assertEqual(failure["reason_code"], "ValueError")
            self.assertEqual(failure["captured_names"], ["ids_before"])
            self.assertIsNotNone(storage.read_key(f"{quarantine_roots[0].relative_to(Path(tmp) / 'staging')}/raw/ids_before.body"))

    def test_fresh_same_source_reuses_first_manifest_and_receipts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "capture"
            root.mkdir()
            self._capture_directory(root)
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            with patch.object(source, "validate_construction_code_identity") as code_identity_check, \
                    patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"), \
                    patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=1_000_000_000)):
                first = source.capture_usace_lateral_buoys(storage, version="draft", capture_directory=str(root))
            code_identity_check.assert_called_once()
            code_identity_args, code_identity_kwargs = code_identity_check.call_args
            self.assertEqual(code_identity_args[1], code_identity_kwargs["identity"]["code_commit"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            manifest_before = storage.read_key(f"{prefix}/metadata/source_manifest.json")
            parquet_before = storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet")
            receipts_before = {name: storage.read_key(f"{prefix}/raw/{name}.receipt.json")
                               for name in json.loads((root / "capture_manifest.json").read_bytes())["receipts"]}
            capture_manifest = json.loads((root / "capture_manifest.json").read_bytes())
            for name, receipt in capture_manifest["receipts"].items():
                receipt["observed_at"] = "2026-10-23T00:00:00Z"
                (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            capture_manifest["receipts"] = {n: json.loads((root / f"{n}.receipt.json").read_bytes())
                                            for n in capture_manifest["receipts"]}
            capture_manifest["artifact_roster"] = capture_roster_digests(root, exclude={"capture_manifest.json"})
            (root / "capture_manifest.json").write_bytes(_canonical(capture_manifest) + b"\n")
            with patch.object(source, "validate_construction_code_identity"), \
                    patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"), \
                    patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=1_000_000_000)):
                second = source.capture_usace_lateral_buoys(storage, version="draft", capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_manifest.json"), manifest_before)
            self.assertEqual(storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet"), parquet_before)
            self.assertEqual({name: storage.read_key(f"{prefix}/raw/{name}.receipt.json") for name in receipts_before},
                             receipts_before)
            self.assertEqual(second["source_capture_evidence_sha256"], first["source_capture_evidence_sha256"])


if __name__ == "__main__":
    unittest.main()

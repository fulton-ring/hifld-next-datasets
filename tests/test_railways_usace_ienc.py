import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.railways_usace_ienc import (
    railways_usace_ienc_complete, railways_usace_ienc_draft_schedule,
    railways_usace_ienc,
)
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.sources import railways_usace_ienc as source
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import _canonical


class RailwaysUsaceIencTests(unittest.TestCase):
    def _documents(self):
        service = {"mapName": "RAILROAD_LINE", "copyrightText": "USACE IENC",
                   "layers": [{"id": 45, "name": "RAILROAD_LINE", "geometryType": "esriGeometryPolyline"}]}
        layer = {"id": 45, "name": "RAILROAD_LINE", "geometryType": "esriGeometryPolyline",
                 "maxRecordCount": 2000, "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF",
                          "title": "Inland Electronic Navigational Charts",
                          "description": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/RAILROAD_LINE", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            "service_after": json.loads(json.dumps(service)), "layer_after": json.loads(json.dumps(layer)),
            "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS},
        }
        for page_index in range((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
            rows = []
            for oid in source.EXPECTED_IDS[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]:
                attrs = {field["name"]: oid if field["name"] == "OBJECTID" else
                         (15000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                         for field in source.ATTRIBUTE_FIELDS}
                rows.append({"attributes": attrs,
                             "geometry": {"paths": [[[-94.0 + oid / 1000, 35.5], [-93.9 + oid / 1000, 35.6]]]}})
            docs[f"page_{page_index:03d}"] = {"spatialReference": {"wkid": 4326}, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(railways_usace_ienc, hifld_ingest_assets)
        self.assertEqual(railways_usace_ienc_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = railways_usace_ienc_complete({"feature_count": 10572, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = railways_usace_ienc_complete({"feature_count": 10571, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         [f"page_{index:03d}" for index in range(43)])
        self.assertIn("objectIds=1%2C2%2C3", urls["page_000"])
        self.assertIn("objectIds=251%2C252%2C253", urls["page_001"])
        self.assertIn("objectIds=10501%2C10502%2C10503", urls["page_042"])
        self.assertNotIn("objectIds=10573", urls["page_042"])
        self.assertIn("returnZ=true", urls["page_000"])
        self.assertIn("returnM=true", urls["page_000"])
        self.assertEqual(len(urls["page_042"].split("objectIds=")[1].split("&")[0].split("%2C")), 72)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 10572)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 21144)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertIn("not a nationwide railways inventory", audit["source_coverage_scope"])

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_count_or_id_population_drift"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
            ("transfer", lambda d: d["page_001"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_002"].update(spatialReference={"wkid": 3857}), "crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                OBJECTID=d["page_000"]["features"][0]["attributes"]["OBJECTID"]),
             "source_page_id_population_drift"),
            ("ordinate", lambda d: d["page_000"]["features"][0]["geometry"]["paths"][0][0].append(20),
             "source_xy_ordinate_contract_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def test_vertex_domain_and_coverage_drift_fail_closed(self):
        def replace_vertex(docs, vertex, index):
            docs["page_000"]["features"][0]["geometry"]["paths"][0][index] = vertex
        cases = [
            ("out-of-range latitude", lambda d: replace_vertex(d, [-94.0, 100.0], 0),
             "source_xy_geographic_domain_violation"),
            ("in-range outside coverage", lambda d: replace_vertex(d, [0.0, 0.0], 1),
             "source_vertex_outside_declared_extent"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))
        boundary = json.loads(json.dumps(self._documents()))
        boundary["page_000"]["features"][0]["geometry"]["paths"] = [[
            [source.EXPECTED_EXTENT["xmin"], source.EXPECTED_EXTENT["ymin"]],
            [source.EXPECTED_EXTENT["xmax"], source.EXPECTED_EXTENT["ymax"]]]]
        _fields, _features, audit = source._validate(self._captures(boundary))
        self.assertEqual(audit["source_vertex_count"], 21144)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertIn("pinned-extent containment of every vertex", audit["drift_guard"])

    def _replay_directory(self, root, docs, mutate=None):
        documents = json.loads(json.dumps(docs))
        if mutate is not None:
            mutate(documents)
        root.mkdir(parents=True)
        roster_urls = dict(source._capture_roster(source.EXPECTED_IDS))
        receipts = {}
        for name in sorted(source.CAPTURE_NAMES):
            body = _canonical(documents[name])
            receipt = {"method": "GET", "url": roster_urls[name], "final_url": roster_urls[name],
                       "observed_at": "2026-11-11T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
                       "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                       "content_encoding": "identity", "content_length": len(body),
                       "error_code": None, "error_type": None}
            receipts[name] = receipt
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        manifest = {"complete": True, "captured_at": "2026-11-11T00:00:00+00:00",
                    "source_feature_count": source.EXPECTED_COUNT, "receipts": receipts,
                    "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}
        (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        return root

    def test_drifted_replay_seals_no_ready_candidate(self):
        def replace_vertex(docs, vertex, index):
            docs["page_000"]["features"][0]["geometry"]["paths"][0][index] = vertex
        cases = [
            ("out-of-range latitude", lambda d: replace_vertex(d, [-94.0, 100.0], 0),
             "source_xy_geographic_domain_violation"),
            ("in-range outside coverage", lambda d: replace_vertex(d, [0.0, 0.0], 1),
             "source_vertex_outside_declared_extent"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                root = self._replay_directory(Path(tmp) / "capture", self._documents(), mutate)
                with self.assertRaisesRegex(ValueError, expected):
                    source.capture_railways_usace_ienc(storage, version="regression",
                                                       capture_directory=str(root))
                self.assertEqual(storage.list_prefix(""), [])

    def test_valid_replay_seals_ready_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            root = self._replay_directory(Path(tmp) / "capture", self._documents())
            first = source.capture_railways_usace_ienc(storage, version="regression",
                                                       capture_directory=str(root))
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], source.EXPECTED_COUNT)
            self.assertTrue(first["source_population_complete"])
            self.assertFalse(first["release_accepted"])
            ready = json.loads(storage.read_key(first["candidate_ready"]))
            self.assertEqual(ready["version"], first["version"])
            self.assertFalse(ready["approved"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            staged = set(storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet", staged)
            self.assertIn(f"{prefix}/raw/page_000.body", staged)
            self.assertIn(f"{prefix}/metadata/candidate_ready.json", staged)
            audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
            self.assertEqual(audit["geometry_valid_count"], source.EXPECTED_COUNT)
            self.assertEqual(audit["geometry_invalid_count"], 0)
            second = source.capture_railways_usace_ienc(storage, version="regression",
                                                        capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_railways_usace_ienc(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.terminal_points_usace_ienc import (
    terminal_points_usace_ienc, terminal_points_usace_ienc_complete,
    terminal_points_usace_ienc_draft_schedule,
)
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import terminal_points_usace_ienc as source
from dagster_hifld.sources.epa_frs_common import _canonical


class TerminalPointsUsaceIencTests(unittest.TestCase):
    def _documents(self):
        service = {"mapName": "TERMINAL_POINT", "copyrightText": "USACE IENC",
                   "layers": [{"id": 1, "name": "TERMINAL_POINT", "geometryType": "esriGeometryPoint"}]}
        layer = {"id": 1, "name": "TERMINAL_POINT", "geometryType": "esriGeometryPoint",
                 "maxRecordCount": 2000, "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF",
                          "title": "Inland Electronic Navigational Charts",
                          "description": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes USACE_IENC_Master_Service_gdb.zip"},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/TERMINAL_POINT", "type": "MapServer"}]},
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
                         (18750 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                         for field in source.ATTRIBUTE_FIELDS}
                rows.append({"attributes": attrs,
                             "geometry": {"x": source.EXPECTED_EXTENT["xmin"] + oid / 10000,
                                          "y": source.EXPECTED_EXTENT["ymin"] + oid / 10000}})
            docs[f"page_{page_index:03d}"] = {"spatialReference": {"wkid": 4326}, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(terminal_points_usace_ienc, hifld_ingest_assets)
        self.assertEqual(terminal_points_usace_ienc_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = terminal_points_usace_ienc_complete({"feature_count": 16, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = terminal_points_usace_ienc_complete({"feature_count": 15, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         [f"page_{index:03d}" for index in range(1)])
        self.assertIn("objectIds=1%2C2%2C3", urls["page_000"])
        self.assertIn("%2C14%2C15%2C16&", urls["page_000"])
        self.assertNotIn("objectIds=17", urls["page_000"])
        self.assertIn("returnZ=true", urls["page_000"])
        self.assertIn("returnM=true", urls["page_000"])
        self.assertEqual(len(urls["page_000"].split("objectIds=")[1].split("&")[0].split("%2C")), 16)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 16)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 16)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertEqual(audit["feature_id_distinct_count"], 16)
        self.assertEqual(audit["feature_id_duplicate_values"], {})
        self.assertEqual(audit["scale_minimum_distinct_values"], [18750])
        self.assertIn("not national coverage", audit["source_coverage_scope"])

    def test_feature_id_duplicates_are_preserved_and_counted_not_rejected(self):
        docs = json.loads(json.dumps(self._documents()))
        docs["page_000"]["features"][1]["attributes"]["Feature_ID"] = "Feature_ID-1"
        _fields, features, audit = source._validate(self._captures(docs))
        self.assertEqual(len(features), 16)
        self.assertEqual(audit["feature_id_distinct_count"], 15)
        self.assertEqual(audit["feature_id_duplicate_values"], {"Feature_ID-1": 2})
        self.assertEqual(audit["feature_id_null_count"], 0)

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_count_or_id_population_drift"),
            ("schema", lambda d: d["layer_before"]["fields"][3].update(name="renamed"),
             "source_field_schema_drift"),
            ("transfer", lambda d: d["page_000"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_000"].update(spatialReference={"wkid": 3857}), "crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                OBJECTID=d["page_000"]["features"][0]["attributes"]["OBJECTID"]),
             "source_page_id_population_drift"),
            ("extra geometry key", lambda d: d["page_000"]["features"][0]["geometry"].update(z=20),
             "source_point_geometry_structure_drift"),
            ("missing coordinates", lambda d: d["page_000"]["features"][0]["geometry"].pop("y"),
             "source_point_coordinates_missing"),
            ("non-finite ordinate", lambda d: d["page_000"]["features"][0]["geometry"].update(x="west"),
             "source_xy_ordinate_contract_drift"),
            ("service layer id renamed away from doc id 1",
             lambda d: (d["service_before"]["layers"][0].update(id=45),
                        d["layer_before"].update(id=45), d["service_after"]["layers"][0].update(id=45),
                        d["layer_after"].update(id=45)),
             "official_terminal_point_service_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def test_point_domain_and_coverage_drift_fail_closed(self):
        def replace_point(docs, point):
            docs["page_000"]["features"][0]["geometry"].update(point)
        cases = [
            ("out-of-range longitude", lambda d: replace_point(d, {"x": 10000.0, "y": 35.0}),
             "source_xy_geographic_domain_violation"),
            ("in-range outside coverage", lambda d: replace_point(d, {"x": 0.0, "y": 0.0}),
             "source_vertex_outside_declared_extent"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))
        boundary = json.loads(json.dumps(self._documents()))
        boundary["page_000"]["features"][0]["geometry"] = {
            "x": source.EXPECTED_EXTENT["xmin"], "y": source.EXPECTED_EXTENT["ymin"]}
        boundary["page_000"]["features"][1]["geometry"] = {
            "x": source.EXPECTED_EXTENT["xmax"], "y": source.EXPECTED_EXTENT["ymax"]}
        _fields, _features, audit = source._validate(self._captures(boundary))
        self.assertEqual(audit["source_vertex_count"], 16)
        self.assertEqual(audit["source_vertex_bounds"]["xmin"], source.EXPECTED_EXTENT["xmin"])
        self.assertEqual(audit["source_vertex_bounds"]["ymax"], source.EXPECTED_EXTENT["ymax"])
        self.assertIn("every observed point against the declared layer coverage extent", audit["drift_guard"])

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
                       "observed_at": "2026-11-14T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
                       "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                       "content_encoding": "identity", "content_length": len(body),
                       "error_code": None, "error_type": None}
            receipts[name] = receipt
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        manifest = {"complete": True, "captured_at": "2026-11-14T00:00:00+00:00",
                    "source_feature_count": source.EXPECTED_COUNT, "receipts": receipts,
                    "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}
        (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        return root

    def test_drifted_replay_seals_no_ready_candidate(self):
        def move_point_outside(docs):
            docs["page_000"]["features"][0]["geometry"].update({"x": 0.0, "y": 0.0})
        cases = [
            ("outside coverage", move_point_outside, "source_vertex_outside_declared_extent"),
            ("renamed field", lambda d: d["layer_before"]["fields"][3].update(name="renamed"),
             "source_field_schema_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                root = self._replay_directory(Path(tmp) / "capture", self._documents(), mutate)
                with self.assertRaisesRegex(ValueError, expected):
                    source.capture_terminal_points_usace_ienc(storage, version="regression",
                                                              capture_directory=str(root))
                self.assertEqual(storage.list_prefix(""), [])

    def test_valid_replay_seals_ready_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            root = self._replay_directory(Path(tmp) / "capture", self._documents())
            first = source.capture_terminal_points_usace_ienc(storage, version="regression",
                                                              capture_directory=str(root))
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], source.EXPECTED_COUNT)
            self.assertEqual(first["source_vertex_count"], 16)
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
            self.assertEqual(audit["feature_id_distinct_count"], 16)
            self.assertEqual(audit["source_date_span"], None)
            second = source.capture_terminal_points_usace_ienc(storage, version="regression",
                                                               capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_terminal_points_usace_ienc(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

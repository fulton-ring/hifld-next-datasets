import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.wreck_areas_usace_ienc import (
    wreck_areas_usace_ienc, wreck_areas_usace_ienc_complete,
    wreck_areas_usace_ienc_draft_schedule,
)
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import wreck_areas_usace_ienc as source
from dagster_hifld.sources.esri_common import canonical as _canonical


class WreckAreasUsaceIencTests(unittest.TestCase):
    def _shell(self, oid):
        x0 = source.EXPECTED_EXTENT["xmin"] + ((oid - 1) % 100) * 0.003
        y0 = source.EXPECTED_EXTENT["ymin"] + ((oid - 1) // 100) * 0.003
        return [[x0, y0], [x0, y0 + 0.001], [x0 + 0.001, y0 + 0.001], [x0 + 0.001, y0], [x0, y0]]

    def _documents(self):
        service = {"mapName": "WRECKS_AREA", "copyrightText": "USACE IENC",
                   "serviceDescription": "The ruined remains of a stranded or sunken vessel which has "
                                         "been rendered useless. (IHO Dictionary, S-32, 5th Edition, 6027)",
                   "layers": [{"id": 54, "name": "WRECKS_AREA", "geometryType": "esriGeometryPolygon"}]}
        layer = {"id": 54, "name": "WRECKS_AREA", "geometryType": "esriGeometryPolygon",
                 "maxRecordCount": 2000, "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        rows = []
        for oid in source.EXPECTED_IDS:
            attrs = {field["name"]: oid if field["name"] == "OBJECTID" else
                     (45000 if field["name"] == "Scale_Minimum" else f"{field['name']}-{oid}")
                     for field in source.ATTRIBUTE_FIELDS}
            rings = [self._shell(oid)]
            if oid == source.EXPECTED_IDS[-1]:
                inset = 0.0002
                shell = rings[0]
                rings.append([[shell[0][0] + inset, shell[0][1] + inset],
                              [shell[2][0] - inset, shell[0][1] + inset],
                              [shell[2][0] - inset, shell[2][1] - inset],
                              [shell[0][0] + inset, shell[2][1] - inset],
                              [shell[0][0] + inset, shell[0][1] + inset]])
            rows.append({"attributes": attrs, "geometry": {"rings": rings}})
        features = {"geometryType": "esriGeometryPolygon", "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                    "fields": source.QUERY_FIELD_DECLARATIONS, "features": rows}
        return {
            "bts_item": {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF",
                         "title": "Inland Electronic Navigational Charts",
                         "description": "USACE updates. Feature classes are "
                                        "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes "
                                        "and USACE_IENC_Master_Service_gdb.zip."},
            "service_directory": {"services": [{"name": "IENC_Feature_Classes/WRECKS_AREA", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            "features": features, "layer_after": json.loads(json.dumps(layer)),
            "service_after": json.loads(json.dumps(service)),
            "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS},
        }

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(wreck_areas_usace_ienc, hifld_ingest_assets)
        self.assertEqual(wreck_areas_usace_ienc_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = wreck_areas_usace_ienc_complete({
            "feature_count": 8, "source_population_complete": True, "source_id_sha256": "abc",
            "geometry_valid_count": 8, "geometry_invalid_count": 0, "geometry_empty_count": 0})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = wreck_areas_usace_ienc_complete({
            "feature_count": 7, "source_population_complete": True, "source_id_sha256": "abc",
            "geometry_valid_count": 8, "geometry_invalid_count": 0, "geometry_empty_count": 0})
        self.assertFalse(failed.passed)
        geometry_failed = wreck_areas_usace_ienc_complete({
            "feature_count": 8, "source_population_complete": True, "source_id_sha256": "abc",
            "geometry_valid_count": 7, "geometry_invalid_count": 1, "geometry_empty_count": 0})
        self.assertFalse(geometry_failed.passed)
        missing = wreck_areas_usace_ienc_complete({"feature_count": 8, "source_population_complete": True})
        self.assertFalse(missing.passed)

    def test_capture_roster_covers_bts_service_layer_and_exact_ids(self):
        urls = dict(source._capture_urls(source.EXPECTED_IDS))
        self.assertEqual(sorted(urls), sorted(source.EXPECTED_CAPTURE_NAMES))
        self.assertEqual(urls["layer_before"],
                         "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes/WRECKS_AREA"
                         "/MapServer/54?f=pjson")
        self.assertIn("objectIds=1%2C2%2C3%2C4%2C5%2C6%2C7%2C8", urls["features"])
        self.assertIn("outSR=4326", urls["features"])
        self.assertIn("returnZ=true", urls["features"])
        self.assertIn("returnM=true", urls["features"])
        self.assertIn("orderByFields=OBJECTID+ASC", urls["features"])
        self.assertIn("ienccloud.us", urls["service_directory"])
        self.assertIn("arcgis.com", urls["bts_item"])

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 8)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 45)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], 8)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["feature_id_distinct_count"], 8)
        self.assertEqual(audit["feature_id_duplicate_values"], {})
        self.assertEqual(audit["feature_id_null_count"], 0)
        self.assertEqual(features[0]["attributes"]["Feature_ID"], "Feature_ID-1")
        self.assertEqual(features[0]["attributes"]["Category_of_Wreck"], "Category_of_Wreck-1")
        self.assertEqual(audit["scale_minimum_distinct_values"], [45000])
        self.assertEqual(audit["source_date_span"],
                         {"min": "Source_Date-1", "max": "Source_Date-8"})
        self.assertEqual(audit["source_vertex_bounds"]["xmin"], source.EXPECTED_EXTENT["xmin"])
        self.assertEqual(audit["source_vertex_bounds"]["ymin"], source.EXPECTED_EXTENT["ymin"])
        self.assertIn("not a nationwide wreck inventory", audit["source_coverage_scope"])

    def test_polygon_wkb_is_xy_only_and_preserves_ring_structure(self):
        shell = self._shell(1)
        single = source._esri_polygon_wkb({"rings": [shell]})
        self.assertEqual(single[:5], b"\x01\x03\x00\x00\x00")
        disjoint = source._esri_polygon_wkb({"rings": [shell, self._shell(2)]})
        self.assertEqual(disjoint[:5], b"\x01\x06\x00\x00\x00")
        inset = 0.0002
        hole = [[shell[0][0] + inset, shell[0][1] + inset],
                [shell[2][0] - inset, shell[0][1] + inset],
                [shell[2][0] - inset, shell[2][1] - inset],
                [shell[0][0] + inset, shell[2][1] - inset],
                [shell[0][0] + inset, shell[0][1] + inset]]
        with_hole = source._esri_polygon_wkb({"rings": [shell, hole]})
        self.assertEqual(with_hole[:5], b"\x01\x03\x00\x00\x00")
        for flags in ({"hasZ": True}, {"hasM": True}, {"hasZ": True, "hasM": True}):
            with self.assertRaisesRegex(ValueError, "source_polygon_dimensional_flags_rejected"):
                source._esri_polygon_wkb({"rings": [shell], **flags})
        unflagged = [[0.0, 0.0, 1.0], [0.0, 1.0, 1.0], [1.0, 1.0, 1.0], [1.0, 0.0, 1.0], [0.0, 0.0, 1.0]]
        with self.assertRaisesRegex(ValueError, "source_polygon_zm_flags_do_not_match_coordinates"):
            source._esri_polygon_wkb({"rings": [unflagged]})

    def test_feature_id_duplicates_are_preserved_and_counted_not_rejected(self):
        docs = json.loads(json.dumps(self._documents()))
        docs["features"]["features"][1]["attributes"]["Feature_ID"] = "Feature_ID-1"
        _fields, features, audit = source._validate(self._captures(docs))
        self.assertEqual(len(features), 8)
        self.assertEqual(audit["feature_id_distinct_count"], 7)
        self.assertEqual(audit["feature_id_duplicate_values"], {"Feature_ID-1": 2})
        self.assertEqual(audit["feature_id_null_count"], 0)

    def test_population_schema_transfer_limit_and_geometry_drift_fail_closed(self):
        def far_hole(docs):
            docs["features"]["features"][0]["geometry"]["rings"].append(
                [[source.EXPECTED_EXTENT["xmax"] - 0.01, source.EXPECTED_EXTENT["ymin"] + 0.01],
                 [source.EXPECTED_EXTENT["xmax"] - 0.01, source.EXPECTED_EXTENT["ymin"] + 0.02],
                 [source.EXPECTED_EXTENT["xmax"] - 0.02, source.EXPECTED_EXTENT["ymin"] + 0.02],
                 [source.EXPECTED_EXTENT["xmax"] - 0.02, source.EXPECTED_EXTENT["ymin"] + 0.01],
                 [source.EXPECTED_EXTENT["xmax"] - 0.01, source.EXPECTED_EXTENT["ymin"] + 0.01]])

        def reversed_shell(docs):
            docs["features"]["features"][0]["geometry"]["rings"][
                0] = list(reversed(docs["features"]["features"][0]["geometry"]["rings"][0]))

        def duplicate_oid(docs):
            attrs = docs["features"]["features"][0]["attributes"]
            docs["features"]["features"][1]["attributes"].update(OBJECTID=attrs["OBJECTID"])

        def move_point(docs, point):
            docs["features"]["features"][0]["geometry"]["rings"][0][1] = point

        cases = [
            ("population", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_count_or_id_population_drift"),
            ("count", lambda d: d["count_after"].update(count=7), "source_count_or_id_population_drift"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
            ("page field schema", lambda d: d["features"]["fields"][2].update(name="renamed"),
             "source_page_field_schema_drift"),
            ("service identity", lambda d: d["service_before"].update(mapName="DRIFTED"),
             "official_wreck_area_service_drift"),
            ("service description", lambda d: d["service_before"].update(
                serviceDescription="drifted"), "official_wreck_area_description_drift"),
            ("bts link", lambda d: d["bts_item"].update(
                description="no linked service"), "bts_ienc_official_distribution_link_drift"),
            ("bts identity", lambda d: d["bts_item"].update(owner="SOMEONE_ELSE"),
             "bts_ienc_item_identity_drift"),
            ("layer identity", lambda d: d["layer_before"].update(id=55),
             "source_layer_identity_extent_or_limit_drift"),
            ("layer extent", lambda d: d["layer_before"].update(
                extent={**source.EXPECTED_EXTENT, "xmin": -90.0, "spatialReference": {"wkid": 4326}}),
             "source_layer_identity_extent_or_limit_drift"),
            ("layer dimensional", lambda d: d["layer_before"].update(hasZ=True),
             "source_layer_dimensional_drift"),
            ("page dimensional", lambda d: d["features"].update(hasZ=True),
             "source_page_dimensional_drift"),
            ("geometry dimensional", lambda d: d["features"]["features"][0]["geometry"].update(hasZ=True),
             "source_geometry_dimensional_drift"),
            ("transfer", lambda d: d["features"].update(exceededTransferLimit=True),
             "source_feature_query_error_or_truncation"),
            ("page CRS", lambda d: d["features"].update(spatialReference={"wkid": 3857}), "crs_drift"),
            ("feature CRS", lambda d: d["features"]["features"][0]["geometry"].update(
                spatialReference={"wkid": 3857}), "crs_drift"),
            ("page geometry type", lambda d: d["features"].update(geometryType="esriGeometryPoint"),
             "source_page_geometry_type_drift"),
            ("extra attribute", lambda d: d["features"]["features"][0]["attributes"].update(Extra="x"),
             "source_attribute_roster_or_order_drift"),
            ("integer type", lambda d: d["features"]["features"][0]["attributes"].update(
                Scale_Minimum="45000"), "source_integer_value_type_drift"),
            ("string type", lambda d: d["features"]["features"][0]["attributes"].update(Feature_ID=17),
             "source_string_value_type_drift"),
            ("oid roster", lambda d: d["features"]["features"][0]["attributes"].update(OBJECTID=99),
             "source_oid_roster_drift"),
            ("duplicate oid", duplicate_oid, "source_feature_id_population_drift"),
            ("open ring", lambda d: d["features"]["features"][0]["geometry"]["rings"][0].pop(),
             "source_polygon_ring_not_closed"),
            ("extra ordinate", lambda d: d["features"]["features"][0]["geometry"]["rings"][0][1].append(20),
             "source_xy_ordinate_contract_drift"),
            ("non-finite ordinate", lambda d: move_point(d, ["west", 37.3]),
             "source_xy_ordinate_contract_drift"),
            ("geographic domain", lambda d: move_point(d, [10000.0, 37.3]),
             "source_xy_geographic_domain_violation"),
            ("outside coverage", lambda d: move_point(d, [0.0, 0.0]),
             "source_vertex_outside_declared_extent"),
            ("hole without shell", far_hole, "source_hole_ownership_ambiguous"),
            ("shell missing", reversed_shell, "source_polygon_shell_missing"),
            ("layer changed during capture", lambda d: d["layer_after"].update(id=55),
             "source_service_or_layer_changed_during_capture"),
            ("service changed during capture", lambda d: d["service_after"].update(mapName="DRIFTED"),
             "source_service_or_layer_changed_during_capture"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def _replay_directory(self, root, docs, mutate=None):
        documents = json.loads(json.dumps(docs))
        if mutate is not None:
            mutate(documents)
        root.mkdir(parents=True)
        roster_urls = dict(source._capture_urls(source.EXPECTED_IDS))
        receipts = {}
        for name in sorted(source.EXPECTED_CAPTURE_NAMES):
            body = _canonical(documents[name])
            receipt = {"method": "GET", "url": roster_urls[name], "final_url": roster_urls[name],
                       "observed_at": "2026-11-15T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": source._sha(body),
                       "bytes": len(body), "sha256": source._sha(body),
                       "content_encoding": "identity", "content_length": len(body),
                       "error_code": None, "error_type": None}
            receipts[name] = receipt
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        manifest = {"complete": True, "captured_at": "2026-11-15T00:00:00+00:00",
                    "source_feature_count": source.EXPECTED_COUNT, "receipts": receipts,
                    "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}
        (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        return root

    def test_drifted_replay_seals_no_ready_candidate(self):
        def move_vertex_outside(docs):
            docs["features"]["features"][0]["geometry"]["rings"][0][1] = [0.0, 0.0]
        cases = [
            ("outside coverage", move_vertex_outside, "source_vertex_outside_declared_extent"),
            ("renamed field", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                root = self._replay_directory(Path(tmp) / "capture", self._documents(), mutate)
                with self.assertRaisesRegex(ValueError, expected):
                    source.capture_wreck_areas_usace_ienc(storage, version="regression",
                                                          capture_directory=str(root))
                self.assertEqual(storage.list_prefix(""), [])

    def test_valid_replay_seals_ready_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            root = self._replay_directory(Path(tmp) / "capture", self._documents())
            with patch.object(source, "validate_construction_code_identity"), \
                 patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                first = source.capture_wreck_areas_usace_ienc(storage, version="regression",
                                                              capture_directory=str(root))
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], source.EXPECTED_COUNT)
            self.assertEqual(first["source_vertex_count"], 45)
            self.assertEqual(first["geometry_valid_count"], 8)
            self.assertEqual(first["geometry_invalid_count"], 0)
            self.assertEqual(first["geometry_empty_count"], 0)
            self.assertTrue(first["source_population_complete"])
            self.assertFalse(first["release_accepted"])
            ready = json.loads(storage.read_key(first["candidate_ready"]))
            self.assertEqual(ready["version"], first["version"])
            self.assertFalse(ready["approved"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            staged = set(storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet", staged)
            self.assertIn(f"{prefix}/raw/features.body", staged)
            self.assertIn(f"{prefix}/raw/bts_item.receipt.json", staged)
            self.assertIn(f"{prefix}/metadata/candidate_ready.json", staged)
            audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
            self.assertEqual(audit["feature_id_distinct_count"], 8)
            self.assertEqual(audit["source_date_span"],
                             {"min": "Source_Date-1", "max": "Source_Date-8"})
            self.assertEqual(audit["source_date_span"]["max"], "Source_Date-8")
            manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
            self.assertEqual(manifest["title"], "Wreck Areas (USACE IENC)")
            self.assertFalse(manifest["approved"])
            with patch.object(source, "validate_construction_code_identity"), \
                 patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                second = source.capture_wreck_areas_usace_ienc(storage, version="regression",
                                                               capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_wreck_areas_usace_ienc(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

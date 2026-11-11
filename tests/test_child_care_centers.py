import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.child_care_centers import (
    child_care_centers, child_care_centers_complete, child_care_centers_draft_schedule,
)
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import child_care_centers as source
from dagster_hifld.sources.epa_frs_common import _canonical


def _sha(payload):
    return hashlib.sha256(_canonical(payload)).hexdigest()


class ChildCareCentersTests(unittest.TestCase):
    def _service(self):
        return {"mapName": source.EXPECTED_SERVICE_MAP_NAME,
                "copyrightText": source.EXPECTED_SERVICE_COPYRIGHT,
                "documentInfo": {"Title": source.EXPECTED_SERVICE_TITLE,
                                 "Subject": source.EXPECTED_SERVICE_SUBJECT},
                "layers": [{"id": layer_id, "name": name, "geometryType": geometry}
                           for layer_id, name, geometry in source.EXPECTED_SERVICE_LAYERS]}

    def _layer(self):
        return {"id": source.LAYER_ID, "name": source.LAYER_NAME,
                "geometryType": "esriGeometryPoint", "maxRecordCount": source.PAGE_SIZE,
                "advancedQueryCapabilities": {"supportsOrderBy": True, "supportsPagination": True},
                "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326}},
                "fields": source.EXPECTED_FIELD_DECLARATIONS}

    def _documents(self, count):
        service, layer = self._service(), self._layer()
        docs = {
            "service_directory": {"services": [{"name": "OEI/ORNL_Education", "type": "MapServer"}]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": count},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": list(range(1, count + 1))},
            "service_after": service, "layer_after": layer,
            "count_after": {"count": count},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": list(range(1, count + 1))},
        }
        rows = []
        for oid in range(1, count + 1):
            attrs = {}
            for field in source.ATTRIBUTE_FIELDS:
                name, kind = field["name"], field["type"]
                if kind == "esriFieldTypeOID":
                    attrs[name] = oid
                elif name == "ID":
                    attrs[name] = f"000018479{min(oid, 2)}"
                elif kind == "esriFieldTypeInteger":
                    attrs[name] = oid * 10
                elif kind == "esriFieldTypeSingle":
                    attrs[name] = 37.5 if name == "LATITUDE" else -113.25
                elif kind == "esriFieldTypeDate":
                    attrs[name] = 1262563200000
                elif name == "STATE":
                    attrs[name] = "UT" if oid % 2 else "CA"
                else:
                    attrs[name] = f"{name}-{oid}"
            rows.append({"attributes": attrs, "geometry": {"x": -113.0 - oid * 0.01, "y": 37.0 + oid * 0.01}})
        for page_index, start in enumerate(range(0, count, source.PAGE_SIZE)):
            docs[f"page_{page_index:03d}"] = {
                "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                "features": rows[start:start + source.PAGE_SIZE]}
        return docs

    def _captures(self, docs):
        return {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}

    def _validate_synthetic(self, count=6):
        return source._validate(self._captures(self._documents(count)),
                                expected_count=count,
                                expected_id_sha256=_sha(list(range(1, count + 1))))

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(child_care_centers, hifld_ingest_assets)
        self.assertEqual(child_care_centers_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = child_care_centers_complete(
            {"feature_count": source.EXPECTED_COUNT, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        for failed_candidate in ({"feature_count": source.EXPECTED_COUNT - 1, "source_population_complete": True},
                                 {"feature_count": source.EXPECTED_COUNT, "source_population_complete": False},
                                 {"feature_count": source.EXPECTED_COUNT}):
            self.assertFalse(child_care_centers_complete(failed_candidate).passed)

    def test_pinned_source_population_constants_are_consistent(self):
        self.assertEqual(source.EXPECTED_COUNT, len(source.EXPECTED_IDS))
        self.assertEqual(source.EXPECTED_ID_SHA256, _sha(sorted(source.EXPECTED_IDS)))
        self.assertEqual(len(source.EXPECTED_FIELD_DECLARATIONS), 29)
        self.assertEqual(len(source.ATTRIBUTE_FIELDS), 28)
        self.assertEqual(source.CAPTURE_NAMES,
                         {"service_directory", "service_before", "layer_before", "count_before",
                          "ids_before", "layer_after", "count_after", "ids_after", "service_after"}
                         | {f"page_{index:03d}" for index in range(116)})

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         [f"page_{index:03d}" for index in range(116)])
        self.assertIn("resultOffset=0", urls["page_000"])
        self.assertIn("resultRecordCount=1000", urls["page_000"])
        self.assertIn("orderByFields=OBJECTID+ASC", urls["page_000"])
        self.assertIn("outSR=4326", urls["page_000"])
        self.assertIn("resultOffset=115000", urls["page_115"])
        self.assertEqual("115036", source.EXPECTED_IDS[-1].__str__())
        self.assertNotIn("resultOffset=116000", urls["page_115"])
        self.assertEqual(urls["service_directory"], source.SERVICE_DIRECTORY_URL)
        self.assertIn("returnCountOnly=true", urls["count_before"])
        self.assertIn("returnIdsOnly=true", urls["ids_before"])

    def test_full_nonfinal_pages_may_flag_transfer_limit_but_final_pages_may_not(self):
        full_docs = self._documents(1500)
        full_docs["page_000"]["exceededTransferLimit"] = True
        fields, features, audit = source._validate(
            self._captures(full_docs), expected_count=1500,
            expected_id_sha256=_sha(list(range(1, 1501))))
        self.assertEqual(audit["source_feature_count"], 1500)

        docs = json.loads(json.dumps(self._documents(1500)))
        docs["page_001"]["exceededTransferLimit"] = True
        with self.assertRaisesRegex(ValueError, "source_page_error_or_transfer_limit"):
            source._validate(self._captures(docs), expected_count=1500,
                             expected_id_sha256=_sha(list(range(1, 1501))))

        docs = json.loads(json.dumps(self._documents(6)))
        docs["layer_before"]["advancedQueryCapabilities"] = {"supportsOrderBy": True, "supportsPagination": False}
        with self.assertRaisesRegex(ValueError, "source_layer_identity_extent_or_limit_drift"):
            source._validate(self._captures(docs), expected_count=6,
                             expected_id_sha256=_sha(list(range(1, 7))))

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = self._validate_synthetic()
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 6)
        self.assertEqual(audit["source_feature_count"], 6)
        self.assertEqual(audit["source_id_sha256"], _sha(list(range(1, 7))))
        self.assertEqual(audit["geometry_valid_count"], 6)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["state_roster"], ["CA", "UT"])
        self.assertEqual(audit["provider_id_distinct_count"], 2)
        self.assertEqual(audit["provider_id_duplicate_count"], 4)
        self.assertIn("2015 ORNL Freedom edition", audit["source_coverage_scope"])
        self.assertIn("retired 2021-06-24 to 2022-03-01 HIFLD edition", audit["source_coverage_scope"])
        fields2, features2, audit2 = self._validate_synthetic()
        self.assertEqual(audit["source_record_sha256"], audit2["source_record_sha256"])
        self.assertEqual(audit["source_geometry_sha256"], audit2["source_geometry_sha256"])

    def test_point_wkb_is_little_endian_xy(self):
        wkb = source._point_wkb(-113.25, 37.5)
        self.assertEqual(struct.unpack("<BI", wkb[:5]), (1, 1))
        self.assertEqual(struct.unpack("<dd", wkb[5:]), (-113.25, 37.5))

    def test_parquet_candidate_retains_native_schema_and_ids(self):
        fields, features, audit = self._validate_synthetic()
        table_bytes = source._parquet_bytes(fields, features)
        self.assertGreater(len(table_bytes), 0)
        import io
        import pyarrow.parquet as pq
        table = pq.read_table(io.BytesIO(table_bytes))
        self.assertEqual(table.num_rows, 6)
        self.assertEqual(table.column_names,
                         [field["name"] for field in source.EXPECTED_FIELD_DECLARATIONS])
        self.assertEqual(table.column("OBJECTID").to_pylist(), [1, 2, 3, 4, 5, 6])
        self.assertEqual(table.column("ID").to_pylist(), ["0000184791", "0000184792", "0000184792",
                                                          "0000184792", "0000184792", "0000184792"])
        self.assertEqual(table.column("POPULATION").to_pylist(), [10, 20, 30, 40, 50, 60])
        geometry = table.column("Shape").to_pylist()
        for oid, wkb in enumerate(geometry, start=1):
            self.assertEqual(struct.unpack("<BI", wkb[:5]), (1, 1))
            self.assertEqual(struct.unpack("<dd", wkb[5:]), (-113.0 - oid * 0.01, 37.0 + oid * 0.01))

    def _drift_cases(self):
        return [
            ("count drift", lambda d: d["count_after"].update(count=5),
             "source_count_or_id_population_drift"),
            ("id roster drift", lambda d: d["ids_after"].update(objectIds=[1, 2, 3, 4, 5, 7]),
             "source_count_or_id_population_drift"),
            ("schema drift", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
            ("service identity drift", lambda d: d["service_before"].update(mapName="Other"),
             "epa_ornl_education_service_identity_drift"),
            ("directory drift", lambda d: d["service_directory"].update(services=[]),
             "epa_oeni_service_directory_drift"),
            ("layer limit drift", lambda d: d["layer_before"].update(maxRecordCount=2000),
             "source_layer_identity_extent_or_limit_drift"),
            ("layer changed during capture", lambda d: d["layer_after"].update(maxRecordCount=2000),
             "source_layer_metadata_changed_during_capture"),
            ("service changed during capture", lambda d: d["service_after"].update(mapName="Other"),
             "source_service_metadata_changed_during_capture"),
            ("transfer limit", lambda d: d["page_000"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("page crs", lambda d: d["page_000"].update(spatialReference={"wkid": 3857}),
             "crs_drift"),
            ("layer dimensional", lambda d: d["layer_before"].update(hasZ=True),
             "source_layer_dimensional_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(OBJECTID=1),
             "source_page_id_population_drift"),
            ("attribute roster", lambda d: d["page_000"]["features"][0]["attributes"].update(extra=1),
             "source_attribute_roster_or_order_drift"),
            ("string type", lambda d: d["page_000"]["features"][0]["attributes"].update(NAME=7),
             "source_string_value_type_drift"),
            ("integer type", lambda d: d["page_000"]["features"][0]["attributes"].update(POPULATION="many"),
             "source_integer_value_type_drift"),
            ("single precision loss", lambda d: d["page_000"]["features"][0]["attributes"].update(LATITUDE=37.12345678901234),
             "source_single_precision_loss"),
            ("date type", lambda d: d["page_000"]["features"][0]["attributes"].update(SOURCE_DATE="2010"),
             "source_date_value_type_drift"),
            ("geometry structure", lambda d: d["page_000"]["features"][0]["geometry"].update(z=0.0),
             "source_point_geometry_structure_drift"),
            ("missing geometry", lambda d: d["page_000"]["features"][0].update(geometry=None),
             "source_point_geometry_structure_drift"),
            ("outside coverage", lambda d: d["page_000"]["features"][0]["geometry"].update(x=-170.0, y=12.0),
             "source_geometry_outside_declared_coverage"),
            ("page population", lambda d: d["page_000"]["features"].pop(),
             "source_page_population_incomplete"),
        ]

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        for label, mutate, expected in self._drift_cases():
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents(6)))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs), expected_count=6,
                                     expected_id_sha256=_sha(list(range(1, 7))))

    def test_capture_name_roster_drift_fails_closed(self):
        docs = self._documents(6)
        docs.pop("service_after")
        with self.assertRaisesRegex(ValueError, "source_capture_name_roster_drift"):
            source._validate(self._captures(docs), expected_count=6,
                             expected_id_sha256=_sha(list(range(1, 7))))

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_child_care_centers(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

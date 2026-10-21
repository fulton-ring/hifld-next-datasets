import copy
import hashlib
import json
import unittest
from unittest.mock import patch

from dagster_hifld.assets.coastlines_usace_ienc import coastlines_usace_ienc_complete
from dagster_hifld.sources import coastlines_usace_ienc as source


def _body(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _capture_fixture():
    ids = [1, 2, 3]
    identity = source._query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})
    layer_url = source.LAYER_URL
    fields = [
        {"name": name, "type": kind, "alias": name, "domain": None,
         **({"length": 254} if kind == "esriFieldTypeString" else {})}
        for name, kind in source.EXPECTED_FIELDS
    ]
    sr = {"wkid": 4326, "latestWkid": 4326}
    layer = {"name": "COASTLINE_LINE", "id": 33, "geometryType": "esriGeometryPolyline",
             "fields": fields, "spatialReference": sr, "maxRecordCount": 2000,
             "extent": {"xmin": -180, "ymin": -90, "xmax": 180, "ymax": 90,
                        "spatialReference": sr}}
    service = {"mapName": "COASTLINE_LINE", "copyrightText": "USACE IENC",
               "layers": [{"id": 33, "name": "COASTLINE_LINE", "geometryType": "esriGeometryPolyline"}]}
    item = {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS",
            "title": "Inland Electronic Navigational Charts",
            "description": "Inland Electronic Navigational Charts updated bi-monthly. "
                           + source.SERVICE_DIRECTORY_URL.split("?")[0]
                           + " USACE_IENC_Master_Service_gdb.zip"}
    rows = []
    for oid in ids:
        rows.append({"attributes": {"OBJECTID": oid, "Category_of_Coastline": "shore",
                       "Scale_Minimum": "1", "Source_Date": "2021", "Source_Indication": "chart",
                       "Source_Dataset": "IENC", "Feature_ID": f"id-{oid}"},
                     "geometry": {"paths": [[[float(oid), 1.0], [float(oid) + 0.5, 1.5]]],
                                  "hasZ": False, "hasM": False,
                                  "spatialReference": {"latestWkid": 4326}}})
    docs = {
        "bts_item": item,
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/COASTLINE_LINE", "type": "MapServer"}]},
        "service_before": service, "service_after": copy.deepcopy(service),
        "layer_before": layer, "layer_after": copy.deepcopy(layer),
        "count_before": {"count": len(ids)}, "count_after": {"count": len(ids)},
        "ids_before": {"objectIds": ids}, "ids_after": {"objectIds": ids}, "ids_seed": {"objectIds": ids},
        "page_000": {"spatialReference": sr, "features": rows[:2], "exceededTransferLimit": True},
        "page_001": {"spatialReference": sr, "features": rows[2:], "exceededTransferLimit": False},
    }
    with patch.multiple(source, EXPECTED_COUNT=3, EXPECTED_IDS=ids,
                        EXPECTED_ID_SHA256=hashlib.sha256(source._canonical(ids)).hexdigest(), PAGE_SIZE=2):
        names = ["ids_seed", *[name for name, _url in source._capture_roster(ids)]]
    # `_capture_roster` uses its module-level count only through the supplied IDs.
    return {name: (_body(docs[name]), {}) for name in names}, docs


class CoastlinesSourceValidationTests(unittest.TestCase):
    def test_registered_completeness_check_exposes_capture_evidence_digest(self):
        check_fn = coastlines_usace_ienc_complete.node_def.compute_fn.decorated_fn
        result = check_fn({
            "feature_count": source.EXPECTED_COUNT,
            "source_population_complete": True,
            "source_capture_evidence_sha256": "a" * 64,
        })
        self.assertTrue(result.passed)
        self.assertEqual(result.metadata["source_capture_evidence_sha256"].value, "a" * 64)

    def _validate(self, mutate=None):
        captures, docs = _capture_fixture()
        if mutate:
            mutate(docs)
        captures = {name: (_body(docs[name]), {}) for name in captures}
        with patch.multiple(source, EXPECTED_COUNT=3, EXPECTED_IDS=[1, 2, 3],
                            EXPECTED_ID_SHA256=hashlib.sha256(source._canonical([1, 2, 3])).hexdigest(), PAGE_SIZE=2):
            return source._validate(captures)

    def test_accepts_complete_patched_count_fixture_and_nonfinal_transfer_flag(self):
        _fields, features, audit = self._validate()
        self.assertEqual(len(features), 3)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], 3)
        self.assertIn(source.BTS_ITEM_ID, audit["drift_guard"])
        self.assertIn("3607d485a9854d6fa458bf507058ceae", audit["drift_guard"])

    def test_capture_roster_rejects_duplicate_names_before_receipt_mapping(self):
        roster = source._capture_roster([1, 2, 3])
        names = [name for name, _url in roster]
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual(names.count("service_before"), 1)
        self.assertEqual(names.count("service_after"), 1)
        with self.assertRaisesRegex(ValueError, "source_capture_roster_duplicate_name"):
            source._assert_unique_capture_roster([
                ("service_before", "https://example.test/first"),
                ("service_before", "https://example.test/second"),
            ])

    def test_rejects_final_page_transfer_limit_flag(self):
        with self.assertRaisesRegex(ValueError, "source_page_transfer_limit_truncated"):
            self._validate(lambda docs: docs["page_001"].update(exceededTransferLimit=True))

    def test_rejects_duplicate_or_missing_oid_population(self):
        def duplicate(docs):
            docs["page_001"]["features"][0]["attributes"]["OBJECTID"] = 2
        with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
            self._validate(duplicate)

    def test_rejects_missing_oid_attribute(self):
        def missing(docs):
            del docs["page_000"]["features"][0]["attributes"]["OBJECTID"]
        with self.assertRaisesRegex(ValueError, "source_record_schema_or_geometry_drift"):
            self._validate(missing)

    def test_rejects_field_schema_drift(self):
        def mutate(docs):
            docs["layer_before"]["fields"][2]["type"] = "esriFieldTypeInteger"
            docs["layer_after"]["fields"][2]["type"] = "esriFieldTypeInteger"
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            self._validate(mutate)

    def test_rejects_crs_conflict_across_advertised_representations(self):
        def mutate(docs):
            docs["page_000"]["spatialReference"] = {"wkid": 3857, "latestWkid": 4326}
        with self.assertRaisesRegex(ValueError, "source_page_crs_drift"):
            self._validate(mutate)

    def test_rejects_layer_metadata_drift_after_feature_pages(self):
        with self.assertRaisesRegex(ValueError, "source_layer_changed_during_capture"):
            self._validate(lambda docs: docs["layer_after"].update(maxRecordCount=1000))


if __name__ == "__main__":
    unittest.main()

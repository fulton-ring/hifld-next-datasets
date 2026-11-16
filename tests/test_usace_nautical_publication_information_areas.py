import json
import unittest

from dagster import DefaultScheduleStatus

from dagster_hifld.assets.usace import (
    usace_nautical_publication_information_areas_usace_ienc,
    usace_nautical_publication_information_areas_draft_schedule,
)
from dagster_hifld.definitions import defs
from dagster_hifld.sources.usace_nautical_publication_information_areas import (
    CURRENT_BTS_ITEM_ID, DATASET_SLUG, EXPECTED_COUNT, EXPECTED_FIELD_DECLARATIONS,
    EXPECTED_FIELDS, LAYER_URL, _validate,
)


class UsaceNauticalPublicationInformationAreasTests(unittest.TestCase):
    def test_official_layer_profile_and_complete_fixture(self):
        self.assertEqual(DATASET_SLUG, "nautical-publication-information-areas-usace-ienc")
        self.assertTrue(LAYER_URL.endswith("/NAUTICAL_PUBLICATION_INFORMATION_AREA/MapServer/67"))
        self.assertEqual(EXPECTED_COUNT, 109)
        parsed_fields, parsed_features, audit = self._validate_complete_fixture()
        self.assertEqual(parsed_fields, EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(parsed_features), EXPECTED_COUNT)
        self.assertEqual(audit["source_feature_count"], EXPECTED_COUNT)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], EXPECTED_COUNT)
        self.assertIn("BTS item delegation", audit["drift_guard"])
        self.assertIn("no national coverage claim", audit["source_coverage_scope"])

    def test_current_bts_item_or_pdf_drift_is_refused(self):
        self._validate_complete_fixture()
        for mutation in (
            {"id": "3607d485a9854d6fa458bf507058ceae"},
            {"owner": "someone_else"},
            {"type": "CSV"},
            {"access": "private"},
            {"title": "Some other publication"},
            {"description": "no service or master geodatabase links"},
        ):
            with self.assertRaisesRegex(ValueError, "current_bts_ienc_item_or_pdf_drift"):
                captures = self._build_captures(bts_item_overrides=mutation)
                _validate(captures)
        with self.assertRaisesRegex(ValueError, "current_bts_ienc_item_or_pdf_drift"):
            captures = self._build_captures(bts_pdf=b"not-a-pdf")
            _validate(captures)

    def test_dagster_registry_contains_candidate_check_and_stopped_refresh(self):
        keys = {key for asset_definition in defs.assets for key in asset_definition.keys}
        self.assertIn(next(iter(usace_nautical_publication_information_areas_usace_ienc.keys)), keys)
        self.assertEqual(
            usace_nautical_publication_information_areas_draft_schedule.default_status,
            DefaultScheduleStatus.STOPPED,
        )

    def _build_captures(self, bts_item_overrides=None, bts_pdf=b"%PDF-1.7\nretained PDF bytes"):
        ids = list(range(1, EXPECTED_COUNT + 1))
        fields = EXPECTED_FIELD_DECLARATIONS
        layer = {
            "name": "NAUTICAL_PUBLICATION_INFORMATION_AREA", "id": 67,
            "geometryType": "esriGeometryPolygon",
            "spatialReference": {"latestWkid": 4326}, "maxRecordCount": 2000,
            "fields": fields,
        }
        bts_item = {
            "id": CURRENT_BTS_ITEM_ID, "owner": "USDOT_BTS", "type": "PDF",
            "access": "public", "title": "Inland Electronic Navigational Charts",
            "description": "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes; USACE_IENC_Master_Service_gdb.zip",
        }
        bts_item.update(bts_item_overrides or {})
        docs = {
            "bts_item": bts_item,
            "layer_before": layer,
            "layer_after": {"fields": fields, "geometryType": "esriGeometryPolygon"},
            "count_before": {"count": EXPECTED_COUNT},
            "count_after": {"count": EXPECTED_COUNT},
            "ids_before": {"objectIds": ids},
            "ids_after": {"objectIds": ids},
        }
        attributes = {name: None for name, _ in EXPECTED_FIELDS if name != "SHAPE"}
        features = []
        for oid in ids:
            row = dict(attributes)
            row.update({"OBJECTID": oid, "Information": "Sailing directions", "Feature_ID": f"NP-{oid:03d}"})
            features.append({"attributes": row, "geometry": {
                "rings": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            }})
        docs["page_000"] = {"spatialReference": {"latestWkid": 4326}, "features": features}
        captures = {name: (json.dumps(doc).encode(), {}) for name, doc in docs.items()}
        captures["bts_pdf"] = (bts_pdf, {})
        return captures

    def _validate_complete_fixture(self):
        return _validate(self._build_captures())


if __name__ == "__main__":
    unittest.main()

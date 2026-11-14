import json
import struct
import tempfile
import unittest
from pathlib import Path

from dagster import DefaultScheduleStatus
from shapely import from_wkb
from shapely.geometry import Polygon

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_military_districts import (
    usace_military_districts, usace_military_districts_complete,
    usace_military_districts_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import usace_military_districts as source
from dagster_hifld.sources.epa_frs_common import _canonical


class UsaceMilitaryDistrictsTests(unittest.TestCase):
    def _shell(self, oid):
        x0 = -93.5 + (oid % 100) * 0.003
        y0 = 35.0 + (oid // 100) * 0.003
        return [[x0, y0], [x0, y0 + 0.001], [x0 + 0.001, y0 + 0.001], [x0 + 0.001, y0], [x0, y0]]

    def _hole(self, shell):
        inset = 0.0002
        return [[shell[0][0] + inset, shell[0][1] + inset],
                [shell[2][0] - inset, shell[0][1] + inset],
                [shell[2][0] - inset, shell[2][1] - inset],
                [shell[0][0] + inset, shell[2][1] - inset],
                [shell[0][0] + inset, shell[0][1] + inset]]

    def _documents(self):
        sr = {"wkid": 4326, "latestWkid": 4326}
        service = {"serviceItemId": source.CATALOG_ITEM_ID,
                   "description": source.ITEM_DESCRIPTION,
                   "serviceDescription": source.SERVICE_DESCRIPTION,
                   "capabilities": "Query,Extract", "maxRecordCount": 1000,
                   "hasVersionedData": False, "spatialReference": sr,
                   "fullExtent": {**source.EXPECTED_EXTENT, "spatialReference": sr},
                   "layers": [{"id": 0, "name": source.LAYER_NAME, "parentLayerId": -1,
                               "subLayerIds": None, "type": "Feature Layer",
                               "geometryType": "esriGeometryPolygon"}]}
        layer = {"id": 0, "name": source.LAYER_NAME, "geometryType": "esriGeometryPolygon",
                 "maxRecordCount": 1000, "objectIdField": "OBJECTID", "globalIdField": "",
                 "typeIdField": "MIL_DIST", "hasZ": False, "hasM": False,
                 "editingInfo": dict(source.EXPECTED_EDITING_INFO),
                 "spatialReference": sr,
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": sr},
                 "advancedQueryCapabilities": {"supportsPagination": True},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "catalog_item": {"id": source.CATALOG_ITEM_ID, "owner": source.CATALOG_ITEM_OWNER,
                             "type": "Feature Service", "title": source.LAYER_NAME,
                             "description": source.ITEM_DESCRIPTION,
                             "licenseInfo": source.ITEM_LICENSE,
                             "spatialReference": "4326",
                             "extent": [[source.EXPECTED_EXTENT["xmin"], source.EXPECTED_EXTENT["ymin"]],
                                        [source.EXPECTED_EXTENT["xmax"], source.EXPECTED_EXTENT["ymax"]]]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            "service_after": json.loads(json.dumps(service)), "layer_after": json.loads(json.dumps(layer)),
            "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS},
        }
        page_fields = [{key: value for key, value in field.items()
                        if key not in ("nullable", "editable")}
                       for field in source.EXPECTED_FIELD_DECLARATIONS]
        for page_index in range((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
            rows = []
            for oid in source.EXPECTED_IDS[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]:
                rings = [self._shell(oid)]
                if oid in (6, 16, 17, 27):
                    rings.append(self._hole(rings[0]))
                attrs = {}
                for field in source.ATTRIBUTE_FIELDS:
                    name, kind = field["name"], field["type"]
                    if name == "OBJECTID":
                        attrs[name] = oid
                    elif kind == "esriFieldTypeString":
                        attrs[name] = f"{name}-{oid}"
                    elif kind == "esriFieldTypeBlob":
                        attrs[name] = ""
                    elif name == "Shape__Area":
                        decoded = from_wkb(source._esri_polygon_wkb({"rings": rings}))
                        attrs[name] = decoded.area
                    elif name == "Shape__Length":
                        decoded = from_wkb(source._esri_polygon_wkb({"rings": rings}))
                        attrs[name] = decoded.length
                    else:
                        attrs[name] = float(oid)
                rows.append({"attributes": attrs, "geometry": {"rings": rings}})
            docs[f"page_{page_index:03d}"] = {"objectIdFieldName": "OBJECTID",
                                              "geometryType": "esriGeometryPolygon",
                                              "geometryProperties": dict(source.EXPECTED_GEOMETRY_PROPERTIES),
                                              "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                                              "fields": page_fields, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (json.dumps(value, ensure_ascii=False, allow_nan=False).encode(), {"status": 200})
                for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(usace_military_districts, hifld_ingest_assets)
        self.assertEqual(usace_military_districts_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = usace_military_districts_complete({"feature_count": 27, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = usace_military_districts_complete({"feature_count": 26, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")], ["page_000"])
        self.assertIn("objectIds=1%2C2%2C3", urls["page_000"])
        self.assertIn("%2C25%2C26%2C27&", urls["page_000"])
        self.assertNotIn("objectIds=28", urls["page_000"])
        self.assertIn("outSR=4326", urls["page_000"])
        self.assertIn("returnZ=true", urls["page_000"])
        self.assertIn("returnM=true", urls["page_000"])
        self.assertIn("orderByFields=OBJECTID+ASC", urls["page_000"])
        self.assertEqual(source.CATALOG_ITEM_URL, "https://www.arcgis.com/sharing/rest/content/items/"
                         "c12cd7878abf4b5cae6442cf4007e0d2?f=json")
        self.assertEqual(source.LAYER_URL, "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/"
                         "arcgis/rest/services/usace_mil_dist/FeatureServer/0")

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 27)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 155)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], 27)
        self.assertEqual(features[0]["attributes"]["AGENCY_SYMBOL"], "AGENCY_SYMBOL-1")
        self.assertEqual(audit["source_last_data_edit_utc"], "2025-04-21T18:55:32.923000Z")
        self.assertEqual(audit["dhs_crosswalk_row"]["status"], "Deprecate")
        self.assertIn("USACE-hosted services7 FeatureServer", audit["source_coverage_scope"])

    def test_polygon_wkb_is_xy_only_and_preserves_ring_structure(self):
        shell = self._shell(1)
        single = source._esri_polygon_wkb({"rings": [shell]})
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 3))
        disjoint = source._esri_polygon_wkb({"rings": [shell, self._shell(2)]})
        self.assertEqual(struct.unpack("<BI", disjoint[:5]), (1, 6))
        with_hole = source._esri_polygon_wkb({"rings": [shell, self._hole(shell)]})
        self.assertEqual(struct.unpack("<BI", with_hole[:5]), (1, 3))
        for flags in ({"hasZ": True}, {"hasM": True}, {"hasZ": True, "hasM": True}):
            with self.assertRaisesRegex(ValueError, "source_polygon_dimensional_flags_rejected"):
                source._esri_polygon_wkb({"rings": [shell], **flags})
        unflagged = [[0.0, 0.0, 1.0], [0.0, 1.0, 1.0], [1.0, 1.0, 1.0], [1.0, 0.0, 1.0], [0.0, 0.0, 1.0]]
        with self.assertRaisesRegex(ValueError, "source_polygon_zm_flags_do_not_match_coordinates"):
            source._esri_polygon_wkb({"rings": [unflagged]})

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population after", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_id_population_drift:after"),
            ("count before", lambda d: d["count_before"].update(count=26),
             "source_count_population_drift:before"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
            ("transfer", lambda d: d["page_000"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_000"].update(spatialReference={"wkid": 3857}),
             "source_page_000_page_crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                OBJECTID=d["page_000"]["features"][0]["attributes"]["OBJECTID"]),
             "source_page_id_population_drift"),
            ("layer dimensional", lambda d: d["layer_before"].update(hasZ=True),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer freshness", lambda d: d["layer_before"]["editingInfo"].update(dataLastEditDate=1745261732924),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer extent", lambda d: d["layer_before"]["extent"].update(xmax=0.0),
             "source_layer_identity_extent_or_freshness_drift"),
            ("page dimensional", lambda d: d["page_000"].update(hasZ=True),
             "source_page_dimensional_drift"),
            ("page identity", lambda d: d["page_000"].update(objectIdFieldName="FID"),
             "source_page_identity_drift"),
            ("page geometry properties", lambda d: d["page_000"].update(geometryProperties={"units": "esriMeters"}),
             "source_page_geometry_properties_drift"),
            ("page field drift", lambda d: d["page_000"]["fields"][2].update(name="renamed"),
             "source_page_physical_schema_changed"),
            ("geometry dimensional", lambda d: d["page_000"]["features"][0]["geometry"].update(hasZ=True),
             "source_geometry_dimensional_drift"),
            ("open ring", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].pop(),
             "source_polygon_ring_not_closed"),
            ("outside coverage", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].__setitem__(
                2, [-185.0, 35.5]),
             "source_geometry_outside_declared_coverage"),
            ("hole without shell", lambda d: d["page_000"]["features"][0]["geometry"]["rings"].append(
                self._hole(self._shell(99))),
             "source_hole_ownership_ambiguous"),
            ("shell missing", lambda d: d["page_000"]["features"][0]["geometry"].update(
                rings=[self._hole(self._shell(99))]),
             "source_polygon_shell_missing"),
            ("item identity", lambda d: d["catalog_item"].update(owner="someone_else"),
             "usace_catalog_item_identity_drift"),
            ("item license text", lambda d: d["catalog_item"].update(licenseInfo="changed"),
             "usace_catalog_item_text_drift"),
            ("item extent", lambda d: d["catalog_item"].update(extent=[[0.0, 0.0], [1.0, 1.0]]),
             "usace_catalog_item_extent_drift"),
            ("service identity", lambda d: d["service_before"].update(serviceItemId="other"),
             "service_before_service_identity_drift"),
            ("service roster", lambda d: d["service_before"]["layers"].append(
                {"id": 1, "name": "Extra", "parentLayerId": -1, "subLayerIds": None,
                 "type": "Feature Layer", "geometryType": "esriGeometryPolygon"}),
             "service_before_service_layer_roster_drift"),
            ("service changed during capture", lambda d: d["service_after"].update(maxRecordCount=2000),
             "source_service_or_layer_changed_during_capture"),
            ("blob json type", lambda d: d["page_000"]["features"][0]["attributes"].update(SE_ANNO_CAD_DATA=5),
             "source_blob_json_text_type_drift"),
            ("string value type", lambda d: d["page_000"]["features"][0]["attributes"].update(EROC=5),
             "source_string_value_type_drift:EROC"),
            ("double value type", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Area="x"),
             "source_double_value_type_drift:Shape__Area"),
            ("area parity", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Area=1.5),
             "source_shape_area_parity_drift"),
            ("length parity", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Length=1.5),
             "source_shape_length_parity_drift"),
            ("attribute order", lambda d: d["page_000"]["features"][0]["attributes"].pop("EROC"),
             "source_attribute_roster_or_order_drift"),
            ("oid value", lambda d: d["page_000"]["features"][0]["attributes"].update(OBJECTID=0),
             "source_oid_value_type_drift"),
            ("oid roster", lambda d: d["page_000"]["features"][0]["attributes"].update(OBJECTID=99),
             "source_feature_id_roster_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_usace_military_districts(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

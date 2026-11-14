import json
import os
import struct
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely import from_wkb
from shapely.geometry import Polygon

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.us_courts_appeals_circuits import (
    us_courts_appeals_circuits, us_courts_appeals_circuits_complete,
    us_courts_appeals_circuits_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import us_courts_appeals_circuits as source
from dagster_hifld.sources.epa_frs_common import _canonical

# Real roster order as observed on 2026-11-14: OBJECTID 1..13 map to the twelve
# geographic circuits plus the unassigned null-geometry 'XX' placeholder.
CIRCUIT_BY_OID = {1: "01", 2: "10", 3: "11", 4: "02", 5: "03", 6: "04", 7: "05",
                  8: "06", 9: "07", 10: "08", 11: "09", 12: "DC", 13: "XX"}


class UsCourtsAppealsCircuitsTests(unittest.TestCase):
    def _shell(self, oid, lon0=-93.5, lat0=35.0):
        x0 = lon0 + (oid % 100) * 0.003
        y0 = lat0 + (oid // 100) * 0.003
        return [[x0, y0], [x0, y0 + 0.001], [x0 + 0.001, y0 + 0.001], [x0 + 0.001, y0], [x0, y0]]

    def _hole(self, shell):
        inset = 0.0002
        return [[shell[0][0] + inset, shell[0][1] + inset],
                [shell[2][0] - inset, shell[0][1] + inset],
                [shell[2][0] - inset, shell[2][1] - inset],
                [shell[0][0] + inset, shell[2][1] - inset],
                [shell[0][0] + inset, shell[0][1] + inset]]

    def _rings_for(self, oid):
        if oid == 9:
            # A shell with one hole exercises winding and hole ownership.
            shell = self._shell(oid)
            return [shell, self._hole(shell)]
        if oid == 11:
            # Two shells model a multi-part circuit polygon.
            return [self._shell(oid), self._shell(oid + 100, lon0=-100.0)]
        return [self._shell(oid)]

    def _features(self):
        rows = []
        for oid in source.EXPECTED_IDS:
            circuit = CIRCUIT_BY_OID[oid]
            attrs = {"OBJECTID": oid, "Circuit": circuit, "Shape__Area": None,
                     "Shape__Length": None, "GSA_Teams": None}
            feature = {"attributes": attrs}
            if oid in source.EXPECTED_NULL_GEOMETRY_IDS:
                rows.append(feature)
                continue
            rings = self._rings_for(oid)
            decoded = from_wkb(source._esri_polygon_wkb({"rings": rings}))
            attrs["Shape__Area"] = decoded.area
            attrs["Shape__Length"] = decoded.length
            feature["geometry"] = {"rings": rings}
            rows.append(feature)
        return rows

    def _documents(self):
        sr = {"wkid": 4326, "latestWkid": 4326}
        service = {"serviceItemId": source.CATALOG_ITEM_ID,
                   "serviceDescription": source.SERVICE_DESCRIPTION,
                   "capabilities": "Query", "maxRecordCount": source.SERVICE_MAX_RECORD_COUNT,
                   "hasVersionedData": False, "supportedQueryFormats": "JSON",
                   "spatialReference": sr,
                   "fullExtent": {**source.SERVICE_FULL_EXTENT, "spatialReference": sr},
                   "layers": [{"id": 0, "name": source.LAYER_NAME, "parentLayerId": -1,
                               "subLayerIds": None, "type": "Feature Layer",
                               "geometryType": "esriGeometryPolygon"}]}
        layer = {"id": 0, "name": source.LAYER_NAME, "type": "Feature Layer",
                 "geometryType": "esriGeometryPolygon",
                 "maxRecordCount": source.LAYER_MAX_RECORD_COUNT,
                 "objectIdField": "OBJECTID", "globalIdField": "", "typeIdField": "",
                 "displayField": source.LAYER_DISPLAY_FIELD,
                 "hasZ": False, "hasM": False, "enableNullGeometry": False,
                 "isDataVersioned": False, "copyrightText": source.LAYER_COPYRIGHT,
                 "serviceItemId": source.CATALOG_ITEM_ID,
                 "editingInfo": dict(source.EXPECTED_EDITING_INFO),
                 "geometryProperties": dict(source.EXPECTED_GEOMETRY_PROPERTIES),
                 "spatialReference": sr,
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": sr},
                 "advancedQueryCapabilities": {"supportsPagination": True},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "catalog_item": {"id": source.CATALOG_ITEM_ID, "owner": source.CATALOG_ITEM_OWNER,
                             "orgId": source.CATALOG_ORG_ID, "type": "Feature Service",
                             "title": source.ITEM_TITLE, "access": "public",
                             "accessInformation": source.ITEM_ACCESS_INFORMATION,
                             "spatialReference": "4326", "snippet": source.ITEM_SNIPPET,
                             "tags": list(source.ITEM_TAGS),
                             "categories": list(source.ITEM_CATEGORIES),
                             "description": source.ITEM_DESCRIPTION,
                             "licenseInfo": source.ITEM_LICENSE,
                             "extent": [list(source.ITEM_EXTENT[0]), list(source.ITEM_EXTENT[1])]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            "service_after": json.loads(json.dumps(service)),
            "layer_after": json.loads(json.dumps(layer)),
            "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS},
        }
        page_fields = [{key: value for key, value in field.items()
                        if key not in ("nullable", "editable")}
                       for field in source.EXPECTED_FIELD_DECLARATIONS]
        for page_index in range(source.PAGE_COUNT):
            chunk = source.EXPECTED_IDS[page_index * source.PAGE_SIZE:
                                        (page_index + 1) * source.PAGE_SIZE]
            rows = [row for row in self._features() if row["attributes"]["OBJECTID"] in chunk]
            docs[f"page_{page_index:03d}"] = {"objectIdFieldName": "OBJECTID",
                                              "globalIdFieldName": "",
                                              "uniqueIdField": {"name": "OBJECTID",
                                                                "isSystemMaintained": True},
                                              "geometryType": "esriGeometryPolygon",
                                              "geometryProperties": dict(source.EXPECTED_GEOMETRY_PROPERTIES),
                                              "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                                              "fields": page_fields, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (json.dumps(value, ensure_ascii=False, allow_nan=False).encode(), {"status": 200})
                for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(us_courts_appeals_circuits, hifld_ingest_assets)
        self.assertEqual(us_courts_appeals_circuits_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = us_courts_appeals_circuits_complete(
            {"feature_count": 13, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = us_courts_appeals_circuits_complete(
            {"feature_count": 12, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_single_page_covers_source_ids(self):
        from urllib.parse import parse_qs, urlsplit
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         ["page_000"])
        params = parse_qs(urlsplit(urls["page_000"]).query)
        self.assertEqual([int(value) for value in params["objectIds"][0].split(",")],
                         source.EXPECTED_IDS)
        self.assertEqual(params["outSR"], ["4326"])
        self.assertEqual(params["orderByFields"], ["OBJECTID ASC"])
        self.assertEqual(params["returnZ"], ["false"])
        self.assertEqual(params["returnM"], ["false"])
        self.assertEqual(params["outFields"], ["*"])
        self.assertEqual(source.CATALOG_ITEM_URL, "https://www.arcgis.com/sharing/rest/content/"
                         "items/d8ff459672c746a4b3d53bae9330701a?f=json")
        self.assertEqual(source.LAYER_URL, "https://services9.arcgis.com/LJKafwRzLHEkKeOL/"
                         "arcgis/rest/services/Circuits_Overall_Boundary/FeatureServer/0")

    def test_source_pins_match_researched_producer_publication(self):
        self.assertEqual(source.DATASET_SLUG, "us-courts-of-appeals-circuits")
        self.assertEqual(source.FILE_SLUG, "us-courts-of-appeals-circuits")
        self.assertEqual(source.PUBLISHER, "Administrative Office of the U.S. Courts (AOUSC)")
        self.assertEqual(source.CATALOG_ORG_NAME, "Judicial Branch (AOUSC)")
        self.assertEqual(source.CATALOG_ORG_ID, "LJKafwRzLHEkKeOL")
        self.assertEqual(source.CATALOG_ITEM_OWNER, "Courts_GIS_AOUSC")
        self.assertEqual(source.EXPECTED_COUNT, 13)
        self.assertEqual(source.EXPECTED_IDS, list(range(1, 14)))
        self.assertEqual(source.EXPECTED_CIRCUIT_ROSTER,
                         ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "DC", "XX"])
        self.assertEqual(source.EXPECTED_NULL_GEOMETRY_IDS, [13])
        self.assertEqual(source.EXPECTED_LAST_DATA_EDIT_UTC, "2025-12-03T16:51:36.628000Z")
        self.assertEqual(source.EXPECTED_LAST_EDIT_UTC, "2026-02-03T16:28:30.448000Z")
        self.assertEqual(source.EXPECTED_RECORD_SHA256,
                         "702c2a7c1f8602f89ebece3b7fd3f8f9761a3de41cac469d3dbc0aff6007508a")
        self.assertEqual(source.EXPECTED_GEOMETRY_SHA256,
                         "3ca0235fbd657f1834346c63c8499696b8e4dd341574770481fa0953d58eaec9")
        self.assertEqual(source._sha(_canonical(source.EXPECTED_IDS)), source.EXPECTED_ID_SHA256)
        self.assertEqual(source._sha(_canonical(source.EXPECTED_CIRCUIT_ROSTER)),
                         source.EXPECTED_CIRCUIT_SHA256)
        self.assertEqual(source._sha(_canonical(source.EXPECTED_FIELD_DECLARATIONS)),
                         "bbe1329112e01bdf02081968554745f24acfba6b80d86ea9c415ffb7e235deec")
        self.assertIn("d8ff459672c746a4b3d53bae9330701a", source.LANDING_URL)
        self.assertEqual(source._sha(_canonical(sorted(CIRCUIT_BY_OID.values()))),
                         source.EXPECTED_CIRCUIT_SHA256)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 13)
        self.assertEqual(sorted(row["attributes"]["OBJECTID"] for row in features),
                         source.EXPECTED_IDS)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], 13)
        self.assertEqual(audit["geometry_valid_count"], 12)
        self.assertEqual(audit["geometry_null_count"], 1)
        self.assertEqual(audit["geometry_null_ids"], [13])
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["source_last_data_edit_utc"], "2025-12-03T16:51:36.628000Z")
        self.assertEqual(audit["dhs_crosswalk_row"]["status"], "Deprecate")
        self.assertEqual(audit["dhs_crosswalk_row"]["agency"], "Federal Judicial Center")
        self.assertIn("Administrative Office of the U.S. Courts", audit["source_coverage_scope"])
        self.assertIn("LJKafwRzLHEkKeOL", audit["catalog_item_org"])
        self.assertEqual(audit["source_record_sha256"],
                         source._sha(_canonical(
                             [[row["attributes"]["OBJECTID"], row["attributes"]] for row in features])))

    def test_candidate_parquet_preserves_fields_ids_and_null_geometry(self):
        fields, features, _audit = source._validate(self._captures(self._documents()))
        parquet = source._parquet_bytes(fields, features)
        table = pq.read_table(__import__("io").BytesIO(parquet))
        self.assertEqual(table.column_names,
                         ["OBJECTID", "Circuit", "Shape__Area", "Shape__Length", "GSA_Teams",
                          "geometry"])
        self.assertEqual(table.num_rows, 13)
        geometry_column = table.column("geometry").to_pylist()
        self.assertIsNone(geometry_column[12])
        self.assertEqual(len([value for value in geometry_column if value is not None]), 12)
        oid_column = table.column("OBJECTID").to_pylist()
        self.assertEqual(oid_column, source.EXPECTED_IDS)
        self.assertEqual(table.column("Circuit").to_pylist(),
                         [CIRCUIT_BY_OID[oid] for oid in source.EXPECTED_IDS])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["primary_column"], "geometry")
        self.assertEqual(geo["columns"]["geometry"]["encoding"], "WKB")
        for value in geometry_column:
            if value is None:
                continue
            geom = from_wkb(value)
            self.assertTrue(geom.is_valid)

    def test_wkb_is_xy_only_polygon_or_multipolygon(self):
        single = source._esri_polygon_wkb({"rings": [self._shell(1)]})
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 3))
        west = [[-180.0, 19.0], [-180.0, 20.0], [-179.9, 20.0], [-179.9, 19.0], [-180.0, 19.0]]
        east = [[179.9, 19.0], [179.9, 20.0], [179.9999, 20.0], [179.9999, 19.0], [179.9, 19.0]]
        dateline = source._esri_polygon_wkb({"rings": [west, east]})
        self.assertEqual(struct.unpack("<BI", dateline[:5]), (1, 6))
        with_hole = source._esri_polygon_wkb({"rings": [self._shell(1), self._hole(self._shell(1))]})
        self.assertEqual(struct.unpack("<BI", with_hole[:5]), (1, 3))
        for flags in ({"hasZ": True}, {"hasM": True}, {"hasZ": True, "hasM": True}):
            with self.assertRaisesRegex(ValueError, "source_polygon_dimensional_flags_rejected"):
                source._esri_polygon_wkb({"rings": [self._shell(1)], **flags})

    def test_population_schema_id_circuit_geometry_and_contract_drift_fail_closed(self):
        cases = [
            ("population after", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_id_population_drift:after"),
            ("id roster gap filled", lambda d: d["ids_before"].update(objectIds=[*range(1, 14, 1)][:-1]),
             "source_id_population_drift:before"),
            ("count before", lambda d: d["count_before"].update(count=12),
             "source_count_population_drift:before"),
            ("schema rename", lambda d: d["layer_before"]["fields"][1].update(name="renamed"),
             "source_field_schema_drift"),
            ("schema type change", lambda d: d["layer_before"]["fields"][1].update(type="esriFieldTypeInteger"),
             "source_field_schema_drift"),
            ("unexpected column", lambda d: d["layer_before"]["fields"].append(
                 {"name": "EXTRA", "type": "esriFieldTypeString", "alias": "EXTRA",
                  "sqlType": "sqlTypeOther", "length": 5, "nullable": True, "editable": True,
                  "domain": None, "defaultValue": None}),
             "source_field_schema_drift"),
            ("page unexpected column", lambda d: d["page_000"]["fields"].append(
                 {"name": "EXTRA", "type": "esriFieldTypeString", "alias": "EXTRA",
                  "sqlType": "sqlTypeOther", "domain": None, "defaultValue": None}),
             "source_page_physical_schema_changed"),
            ("transfer", lambda d: d["page_000"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("page error", lambda d: d["page_000"].update(error={"code": 400, "message": "x"}),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_000"].update(spatialReference={"wkid": 3857}),
             "source_page_000_page_crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                OBJECTID=d["page_000"]["features"][0]["attributes"]["OBJECTID"]),
             "source_page_id_population_drift"),
            ("layer dimensional", lambda d: d["layer_before"].update(hasZ=True),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer freshness", lambda d: d["layer_before"]["editingInfo"].update(dataLastEditDate=1764780696630),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer extent", lambda d: d["layer_before"]["extent"].update(xmax=0.0),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer copyright", lambda d: d["layer_before"].update(copyrightText="someone else"),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer display field", lambda d: d["layer_before"].update(displayField="GSA_Teams"),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer service item", lambda d: d["layer_before"].update(serviceItemId="other"),
             "source_layer_identity_extent_or_freshness_drift"),
            ("page dimensional", lambda d: d["page_000"].update(hasZ=True),
             "source_page_dimensional_drift"),
            ("page identity", lambda d: d["page_000"].update(objectIdFieldName="FID"),
             "source_page_identity_drift"),
            ("page unique field", lambda d: d["page_000"].update(
                uniqueIdField={"name": "OBJECTID", "isSystemMaintained": False}),
             "source_page_identity_drift"),
            ("page geometry properties", lambda d: d["page_000"].update(
                geometryProperties={"units": "esriMeters"}),
             "source_page_geometry_properties_drift"),
            ("page field drift", lambda d: d["page_000"]["fields"][1].update(name="renamed"),
             "source_page_physical_schema_changed"),
            ("geometry dimensional", lambda d: (
                d["page_000"]["features"][0]["geometry"].update(hasZ=True),
                d["page_000"]["features"][0]["attributes"].update(Shape__Area=None, Shape__Length=None)),
             "source_geometry_dimensional_drift"),
            ("open ring", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].pop(),
             "source_polygon_ring_not_closed"),
            ("outside coverage", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].__setitem__(
                2, [-185.0, 35.5]),
             "source_geometry_outside_declared_coverage"),
            ("hole without shell", lambda d: d["page_000"]["features"][8]["geometry"].update(
                rings=[self._hole(self._shell(99))]),
             "source_polygon_shell_missing"),
            ("item identity", lambda d: d["catalog_item"].update(owner="someone_else"),
             "aousc_catalog_item_identity_drift"),
            ("item org", lambda d: d["catalog_item"].update(orgId="otherorg"),
             "aousc_catalog_item_identity_drift"),
            ("item access", lambda d: d["catalog_item"].update(access="private"),
             "aousc_catalog_item_identity_drift"),
            ("item accessInformation", lambda d: d["catalog_item"].update(accessInformation="Esri"),
             "aousc_catalog_item_identity_drift"),
            ("item license text", lambda d: d["catalog_item"].update(licenseInfo="changed"),
             "aousc_catalog_item_text_drift"),
            ("item description text", lambda d: d["catalog_item"].update(description="changed"),
             "aousc_catalog_item_text_drift"),
            ("item snippet", lambda d: d["catalog_item"].update(snippet="changed"),
             "aousc_catalog_item_text_drift"),
            ("item tags", lambda d: d["catalog_item"].update(tags=["Other"]),
             "aousc_catalog_item_text_drift"),
            ("item category", lambda d: d["catalog_item"].update(categories=["/Categories/Other"]),
             "aousc_catalog_item_text_drift"),
            ("item extent", lambda d: d["catalog_item"].update(extent=[[0.0, 0.0], [1.0, 1.0]]),
             "aousc_catalog_item_extent_drift"),
            ("service identity", lambda d: d["service_before"].update(serviceItemId="other"),
             "service_before_service_identity_drift"),
            ("service roster", lambda d: d["service_before"]["layers"].append(
                 {"id": 1, "name": "Extra", "parentLayerId": -1, "subLayerIds": None,
                  "type": "Feature Layer", "geometryType": "esriGeometryPolygon"}),
             "service_before_service_layer_roster_drift"),
            ("service changed during capture", lambda d: d["service_after"].update(maxRecordCount=100),
             "source_service_or_layer_changed_during_capture"),
            ("layer changed during capture", lambda d: d["layer_after"]["fields"][1].update(name="renamed"),
             "source_service_or_layer_changed_during_capture"),
            ("string value type", lambda d: d["page_000"]["features"][0]["attributes"].update(Circuit=5),
             "source_string_value_type_drift:Circuit"),
            ("double value type", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Area="x"),
             "source_double_value_type_drift:Shape__Area"),
            ("area parity", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Area=1.5),
             "source_shape_area_parity_drift"),
            ("length parity", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Length=1.5),
             "source_shape_length_parity_drift"),
            ("attribute order", lambda d: d["page_000"]["features"][0]["attributes"].pop("GSA_Teams"),
             "source_attribute_roster_or_order_drift"),
            ("oid value", lambda d: d["page_000"]["features"][0]["attributes"].update(OBJECTID=0),
             "source_oid_value_type_drift"),
            ("oid roster", lambda d: d["page_000"]["features"][0]["attributes"].update(OBJECTID=99),
             "source_feature_id_roster_drift"),
            ("circuit roster", lambda d: d["page_000"]["features"][0]["attributes"].update(Circuit="99"),
             "source_circuit_roster_drift"),
            ("null geometry roster", lambda d: d["page_000"]["features"][11].pop("geometry"),
             "source_null_geometry_roster_drift"),
            ("null geometry gained", lambda d: d["page_000"]["features"][12].update(
                geometry={"rings": [self._shell(13)]}),
             "source_null_geometry_roster_drift"),
            ("capture roster", lambda d: d.pop("service_after"),
             "source_capture_name_roster_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def test_geometry_invalid_ring_fails_closed(self):
        # A crossing bowtie ring is invalid under OGC; this source pins zero
        # invalid geometries, so an invalid ring must abort the capture.
        spike = {"rings": [[[0.0, 20.0], [0.0, 24.0], [2.0, 24.0], [2.0, 22.0],
                            [2.0, 24.0], [4.0, 24.0], [4.0, 20.0], [0.0, 20.0]]]}
        self.assertFalse(from_wkb(source._esri_polygon_wkb(spike)).is_valid)
        docs = self._documents()
        docs["page_000"]["features"][0]["geometry"] = dict(spike)
        docs["page_000"]["features"][0]["attributes"].update(Shape__Area=None, Shape__Length=None)
        with self.assertRaisesRegex(ValueError, "source_geometry_invalid_count_drift"):
            source._validate(self._captures(docs))

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_us_courts_appeals_circuits(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

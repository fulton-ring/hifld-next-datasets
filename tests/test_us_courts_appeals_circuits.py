import json
import os
import struct
import tempfile
import unittest
from unittest.mock import patch

import httpx
from pathlib import Path

import pyarrow.parquet as pq
from dagster import AssetCheckSeverity, DefaultScheduleStatus
from shapely import from_wkb
from shapely.geometry import Polygon

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.us_courts_appeals_circuits import (
    us_courts_appeals_circuits, us_courts_appeals_circuits_complete,
    us_courts_appeals_circuits_draft_schedule,
)
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import us_courts_appeals_circuits as source
from dagster_hifld.sources import capture_common
from dagster_hifld.sources.esri_common import canonical as _canonical

# Real roster order as observed on 2026-11-14: OBJECTID 1..13 map to the twelve
# geographic circuits plus the unassigned null-geometry 'XX' placeholder.
CIRCUIT_BY_OID = {1: "01", 2: "10", 3: "11", 4: "02", 5: "03", 6: "04", 7: "05",
                  8: "06", 9: "07", 10: "08", 11: "09", 12: "DC", 13: "XX"}


FIXTURE_EXPECTED_COUNT = 13
FIXTURE_EXPECTED_IDS = list(range(1, 14))
FIXTURE_EXPECTED_ID_SHA256 = "c94747afa847f98b263eca5299b28438181251bdd86c930a56c2e16f685c4294"
FIXTURE_EXPECTED_NULL_GEOMETRY_IDS = [13]
FIXTURE_PAGE_COUNT = (len(FIXTURE_EXPECTED_IDS) + source.PAGE_SIZE - 1) // source.PAGE_SIZE
FIXTURE_EXPECTED_EDITING_INFO = {"lastEditDate": 1770136110448,
                         "schemaLastEditDate": 1764780696628,
                         "dataLastEditDate": 1764780696628}


class UsCourtsAppealsCircuitsTests(unittest.TestCase):
    def _added_documents(self):
        docs = self._documents()
        added = json.loads(json.dumps(docs["page_000"]["features"][0]))
        added["attributes"].update(OBJECTID=27, Circuit="ZZ")
        docs["page_000"]["features"].append(added)
        for side in ("before", "after"):
            docs[f"ids_{side}"]["objectIds"] = list(range(1, 14)) + [27]
            docs[f"count_{side}"]["count"] = 14
            docs[f"layer_{side}"]["editingInfo"]["dataLastEditDate"] += 1000
        return docs

    def test_upstream_added_feature_uses_capture_witness_and_warns(self):
        docs = self._added_documents()
        added = docs["page_000"]["features"][-1]
        fields, features, audit = source._validate(self._captures(docs))
        self.assertEqual(len(features), 14)
        self.assertEqual(audit["source_feature_count"], 14)
        self.assertEqual(audit["source_id_max"], 27)
        self.assertEqual(audit["source_last_data_edit_utc"], "2025-12-03T16:51:37.628000Z")
        self.assertEqual(features[-1], added)
        candidate = source._candidate_result(audit, "candidate", "v-test", "capture-hash",
                                             reused=False, root=Path("captures"))
        self.assertEqual(candidate["source_reported_count"], 14)
        self.assertTrue(us_courts_appeals_circuits_complete(candidate).passed)
        from dagster_hifld.assets.us_courts_appeals_circuits import us_courts_appeals_circuits_drift
        drift = us_courts_appeals_circuits_drift(candidate)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(list(us_courts_appeals_circuits_drift.check_specs)[0].blocking)
        baseline = {**candidate, "version": "v-approved", "release_accepted": True,
                    "feature_count": FIXTURE_EXPECTED_COUNT, "source_id_sha256": FIXTURE_EXPECTED_ID_SHA256}
        compared = approved_baseline_drift(candidate, baseline)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.severity, AssetCheckSeverity.WARN)
        self.assertEqual(compared.metadata["feature_count_delta"].value, 1)
        self.assertTrue(compared.metadata["native_id_roster_changed"].value)
        self.assertEqual(pq.read_table(__import__("io").BytesIO(
            source._parquet_bytes(fields, features))).num_rows, 14)

    def test_torn_duplicate_and_missing_capture_witnesses_are_refused(self):
        cases = [
            ("same count changed IDs", lambda d: d["ids_after"]["objectIds"].__setitem__(-1, 27 + 1),
             "source_count_or_id_roster_drift"),
            ("count changed", lambda d: d["count_after"].update(count=13 + 2),
             "source_count_or_id_roster_drift"),
            ("duplicate IDs", lambda d: [d[f"ids_{side}"]["objectIds"].__setitem__(-1, 1)
                                        for side in ("before", "after")], "source_count_or_id_roster_drift"),
            ("missing witness", lambda d: d.pop("count_after"), "source_capture_witness_missing"),
            ("missing page", lambda d: d.pop("page_000"), "source_capture_name_roster_drift"),
            ("omitted feature", lambda d: d["page_000"]["features"].pop(), "source_page_population_incomplete"),
            ("empty population", lambda d: [(d[f"ids_{side}"].update(objectIds=[]),
                                             d[f"count_{side}"].update(count=0))
                                            for side in ("before", "after")], "source_population_empty"),
        ]
        for label, mutate, reason in cases:
            with self.subTest(label=label):
                docs = self._added_documents()
                mutate(docs)
                with self.assertRaisesRegex(ValueError, reason):
                    source._validate(self._captures(docs))

    def test_complete_removal_and_all_null_geometry_use_observed_counts(self):
        docs = self._documents()
        docs["page_000"]["features"].pop(0)
        for feature in docs["page_000"]["features"]:
            feature.pop("geometry", None)
        for side in ("before", "after"):
            docs[f"ids_{side}"]["objectIds"] = list(range(2, 14))
            docs[f"count_{side}"]["count"] = 12
        fields, features, audit = source._validate(self._captures(docs))
        self.assertEqual(audit["source_feature_count"], 12)
        self.assertEqual(audit["source_id_min"], 2)
        self.assertEqual(audit["geometry_null_count"], 12)
        self.assertIsNone(audit["geometry_bounds"])
        table = pq.read_table(__import__("io").BytesIO(source._parquet_bytes(fields, features)))
        self.assertEqual(table.column("OBJECTID").to_pylist(), list(range(2, 14)))
        self.assertEqual(table.column("geometry").to_pylist(), [None] * 12)

    def test_dynamic_acquisition_replay_and_transport_integrity(self):
        docs = self._added_documents()
        docs["page_001"] = {**docs["page_000"], "features": [docs["page_000"]["features"].pop()]}
        ids = docs["ids_before"]["objectIds"]
        with tempfile.TemporaryDirectory() as temporary, patch.object(source, "PAGE_SIZE", 13), \
                patch.object(source, "validate_construction_code_identity"), \
                patch.object(capture_common, "validate_construction_code_identity"):
            responses = {}
            for name, url in source._capture_roster(ids):
                responses.setdefault(url, []).append(name)

            def handle(request):
                name = responses[str(request.url)].pop(0)
                return httpx.Response(200, stream=httpx.ByteStream(json.dumps(docs[name]).encode()),
                                      headers={"Content-Type": "application/json"})

            client = httpx.Client(transport=httpx.MockTransport(handle))
            storage = StagingStorageResource(local_dir=str(Path(temporary) / "stage"))
            with patch.object(source.httpx, "Client", return_value=client):
                result = source.capture_us_courts_appeals_circuits(storage, version="fixture-capture",
                    capture_directory=str(Path(temporary) / "captures"))
            self.assertEqual(result["feature_count"], 13 + 1)
            self.assertEqual(result["source_reported_count"], 13 + 1)
            self.assertEqual(result["source_id_max"], 27)
            self.assertTrue(us_courts_appeals_circuits_complete(result).passed)
            root = Path(result["capture_directory"])
            self.assertTrue((root / "page_001.body").exists())
            self.assertTrue(all(not remaining for remaining in responses.values()))
            replay = source._read_replay(root)
            self.assertEqual(source._validate(replay)[2]["source_feature_count"], 13 + 1)
            with patch.object(source.httpx, "Client", side_effect=AssertionError("network on replay")):
                reused = source.capture_us_courts_appeals_circuits(storage, version="fixture-replay", capture_directory=str(root))
            self.assertEqual(reused["version"], result["version"])
            self.assertTrue(reused["reused"])
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            receipt_path = root / "page_001.receipt.json"
            receipt = json.loads(receipt_path.read_bytes())
            receipt["url"] = source.LAYER_URL + "/query?objectIds=999"
            receipt_path.write_bytes(_canonical(receipt))
            manifest["receipts"]["page_001"] = receipt
            manifest["artifact_roster"] = source.capture_roster_digests(root, exclude={"capture_manifest.json"})
            (root / "capture_manifest.json").write_bytes(_canonical(manifest))
            with self.assertRaisesRegex(ValueError, "request|url"):
                source._read_replay(root)
            for name, url in source._capture_roster(ids):
                responses.setdefault(url, []).append(name)
            docs["count_after"]["count"] += 1
            failed_root = Path(temporary) / "torn"
            client = httpx.Client(transport=httpx.MockTransport(handle))
            with patch.object(source.httpx, "Client", return_value=client):
                with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
                    source.capture_us_courts_appeals_circuits(storage, version="torn-capture",
                        capture_directory=str(failed_root))
            failure = next(failed_root.rglob("capture_manifest.json"))
            self.assertFalse(json.loads(failure.read_bytes())["complete"])
            self.assertTrue((failure.parent / "ids_after.body").exists())
            self.assertTrue(any(key.endswith("/raw/ids_after.body")
                                for key in storage.list_prefix(f"_quarantine/{source.DATASET_SLUG}")))

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
        for oid in FIXTURE_EXPECTED_IDS:
            circuit = CIRCUIT_BY_OID[oid]
            attrs = {"OBJECTID": oid, "Circuit": circuit, "Shape__Area": None,
                     "Shape__Length": None, "GSA_Teams": None}
            feature = {"attributes": attrs}
            if oid in FIXTURE_EXPECTED_NULL_GEOMETRY_IDS:
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
                 "editingInfo": dict(FIXTURE_EXPECTED_EDITING_INFO),
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
            "count_before": {"count": FIXTURE_EXPECTED_COUNT},
            "ids_before": {"objectIds": FIXTURE_EXPECTED_IDS},
            "service_after": json.loads(json.dumps(service)),
            "layer_after": json.loads(json.dumps(layer)),
            "count_after": {"count": FIXTURE_EXPECTED_COUNT},
            "ids_after": {"objectIds": FIXTURE_EXPECTED_IDS},
        }
        page_fields = [{key: value for key, value in field.items()
                        if key not in ("nullable", "editable")}
                       for field in source.EXPECTED_FIELD_DECLARATIONS]
        for page_index in range(FIXTURE_PAGE_COUNT):
            chunk = FIXTURE_EXPECTED_IDS[page_index * source.PAGE_SIZE:
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
            {"feature_count": 13, "source_reported_count": 13, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = us_courts_appeals_circuits_complete(
            {"feature_count": 12, "source_reported_count": 13, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_single_page_covers_source_ids(self):
        from urllib.parse import parse_qs, urlsplit
        urls = dict(source._capture_roster(FIXTURE_EXPECTED_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         ["page_000"])
        params = parse_qs(urlsplit(urls["page_000"]).query)
        self.assertEqual([int(value) for value in params["objectIds"][0].split(",")],
                         FIXTURE_EXPECTED_IDS)
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
        self.assertEqual(source._sha(_canonical(source.EXPECTED_FIELD_DECLARATIONS)),
                         "bbe1329112e01bdf02081968554745f24acfba6b80d86ea9c415ffb7e235deec")
        self.assertIn("d8ff459672c746a4b3d53bae9330701a", source.LANDING_URL)

    def test_runtime_has_no_snapshot_population_pins(self):
        for name in ("EXPECTED_COUNT", "EXPECTED_IDS", "EXPECTED_ID_SHA256",
                     "EXPECTED_RECORD_SHA256", "EXPECTED_GEOMETRY_SHA256",
                     "EXPECTED_CIRCUIT_ROSTER", "EXPECTED_NULL_GEOMETRY_IDS", "EXPECTED_EDITING_INFO"):
            self.assertFalse(hasattr(source, name), name)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 13)
        self.assertEqual(sorted(row["attributes"]["OBJECTID"] for row in features),
                         FIXTURE_EXPECTED_IDS)
        self.assertEqual(audit["source_id_sha256"], FIXTURE_EXPECTED_ID_SHA256)
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
        self.assertEqual(oid_column, FIXTURE_EXPECTED_IDS)
        self.assertEqual(table.column("Circuit").to_pylist(),
                         [CIRCUIT_BY_OID[oid] for oid in FIXTURE_EXPECTED_IDS])
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
            ("population after", lambda d: d["ids_after"].update(objectIds=FIXTURE_EXPECTED_IDS[:-1]),
             "source_count_or_id_roster_drift"),
            ("id roster gap filled", lambda d: d["ids_before"].update(objectIds=[*range(1, 14, 1)][:-1]),
             "source_count_or_id_roster_drift"),
            ("count before", lambda d: d["count_before"].update(count=12),
             "source_count_or_id_roster_drift"),
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
             "source_service_or_layer_changed_during_capture"),
            ("invalid edit timestamp", lambda d: d["layer_before"]["editingInfo"].update(dataLastEditDate="invalid"),
             "source_layer_editing_info_invalid"),
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
        spike = {"rings": [[[0.0, 20.0], [0.0, 24.0], [2.0, 24.0], [2.0, 22.0],
                            [2.0, 24.0], [4.0, 24.0], [4.0, 20.0], [0.0, 20.0]]]}
        self.assertFalse(from_wkb(source._esri_polygon_wkb(spike)).is_valid)
        docs = self._documents()
        docs["page_000"]["features"][0]["geometry"] = dict(spike)
        docs["page_000"]["features"][0]["attributes"].update(Shape__Area=None, Shape__Length=None)
        with self.assertRaisesRegex(ValueError, "source_geometry_invalid"):
            source._validate(self._captures(docs))

    def test_source_edits_are_preserved_without_population_pins(self):
        cases = [
            ("circuit roster", lambda d: d["page_000"]["features"][0]["attributes"].update(Circuit="99")),
            ("null geometry roster", lambda d: d["page_000"]["features"][11].pop("geometry")),
            ("null geometry gained", lambda d: d["page_000"]["features"][12].update(
                geometry={"rings": [self._shell(13)]})),
        ]
        for label, mutate in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                fields, features, audit = source._validate(self._captures(docs))
                self.assertEqual(features, docs["page_000"]["features"])
                self.assertEqual(audit["source_record_sha256"], source._sha(_canonical(
                    [[row["attributes"]["OBJECTID"], row["attributes"]] for row in features])))
                self.assertEqual(audit["source_geometry_sha256"], source._sha(_canonical(
                    [[row["attributes"]["OBJECTID"], row.get("geometry")] for row in features])))
                self.assertEqual(audit["geometry_null_ids"], [row["attributes"]["OBJECTID"]
                    for row in features if row.get("geometry") is None])
                table = pq.read_table(__import__("io").BytesIO(source._parquet_bytes(fields, features)))
                for field in fields:
                    self.assertEqual(table.column(field["name"]).to_pylist(),
                                     [row["attributes"][field["name"]] for row in features])
                for row, value in zip(features, table.column("geometry").to_pylist()):
                    self.assertEqual(value, None if row.get("geometry") is None
                                     else source._esri_polygon_wkb(row["geometry"]))

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_us_courts_appeals_circuits(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

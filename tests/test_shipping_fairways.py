import io
import json
import struct
import tempfile
import unittest
import unittest.mock
from pathlib import Path

import httpx
import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.shipping_fairways import (
    shipping_fairways, shipping_fairways_complete,
    shipping_fairways_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import shipping_fairways as source
from dagster_hifld.sources.epa_frs_common import _canonical


class ShippingFairwaysTests(unittest.TestCase):
    def _shell(self, oid):
        x0 = -140.0 + (oid % 100) * 0.003
        y0 = 20.0 + (oid // 100) * 0.003
        return [[x0, y0], [x0, y0 + 0.001], [x0 + 0.001, y0 + 0.001], [x0 + 0.001, y0], [x0, y0]]

    def _documents(self):
        service = {"mapName": "MarineTransportation", "copyrightText": source.SERVICE_COPYRIGHT,
                   "maxRecordCount": 10000, "layers": source.EXPECTED_SERVICE_LAYERS}
        layer = {"id": 0, "name": source.EXPECTED_LAYER_NAME,
                 "geometryType": "esriGeometryPolygon", "maxRecordCount": 10000,
                 "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "service_directory": {"services": [{"name": "NavigationChartData/MarineTransportation",
                                                "type": "MapServer"}]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            "service_after": service, "layer_after": layer,
            "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS},
        }
        for page_index in range((source.EXPECTED_COUNT + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
            rows = []
            for oid in source.EXPECTED_IDS[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]:
                attrs = {field["name"]: oid if field["name"] == "OBJECTID" else
                         f"{field['name']}-{oid}" for field in source.ATTRIBUTE_FIELDS}
                rings = [self._shell(oid)]
                if oid % 50 == 0:
                    shell = rings[0]
                    inset = 0.0002
                    rings.append([[shell[0][0] + inset, shell[0][1] + inset],
                                  [shell[2][0] - inset, shell[0][1] + inset],
                                  [shell[2][0] - inset, shell[2][1] - inset],
                                  [shell[0][0] + inset, shell[2][1] - inset],
                                  [shell[0][0] + inset, shell[0][1] + inset]])
                rows.append({"attributes": attrs, "geometry": {"rings": rings}})
            docs[f"page_{page_index:03d}"] = {"spatialReference": {"wkid": 4326}, "features": rows}
        return docs

    def _captures(self, docs):
        captures = {name: (_canonical(value), {"status": 200}) for name, value in docs.items()}
        inport = ("<html><body>Shipping Fairways, Lanes, and Zones for US waters "
                  "https://encdirect.noaa.gov/theme_layers/data/shipping_lanes/shippinglanes.zip "
                  "https://encdirect.noaa.gov/arcgis/rest/services/NavigationChartData/MarineTransportation/MapServer "
                  "Office of Coast Survey</body></html>")
        captures["inport_item"] = (inport.encode(), {"status": 200})
        return captures

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(shipping_fairways, hifld_ingest_assets)
        self.assertEqual(shipping_fairways_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = shipping_fairways_complete({"feature_count": 346, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = shipping_fairways_complete({"feature_count": 345, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_covers_producer_lineage_and_sparse_ids(self):
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        self.assertEqual(sorted(urls), sorted(source.CAPTURE_NAMES))
        self.assertEqual(urls["inport_item"], source.INPORT_URL)
        self.assertEqual(urls["service_directory"], source.SERVICE_DIRECTORY_URL)
        pages = [name for name in sorted(urls) if name.startswith("page_")]
        self.assertEqual(pages, ["page_000", "page_001", "page_002", "page_003"])
        self.assertIn("objectIds=144533%2C144534%2C144535", urls["page_000"])
        self.assertIn("144544%2C144833", urls["page_000"])
        self.assertNotIn("144889", urls["page_000"])
        self.assertIn("objectIds=144889", urls["page_001"])
        self.assertIn("145088", urls["page_002"])
        self.assertIn("objectIds=145089", urls["page_003"])
        self.assertIn("145166", urls["page_003"])
        self.assertNotIn("144544", urls["page_003"])
        for name in pages:
            self.assertIn("returnZ=true", urls[name])
            self.assertIn("returnM=true", urls[name])
        self.assertEqual(len(urls["page_000"].split("objectIds=")[1].split("&")[0].split("%2C")), 100)
        self.assertEqual(len(urls["page_000"].split("objectIds=")[1].split("&")[0].split("%2C")), 100)
        self.assertEqual(len(urls["page_003"].split("objectIds=")[1].split("&")[0].split("%2C")), 78)
        self.assertEqual(len(source.EXPECTED_IDS), 346)
        self.assertEqual(len(set(source.EXPECTED_IDS)), 346)
        self.assertEqual(source.EXPECTED_IDS, sorted(source.EXPECTED_IDS))
        self.assertEqual(min(source.EXPECTED_IDS), 144533)
        self.assertEqual(max(source.EXPECTED_IDS), 145166)
        self.assertNotEqual(source.EXPECTED_IDS, list(range(min(source.EXPECTED_IDS), max(source.EXPECTED_IDS) + 1)))

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 346)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_vertex_count"], 1765)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], 346)
        self.assertEqual(audit["source_id_min"], 144533)
        self.assertEqual(audit["source_id_max"], 145166)
        self.assertEqual([row["attributes"]["OBJECTID"] for row in features], source.EXPECTED_IDS)
        self.assertEqual(features[0]["attributes"]["THEMELAYER"], "THEMELAYER-144533")
        self.assertIn("NOAA OCS ENCDirect", audit["source_coverage_scope"])
        self.assertIn("US Army Corps of Engineers", audit["inventory_agency_column_mismatch"])

    def test_polygon_wkb_is_xy_only_and_preserves_ring_structure(self):
        shell = self._shell(1)
        single = source._esri_polygon_wkb({"rings": [shell]})
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 3))
        disjoint = source._esri_polygon_wkb({"rings": [shell, self._shell(2)]})
        self.assertEqual(struct.unpack("<BI", disjoint[:5]), (1, 6))
        inset = 0.0002
        hole = [[shell[0][0] + inset, shell[0][1] + inset],
                [shell[2][0] - inset, shell[0][1] + inset],
                [shell[2][0] - inset, shell[2][1] - inset],
                [shell[0][0] + inset, shell[2][1] - inset],
                [shell[0][0] + inset, shell[0][1] + inset]]
        with_hole = source._esri_polygon_wkb({"rings": [shell, hole]})
        self.assertEqual(struct.unpack("<BI", with_hole[:5]), (1, 3))
        for flags in ({"hasZ": True}, {"hasM": True}, {"hasZ": True, "hasM": True}):
            with self.assertRaisesRegex(ValueError, "source_polygon_dimensional_flags_rejected"):
                source._esri_polygon_wkb({"rings": [shell], **flags})
        unflagged = [[0.0, 0.0, 1.0], [0.0, 1.0, 1.0], [1.0, 1.0, 1.0], [1.0, 0.0, 1.0], [0.0, 0.0, 1.0]]
        with self.assertRaisesRegex(ValueError, "source_polygon_zm_flags_do_not_match_coordinates"):
            source._esri_polygon_wkb({"rings": [unflagged]})

    def test_population_schema_lineage_and_crs_drift_fail_closed(self):
        cases = [
            ("population", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_count_or_id_population_drift"),
            ("id gap", lambda d: d["ids_before"].update(
                objectIds=[oid for oid in source.EXPECTED_IDS if oid != 144833]),
             "source_count_or_id_population_drift"),
            ("contiguous substitution", lambda d: d["ids_before"].update(
                objectIds=list(range(144533, 144533 + 346))),
             "source_count_or_id_population_drift"),
            ("schema", lambda d: d["layer_before"]["fields"][1].update(name="renamed"),
             "source_field_schema_drift"),
            ("alias", lambda d: d["layer_before"]["fields"][2].update(alias="OTHER"),
             "source_field_schema_drift"),
            ("transfer", lambda d: d["page_000"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_000"].update(spatialReference={"wkid": 3857}), "crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                OBJECTID=d["page_000"]["features"][0]["attributes"]["OBJECTID"]),
             "source_page_id_population_drift"),
            ("layer dimensional", lambda d: d["layer_before"].update(hasZ=True),
             "source_layer_dimensional_drift"),
            ("page dimensional", lambda d: d["page_000"].update(hasZ=True),
             "source_page_dimensional_drift"),
            ("geometry dimensional", lambda d: d["page_000"]["features"][0]["geometry"].update(hasZ=True),
             "source_geometry_dimensional_drift"),
            ("open ring", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].pop(),
             "source_polygon_ring_not_closed"),
            ("outside coverage", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].__setitem__(
                2, [-200.0, 35.5]),
             "source_geometry_outside_declared_coverage"),
            ("service drift", lambda d: d["service_after"].update(mapName="Other"),
             "source_service_or_layer_changed_during_capture"),
            ("service roster", lambda d: d["service_before"]["layers"].pop(),
             "official_encdirect_service_drift"),
            ("inport lineage", lambda d: None,
             "inport_lineage_record_drift"),
            ("directory drift", lambda d: d["service_directory"].update(services=[]),
             "official_encdirect_service_directory_drift"),
            ("attribute type", lambda d: d["page_000"]["features"][0]["attributes"].update(OBJNAM=7),
             "source_string_value_type_drift"),
            ("hole without shell", lambda d: d["page_000"]["features"][0]["geometry"]["rings"].append(
                [[-140.5, 20.5], [-140.45, 20.5], [-140.45, 20.55], [-140.5, 20.55], [-140.5, 20.5]]),
             "source_hole_ownership_ambiguous"),
            ("shell missing", lambda d: d["page_000"]["features"][0]["geometry"].update(
                rings=[[[-140.5, 20.5], [-140.45, 20.5], [-140.45, 20.55], [-140.5, 20.55], [-140.5, 20.5]]]),
             "source_polygon_shell_missing"),
        ]
        for index, (label, mutate, expected) in enumerate(cases):
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                if label == "inport lineage":
                    captures = self._captures(docs)
                    captures["inport_item"] = (b"<html>unrelated page</html>", {"status": 200})
                    with self.assertRaisesRegex(ValueError, expected):
                        source._validate(captures)
                    continue
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_shipping_fairways(storage, version="../outside")

    def _mock_client(self, responses):
        class Body(httpx.SyncByteStream):
            def __init__(self, data):
                self._data = data

            def __iter__(self):
                yield self._data

        queue = [httpx.Response(status, stream=Body(body)) for status, body in responses]

        def handler(request):
            if not queue:
                raise AssertionError("unexpected extra request")
            return queue.pop(0)

        return httpx.Client(transport=httpx.MockTransport(handler))

    def test_capture_with_retry_survives_flaky_service_and_fails_closed(self):
        error_body = json.dumps({"error": {"code": 500,
                                           "message": "Error handling service request :Could not find service. "
                                                      "Service may be stopped or it may not be configured.",
                                           "details": []}}).encode()
        ok_body = json.dumps({"count": 346}).encode()
        responses = [(500, error_body), (200, error_body), (200, ok_body)]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self._mock_client(responses) as client, \
                    unittest.mock.patch.object(source, "_retry_budget", lambda attempt: 0):
                body, receipt = source._capture_with_retry(client, source.LAYER_URL, root, "count_before")
            self.assertEqual(json.loads(body), {"count": 346})
            self.assertEqual(receipt["status"], 200)
            self.assertEqual(sorted(path.name for path in root.iterdir()),
                             ["count_before.body", "count_before.receipt.json", "count_before.wire"])
            final_receipt = json.loads((root / "count_before.receipt.json").read_bytes())
            self.assertEqual(final_receipt["sha256"], final_receipt["sha256"] and source._sha(body))
        bad = [(400, json.dumps({"error": {"code": 400, "message": "Failed to execute query.",
                                           "details": []}}).encode())]
        with tempfile.TemporaryDirectory() as tmp:
            with self._mock_client(bad) as client, \
                    unittest.mock.patch.object(source, "_retry_budget", lambda attempt: 0):
                with self.assertRaisesRegex(ValueError, "incomplete_source_capture:count_before"):
                    source._capture_with_retry(client, source.LAYER_URL, Path(tmp), "count_before")
            retained = json.loads((Path(tmp) / "count_before.receipt.json").read_bytes())
            self.assertEqual(retained["status"], 400)
        inner_error = [(200, json.dumps({"error": {"code": 400, "message": "Failed to execute query.",
                                                   "details": []}}).encode())]
        with tempfile.TemporaryDirectory() as tmp:
            with self._mock_client(inner_error) as client, \
                    unittest.mock.patch.object(source, "_retry_budget", lambda attempt: 0):
                with self.assertRaisesRegex(ValueError, "source_endpoint_error:count_before:400"):
                    source._capture_with_retry(client, source.LAYER_URL, Path(tmp), "count_before")
            self.assertTrue((Path(tmp) / "count_before.body").is_file())

    def test_candidate_parquet_roundtrip_preserves_native_types_and_crs(self):
        docs = self._documents()
        fields, features, audit = source._validate(self._captures(docs))
        parquet = source._parquet_bytes(fields, features)
        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.num_rows, 346)
        self.assertEqual(table.column_names, ["OBJECTID", "THEMELAYER", "INFORM", "OBJNAM", "geometry"])
        self.assertEqual(str(table.schema.field("OBJECTID").type), "int64")
        self.assertEqual(str(table.schema.field("THEMELAYER").type), "string")
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["primary_column"], "geometry")
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
        self.assertEqual(sorted(geo["columns"]["geometry"]["geometry_types"]), ["MultiPolygon", "Polygon"])


if __name__ == "__main__":
    unittest.main()

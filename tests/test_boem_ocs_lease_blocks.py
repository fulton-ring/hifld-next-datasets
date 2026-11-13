"""Focused contract and repeat-acquisition tests for the BOEM OCS lease-block adapter."""
from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import boem_ocs_lease_blocks as blocks
from dagster_hifld.sources import epa_frs_common


def _body(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


BLOCK_RING = [[-93.109375, 29.61602783203125], [-93.10858154296875, 29.575439453125],
              [-93.15496826171875, 29.5748291015625], [-93.1557, 29.6154],
              [-93.109375, 29.61602783203125]]


def _fixture():
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False, "length": 0},
        {"name": "PROT_NUMBER", "type": "esriFieldTypeString", "nullable": True, "length": 7},
        {"name": "BLOCK_NUMBER", "type": "esriFieldTypeString", "nullable": True, "length": 6},
        {"name": "BLK_FED_APRV_DT", "type": "esriFieldTypeString", "nullable": True, "length": 11},
        {"name": "Shape__Area", "type": "esriFieldTypeDouble", "nullable": True, "length": 0},
        {"name": "Shape__Length", "type": "esriFieldTypeDouble", "nullable": True, "length": 0},
    ]
    ids = [643213655, 643213656, 643213657, 644434609]
    attrs_rows = [
        {"PROT_NUMBER": "LA1", "BLOCK_NUMBER": "   71", "BLK_FED_APRV_DT": "01-JUL-2011",
         "Shape__Area": 0.00188226, "Shape__Length": 0.17392168171682998},
        {"PROT_NUMBER": "LA1", "BLOCK_NUMBER": "   72", "BLK_FED_APRV_DT": "01-JUL-2011",
         "Shape__Area": 0.00188832, "Shape__Length": 0.17421980968199502},
        {"PROT_NUMBER": "NN08-01", "BLOCK_NUMBER": " 7101", "BLK_FED_APRV_DT": "03-JUN-2003",
         "Shape__Area": 0.0021, "Shape__Length": None},
        {"PROT_NUMBER": "NN08-01", "BLOCK_NUMBER": " 7102", "BLK_FED_APRV_DT": None,
         "Shape__Area": 0, "Shape__Length": 0},
    ]
    geometries = [
        {"rings": [BLOCK_RING]},
        {"rings": [[[ -93.2, 29.6], [ -93.16, 29.6], [ -93.16, 29.57], [ -93.2, 29.57],
                    [ -93.2, 29.6]],
                   [[ -93.1, 29.6], [ -93.05, 29.6], [ -93.05, 29.57], [ -93.1, 29.57],
                    [ -93.1, 29.6]]]},
        {"rings": [[[ -160.5, 61.0], [ -160.4, 61.0], [ -160.4, 60.9], [ -160.5, 60.9],
                    [ -160.5, 61.0]]]},
        {"rings": []},
    ]
    features = [{"attributes": {"OBJECTID": oid, **attrs}, "geometry": geom}
                for oid, attrs, geom in zip(ids, attrs_rows, geometries)]
    layer = {"id": blocks.LAYER_ID, "name": blocks.LAYER_NAME,
             "geometryType": "esriGeometryPolygon", "objectIdField": "OBJECTID",
             "hasZ": False, "hasM": False, "maxRecordCount": 10000,
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"serviceDescription": "Map Service contains primary data types created by "
                                     "both the Bureau of Ocean Energy Management (BOEM) and "
                                     "the Bureau of Safety and Environmental Enforcement "
                                     "(BSEE). Contact the Geospatial Services Division at "
                                     "mappingboundarybranch@boem.gov.",
               "spatialReference": {"wkid": 4269, "latestWkid": 4269},
               "layers": [{"id": blocks.LAYER_ID, "name": blocks.LAYER_NAME}]}
    id_doc = {"objectIds": ids}
    count_doc = {"count": len(ids)}
    page = {"spatialReference": {"wkid": 4269, "latestWkid": 4269}, "features": features}
    docs = {
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "ids_before": id_doc, "ids_after": id_doc,
        "count_before": count_doc, "count_after": count_doc,
        "page_000": page,
    }
    receipt = {"status": 200, "complete_body": True, "observed_at": "2026-01-01T00:00:00Z"}
    captures = {}
    page_urls = [blocks._query(where="1=1", outFields="*", returnGeometry="true", outSR="4269",
                               orderByFields="OBJECTID ASC", resultOffset=page * blocks.PAGE_SIZE,
                               resultRecordCount=blocks.PAGE_SIZE)
                 for page in range(0, (len(ids) + blocks.PAGE_SIZE - 1) // blocks.PAGE_SIZE)]
    for key, value in docs.items():
        body = _body(value)
        url = (blocks.SERVICE_URL + "?f=pjson" if key.startswith("service_") else
               blocks.LAYER_URL + "?f=pjson" if key.startswith("layer_") else
               blocks.LAYER_URL + "/query?where=1%3D1&returnCountOnly=true&f=json"
               if key.startswith("count_") else
               blocks.LAYER_URL + "/query?where=1%3D1&returnIdsOnly=true&f=json"
               if key.startswith("ids_") else
               page_urls[int(key.split("_")[1])])
        captures[key] = (body, {**receipt, "url": url, "final_url": url, "bytes": len(body),
                                "sha256": hashlib.sha256(body).hexdigest(),
                                "wire_bytes": len(body),
                                "wire_sha256": hashlib.sha256(body).hexdigest()})
    return captures


_UNPINNED = {
    "EXPECTED_COUNT": None,
    "EXPECTED_IDS_SHA256": "",
    "EXPECTED_SCHEMA_SHA256": "",
    "EXPECTED_RECORD_SHA256": "",
    "EXPECTED_GEOMETRY_SHA256": "",
    "EXPECTED_NULL_GEOMETRY_COUNT": None,
    "EXPECTED_RING_COUNT_DISTRIBUTION": None,
    "EXPECTED_UNIQUE_PROT_NUMBER_COUNT": None,
    "EXPECTED_UNIQUE_BLOCK_KEY_COUNT": None,
    "EXPECTED_DUPLICATE_BLOCK_KEY_COUNT": None,
    "EXPECTED_OBSERVED_BOUNDS": None,
}


@contextmanager
def fixture_pins():
    """Pin every module baseline to the fixture capture's own audit values."""
    with ExitStack() as stack:
        for name, value in _UNPINNED.items():
            stack.enter_context(patch.object(blocks, name, value))
        audit = blocks._validate(_fixture())[3]
        stack.close()
        pins = {
            "EXPECTED_COUNT": audit["feature_count"],
            "EXPECTED_IDS_SHA256": audit["source_id_sha256"],
            "EXPECTED_SCHEMA_SHA256": audit["source_schema_sha256"],
            "EXPECTED_RECORD_SHA256": audit["source_record_sha256"],
            "EXPECTED_GEOMETRY_SHA256": audit["source_geometry_sha256"],
            "EXPECTED_NULL_GEOMETRY_COUNT": audit["null_geometry_count"],
            "EXPECTED_RING_COUNT_DISTRIBUTION": audit["ring_count_distribution"],
            "EXPECTED_UNIQUE_PROT_NUMBER_COUNT": audit["unique_prot_number_count"],
            "EXPECTED_UNIQUE_BLOCK_KEY_COUNT": audit["unique_block_key_count"],
            "EXPECTED_DUPLICATE_BLOCK_KEY_COUNT": audit["duplicate_block_key_count"],
            "EXPECTED_OBSERVED_BOUNDS": audit["geometry_bounds"],
        }
        for name, value in pins.items():
            stack.enter_context(patch.object(blocks, name, value))
        yield audit


class BoemOcsLeaseBlocksTests(unittest.TestCase):
    def test_native_fields_polygon_nulls_and_audit_counts(self):
        with fixture_pins():
            _, fields, features, audit = blocks._validate(_fixture())
        self.assertEqual([field["name"] for field in fields], blocks.FIELD_NAMES)
        self.assertEqual(len(features), 4)
        self.assertEqual(audit["unique_objectid_count"], 4)
        self.assertEqual(audit["unique_block_key_count"], 4)
        self.assertEqual(audit["duplicate_block_key_count"], 0)
        self.assertEqual(audit["null_counts_by_field"]["BLK_FED_APRV_DT"], 1)
        self.assertEqual(audit["null_counts_by_field"]["Shape__Length"], 1)
        self.assertEqual(audit["null_geometry_count"], 1)
        self.assertEqual(audit["ring_count_distribution"], {1: 2, 2: 1})
        self.assertEqual(audit["source_crs_epsg"], 4269)
        self.assertEqual(audit["geometry_bounds"][0], -160.5)
        self.assertEqual(audit["geometry_bounds"][3], 61.0)

    def test_spatial_reference_alias_is_valid_but_contradictions_are_rejected(self):
        with fixture_pins():
            captures = _fixture()
            _, _, _, audit = blocks._validate(captures)
            self.assertEqual(audit["source_crs_epsg"], 4269)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["layer_before"][0])
            doc["extent"]["spatialReference"] = {"wkid": 4326, "latestWkid": 4269}
            candidate["layer_before"] = (_body(doc), candidate["layer_before"][1])
            with self.assertRaisesRegex(ValueError, "source_layer_spatial_reference_drift"):
                blocks._validate(candidate)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["page_000"][0])
            doc["spatialReference"] = {"wkid": 4326, "latestWkid": 4269}
            candidate["page_000"] = (_body(doc), candidate["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_spatial_reference_drift"):
                blocks._validate(candidate)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["page_000"][0])
            doc["features"][0]["geometry"]["spatialReference"] = {"wkid": 3857}
            candidate["page_000"] = (_body(doc), candidate["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_geometry_spatial_reference_drift"):
                blocks._validate(candidate)

    def test_missing_id_truncation_and_schema_drift_are_rejected(self):
        with fixture_pins():
            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"].pop()
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"].pop("OBJECTID")
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["OBJECTID"] = 999
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["UNEXPECTED_COLUMN"] = "x"
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_feature_field_roster_drift"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["BLOCK_NUMBER"] = 71
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_string_field_type_drift"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["Shape__Area"] = "big"
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_double_field_type_drift"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"] = {"x": -93.1, "y": 29.6}
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_geometry_invalid"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"]["rings"][0][2][0] = -200.0
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_coordinates_invalid"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"]["rings"][0].pop(-1)
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_ring_unclosed"):
                blocks._validate(captures)

            # Nested (hole) rings fail closed: the producer publishes disjoint
            # outer rings only.
            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            inner_ring = [[-93.12, 29.60], [-93.12, 29.59], [-93.11, 29.59],
                          [-93.11, 29.60], [-93.12, 29.60]]
            doc["features"][0]["geometry"]["rings"] = [BLOCK_RING, inner_ring]
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_nested_rings_unexpected"):
                blocks._validate(captures)

            # Duplicate (PROT_NUMBER, BLOCK_NUMBER) keys are allowed (the
            # producer publishes split blocks as same-key rows) but are pinned:
            # with the duplicate gate pinned to 0 the mutated fixture refuses.
            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["BLOCK_NUMBER"] = "   72"
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with patch.object(blocks, "EXPECTED_DUPLICATE_BLOCK_KEY_COUNT", 0):
                with self.assertRaisesRegex(ValueError, "source_block_key_uniqueness_drift"):
                    blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["exceededTransferLimit"] = True
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_error_or_truncation"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][1]["attributes"]["OBJECTID"], doc["features"][0]["attributes"]["OBJECTID"] = (
                doc["features"][0]["attributes"]["OBJECTID"], doc["features"][1]["attributes"]["OBJECTID"])
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_order_drift"):
                blocks._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["layer_before"][0])
            doc["fields"][1]["type"] = "esriFieldTypeInteger"
            captures["layer_before"] = (_body(doc), captures["layer_before"][1])
            with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
                blocks._validate(captures)

    def test_pinned_baselines_are_enforced_once_set(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with fixture_pins() as audit:
                fixture = _fixture()
                with patch.object(blocks, "_load_offline", return_value=copy.deepcopy(fixture)), \
                        patch.object(blocks, "_geoparquet", return_value=b"candidate"), \
                        patch.object(blocks, "processing_identity",
                                     return_value={"code_commit": "a" * 40,
                                                   "files": {"x.py": "b" * 64}}), \
                        patch.object(blocks, "validate_construction_code_identity",
                                     return_value=None), \
                        patch.object(epa_frs_common, "validate_construction_code_identity",
                                     return_value=None):
                    result = blocks.capture_boem_ocs_lease_blocks(
                        storage, offline_capture_directory=tmp)
                    self.assertFalse(result["reused"])
                    pins = [
                        ("EXPECTED_IDS_SHA256", audit["source_id_sha256"]),
                        ("EXPECTED_RECORD_SHA256", audit["source_record_sha256"]),
                        ("EXPECTED_GEOMETRY_SHA256", audit["source_geometry_sha256"]),
                        ("EXPECTED_NULL_GEOMETRY_COUNT", audit["null_geometry_count"]),
                        ("EXPECTED_RING_COUNT_DISTRIBUTION", audit["ring_count_distribution"]),
                        ("EXPECTED_DUPLICATE_BLOCK_KEY_COUNT",
                         audit["duplicate_block_key_count"]),
                        ("EXPECTED_OBSERVED_BOUNDS", audit["geometry_bounds"]),
                    ]
                    for name, value in pins:
                        with self.subTest(pin=name):
                            with patch.object(blocks, name, value):
                                again = blocks.capture_boem_ocs_lease_blocks(
                                    storage, offline_capture_directory=tmp)
                                self.assertTrue(again["reused"])
                    # A pinned baseline set to a wrong value refuses the capture.
                    for name, value in (("EXPECTED_IDS_SHA256", "0" * 64),
                                        ("EXPECTED_COUNT", audit["feature_count"] + 1)):
                        with self.subTest(refusal=name):
                            with patch.object(blocks, name, value):
                                with self.assertRaisesRegex(ValueError, "source_"):
                                    blocks.capture_boem_ocs_lease_blocks(
                                        storage, offline_capture_directory=tmp)

    def test_same_content_reuses_original_receipts_and_changed_content_versions(self):
        fixture = _fixture()
        stable_identity = {"code_commit": "a" * 40, "files": {"x.py": "b" * 64}}

        def parquet(features, _fields, _root):
            return b"candidate:" + hashlib.sha256(_body([f["attributes"] for f in features])).digest()

        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with fixture_pins():
                def run(observed_at, mutate=False):
                    captures = copy.deepcopy(fixture)
                    for _, receipt in captures.values():
                        receipt["observed_at"] = observed_at
                    if mutate:
                        doc = json.loads(captures["page_000"][0])
                        doc["features"][0]["attributes"]["BLK_FED_APRV_DT"] = "02-JUL-2011"
                        body = _body(doc)
                        receipt = captures["page_000"][1]
                        receipt.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest(),
                                       wire_bytes=len(body),
                                       wire_sha256=hashlib.sha256(body).hexdigest())
                        captures["page_000"] = (body, receipt)
                    extra = ([patch.object(blocks, "EXPECTED_RECORD_SHA256", "")] if mutate else [])
                    with patch.object(blocks, "_load_offline", return_value=captures), \
                            patch.object(blocks, "_geoparquet", side_effect=parquet), \
                            patch.object(blocks, "processing_identity",
                                         return_value=stable_identity), \
                            patch.object(blocks, "validate_construction_code_identity",
                                         return_value=None), \
                            patch.object(epa_frs_common, "validate_construction_code_identity",
                                         return_value=None):
                        with ExitStack() as stack:
                            for extra_patch in extra:
                                stack.enter_context(extra_patch)
                            return blocks.capture_boem_ocs_lease_blocks(
                                storage, offline_capture_directory=tmp)

                first = run("2026-01-01T00:00:00Z")
                first_manifest = json.loads(storage.read_key(
                    first["candidate_ready"].replace("metadata/candidate_ready.json",
                                                     "metadata/source_manifest.json")))
                second = run("2026-01-02T00:00:00Z")
                second_manifest = json.loads(storage.read_key(
                    second["candidate_ready"].replace("metadata/candidate_ready.json",
                                                      "metadata/source_manifest.json")))
                self.assertEqual(first["version"], second["version"])
                self.assertTrue(second["reused"])
                self.assertEqual(first_manifest["inputs"], second_manifest["inputs"])
                changed = run("2026-01-03T00:00:00Z", mutate=True)
                self.assertNotEqual(first["version"], changed["version"])

    def test_manifest_records_first_party_host_and_pending_acceptance(self):
        fixture = _fixture()
        stable_identity = {"code_commit": "a" * 40, "files": {"x.py": "b" * 64}}
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with fixture_pins():
                with patch.object(blocks, "_load_offline", return_value=copy.deepcopy(fixture)), \
                        patch.object(blocks, "_geoparquet", return_value=b"candidate"), \
                        patch.object(blocks, "processing_identity", return_value=stable_identity), \
                        patch.object(blocks, "validate_construction_code_identity",
                                     return_value=None), \
                        patch.object(epa_frs_common, "validate_construction_code_identity",
                                     return_value=None):
                    result = blocks.capture_boem_ocs_lease_blocks(
                        storage, offline_capture_directory=tmp)
            base = result["candidate_ready"].replace("metadata/candidate_ready.json", "metadata/")
            manifest = json.loads(storage.read_key(base + "source_manifest.json"))
            self.assertFalse(manifest["source_authority_accepted"])
            self.assertTrue(manifest["source_host_first_party"])
            self.assertTrue(all(host == "gis.boem.gov" for host in
                                [inp["host"] for inp in manifest["inputs"]]))
            self.assertIn("no ArcGIS Online hosting account involved",
                          manifest["source_authority_note"])
            self.assertIn("geoseas.geoplatform.gov", manifest["lineage"])
            self.assertIn("maintained producer REST publication", manifest["lineage"])
            self.assertIsNone(manifest["source_release_date"])
            self.assertIn("no dated source release",
                          manifest["inputs"][0]["release_date_unknown_reason"])
            audit = json.loads(storage.read_key(base + "source_audit.json"))
            self.assertFalse(audit["source_authority_accepted"])
            ready = json.loads(storage.read_key(base + "candidate_ready.json"))
            self.assertFalse(ready["source_authority_accepted"])
            self.assertFalse(result["source_authority_accepted"])

    def test_geoparquet_roundtrip_preserves_native_values_and_geometry(self):
        with fixture_pins():
            fixture = _fixture()
            _, fields, features, _ = blocks._validate(copy.deepcopy(fixture))
            with tempfile.TemporaryDirectory() as tmp:
                body = blocks._geoparquet(features, fields, Path(tmp))
        import io
        import pyarrow.parquet as pq
        from shapely import from_wkb
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.column_names, [*blocks.FIELD_NAMES, "geometry"])
        self.assertEqual(table.num_rows, 4)
        self.assertEqual(table.column("OBJECTID").to_pylist(),
                         [643213655, 643213656, 643213657, 644434609])
        # Space-padded block numbers survive the round trip verbatim.
        self.assertEqual(table.column("BLOCK_NUMBER").to_pylist(),
                         ["   71", "   72", " 7101", " 7102"])
        self.assertEqual(table.column("BLK_FED_APRV_DT").to_pylist(),
                         ["01-JUL-2011", "01-JUL-2011", "03-JUN-2003", None])
        self.assertEqual(table.column("Shape__Length").to_pylist()[2], None)
        geometries = table.column("geometry").to_pylist()
        self.assertIsNone(geometries[3])
        polygon = from_wkb(geometries[0])
        self.assertEqual(polygon.geom_type, "Polygon")
        first_ring = list(polygon.exterior.coords)
        self.assertEqual([round(c, 7) for c in first_ring[0]],
                         [round(c, 7) for c in first_ring[-1]])
        self.assertEqual(round(first_ring[0][0], 9), -93.109375)
        multipart = from_wkb(geometries[1])
        self.assertEqual(multipart.geom_type, "MultiPolygon")
        self.assertEqual(len(multipart.geoms), 2)
        self.assertEqual(round(list(multipart.geoms[1].exterior.coords)[0][0], 9), -93.1)
        geo_metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                         ["Polygon", "MultiPolygon"])
        self.assertEqual(geo_metadata["columns"]["geometry"]["crs"].get("name", ""), "NAD83")


if __name__ == "__main__":
    unittest.main()

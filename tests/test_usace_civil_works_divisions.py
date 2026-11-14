"""Focused contract and repeat-acquisition tests for the USACE Civil Works Divisions."""
from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_civil_works_divisions import (
    usace_civil_works_divisions, usace_civil_works_divisions_complete,
    usace_civil_works_divisions_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_frs_common
from dagster_hifld.sources import usace_civil_works_divisions as ucwd


def _body(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


SHELL_A = [[-84.0, 38.0], [-84.0, 39.0], [-83.0, 39.0], [-83.0, 38.0], [-84.0, 38.0]]
SHELL_B = [[-75.0, 36.0], [-75.0, 40.0], [-70.0, 40.0], [-70.0, 36.0], [-75.0, 36.0]]
HOLE_B = [[-73.5, 37.5], [-72.5, 37.5], [-72.5, 38.5], [-73.5, 38.5], [-73.5, 37.5]]
SHELL_C = [[-123.0, 45.0], [-123.0, 46.0], [-122.0, 46.0], [-122.0, 45.0], [-123.0, 45.0]]
HOLE_C1 = [[-122.9, 45.1], [-122.8, 45.1], [-122.8, 45.2], [-122.9, 45.2], [-122.9, 45.1]]
HOLE_C2 = [[-122.7, 45.5], [-122.6, 45.5], [-122.6, 45.6], [-122.7, 45.6], [-122.7, 45.5]]
SHELL_D = [[-170.0, -14.5], [-170.0, -14.0], [-169.0, -14.0], [-169.0, -14.5],
           [-170.0, -14.5]]


def _attrs(oid, division, sym):
    return {"OBJECTID": oid, "DIVISION": division, "DIV_SYM": sym, "EROC": "H0",
            "HQ_EROC": "S0", "REPORTS_TO": "Headquarters",
            "USACE_DIVISION_CODE": "CE" + sym, "INPOLY_FID": oid,
            "MAXSIMPTOL": 8.223e-05, "MINSIMPTOL": 8.223e-05,
            "SE_ANNO_CAD_DATA": "", "Shape__Area": 100.5 + oid,
            "Shape__Length": 50.5 + oid}


def _fixture():
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID",
         "length": None, "nullable": False, "domain": None, "sqlType": "sqlTypeOther",
         "defaultValue": None},
        {"name": "DIVISION", "type": "esriFieldTypeString", "alias": "DIVISION",
         "length": 40, "nullable": True, "domain": None, "sqlType": "sqlTypeOther",
         "defaultValue": None},
        {"name": "DIV_SYM", "type": "esriFieldTypeString", "alias": "DIV_SYM",
         "length": 3, "nullable": True, "domain": None, "sqlType": "sqlTypeOther",
         "defaultValue": None},
        {"name": "EROC", "type": "esriFieldTypeString", "alias": "EROC", "length": 255,
         "nullable": True, "domain": None, "sqlType": "sqlTypeOther", "defaultValue": None},
        {"name": "HQ_EROC", "type": "esriFieldTypeString", "alias": "HQ_EROC",
         "length": 255, "nullable": True, "domain": None, "sqlType": "sqlTypeOther",
         "defaultValue": None},
        {"name": "REPORTS_TO", "type": "esriFieldTypeString", "alias": "REPORTS_TO",
         "length": 255, "nullable": True, "domain": None, "sqlType": "sqlTypeOther",
         "defaultValue": None},
        {"name": "USACE_DIVISION_CODE", "type": "esriFieldTypeString",
         "alias": "USACE_DIVISION_CODE", "length": 255, "nullable": True, "domain": None,
         "sqlType": "sqlTypeOther", "defaultValue": None},
        {"name": "INPOLY_FID", "type": "esriFieldTypeInteger", "alias": "InPoly_FID",
         "length": None, "nullable": True, "domain": None, "sqlType": "sqlTypeOther",
         "defaultValue": None},
        {"name": "MAXSIMPTOL", "type": "esriFieldTypeDouble", "alias": "MaxSimpTol",
         "length": None, "nullable": True, "domain": None, "sqlType": "sqlTypeOther",
         "defaultValue": None},
        {"name": "MINSIMPTOL", "type": "esriFieldTypeDouble", "alias": "MinSimpTol",
         "length": None, "nullable": True, "domain": None, "sqlType": "sqlTypeOther",
         "defaultValue": None},
        {"name": "SE_ANNO_CAD_DATA", "type": "esriFieldTypeBlob",
         "alias": "SE_ANNO_CAD_DATA", "length": None, "nullable": True, "domain": None,
         "sqlType": "sqlTypeOther", "defaultValue": None},
        {"name": "Shape__Area", "type": "esriFieldTypeDouble", "alias": "Shape__Area",
         "length": None, "nullable": True, "domain": None, "sqlType": "sqlTypeDouble",
         "defaultValue": None},
        {"name": "Shape__Length", "type": "esriFieldTypeDouble", "alias": "Shape__Length",
         "length": None, "nullable": True, "domain": None, "sqlType": "sqlTypeDouble",
         "defaultValue": None},
    ]
    ids = [1, 2, 3, 4, 5, 6, 7, 8]
    names = [("Great Lakes and Ohio River Division", "LRD"),
             ("Mississippi Valley Division", "MVD"),
             ("North Atlantic Division", "NAD"),
             ("Northwestern Division", "NWD"),
             ("Pacific Ocean Division", "POD"),
             ("South Atlantic Division", "SAD"),
             ("South Pacific Division", "SPD"),
             ("Southwestern Division", "SWD")]
    geometries = [
        {"rings": [SHELL_A]},
        {"rings": [SHELL_B, HOLE_B]},
        {"rings": [SHELL_C, HOLE_C1, HOLE_C2]},
        {"rings": [SHELL_A, SHELL_B, HOLE_B]},
        {"rings": [SHELL_D, SHELL_A, SHELL_B, SHELL_C]},
        {"rings": [SHELL_A, SHELL_B, SHELL_C, HOLE_B, HOLE_C1, HOLE_C2]},
        {"rings": [SHELL_A]},
        {"rings": [SHELL_A]},
    ]
    features = [{"attributes": _attrs(oid, *names[index]), "geometry": geom}
                for index, (oid, geom) in enumerate(zip(ids, geometries))]
    layer = {"id": ucwd.LAYER_ID, "name": ucwd.LAYER_NAME,
             "geometryType": "esriGeometryPolygon", "objectIdField": "OBJECTID",
             "hasZ": False, "hasM": False, "maxRecordCount": 1000,
             "advancedQueryCapabilities": {"supportsPagination": True},
             "spatialReference": {"wkid": 4140, "latestWkid": 4140},
             "extent": {"spatialReference": {"wkid": 4140, "latestWkid": 4140}},
             "fields": fields}
    service = {"serviceDescription": "Polygons showing USACE Civil Works Division "
                                     "boundaries.",
               "spatialReference": {"wkid": 4140, "latestWkid": 4140},
               "maxRecordCount": 1000, "currentVersion": "12.0",
               "layers": [{"id": ucwd.LAYER_ID, "name": ucwd.LAYER_NAME}]}
    item = {"id": ucwd.ITEM_ID, "owner": ucwd.ITEM_OWNER, "type": "Feature Service",
            "url": ucwd.SERVICE_URL, "access": "public",
            "licenseInfo": "This data set is publicly available without any use "
                           "restrictions.",
            "modified": 1769611189000}
    id_doc = {"objectIds": ids}
    count_doc = {"count": len(ids)}
    pages = []
    for page in range(0, (len(ids) + ucwd.PAGE_SIZE - 1) // ucwd.PAGE_SIZE):
        pages.append({"spatialReference": {"wkid": 4140, "latestWkid": 4140},
                      "features": features[page * ucwd.PAGE_SIZE:(page + 1) * ucwd.PAGE_SIZE]})
    docs = {
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "item_before": item, "item_after": item,
        "ids_before": id_doc, "ids_after": id_doc,
        "count_before": count_doc, "count_after": count_doc,
    }
    for index, page in enumerate(pages):
        docs[f"page_{index:03d}"] = page
    receipt = {"status": 200, "complete_body": True, "observed_at": "2026-01-01T00:00:00Z"}
    captures = {}
    page_urls = [ucwd._query(where="1=1", outFields="*", returnGeometry="true",
                             orderByFields="OBJECTID ASC", resultOffset=page * ucwd.PAGE_SIZE,
                             resultRecordCount=ucwd.PAGE_SIZE)
                 for page in range(0, (len(ids) + ucwd.PAGE_SIZE - 1) // ucwd.PAGE_SIZE)]
    for key, value in docs.items():
        body = _body(value)
        url = (ucwd.SERVICE_URL + "?f=pjson" if key.startswith("service_") else
               ucwd.LAYER_URL + "?f=pjson" if key.startswith("layer_") else
               ucwd.ITEM_URL if key.startswith("item_") else
               ucwd.LAYER_URL + "/query?where=1%3D1&returnCountOnly=true&f=json"
               if key.startswith("count_") else
               ucwd.LAYER_URL + "/query?where=1%3D1&returnIdsOnly=true&f=json"
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
    "EXPECTED_SHELL_COUNT_DISTRIBUTION": None,
    "EXPECTED_HOLE_COUNT": None,
    "EXPECTED_VERTEX_COUNT": None,
    "EXPECTED_OBSERVED_BOUNDS": None,
}


@contextmanager
def fixture_pins():
    """Pin every module baseline to the fixture capture's own audit values."""
    with ExitStack() as stack:
        for name, value in _UNPINNED.items():
            stack.enter_context(patch.object(ucwd, name, value))
        audit = ucwd._validate(_fixture())[3]
        stack.close()
        pins = {
            "EXPECTED_COUNT": audit["feature_count"],
            "EXPECTED_IDS_SHA256": audit["source_id_sha256"],
            "EXPECTED_SCHEMA_SHA256": audit["source_schema_sha256"],
            "EXPECTED_RECORD_SHA256": audit["source_record_sha256"],
            "EXPECTED_GEOMETRY_SHA256": audit["source_geometry_sha256"],
            "EXPECTED_NULL_GEOMETRY_COUNT": audit["null_geometry_count"],
            "EXPECTED_SHELL_COUNT_DISTRIBUTION": audit["shell_count_distribution"],
            "EXPECTED_HOLE_COUNT": audit["hole_count"],
            "EXPECTED_VERTEX_COUNT": audit["vertex_count"],
            "EXPECTED_OBSERVED_BOUNDS": audit["geometry_bounds"],
        }
        for name, value in pins.items():
            stack.enter_context(patch.object(ucwd, name, value))
        yield audit


class UsaceCivilWorksDivisionsTests(unittest.TestCase):
    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(usace_civil_works_divisions, hifld_ingest_assets)
        self.assertEqual(usace_civil_works_divisions_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = usace_civil_works_divisions_complete({"feature_count": 8,
                                                      "candidate_ready": "ready"})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["source_authority_accepted"].value)
        failed = usace_civil_works_divisions_complete({"feature_count": 7,
                                                       "candidate_ready": "ready"})
        self.assertFalse(failed.passed)

    def test_native_fields_rings_holes_and_audit_counts(self):
        with fixture_pins():
            _, fields, features, audit = ucwd._validate(_fixture())
        self.assertEqual([field["name"] for field in fields], ucwd.FIELD_NAMES)
        self.assertEqual(len(features), 8)
        self.assertEqual(audit["unique_objectid_count"], 8)
        self.assertEqual(audit["null_counts_by_field"]["SE_ANNO_CAD_DATA"], 0)
        self.assertEqual(audit["null_geometry_count"], 0)
        self.assertEqual(audit["shell_count_distribution"], {1: 5, 2: 1, 3: 1, 4: 1})
        self.assertEqual(audit["hole_count"], 7)
        self.assertEqual(audit["vertex_count"], 105)
        self.assertEqual(audit["invalid_geometry_ids"], [])
        self.assertEqual(audit["source_crs_wkid"], 4140)
        self.assertEqual(audit["geometry_bounds"], [-170.0, -14.5, -70.0, 46.0])

    def test_page_roster_walks_sorted_ids_without_overlap(self):
        ids = list(range(1, 451))
        urls = dict(ucwd._capture_names(sorted(ids)))
        pages = [name for name in sorted(urls) if name.startswith("page_")]
        self.assertEqual(pages, [f"page_{index:03d}" for index in range(3)])
        self.assertIn(f"resultOffset=0&resultRecordCount={ucwd.PAGE_SIZE}", urls["page_000"])
        self.assertIn(f"resultOffset={ucwd.PAGE_SIZE}", urls["page_001"])
        self.assertIn("orderByFields=OBJECTID+ASC", urls["page_000"])
        self.assertNotIn("outSR=", urls["page_000"])
        self.assertEqual(len(urls), 13)

    def test_spatial_reference_deviations_are_rejected(self):
        with fixture_pins():
            captures = _fixture()
            _, _, _, audit = ucwd._validate(captures)
            self.assertEqual(audit["source_crs_wkid"], 4140)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["layer_before"][0])
            doc["spatialReference"] = {"wkid": 4326, "latestWkid": 4140}
            candidate["layer_before"] = (_body(doc), candidate["layer_before"][1])
            with self.assertRaisesRegex(ValueError, "source_layer_spatial_reference_drift"):
                ucwd._validate(candidate)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["layer_before"][0])
            doc["extent"]["spatialReference"] = {"wkid": 4269, "latestWkid": 4269}
            candidate["layer_before"] = (_body(doc), candidate["layer_before"][1])
            with self.assertRaisesRegex(ValueError,
                                        "source_layer_extent_spatial_reference_drift"):
                ucwd._validate(candidate)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["page_000"][0])
            doc["spatialReference"] = {"wkid": 4326, "latestWkid": 4269}
            candidate["page_000"] = (_body(doc), candidate["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_spatial_reference_drift"):
                ucwd._validate(candidate)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["page_000"][0])
            doc["features"][0]["geometry"]["spatialReference"] = {"wkid": 3857}
            candidate["page_000"] = (_body(doc), candidate["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_geometry_spatial_reference_drift"):
                ucwd._validate(candidate)

    def test_missing_id_truncation_schema_and_geometry_drift_are_rejected(self):
        with fixture_pins():
            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"].pop()
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_drift"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"].pop("OBJECTID")
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_drift"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["OBJECTID"] = 999
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_drift"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["UNEXPECTED_COLUMN"] = "x"
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_feature_field_roster_drift"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["DIV_SYM"] = 16
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_string_field_type_or_width_drift"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["DIVISION"] = "D" * 41
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_string_field_type_or_width_drift"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["INPOLY_FID"] = "1"
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_integer_field_type_drift"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["Shape__Area"] = "big"
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_double_field_type_drift"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["SE_ANNO_CAD_DATA"] = "binary?"
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_blob_field_json_value_drift"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"] = {"paths": [SHELL_A]}
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_geometry_invalid"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"]["rings"][0][1][0] = -200.0
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_coordinates_invalid"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"]["rings"] = [[[-84.0, 38.0], [-83.0, 38.0]]]
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_ring_invalid"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"]["rings"] = [HOLE_B]
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_hole_without_containing_shell"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][1]["geometry"]["rings"].append(HOLE_C1)
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polygon_hole_without_containing_shell"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["OBJECTID"], doc["features"][1]["attributes"]["OBJECTID"] = (
                doc["features"][1]["attributes"]["OBJECTID"], doc["features"][0]["attributes"]["OBJECTID"])
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_drift"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["exceededTransferLimit"] = True
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_error_or_truncation"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["layer_before"][0])
            doc["fields"][1]["type"] = "esriFieldTypeInteger"
            captures["layer_before"] = (_body(doc), captures["layer_before"][1])
            with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
                ucwd._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["count_after"][0])
            doc["count"] = 7
            captures["count_after"] = (_body(doc), captures["count_after"][1])
            with self.assertRaisesRegex(ValueError, "source_count_bracket_mismatch"):
                ucwd._validate(captures)

    def test_empty_rings_and_missing_geometry_are_gated_to_null(self):
        self.assertIsNone(ucwd._geometry_rings({"rings": []}))
        self.assertIsNone(ucwd._geometry_rings(None))
        self.assertIsNone(ucwd._geometry_rings({}))
        self.assertEqual(ucwd._geometry_rings({"rings": [SHELL_A]}), [SHELL_A])

    def test_pinned_baselines_are_enforced_once_set(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with fixture_pins() as audit:
                fixture = _fixture()
                with patch.object(ucwd, "_load_offline", return_value=copy.deepcopy(fixture)), \
                        patch.object(ucwd, "_geoparquet", return_value=b"candidate"), \
                        patch.object(ucwd, "processing_identity",
                                     return_value={"code_commit": "a" * 40,
                                                   "files": {"x.py": "b" * 64}}), \
                        patch.object(ucwd, "validate_construction_code_identity",
                                     return_value=None), \
                        patch.object(epa_frs_common, "validate_construction_code_identity",
                                     return_value=None):
                    result = ucwd.capture_usace_civil_works_divisions(
                        storage, offline_capture_directory=tmp)
                    self.assertFalse(result["reused"])
                    pins = [
                        ("EXPECTED_IDS_SHA256", audit["source_id_sha256"]),
                        ("EXPECTED_RECORD_SHA256", audit["source_record_sha256"]),
                        ("EXPECTED_GEOMETRY_SHA256", audit["source_geometry_sha256"]),
                        ("EXPECTED_NULL_GEOMETRY_COUNT", audit["null_geometry_count"]),
                        ("EXPECTED_SHELL_COUNT_DISTRIBUTION", audit["shell_count_distribution"]),
                        ("EXPECTED_HOLE_COUNT", audit["hole_count"]),
                        ("EXPECTED_VERTEX_COUNT", audit["vertex_count"]),
                        ("EXPECTED_OBSERVED_BOUNDS", audit["geometry_bounds"]),
                    ]
                    for name, value in pins:
                        with self.subTest(pin=name):
                            with patch.object(ucwd, name, value):
                                again = ucwd.capture_usace_civil_works_divisions(
                                    storage, offline_capture_directory=tmp)
                                self.assertTrue(again["reused"])
                    for name, value in (("EXPECTED_IDS_SHA256", "0" * 64),
                                        ("EXPECTED_COUNT", audit["feature_count"] + 1)):
                        with self.subTest(refusal=name):
                            with patch.object(ucwd, name, value):
                                with self.assertRaisesRegex(ValueError, "source_"):
                                    ucwd.capture_usace_civil_works_divisions(
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
                        doc["features"][0]["attributes"]["DIVISION"] = "Renamed Division"
                        body = _body(doc)
                        receipt = captures["page_000"][1]
                        receipt.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest(),
                                       wire_bytes=len(body),
                                       wire_sha256=hashlib.sha256(body).hexdigest())
                        captures["page_000"] = (body, receipt)
                    extra = ([patch.object(ucwd, "EXPECTED_RECORD_SHA256", "")] if mutate else [])
                    with patch.object(ucwd, "_load_offline", return_value=captures), \
                            patch.object(ucwd, "_geoparquet", side_effect=parquet), \
                            patch.object(ucwd, "processing_identity",
                                         return_value=stable_identity), \
                            patch.object(ucwd, "validate_construction_code_identity",
                                         return_value=None), \
                            patch.object(epa_frs_common, "validate_construction_code_identity",
                                         return_value=None):
                        with ExitStack() as stack:
                            for extra_patch in extra:
                                stack.enter_context(extra_patch)
                            return ucwd.capture_usace_civil_works_divisions(
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

    def test_manifest_records_agol_host_authority_chain_and_pending_acceptance(self):
        fixture = _fixture()
        stable_identity = {"code_commit": "a" * 40, "files": {"x.py": "b" * 64}}
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with fixture_pins():
                with patch.object(ucwd, "_load_offline", return_value=copy.deepcopy(fixture)), \
                        patch.object(ucwd, "_geoparquet", return_value=b"candidate"), \
                        patch.object(ucwd, "processing_identity", return_value=stable_identity), \
                        patch.object(ucwd, "validate_construction_code_identity",
                                     return_value=None), \
                        patch.object(epa_frs_common, "validate_construction_code_identity",
                                     return_value=None):
                    result = ucwd.capture_usace_civil_works_divisions(
                        storage, offline_capture_directory=tmp)
            base = result["candidate_ready"].replace("metadata/candidate_ready.json", "metadata/")
            manifest = json.loads(storage.read_key(base + "source_manifest.json"))
            self.assertFalse(manifest["source_authority_accepted"])
            self.assertFalse(manifest["source_host_first_party"])
            for inp in manifest["inputs"]:
                expected_host = "www.arcgis.com" if inp["input_id"].startswith("item_") \
                    else "services7.arcgis.com"
                self.assertEqual(inp["host"], expected_host)
            self.assertIn("usace_crrel_als", manifest["source_authority_note"])
            self.assertIn("n1YM8pTrFmm7L4hs", manifest["source_authority_note"])
            self.assertIn("DISDI MIRTA", manifest["source_authority_note"])
            self.assertIn(ucwd.HIFLD_PORTAL_ITEM_ID, manifest["lineage"])
            self.assertIn("10.3886/E241367V1", manifest["lineage"])
            self.assertIn("HTTP 403", manifest["lineage"])
            self.assertIn("resequence OBJECTIDs", manifest["lineage"])
            self.assertIn("portal registration", manifest["lineage"])
            self.assertIsNone(manifest["source_release_date"])
            self.assertIn("editing timestamp", manifest["inputs"][0]["release_date_unknown_reason"])
            self.assertIn("04/16/20", manifest["inputs"][0]["release_date_unknown_reason"])
            audit = json.loads(storage.read_key(base + "source_audit.json"))
            self.assertFalse(audit["source_authority_accepted"])
            self.assertEqual(audit["crosswalk_doi"], ucwd.CROSSWALK_DOI)
            self.assertEqual(audit["hifld_portal_item_id"], ucwd.HIFLD_PORTAL_ITEM_ID)
            self.assertIn("wkid", audit["source_crs_note"])
            ready = json.loads(storage.read_key(base + "candidate_ready.json"))
            self.assertFalse(ready["source_authority_accepted"])
            self.assertFalse(result["source_authority_accepted"])

    def test_geoparquet_roundtrip_preserves_native_values_and_geometry(self):
        with fixture_pins():
            fixture = _fixture()
            _, fields, features, _ = ucwd._validate(copy.deepcopy(fixture))
            with tempfile.TemporaryDirectory() as tmp:
                body = ucwd._geoparquet(features, fields, Path(tmp))
        import io
        import pyarrow.parquet as pq
        from shapely import from_wkb
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.column_names, [*ucwd.FIELD_NAMES, "geometry"])
        self.assertEqual(table.num_rows, 8)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [1, 2, 3, 4, 5, 6, 7, 8])
        self.assertEqual(table.column("DIV_SYM").to_pylist(),
                         ["LRD", "MVD", "NAD", "NWD", "POD", "SAD", "SPD", "SWD"])
        self.assertEqual(table.column("USACE_DIVISION_CODE").to_pylist()[:2],
                         ["CELRD", "CEMVD"])
        self.assertEqual(table.column("INPOLY_FID").to_pylist()[:2], [1, 2])
        self.assertEqual(set(table.column("SE_ANNO_CAD_DATA").to_pylist()), {""})
        geometries = table.column("geometry").to_pylist()
        single = from_wkb(geometries[0])
        self.assertEqual(single.geom_type, "Polygon")
        self.assertEqual(single.bounds, (-84.0, 38.0, -83.0, 39.0))
        with_hole = from_wkb(geometries[1])
        self.assertEqual(with_hole.geom_type, "Polygon")
        self.assertEqual(len(with_hole.interiors), 1)
        self.assertEqual(with_hole.interiors[0].bounds, (-73.5, 37.5, -72.5, 38.5))
        multipart = from_wkb(geometries[3])
        self.assertEqual(multipart.geom_type, "MultiPolygon")
        self.assertEqual(len(multipart.geoms), 2)
        islands = from_wkb(geometries[4])
        self.assertEqual(islands.geom_type, "MultiPolygon")
        self.assertEqual(len(islands.geoms), 4)
        geo_metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(sorted(geo_metadata["columns"]["geometry"]["geometry_types"]),
                         ["MultiPolygon", "Polygon"])
        self.assertIn("NAD83", geo_metadata["columns"]["geometry"]["crs"].get("name", ""))
        self.assertEqual(geo_metadata["columns"]["geometry"]["bbox"],
                         [-170.0, -14.5, -70.0, 46.0])

    def test_transient_null_roster_answers_are_refetched_with_witnesses(self):
        import httpx

        class _Stream(httpx.SyncByteStream):
            def __init__(self, payload):
                self.body = payload

            def __iter__(self):
                yield self.body

            def close(self):
                return None

        null_answer = _body({"objectIdFieldName": "OBJECTID", "objectIds": None})
        real_answer = _body({"objectIdFieldName": "OBJECTID", "objectIds": [1, 2]})
        url = ucwd._query(where="1=1", returnIdsOnly="true")
        responses = iter([null_answer, real_answer])

        def respond(request):
            return httpx.Response(200, stream=_Stream(next(responses)),
                                  headers={"content-type": "application/json"})

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            client = httpx.Client(transport=httpx.MockTransport(respond))
            with patch.object(ucwd, "_BRACKET_RETRY_BASE_DELAY_SECONDS", 0.0):
                body, receipt = ucwd._acquire_document(client, root, "ids_after", url)
            self.assertEqual(json.loads(body)["objectIds"], [1, 2])
            self.assertTrue(receipt["complete_body"])
            witnesses = list(root.glob("ids_after.error-*.body"))
            self.assertEqual(len(witnesses), 1)
            self.assertEqual(witnesses[0].read_bytes(), null_answer)

            responses = iter([null_answer] * 4)
            client = httpx.Client(transport=httpx.MockTransport(respond))
            with patch.object(ucwd, "_BRACKET_RETRY_BASE_DELAY_SECONDS", 0.0):
                body, _receipt = ucwd._acquire_document(client, root, "ids_after", url)
            self.assertEqual(json.loads(body)["objectIds"], None)
            self.assertEqual(len(list(root.glob("ids_after.error-*.body"))), 5)
            with self.assertRaisesRegex(ValueError,
                                        "source_id_population_changed_during_capture"):
                ucwd._validate({"ids_before": (_body({"objectIds": [1]}), {}),
                                "ids_after": (body, {}),
                                "count_before": (_body({"count": 1}), {}),
                                "count_after": (_body({"count": 1}), {}),
                                "service_before": (_body({"layers": []}), {}),
                                "service_after": (_body({"layers": []}), {}),
                                "layer_before": (_body({"layers": []}), {}),
                                "layer_after": (_body({"layers": []}), {}),
                                "item_before": (_body({"id": ucwd.ITEM_ID}), {}),
                                "item_after": (_body({"id": ucwd.ITEM_ID}), {})})

    def test_entrypoint_quarantines_acquisition_validation_failures(self):
        fixture = _fixture()
        doc = json.loads(fixture["page_000"][0])
        doc["features"].pop()
        fixture["page_000"] = (_body(doc), fixture["page_000"][1])
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with fixture_pins(), patch.object(ucwd, "_load_offline", return_value=fixture):
                with self.assertRaisesRegex(ValueError, "source_page_id_slice_drift"):
                    ucwd.capture_usace_civil_works_divisions(storage, offline_capture_directory=tmp)
            quarantined = sorted(str(path.relative_to(tmp)) for path in Path(tmp).rglob("*")
                                 if path.is_file())
            self.assertTrue(quarantined)
            self.assertTrue(all(name.startswith(f"_quarantine/{ucwd.DATASET_SLUG}/")
                                for name in quarantined), quarantined)
            token_root = Path(tmp) / "_quarantine" / ucwd.DATASET_SLUG
            tokens = [path for path in token_root.iterdir() if path.is_dir()]
            self.assertEqual(len(tokens), 1)
            failure = json.loads((tokens[0] / "metadata" / "failure.json").read_bytes())
            self.assertEqual(failure["reason_code"], "source_page_id_slice_drift:page_000")
            self.assertEqual(sorted(failure["captured_names"]), sorted(fixture))
            for name in fixture:
                self.assertTrue((tokens[0] / "raw" / f"{name}.body").is_file(), name)
                self.assertTrue((tokens[0] / "raw" / f"{name}.wire").is_file(), name)
                self.assertTrue((tokens[0] / "raw" / f"{name}.receipt.json").is_file(), name)


if __name__ == "__main__":
    unittest.main()

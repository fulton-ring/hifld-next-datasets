"""Focused contract and repeat-acquisition tests for the SLA boundary adapter."""
from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.submerged_land_act_boundary import (
    submerged_land_act_boundary, submerged_land_act_boundary_complete,
    submerged_land_act_boundary_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import capture_common
from dagster_hifld.sources import submerged_land_act_boundary as sla


def _body(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


LINE_A = [[-92.83038330078125, 29.59698486328125], [-92.8302001953125, 29.59698486328125],
          [-92.8301001953125, 29.59688486328125]]
LINE_B = [[-88.0, 30.2], [-88.01, 30.21], [-88.02, 30.22]]


def _fixture():
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False, "length": None},
        {"name": "BDRY_APRV_DATE", "type": "esriFieldTypeString", "nullable": True, "length": 11},
        {"name": "BLOCK_NUMBER", "type": "esriFieldTypeString", "nullable": True, "length": 6},
        {"name": "BDRY_NAME_TEXT", "type": "esriFieldTypeString", "nullable": True, "length": 80},
        {"name": "Shape__Length", "type": "esriFieldTypeDouble", "nullable": True, "length": None},
    ]
    ids = [14728977, 14728978, 14728979, 14733728]
    attrs_rows = [
        {"BDRY_APRV_DATE": "01-NOV-2000", "BLOCK_NUMBER": "   16",
         "BDRY_NAME_TEXT": "Submerged Lands Act Boundary",
         "Shape__Length": 0.054132906077027795},
        {"BDRY_APRV_DATE": "01-NOV-2000", "BLOCK_NUMBER": "   17",
         "BDRY_NAME_TEXT": "Submerged Lands Act Boundary", "Shape__Length": 0.06},
        {"BDRY_APRV_DATE": None, "BLOCK_NUMBER": " 7101",
         "BDRY_NAME_TEXT": "Submerged Lands Act Boundary", "Shape__Length": None},
        {"BDRY_APRV_DATE": "09-SEP-1998", "BLOCK_NUMBER": " 7102",
         "BDRY_NAME_TEXT": "Submerged Lands Act Boundary", "Shape__Length": 0.5},
    ]
    geometries = [
        {"paths": [LINE_A]},
        {"paths": [LINE_A, LINE_B]},
        {"paths": []},
        {"paths": [[[ -160.5, 61.0], [ -160.4, 61.0], [ -160.3, 61.0]]]},
    ]
    features = [{"attributes": {"OBJECTID": oid, **attrs}, "geometry": geom}
                for oid, attrs, geom in zip(ids, attrs_rows, geometries)]
    layer = {"id": sla.LAYER_ID, "name": sla.LAYER_NAME,
             "geometryType": "esriGeometryPolyline", "objectIdField": "OBJECTID",
             "hasZ": False, "hasM": False, "maxRecordCount": 10000,
             "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": {"wkid": 4269, "latestWkid": 4269},
             "extent": {"spatialReference": {"wkid": 4269, "latestWkid": 4269}},
             "fields": fields}
    service = {"serviceDescription": "Map Service contains primary data types created by "
                                     "both the Bureau of Ocean Energy Management (BOEM) and "
                                     "the Bureau of Safety and Environmental Enforcement "
                                     "(BSEE). Contact the Geospatial Services Division at "
                                     "mappingboundarybranch@boem.gov.",
               "spatialReference": {"wkid": 4269, "latestWkid": 4269},
               "layers": [{"id": sla.LAYER_ID, "name": sla.LAYER_NAME}]}
    id_doc = {"objectIds": ids}
    count_doc = {"count": len(ids)}
    pages = []
    for page in range(0, (len(ids) + sla.PAGE_SIZE - 1) // sla.PAGE_SIZE):
        pages.append({"spatialReference": {"wkid": 4269, "latestWkid": 4269},
                      "features": features[page * sla.PAGE_SIZE:(page + 1) * sla.PAGE_SIZE]})
    docs = {
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "ids_before": id_doc, "ids_after": id_doc,
        "count_before": count_doc, "count_after": count_doc,
    }
    for index, page in enumerate(pages):
        docs[f"page_{index:03d}"] = page
    receipt = {"status": 200, "complete_body": True, "observed_at": "2026-01-01T00:00:00Z"}
    captures = {}
    page_urls = [sla._query(where="1=1", outFields="*", returnGeometry="true",
                            returnZ="true", returnM="true", outSR="4269",
                            orderByFields="OBJECTID ASC", resultOffset=page * sla.PAGE_SIZE,
                            resultRecordCount=sla.PAGE_SIZE)
                 for page in range(0, (len(ids) + sla.PAGE_SIZE - 1) // sla.PAGE_SIZE)]
    for key, value in docs.items():
        body = _body(value)
        url = (sla.SERVICE_URL + "?f=pjson" if key.startswith("service_") else
               sla.LAYER_URL + "?f=pjson" if key.startswith("layer_") else
               sla.LAYER_URL + "/query?where=1%3D1&returnCountOnly=true&f=json"
               if key.startswith("count_") else
               sla.LAYER_URL + "/query?where=1%3D1&returnIdsOnly=true&f=json"
               if key.startswith("ids_") else
               page_urls[int(key.split("_")[1])])
        captures[key] = (body, {**receipt, "url": url, "final_url": url, "bytes": len(body),
                                "sha256": hashlib.sha256(body).hexdigest(),
                                "wire_bytes": len(body),
                                "wire_sha256": hashlib.sha256(body).hexdigest()})
    return captures


@contextmanager
def fixture_pins():
    """Use the fixture's field aliases and lengths for the schema contract."""
    captures = _fixture()
    fields = json.loads(captures["layer_before"][0])["fields"]
    with patch.object(sla, "EXPECTED_SCHEMA_SHA256", hashlib.sha256(_body(fields)).hexdigest()):
        yield sla._validate(captures)[3]


class SubmergedLandActBoundaryTests(unittest.TestCase):
    def test_capture_witness_drift_and_missing_asset_evidence_are_refused(self):
        for name, doc in (
            ("ids_after", {"objectIds": [14728977, 14728978, 14728979, 14733729]}),
            ("ids_before", {"objectIds": [14728977, 14728977, 14728979, 14733728]}),
            ("count_before", {"count": 5}),
            ("count_after", {"count": 5}),
            ("service_after", {"changed": True}),
            ("layer_after", {"changed": True}),
        ):
            with self.subTest(document=name), fixture_pins():
                captures = _fixture()
                captures[name] = (_body(doc), captures[name][1])
                with self.assertRaises(ValueError):
                    sla._validate(captures)
        candidate = {"feature_count": 4, "source_reported_count": 4,
                     "source_population_complete": True, "candidate_ready": "ready"}
        for updates in ({"source_reported_count": None}, {"source_reported_count": True},
                        {"source_reported_count": 0}, {"source_population_complete": False},
                        {"source_reported_count": 5}, {"release_accepted": True}):
            with self.subTest(candidate=updates):
                self.assertFalse(submerged_land_act_boundary_complete({**candidate, **updates}).passed)

    def test_upstream_row_added_passes_capture_and_warns_against_approved_release(self):
        from dagster import AssetCheckSeverity
        from dagster_hifld.capture_completeness import approved_baseline_drift

        captures = _fixture()
        page = json.loads(captures["page_000"][0])
        added = copy.deepcopy(page["features"][0])
        added["attributes"]["OBJECTID"] = 14733729
        page["features"].append(added)
        updates = {"page_000": page}
        for name in ("ids_before", "ids_after"):
            updates[name] = {"objectIds": [14728977, 14728978, 14728979, 14733728, 14733729]}
        for name in ("count_before", "count_after"):
            updates[name] = {"count": 5}
        for name, doc in updates.items():
            body = _body(doc)
            receipt = dict(captures[name][1])
            receipt.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest(),
                           wire_bytes=len(body), wire_sha256=hashlib.sha256(body).hexdigest())
            captures[name] = (body, receipt)
        schema_sha = hashlib.sha256(_body(json.loads(captures["layer_before"][0])["fields"])).hexdigest()
        with patch.object(sla, "EXPECTED_SCHEMA_SHA256", schema_sha):
            original = sla._validate(_fixture())[3]
            audit = sla._validate(captures)[3]
        candidate = {"dataset_slug": sla.DATASET_SLUG, "file_slug": sla.FILE_SLUG,
                     "feature_count": audit["feature_count"],
                     "source_reported_count": audit["source_reported_count"],
                     "source_population_complete": audit["source_population_complete"],
                     "source_id_sha256": audit["source_id_sha256"], "candidate_ready": "ready"}
        self.assertTrue(submerged_land_act_boundary_complete(candidate).passed)
        baseline = {**candidate, "feature_count": original["feature_count"],
                    "source_id_sha256": original["source_id_sha256"],
                    "version": "approved-fixture", "release_accepted": True}
        drift = approved_baseline_drift(candidate, baseline)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertEqual(drift.metadata["feature_count_delta"].value, 1)

        import httpx
        from dagster import DagsterInstance, materialize
        from dagster_hifld.assets.submerged_land_act_boundary import (
            submerged_land_act_boundary_approved_baseline_drift,
        )
        from urllib.parse import parse_qsl, urlsplit

        def request_key(url):
            parsed = urlsplit(str(url))
            return parsed.path, tuple(sorted(parse_qsl(parsed.query)))

        responses = {request_key(url): captures[name][0]
                     for name, url in sla._capture_names(updates["ids_before"]["objectIds"])}

        def respond(request):
            return httpx.Response(200, stream=httpx.ByteStream(responses[request_key(request.url)]))

        with tempfile.TemporaryDirectory() as tmp, DagsterInstance.ephemeral() as instance:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            client = httpx.Client(transport=httpx.MockTransport(respond))
            with patch.object(sla, "EXPECTED_SCHEMA_SHA256", schema_sha), \
                    patch.object(sla.httpx, "Client", return_value=client), \
                    patch.object(sla, "processing_identity", return_value={
                        "code_commit": "a" * 40, "files": {"x.py": "b" * 64}}), \
                    patch.object(sla, "validate_construction_code_identity"), \
                    patch.object(capture_common, "validate_construction_code_identity"):
                run = materialize(
                    [submerged_land_act_boundary, submerged_land_act_boundary_complete,
                     submerged_land_act_boundary_approved_baseline_drift], instance=instance,
                    resources={"staging_storage": storage},
                    run_config={"ops": {submerged_land_act_boundary.op.name: {
                        "config": {"capture_directory": str(Path(tmp) / "capture")}}}})
            self.assertTrue(run.success)
            result = run.output_for_node(submerged_land_act_boundary.op.name)
            self.assertEqual(result["feature_count"], 5)
            self.assertTrue(result["source_population_complete"])
            self.assertEqual(result["source_reported_count"], 5)
            self.assertFalse(result["release_accepted"])
            checks = {event.check_name: event for event in run.get_asset_check_evaluations()}
            self.assertTrue(checks["submerged_land_act_boundary_complete"].passed)
            skipped = checks["submerged_land_act_boundary_approved_baseline_drift"]
            self.assertFalse(skipped.passed)
            self.assertEqual(skipped.severity, AssetCheckSeverity.WARN)
            self.assertEqual(skipped.metadata["comparison_status"].value,
                             "skipped_no_approved_baseline")
            parquet_key = result["candidate_ready"].removesuffix("metadata/candidate_ready.json") \
                + f"geoparquet/{sla.FILE_SLUG}.parquet"
            import io
            import pyarrow.parquet as pq
            table = pq.read_table(io.BytesIO(storage.read_key(parquet_key)))
            self.assertEqual(table.num_rows, 5)
            self.assertEqual(table.column("OBJECTID").to_pylist()[-1], 14733729)

    def test_default_capture_root_keeps_the_historical_layout(self):
        self.assertEqual(str(sla.DEFAULT_CAPTURE_ROOT),
                         str(Path(tempfile.gettempdir()) / "sla-boundary-captures"))

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(submerged_land_act_boundary, hifld_ingest_assets)
        self.assertEqual(submerged_land_act_boundary_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = submerged_land_act_boundary_complete({"feature_count": 5_746,
                                                      "source_reported_count": 5_746,
                                                      "source_population_complete": True,
                                                      "candidate_ready": "ready"})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["source_authority_accepted"].value)
        failed = submerged_land_act_boundary_complete({"feature_count": 5_745,
                                                       "source_reported_count": 5_746,
                                                       "source_population_complete": True,
                                                       "candidate_ready": "ready"})
        self.assertFalse(failed.passed)

    def test_native_fields_paths_nulls_and_audit_counts(self):
        with fixture_pins():
            _, fields, features, audit = sla._validate(_fixture())
        self.assertEqual([field["name"] for field in fields], sla.FIELD_NAMES)
        self.assertEqual(len(features), 4)
        self.assertEqual(audit["unique_objectid_count"], 4)
        self.assertEqual(audit["null_counts_by_field"]["BDRY_APRV_DATE"], 1)
        self.assertEqual(audit["null_counts_by_field"]["Shape__Length"], 1)
        self.assertEqual(audit["null_geometry_count"], 1)
        self.assertEqual(audit["path_count_distribution"], {1: 2, 2: 1})
        self.assertEqual(audit["source_crs_epsg"], 4269)
        self.assertEqual(audit["geometry_bounds"][0], -160.5)
        self.assertEqual(audit["geometry_bounds"][3], 61.0)

    def test_page_roster_walks_sorted_ids_without_overlap(self):
        ids = list(range(14728977, 14728977 + 4_500))
        urls = dict(sla._capture_names(sorted(ids)))
        pages = [name for name in sorted(urls) if name.startswith("page_")]
        self.assertEqual(pages, [f"page_{index:03d}" for index in range(3)])
        self.assertIn(f"resultOffset=0&resultRecordCount={sla.PAGE_SIZE}", urls["page_000"])
        self.assertIn(f"resultOffset={sla.PAGE_SIZE}", urls["page_001"])
        self.assertIn("orderByFields=OBJECTID+ASC", urls["page_000"])
        self.assertIn("outSR=4269", urls["page_000"])
        self.assertEqual(len(urls["page_002"].split("resultOffset=")[1].split("&")[0]),
                         len(str(2 * sla.PAGE_SIZE)))

    def test_spatial_reference_alias_is_valid_but_contradictions_are_rejected(self):
        with fixture_pins():
            captures = _fixture()
            _, _, _, audit = sla._validate(captures)
            self.assertEqual(audit["source_crs_epsg"], 4269)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["layer_before"][0])
            doc["extent"]["spatialReference"] = {"wkid": 4326, "latestWkid": 4269}
            candidate["layer_before"] = (_body(doc), candidate["layer_before"][1])
            with self.assertRaisesRegex(ValueError, "source_layer_spatial_reference_drift"):
                sla._validate(candidate)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["page_000"][0])
            doc["spatialReference"] = {"wkid": 4326, "latestWkid": 4269}
            candidate["page_000"] = (_body(doc), candidate["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_spatial_reference_drift"):
                sla._validate(candidate)

            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate["page_000"][0])
            doc["features"][0]["geometry"]["spatialReference"] = {"wkid": 3857}
            candidate["page_000"] = (_body(doc), candidate["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_geometry_spatial_reference_drift"):
                sla._validate(candidate)

    def test_missing_id_truncation_schema_and_geometry_drift_are_rejected(self):
        with fixture_pins():
            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"].pop()
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_drift"):
                sla._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"].pop("OBJECTID")
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_drift"):
                sla._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["OBJECTID"] = 999
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_drift"):
                sla._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["UNEXPECTED_COLUMN"] = "x"
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_feature_field_roster_drift"):
                sla._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["BLOCK_NUMBER"] = 16
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_string_field_type_drift"):
                sla._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["Shape__Length"] = "big"
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_double_field_type_drift"):
                sla._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"] = {"rings": [LINE_A]}
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polyline_geometry_invalid"):
                sla._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"]["paths"][0][1][0] = -200.0
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polyline_coordinates_invalid"):
                sla._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["geometry"]["paths"] = [[[-92.83, 29.59]]]
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_polyline_path_invalid"):
                sla._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["features"][0]["attributes"]["OBJECTID"], doc["features"][1]["attributes"]["OBJECTID"] = (
                doc["features"][1]["attributes"]["OBJECTID"], doc["features"][0]["attributes"]["OBJECTID"])
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_drift"):
                sla._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["page_000"][0])
            doc["exceededTransferLimit"] = True
            captures["page_000"] = (_body(doc), captures["page_000"][1])
            with self.assertRaisesRegex(ValueError, "source_page_error_or_truncation"):
                sla._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["layer_before"][0])
            doc["fields"][1]["type"] = "esriFieldTypeInteger"
            captures["layer_before"] = (_body(doc), captures["layer_before"][1])
            with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
                sla._validate(captures)

            captures = _fixture()
            doc = json.loads(captures["count_after"][0])
            doc["count"] = 5_745
            captures["count_after"] = (_body(doc), captures["count_after"][1])
            with self.assertRaisesRegex(ValueError, "source_count_bracket_mismatch"):
                sla._validate(captures)

    def test_pinned_baselines_are_enforced_once_set(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with fixture_pins() as audit:
                fixture = _fixture()
                with patch.object(sla, "_load_offline", return_value=copy.deepcopy(fixture)), \
                        patch.object(sla, "_geoparquet", return_value=b"candidate"), \
                        patch.object(sla, "processing_identity",
                                     return_value={"code_commit": "a" * 40,
                                                   "files": {"x.py": "b" * 64}}), \
                        patch.object(sla, "validate_construction_code_identity",
                                     return_value=None), \
                        patch.object(capture_common, "validate_construction_code_identity",
                                     return_value=None):
                    result = sla.capture_submerged_land_act_boundary(
                        storage, offline_capture_directory=tmp)
                    self.assertFalse(result["reused"])
                    pins = [
                        ("EXPECTED_SCHEMA_SHA256", audit["source_schema_sha256"]),
                    ]
                    for name, value in pins:
                        with self.subTest(pin=name):
                            with patch.object(sla, name, value):
                                again = sla.capture_submerged_land_act_boundary(
                                    storage, offline_capture_directory=tmp)
                                self.assertTrue(again["reused"])
                    for name, value in (("EXPECTED_SCHEMA_SHA256", "0" * 64),):
                        with self.subTest(refusal=name):
                            with patch.object(sla, name, value):
                                with self.assertRaisesRegex(ValueError, "source_"):
                                    sla.capture_submerged_land_act_boundary(
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
                        doc["features"][0]["attributes"]["BDRY_APRV_DATE"] = "02-NOV-2000"
                        body = _body(doc)
                        receipt = captures["page_000"][1]
                        receipt.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest(),
                                       wire_bytes=len(body),
                                       wire_sha256=hashlib.sha256(body).hexdigest())
                        captures["page_000"] = (body, receipt)
                    with patch.object(sla, "_load_offline", return_value=captures), \
                            patch.object(sla, "_geoparquet", side_effect=parquet), \
                            patch.object(sla, "processing_identity",
                                         return_value=stable_identity), \
                            patch.object(sla, "validate_construction_code_identity",
                                         return_value=None), \
                            patch.object(capture_common, "validate_construction_code_identity",
                                         return_value=None):
                        return sla.capture_submerged_land_act_boundary(
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

    def test_manifest_records_first_party_host_crosswalk_and_pending_acceptance(self):
        fixture = _fixture()
        stable_identity = {"code_commit": "a" * 40, "files": {"x.py": "b" * 64}}
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with fixture_pins():
                with patch.object(sla, "_load_offline", return_value=copy.deepcopy(fixture)), \
                        patch.object(sla, "_geoparquet", return_value=b"candidate"), \
                        patch.object(sla, "processing_identity", return_value=stable_identity), \
                        patch.object(sla, "validate_construction_code_identity",
                                     return_value=None), \
                        patch.object(capture_common, "validate_construction_code_identity",
                                     return_value=None):
                    result = sla.capture_submerged_land_act_boundary(
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
            self.assertIn("resequence all OBJECTIDs", manifest["lineage"])
            self.assertIsNone(manifest["source_release_date"])
            self.assertIn("no dated source release",
                          manifest["inputs"][0]["release_date_unknown_reason"])
            audit = json.loads(storage.read_key(base + "source_audit.json"))
            self.assertFalse(audit["source_authority_accepted"])
            self.assertEqual(audit["crosswalk_open_id"], sla.CROSSWALK_OPEN_ID)
            ready = json.loads(storage.read_key(base + "candidate_ready.json"))
            self.assertFalse(ready["source_authority_accepted"])
            self.assertFalse(result["source_authority_accepted"])

    def test_geoparquet_roundtrip_preserves_native_values_and_geometry(self):
        with fixture_pins():
            fixture = _fixture()
            _, fields, features, _ = sla._validate(copy.deepcopy(fixture))
            with tempfile.TemporaryDirectory() as tmp:
                body = sla._geoparquet(features, fields, Path(tmp))
        import io
        import pyarrow.parquet as pq
        from shapely import from_wkb
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.column_names, [*sla.FIELD_NAMES, "geometry"])
        self.assertEqual(table.num_rows, 4)
        self.assertEqual(table.column("OBJECTID").to_pylist(),
                         [14728977, 14728978, 14728979, 14733728])
        self.assertEqual(table.column("BLOCK_NUMBER").to_pylist(),
                         ["   16", "   17", " 7101", " 7102"])
        self.assertEqual(table.column("BDRY_APRV_DATE").to_pylist(),
                         ["01-NOV-2000", "01-NOV-2000", None, "09-SEP-1998"])
        self.assertEqual(table.column("Shape__Length").to_pylist()[2], None)
        geometries = table.column("geometry").to_pylist()
        self.assertIsNone(geometries[2])
        line = from_wkb(geometries[0])
        self.assertEqual(line.geom_type, "LineString")
        self.assertEqual(line.coords[0][0], -92.83038330078125)
        self.assertEqual(line.coords[0][1], 29.59698486328125)
        multipart = from_wkb(geometries[1])
        self.assertEqual(multipart.geom_type, "MultiLineString")
        self.assertEqual(len(multipart.geoms), 2)
        self.assertEqual(list(multipart.geoms[1].coords)[0][0], -88.0)
        geo_metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                         ["LineString", "MultiLineString"])
        self.assertEqual(geo_metadata["columns"]["geometry"]["crs"].get("name", ""), "NAD83")

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
        real_answer = _body({"objectIdFieldName": "OBJECTID", "objectIds": [14728977, 14728978]})
        url = sla._query(where="1=1", returnIdsOnly="true")
        responses = iter([null_answer, real_answer])

        def respond(request):
            return httpx.Response(200, stream=_Stream(next(responses)),
                                  headers={"content-type": "application/json"})

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            client = httpx.Client(transport=httpx.MockTransport(respond))
            with patch.object(sla, "_BRACKET_RETRY_BASE_DELAY_SECONDS", 0.0):
                body, receipt = sla._acquire_document(client, root, "ids_after", url)
            self.assertEqual(json.loads(body)["objectIds"], [14728977, 14728978])
            self.assertTrue(receipt["complete_body"])
            witnesses = list(root.glob("ids_after.error-*.body"))
            self.assertEqual(len(witnesses), 1)
            self.assertEqual(witnesses[0].read_bytes(), null_answer)

            responses = iter([null_answer] * 4)
            client = httpx.Client(transport=httpx.MockTransport(respond))
            with patch.object(sla, "_BRACKET_RETRY_BASE_DELAY_SECONDS", 0.0):
                body, _receipt = sla._acquire_document(client, root, "ids_after", url)
            self.assertEqual(json.loads(body)["objectIds"], None)
            self.assertEqual(len(list(root.glob("ids_after.error-*.body"))), 5)
            with self.assertRaisesRegex(ValueError,
                                        "source_id_population_changed_during_capture"):
                sla._validate({"ids_before": (_body({"objectIds": [14728977]}), {}),
                               "ids_after": (body, {}),
                               "count_before": (_body({"count": 1}), {}),
                               "count_after": (_body({"count": 1}), {}),
                               "service_before": (_body({"layers": []}), {}),
                               "service_after": (_body({"layers": []}), {}),
                               "layer_before": (_body({"layers": []}), {}),
                               "layer_after": (_body({"layers": []}), {})})

    def test_entrypoint_quarantines_acquisition_validation_failures(self):
        fixture = _fixture()
        doc = json.loads(fixture["page_000"][0])
        doc["features"].pop()
        fixture["page_000"] = (_body(doc), fixture["page_000"][1])
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with fixture_pins(), patch.object(sla, "_load_offline", return_value=fixture):
                with self.assertRaisesRegex(ValueError, "source_page_id_slice_drift"):
                    sla.capture_submerged_land_act_boundary(storage, offline_capture_directory=tmp)
            quarantined = sorted(str(path.relative_to(tmp)) for path in Path(tmp).rglob("*")
                                 if path.is_file())
            self.assertTrue(quarantined)
            self.assertTrue(all(name.startswith(f"_quarantine/{sla.DATASET_SLUG}/")
                                for name in quarantined), quarantined)
            token_root = Path(tmp) / "_quarantine" / sla.DATASET_SLUG
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

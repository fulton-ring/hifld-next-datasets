from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode

import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely.geometry import Point

from dagster_hifld.assets.fema_nss_shelter_locations import (
    fema_nss_shelter_locations_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import fema_nss_shelter_locations as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def body_bytes(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def repo_head():
    root = Path(source.__file__).parents[3]
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()


def synthetic_layer(**overrides):
    layer = {
        "name": source.LAYER_NAME,
        "description": source.LAYER_DESCRIPTION,
        "geometryType": "esriGeometryPoint",
        "definitionExpression": None,
        "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
        "extent": {"spatialReference": {"wkid": 4326, "latestWkid": 4326}},
        "fields": [dict(f) for f in source.FIELD_DECLARATIONS],
    }
    layer.update(overrides)
    return layer


def data_elements_body(entry_layer_id=source.LAYER_ID, nested_layer_id=16, has_z=False,
                       has_m=False, shape_type="esriGeometryPoint", data_element=None):
    element = {"layerId": nested_layer_id, "hasZ": has_z, "hasM": has_m,
               "shapeType": shape_type, "featureType": "esriFTSimple", "dsId": 28}
    if data_element is not None:
        element = data_element
    return {"layerDataElements": [{"layerId": entry_layer_id, "dataElement": element}]}


def synthetic_features(geometries):
    attrs = [f for f in source.FIELD_DECLARATIONS if f["type"] != "esriFieldTypeGeometry"]
    features = []
    for oid, geometry in zip((101, 102), geometries):
        row = {f["name"]: None for f in attrs}
        row.update({"objectid": oid, "shelter_id": 9001, "shelter_status_code": "TEST",
                    "status": "SYN", "shelter_open_date": 1767225600000})
        features.append({"attributes": row, "geometry": geometry})
    return features


def full_capture_bodies(layer, dimensions, dimensions_after=None, features=None):
    features = features if features is not None else synthetic_features(
        [{"x": -77.04, "y": 38.90}, {"x": -122.33, "y": 47.61}])
    service = {"description": source.SERVICE_DESCRIPTION}
    portal = {"id": source.ITEM_ID, "owner": "FEMA_MAC",
              "title": "National Shelter System Facilities", "type": "Feature Service",
              "url": source.PORTAL_LAYER_URL,
              "access": "public", "modified": 1756399172000}
    ids = {"objectIdFieldName": "objectid", "objectIds": [101, 102]}
    count = len(features)
    page_params = {
        "objectIds": ",".join(str(oid) for oid in sorted(row["attributes"]["objectid"] for row in features)),
        "outFields": "*", "returnGeometry": "true", "returnZ": "true",
        "returnM": "true", "outSR": "4326", "resultRecordCount": source.PAGE_SIZE,
        "f": "json"}
    page = {"spatialReference": {"wkid": 4326, "latestWkid": 4326},
            "exceededTransferLimit": False, "features": features}
    dimensions_after = dimensions if dimensions_after is None else dimensions_after
    return {
        "portal_item": (source.PORTAL_ITEM_URL, None, portal),
        "service_before": (source.SERVICE_URL, {"f": "pjson"}, service),
        "service_after": (source.SERVICE_URL, {"f": "pjson"}, service),
        "layer_before": (source.LAYER_URL, {"f": "pjson"}, layer),
        "layer_after": (source.LAYER_URL, {"f": "pjson"}, layer),
        "data_elements_before": (source.QUERY_DATA_ELEMENTS_URL,
                                  {"f": "pjson", "layers": f"[{source.LAYER_ID}]"}, dimensions),
        "data_elements_after": (source.QUERY_DATA_ELEMENTS_URL,
                                 {"f": "pjson", "layers": f"[{source.LAYER_ID}]"}, dimensions_after),
        "count_before": (source.LAYER_URL + "/query",
                         {"where": "1=1", "returnCountOnly": "true", "f": "json"}, {"count": count}),
        "count_after": (source.LAYER_URL + "/query",
                        {"where": "1=1", "returnCountOnly": "true", "f": "json"}, {"count": count}),
        "ids_before": (source.LAYER_URL + "/query",
                       {"where": "1=1", "returnIdsOnly": "true", "f": "json"}, ids),
        "ids_after": (source.LAYER_URL + "/query",
                      {"where": "1=1", "returnIdsOnly": "true", "f": "json"}, ids),
        "page_000": (source.LAYER_URL + "/query", page_params, page),
    }


def build_retained_capture(root, bodies):
    root.mkdir(parents=True, exist_ok=True)
    captures = {}
    for name, (url, params, obj) in bodies.items():
        body = body_bytes(obj)
        target = url + ("?" + urlencode(params) if params else "")
        receipt = {"method": "GET", "url": target, "final_url": target,
                   "observed_at": "2026-11-08T00:00:00+00:00", "status": 200,
                   "complete_body": True, "bytes": len(body), "sha256": sha256(body),
                   "wire_bytes": len(body), "wire_sha256": sha256(body),
                   "content_encoding": None, "content_length": None,
                   "error_code": None, "error_type": None}
        (root / f"{name}.wire").write_bytes(body)
        (root / f"{name}.body").write_bytes(body)
        (root / f"{name}.receipt.json").write_bytes(canonical(receipt) + b"\n")
        captures[name] = (body, receipt)
    retained = sum(len(body) + len(body) for body, _ in captures.values())
    _, manifest_sha = source._write_capture_manifest(
        root, captures, retained, source.MAX_CAPTURE_BYTES, source.MAX_RESPONSE_BYTES)
    return manifest_sha


def replay_retained_capture(tmp, bodies):
    """Replay a retained capture through the real unpatched module path.

    Only the transport is absent by construction (retained receipts); request
    roster/receipt validation, provenance, processing identity and code-identity
    validation all run for real.
    """
    storage = StagingStorageResource(local_dir=str(tmp / "staging"), use_local=True,
                                     backend="local")
    manifest_sha = build_retained_capture(tmp / "retained", bodies)
    return source.capture_fema_nss_shelter_locations(
        storage, version="vsynthetic",
        retained_capture_directory=str(tmp / "retained"),
        retained_capture_manifest_sha256=manifest_sha), storage


def quarantine_raw_bodies(tmp):
    root = tmp / "staging" / "_quarantine" / source.DATASET_SLUG
    bodies = {}
    for failure in root.glob("*/metadata/failure.json"):
        for raw in (failure.parent.parent / "raw").glob("*.body"):
            bodies[raw.name] = raw.read_bytes()
    return bodies


class FemaNssShelterLocationsTests(unittest.TestCase):
    def test_tiny_synthetic_live_capture_preserves_native_fields_and_binds_real_identity(self):
        fields = [dict(f) for f in source.FIELD_DECLARATIONS]
        attrs = [f for f in fields if f["type"] != "esriFieldTypeGeometry"]
        service = {"description": source.SERVICE_DESCRIPTION}
        layer = synthetic_layer(hasZ=False, hasM=False)
        portal = {"id": source.ITEM_ID, "owner": "FEMA_MAC",
                  "title": "National Shelter System Facilities", "type": "Feature Service",
                  "url": source.PORTAL_LAYER_URL,
                  "access": "public", "modified": 1756399172000}
        ids = {"objectIdFieldName": "objectid", "objectIds": [101, 102]}
        dimensions = data_elements_body()
        features = []
        for oid, x, y in ((101, -77.04, 38.90), (102, -122.33, 47.61)):
            row = {f["name"]: None for f in attrs}
            row.update({"objectid": oid, "shelter_id": 9001, "shelter_status_code": "TEST",
                        "status": "SYN", "shelter_open_date": 1767225600000,
                        "org_email": None, "org_poc_phone": None})
            features.append({"attributes": row, "geometry": {"x": x, "y": y}})
        bodies = {
            "portal_item": portal,
            "service_before": service,
            "service_after": service,
            "layer_before": layer,
            "layer_after": layer,
            "data_elements_before": dimensions,
            "data_elements_after": dimensions,
            "count_before": {"count": 2},
            "count_after": {"count": 2},
            "ids_before": ids,
            "ids_after": ids,
            "page_000": {"spatialReference": {"wkid": 4326, "latestWkid": 4326},
                         "exceededTransferLimit": False, "features": features},
        }
        observed = {}

        def fake_capture(_client, url, root, name, **kwargs):
            body = json.dumps(bodies[name], separators=(",", ":"), ensure_ascii=False).encode()
            receipt = {"method": "GET", "url": url, "final_url": url,
                       "observed_at": "2026-10-22T00:00:00+00:00", "status": 200,
                       "complete_body": True, "bytes": len(body),
                       "sha256": hashlib.sha256(body).hexdigest(),
                       "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest()}
            root.mkdir(parents=True, exist_ok=True)
            kwargs["before_retain"]("wire", body)
            kwargs["before_retain"]("body", body)
            (root / f"{name}.wire").write_bytes(body)
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.receipt.json").write_bytes(canonical(receipt) + b"\n")
            observed[name] = url
            return body, receipt

        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            storage = StagingStorageResource(local_dir=str(tmp / "staging"), use_local=True,
                                             backend="local")
            with patch.object(source, "capture_get", side_effect=fake_capture), \
                 patch.object(source.httpx, "Client"):
                result = source.capture_fema_nss_shelter_locations(
                    storage, version="vsynthetic", capture_directory=str(tmp / "capture"))

            self.assertEqual(result["feature_count"], 2)
            self.assertFalse(result["release_accepted"])
            self.assertFalse(result["rights_accepted"])
            self.assertEqual(fema_nss_shelter_locations_draft_schedule.default_status,
                             DefaultScheduleStatus.STOPPED)
            ready = json.loads(storage.read_key(result["candidate_ready"]))
            self.assertFalse(ready["approved"])
            self.assertFalse(ready["historical_equivalence_accepted"])
            native_path = (tmp / "staging" / result["candidate_ready"].replace(
                "metadata/candidate_ready.json", f"geoparquet/{source.DATASET_SLUG}.parquet"))
            table = pq.read_table(native_path)
            self.assertEqual(table.column_names,
                             [f["name"] for f in attrs] + ["geometry"])
            self.assertEqual(table["objectid"].to_pylist(), [101, 102])
            self.assertEqual(table["shelter_id"].to_pylist(), [9001, 9001])
            self.assertEqual(table["shelter_status_code"].to_pylist(), ["TEST", "TEST"])
            self.assertEqual(table["shelter_open_date"].to_pylist(), [1767225600000] * 2)
            self.assertEqual(table["org_email"].to_pylist(), [None, None])
            self.assertEqual(table["geometry"].to_pylist(), [
                Point(-77.04, 38.90).wkb, Point(-122.33, 47.61).wkb])
            geo = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo["columns"]["geometry"]["crs"]["id"],
                             {"authority": "EPSG", "code": 4326})
            self.assertIn("ids_before", observed)
            self.assertEqual(source.DATASET_SLUG, "national-shelter-system-facilities")
            identity = json.loads(storage.read_key(
                result["candidate_ready"].replace("metadata/candidate_ready.json",
                                                  "metadata/processing_identity.json")))
            self.assertEqual(identity["code_identity_scheme"], "git-commit-blobs-v1")
            self.assertEqual(identity["working_tree_status"], "clean")
            self.assertEqual(identity["code_commit"], repo_head())
            self.assertIn("src/dagster_hifld/sources/fema_nss_shelter_locations.py",
                          identity["files"])
            manifest = json.loads(storage.read_key(
                result["candidate_ready"].replace("metadata/candidate_ready.json",
                                                  "metadata/source_manifest.json")))
            self.assertEqual(manifest["code_commit"], repo_head())

    def test_retained_replay_of_live_catalog_shape_with_explicit_declaration_captures(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            result, storage = replay_retained_capture(
                tmp, full_capture_bodies(synthetic_layer(), data_elements_body()))
            self.assertEqual(result["feature_count"], 2)
            audit = json.loads(storage.read_key(
                result["candidate_ready"].replace("metadata/candidate_ready.json",
                                                 "metadata/source_audit.json")))
            self.assertEqual(audit["source_geometry_dimensions"],
                             {"hasZ": False, "hasM": False,
                              "declared_by": "same-service MapServer queryDataElements dataset declaration for the requested layer",
                              "layer_catalog_declared_hasZ": False,
                              "layer_catalog_declared_hasM": False})
            manifest = json.loads(storage.read_key(
                result["candidate_ready"].replace("metadata/candidate_ready.json",
                                                 "metadata/source_manifest.json")))
            pages = [i for i in manifest["inputs"] if i["input_id"].startswith("page_")]
            self.assertTrue(pages)
            for page in pages:
                self.assertIn("returnZ=true", page["retrieval_url"])
                self.assertIn("returnM=true", page["retrieval_url"])
            declaration = [i for i in manifest["inputs"]
                           if i["input_id"] == "data_elements_before"]
            self.assertEqual(declaration[0]["retrieval_url"],
                             source.QUERY_DATA_ELEMENTS_URL + "?f=pjson&layers=%5B5%5D")
            self.assertEqual(manifest["code_commit"], repo_head())

    def test_retained_replay_reuses_the_sealed_candidate_from_the_real_identity_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            storage = StagingStorageResource(local_dir=str(tmp / "staging"), use_local=True,
                                            backend="local")
            manifest_sha = build_retained_capture(
                tmp / "retained", full_capture_bodies(synthetic_layer(), data_elements_body()))
            first = source.capture_fema_nss_shelter_locations(
                storage, version="vsynthetic",
                retained_capture_directory=str(tmp / "retained"),
                retained_capture_manifest_sha256=manifest_sha)
            second = source.capture_fema_nss_shelter_locations(
                storage, version="vsynthetic",
                retained_capture_directory=str(tmp / "retained"),
                retained_capture_manifest_sha256=manifest_sha)
            self.assertFalse(first.get("reused", False))
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])
            identity = json.loads(storage.read_key(
                first["candidate_ready"].replace("metadata/candidate_ready.json",
                                                 "metadata/processing_identity.json")))
            self.assertEqual(identity["code_identity_scheme"], "git-commit-blobs-v1")
            self.assertEqual(identity["code_commit"], repo_head())
            self.assertEqual(identity["working_tree_status"], "clean")

    def test_retained_replay_nested_data_element_layer_id_is_not_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            result, _ = replay_retained_capture(
                tmp, full_capture_bodies(synthetic_layer(),
                                         data_elements_body(nested_layer_id=17)))
            self.assertEqual(result["feature_count"], 2)

    def test_retained_replay_rejects_missing_or_malformed_dimension_declarations(self):
        for dimensions in ({"layerDataElements": []},
                           {},
                           {"layerDataElements": [5]},
                           data_elements_body(data_element={"layerId": 16, "hasZ": False,
                                                             "shapeType": "esriGeometryPoint"}),
                           {"error": {"code": 400, "message": "synthetic"}}):
            with self.subTest(dimensions=dimensions), tempfile.TemporaryDirectory() as tmp:
                tmp = Path(tmp)
                bodies = full_capture_bodies(synthetic_layer(), dimensions)
                with self.assertRaises(ValueError) as raised:
                    replay_retained_capture(tmp, bodies)
                self.assertIn(str(raised.exception),
                              ("source_data_elements_invalid",
                               "source_data_element_dimensions_unpinned",
                               "source_response_error:data_elements_before"))

    def test_retained_replay_rejects_changed_outer_layer_id(self):
        for entry_layer_id in (4, 6, "5", None):
            with self.subTest(entry_layer_id=entry_layer_id), tempfile.TemporaryDirectory() as tmp:
                tmp = Path(tmp)
                bodies = full_capture_bodies(synthetic_layer(),
                                             data_elements_body(entry_layer_id=entry_layer_id))
                with self.assertRaisesRegex(ValueError, "source_data_element_layer_id_drift"):
                    replay_retained_capture(tmp, bodies)

    def test_retained_replay_rejects_non_boolean_dimension_declarations(self):
        for has_z, has_m in (("false", False), (0, False), (None, False),
                             (False, "true"), (False, 1), (False, None)):
            with self.subTest(has_z=has_z, has_m=has_m), tempfile.TemporaryDirectory() as tmp:
                tmp = Path(tmp)
                bodies = full_capture_bodies(synthetic_layer(),
                                             data_elements_body(has_z=has_z, has_m=has_m))
                with self.assertRaisesRegex(ValueError, "source_data_element_dimensions_unpinned"):
                    replay_retained_capture(tmp, bodies)

    def test_retained_replay_rejects_conflicting_or_unsupported_dimensions(self):
        cases = [
            (synthetic_layer(hasZ=True, hasM=False), data_elements_body(),
             "source_geometry_dimensions_conflict"),
            (synthetic_layer(hasZ="maybe", hasM=False), data_elements_body(),
             "source_geometry_dimensions_unpinned"),
            (synthetic_layer(hasZ=None, hasM=False), data_elements_body(),
             "source_geometry_dimensions_unpinned"),
            (synthetic_layer(hasZ=False, hasM=False), data_elements_body(has_m=True),
             "source_geometry_dimensions_conflict"),
            (synthetic_layer(hasZ=False, hasM="no"), data_elements_body(),
             "source_geometry_dimensions_unpinned"),
            (synthetic_layer(), data_elements_body(shape_type="esriGeometryPolyline"),
             "source_geometry_dimensions_conflict"),
            (synthetic_layer(), data_elements_body(has_z=True), "source_geometry_dimensions_unsupported"),
        ]
        for layer, dimensions, expected in cases:
            with self.subTest(layer=layer.get("hasZ", "absent"), dimensions=dimensions), \
                    tempfile.TemporaryDirectory() as tmp:
                tmp = Path(tmp)
                with self.assertRaisesRegex(ValueError, expected):
                    replay_retained_capture(tmp, full_capture_bodies(layer, dimensions))

    def test_retained_replay_rejects_declaration_changed_during_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            changed = data_elements_body(nested_layer_id=16, has_z=True)
            bodies = full_capture_bodies(synthetic_layer(), data_elements_body(),
                                         dimensions_after=changed)
            with self.assertRaisesRegex(ValueError, "source_changed_during_capture"):
                replay_retained_capture(tmp, bodies)

    def test_retained_replay_aborts_on_unexpected_z_or_m_ordinates_without_dropping(self):
        for extra, ordinate in (({"z": 12.5}, b'"z":12.5'), ({"m": 99.5}, b'"m":99.5'),
                                ({"z": 12.5, "m": 99.5}, b'"z":12.5')):
            with self.subTest(extra=extra), tempfile.TemporaryDirectory() as tmp:
                tmp = Path(tmp)
                features = synthetic_features([{"x": -77.04, "y": 38.90} | extra,
                                               {"x": -122.33, "y": 47.61}])
                bodies = full_capture_bodies(synthetic_layer(), data_elements_body(),
                                             features=features)
                with self.assertRaisesRegex(ValueError, "source_geometry_dimensions_drift"):
                    replay_retained_capture(tmp, bodies)
                quarantined = quarantine_raw_bodies(tmp)
                self.assertIn(ordinate, quarantined["page_000.body"])
                staged = sorted(p.name for p in (tmp / "staging").iterdir())
                self.assertEqual(staged, ["_quarantine"])

    def test_retained_replay_missing_declaration_receipt_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            bodies = full_capture_bodies(synthetic_layer(), data_elements_body())
            del bodies["data_elements_before"]
            with self.assertRaisesRegex(
                    ValueError, "retained_capture_request_mismatch:data_elements_before"):
                replay_retained_capture(tmp, bodies)

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, \
                tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          "geoparquet/national-shelter-system-facilities.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/national-shelter-system-facilities.parquet",
                                           b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError,
                                            "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, source.DATASET_SLUG,
                                        source.FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)

    def test_schema_contract_uses_the_declared_oid_and_all_72_attributes(self):
        metadata = {"fields": source.FIELD_DECLARATIONS}
        parsed = source._field_contract(metadata)
        self.assertEqual(len(parsed), 72)
        self.assertEqual([f["name"] for f in parsed if f["type"] == "esriFieldTypeOID"],
                         ["objectid"])
        changed = {"fields": [dict(f) for f in source.FIELD_DECLARATIONS]}
        changed["fields"][0]["alias"] = "changed"
        with self.assertRaisesRegex(ValueError, "source_schema_drift"):
            source._field_contract(changed)
        self.assertEqual(source.MAX_RESPONSE_BYTES, 4_000_000)
        # The capture budget must hold the full live population (254,968,822
        # bytes were retained by page_273 of 288 on 2026-11-11 before the old
        # 256,000,000-byte bound failed the run) while staying bounded.
        self.assertGreater(source.MAX_CAPTURE_BYTES, 269_000_000)
        self.assertLessEqual(source.MAX_CAPTURE_BYTES, 512_000_000)


if __name__ == "__main__":
    unittest.main()

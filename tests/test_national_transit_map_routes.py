"""Synthetic checks for the current BTS NTAD routes source adapter."""
from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq
import shapely
from dagster import DagsterInstance, materialize

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.national_transit_map_routes import (
    national_transit_map_routes,
    national_transit_map_routes_complete,
    national_transit_map_routes_draft_schedule,
)
from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import national_transit_map_routes as source

_HTTPX_CLIENT = httpx.Client

# Dated 2026-11-09 anonymous probes of the live producer (PR #694): count,
# sorted-ID-roster sha256 and canonical schema sha256 recomputed from
# FeatureServer/0 metadata after the 2026-11-02T15:21:08Z republish.
DATED_LIVE_COUNT = 76_687
DATED_LIVE_SCHEMA_SHA256 = "86e2861b4da8fce1f1b837afe74382b27992ff7956ec06eec641b5ffe81a689b"
DATED_LIVE_ID_SHA256 = "b22ed62787e1a89324226b0957339ed34bcaba3ff4413b22e9fbcd7e88d2a250"


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def _sha(value):
    return hashlib.sha256(value).hexdigest()


def _fields():
    rows = []
    for name, kind in zip(source.EXPECTED_FIELD_NAMES, source.EXPECTED_FIELD_TYPES):
        field = {"name": name, "type": kind, "alias": name,
                 "nullable": name != "OBJECTID", "domain": None}
        if kind == "esriFieldTypeString":
            field["length"] = 254
        rows.append(field)
    return rows


def _feature_paths():
    # Native Z-preserving producer shapes: one all-zero-z path and one path
    # carrying a nonzero z so carry-through is observable end to end.
    return (
        [[[0.0, 1.0, 0.0], [2.0, 3.0, 0.0]], [[4.0, 5.0, 5.0], [6.0, 7.0, 5.0]]],
        [[[8.0, 9.0, 0.0], [10.0, 11.0, 0.0]]],
    )


def _fixture_factory():
    fields = _fields()
    schema_sha = _sha(_canonical(fields))
    ids_sha = _sha(_canonical([1, 2]))
    item = {
        "id": source.ITEM_ID,
        "owner": "USDOT_BTS",
        "orgId": "xOi1kZaI0eWDREZv",
        "title": source.LAYER_NAME,
        "url": source.SERVICE_URL,
        "access": "public",
        "licenseInfo": "Creative Commons Attribution 3.0 United States (CC-BY-3.0)",
        "description": (
            "The National Transit Map Routes dataset was compiled on September 23, 2026 "
            "from the Bureau of Transportation Statistics (BTS) and is part of the "
            "National Transportation Atlas Database (NTAD)."
        ),
    }
    service = {
        "serviceItemId": source.ITEM_ID,
        "layers": [{"id": 0, "name": source.LAYER_NAME,
                    "type": "Feature Layer", "geometryType": "esriGeometryPolyline"}],
        "maxRecordCount": 1000,
    }
    layer = {
        "name": source.LAYER_NAME,
        "geometryType": "esriGeometryPolyline",
        "objectIdField": "OBJECTID",
        "hasZ": True,
        "hasM": False,
        "extent": {"spatialReference": {"wkid": 4326, "latestWkid": 4326}},
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "maxRecordCount": 2000,
        "fields": fields,
        "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
    }
    features = []
    for oid, routeshort, pathsets, length in (
        (1, "R1", _feature_paths()[0], 4.0),
        (2, "R2", _feature_paths()[1], 2.0),
    ):
        attrs = {field["name"]: None for field in fields}
        attrs.update({"OBJECTID": oid, "ntd_id": f"NTD{oid}", "feed_id": f"feed{oid}",
                      "shape_id": f"shape{oid}", "route_id": f"route{oid}",
                      "agency_id": f"agency{oid}", "route_short_name": routeshort,
                      "route_type": "3", "route_type_text": "Bus", "download_date": "2026-09-23",
                      "Shape__Length": length})
        features.append({"attributes": attrs,
                         "geometry": {"hasZ": True, "paths": pathsets}})

    feature_requests = []
    knobs = {"final_page_exceeded_transfer_limit": False,
             "page_failures_before_success": 0}

    def factory():
        def handler(request):
            query = parse_qs(request.url.query.decode())
            path = request.url.path
            if path.endswith("/query") and query.get("returnCountOnly") == ["true"]:
                result = {"count": 2}
            elif path.endswith("/query") and query.get("returnIdsOnly") == ["true"]:
                result = {"objectIdFieldName": "OBJECTID", "objectIds": [1, 2]}
            elif path.endswith("/query"):
                feature_requests.append(path + "?" + request.url.query.decode())
                if knobs["page_failures_before_success"] > 0:
                    knobs["page_failures_before_success"] -= 1
                    return httpx.Response(504, request=request)
                offset = int(query.get("resultOffset", ["0"])[0])
                limit = int(query.get("resultRecordCount", ["1000"])[0])
                served = features[offset:offset + limit]
                result = {"spatialReference": {"wkid": 4326, "latestWkid": 4326},
                          "hasZ": True, "features": served}
                more = offset + len(served) < len(features)
                if more or knobs["final_page_exceeded_transfer_limit"]:
                    result["exceededTransferLimit"] = True
            elif path.endswith("/FeatureServer/0"):
                result = layer
            elif path.endswith("/FeatureServer"):
                result = service
            elif path.endswith("/sharing/rest/content/items/" + source.ITEM_ID):
                result = item
            else:
                return httpx.Response(404, request=request)
            body = _canonical(result)
            return httpx.Response(200, stream=httpx.ByteStream(body), request=request,
                                  headers={"content-length": str(len(body))})
        return _HTTPX_CLIENT(transport=httpx.MockTransport(handler), follow_redirects=False)

    return factory, fields, features, layer, schema_sha, ids_sha, feature_requests, knobs


class NationalTransitMapRoutesTests(unittest.TestCase):
    def _patched(self, factory, schema_sha, ids_sha, extra=()):
        return (
            patch.object(source, "EXPECTED_SOURCE_COUNT", 2),
            patch.object(source, "EXPECTED_SOURCE_SCHEMA_SHA256", schema_sha),
            patch.object(source, "EXPECTED_SOURCE_ID_SHA256", ids_sha),
            patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: factory()),
            *extra,
        )

    def test_mocked_capture_preserves_schema_and_native_z(self):
        factory, fields, features, _layer, schema_sha, ids_sha, feature_requests, _knobs = _fixture_factory()
        patchers = self._patched(factory, schema_sha, ids_sha)
        with tempfile.TemporaryDirectory() as tmp:
            for p in patchers:
                p.start()
            self.addCleanup(lambda: [p.stop() for p in reversed(patchers)])
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            instance_dir = Path(tmp) / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            capture_root = Path(tmp) / "capture"
            with DagsterInstance.from_config(str(instance_dir)) as instance:
                result = materialize(
                    [national_transit_map_routes],
                    run_config={"ops": {national_transit_map_routes.op.name: {
                        "config": {"capture_directory": str(capture_root)}}}},
                    resources={"staging_storage": storage}, instance=instance,
                )
            self.assertTrue(result.success)
            candidate = result.output_for_node(national_transit_map_routes.op.name)
            self.assertEqual(candidate["feature_count"], 2)
            self.assertTrue(candidate["source_population_complete"])
            self.assertEqual(candidate["source_z_point_count"], 6)
            self.assertEqual(candidate["source_nonzero_z_point_count"], 2)
            self.assertTrue(all("returnZ=true" in request for request in feature_requests))
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
            audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
            self.assertEqual(audit["source_schema_sha256"], schema_sha)
            self.assertEqual([item["name"] for item in audit["source_fields"]],
                             [field["name"] for field in fields])
            self.assertEqual(audit["source_fields"][-1]["name"], "Shape__Length")
            self.assertEqual(audit["source_geometry_type"], "esriGeometryPolyline")
            self.assertTrue(audit["source_geometry_has_z"])
            self.assertEqual(audit["source_z_point_count"], 6)
            self.assertEqual(audit["source_nonzero_z_point_count"], 2)
            paths1, paths2 = _feature_paths()
            expected_geom_repr = [[1, paths1], [2, paths2]]
            self.assertEqual(audit["source_geometry_sha256"], _sha(_canonical(expected_geom_repr)))
            self.assertEqual(audit["source_record_sha256"], _sha(_canonical(features)))
            self.assertFalse(audit["rights_accepted"])
            self.assertFalse(audit["release_accepted"])
            parquet_key = f"{prefix}/geoparquet/{source.DATASET_SLUG}.parquet"
            table = pq.read_table(io.BytesIO(storage.read_key(parquet_key)))
            self.assertEqual(table.column_names, source.EXPECTED_FIELD_NAMES + ["geometry"])
            self.assertEqual(table.column("Shape__Length").to_pylist(), [4.0, 2.0])
            _primary, column, crs = geometry_metadata(table.schema)
            self.assertEqual(column["geometry_types"], ["MultiLineString Z"])
            self.assertEqual(crs.to_epsg(), 4326)
            geom = shapely.from_wkb(table.column("geometry")[0].as_py())
            self.assertEqual(geom.geom_type, "MultiLineString")
            self.assertTrue(geom.has_z)
            self.assertEqual(list(geom.geoms[0].coords), [(0.0, 1.0, 0.0), (2.0, 3.0, 0.0)])
            self.assertEqual(list(geom.geoms[1].coords), [(4.0, 5.0, 5.0), (6.0, 7.0, 5.0)])
            self.assertIn(f"{prefix}/raw/page_000.body", storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/raw/ids_before.body", storage.list_prefix(prefix))

    def test_multipage_capture_accepts_exceeded_transfer_limit_on_nonfinal_page(self):
        factory, _fields, _features, _layer, schema_sha, ids_sha, feature_requests, _knobs = _fixture_factory()
        patchers = self._patched(factory, schema_sha, ids_sha,
                                 extra=(patch.object(source, "PAGE_SIZE", 1),))
        with tempfile.TemporaryDirectory() as tmp:
            for p in patchers:
                p.start()
            self.addCleanup(lambda: [p.stop() for p in reversed(patchers)])
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            candidate = source.capture_national_transit_map_routes(
                storage, capture_directory=str(Path(tmp) / "captures"))
            self.assertEqual(candidate["feature_count"], 2)
            self.assertEqual(len(feature_requests), 2)
            self.assertTrue(all("returnZ=true" in request for request in feature_requests))
            self.assertTrue(all("resultOffset=" in request for request in feature_requests))

    def test_exceeded_transfer_limit_on_final_page_is_rejected(self):
        factory, _fields, _features, _layer, schema_sha, ids_sha, _requests, knobs = _fixture_factory()
        knobs["final_page_exceeded_transfer_limit"] = True
        patchers = self._patched(factory, schema_sha, ids_sha)
        with tempfile.TemporaryDirectory() as tmp:
            for p in patchers:
                p.start()
            self.addCleanup(lambda: [p.stop() for p in reversed(patchers)])
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "source_page_error_or_truncated"):
                source.capture_national_transit_map_routes(
                    storage, capture_directory=str(Path(tmp) / "captures"))

    def test_two_dimensional_geometry_is_rejected(self):
        factory, _fields, features, _layer, schema_sha, ids_sha, _requests, _knobs = _fixture_factory()
        features[0]["geometry"]["paths"] = [[[0.0, 1.0], [2.0, 3.0]]]
        patchers = self._patched(factory, schema_sha, ids_sha)
        with tempfile.TemporaryDirectory() as tmp:
            for p in patchers:
                p.start()
            self.addCleanup(lambda: [p.stop() for p in reversed(patchers)])
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "source_polyline_coordinate_invalid"):
                source.capture_national_transit_map_routes(
                    storage, capture_directory=str(Path(tmp) / "captures"))

    def test_geometry_without_has_z_is_rejected(self):
        factory, _fields, features, _layer, schema_sha, ids_sha, _requests, _knobs = _fixture_factory()
        features[1]["geometry"] = {"paths": _feature_paths()[1]}
        patchers = self._patched(factory, schema_sha, ids_sha)
        with tempfile.TemporaryDirectory() as tmp:
            for p in patchers:
                p.start()
            self.addCleanup(lambda: [p.stop() for p in reversed(patchers)])
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "source_polyline_geometry_roster_drift"):
                source.capture_national_transit_map_routes(
                    storage, capture_directory=str(Path(tmp) / "captures"))

    def test_geometry_with_has_z_false_is_rejected(self):
        factory, _fields, features, _layer, schema_sha, ids_sha, _requests, _knobs = _fixture_factory()
        features[1]["geometry"] = {"hasZ": False, "paths": _feature_paths()[1]}
        patchers = self._patched(factory, schema_sha, ids_sha)
        with tempfile.TemporaryDirectory() as tmp:
            for p in patchers:
                p.start()
            self.addCleanup(lambda: [p.stop() for p in reversed(patchers)])
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "source_polyline_geometry_dimension_drift"):
                source.capture_national_transit_map_routes(
                    storage, capture_directory=str(Path(tmp) / "captures"))

    def test_layer_without_z_dimension_is_rejected_before_page_requests(self):
        factory, _fields, _features, layer, schema_sha, ids_sha, feature_requests, _knobs = _fixture_factory()
        # The layer metadata pin is validated against the hosted layer: hasZ must
        # now be True (2026-11-09 probe), so a 2-D layer stops the capture early.
        layer["hasZ"] = False
        patchers = self._patched(factory, schema_sha, ids_sha)
        with tempfile.TemporaryDirectory() as tmp:
            for p in patchers:
                p.start()
            self.addCleanup(lambda: [p.stop() for p in reversed(patchers)])
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "source_layer_identity_or_crs_drift"):
                source.capture_national_transit_map_routes(
                    storage, capture_directory=str(Path(tmp) / "captures"))
            self.assertEqual(feature_requests, [])

    def test_invalid_pinned_seed_is_rejected_before_feature_page_request(self):
        factory, _fields, _features, _layer, schema_sha, _ids_sha, feature_requests, _knobs = _fixture_factory()
        patchers = self._patched(factory, schema_sha, "0" * 64)
        with tempfile.TemporaryDirectory() as tmp:
            for p in patchers:
                p.start()
            self.addCleanup(lambda: [p.stop() for p in reversed(patchers)])
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "source_seed_id_roster_drift"):
                source.capture_national_transit_map_routes(
                    storage, capture_directory=str(Path(tmp) / "captures"))
            self.assertEqual(feature_requests, [])

    def test_non_integer_page_object_id_stops_before_next_page(self):
        factory, _fields, features, _layer, schema_sha, ids_sha, feature_requests, _knobs = _fixture_factory()
        features[0]["attributes"]["OBJECTID"] = True
        patchers = self._patched(factory, schema_sha, ids_sha,
                                 extra=(patch.object(source, "PAGE_SIZE", 1),))
        with tempfile.TemporaryDirectory() as tmp:
            for p in patchers:
                p.start()
            self.addCleanup(lambda: [p.stop() for p in reversed(patchers)])
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "source_page_object_id_type_drift"):
                source.capture_national_transit_map_routes(
                    storage, capture_directory=str(Path(tmp) / "captures"))
            self.assertEqual(len(feature_requests), 1)

    def test_streaming_list_digest_matches_materialized_canonical(self):
        for value in ([], [1, 2], [{"b": 1, "a": 2}, [3, 4]],
                      [[1, [[[0.0, 1.0, 0.0], [2.0, 3.0, 5.0]]]], [2, None]]):
            digest = source._StreamingCanonicalListDigest()
            for element in value:
                digest.add(element)
            self.assertEqual(digest.hexdigest(), _sha(_canonical(value)))

    def test_transient_page_failure_is_retried_within_bounds(self):
        factory, _fields, _features, _layer, schema_sha, ids_sha, _requests, knobs = _fixture_factory()
        knobs["page_failures_before_success"] = 1
        patchers = self._patched(factory, schema_sha, ids_sha,
                                 extra=(patch.object(source, "PAGE_RETRY_BACKOFF_SECONDS", (0,)),))
        with tempfile.TemporaryDirectory() as tmp:
            for p in patchers:
                p.start()
            self.addCleanup(lambda: [p.stop() for p in reversed(patchers)])
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            candidate = source.capture_national_transit_map_routes(
                storage, capture_directory=str(Path(tmp) / "captures"))
            self.assertEqual(candidate["feature_count"], 2)

    def test_persistent_page_failure_fails_closed_after_bounded_retry(self):
        factory, _fields, _features, _layer, schema_sha, ids_sha, _requests, knobs = _fixture_factory()
        knobs["page_failures_before_success"] = 99
        patchers = self._patched(factory, schema_sha, ids_sha,
                                 extra=(patch.object(source, "PAGE_ATTEMPTS", 3),
                                        patch.object(source, "PAGE_RETRY_BACKOFF_SECONDS", (0, 0))))
        with tempfile.TemporaryDirectory() as tmp:
            for p in patchers:
                p.start()
            self.addCleanup(lambda: [p.stop() for p in reversed(patchers)])
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "incomplete_source_capture:page_000"):
                source.capture_national_transit_map_routes(
                    storage, capture_directory=str(Path(tmp) / "captures"))

    def test_failure_evidence_skips_oversized_files_without_reading_them(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (root / "page_000.body").open("wb") as stream:
                stream.truncate(source.MAX_BODY_BYTES + 1)
            with (root / "page_000.receipt.json").open("wb") as stream:
                stream.truncate(source.MAX_FAILURE_RECEIPT_BYTES + 1)
            (root / "page_000.wire").touch()
            with (root / "page_001.body").open("wb") as stream:
                stream.truncate(source.MAX_BODY_BYTES + 1)
            (root / "page_001.receipt.json").write_text("{}")
            (root / "page_001.wire").touch()
            evidence, omitted = source._bounded_failure_evidence(root, set(), "test_failure")
            self.assertEqual(evidence, {})
            self.assertEqual(omitted, ["page_000", "page_001"])

    def test_committed_pins_record_the_dated_2026_11_09_live_probe(self):
        self.assertEqual(source.EXPECTED_SOURCE_COUNT, DATED_LIVE_COUNT)
        self.assertEqual(source.EXPECTED_SOURCE_SCHEMA_SHA256, DATED_LIVE_SCHEMA_SHA256)
        self.assertEqual(source.EXPECTED_SOURCE_ID_SHA256, DATED_LIVE_ID_SHA256)
        self.assertEqual(source.PAGE_SIZE, 1000)
        self.assertGreaterEqual(source.MAX_BODY_BYTES, 32_000_000)
        self.assertGreaterEqual(source.MAX_CAPTURE_TOTAL_BYTES, 4_000_000_000)
        self.assertTrue(2 <= source.PAGE_ATTEMPTS <= 8)

    def test_registered_draft_is_stopped_and_gate_keeps_approvals_false(self):
        self.assertIn(national_transit_map_routes, hifld_ingest_assets)
        self.assertEqual(national_transit_map_routes_draft_schedule.default_status.value, "STOPPED")
        check = national_transit_map_routes_complete.node_def.compute_fn.decorated_fn
        incomplete = check({"source_population_complete": True, "feature_count": 2,
                            "source_null_geometry_count": 0, "source_invalid_geometry_count": 0})
        self.assertFalse(incomplete.passed)
        complete = check({"source_population_complete": True, "feature_count": DATED_LIVE_COUNT,
                          "source_null_geometry_count": 0, "source_invalid_geometry_count": 0})
        self.assertTrue(complete.passed)
        for metadata in (incomplete.metadata, complete.metadata):
            self.assertFalse(metadata["rights_accepted"].value)
            self.assertFalse(metadata["release_accepted"].value)


if __name__ == "__main__":
    unittest.main()

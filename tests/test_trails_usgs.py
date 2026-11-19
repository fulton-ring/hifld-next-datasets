import copy
import gzip
import io
import json
import math
import pathlib
import struct
import tempfile
from datetime import datetime, timezone
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

import httpx
import pyarrow.parquet as pq
import gzip

_REAL_HTTPX_CLIENT = httpx.Client

from dagster_hifld.sources import trails_usgs

_REAL_HTTPX_CLIENT = httpx.Client
from dagster_hifld.sources.trails_usgs import (
    BATCH_SIZE,
    EXPECTED_FIELD_NAMES,
    LAYER_URL,
    _capture_into,
    _fields,
    _layer_contract_unchanged,
    _load,
    _parquet,
    _parquet_pages,
    _validate_page_metadata,
    _validate_polyline_geometry,
    _validate_dimensions_probe,
    _validate_request_scope,
    _witness_refused,
    _wkb_polyline,
)


def _layer_metadata():
    types = {"objectid": "esriFieldTypeOID", "publisheddate": "esriFieldTypeDate",
             "sourceeditdate": "esriFieldTypeDate", "loaddate": "esriFieldTypeDate",
             "lengthmiles": "esriFieldTypeDouble", "networklength": "esriFieldTypeDouble",
             "globalid": "esriFieldTypeGlobalID"}
    return {
        "id": 37, "name": "Trails", "geometryType": "esriGeometryPolyline",
        "hasM": True,
        "geometryField": {"name": "shape"},
        "extent": {"spatialReference": {"wkid": 102100, "latestWkid": 3857}},
        "fields": ([{"name": name, "type": types.get(name, "esriFieldTypeString")}
                    for name in EXPECTED_FIELD_NAMES]
                   + [{"name": "shape", "type": "esriFieldTypeGeometry"},
                      {"name": "shape_Length", "type": "esriFieldTypeDouble"}]),
    }


def _feature(row_number, path):
    fields = _fields(_layer_metadata())
    attrs = {}
    for field in fields:
        name, kind = field["name"], field["type"]
        if kind in {"esriFieldTypeOID", "esriFieldTypeInteger", "esriFieldTypeSmallInteger"}:
            value = row_number + 1
        elif kind == "esriFieldTypeDate":
            value = row_number * 1000
        elif kind in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
            value = float(row_number) + 0.25
        else:
            value = f"{name}-{row_number}"
        attrs[name] = value
    return {"attributes": attrs, "geometry": {"hasM": True, "paths": path}}


def _captures(ids):
    docs = {"service_before": {}, "service_after": {}, "layer_before": _layer_metadata(),
            "layer_after": _layer_metadata(), "count_before": {"count": len(ids)},
            "count_after": {"count": len(ids)}, "ids_before": {"objectIds": ids}, "ids_after": {"objectIds": ids}}
    urls = {}
    for name in docs:
        if name.startswith("service_"):
            urls[name] = trails_usgs.SERVICE_URL + "?f=pjson"
        elif name.startswith("layer_"):
            urls[name] = LAYER_URL + "?f=pjson"
        else:
            urls[name] = trails_usgs._query({"where": "1=1",
                "returnCountOnly" if name.startswith("count_") else "returnIdsOnly": "true", "f": "json"})
    metadata = {"geometryType": "esriGeometryPolyline", "spatialReference": {"wkid": 3857}}
    path = [[[1.0, 2.0, 7.25], [3.0, 4.0, None]]]
    docs["dimensions_probe"] = {**metadata, "features": [_feature(ids[0] - 1, path)]}
    urls["dimensions_probe"] = trails_usgs._query({"objectIds": str(ids[0]), "outFields": "OBJECTID",
        "returnGeometry": "true", "returnZ": "true", "returnM": "true", "f": "json"})
    for start in range(0, len(ids), trails_usgs.BATCH_SIZE):
        name = f"page_{start:05d}"
        docs[name] = {**metadata, "features": [_feature(oid - 1, path) for oid in ids[start:start + trails_usgs.BATCH_SIZE]]}
        urls[name] = trails_usgs._query({"where": "1=1", "outFields": "*", "returnGeometry": "true",
            "returnZ": "true", "returnM": "true", "orderByFields": "objectid ASC", "resultOffset": start,
            "resultRecordCount": trails_usgs.BATCH_SIZE, "f": "json"})
    result = {}
    for name, doc in docs.items():
        body = json.dumps(doc).encode()
        result[name] = (body, {"method": "GET", "url": urls[name], "final_url": urls[name], "status": 200,
            "complete_body": True, "observed_at": "2026-11-19T00:00:00Z", "bytes": len(body),
            "sha256": trails_usgs._sha(body), "wire_bytes": len(body), "wire_sha256": trails_usgs._sha(body)})
    return result


def _client_factory(captures, encoding="identity", refuse_first_page=False):
    bodies = {receipt["url"]: body for body, receipt in captures.values()}
    refused = []

    def handler(request):
        if refuse_first_page and "resultOffset=0" in str(request.url) and not refused:
            refused.append(str(request.url))
            return httpx.Response(503, stream=httpx.ByteStream(b"unavailable"))
        body = bodies[str(request.url)]
        if encoding == "gzip":
            body = gzip.compress(body)
        return httpx.Response(200, headers={"Content-Encoding": encoding, "Content-Length": str(len(body))},
                              stream=httpx.ByteStream(body))

    return lambda *args, **kwargs: _REAL_HTTPX_CLIENT(transport=httpx.MockTransport(handler))


def _client_factory(captures, encoding="identity", refuse_first_page=False):
    bodies = {receipt["url"]: body for body, receipt in captures.values()}
    refused = False

    def handler(request):
        nonlocal refused
        if refuse_first_page and not refused and "resultOffset=0" in str(request.url):
            refused = True
            return httpx.Response(503, stream=httpx.ByteStream(b"temporarily unavailable"))
        body = bodies[str(request.url)]
        if encoding == "gzip":
            body = gzip.compress(body)
        return httpx.Response(200, headers={"Content-Encoding": encoding, "Content-Length": str(len(body))},
                              stream=httpx.ByteStream(body))

    return lambda *args, **kwargs: _REAL_HTTPX_CLIENT(transport=httpx.MockTransport(handler))


class TrailsUSGSTests(unittest.TestCase):
    def test_changed_population_matches_capture_time_witnesses(self):
        with patch.object(trails_usgs, "BATCH_SIZE", 2):
            _, audit = trails_usgs._population(_captures([1, 3, 9]))
        self.assertEqual(audit["feature_count"], 3)

    def test_changed_publications_seal_new_versions_without_rewriting_prior_objects(self):
        from dagster_hifld.resources import StagingStorageResource

        with tempfile.TemporaryDirectory() as tmp, patch.object(trails_usgs, "BATCH_SIZE", 2):
            storage = StagingStorageResource(use_local=True, local_dir=tmp + "/stage", prefix="")
            originals, versions = {}, []
            for ids in ([2, 8], [1, 3, 9, 12, 20]):
                with patch.object(trails_usgs.httpx, "Client", side_effect=_client_factory(_captures(ids))):
                    result = trails_usgs.capture_trails(storage, version="dynamic", capture_directory=tmp + "/captures")
                self.assertEqual(result["feature_count"], len(ids))
                self.assertNotIn(result["version"], versions)
                versions.append(result["version"])
                for key, snapshot in originals.items():
                    self.assertEqual((storage.read_key(key), storage.object_snapshot(key)), snapshot)
                originals = {key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}

    def test_baseline_warn_is_not_completeness_or_approval(self):
        from dagster import AssetCheckSeverity
        from dagster_hifld.capture_completeness import approved_baseline_drift
        from dagster_hifld.assets.trails_usgs import trails_population_complete, trails_baseline_drift

        candidate = {"dataset_slug": "trails-2", "file_slug": "trails-2", "feature_count": 3,
                     "source_id_sha256": "new", "source_population_complete": True}
        self.assertTrue(trails_population_complete(candidate).passed)
        self.assertFalse(trails_population_complete({**candidate, "source_population_complete": False}).passed)
        skipped = trails_baseline_drift(candidate)
        self.assertEqual(skipped.severity, AssetCheckSeverity.WARN)
        self.assertEqual(skipped.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        reference = {**candidate, "version": "approved-fixture", "release_accepted": True,
                     "feature_count": 2, "source_id_sha256": "old"}
        compared = approved_baseline_drift(candidate, reference)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.metadata["feature_count_delta"].value, 1)

    def test_population_brackets_and_dynamic_pages_fail_closed(self):
        with patch.object(trails_usgs, "BATCH_SIZE", 2):
            for target, change, error in (
                ("count_after", {"count": 4}, "source_count_or_id_roster_drift"),
                ("ids_after", {"objectIds": [1, 3, 10]}, "source_count_or_id_roster_drift"),
                ("service_after", {"changed": True}, "trails_service_changed_during_capture"),
                ("layer_after", {"hasM": False}, "trails_layer_changed_during_capture"),
                ("page_00002", {"features": []}, "trails_page_population_incomplete"),
            ):
                with self.subTest(target=target):
                    captures = _captures([1, 3, 9])
                    body = json.loads(captures[target][0])
                    body.update(change)
                    captures[target] = (json.dumps(body).encode(), captures[target][1])
                    with self.assertRaisesRegex(ValueError, error):
                        trails_usgs._population(captures)

    def test_real_sealing_replay_and_refresh_preserve_m_and_refused_evidence(self):
        from dagster_hifld.resources import StagingStorageResource

        with tempfile.TemporaryDirectory() as tmp, patch.object(trails_usgs, "BATCH_SIZE", 2):
            storage = StagingStorageResource(use_local=True, local_dir=tmp + "/stage", prefix="")
            captures = _captures([1, 3, 9])
            with patch.object(trails_usgs.httpx, "Client", side_effect=_client_factory(captures, refuse_first_page=True)):
                first = trails_usgs.capture_trails(storage, version="first", capture_directory=tmp + "/captures")
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], 3)
            root = pathlib.Path(first["capture_directory"])
            originals = {key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}
            self.assertEqual(sum("/evidence/refused/" in key for key in originals), 3)
            parquet_key = first["candidate_ready"].replace("metadata/candidate_ready.json", "geoparquet/trails-2.parquet")
            table = pq.read_table(io.BytesIO(storage.read_key(parquet_key)))
            self.assertEqual(table.column("objectid").to_pylist(), [1, 3, 9])
            self.assertEqual(table.column("geometry")[0].as_py(), _wkb_polyline([[[1.0, 2.0, 7.25], [3.0, 4.0, None]]]))
            manifest_bytes = (root / "capture_manifest.json").read_bytes()
            with patch.object(trails_usgs.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                replay = trails_usgs.capture_trails(storage, version="replay", capture_directory=str(root))
            self.assertTrue(replay["reused"])
            with patch.object(trails_usgs.httpx, "Client", side_effect=_client_factory(captures)):
                refresh = trails_usgs.capture_trails(storage, version="refresh", capture_directory=tmp + "/captures")
            self.assertTrue(refresh["reused"])
            self.assertEqual(refresh["version"], first["version"])
            self.assertEqual((root / "capture_manifest.json").read_bytes(), manifest_bytes)
            self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}, originals)
            storage.write_key(parquet_key, b"corrupt parquet")
            tampered = {key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}
            with patch.object(trails_usgs.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                with self.assertRaisesRegex(ValueError, "cached_candidate_object_integrity_failed"):
                    trails_usgs.capture_trails(storage, version="corrupt", capture_directory=str(root))
            self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}, tampered)

    def test_forged_decoded_identity_and_gzip_pages_fail_without_mutation(self):
        from dagster_hifld.resources import StagingStorageResource

        for encoding in ("identity", "gzip"):
            with self.subTest(encoding=encoding), tempfile.TemporaryDirectory() as tmp, patch.object(trails_usgs, "BATCH_SIZE", 2):
                storage = StagingStorageResource(use_local=True, local_dir=tmp + "/stage", prefix="")
                with patch.object(trails_usgs.httpx, "Client", side_effect=_client_factory(_captures([1, 3, 9]), encoding)):
                    first = trails_usgs.capture_trails(storage, version="wire", capture_directory=tmp + "/captures")
                root = pathlib.Path(first["capture_directory"])
                originals = {key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}
                with patch.object(trails_usgs.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                    self.assertTrue(trails_usgs.capture_trails(storage, version="honest", capture_directory=str(root))["reused"])
                body = (root / "page_00000.body").read_bytes().replace(b"name-0", b"name-X")
                (root / "page_00000.body").write_bytes(body)
                receipt = json.loads((root / "page_00000.receipt.json").read_bytes())
                receipt.update(bytes=len(body), sha256=trails_usgs._sha(body))
                (root / "page_00000.receipt.json").write_bytes(json.dumps(receipt).encode())
                manifest = json.loads((root / "capture_manifest.json").read_bytes())
                manifest["receipts"]["page_00000"] = receipt
                manifest.update(trails_usgs._artifact_rosters(root))
                (root / "capture_manifest.json").write_bytes(json.dumps(manifest).encode())
                with patch.object(trails_usgs.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                    with self.assertRaisesRegex(ValueError, "retained_capture_wire_body_mismatch"):
                        trails_usgs.capture_trails(storage, version="forged", capture_directory=str(root))
                self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key)) for key in storage.list_prefix("")}, originals)

    def test_trails_layer_keeps_all_native_attributes_and_excludes_geometry_members(self):
        fields = _fields(_layer_metadata())
        self.assertEqual([field["name"] for field in fields], EXPECTED_FIELD_NAMES + ["shape_Length"])
        self.assertEqual(fields[13]["name"], "objectid")
        self.assertEqual(fields[13]["type"], "esriFieldTypeOID")
        self.assertEqual(fields[-2]["name"], "globalid")
        self.assertEqual(fields[-1]["name"], "shape_Length")
        self.assertEqual(fields[-1]["type"], "esriFieldTypeDouble")

    def test_trails_layer_schema_drift_fails_closed(self):
        layer = copy.deepcopy(_layer_metadata())
        layer["fields"][2]["name"] = "trailsurface_1"
        with self.assertRaisesRegex(ValueError, "trails_layer_schema_drift"):
            _fields(layer)

    def test_layer_geometry_type_or_crs_drift_fails_closed(self):
        layer = copy.deepcopy(_layer_metadata())
        layer["geometryType"] = "esriGeometryPoint"
        with self.assertRaisesRegex(ValueError, "trails_layer_schema_drift"):
            _fields(layer)
        layer = copy.deepcopy(_layer_metadata())
        layer["extent"]["spatialReference"] = {"wkid": 4269}
        with self.assertRaisesRegex(ValueError, "trails_spatial_reference_drift"):
            _fields(layer)

    def test_query_page_metadata_rejects_geometry_or_crs_drift(self):
        page = {"geometryType": "esriGeometryPolyline", "spatialReference": {"wkid": 102100, "latestWkid": 3857}}
        _validate_page_metadata(page, "page_00000")
        wrong_crs = copy.deepcopy(page)
        wrong_crs["spatialReference"] = {"wkid": 4326}
        with self.assertRaisesRegex(ValueError, "trails_spatial_reference_drift"):
            _validate_page_metadata(wrong_crs, "page_00000")
        wrong_geometry = copy.deepcopy(page)
        wrong_geometry["geometryType"] = "esriGeometryPolygon"
        with self.assertRaisesRegex(ValueError, "trails_page_geometry_type_drift"):
            _validate_page_metadata(wrong_geometry, "page_00000")

    def test_polyline_geometry_retains_served_m_and_fails_closed_on_drift(self):
        # The layer declares hasM: true (observed 2026-11-14), so served
        # [x, y, m] vertices are within the producer's declared schema: null m
        # and finite populated m are both retained; z and non-finite fail closed.
        _validate_polyline_geometry({"hasM": True, "paths": [[[1.5, -2.5, None], [3.0, 4.0, None]]]})
        _validate_polyline_geometry({"hasM": True, "paths": [[[1.5, -2.5], [3.0, 4.0]]]})
        _validate_polyline_geometry({"hasM": True, "paths": [[[1.5, -2.5, None], [3.0, 4.0, 7.25]]]})
        _validate_polyline_geometry({"hasM": True, "paths": [[[1.5, -2.5, 1274473973.0000002]]]})
        for bad in (
            {"hasM": True, "paths": [[[1.5, -2.5], [float("nan"), 4.0]]]},
            {"hasM": True, "paths": [[[1.5, -2.5, float("nan")]]]},
            {"hasM": True, "paths": [[[1.5, -2.5, float("inf")]]]},
            {"hasM": True, "paths": [[[1.5, -2.5, "x"]]]},
            {"hasM": True, "paths": [[[1.5, -2.5], ["x", 4.0]]]},
            {"hasM": True, "paths": [[[1.5, -2.5]]], "z": 3},
            {"hasM": True, "paths": [[[1.5, -2.5]]], "m": 3},
            {"hasM": True, "paths": []},
            {"hasM": True, "paths": [[[1.5]]]},
            {"hasM": True, "paths": [[[1.5, -2.5, None, 3.0]]]},
        ):
            with self.subTest(bad=bad), self.assertRaisesRegex(
                    ValueError, "trails_(unhandled_z_or_m_ordinate|polyline_geometry_drift|nonfinite_geometry)"):
                _validate_polyline_geometry(bad)

    def test_polyline_geometry_rejects_populated_hasz(self):
        geometry = {"hasM": True, "hasZ": True, "paths": [[[1.5, -2.5, None]]]}
        with self.assertRaisesRegex(ValueError, "trails_unhandled_z_or_m_ordinate"):
            _validate_polyline_geometry(geometry)

    def test_layer_pins_declared_m_dimension(self):
        layer = copy.deepcopy(_layer_metadata())
        layer["hasM"] = False
        with self.assertRaisesRegex(ValueError, "trails_layer_schema_drift"):
            _fields(layer)
        layer = copy.deepcopy(_layer_metadata())
        layer.pop("hasM")
        with self.assertRaisesRegex(ValueError, "trails_layer_schema_drift"):
            _fields(layer)

    def test_layer_before_after_gate_excludes_presentation_metadata(self):
        # Observed 2026-11-14: the producer rotated drawingInfo.colorRamp
        # colors across a 34-minute live capture while every data-contract
        # member stayed identical; the gate must not fail on presentation.
        after = copy.deepcopy(_layer_metadata())
        after["drawingInfo"] = {"renderer": {"authoringInfo": {"colorRamp": {"fromColor": [1, 2, 3]}}}}
        _layer_contract_unchanged(_layer_metadata(), after)
        after["maxRecordCount"] = 1000
        with self.assertRaisesRegex(ValueError, "trails_layer_changed_during_capture"):
            _layer_contract_unchanged(_layer_metadata(), after)
        after = copy.deepcopy(_layer_metadata())
        after["fields"][3]["name"] = "icewatercraft_2"
        with self.assertRaisesRegex(ValueError, "trails_layer_changed_during_capture"):
            _layer_contract_unchanged(_layer_metadata(), after)
        after = copy.deepcopy(_layer_metadata())
        after["extent"]["spatialReference"] = {"wkid": 4269}
        with self.assertRaisesRegex(ValueError, "trails_layer_changed_during_capture"):
            _layer_contract_unchanged(_layer_metadata(), after)

    def test_dimensions_probe_pins_requested_oid_and_retains_m_values(self):
        probe = {"geometryType": "esriGeometryPolyline",
                 "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                 "features": [{"attributes": {"objectid": 1475127},
                               "geometry": {"hasM": True, "paths": [[[1.0, 2.0, None]]]}}]}
        _validate_dimensions_probe(probe, 1475127)
        probe["features"][0]["geometry"]["paths"][0][0][2] = 4.0
        _validate_dimensions_probe(probe, 1475127)

    def test_capture_receipt_scope_pins_native_id_roster_request(self):
        ids = [101, 102]
        params = {"where": "1=1", "returnIdsOnly": "true", "f": "json"}
        url = LAYER_URL + "/query?" + urlencode(params)
        _validate_request_scope("ids_before", {"url": url, "final_url": url}, ids)
        wrong = LAYER_URL + "/query?" + urlencode({**params, "outFields": "*"})
        with self.assertRaisesRegex(ValueError, "trails_capture_request_scope_drift"):
            _validate_request_scope("ids_before", {"url": wrong, "final_url": wrong}, ids)

    def test_page_request_scope_explicitly_requests_z_and_m(self):
        ids = [101, 102]
        params = {"where": "1=1", "outFields": "*", "returnGeometry": "true",
                  "returnZ": "true", "returnM": "true", "orderByFields": "objectid ASC",
                  "resultOffset": "0", "resultRecordCount": str(BATCH_SIZE), "f": "json"}
        url = LAYER_URL + "/query?" + urlencode(params)
        _validate_request_scope("page_00000", {"url": url, "final_url": url}, ids)
        params.pop("returnM")
        url_without_m = LAYER_URL + "/query?" + urlencode(params)
        with self.assertRaisesRegex(ValueError, "trails_capture_request_scope_drift"):
            _validate_request_scope("page_00000", {"url": url_without_m, "final_url": url_without_m}, ids)
        # A 2,000-id objectIds page URI was answered HTTP 414 by the producer
        # (2026-11-14); the page contract must not reintroduce it.
        self.assertNotIn("objectIds", url)

    def test_batch_size_is_within_publisher_page_limit(self):
        self.assertEqual(BATCH_SIZE, 2000)

    def test_wkb_retains_populated_m_ordinates_verbatim(self):
        served = [[[1.5, -2.5, 7.25], [3.0, 4.0, 9.5]]]
        body = _wkb_polyline(served)
        self.assertEqual(body[:9], struct.pack("<BII", 1, 2005, 1))
        self.assertEqual(body[9:18], struct.pack("<BII", 1, 2002, 2))
        self.assertEqual(body[18:], struct.pack("<ddd", 1.5, -2.5, 7.25) + struct.pack("<ddd", 3.0, 4.0, 9.5))
        # All-null served m keeps the established 2D emission.
        plain = _wkb_polyline([[[1.5, -2.5, None], [3.0, 4.0, None]]])
        self.assertEqual(plain[:9], struct.pack("<BII", 1, 5, 1))
        self.assertEqual(plain[9:18], struct.pack("<BII", 1, 2, 2))
        self.assertEqual(plain[18:], struct.pack("<dddd", 1.5, -2.5, 3.0, 4.0))
        # A served null inside a populated-m feature encodes as NaN, the WKB
        # null-m convention, while populated values stay verbatim.
        mixed = _wkb_polyline([[[1.5, -2.5, None], [3.0, 4.0, 9.5]]])
        self.assertEqual(mixed[:9], struct.pack("<BII", 1, 2005, 1))
        x0, y0, m0 = struct.unpack("<ddd", mixed[18:42])
        x1, y1, m1 = struct.unpack("<ddd", mixed[42:66])
        self.assertEqual((x0, y0), (1.5, -2.5))
        self.assertTrue(math.isnan(m0))
        self.assertEqual((x1, y1, m1), (3.0, 4.0, 9.5))

    def test_sample_population_parquet_declares_served_m_dimension(self):
        fields = _fields(_layer_metadata())
        features = [
            _feature(0, [[[20.0, 20.0, 7.25], [30.0, 25.0, 9.5]]]),
            _feature(1, [[[0.0, 0.0, None], [10.0, 5.0, None]]]),
            _feature(2, [[[40.0, 40.0, None], [50.0, 45.0, 11.0]]]),
        ]
        body = _parquet(features, fields, len(features))
        table = pq.read_table(io.BytesIO(body))
        for row_number, feature in enumerate(features):
            self.assertEqual(table.column("geometry")[row_number].as_py(),
                             _wkb_polyline(feature["geometry"]["paths"]))
        metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(metadata["columns"]["geometry"]["geometry_types"],
                         ["MultiLineString", "MultiLineString M"])

    def test_parquet_pages_rekeys_served_case_insensitive_attribute_keys(self):
        # The producer serves the declared 'shape_Length' as 'shape_length';
        # the page builder must read values through the lowercased served key
        # while emitting the declared name in the schema (regression for the
        # KeyError first reached after the geometry contract amendment).
        import tempfile
        fields = _fields(_layer_metadata())
        feature = _feature(0, [[[1.5, -2.5, 7.25], [3.0, 4.0, None]]])
        served = {"attributes": {key.lower(): value for key, value in feature["attributes"].items()},
                  "geometry": feature["geometry"]}
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "page_00000.body").write_bytes(json.dumps({"features": [served]}).encode())
            body = _parquet_pages(root, fields, 1)
        table = pq.read_table(io.BytesIO(body))
        self.assertIn("shape_Length", table.schema.names)
        self.assertEqual(table.column("shape_Length")[0].as_py(),
                         float(feature["attributes"]["shape_Length"]))
        self.assertEqual(table.column("geometry")[0].as_py(), _wkb_polyline(feature["geometry"]["paths"]))
        metadata = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(metadata["columns"]["geometry"]["geometry_types"], ["MultiLineString M"])

    def test_sample_population_parquet_round_trips_attributes_and_native_multilinestring_wkb(self):
        fields = _fields(_layer_metadata())
        path_a = [[0.0, 0.0, None], [10.0, 5.0, None]]
        path_b = [[20.0, 20.0, None], [30.0, 25.0, None], [40.0, 40.0, None]]
        features = [_feature(row, [path_a] if row % 2 else [path_a, path_b]) for row in range(3)]
        body = _parquet(features, fields, len(features))
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.num_rows, 3)
        for row_number, feature in enumerate(features):
            for field in fields:
                name = field["name"]
                expected = feature["attributes"][name]
                if field["type"] == "esriFieldTypeDate":
                    expected = datetime.fromtimestamp(expected / 1000, tz=timezone.utc)
                elif field["type"] in {"esriFieldTypeDouble", "esriFieldTypeSingle"}:
                    expected = float(expected)
                self.assertEqual(table.column(name)[row_number].as_py(), expected)
            self.assertEqual(table.column("geometry")[row_number].as_py(),
                             _wkb_polyline(feature["geometry"]["paths"]))
        self.assertEqual(_wkb_polyline([path_a]), struct.pack("<BII", 1, 5, 1) + struct.pack(
            "<BII", 1, 2, 2) + struct.pack("<dddd", 0.0, 0.0, 10.0, 5.0))
        metadata = __import__("json").loads(table.schema.metadata[b"geo"])
        self.assertEqual(metadata["columns"]["geometry"]["geometry_types"], ["MultiLineString"])


def _transport_drop_handler(body, calls, mode="transport"):
    """First call emulates a producer refusal; the second serves the page.

    MockTransport responses must stream their content (httpx marks a bytes
    body as already consumed), so the served page is an iterator.
    """
    def handler(request):
        calls.append(1)
        if len(calls) == 1:
            if mode == "truncated":
                # A 200 stream that ends before its declared Content-Length:
                # capture_http_response retains the delivered bytes and flags
                # the result incomplete.
                def stream():
                    yield b"partial-page"
                return httpx.Response(200, content=stream(), headers={"Content-Length": "999999"})
            raise httpx.ConnectError("producer dropped the transport", request=request)
        return httpx.Response(200, content=iter([body]))
    return handler


class TrailsRetryReplayTests(unittest.TestCase):
    """Refused attempts stay retained but a later successful attempt replays."""

    def _replay_manifest(self, root, name, receipt):
        manifest = {"complete": True, "artifact_names": [name], "receipts": {name: receipt}}
        (root / "capture_manifest.json").write_bytes(json.dumps(manifest, sort_keys=True).encode() + b"\n")

    def test_dropped_transport_then_successful_retry_replays(self):
        # Observed on the live capture: the producer dropped the first page's
        # transport; the retry succeeded. The completed capture must remain
        # replayable (previously the root-level error evidence failed
        # trails_capture_roster_invalid in _load).
        body = b'{"features": []}'
        calls = []
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "capture"
            root.mkdir()
            with httpx.Client(transport=httpx.MockTransport(_transport_drop_handler(body, calls)),
                              follow_redirects=False) as client:
                body_out, receipt, refused = trails_usgs._capture(
                    client, LAYER_URL + "?f=pjson", root, "probe", 0)
            self.assertEqual(body_out, body)
            self.assertEqual(receipt["status"], 200)
            self.assertEqual(len(calls), 2)
            self.assertGreater(refused, 0)
            refused_dir = root / "refused"
            self.assertTrue(refused_dir.is_dir() and not refused_dir.is_symlink())
            names = sorted(p.name for p in refused_dir.iterdir())
            self.assertEqual(len(names), 3)
            for suffix in ("body", "wire", "receipt.json"):
                self.assertTrue(any(name.endswith("." + suffix) for name in names))
            for name in names:
                self.assertRegex(name, r"^probe\.error-[0-9]+\.(body|wire|receipt\.json)$")
            self._replay_manifest(root, "probe", receipt)
            self.assertEqual(_load(root), {"probe": (body, receipt)})

    def test_truncated_refusal_evidence_is_hash_pinned_and_budget_counted(self):
        body = b'{"page": 1}'
        calls = []
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "capture"
            root.mkdir()
            with httpx.Client(transport=httpx.MockTransport(
                    _transport_drop_handler(body, calls, mode="truncated")),
                    follow_redirects=False) as client:
                captures = {}
                added = _capture_into(captures, client, LAYER_URL + "?f=pjson", root, "probe", 0)
            self.assertEqual(len(calls), 2)
            retained_body, receipt = captures["probe"]
            self.assertEqual(added, len(retained_body) + receipt["wire_bytes"]
                             + sum(p.stat().st_size for p in (root / "refused").iterdir()))
            error_body = next((root / "refused").glob("probe.error-*.body")).read_bytes()
            error_wire = next((root / "refused").glob("probe.error-*.wire")).read_bytes()
            error_receipt = json.loads(next((root / "refused").glob("probe.error-*.receipt.json")).read_bytes())
            self.assertEqual(error_body, b"partial-page")
            self.assertEqual(error_receipt["bytes"], len(error_body))
            self.assertEqual(error_receipt["sha256"], trails_usgs._sha(error_body))
            self.assertEqual(error_receipt["wire_bytes"], len(error_wire))
            self.assertEqual(error_receipt["wire_sha256"], trails_usgs._sha(error_wire))
            self._replay_manifest(root, "probe", receipt)
            # Retained refused evidence is counted in the capture budget: with
            # the budget capped at the successful total the replay must fail
            # closed, and without the cap it must pass.
            with patch.object(trails_usgs, "MAX_CAPTURE_BYTES",
                              len(retained_body) + receipt["wire_bytes"]):
                with self.assertRaisesRegex(ValueError, "trails_capture_budget_exceeded"):
                    _load(root)
            self.assertEqual(_load(root), {"probe": (retained_body, receipt)})

    def _synthetic_capture(self, root):
        body = wire = b'{"features": []}'
        receipt = {"method": "GET", "url": LAYER_URL + "?f=pjson", "status": 200,
                   "bytes": len(body), "sha256": trails_usgs._sha(body),
                   "wire_bytes": len(wire), "wire_sha256": trails_usgs._sha(wire), "complete_body": True}
        (root / "probe.body").write_bytes(body)
        (root / "probe.wire").write_bytes(wire)
        (root / "probe.receipt.json").write_bytes(json.dumps(receipt).encode() + b"\n")
        self._replay_manifest(root, "probe", receipt)
        return receipt

    def _refused_receipt(self, body=b'{"x": 1}', wire=b"refused-wire", status=None):
        return {"method": "GET", "url": LAYER_URL + "?f=pjson", "status": status,
                "bytes": len(body), "sha256": trails_usgs._sha(body),
                "wire_bytes": len(wire), "wire_sha256": trails_usgs._sha(wire)}

    def test_witness_refused_moves_evidence_into_the_refused_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "probe.body").write_bytes(b"refused-body")
            (root / "probe.wire").write_bytes(b"refused-wire")
            (root / "probe.receipt.json").write_bytes(b"{}")
            moved = _witness_refused(root, "probe")
            self.assertEqual(moved, len(b"refused-body") + len(b"refused-wire") + len(b"{}"))
            self.assertEqual({p.name for p in root.iterdir()}, {"refused"})
            self.assertEqual(len(list((root / "refused").iterdir())), 3)

    def test_load_validates_synthetic_refused_evidence(self):
        captured_body = b'{"features": []}'
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            receipt = self._synthetic_capture(root)
            refused = root / "refused"
            refused.mkdir()
            body, wire = b'{"x": 1}', b"refused-wire"
            (refused / "probe.error-42.body").write_bytes(body)
            (refused / "probe.error-42.wire").write_bytes(wire)
            (refused / "probe.error-42.receipt.json").write_bytes(
                json.dumps(self._refused_receipt(body, wire)).encode() + b"\n")
            self.assertEqual(_load(root), {"probe": (captured_body, receipt)})
            # A second refused stamp for the same artifact stays valid.
            (refused / "probe.error-43.body").write_bytes(b"")
            (refused / "probe.error-43.wire").write_bytes(b"")
            (refused / "probe.error-43.receipt.json").write_bytes(
                json.dumps(self._refused_receipt(b"", b"")).encode() + b"\n")
            self.assertEqual(_load(root), {"probe": (captured_body, receipt)})

    def test_load_rejects_invalid_refused_evidence(self):
        cases = {
            "unknown_name": lambda refused: [self._write_refused(refused, "stranger", b'{"x": 1}')],
            "incomplete_triple": lambda refused: [self._write_refused(refused, "probe", b'{"x": 1}',
                                                                     include=("body", "receipt.json"))],
            "hash_mismatch": lambda refused: [self._write_refused(refused, "probe", b'{"x": 1}',
                                                                 corrupt_body=True)],
        }
        for label, build in cases.items():
            with self.subTest(case=label), tempfile.TemporaryDirectory() as tmp:
                root = pathlib.Path(tmp)
                self._synthetic_capture(root)
                (root / "refused").mkdir()
                build(root / "refused")
                with self.assertRaisesRegex(ValueError, "trails_refused_evidence_invalid"):
                    _load(root)

    def _write_refused(self, refused, name, body, *, stamp=42, include=("body", "wire", "receipt.json"),
                       corrupt_body=False):
        wire = b"refused-wire"
        if "body" in include:
            (refused / f"{name}.error-{stamp}.body").write_bytes(body + (b"corrupted" if corrupt_body else b""))
        if "wire" in include:
            (refused / f"{name}.error-{stamp}.wire").write_bytes(wire)
        if "receipt.json" in include:
            (refused / f"{name}.error-{stamp}.receipt.json").write_bytes(
                json.dumps(self._refused_receipt(body, wire)).encode() + b"\n")

    def test_load_rejects_malformed_symlinked_or_empty_refused_locations(self):
        with self.subTest(case="empty_refused_dir"), tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self._synthetic_capture(root)
            (root / "refused").mkdir()
            with self.assertRaisesRegex(ValueError, "trails_capture_roster_invalid"):
                _load(root)
        with self.subTest(case="symlinked_refused_dir"), tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self._synthetic_capture(root)
            other = pathlib.Path(tmp) / "elsewhere"
            other.mkdir()
            (root / "refused").symlink_to(other, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "trails_capture_roster_invalid"):
                _load(root)
        with self.subTest(case="symlink_inside_refused"), tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "capture"
            root.mkdir()
            self._synthetic_capture(root)
            refused = root / "refused"
            refused.mkdir()
            outside = pathlib.Path(tmp) / "elsewhere"
            outside.mkdir()
            target = outside / "target.body"
            target.write_bytes(b'{"x": 1}')
            self._write_refused(refused, "probe", b'{"x": 1}')
            (refused / "probe.error-99.body").symlink_to(target)
            with self.assertRaisesRegex(ValueError, "trails_refused_evidence_invalid"):
                _load(root)
        with self.subTest(case="root_level_error_file"), tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            self._synthetic_capture(root)
            (root / "probe.error-1.body").write_bytes(b"stray")
            with self.assertRaisesRegex(ValueError, "trails_capture_roster_invalid"):
                _load(root)


if __name__ == "__main__":
    unittest.main()

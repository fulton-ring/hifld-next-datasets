import hashlib
import io
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.tmas_stations import (
    tmas_stations, tmas_stations_complete, tmas_stations_draft_schedule,
)
from dagster_hifld.sources import tmas_stations as source
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import _canonical

_REAL_HTTPX_CLIENT = __import__("httpx").Client

# The live layer holds 8060 contiguous OBJECTIDs; tests exercise the identical
# logic against a small patched roster for speed. Pins that bind captured
# content (record/geometry digests, bounds, state and functional-class value
# sets, Station_Id duplicate counts) are derived from the fixture in setUp so
# the base documents validate cleanly and every drift case isolates its own
# check.
PATCH_IDS = list(range(1, 7))


def _feature(oid: int) -> dict:
    attrs = {"OBJECTID": oid, "Station_Id": f"S{oid:03d}" if oid != 3 else "S001",
             "functional_class": "1U" if oid % 2 else "3R",
             "state": "DC" if oid == 5 else "VA",
             "latitude": 36.0 + oid / 1000, "longitude": -104.0 - oid / 1000}
    return {"attributes": attrs,
            "geometry": {"x": attrs["longitude"], "y": attrs["latitude"]}}


def _fixture_pins(mutate=None):
    """Digest/roster pins computed exactly the way the module recomputes them."""
    ids = list(range(1, 7))
    features = sorted((_feature(oid) for oid in ids), key=lambda f: f["attributes"]["OBJECTID"])
    if mutate is not None:
        mutate(features)
    record_sha = source._list_sha(features)
    geometry_records = [[f["attributes"]["OBJECTID"], f["geometry"]["x"], f["geometry"]["y"]]
                        for f in features]
    geometry_sha = source._list_sha(geometry_records)
    xs = [f["geometry"]["x"] for f in features]
    ys = [f["geometry"]["y"] for f in features]
    station_ids = [f["attributes"]["Station_Id"] for f in features]
    uniques = len({value for value in station_ids if value is not None})
    return {
        "EXPECTED_COUNT": 6, "PAGE_START_RECORDS": 2,
        "EXPECTED_ID_SHA256": hashlib.sha256(_canonical(ids)).hexdigest(),
        "EXPECTED_RECORD_SHA256": record_sha,
        "EXPECTED_GEOMETRY_SHA256": geometry_sha,
        "EXPECTED_OBSERVED_BOUNDS": {"xmin": min(xs), "ymin": min(ys),
                                     "xmax": max(xs), "ymax": max(ys)},
        "EXPECTED_STATES": ["DC", "VA"],
        "EXPECTED_FUNCTIONAL_CLASSES": {"1U", "3R"},
        "EXPECTED_STATION_ID_NULL_COUNT": 0,
        "EXPECTED_STATION_ID_UNIQUE_COUNT": uniques,
        "EXPECTED_STATION_ID_DUPLICATE_COUNT": len(station_ids) - uniques,
    }


def _base_documents():
    service = {"serviceItemId": source.ITEM_ID, "serviceName": "NTAD_Travel_Monitoring_Analysis_System_Stations",
               "spatialReference": {"wkid": 4326, "latestWkid": 4326},
               "layers": [{"id": layer_id, "name": name, "type": layer_type}
                          for layer_id, name, layer_type in source.EXPECTED_SERVICE_LAYER_ROSTER]}
    layer = {"id": source.LAYER_ID, "name": source.LAYER_NAME, "type": "Feature Layer",
             "geometryType": "esriGeometryPoint", "objectIdField": "OBJECTID",
             "hasZ": False, "hasM": False, "maxRecordCount": 2000,
             "spatialReference": {"wkid": 4326, "latestWkid": 4326},
             "extent": {**source.EXPECTED_LAYER_EXTENT,
                        "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
             "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
             "editingInfo": {"lastEditDate": 1776462237370},
             "fields": [
                 {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID",
                  "nullable": False, "domain": None},
                 {"name": "Station_Id", "type": "esriFieldTypeString", "alias": "station_id",
                  "length": 8000, "nullable": True, "domain": None},
                 {"name": "functional_class", "type": "esriFieldTypeString", "alias": "functional_class",
                  "length": 8000, "nullable": True, "domain": None},
                 {"name": "state", "type": "esriFieldTypeString", "alias": "state",
                  "length": 8000, "nullable": True, "domain": None},
                 {"name": "latitude", "type": "esriFieldTypeDouble", "alias": "latitude",
                  "nullable": True, "domain": None},
                 {"name": "longitude", "type": "esriFieldTypeDouble", "alias": "longitude",
                  "nullable": True, "domain": None},
             ]}
    portal = {"id": source.ITEM_ID, "owner": "USDOT_BTS", "orgId": source.ITEM_ORG_ID,
              "title": source.LAYER_NAME, "access": "public", "type": "Feature Service",
              "created": 1720552268000, "modified": 1776462240000,
              "description": ("...compiled on December 31, 2025 and was published on March 23, 2026 "
                              "from the Federal Highway Administration (FHWA), and is part of the "
                              "U.S. Department of Transportation (USDOT)/Bureau of Transportation "
                              "Statistics (BTS) National Transportation Atlas Database (NTAD). "
                              "Geospatial station data from the FHWA Travel Monitoring Analysis "
                              "System (TMAS) database...")}
    docs = {"ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": list(PATCH_IDS)},
            "portal_item": portal, "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": list(PATCH_IDS)},
            "service_after": json.loads(json.dumps(service)),
            "layer_after": json.loads(json.dumps(layer)),
            "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": list(PATCH_IDS)}}
    for offset in range(0, source.EXPECTED_COUNT, source.PAGE_START_RECORDS):
        count = min(source.PAGE_START_RECORDS, source.EXPECTED_COUNT - offset)
        rows = [_feature(oid) for oid in PATCH_IDS[offset:offset + count]]
        docs[source._page_name(offset, count)] = {
            "spatialReference": {"wkid": 4326, "latestWkid": 4326}, "features": rows}
    return docs


def _docs_with_state_drift():
    docs = json.loads(json.dumps(_base_documents()))
    docs["page_000000_00002"]["features"][0]["attributes"]["state"] = "XX"
    return docs


def _BASE_DOCS_LOADER():
    return _base_documents()


def _synthetic_client_factory(docs):
    """Serve the fixture documents as raw-order Esri JSON responses."""
    urls = {"ids_seed": source._query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
            **{name: url for name, url in source._doc_urls()},
            **{name: url for name, url in source._after_doc_urls()}}
    for offset in range(0, source.EXPECTED_COUNT, source.PAGE_START_RECORDS):
        count = min(source.PAGE_START_RECORDS, source.EXPECTED_COUNT - offset)
        urls[source._page_name(offset, count)] = source._page_query(offset, count)
    bodies = {url: json.dumps(docs[name], separators=(",", ":"), ensure_ascii=False).encode()
              for name, url in urls.items()}
    import httpx

    class _ByteStream(httpx.SyncByteStream):
        def __init__(self, chunks):
            self._chunks = chunks

        def __iter__(self):
            return iter(self._chunks)

    def handler(request):
        key = str(request.url)
        if key not in bodies:
            return httpx.Response(404, content=b'{"error":{"code":404}}')
        body = bodies[key]
        return httpx.Response(200, stream=_ByteStream([body]),
                              headers={"Content-Type": "application/json",
                                       "Content-Length": str(len(body))})

    def factory(*args, **kwargs):
        return _REAL_HTTPX_CLIENT(transport=httpx.MockTransport(handler))

    return factory


class TmasStationsTests(unittest.TestCase):
    def setUp(self):
        self._patches = [patch.object(source, name, value) for name, value in _fixture_pins().items()]
        for item in self._patches:
            item.start()
            self.addCleanup(item.stop)

    def _documents(self, *, mutate=None):
        docs = _base_documents()
        if mutate is not None:
            mutate(docs)
        return docs

    def _captures(self, docs):
        # Esri responses preserve field order; capture bodies are stored raw
        # (insertion order), so test bodies must not be key-sorted.
        return {name: (json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode(),
                       {"status": 200}) for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(tmas_stations, hifld_ingest_assets)
        self.assertEqual(tmas_stations_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = tmas_stations_complete({"feature_count": 8060, "source_null_geometry_count": 0,
                                        "source_invalid_geometry_count": 0})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = tmas_stations_complete({"feature_count": 8059, "source_null_geometry_count": 0,
                                         "source_invalid_geometry_count": 0})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_tile_the_roster(self):
        with patch.object(source, "EXPECTED_COUNT", 7), patch.object(source, "PAGE_START_RECORDS", 3):
            names = source._capture_names()
        self.assertEqual(sorted(name for name in names if name.startswith("page_")),
                         ["page_000000_00003", "page_000003_00003", "page_000006_00001"])
        query = source._page_query(0, 3)
        self.assertIn("resultOffset=0", query)
        self.assertIn("resultRecordCount=3", query)
        self.assertIn("orderByFields=OBJECTID+ASC", query)
        self.assertIn("outSR=4326", query)

    def test_complete_native_population_and_geometry_are_validated(self):
        layer, fields_value, extracted = source._validate_captures(self._captures(self._documents()))
        audit = extracted["audit"]
        self.assertEqual([f["name"] for f in fields_value], source.EXPECTED_FIELD_NAMES)
        self.assertEqual(len(extracted["features"]), 6)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_geometry_type"], "esriGeometryPoint")
        self.assertEqual(audit["null_geometry_count"], 0)
        self.assertEqual(audit["geometry_attribute_mismatch_count"], 0)
        self.assertEqual(audit["unique_station_id_count"], 5)
        self.assertEqual(audit["station_id_duplicate_count"], 1)
        self.assertEqual(audit["observed_state_count"], 2)
        self.assertEqual(audit["source_null_counts"]["latitude"], 0)

    def test_parquet_preserves_native_attributes_and_point_geometry(self):
        layer, fields_value, extracted = source._validate_captures(self._captures(self._documents()))
        data = source._parquet_bytes(fields_value, extracted["features"])
        table = pq.read_table(io.BytesIO(data))
        self.assertEqual(table.num_rows, 6)
        self.assertEqual(table.schema.field("Station_Id").type.__str__(), "string")
        self.assertEqual(table.schema.field("latitude").type.__str__(), "double")
        self.assertEqual(table.schema.field("OBJECTID").type.__str__(), "int64")
        self.assertEqual(table.column("OBJECTID").to_pylist(), PATCH_IDS)
        first = struct.unpack("<BI", table.column("geometry")[0].as_py()[:5])
        self.assertEqual(first[1], 1)  # Point WKB header
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 4326})

    def test_population_schema_transfer_and_value_drift_fail_closed(self):
        cases = [
            ("count", lambda d: d["count_after"].update(count=source.EXPECTED_COUNT - 1),
             "source_count_drift"),
            ("ids", lambda d: d["ids_after"].update(objectIds=PATCH_IDS[:-1]),
             "source_id_population_drift"),
            ("roster digest",
             lambda d: d["ids_before"].update(objectIds=PATCH_IDS[:-1] + [99]),
             "source_id_population_drift"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_schema_contract_drift"),
            ("field roster", lambda d: d["layer_before"]["fields"].pop(1),
             "source_schema_contract_drift"),
            ("extent", lambda d: d["layer_before"]["extent"].update(xmin=-1.0),
             "source_layer_extent_drift"),
            ("record limit", lambda d: d["layer_before"].update(maxRecordCount=1000),
             "source_record_limit_drift"),
            ("portal", lambda d: d["portal_item"].update(owner="someone_else"),
             "source_portal_item_identity_drift"),
            ("FHWA relation", lambda d: d["portal_item"].update(description="unrelated text"),
             "official_FHWA_NTAD_relationship_drift"),
            ("service item", lambda d: d["service_before"].update(serviceItemId="other"),
             "official_BTS_tmas_stations_service_identity_drift"),
            ("changed during capture", lambda d: d["layer_after"].update(name="other"),
             "source_layer_metadata_changed_during_capture"),
            ("page slice", lambda d: d["page_000000_00002"]["features"][0]["attributes"].update(
                OBJECTID=99), "source_page_id_slice_incomplete_or_reordered"),
            ("attribute order", lambda d: d["page_000000_00002"]["features"][0]["attributes"].update(
                {"extra": 1}), "source_record_attribute_schema_drift"),
            ("string type", lambda d: d["page_000000_00002"]["features"][0]["attributes"].update(
                state=5), "source_string_value_type_drift"),
            ("double type", lambda d: d["page_000000_00002"]["features"][0]["attributes"].update(
                latitude="36.5"), "source_double_value_type_drift"),
            ("state contract", lambda d: d["page_000000_00002"]["features"][0]["attributes"].update(
                state="XX"), "source_state_value_contract_drift"),
            ("functional class contract",
             lambda d: d["page_000000_00002"]["features"][0]["attributes"].update(functional_class="9Z"),
             "source_functional_class_value_contract_drift"),
            ("coordinate range", lambda d: d["page_000000_00002"]["features"][0]["geometry"].update(
                x=200.0), "source_coordinate_range_drift"),
            ("geometry structure", lambda d: d["page_000000_00002"]["features"][0].update(
                geometry={"x": -104.0}), "source_geometry_population_drift"),
            ("null population", lambda d: d["page_000000_00002"]["features"][0]["attributes"].update(
                latitude=None), "source_null_population_drift"),
            ("station id duplicates",
             lambda d: d["page_000000_00002"]["features"][1]["attributes"].update(Station_Id="S001"),
             "source_station_id_population_drift"),
            ("geometry attribute mismatch",
             lambda d: d["page_000000_00002"]["features"][0]["geometry"].update(x=-150.0),
             "source_geometry_population_drift"),
            ("record values", lambda d: d["page_000000_00002"]["features"][0]["attributes"].update(
                Station_Id="S002"), "source_record_value_drift"),
            ("coverage bounds", lambda d: (
                d["page_000000_00002"]["features"][0]["geometry"].update(y=1.0),
                d["page_000000_00002"]["features"][0]["attributes"].update(latitude=1.0)),
             "source_observed_coverage_drift"),
            ("page tiling", lambda d: d.pop("page_000004_00002"),
             "source_capture_roster_incomplete_or_unexpected"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate_captures(self._captures(docs))

    def test_geometry_attribute_tolerance_boundary(self):
        def move_geometry(features, delta):
            features[0]["geometry"]["x"] += delta
        with self.subTest(label="within tolerance accepted"):
            docs = json.loads(json.dumps(self._documents()))
            move_geometry(docs["page_000000_00002"]["features"], 5e-9)
            pins = _fixture_pins(mutate=lambda feats: move_geometry(feats, 5e-9))
            with patch.object(source, "EXPECTED_RECORD_SHA256", pins["EXPECTED_RECORD_SHA256"]), \
                    patch.object(source, "EXPECTED_GEOMETRY_SHA256", pins["EXPECTED_GEOMETRY_SHA256"]), \
                    patch.object(source, "EXPECTED_OBSERVED_BOUNDS", pins["EXPECTED_OBSERVED_BOUNDS"]):
                layer, fields_value, extracted = source._validate_captures(self._captures(docs))
            self.assertEqual(extracted["audit"]["geometry_attribute_mismatch_count"], 0)
        with self.subTest(label="beyond tolerance fails closed"):
            docs = json.loads(json.dumps(self._documents()))
            move_geometry(docs["page_000000_00002"]["features"], 1e-7)
            with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
                source._validate_captures(self._captures(docs))

    def _replay_directory(self, root, docs, mutate=None):
        documents = json.loads(json.dumps(docs))
        if mutate is not None:
            mutate(documents)
        root.mkdir(parents=True)
        urls = {"ids_seed": source._query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                **{name: url for name, url in source._doc_urls()},
                **{name: url for name, url in source._after_doc_urls()}}
        for offset in range(0, source.EXPECTED_COUNT, source.PAGE_START_RECORDS):
            count = min(source.PAGE_START_RECORDS, source.EXPECTED_COUNT - offset)
            name = source._page_name(offset, count)
            urls[name] = source._page_query(offset, count)
        for name in sorted(source._capture_names()):
            body = json.dumps(documents[name], separators=(",", ":"), ensure_ascii=False).encode()
            receipt = {"method": "GET", "url": urls[name], "final_url": urls[name],
                       "observed_at": "2026-11-14T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
                       "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                       "content_encoding": "identity", "content_length": len(body),
                       "error_code": None, "error_type": None}
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        return root

    def test_valid_replay_seals_ready_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            root = self._replay_directory(Path(tmp) / "capture", self._documents())
            factory = _synthetic_client_factory(self._documents())
            with patch.object(source.httpx, "Client", side_effect=factory):
                first = source.capture_tmas_stations(storage, version="regression",
                                                     capture_directory=str(root))
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], 6)
            self.assertFalse(first["release_accepted"])
            ready = json.loads(storage.read_key(first["candidate_ready"]))
            self.assertEqual(ready["version"], first["version"])
            self.assertFalse(ready["approved"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            staged = set(storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet", staged)
            self.assertIn(f"{prefix}/raw/page_000000_00002.body", staged)
            self.assertIn(f"{prefix}/metadata/candidate_ready.json", staged)
            audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
            self.assertEqual(audit["null_geometry_count"], 0)
            self.assertEqual(audit["geometry_attribute_mismatch_count"], 0)
            with patch.object(source.httpx, "Client", side_effect=factory):
                second = source.capture_tmas_stations(storage, version="regression",
                                                      capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])

    def test_drifted_replay_seals_no_ready_candidate(self):
        def mutate(docs):
            docs["page_000000_00002"]["features"][0]["attributes"]["state"] = "XX"
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            root = self._replay_directory(Path(tmp) / "capture", self._documents(), mutate)
            factory = _synthetic_client_factory(_docs_with_state_drift())
            with patch.object(source.httpx, "Client", side_effect=factory):
                with self.assertRaisesRegex(ValueError, "source_state_value_contract_drift"):
                    source.capture_tmas_stations(storage, version="regression", capture_directory=str(root))
            self.assertEqual(storage.list_prefix(source.DATASET_SLUG), [])

    def test_nonlocal_or_published_storage_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
            source.capture_tmas_stations(
                StagingStorageResource(bucket="remote", use_local=False, local_dir="/tmp/x"),
                version="v1")

    def test_invalid_version_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "invalid_version"):
                source.capture_tmas_stations(storage, version="bad/version")


class TmasStationsRetryTests(unittest.TestCase):
    def test_transient_incomplete_capture_is_retried_with_attempt_evidence(self):
        calls = []
        sentinel = (b"body", {"status": 200})

        def flaky_capture_get(client, url, root, name, max_bytes):
            calls.append(name)
            if len(calls) == 1:
                (root / f"{name}.body").write_bytes(b"partial")
                (root / f"{name}.wire").write_bytes(b"partial")
                (root / f"{name}.receipt.json").write_bytes(b"{}")
                raise ValueError(f"incomplete_source_capture:{name}")
            self.assertFalse((root / f"{name}.body").exists())
            return sentinel

        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(source, "capture_get", flaky_capture_get):
            root = Path(tmp)
            result = source._capture_with_retry(None, "https://example.invalid", root, "layer_before",
                                                max_bytes=1024, attempts=3, base_delay=0.0)
            self.assertEqual(result, sentinel)
            self.assertEqual(calls, ["layer_before", "layer_before"])
            self.assertTrue((root / "layer_before.attempt-01.body").exists())

    def test_producer_error_document_is_retried_then_returned(self):
        calls = []

        def error_doc(client, url, root, name, max_bytes):
            calls.append(name)
            body = b'{"error":{"code":400,"message":"x"}}'
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            (root / f"{name}.receipt.json").write_bytes(b"{}")
            return body, {"bytes": len(body)}

        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(source, "capture_get", error_doc):
            body, _receipt = source._capture_with_retry(None, "https://example.invalid", Path(tmp),
                                                        "count_before", max_bytes=1024,
                                                        attempts=3, base_delay=0.0)
            self.assertEqual(len(calls), 3)
            self.assertTrue(body.startswith(b'{"error"'))

    def test_persistently_incomplete_capture_fails_after_bounded_retries(self):
        calls = []

        def failing_capture_get(client, url, root, name, max_bytes):
            calls.append(name)
            raise ValueError(f"incomplete_source_capture:{name}")

        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(source, "capture_get", failing_capture_get):
            with self.assertRaisesRegex(ValueError, "incomplete_source_capture:layer_before"):
                source._capture_with_retry(None, "https://example.invalid", Path(tmp), "layer_before",
                                           max_bytes=1024, attempts=3, base_delay=0.0)
            self.assertEqual(len(calls), 3)

    def test_capture_budget_is_enforced(self):
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(source, "MAX_CAPTURE_TOTAL_BYTES", 16):
            with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded_before_request"):
                source._capture_with_retry(None, "https://example.invalid", Path(tmp), "layer_before",
                                           max_bytes=1024, attempts=3, base_delay=0.0)


class TmasStationsLiveRosterTests(unittest.TestCase):
    def test_live_page_roster_covers_8060_ids_in_5_pages(self):
        self.assertEqual(source.EXPECTED_COUNT, 8060)
        names = source._capture_names()
        self.assertEqual(sorted(name for name in names if name.startswith("page_")),
                         ["page_000000_02000", "page_002000_02000", "page_004000_02000",
                          "page_006000_02000", "page_008000_00060"])
        self.assertEqual(len(names), 10 + 5)
        self.assertEqual(source._sha(_canonical(list(range(1, 8061)))), source.EXPECTED_ID_SHA256)

    def test_page_urls_stay_under_the_request_line_limit(self):
        for offset in range(0, source.EXPECTED_COUNT, source.PAGE_START_RECORDS):
            count = min(source.PAGE_START_RECORDS, source.EXPECTED_COUNT - offset)
            self.assertLess(len(source._page_query(offset, count)), 8000)


if __name__ == "__main__":
    unittest.main()

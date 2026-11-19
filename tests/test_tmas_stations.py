import hashlib
import gzip
import io
import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from dagster import AssetCheckSeverity, DefaultScheduleStatus
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.tmas_stations import (
    tmas_stations, tmas_stations_complete, tmas_stations_draft_schedule, tmas_stations_baseline_drift,
)
from dagster_hifld.sources import tmas_stations as source
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.esri_common import canonical as _canonical

_REAL_HTTPX_CLIENT = __import__("httpx").Client

PATCH_IDS = list(range(1, 7))


def _feature(oid: int) -> dict:
    attrs = {"OBJECTID": oid, "Station_Id": f"S{oid:03d}" if oid != 3 else "S001",
             "functional_class": "1U" if oid % 2 else "3R",
             "state": "DC" if oid == 5 else "VA",
             "latitude": 36.0 + oid / 1000, "longitude": -104.0 - oid / 1000}
    return {"attributes": attrs,
            "geometry": {"x": attrs["longitude"], "y": attrs["latitude"]}}


def _base_documents(ids=None):
    ids = PATCH_IDS if ids is None else ids
    service = {"serviceItemId": source.ITEM_ID, "serviceName": "NTAD_Travel_Monitoring_Analysis_System_Stations",
               "spatialReference": {"wkid": 4326, "latestWkid": 4326},
               "layers": [{"id": layer_id, "name": name, "type": layer_type}
                          for layer_id, name, layer_type in source.EXPECTED_SERVICE_LAYER_ROSTER]}
    layer = {"id": source.LAYER_ID, "name": source.LAYER_NAME, "type": "Feature Layer",
             "geometryType": "esriGeometryPoint", "objectIdField": "OBJECTID",
             "hasZ": False, "hasM": False, "maxRecordCount": 2000,
             "spatialReference": {"wkid": 4326, "latestWkid": 4326},
             "extent": {"xmin": -165.39294, "ymin": 17.971656, "xmax": -65.779571, "ymax": 64.97835,
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
    docs = {"ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": list(ids)},
            "portal_item": portal, "service_before": service, "layer_before": layer,
            "count_before": {"count": len(ids)},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": list(ids)},
            "service_after": json.loads(json.dumps(service)),
            "layer_after": json.loads(json.dumps(layer)),
            "count_after": {"count": len(ids)},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": list(ids)}}
    for offset in range(0, len(ids), source.PAGE_START_RECORDS):
        count = min(source.PAGE_START_RECORDS, len(ids) - offset)
        rows = [_feature(oid) for oid in ids[offset:offset + count]]
        docs[source._page_name(offset, count)] = {
            "spatialReference": {"wkid": 4326, "latestWkid": 4326}, "features": rows}
    return docs


def _docs_with_state_drift():
    docs = json.loads(json.dumps(_base_documents()))
    docs["page_000000_00002"]["features"][0]["attributes"]["state"] = "XX"
    return docs


def _BASE_DOCS_LOADER():
    return _base_documents()


def _synthetic_client_factory(docs, *, encoding="identity"):
    """Serve the fixture documents as raw-order Esri JSON responses."""
    urls = {"ids_seed": source._query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
            **{name: url for name, url in source._doc_urls()},
            **{name: url for name, url in source._after_doc_urls()}}
    for name in docs:
        if name.startswith("page_"):
            urls[name] = source._page_query(*source._parse_page_name(name))
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
        if encoding == "gzip":
            body = gzip.compress(body)
        return httpx.Response(200, stream=_ByteStream([body]),
                              headers={"Content-Type": "application/json",
                                       "Content-Encoding": encoding,
                                       "Content-Length": str(len(body))})

    def factory(*args, **kwargs):
        return _REAL_HTTPX_CLIENT(transport=httpx.MockTransport(handler))

    return factory


class TmasStationsTests(unittest.TestCase):
    def setUp(self):
        pages = patch.object(source, "PAGE_START_RECORDS", 2)
        pages.start()
        self.addCleanup(pages.stop)

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
        check = tmas_stations_complete({"feature_count": 6, "source_population_complete": True, "source_null_geometry_count": 0,
                                        "source_invalid_geometry_count": 0})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = tmas_stations_complete({"feature_count": 6, "source_population_complete": False, "source_null_geometry_count": 0,
                                         "source_invalid_geometry_count": 0})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_tile_the_roster(self):
        names = source._capture_names([(0, 3), (3, 3), (6, 1)])
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
        self.assertEqual(audit["source_id_sha256"], hashlib.sha256(_canonical(PATCH_IDS)).hexdigest())
        self.assertEqual(audit["source_geometry_type"], "esriGeometryPoint")
        self.assertEqual(audit["null_geometry_count"], 0)
        self.assertEqual(audit["geometry_attribute_mismatch_count"], 0)
        self.assertEqual(audit["unique_station_id_count"], 5)
        self.assertEqual(audit["station_id_duplicate_count"], 1)
        self.assertEqual(audit["observed_state_count"], 2)
        self.assertEqual(audit["source_null_counts"]["latitude"], 0)

    def test_changed_native_roster_is_complete_against_publisher_witnesses(self):
        docs = self._documents()
        for name in ("ids_seed", "ids_before", "ids_after"):
            docs[name]["objectIds"][-1] = 9
        docs["page_000004_00002"]["features"][-1]["attributes"]["OBJECTID"] = 9
        _, _, extracted = source._validate_captures(self._captures(docs))
        self.assertEqual(extracted["ids"], [1, 2, 3, 4, 5, 9])

    def test_additions_and_removals_cross_page_boundaries_without_snapshot_pins(self):
        for ids in ([2, 8, 15], [1, 2, 3, 4, 5, 6, 12]):
            with self.subTest(ids=ids):
                docs = _base_documents(ids)
                docs["ids_after"]["objectIds"].reverse()
                _, _, extracted = source._validate_captures(self._captures(docs))
                self.assertEqual(extracted["ids"], ids)
                self.assertEqual(extracted["audit"]["source_feature_count"], len(ids))
                self.assertEqual(extracted["audit"]["source_page_count"], (len(ids) + 1) // 2)

    def test_nullable_values_duplicates_and_changed_coverage_are_observations(self):
        docs = self._documents()
        rows = docs["page_000000_00002"]["features"]
        rows[0]["attributes"].update(Station_Id=None, state=None, latitude=None)
        rows[1]["attributes"]["Station_Id"] = "S001"
        rows[0]["geometry"].update(x=-105.0)
        rows[0]["attributes"]["longitude"] = -105.0
        _, _, extracted = source._validate_captures(self._captures(docs))
        audit = extracted["audit"]
        self.assertEqual(audit["station_id_null_count"], 1)
        self.assertEqual(audit["source_null_counts"]["latitude"], 1)
        self.assertEqual(audit["observed_bounds"]["xmin"], -105.0)
        table = pq.read_table(io.BytesIO(source._parquet_bytes(docs["layer_before"]["fields"], extracted["features"])))
        self.assertIsNone(table.column("Station_Id")[0].as_py())

    def test_baseline_warn_is_separate_from_blocking_completeness(self):
        candidate = {"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
                     "feature_count": 7, "source_id_sha256": "new", "source_population_complete": True,
                     "source_null_geometry_count": 0, "source_invalid_geometry_count": 0}
        self.assertTrue(tmas_stations_complete(candidate).passed)
        skipped = tmas_stations_baseline_drift(candidate)
        self.assertFalse(skipped.passed)
        self.assertEqual(skipped.severity, AssetCheckSeverity.WARN)
        self.assertEqual(skipped.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        reference = {**candidate, "version": "approved-fixture", "release_accepted": True,
                     "feature_count": 6, "source_id_sha256": "old"}
        compared = approved_baseline_drift(candidate, reference)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.severity, AssetCheckSeverity.WARN)
        self.assertEqual(compared.metadata["feature_count_delta"].value, 1)
        self.assertFalse(next(iter(tmas_stations_baseline_drift.check_specs)).blocking)

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
            ("count", lambda d: d["count_after"].update(count=len(PATCH_IDS) - 1),
             "source_count_or_id_roster_drift"),
            ("ids", lambda d: d["ids_after"].update(objectIds=PATCH_IDS[:-1]),
             "source_count_or_id_roster_drift"),
            ("roster digest",
             lambda d: d["ids_before"].update(objectIds=PATCH_IDS[:-1] + [99]),
             "source_count_or_id_roster_drift"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_schema_contract_drift"),
            ("field roster", lambda d: d["layer_before"]["fields"].pop(1),
             "source_schema_contract_drift"),
            ("extent", lambda d: d["layer_before"]["extent"].update(xmin=-181.0),
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
            ("geometry attribute mismatch",
             lambda d: d["page_000000_00002"]["features"][0]["geometry"].update(x=-150.0),
             "source_geometry_population_drift"),
            ("page tiling", lambda d: d.pop("page_000004_00002"),
             "source_page_tiling_incomplete_or_overlapped"),
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
            layer, fields_value, extracted = source._validate_captures(self._captures(docs))
            self.assertEqual(extracted["audit"]["geometry_attribute_mismatch_count"], 0)
        with self.subTest(label="beyond tolerance fails closed"):
            docs = json.loads(json.dumps(self._documents()))
            move_geometry(docs["page_000000_00002"]["features"], 1e-7)
            with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
                source._validate_captures(self._captures(docs))

    def test_valid_replay_seals_ready_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            root = Path(tmp) / "capture"
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
            originals = {key: (storage.read_key(key), storage.object_snapshot(key))
                         for key in storage.list_prefix("")}
            capture_root = Path(first["capture_directory"])
            capture_manifest = (capture_root / "capture_manifest.json").read_bytes()
            with patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                second = source.capture_tmas_stations(storage, version="regression",
                                                      capture_directory=first["capture_directory"])
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])
            self.assertEqual((capture_root / "capture_manifest.json").read_bytes(), capture_manifest)
            with patch.object(source.httpx, "Client", side_effect=factory):
                refresh = source.capture_tmas_stations(storage, version="fresh", capture_directory=str(root))
            self.assertTrue(refresh["reused"])
            self.assertEqual(refresh["version"], first["version"])
            self.assertNotEqual(refresh["capture_directory"], first["capture_directory"])
            self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key))
                              for key in storage.list_prefix("")}, originals)

    def test_wire_body_binding_rejects_forged_decoded_evidence(self):
        for encoding in ("identity", "gzip"):
            with self.subTest(encoding=encoding), tempfile.TemporaryDirectory() as tmp:
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                factory = _synthetic_client_factory(self._documents(), encoding=encoding)
                with patch.object(source.httpx, "Client", side_effect=factory):
                    first = source.capture_tmas_stations(storage, version="wire", capture_directory=tmp + "/capture")
                root = Path(first["capture_directory"])
                originals = {key: (storage.read_key(key), storage.object_snapshot(key))
                             for key in storage.list_prefix("")}
                with patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                    replay = source.capture_tmas_stations(storage, version="replay", capture_directory=str(root))
                self.assertEqual(replay["version"], first["version"])
                name = "page_000000_00002"
                body = (root / f"{name}.body").read_bytes().replace(b"S001", b"S999")
                (root / f"{name}.body").write_bytes(body)
                receipt = json.loads((root / f"{name}.receipt.json").read_bytes())
                receipt.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
                (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
                manifest = json.loads((root / "capture_manifest.json").read_bytes())
                manifest["receipts"][name] = receipt
                manifest["artifact_roster"] = source.capture_roster_digests(root, exclude={"capture_manifest.json"})
                (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
                with patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                    with self.assertRaisesRegex(ValueError, "retained_capture_wire_body_mismatch"):
                        source.capture_tmas_stations(storage, version="forged", capture_directory=str(root))
                self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key))
                                  for key in storage.list_prefix("")}, originals)

    def test_replay_custody_and_cached_parquet_integrity_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            factory = _synthetic_client_factory(self._documents())
            with patch.object(source.httpx, "Client", side_effect=factory):
                first = source.capture_tmas_stations(storage, version="custody", capture_directory=tmp + "/capture")
            root = Path(first["capture_directory"])
            originals = {key: (storage.read_key(key), storage.object_snapshot(key))
                         for key in storage.list_prefix("")}
            (root / "extra.body").write_bytes(b"unbound")
            with self.assertRaisesRegex(ValueError, "capture_roster_mismatch"):
                source._read_replay(root)
            (root / "extra.body").unlink()
            name = "page_000000_00002"
            receipt_path = root / f"{name}.receipt.json"
            receipt_bytes = receipt_path.read_bytes()
            manifest_path = root / "capture_manifest.json"
            manifest_bytes = manifest_path.read_bytes()
            receipt = json.loads(receipt_bytes)
            receipt.update(url=source._page_query(2, 2), final_url=source._page_query(2, 2))
            receipt_path.write_bytes(_canonical(receipt))
            manifest = json.loads(manifest_bytes)
            manifest["receipts"][name] = receipt
            manifest["artifact_roster"] = source.capture_roster_digests(root, exclude={"capture_manifest.json"})
            manifest_path.write_bytes(_canonical(manifest))
            with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
                source._read_replay(root)
            receipt_path.write_bytes(receipt_bytes)
            manifest_path.write_bytes(manifest_bytes)
            self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key))
                              for key in storage.list_prefix("")}, originals)
            parquet_key = first["candidate_ready"].replace("metadata/candidate_ready.json", f"geoparquet/{source.FILE_SLUG}.parquet")
            storage.write_key(parquet_key, b"corrupt parquet")
            tampered = {key: (storage.read_key(key), storage.object_snapshot(key))
                        for key in storage.list_prefix("")}
            with patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                with self.assertRaisesRegex(ValueError, "cached_candidate_object_integrity_failed"):
                    source.capture_tmas_stations(storage, version="corrupt", capture_directory=str(root))
            self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key))
                              for key in storage.list_prefix("")}, tampered)

    def test_drifted_replay_seals_no_ready_candidate(self):
        def mutate(docs):
            docs["page_000000_00002"]["features"][0]["attributes"]["state"] = "XX"
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            root = Path(tmp) / "capture"
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


class TmasStationsPageRosterTests(unittest.TestCase):
    def test_large_dynamic_roster_and_request_line_limit(self):
        count = 8061
        pages = [(offset, min(2000, count - offset)) for offset in range(0, count, 2000)]
        names = source._capture_names(pages)
        self.assertIn("page_008000_00061", names)
        self.assertEqual(len(names), 15)
        for offset, size in pages:
            self.assertLess(len(source._page_query(offset, size)), 8000)

import json
import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import httpx
from shapely.geometry import Point

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.locks_usace_ienc import (
    locks_usace_ienc, locks_usace_ienc_complete, locks_usace_ienc_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import locks_usace_ienc as source


def _metadata(wkid=4019):
    return {
        "id": 0, "name": "Lock", "geometryType": "esriGeometryPoint",
        "objectIdField": "OBJECTID", "spatialReference": {"wkid": wkid, "latestWkid": wkid},
        "hasZ": False, "hasM": False,
        "fields": [{"name": name, "type": kind, "length": width,
                    "nullable": name != "OBJECTID"}
                   for name, kind, width in source.EXPECTED_FIELDS],
    }


def _table(*, blob_null=False, oid_shift=0, rows=source.EXPECTED_COUNT):
    arrays = []
    for name, _kind, _width in source.EXPECTED_FIELDS:
        dtype = source.EXPECTED_PYARROW_TYPES[name]
        if name == "OBJECTID":
            values = [i + 1 + oid_shift for i in range(rows)]
        elif name == "SE_ANNO_CAD_DATA":
            values = [None if blob_null else b"" for _ in range(rows)]
        else:
            values = [None] * rows
        arrays.append(pa.array(values, type=dtype))
    points = [Point(float(i), float(i + 1)).wkb for i in range(rows)]
    arrays.append(pa.array(points, type=pa.binary()))
    return pa.Table.from_arrays(arrays, names=source.EXPECTED_COLUMN_ORDER)


class LocksUsaceIencTests(unittest.TestCase):
    def test_registered_draft_asset_and_blocked_acceptance(self):
        self.assertIn(locks_usace_ienc, hifld_ingest_assets)
        self.assertEqual(locks_usace_ienc_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = locks_usace_ienc_complete({"feature_count": 234, "source_population_complete": True})
        self.assertTrue(check.passed)
        for flag in ("mapping_approved", "rights_accepted", "release_accepted", "schema_contract_accepted"):
            self.assertFalse(check.metadata[flag].value)

    def test_candidate_preserves_native_oid_binary_fields_wkb_and_unknown_datum_crs(self):
        table, audit = source.validate_native_table(_table(),
            {"crs": "EPSG:4019", "geometry_type": "Point", "layer_metadata": _metadata()},
            source.EXPECTED_OIDS)
        self.assertEqual(table.num_rows, 234)
        self.assertEqual(table.column_names, source.EXPECTED_COLUMN_ORDER)
        self.assertEqual(table.column("OBJECTID").to_pylist(), source.EXPECTED_OIDS)
        self.assertEqual(table.column("SE_ANNO_CAD_DATA").to_pylist(), [b""] * 234)
        self.assertEqual(audit["native_crs_authority"], "EPSG:4019")
        self.assertEqual(audit["native_crs_name"], "Unknown datum based upon the GRS 1980 ellipsoid")
        self.assertFalse(audit["native_crs_mapping_approved"])
        raw = source._canonical_geoparquet(table)
        import io
        output = pq.read_table(io.BytesIO(raw))
        self.assertEqual(output.column("OBJECTID").to_pylist(), source.EXPECTED_OIDS)
        self.assertEqual(output.column("SE_ANNO_CAD_DATA").to_pylist(), [b""] * 234)
        geo = json.loads(output.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 4019})

    def test_quarantines_blob_null_oid_count_and_crs_drift(self):
        cases = [
            (_table(blob_null=True), _metadata(), "native_blob_null_or_nonempty_drift"),
            (_table(oid_shift=1), _metadata(), "native_oid_roster_drift"),
            (_table(), _metadata(wkid=4326), "source_layer_schema_identity_or_crs_drift"),
            (_table(rows=233), _metadata(), "native_feature_count_drift"),
        ]
        for table, metadata, error in cases:
            with self.subTest(error=error), self.assertRaisesRegex(source.LocksQuarantineError, error):
                source.validate_native_table(table,
                    {"crs": "EPSG:4019", "geometry_type": "Point", "layer_metadata": metadata},
                    source.EXPECTED_OIDS)

    def test_safe_filegdb_zip_accepts_native_archive_and_rejects_traversal(self):
        valid_sink = io.BytesIO()
        with zipfile.ZipFile(valid_sink, "w") as archive:
            archive.writestr("locks.gdb/gdb", b"safe")
        archive_bytes = valid_sink.getvalue()
        root, audit = source._safe_gdb_archive(archive_bytes)
        self.assertTrue(root.endswith(".gdb"))
        self.assertEqual(audit["expanded_bytes"], 4)
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w") as archive:
            archive.writestr("locks.gdb/../escape", b"x")
        with self.assertRaisesRegex(source.LocksQuarantineError, "unsafe_filegdb_zip_member"):
            source._safe_gdb_archive(sink.getvalue())

    def test_service_capture_requires_exact_pre_post_roster_and_completed_replica(self):
        status_url = source.SERVICE_URL + "/jobs/job-id"
        result_url = source.SERVICE_URL + "/replicafilescache/locks.zip"
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("locks.gdb/gdb", b"x")
        schema = _metadata()
        count = {"count": 234}
        ids = {"objectIds": source.EXPECTED_OIDS}
        job = {"status": "Completed", "recordCount": 234, "resultUrl": result_url,
               "replicaName": "hifld_locks_151_fixture"}
        create = {"statusUrl": status_url, "replicaName": "hifld_locks_151_fixture"}
        docs = {
            "metadata_before": (schema, "GET", source.LAYER_URL + "?f=json"),
            "count_before": (count, "GET", source.LAYER_URL + "/query?" + source.urlencode(
                {"where": "1=1", "returnCountOnly": "true", "f": "json"})),
            "ids_before": (ids, "GET", source.LAYER_URL + "/query?" + source.urlencode(
                {"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
            "create_replica": (create, "POST", source.CREATE_REPLICA_URL),
            "poll_01": (job, "GET", status_url + "?f=json"),
            "result_head": (None, "HEAD", result_url),
            "archive": (archive.getvalue(), "GET", result_url),
            "metadata_after": (schema, "GET", source.LAYER_URL + "?f=json"),
            "count_after": (count, "GET", source.LAYER_URL + "/query?" + source.urlencode(
                {"where": "1=1", "returnCountOnly": "true", "f": "json"})),
            "ids_after": (ids, "GET", source.LAYER_URL + "/query?" + source.urlencode(
                {"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
        }
        captures = {}
        for name, (body, method, url) in docs.items():
            body_bytes = b"" if body is None else body if isinstance(body, bytes) else source.canonical(body)
            receipt = {"method": method, "url": url, "final_url": url, "status": 200,
                       "response_headers": {"content-length": str(len(archive.getvalue()))} if name == "result_head" else {}}
            captures[name] = (body_bytes, receipt)
        result = source._validate_service_bundle(captures)
        self.assertEqual(result[2], source.EXPECTED_OIDS)
        self.assertEqual(result[3], archive.getvalue())

        broken = dict(captures)
        broken["ids_after"] = (source.canonical({"objectIds": source.EXPECTED_OIDS[:-1]}), captures["ids_after"][1])
        with self.assertRaisesRegex(source.LocksQuarantineError, "source_count_or_id_roster_drift:after"):
            source._validate_service_bundle(broken)

    def test_empty_explicit_capture_directory_starts_one_live_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture = root / "capture"
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "stage"), prefix="")
            with patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=500_000_000)), \
                 patch.object(source, "_capture_live", side_effect=RuntimeError("network path stubbed")) as live:
                with self.assertRaisesRegex(RuntimeError, "network path stubbed"):
                    source.capture_locks_usace_ienc(storage, capture_directory=str(capture))
            live.assert_called_once_with(capture, resume_from=None)
            self.assertEqual(len(list((root / "stage" / "_quarantine").rglob("failure.json"))), 1)

    def test_explicit_capture_directory_dispatches_live_or_sealed_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "stage"), prefix="")
            sealed = root / "sealed"
            sealed.mkdir()
            (sealed / "capture_manifest.json").write_text("{}")
            with patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=500_000_000)), \
                 patch.object(source, "_capture_manifest", return_value={"sealed": True}) as validate_manifest, \
                 patch.object(source, "_load_capture", return_value=({}, {"create_replica_request": {
                     "parameters": {"replicaName": "hifld_locks_151_test"}}})) as load_capture, \
                 patch.object(source, "_capture_live") as live, \
                 patch.object(source, "_validate_service_bundle", side_effect=source.LocksQuarantineError("fixture-stop")), \
                 patch.object(source, "_preserve_failed_capture"):
                with self.assertRaisesRegex(source.LocksQuarantineError, "fixture-stop"):
                    source.capture_locks_usace_ienc(storage, capture_directory=str(sealed))
            validate_manifest.assert_called_once_with(sealed)
            load_capture.assert_called_once_with(sealed)
            live.assert_not_called()
            unsealed = root / "unsealed"
            unsealed.mkdir()
            (unsealed / "stray.json").write_text("{}")
            with patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=500_000_000)), \
                 patch.object(source, "_capture_live") as live:
                with self.assertRaisesRegex(source.LocksQuarantineError, "explicit_offline_capture_must_be_sealed"):
                    source.capture_locks_usace_ienc(storage, capture_directory=str(unsealed))
            live.assert_not_called()

    def test_replica_download_redirect_is_bounded_recorded_and_credential_free(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            start = source.SERVICE_URL + "/replicafilescache/test.zip"
            final = "https://www.arcgis.com/sharing/rest/content/items/test/data"
            calls = []
            def handler(request):
                calls.append(request)
                if str(request.url) == start:
                    return httpx.Response(302, headers={"Location": final, "Content-Length": "8",
                        "Content-Type": "text/plain", "Set-Cookie": "sid=secret"},
                        stream=httpx.ByteStream(b"redirect"))
                self.assertEqual(str(request.url), final)
                self.assertNotIn("cookie", request.headers)
                self.assertNotIn("authorization", request.headers)
                return httpx.Response(200, headers={"Content-Length": "7"}, stream=httpx.ByteStream(b"archive"))
            with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False) as client:
                body, receipt = source._perform(client, "GET", start, root, "archive",
                    max_bytes=1024, budget=[0], allow_official_redirects=True)
            self.assertEqual(body, b"archive")
            self.assertEqual(receipt["status"], 200)
            self.assertEqual(receipt["final_url"], final)
            self.assertEqual(receipt["redirects"][0]["location"], final)
            self.assertEqual((root / "archive.redirect_01.body").read_bytes(), b"redirect")
            self.assertTrue((root / "archive.redirect_01.receipt.json").is_file())
            self.assertEqual(len(calls), 2)

    def test_replica_download_rejects_nonofficial_or_http_redirect_target(self):
        for target in ("http://www.arcgis.com/data.zip", "https://example.org/data.zip"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                start = source.SERVICE_URL + "/replicafilescache/test.zip"
                calls = []
                def handler(request):
                    calls.append(request)
                    return httpx.Response(302, headers={"Location": target, "Content-Length": "8"},
                                           stream=httpx.ByteStream(b"redirect"))
                with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False) as client:
                    with self.assertRaisesRegex(source.LocksQuarantineError,
                                                "replica_download_redirect_target_not_official_https"):
                        source._perform(client, "GET", start, root, "archive",
                            max_bytes=1024, budget=[0], allow_official_redirects=True)
                self.assertEqual(len(calls), 1)
                self.assertTrue((root / "archive.redirect_01.receipt.json").is_file())

    def test_replica_download_redirect_hop_limit_is_enforced_and_receipted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            start = source.SERVICE_URL + "/replicafilescache/test.zip"
            calls = []
            def handler(request):
                calls.append(request)
                return httpx.Response(302, headers={"Location": f"https://www.arcgis.com/data/{len(calls)}",
                    "Content-Length": "0"}, stream=httpx.ByteStream(b""))
            with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False) as client:
                with self.assertRaisesRegex(source.LocksQuarantineError,
                                            "replica_download_redirect_limit_or_method_invalid"):
                    source._perform(client, "GET", start, root, "archive",
                        max_bytes=1024, budget=[0], allow_official_redirects=True)
            self.assertEqual(len(calls), source.MAX_DOWNLOAD_REDIRECTS + 1)
            self.assertTrue((root / f"archive.redirect_{source.MAX_DOWNLOAD_REDIRECTS + 1:02d}.receipt.json").is_file())

    def test_completed_replica_resume_requires_intact_partial_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "partial"
            root.mkdir()
            status_url = source.SERVICE_URL + "/jobs/job-id"
            result_url = source.SERVICE_URL + "/replicafilescache/replica.zip"
            params = {"replicaName": "hifld_locks_151_existing", "layers": "0", "syncModel": "none",
                "dataFormat": "filegdb", "transportType": "esriTransportTypeUrl", "async": "true",
                "returnAttachments": "false", "f": "json"}
            request_body = source.urlencode(params).encode("ascii")
            (root / "create_replica.request.body").write_bytes(request_body)
            docs = {
                "metadata_before": (_metadata(), "GET", source.LAYER_URL + "?f=json"),
                "count_before": ({"count": 234}, "GET", source.LAYER_URL + "/query?" + source.urlencode(
                    {"where": "1=1", "returnCountOnly": "true", "f": "json"})),
                "ids_before": ({"objectIds": source.EXPECTED_OIDS}, "GET", source.LAYER_URL + "/query?" + source.urlencode(
                    {"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                "create_replica": ({"statusUrl": status_url},
                    "POST", source.CREATE_REPLICA_URL),
                "poll_01": ({"status": "Completed", "recordCount": 234, "resultUrl": result_url,
                    "replicaName": params["replicaName"]}, "GET", status_url + "?f=json"),
                "result_head": (None, "HEAD", result_url),
            }
            for name, (body, method, url) in docs.items():
                raw = b"" if body is None else body if isinstance(body, bytes) else source.canonical(body)
                wire = b""
                status = 302 if name == "result_head" else 200
                receipt = {"method": method, "url": url, "final_url": url, "status": status,
                    "complete_body": status == 200, "observed_at": "2026-10-22T00:00:00+00:00",
                    "bytes": len(raw), "sha256": source.digest(raw), "wire_bytes": len(wire),
                    "wire_sha256": source.digest(wire), "response_headers":
                    {"content-length": "370"} if name == "result_head" else {}}
                if name == "create_replica":
                    receipt.update({"request_body_bytes": len(request_body),
                                    "request_body_sha256": source.digest(request_body)})
                (root / f"{name}.body").write_bytes(raw)
                (root / f"{name}.wire").write_bytes(wire)
                (root / f"{name}.receipt.json").write_bytes(source.canonical(receipt))
            captures, request_info, loaded_params, loaded_url, prior = source._load_partial_completed_replica(root)
            self.assertNotIn("result_head", captures)
            self.assertEqual(loaded_params, params)
            self.assertEqual(loaded_url, result_url)
            self.assertEqual(request_info["sha256"], source.digest(request_body))
            self.assertEqual(prior["failed_head_status"], 302)
            archive_io = io.BytesIO()
            with zipfile.ZipFile(archive_io, "w") as zf:
                zf.writestr("locks.gdb/gdb", b"safe")
            archive = archive_io.getvalue()
            redirect_url = "https://www.arcgis.com/sharing/rest/content/items/replica/data"
            requests = []
            def response(status, body=b"", headers=None):
                headers = dict(headers or {})
                headers.setdefault("Content-Length", str(len(body)))
                return httpx.Response(status, headers=headers, stream=httpx.ByteStream(body))
            def handler(request):
                requests.append(request)
                url = str(request.url)
                if request.method == "HEAD" and url == result_url:
                    return response(302, headers={"Location": redirect_url})
                if request.method == "HEAD" and url == redirect_url:
                    return response(200, headers={"Content-Length": str(len(archive))})
                if request.method == "GET" and url == result_url:
                    return response(302, b"redirect", {"Location": redirect_url})
                if request.method == "GET" and url == redirect_url:
                    return response(200, archive)
                if url == source.LAYER_URL + "?f=json":
                    return response(200, source.canonical(_metadata()))
                if "returnCountOnly=true" in url:
                    return response(200, source.canonical({"count": 234}))
                if "returnIdsOnly=true" in url:
                    return response(200, source.canonical({"objectIds": source.EXPECTED_OIDS}))
                raise AssertionError(f"unexpected resume request {request.method} {url}")
            mock_client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
            resumed_root = root.parent / "resumed"
            with patch.object(source.httpx, "Client", return_value=mock_client), \
                 patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=500_000_000)):
                resumed, manifest = source._capture_live(resumed_root, resume_from=root)
            self.assertFalse(any(request.method == "POST" for request in requests))
            self.assertIn("continued_completed_replica", manifest)
            self.assertEqual(source._validate_service_bundle(
                resumed, expected_replica_name=params["replicaName"])[3], archive)
            (root / "poll_01.body").write_bytes(b"tampered")
            with self.assertRaisesRegex(source.LocksQuarantineError, "resume_replica_receipt_integrity_failure:poll_01"):
                source._load_partial_completed_replica(root)

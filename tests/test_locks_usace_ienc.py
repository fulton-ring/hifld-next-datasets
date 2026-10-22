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
               "replicaName": "replica"}
        create = {"statusUrl": status_url, "replicaName": "replica"}
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
            live.assert_called_once_with(capture)
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
                 patch.object(source, "_load_capture", return_value=({}, {"sealed": True})) as load_capture, \
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

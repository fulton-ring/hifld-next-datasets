from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.usace_recommended_tracks import (
    recommended_tracks_draft_schedule,
    usace_recommended_tracks_complete,
    usace_recommended_tracks_ienc,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import usace_recommended_tracks as source


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


class IencFixture:
    def __init__(self, expected_count=2, *, count_after=None, schema_drift=False):
        self.expected_count = expected_count
        self.count_after = count_after if count_after is not None else expected_count
        self.schema_drift = schema_drift
        self.ids = list(range(1, expected_count + 1))
        self.extent = {"xmin": -10.0, "ymin": -10.0, "xmax": 10.0, "ymax": 10.0,
                       "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
        self.fields = [{"name": name, "type": kind} for name, kind in source.ATTRIBUTE_FIELDS]
        self.declarations = [dict(field) for field in source.EXPECTED_DECLARATIONS]
        self.responses = {}

    def metadata(self):
        declarations = [dict(field) for field in self.declarations]
        if self.schema_drift:
            declarations[2]["type"] = "esriFieldTypeInteger"
        return {
            "id": 46, "name": "RECOMMENDED_TRACK_LINE", "geometryType": "esriGeometryPolyline",
            "maxRecordCount": 2000, "copyrightText": "USACE IENC", "fields": declarations,
            "extent": self.extent, "spatialReference": {"wkid": 4326, "latestWkid": 4326},
            "hasZ": False, "hasM": False,
        }

    def feature(self, identity):
        return {
            "attributes": {
                "OBJECTID": identity,
                "Category_of_Recommended_Track": "Category",
                "Traffic_Flow": "Two-Way",
                "Information": "fixture",
                "Scale_Minimum": 45000,
                "Source_Date": "20040101",
                "Source_Indication": "USACE IENC fixture",
                "Source_Dataset": "fixture",
                "Feature_ID": f"F-{identity}",
            },
            "geometry": {"paths": [[[float(identity), 1.0], [float(identity) + 0.5, 2.0]]]},
        }

    def handler(self, request):
        parsed = urlsplit(str(request.url))
        params = parse_qs(parsed.query)
        if parsed.path == urlsplit(source.BTS_ITEM_URL).path:
            value = {"id": source.BTS_ITEM_ID, "owner": "USDOT_BTS",
                     "title": "Inland Electronic Navigational Charts", "type": "PDF", "access": "public",
                     "description": "IENC Feature Classes: https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes"}
        elif parsed.path == urlsplit(source.PRIOR_BTS_ITEM_URL).path:
            value = {"error": {"code": 403, "message": "not available"}}
        elif parsed.path == urlsplit(source.SERVICE_DIRECTORY_URL).path:
            value = {"services": [{"name": "IENC_Feature_Classes/RECOMMENDED_TRACK_LINE", "type": "MapServer"}]}
        elif parsed.path == urlsplit(source.LAYER_URL).path:
            value = self.metadata()
        elif parsed.path == urlsplit(source.LAYER_QUERY_URL).path:
            if params.get("returnCountOnly") == ["true"]:
                count = self.expected_count
                if params.get("where") == ["1=1"] and self.count_after != self.expected_count:
                    # The after snapshot is the second count request.
                    count = self.count_after if sum(
                        1 for key in self.responses if "returnCountOnly" in key
                    ) else self.expected_count
                value = {"count": count}
            elif params.get("returnIdsOnly") == ["true"]:
                value = {"objectIdFieldName": "OBJECTID", "objectIds": self.ids}
            else:
                ids = [int(value) for value in params["objectIds"][0].split(",")]
                value = {"geometryType": "esriGeometryPolyline",
                         "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                         "fields": self.fields,
                         "features": [self.feature(identity) for identity in ids]}
        else:
            raise AssertionError(f"unexpected URL {request.url}")
        body = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        self.responses[str(request.url)] = body
        return httpx.Response(200, content=body, headers={"content-type": "application/json"}, request=request)


class RecommendedTracksSourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="recommended-tracks-")
        self.addCleanup(self.tmp.cleanup)
        self.storage = StagingStorageResource(use_local=True, local_dir=self.tmp.name)
        self.identity_patch = patch.object(source, "processing_identity", return_value={
            "code_commit": "test-packaged-identity", "code_identity_scheme": "test", "files": {},
        })
        self.validation_patch = patch.object(source, "validate_construction_code_identity")
        self.identity_patch.start()
        self.validation_patch.start()
        self.addCleanup(self.identity_patch.stop)
        self.addCleanup(self.validation_patch.stop)

    def _capture(self, fixture):
        client = httpx.Client(transport=httpx.MockTransport(fixture.handler), follow_redirects=False)
        def capture_mock(client_arg, url, root, name, *, max_bytes):
            response = client_arg.get(url, headers={"Accept-Encoding": "identity"})
            body = response.content
            wire = body  # MockTransport exposes decoded test bytes as its wire representation.
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(wire)
            receipt = {
                "method": "GET", "url": url, "final_url": str(response.url),
                "observed_at": datetime.now(timezone.utc).isoformat(), "status": response.status_code,
                "complete_body": True, "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest(),
                "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                "content_encoding": None, "content_length": len(body), "error_code": None,
            }
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt
        try:
            with patch.object(source, "EXPECTED_COUNT", fixture.expected_count), \
                 patch.object(source, "EXPECTED_ID_SHA256", hashlib.sha256(_canonical(fixture.ids)).hexdigest()), \
                 patch.object(source, "EXPECTED_EXTENT", {k: fixture.extent[k] for k in ("xmin", "ymin", "xmax", "ymax")}), \
                 patch.object(source, "capture_get", capture_mock):
                result = source.capture(self.storage, client=client)
        finally:
            client.close()
        return result

    def test_complete_capture_preserves_ids_fields_geometry_and_raw_receipts(self):
        fixture = IencFixture()
        result = self._capture(fixture)
        prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}"
        audit = json.loads(self.storage.read_key(prefix + "/metadata/source_audit.json"))
        manifest = json.loads(self.storage.read_key(prefix + "/metadata/source_manifest.json"))
        table = pq.read_table(pa.BufferReader(self.storage.read_key(
            self.storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG,
                                               result["version"],
                                               f"geoparquet/{source.FILE_SLUG}.parquet"))))
        self.assertEqual(result["feature_count"], 2)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [1, 2])
        self.assertEqual(table.column("Scale_Minimum").to_pylist(), [45000, 45000])
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(audit["objectid_sha256"], hashlib.sha256(_canonical([1, 2])).hexdigest())
        self.assertFalse(manifest["approved"])
        self.assertIn("succession", manifest["lineage"].lower())
        self.assertFalse(json.loads(self.storage.read_key(prefix + "/metadata/candidate_ready.json"))["release_accepted"])
        with patch("dagster_hifld.assets.usace_recommended_tracks.EXPECTED_COUNT", 2), \
             patch("dagster_hifld.assets.usace_recommended_tracks.EXPECTED_ID_SHA256",
                   hashlib.sha256(_canonical([1, 2])).hexdigest()):
            self.assertTrue(usace_recommended_tracks_complete(None, result, self.storage).passed)
        for name, body in fixture.responses.items():
            self.assertTrue(body)
        self.assertTrue(self.storage.list_keys(source.DATASET_SLUG, source.FILE_SLUG, result["version"]))
        self.assertEqual(recommended_tracks_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(next(iter(usace_recommended_tracks_ienc.keys)).path,
                         ["ingest", source.DATASET_SLUG, source.FILE_SLUG])

    def test_count_change_quarantines_all_acquired_raw_receipts(self):
        fixture = IencFixture(count_after=3)
        with self.assertRaisesRegex(ValueError, "source_population_or_metadata_changed"):
            self._capture(fixture)
        quarantine = self.storage.list_prefix(f"_quarantine/{source.DATASET_SLUG}")
        self.assertTrue(any(key.endswith("/metadata/failure.json") for key in quarantine))
        self.assertGreaterEqual(sum(key.endswith(".body") for key in quarantine), 7)
        self.assertTrue(any("page_000000.body" in key for key in quarantine))

    def test_layer_schema_change_fails_closed_and_quarantines_receipts(self):
        fixture = IencFixture(schema_drift=True)
        with self.assertRaisesRegex(ValueError, "source_schema_drift"):
            self._capture(fixture)
        self.assertTrue(self.storage.list_prefix(f"_quarantine/{source.DATASET_SLUG}"))

    def test_registered_source_gate_rejects_corrupt_population_audit(self):
        version = "v1.0.0-test"
        prefix = self.storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, version, "")
        self.storage.write_key(prefix + "metadata/source_audit.json", _canonical({
            "feature_count": 665, "objectid_sha256": source.EXPECTED_ID_SHA256,
            "count_before": 666, "count_after": 666, "source_scope": "IENC only",
        }))
        self.storage.write_key(prefix + "metadata/candidate_ready.json", _canonical({
            "source_population_complete": True, "source_id_sha256": source.EXPECTED_ID_SHA256,
            "release_accepted": False,
        }))
        result = usace_recommended_tracks_complete(None, {
            "version": version, "feature_count": 666, "source_population_complete": True,
            "source_id_sha256": source.EXPECTED_ID_SHA256,
        }, self.storage)
        self.assertFalse(result.passed)
        self.assertTrue(next(iter(usace_recommended_tracks_complete.check_specs)).blocking)

    def test_normal_repository_registers_the_stopped_source_job_and_check(self):
        from dagster_hifld.definitions import defs

        repository = defs.get_repository_def()
        self.assertIn("recommended_tracks_usace_ienc_draft_refresh",
                      {job.name for job in repository.get_all_jobs()})
        self.assertIn("recommended_tracks_draft_schedule", {item.name for item in defs.schedules})
        asset_keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertIn(next(iter(usace_recommended_tracks_ienc.keys)), asset_keys)
        check_specs = [spec for definition in defs.asset_checks for spec in definition.check_specs]
        self.assertIn("recommended_tracks_complete", {spec.name for spec in check_specs})
        self.assertEqual(recommended_tracks_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()

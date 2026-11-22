"""Population capture maps retain native disk custody without retaining page bodies."""
import json
import tempfile
import unittest
import weakref

import httpx
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import arcgis_layer, boem_ocs_diagrams, usace_pontoon_areas, ems_stations
from dagster_hifld.sources.esri_common import canonical, sha


class TrackedBody(bytearray):
    pass


class PopulationMemoryTests(unittest.TestCase):
    def test_public_roster_owner_discards_each_fetched_page(self):
        ids = list(range(1, 19))
        roster = arcgis_layer.layer_capture_roster(ids, layer_url="https://source.invalid/0",
            service_url="https://source.invalid", page_size=1)
        refs, peaks = [], []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def fetch(_client, _url, root, name):
                document = {"objectIds": ids} if name.startswith("ids_") else {"count": len(ids)}
                body = TrackedBody(json.dumps(document).encode())
                (root / f"{name}.body").write_bytes(body)
                if name.startswith("page_"):
                    refs.append(weakref.ref(body))
                    peaks.append(sum(ref() is not None for ref in refs))
                return body, {}
            captures = arcgis_layer.capture_layer_roster(object(), root, roster, fetch, captured_ids=ids)
            self.assertLessEqual(max(peaks), 2)
            self.assertTrue(all(ref() is None for ref in refs))
            self.assertEqual(json.loads(captures["ids_after"][0])["objectIds"], ids)

    def test_native_boem_reader_keeps_receipt_conversion_without_body_population(self):
        refs, peaks = [], []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "capture-summary.json").write_text('{"complete":true}')
            for index in range(18):
                name = f"page_{index:03d}"
                body = json.dumps({"features": [{"attributes": {"OBJECTID": index}}]}).encode()
                (root / f"{name}.body").write_bytes(body)
                (root / f"{name}.receipt.json").write_bytes(canonical(
                    {"complete": True, "status": 200, "bytes": len(body), "sha256": sha(body)}))
            original = Path.read_bytes
            def read(path):
                body = original(path)
                if path.suffix == ".body":
                    body = TrackedBody(body)
                    refs.append(weakref.ref(body))
                    peaks.append(sum(ref() is not None for ref in refs))
                return body
            with patch.object(Path, "read_bytes", read):
                captures = boem_ocs_diagrams._load_offline(root)
                self.assertLessEqual(max(peaks), 2)
                self.assertTrue(all(ref() is None for ref in refs))
                body, receipt = captures["page_007"]
                self.assertEqual(json.loads(body)["features"][0]["attributes"]["OBJECTID"], 7)
                self.assertTrue(receipt["complete_body"])
                self.assertEqual(receipt["wire_sha256"], sha(body))
            (root / "page_017.body").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "offline_capture_body_receipt_mismatch"):
                captures["page_017"]

    def test_ems_live_capture_keeps_byte_mapping_without_population_bodies(self):
        source = ems_stations
        ids = list(range(1, 19))
        refs, peaks = [], []
        def tracked(data):
            body = TrackedBody(data)
            refs.append(weakref.ref(body))
            peaks.append(sum(ref() is not None for ref in refs))
            return body
        def respond(request):
            query = dict(request.url.params)
            if query.get("returnIdsOnly") == "true":
                document = {"objectIds": ids}
            elif query.get("returnCountOnly") == "true":
                document = {"count": len(ids)}
            else:
                document = {"features": [], "name": "source page"}
            return httpx.Response(200, json=document)
        original_client = httpx.Client
        with tempfile.TemporaryDirectory() as tmp, patch.object(source, "COUNT", len(ids)), \
             patch.object(source, "PAGE_SIZE", 1), patch.object(source, "bytes", tracked, create=True), \
             patch.object(source, "default_capture_dir", return_value=Path(tmp)), \
             patch.object(source.httpx, "Client", side_effect=lambda **kw: original_client(
                 transport=httpx.MockTransport(respond), **kw)):
            bodies, receipts = source._capture(None)
            self.assertGreater(len(refs), len(ids))
            self.assertLessEqual(max(peaks), 2)
            self.assertTrue(all(ref() is None for ref in refs))
            self.assertEqual(json.loads(bodies["page_007"])["name"], "source page")
            self.assertEqual(len(receipts), len(ids) + 5)

    def test_inline_ienc_replay_discards_pages_before_native_schema_validation(self):
        source = usace_pontoon_areas
        ids = list(range(1, 19))
        refs, peaks = [], []
        class ReachedValidation(Exception):
            pass
        with tempfile.TemporaryDirectory() as tmp, patch.object(source, "PAGE_SIZE", 1):
            root = Path(tmp) / "capture"
            root.mkdir()
            urls = {"ids_seed": source._query({"where": "1=1", "returnIdsOnly": "true", "f": "json"}),
                    **dict(source._capture_roster(ids))}
            receipts = {}
            for name, url in urls.items():
                document = {"objectIds": ids} if name.startswith("ids_") else {"count": len(ids)}
                body = canonical(document)
                receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                    "complete_body": True, "bytes": len(body), "sha256": sha(body),
                    "wire_bytes": len(body), "wire_sha256": sha(body),
                    "content_encoding": "identity", "content_length": len(body), "error_code": None}
                for suffix, data in (("body", body), ("wire", body), ("receipt.json", canonical(receipt) + b"\n")):
                    (root / f"{name}.{suffix}").write_bytes(data)
                receipts[name] = receipt
            manifest = {"complete": True, "receipts": receipts, "attempts": {},
                "artifact_roster": capture_roster_digests(root)}
            (root / "capture_manifest.json").write_bytes(canonical(manifest))
            original = source._load_replay
            def load(root, name):
                body, receipt = original(root, name)
                if name.startswith("page_"):
                    body = TrackedBody(body)
                    refs.append(weakref.ref(body))
                    peaks.append(sum(ref() is not None for ref in refs))
                return body, receipt
            def validate(captures):
                self.assertLessEqual(max(peaks), 2)
                self.assertTrue(all(ref() is None for ref in refs))
                self.assertEqual(json.loads(captures["page_007"][0]), {"count": len(ids)})
                raise ReachedValidation()
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"), prefix="")
            with patch.object(source, "_load_replay", load), patch.object(source, "_validate", validate):
                with self.assertRaises(ReachedValidation):
                    source.capture_usace_pontoon_areas(storage, version="memory", capture_directory=str(root))

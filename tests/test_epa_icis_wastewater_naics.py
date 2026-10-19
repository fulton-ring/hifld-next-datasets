from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

import pyarrow.parquet as pq
import httpx

from dagster_hifld.assets import epa_icis_wastewater_naics as worker
from dagster_hifld.assets.epa_icis_wastewater_naics import (
    DATASET_SLUG,
    EXPECTED_ROWS,
    PROFILE,
    _triplets_sha,
    _typed_parquet,
)
from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.resources import StagingStorageResource


class EPAICISWastewaterNAICSTests(unittest.TestCase):
    def test_profile_pins_exact_epa_hosted_naics_table(self):
        self.assertEqual(DATASET_SLUG, "icis-wastewater-treatment-plants-naics-codes")
        self.assertEqual(PROFILE.portal_item_owner, "EPA_GEO")
        self.assertEqual(PROFILE.portal_item_id, "0895b107f9184e7cb31707767b506a64")
        self.assertEqual(PROFILE.table_id, 1)
        self.assertEqual(PROFILE.expected_rows, EXPECTED_ROWS)
        self.assertEqual(PROFILE.field_names,
                         ("OBJECTID", "REGISTRY_ID", "NAICS_CODE", "PRIMARY_INDICATOR"))
        self.assertIsNone(PROFILE.relationship_name)
        self.assertIsNone(PROFILE.source_vintage)

    def test_native_naics_strings_ids_and_nonspatial_schema_round_trip(self):
        rows = [
            {"OBJECTID": 1, "REGISTRY_ID": "001234567890", "NAICS_CODE": "004112",
             "PRIMARY_INDICATOR": "PRIMARY"},
            {"OBJECTID": 2, "REGISTRY_ID": None, "NAICS_CODE": "000000",
             "PRIMARY_INDICATOR": "UNKNOWN"},
        ]
        fields = tuple({"name": name, "type": esri_type, "nullable": nullable,
                        "length": length} for name, esri_type, nullable, length in PROFILE.fields)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "source.parquet"
            path.write_bytes(_typed_parquet(rows, fields))
            table = pq.read_table(path)
        self.assertEqual(table.schema.names, list(PROFILE.field_names))
        self.assertEqual(str(table.schema.field("OBJECTID").type), "int64")
        self.assertFalse(table.schema.field("OBJECTID").nullable)
        self.assertEqual(str(table.schema.field("REGISTRY_ID").type), "string")
        self.assertTrue(table.schema.field("REGISTRY_ID").nullable)
        self.assertEqual(str(table.schema.field("NAICS_CODE").type), "string")
        self.assertEqual(table.to_pylist(), rows)
        self.assertEqual(table.to_pylist()[0]["NAICS_CODE"], "004112")
        triplets_sha = _triplets_sha(rows)
        self.assertRegex(triplets_sha, r"^[0-9a-f]{64}$")
        reordered = _triplets_sha([rows[1], rows[0]])
        self.assertEqual(triplets_sha, reordered)
        self.assertNotEqual(triplets_sha, _triplets_sha([
            {**rows[0], "NAICS_CODE": "41"}, rows[1],
        ]))
        self.assertNotEqual(triplets_sha, _triplets_sha([
            rows[0], {**rows[1], "REGISTRY_ID": "None"},
        ]))

    def test_tiny_capture_catalog_and_cached_reuse_reach_finalizer(self):
        rows = [
            {"OBJECTID": 1, "REGISTRY_ID": "001234567890", "NAICS_CODE": "004112",
             "PRIMARY_INDICATOR": "PRIMARY"},
            {"OBJECTID": 2, "REGISTRY_ID": None, "NAICS_CODE": "000000",
             "PRIMARY_INDICATOR": "UNKNOWN"},
        ]
        fields = [{"name": name, "type": esri_type, "nullable": nullable, "length": length}
                  for name, esri_type, nullable, length in PROFILE.fields]
        table = {"name": PROFILE.table_name, "geometryType": None, "fields": fields,
                 "relationships": []}
        service = {"description": "EPA ICIS wastewater service", "tables": [
            {"id": PROFILE.table_id, "name": PROFILE.table_name}]}
        portal = {"id": PROFILE.portal_item_id, "owner": PROFILE.portal_item_owner,
                  "title": PROFILE.portal_item_title, "type": "Feature Service",
                  "url": PROFILE.service_url, "modified": 1779295761000}

        class JSONStream(httpx.SyncByteStream):
            def __init__(self, value):
                import json
                self.body = json.dumps(value, separators=(",", ":")).encode()

            def __iter__(self):
                yield self.body

            def close(self):
                return None

        def respond(request):
            url = urlparse(str(request.url))
            query = parse_qs(url.query)
            if "/sharing/rest/content/items/" in url.path:
                payload = portal
            elif str(request.url).split("?", 1)[0].rstrip("/") == PROFILE.service_url.rstrip("/"):
                payload = service
            elif url.path.endswith(f"/{PROFILE.table_id}"):
                payload = table
            elif query.get("returnCountOnly") == ["true"]:
                payload = {"count": 2}
            elif query.get("returnIdsOnly") == ["true"]:
                payload = {"objectIds": [1, 2]}
            else:
                offset = int(query.get("resultOffset", [0])[0])
                limit = int(query.get("resultRecordCount", [2])[0])
                payload = {"features": [{"attributes": row} for row in rows[offset:offset + limit]]}
            return httpx.Response(200, stream=JSONStream(payload),
                                  headers={"content-type": "application/json"})

        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            client = httpx.Client(transport=httpx.MockTransport(respond), follow_redirects=False)
            with client, patch.object(worker, "PROFILE", replace(PROFILE, expected_rows=2)), \
                    patch.object(worker, "EXPECTED_ROWS", 2):
                captured = worker.capture_epa_icis_wastewater_naics(storage, client=client)
                summary = summarize_staged_catalog(
                    storage, DATASET_SLUG, DATASET_SLUG, captured["version"], DATASET_SLUG,
                )
                write_catalog_metadata(storage, dataset_slug=DATASET_SLUG, file_slug=DATASET_SLUG,
                                       version=captured["version"], quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                cached = worker.capture_epa_icis_wastewater_naics(
                    storage, offline_replay_version=captured["version"], client=client,
                )
            self.assertEqual(captured["row_count"], 2)
            self.assertFalse(captured["release_accepted"])
            self.assertEqual(summary.quality_manifest["feature_count"], 2)
            self.assertTrue(cached["reused"])


if __name__ == "__main__":
    unittest.main()

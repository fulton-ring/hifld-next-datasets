import json
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pyarrow.parquet as pq
import pyarrow as pa

from dagster_hifld.sources import epa_frs_tri as tri
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.sources.epa_frs_common import candidate_prefix, verify_cached_candidate


class EpaFrsTriTests(unittest.TestCase):
    def _capture_fixture(self):
        fields = []
        for name in tri.EXPECTED_FIELD_NAMES:
            kind = "esriFieldTypeString"
            if name == "OBJECTID": kind = "esriFieldTypeOID"
            elif name == "ACCURACY_VALUE": kind = "esriFieldTypeInteger"
            elif name in {"CREATE_DATE", "UPDATE_DATE", "LAST_REPORTED_DATE"}: kind = "esriFieldTypeDate"
            elif name in {"LATITUDE83", "LONGITUDE83"}: kind = "esriFieldTypeDouble"
            fields.append({"name":name,"type":kind,"alias":name,"nullable":True})
        features = []
        for oid, registry, x, y, date in ((8,"EPA0008",-70.5,42.25,1700000000123),(21,"EPA0021",-71.5,41.25,1700000000456)):
            attrs = {field["name"]: f"value-{oid}" for field in fields}
            attrs.update({"OBJECTID":oid,"REGISTRY_ID":registry,"LATITUDE83":y,"LONGITUDE83":x,
                          "ACCURACY_VALUE":3,"CREATE_DATE":date,"UPDATE_DATE":date+1,"LAST_REPORTED_DATE":date+2})
            features.append({"attributes":attrs,"geometry":{"x":x,"y":y}})
        layer = {"name":tri.LAYER_NAME,"geometryType":"esriGeometryPoint","objectIdField":"OBJECTID","hasZ":False,"hasM":False,
                 "extent":{"spatialReference":{"latestWkid":4269}},
                 "advancedQueryCapabilities":{"supportsPagination":True,"supportsOrderBy":True},"fields":fields}
        service = {"serviceItemId":tri.ITEM_ID,"layers":[{"id":0,"name":tri.LAYER_NAME}]}
        portal = {"id":tri.ITEM_ID,"owner":"EPA_GEO","title":"EPA Facility Registry Service - Toxic Release Inventory (TRI) Sites",
                  "url":tri.SERVICE_URL,"access":"public",
                  "description":"maintained by the EPA FRS Support team and based on data from the EPA's national environmental program for the Toxic Release Inventory. Geographic Coverage: United States, including territories and tribal areas."}
        docs = {
            "portal_item":portal,"portal_data":{},"service_before":service,"service_after":service,
            "layer_before":layer,"layer_after":layer,
            "count_before":{"count":2},"count_after":{"count":2},
            "ids_seed":{"objectIds":[8,21]},"ids_before":{"objectIds":[8,21]},"ids_after":{"objectIds":[8,21]},
            "page_000":{"spatialReference":{"latestWkid":4269},"features":features},
        }
        _mode, plan = tri._page_plan([8,21], layer)
        return {name:(json.dumps(doc).encode(),({"url":plan[0][1]} if name.startswith("page_") else {}))
                for name,doc in docs.items()}

    def _schema_pin(self, captures):
        doc = json.loads(captures["layer_before"][0])
        digest = tri._sha(tri._canonical([tri._plain_field(field) for field in doc["fields"]]))
        return patch.object(tri, "EXPECTED_SOURCE_SCHEMA_SHA256", digest)

    def test_complete_exact_roster_and_native_values_build_unapproved_candidate(self):
        captures = self._capture_fixture()
        with self._schema_pin(captures):
            layer, fields, features, audit = tri._validate(captures)
            body = tri._parquet_bytes(fields, features)
        self.assertEqual(layer["name"], "FRS_INTERESTS_TRI")
        self.assertEqual(audit["source_feature_count"], 2)
        self.assertTrue(audit["source_population_complete"])
        self.assertEqual(audit["source_crs_wkid"], 4269)
        table = pq.read_table(pa.BufferReader(body))
        self.assertEqual(table.column("REGISTRY_ID").to_pylist(), ["EPA0008", "EPA0021"])
        self.assertEqual(table.column("CREATE_DATE").to_pylist(), [1700000000123, 1700000000456])
        points = [struct.unpack("<BIdd", value)[2:] for value in table.column("geometry").to_pylist()]
        self.assertEqual(points, [(-70.5,42.25),(-71.5,41.25)])
        self.assertEqual(table.schema.field("CREATE_DATE").type, pa.int64())
        self.assertTrue(all(not mapping["approved"] for mapping in audit["proposed_physical_field_mappings"]))

    def test_rejects_roster_change_before_candidate_construction(self):
        captures = self._capture_fixture()
        changed = json.loads(captures["ids_after"][0])
        changed["objectIds"] = [8,22]
        captures["ids_after"] = (json.dumps(changed).encode(), {})
        with self._schema_pin(captures), self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
            tri._validate(captures)

    def test_retains_source_null_geometry_without_dropping_its_record(self):
        captures = self._capture_fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][1]["geometry"] = None
        captures["page_000"] = (json.dumps(page).encode(), captures["page_000"][1])
        with self._schema_pin(captures):
            _layer, fields, features, audit = tri._validate(captures)
            body = tri._parquet_bytes(fields, features)
        self.assertEqual(audit["source_feature_count"], 2)
        self.assertEqual(audit["source_null_geometry_count"], 1)
        self.assertEqual(len(features), 2)
        self.assertIsNone(features[1]["geometry"])
        self.assertGreater(len(body), 0)

    def test_rejects_source_schema_drift(self):
        captures = self._capture_fixture()
        doc = json.loads(captures["layer_before"][0])
        doc["fields"][1]["alias"] = "Changed field alias"
        captures["layer_before"] = (json.dumps(doc).encode(), {})
        with self.assertRaisesRegex(ValueError, "source_schema_drift"):
            tri._validate(captures)

    def test_uses_short_ordered_offset_get_pages_when_supported(self):
        _mode, plan = tri._page_plan([1,2,3], {"advancedQueryCapabilities":{
            "supportsPagination":True,"supportsOrderBy":True}})
        name,url,expected_ids = plan[0]
        params = parse_qs(urlsplit(url).query)
        self.assertEqual(name, "page_000")
        self.assertEqual(expected_ids, [1,2,3])
        self.assertEqual(params["resultOffset"], ["0"])
        self.assertEqual(params["resultRecordCount"], [str(tri.PAGE_SIZE)])
        self.assertNotIn("objectIds", params)
        self.assertLess(len(url), tri.MAX_OBJECT_ID_URL_LENGTH)

    def test_object_id_fallback_chunks_keep_every_id_under_url_limit(self):
        ids = list(range(1,5001))
        chunks = tri._object_id_chunks(ids)
        self.assertEqual([value for chunk in chunks for value in chunk], ids)
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            params = {"objectIds":",".join(map(str,chunk)),"outFields":"*","returnGeometry":"true",
                "returnZ":"true","returnM":"true","outSR":"4269","orderByFields":"OBJECTID ASC","f":"json"}
            self.assertLess(len(tri._query_url(**params)), tri.MAX_OBJECT_ID_URL_LENGTH)

    def test_small_capture_seals_and_reuses_candidate(self):
        captures = self._capture_fixture()
        fixture_by_url = {}
        for name, url in tri._source_metadata_urls():
            fixture_by_url[url] = captures[name][0]
        for name, url, _ids in tri._page_plan([8,21], json.loads(captures["layer_before"][0]))[1]:
            fixture_by_url[url] = captures[name][0]
        for name, url in tri._source_after_urls():
            fixture_by_url[url] = captures[name][0]
        fixture_by_url[tri._query_url(where="1=1", returnIdsOnly="true", f="json")] = captures["ids_seed"][0]

        def fake_get(_client, url, capture_root, name, *, max_bytes):
            body = fixture_by_url[url]
            receipt = {"method":"GET", "url":url, "final_url":url,
                "observed_at":"2026-01-01T00:00:00+00:00", "status":200,
                "complete_body":True, "bytes":len(body), "sha256":tri._sha(body),
                "wire_bytes":len(body), "wire_sha256":tri._sha(body), "content_encoding":"identity",
                "content_length":len(body), "error_code":None}
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.body").write_bytes(body)
            return body, receipt

        layer = json.loads(captures["layer_before"][0])
        schema_sha = tri._sha(tri._canonical([tri._plain_field(f) for f in layer["fields"]]))
        with tempfile.TemporaryDirectory() as tmp, self._schema_pin(captures), \
                patch.object(tri, "capture_get", side_effect=fake_get):
            storage = tri.StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            result = tri.capture_epa_frs_tri(storage, version="fixture-v1", capture_directory=tmp)
            self.assertEqual(result["feature_count"], 2)
            self.assertFalse(result["release_accepted"])
            prefix = candidate_prefix(tri.DATASET_SLUG, tri.FILE_SLUG, result["version"])
            state_key = f"_construction-state/{tri.DATASET_SLUG}/{result['version']}.json"
            identity = tri._code_identity()
            bundle = json.loads(load_first_construction(
                storage, state_key, result["version"], tri._sha(tri._canonical(identity))))
            payload = bundle["payload"]
            self.assertEqual(payload["source_schema_sha256"], schema_sha)
            reused = verify_cached_candidate(storage, Path(__file__).resolve().parents[1],
                state_key, prefix, result["version"], tri._sha(tri._canonical(identity)), identity,
                payload["source_record_sha256"], schema_sha, payload["objects"], payload["capture_receipts"])
            self.assertTrue(reused["reused"])

            self.assertTrue(verify_cached_candidate(storage, Path(__file__).resolve().parents[1],
                state_key, prefix, result["version"], tri._sha(tri._canonical(identity)), identity,
                payload["source_record_sha256"], schema_sha, payload["objects"], payload["capture_receipts"])["reused"])


if __name__ == "__main__":
    unittest.main()

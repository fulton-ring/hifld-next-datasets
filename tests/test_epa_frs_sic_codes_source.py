import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.sources import epa_frs_sic as sic
from dagster_hifld.sources.epa_frs_common import candidate_prefix


class EpaFrsSicCodesSourceTests(unittest.TestCase):
    def _fields(self):
        fields = []
        for name, kind, nullable, length in sic.FIELDS:
            fields.append({"name": name, "type": kind, "alias": name,
                           "length": length, "nullable": nullable})
        return fields

    def _features(self):
        rows = []
        for oid, acrnm, sys_id, code, indicator in ((7, "ICIS", "46748", "4213", "SECONDARY"),
                                                    (11, "NPDES", "GAR372473", "1521", "PRIMARY")):
            attrs = {"OBJECTID": oid, "PGM_SYS_ACRNM": acrnm, "PGM_SYS_ID": sys_id,
                     "SIC_CODE": code, "PRIMARY_INDICATOR": indicator,
                     "KEY_FIELD": f"{acrnm}{sys_id}"}
            rows.append({"attributes": attrs, "geometry": None})
        return rows

    def _docs(self, fields, features):
        ids = [row["attributes"]["OBJECTID"] for row in features]
        table = {"name": sic.TABLE_NAME, "type": "Table", "fields": fields,
                 "relationships": [], "maxRecordCount": 5000}
        service = {"tables": [{"id": sic.TABLE_ID, "name": sic.TABLE_NAME}],
                   "description": "FRS interest map service"}
        return {
            "service": service, "service_after": service,
            "table_before": table, "table_after": table,
            "count": {"count": len(ids)},
            "ids": {"objectIds": ids},
            "pages": features,
        }

    def _url_body(self, url, docs):
        path, _, raw_query = url.partition("?")
        params = dict(pair.split("=", 1) for pair in raw_query.split("&")) if raw_query else {}
        if path == sic.SERVICE_URL:
            return json.dumps(docs["service"]).encode()
        if path == sic.TABLE_URL:
            return json.dumps(docs["table_before"]).encode()
        if path != sic.TABLE_URL + "/query":
            raise AssertionError("unexpected fixture URL " + url)
        if params.get("returnCountOnly") == "true":
            return json.dumps(docs["count"]).encode()
        if params.get("returnIdsOnly") == "true":
            return json.dumps(docs["ids"]).encode()
        offset = int(params["resultOffset"])
        page_size = int(params["resultRecordCount"])
        return json.dumps({"features": docs["pages"][offset:offset + page_size]}).encode()

    def _fake_get(self, docs):
        def fake_get(_client, url, capture_root, name, *, max_bytes):
            body = self._url_body(url, docs)
            receipt = {"method": "GET", "url": url, "final_url": url,
                       "observed_at": "2026-10-21T00:00:00+00:00", "status": 200,
                       "complete_body": True, "bytes": len(body), "sha256": sic._sha(body),
                       "wire_bytes": len(body), "wire_sha256": sic._sha(body),
                       "content_encoding": "identity", "content_length": len(body),
                       "error_code": None}
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.body").write_bytes(body)
            return body, receipt
        return fake_get

    def _run_capture(self, tmp, docs, get=None):
        with patch.object(sic, "EXPECTED_ROWS", len(docs["pages"])), \
                patch.object(sic, "capture_get", side_effect=get or self._fake_get(docs)):
            storage = sic.StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            return sic.capture_epa_frs_sic(storage, version="fixture-v1", capture_directory=tmp)

    def test_complete_exact_roster_builds_unapproved_nonspatial_candidate(self):
        docs = self._docs(self._fields(), self._features())
        with tempfile.TemporaryDirectory() as tmp:
            result = self._run_capture(tmp, docs)
            self.assertEqual(result["row_count"], 2)
            self.assertTrue(result["source_population_complete"])
            self.assertFalse(result["release_accepted"])
            prefix = candidate_prefix(sic.DATASET_SLUG, sic.FILE_SLUG, result["version"])
            table = pq.read_table(Path(tmp) / f"{prefix}/{sic.NATIVE_PATH}")
            self.assertEqual(table.schema.names, list(sic.FIELD_NAMES))
            self.assertEqual(table.column("SIC_CODE").to_pylist(), ["4213", "1521"])
            self.assertEqual(table.column("KEY_FIELD").to_pylist(), ["ICIS46748", "NPDESGAR372473"])
            self.assertEqual(table.schema.field("OBJECTID").type, pa.int64())
            self.assertEqual(table.schema.metadata[b"hifld:geometry"], b"absent")
            self.assertEqual(table.schema.field("KEY_FIELD").nullable, True)
            self.assertEqual(table.schema.field("SIC_CODE").nullable, False)
            audit = json.loads((Path(tmp) / f"{prefix}/metadata/source_audit.json").read_bytes())
            self.assertTrue(audit["source_population_complete"])
            self.assertFalse(audit["release_accepted"])
            self.assertFalse(audit["freshness_accepted"])
            self.assertEqual(audit["source_url"], sic.TABLE_URL)
            self.assertEqual(audit["equivalent_official_distribution"]["portal_item_id"],
                             sic.AGOL_ITEM_ID)

    def test_rejects_id_roster_drift(self):
        docs = self._docs(self._fields(), self._features())
        docs["ids"] = {"objectIds": [7, 11, 13]}
        with tempfile.TemporaryDirectory() as tmp, \
                self.assertRaisesRegex(Exception, "source_count_and_id_roster_disagree"):
            self._run_capture(tmp, docs)

    def test_rejects_source_schema_drift(self):
        fields = self._fields()
        fields[3]["length"] = 8
        docs = self._docs(fields, self._features())
        with tempfile.TemporaryDirectory() as tmp, \
                self.assertRaisesRegex(Exception, "source_table_schema_changed"):
            self._run_capture(tmp, docs)

    def test_quarantines_incomplete_page_capture(self):
        docs = self._docs(self._fields(), self._features())

        def broken_get(_client, url, capture_root, name, *, max_bytes):
            if "resultOffset=0" in url:
                body = json.dumps({"features": docs["pages"][:1]}).encode()
                receipt = {"method": "GET", "url": url, "final_url": url,
                           "observed_at": "2026-10-21T00:00:00+00:00", "status": 200,
                           "complete_body": True, "bytes": len(body), "sha256": sic._sha(body),
                           "wire_bytes": len(body), "wire_sha256": sic._sha(body),
                           "content_encoding": "identity", "content_length": len(body),
                           "error_code": None}
                (capture_root / f"{name}.wire").write_bytes(body)
                (capture_root / f"{name}.body").write_bytes(body)
                return body, receipt
            return self._fake_get(docs)(_client, url, capture_root, name, max_bytes=max_bytes)

        with tempfile.TemporaryDirectory() as tmp, \
                self.assertRaisesRegex(Exception, "source_page_row_count_incomplete"):
            self._run_capture(tmp, docs, get=broken_get)
            quarantined = list((Path(tmp) / "_quarantine" / sic.DATASET_SLUG).iterdir())
            self.assertEqual(len(quarantined), 1)
            failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
            self.assertEqual(failure["reason_code"], "EpaFrsSicCaptureError")
            self.assertTrue((quarantined[0] / "raw" / "page_000.body").exists())

    def test_second_identical_capture_reuses_sealed_candidate(self):
        docs = self._docs(self._fields(), self._features())
        with tempfile.TemporaryDirectory() as tmp:
            first = self._run_capture(tmp, docs)
            second = self._run_capture(tmp, docs)
            self.assertTrue(second["reused"])
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(first["source_record_sha256"], second["source_record_sha256"])
            state_key = f"_construction-state/{sic.DATASET_SLUG}/{first['version']}.json"
            bundle = json.loads(sic.load_first_construction(_storage(tmp), state_key,
                first["version"], sic._sha(sic._canonical(
                    sic.processing_identity(Path(sic.__file__).parents[3])))))
            self.assertEqual(bundle["payload"]["source_record_sha256"], first["source_record_sha256"])
            self.assertIn("raw/service.body", bundle["payload"]["objects"])


def _storage(tmp):
    return sic.StagingStorageResource(local_dir=tmp, use_local=True, backend="local")


if __name__ == "__main__":
    unittest.main()

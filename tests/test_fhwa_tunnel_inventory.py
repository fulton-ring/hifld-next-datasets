"""Tiny, bounded tests for FHWA NTI source-contract and release blocking."""
from __future__ import annotations

import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.assets.fhwa_tunnel_inventory import fhwa_national_tunnel_inventory_release_gate
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import fhwa_tunnel_inventory as source


class _EmptyClient:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class FhwaTunnelInventoryTests(unittest.TestCase):
    def _docs(self):
        fields = [
            {"name":"OBJECTID","type":"esriFieldTypeOID","alias":"OBJECTID","nullable":False,"domain":None},
            {"name":"tunnel_name_i2","type":"esriFieldTypeString","alias":"tunnel_name_i2","length":10,"nullable":True,"domain":None},
        ]
        service = {"serviceItemId":source.ITEM_ID,"isView":True,"description":(
            "FHWA Federal Highway Administration (FHWA); compiled on September 02, 2025; "
            "published on August 26, 2025"),"layers":[{"id":0,"name":source.LAYER_NAME,"geometryType":"esriGeometryPoint"}]}
        layer = {"id":0,"name":source.LAYER_NAME,"geometryType":"esriGeometryPoint","objectIdField":"OBJECTID",
            "hasZ":False,"hasM":False,"extent":{"spatialReference":{"latestWkid":4326}},"fields":fields}
        portal = {"id":source.ITEM_ID,"owner":"USDOT_BTS","title":"National Tunnel Inventory",
            "url":source.SERVICE_URL,"access":"public","modified":1}
        features = [
            {"attributes":{"OBJECTID":1,"tunnel_name_i2":"Alpha"},"geometry":{"x":-100.0,"y":40.0}},
            {"attributes":{"OBJECTID":2,"tunnel_name_i2":None},"geometry":{"x":-101.0,"y":41.0}},
        ]
        ids = [1,2]
        geometry_rows = [[1,-100.0,40.0],[2,-101.0,41.0]]
        count = {"count":2}
        id_doc = {"objectIdFieldName":"OBJECTID","objectIds":ids}
        page = {"geometryType":"esriGeometryPoint","spatialReference":{"latestWkid":4326},
                "fields":fields,"features":features}
        docs = {"portal_item":portal,"service_before":service,"layer_before":layer,
                "count_before":count,"ids_before":id_doc,"page_000":page,
                "layer_after":layer,"service_after":service,"count_after":count,"ids_after":id_doc}
        return docs, fields, features, ids, geometry_rows

    def _pins(self):
        docs, fields, features, ids, geometry_rows = self._docs()
        del docs
        return {
            "EXPECTED_COUNT": 2,
            "EXPECTED_ID_SHA256": source._sha(source._canonical(ids)),
            "EXPECTED_RECORD_SHA256": source._sha(source._canonical(features)),
            "EXPECTED_GEOMETRY_SHA256": source._sha(source._canonical(geometry_rows)),
            "EXPECTED_SCHEMA_SHA256": source._sha(source._canonical(
                [source._plain_field(row) for row in fields])),
        }

    def _captures(self):
        docs, _fields, _features, _ids, _geometry_rows = self._docs()
        with patch.multiple(source, **self._pins()):
            return {name:(source._canonical(doc),{}) for name,doc in docs.items()}

    def _fake_capture_get(self, body_for):
        """Serve ten fixture inputs per acquisition with per-call receipt timestamps."""
        observed, calls = [], []
        def fake(_client, url, capture_root, name, *, max_bytes):
            calls.append(name)
            body = body_for(name)
            digest = source._sha(body)
            receipt = {"method": "GET", "url": url, "final_url": url,
                       "observed_at": f"2026-01-01T00:{len(calls):02d}:00+00:00",
                       "status": 200, "complete_body": True, "wire_bytes": len(body),
                       "wire_sha256": digest, "bytes": len(body), "sha256": digest,
                       "content_encoding": None, "content_length": str(len(body)),
                       "error_code": None}
            (capture_root/f"{name}.body").write_bytes(body)
            (capture_root/f"{name}.wire").write_bytes(body)
            (capture_root/f"{name}.receipt.json").write_bytes(source._canonical(receipt)+b"\n")
            observed.append(receipt["observed_at"])
            return body, receipt
        return fake, observed

    def test_source_contract_accepts_complete_native_ids_and_point_geometry(self):
        captures=self._captures()
        docs={name:json.loads(body) for name,(body,_) in captures.items()}
        fields=[source._plain_field(row) for row in docs["layer_before"]["fields"]]
        features=docs["page_000"]["features"]
        geometries=sorted([[f["attributes"]["OBJECTID"],f["geometry"]["x"],f["geometry"]["y"]] for f in features])
        with patch.object(source, "EXPECTED_COUNT", 2), \
             patch.object(source, "EXPECTED_ID_SHA256", source._sha(source._canonical([1,2]))), \
             patch.object(source, "EXPECTED_RECORD_SHA256", source._sha(source._canonical(features))), \
             patch.object(source, "EXPECTED_GEOMETRY_SHA256", source._sha(source._canonical(geometries))), \
             patch.object(source, "EXPECTED_SCHEMA_SHA256", source._sha(source._canonical(fields))):
            layer,fields,extracted=source._validate(captures)
        self.assertEqual(layer["name"],source.LAYER_NAME)
        self.assertEqual(len(fields),2)
        self.assertEqual([row["attributes"]["OBJECTID"] for row in extracted["features"]],[1,2])
        self.assertEqual(extracted["audit"]["source_crs_epsg"],4326)

    def test_drifted_integer_value_is_rejected_without_coercion(self):
        captures=self._captures()
        docs={name:json.loads(body) for name,(body,_) in captures.items()}
        docs["page_000"]["features"][0]["attributes"]["OBJECTID"]="1"
        captures={name:(source._canonical(doc),{}) for name,doc in docs.items()}
        with patch.object(source, "EXPECTED_COUNT", 2), \
             patch.object(source, "EXPECTED_ID_SHA256", source._sha(source._canonical([1,2]))), \
             patch.object(source, "EXPECTED_SCHEMA_SHA256", source._sha(source._canonical([
                 source._plain_field(row) for row in docs["layer_before"]["fields"]]))):
            with self.assertRaisesRegex(ValueError,"source_objectid_physical_type_drift"):
                source._validate(captures)

    def test_release_gate_stays_closed_when_population_is_complete(self):
        result=fhwa_national_tunnel_inventory_release_gate({"feature_count":580})
        self.assertFalse(result.passed)
        self.assertFalse(result.metadata["source_edition_accepted"].value)
        self.assertFalse(result.metadata["release_accepted"].value)

    def _capture(self, storage, root, captures_dir, body_for, fake):
        with patch.multiple(source, **self._pins()), \
             patch.object(source, "capture_get", side_effect=fake), \
             patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: _EmptyClient()):
            return source.capture_fhwa_tunnel_inventory(storage, version="vfixture",
                                                        capture_directory=str(root/captures_dir))

    def test_second_fresh_acquisition_with_new_receipts_reuses_candidate(self):
        docs, _fields, _features, _ids, _geometry_rows = self._docs()
        bodies = {name: source._canonical(doc) for name, doc in docs.items()}
        fake, observed = self._fake_capture_get(lambda name: bodies[name])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = StagingStorageResource(use_local=True, local_dir=str(root/"staging"), prefix="")
            first = self._capture(storage, root, "captures-a", bodies, fake)
            self.assertFalse(first["reused"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            original_receipt = storage.read_key(f"{prefix}/raw/portal_item.receipt.json")
            original_manifest = storage.read_key(f"{prefix}/metadata/source_manifest.json")
            original_parquet = storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet")
            second = self._capture(storage, root, "captures-b", bodies, fake)
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])
            self.assertEqual(second["feature_count"], first["feature_count"])
            self.assertNotEqual(observed[0], observed[len(bodies)])
            self.assertEqual(json.loads(original_receipt)["observed_at"], observed[0])
            # The original immutable provenance survives; fresh receipts are not restamped.
            self.assertEqual(storage.read_key(f"{prefix}/raw/portal_item.receipt.json"), original_receipt)
            self.assertEqual(storage.read_key(f"{prefix}/metadata/source_manifest.json"), original_manifest)
            self.assertEqual(storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet"), original_parquet)
            self.assertEqual(sorted(observed), sorted(set(observed)), "every receipt carries a distinct timestamp")

    def test_reacquisition_with_changed_raw_capture_evidence_fails_closed(self):
        docs, _fields, _features, _ids, _geometry_rows = self._docs()
        bodies = {name: source._canonical(doc) for name, doc in docs.items()}
        restamped = {name: json.dumps(json.loads(body), indent=2).encode() for name, body in bodies.items()}
        holder = {"bodies": bodies}
        fake, _observed = self._fake_capture_get(lambda name: holder["bodies"][name])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = StagingStorageResource(use_local=True, local_dir=str(root/"staging"), prefix="")
            first = self._capture(storage, root, "captures-a", bodies, fake)
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            original_receipt = storage.read_key(f"{prefix}/raw/portal_item.receipt.json")
            holder["bodies"] = restamped
            with self.assertRaisesRegex(ValueError, "cached_source_evidence_changed"):
                self._capture(storage, root, "captures-b", restamped, fake)
            self.assertEqual(storage.read_key(f"{prefix}/raw/portal_item.receipt.json"), original_receipt)
            self.assertEqual(storage.read_key(f"{prefix}/raw/portal_item.body"), bodies["portal_item"])
            self.assertTrue(storage.object_snapshot(
                f"_construction-state/{source.DATASET_SLUG}/{first['version']}.json"))


if __name__ == "__main__":
    unittest.main()

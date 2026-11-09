"""Focused contract checks for the FUDS Projects point source adapter."""
from __future__ import annotations

import hashlib
import json
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq

from dagster_hifld.sources import fuds_projects_points as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _fixture():
    fields = [
        {"name":"OBJECTID","type":"esriFieldTypeOID","alias":"OBJECTID","domain":None},
        {"name":"NAME","type":"esriFieldTypeString","alias":"Name","length":40,"domain":None},
        {"name":"SHAPE","type":"esriFieldTypeGeometry","alias":"SHAPE","domain":None},
        {"name":"SE_ANNO_CAD_DATA","type":"esriFieldTypeBlob","alias":"SE_ANNO_CAD_DATA","domain":None},
    ]
    layer = {"id":2,"name":"FUDS Project Point","description":"The physical location of a specific project.",
             "geometryType":"esriGeometryPoint","maxRecordCount":2000,
             "advancedQueryCapabilities":{"supportsPagination":True},
             "spatialReference":{"wkid":4326,"latestWkid":4326},"fields":fields}
    service = {"mapName":"FUDS Data","copyrightText":"US Army Corps of Engineers",
               "layers":[{"id":2,"name":"FUDS Project Point","type":"Feature Layer"}]}
    features = [
        {"attributes":{"OBJECTID":10,"NAME":"North","SE_ANNO_CAD_DATA":None},"geometry":{"x":-91.5,"y":40.25}},
        {"attributes":{"OBJECTID":11,"NAME":"No geometry","SE_ANNO_CAD_DATA":None},"geometry":None},
        {"attributes":{"OBJECTID":12,"NAME":"South","SE_ANNO_CAD_DATA":None},"geometry":{"x":-92.0,"y":39.0}},
    ]
    ids = [10,11,12]
    schema = [{k:f[k] for k in ("name","type","alias","length","nullable","domain") if k in f} for f in fields]
    geom = [[10,-91.5,40.25],[11,None,None],[12,-92.0,39.0]]
    page0 = {"features":features[:2],"spatialReference":{"wkid":4326,"latestWkid":4326},"exceededTransferLimit":True}
    page1 = {"features":features[2:],"spatialReference":{"wkid":4326,"latestWkid":4326}}
    docs = {"service_before":service,"service_after":service,"layer_before":layer,"layer_after":layer,
            "count_before":{"count":3},"count_after":{"count":3},
            "ids_before":{"objectIdFieldName":"OBJECTID","objectIds":ids},
            "ids_after":{"objectIdFieldName":"OBJECTID","objectIds":ids},
            "ids_seed":{"objectIdFieldName":"OBJECTID","objectIds":ids},"page_000":page0,"page_001":page1}
    captures = {name:(json.dumps(doc,separators=(",", ":"),ensure_ascii=False).encode(),{"status":200}) for name,doc in docs.items()}
    return fields, features, captures, schema, geom


class FudsProjectsPointsTests(unittest.TestCase):
    def test_id_roster_response_order_is_not_population_drift(self):
        _, _, captures, schema, geom = _fixture()
        for name, ids in (("ids_seed", [12,10,11]), ("ids_before", [11,12,10]), ("ids_after", [10,11,12])):
            doc = json.loads(captures[name][0]); doc["objectIds"] = ids
            captures[name] = (json.dumps(doc,separators=(",", ":")).encode(), captures[name][1])
        features = [json.loads(captures[f"page_{i:03d}"][0])["features"] for i in range(2)]
        features = [row for page in features for row in page]
        record_sha = hashlib.sha256(_canonical(features)).hexdigest()
        with patch.multiple(source, EXPECTED_COUNT=3, PAGE_SIZE=2,
                            EXPECTED_LAYER_ROSTER=[(2,"FUDS Project Point","Feature Layer")],
                            EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10,11,12])).hexdigest(),
                            EXPECTED_RECORD_SHA256=record_sha,
                            EXPECTED_GEOMETRY_SHA256=hashlib.sha256(_canonical(geom)).hexdigest(),
                            EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest()):
            _, _, extracted = source._validate_captures(captures)
        self.assertEqual(extracted["ids"], [10,11,12])

    def test_z_or_m_layer_is_rejected_before_2d_candidate(self):
        for dimension in ("hasZ", "hasM"):
            with self.subTest(dimension=dimension):
                _, _, captures, schema, _ = _fixture()
                for name in ("layer_before", "layer_after"):
                    layer = json.loads(captures[name][0]); layer[dimension] = True
                    captures[name] = (json.dumps(layer,separators=(",", ":")).encode(), captures[name][1])
                with patch.multiple(source, EXPECTED_COUNT=3, PAGE_SIZE=2,
                                    EXPECTED_LAYER_ROSTER=[(2,"FUDS Project Point","Feature Layer")],
                                    EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10,11,12])).hexdigest(),
                                    EXPECTED_RECORD_SHA256="0"*64, EXPECTED_GEOMETRY_SHA256="0"*64,
                                    EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest()):
                    with self.assertRaisesRegex(ValueError,"source_z_or_m_geometry_unsupported"):
                        source._validate_captures(captures)

    def test_complete_capture_and_native_parquet_preserve_nulls_and_source_shape(self):
        fields, features, captures, schema, geom = _fixture()
        record_sha = hashlib.sha256(_canonical(features)).hexdigest()
        schema_sha = hashlib.sha256(_canonical(schema)).hexdigest()
        geom_sha = hashlib.sha256(_canonical(geom)).hexdigest()
        with patch.multiple(source, EXPECTED_COUNT=3, PAGE_SIZE=2, EXPECTED_LAYER_ROSTER=[(2,"FUDS Project Point","Feature Layer")],
                            EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10,11,12])).hexdigest(),
                            EXPECTED_RECORD_SHA256=record_sha,
                            EXPECTED_GEOMETRY_SHA256=geom_sha,
                            EXPECTED_SCHEMA_SHA256=schema_sha):
            _, original_fields, extracted = source._validate_captures(captures)
            self.assertEqual(extracted["audit"]["source_feature_count"],3)
            self.assertEqual(extracted["audit"]["null_geometry_count"],1)
            body = source._parquet_bytes(original_fields, extracted["features"])
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body); stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows,3)
        self.assertEqual(table.column("OBJECTID").to_pylist(),[10,11,12])
        self.assertEqual(table.column("SE_ANNO_CAD_DATA").type.__str__(),"binary")
        self.assertEqual(table.column("SE_ANNO_CAD_DATA").null_count,3)
        self.assertEqual(table.column("SHAPE").null_count,1)

    def test_final_page_cannot_claim_transfer_truncation(self):
        _, _, captures, _, _ = _fixture()
        final = json.loads(captures["page_001"][0])
        final["exceededTransferLimit"] = True
        captures["page_001"] = (json.dumps(final,separators=(",", ":")).encode(),{"status":200})
        with patch.multiple(source, EXPECTED_COUNT=3, PAGE_SIZE=2, EXPECTED_LAYER_ROSTER=[(2,"FUDS Project Point","Feature Layer")],
                            EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10,11,12])).hexdigest(),
                            EXPECTED_RECORD_SHA256="0"*64, EXPECTED_GEOMETRY_SHA256="0"*64,
                            EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(_fixture()[3])).hexdigest()):
            with self.assertRaisesRegex(ValueError,"source_page_unexpected_exceeded_transfer_limit"):
                source._validate_captures(captures)

    def test_unexpected_field_or_record_change_is_quarantined_by_contract(self):
        _, _, captures, schema, _ = _fixture()
        layer = json.loads(captures["layer_before"][0]); layer["fields"].append({"name":"SURPRISE","type":"esriFieldTypeString"})
        captures["layer_before"] = (json.dumps(layer,separators=(",", ":")).encode(),{"status":200})
        with patch.multiple(source, EXPECTED_COUNT=3, PAGE_SIZE=2, EXPECTED_LAYER_ROSTER=[(2,"FUDS Project Point","Feature Layer")],
                            EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10,11,12])).hexdigest(),
                            EXPECTED_RECORD_SHA256="0"*64, EXPECTED_GEOMETRY_SHA256="0"*64,
                            EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest()):
            with self.assertRaisesRegex(ValueError,"source_schema_contract_drift"):
                source._validate_captures(captures)

    def test_missing_page_id_is_not_accepted(self):
        _, _, captures, schema, _ = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"] = page["features"][:1]
        captures["page_000"] = (json.dumps(page,separators=(",", ":")).encode(),{"status":200})
        with patch.multiple(source, EXPECTED_COUNT=3, PAGE_SIZE=2, EXPECTED_LAYER_ROSTER=[(2,"FUDS Project Point","Feature Layer")],
                            EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10,11,12])).hexdigest(),
                            EXPECTED_RECORD_SHA256="0"*64, EXPECTED_GEOMETRY_SHA256="0"*64,
                            EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest()):
            with self.assertRaisesRegex(ValueError,"source_page_id_slice_incomplete_or_reordered"):
                source._validate_captures(captures)

    def test_record_value_change_is_detected(self):
        _, features, captures, schema, geom = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["NAME"] = "Changed"
        captures["page_000"] = (json.dumps(page,separators=(",", ":")).encode(),{"status":200})
        changed = page["features"] + json.loads(captures["page_001"][0])["features"]
        with patch.multiple(source, EXPECTED_COUNT=3, PAGE_SIZE=2, EXPECTED_LAYER_ROSTER=[(2,"FUDS Project Point","Feature Layer")],
                            EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10,11,12])).hexdigest(),
                            EXPECTED_RECORD_SHA256=hashlib.sha256(_canonical(features)).hexdigest(),
                            EXPECTED_GEOMETRY_SHA256=hashlib.sha256(_canonical(geom)).hexdigest(),
                            EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest()):
            with self.assertRaisesRegex(ValueError,"source_record_value_drift"):
                source._validate_captures(captures)

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch

        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        fields, features, fixture_captures, schema, geom = _fixture()
        record_sha = hashlib.sha256(_canonical(features)).hexdigest()
        schema_sha = hashlib.sha256(_canonical(schema)).hexdigest()
        geometry_sha = hashlib.sha256(_canonical(geom)).hexdigest()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            body = bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method":"GET","url":url,"final_url":url,"observed_at":"2026-10-19T00:00:00+00:00",
                       "status":200,"complete_body":True,"wire_bytes":len(body),"wire_sha256":digest,
                       "bytes":len(body),"sha256":digest,"content_encoding":None,"content_length":str(len(body)),"error_code":None}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt)+b"\n")
            return body, receipt

        class EmptyClient:
            def __enter__(self): return self
            def __exit__(self, *_args): return False

        with tempfile.TemporaryDirectory(prefix="fuds-projects-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, EXPECTED_COUNT=3, PAGE_SIZE=2,
                                EXPECTED_LAYER_ROSTER=[(2,"FUDS Project Point","Feature Layer")],
                                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10,11,12])).hexdigest(),
                                EXPECTED_RECORD_SHA256=record_sha,
                                EXPECTED_GEOMETRY_SHA256=geometry_sha,
                                EXPECTED_SCHEMA_SHA256=schema_sha), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_fuds_projects_points(storage, version="vfixture", capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"],3)
                self.assertEqual(candidate["source_null_geometry_count"],1)
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                for name, body in bodies.items():
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.body"),body)
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.wire"),body)
                snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG, candidate["version"])
                manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG, candidate["version"]).metadata
                summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                    candidate["version"], source.DATASET_SLUG,
                                                    source_metadata=manifest)
                self.assertEqual(summary.quality_manifest["null_geometry_count"],1)
                self.assertFalse(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG, file_slug=source.FILE_SLUG,
                                       version=candidate["version"], quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_fuds_projects_points(storage, version="vfixture", capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"],candidate["version"])

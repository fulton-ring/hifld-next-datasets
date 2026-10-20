"""Focused contract and repeat-acquisition tests for the BOEM OCS adapter."""
from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import boem_ocs_diagrams as boem
from dagster_hifld.sources import epa_frs_common


def _body(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _fixture():
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
        {"name": "RGN_CODE", "type": "esriFieldTypeString", "nullable": False, "length": 1},
        {"name": "PROT_NUM", "type": "esriFieldTypeString", "nullable": False, "length": 7},
        {"name": "PROT_NAME", "type": "esriFieldTypeString", "nullable": False, "length": 60},
        {"name": "PROT_APRV_DATE", "type": "esriFieldTypeDate", "nullable": True, "length": 8},
        {"name": "REMARK", "type": "esriFieldTypeString", "nullable": True, "length": 2000},
        {"name": "FED_REG_DATE", "type": "esriFieldTypeDate", "nullable": True, "length": 8},
        {"name": "Shape__Area", "type": "esriFieldTypeDouble", "nullable": True},
        {"name": "Shape__Length", "type": "esriFieldTypeDouble", "nullable": True},
    ]
    ids = [1, 2, 3, 4]
    regions = ["A", "G", "P", "Y"]
    features = []
    for oid, region in zip(ids, regions):
        x = oid * 1000
        features.append({
            "attributes": {"OBJECTID": oid, "RGN_CODE": region, "PROT_NUM": f"AA{oid:02d}-01",
                           "PROT_NAME": f"Map {oid}", "PROT_APRV_DATE": None, "REMARK": None,
                           "FED_REG_DATE": None, "Shape__Area": 100.0, "Shape__Length": 40.0},
            "geometry": {"rings": [[[x, 0], [x + 10, 0], [x + 10, 10], [x, 10], [x, 0]]]},
        })
    layer = {"name": boem.LAYER_NAME, "geometryType": "esriGeometryPolygon", "hasZ": False, "hasM": False,
             "spatialReference": {"wkid": 102100, "latestWkid": 3857},
             "extent": {"spatialReference": {"wkid": 102100, "latestWkid": 3857}}, "fields": fields}
    service = {"serviceItemId": boem.ITEM_ID, "layers": [{"id": boem.LAYER_ID, "name": boem.LAYER_NAME}]}
    id_doc = {"objectIds": ids}
    count_doc = {"count": len(ids)}
    page = {"spatialReference": {"wkid": 102100, "latestWkid": 3857}, "features": features}
    docs = {
        "portal_item": {"id": boem.ITEM_ID, "owner": "BOEM_GeoSEAS", "access": "public", "url": boem.SERVICE_URL},
        "service_before": service, "service_after": service,
        "layer_before": layer, "layer_after": layer,
        "ids_before": id_doc, "ids_after": id_doc,
        "count_before": count_doc, "count_after": count_doc,
        "page_000": page,
    }
    receipt = {"status": 200, "complete_body": True, "observed_at": "2026-01-01T00:00:00Z"}
    captures = {}
    for key, value in docs.items():
        body = _body(value)
        url = (boem.ITEM_URL if key == "portal_item" else
               boem.SERVICE_URL + "?f=pjson" if key.startswith("service_") else
               boem.LAYER_URL + "?f=pjson" if key.startswith("layer_") else
               boem.LAYER_URL + "/query?where=1%3D1" if key.startswith("count_") else
               boem.LAYER_URL + "/query?returnIdsOnly=true" if key.startswith("ids_") else
               boem.LAYER_URL + "/query?objectIds=1%2C2%2C3%2C4")
        captures[key] = (body, {**receipt, "url": url, "final_url": url, "bytes": len(body),
                                "sha256": hashlib.sha256(body).hexdigest(), "wire_bytes": len(body),
                                "wire_sha256": hashlib.sha256(body).hexdigest()})
    return captures


class BoemOcsDiagramsTests(unittest.TestCase):
    def test_all_four_regions_and_native_fields_are_required(self):
        _, fields, features, audit = boem._validate(_fixture())
        self.assertEqual([field["name"] for field in fields], [field[0] for field in boem.EXPECTED_FIELDS])
        self.assertEqual(len(features), 4)
        self.assertEqual(audit["region_counts"], {"A": 1, "G": 1, "P": 1, "Y": 1})
        self.assertEqual(audit["unique_prot_num_count"], 4)

    def test_spatial_reference_alias_is_valid_but_contradictions_are_rejected(self):
        captures = _fixture()
        _, _, _, audit = boem._validate(captures)
        self.assertEqual(audit["source_crs_epsg"], 3857)

        for doc_name, path, bad_sr in (
            ("layer_before", ("spatialReference",), {"wkid": 26910, "latestWkid": 3857}),
            ("page_000", ("spatialReference",), {"wkid": 102100, "latestWkid": 4326}),
        ):
            candidate = copy.deepcopy(captures)
            doc = json.loads(candidate[doc_name][0])
            doc[path[0]] = bad_sr
            candidate[doc_name] = (_body(doc), candidate[doc_name][1])
            with self.assertRaisesRegex(ValueError, "spatial_reference_drift"):
                boem._validate(candidate)

        candidate = copy.deepcopy(captures)
        doc = json.loads(candidate["layer_before"][0])
        from pyproj import CRS
        doc["spatialReference"]["wkt"] = CRS.from_epsg(4326).to_wkt()
        candidate["layer_before"] = (_body(doc), candidate["layer_before"][1])
        with self.assertRaisesRegex(ValueError, "source_layer_spatial_reference_drift"):
            boem._validate(candidate)

        candidate = copy.deepcopy(captures)
        doc = json.loads(candidate["page_000"][0])
        doc["features"][0]["geometry"]["spatialReference"] = {"wkid": 4326}
        candidate["page_000"] = (_body(doc), candidate["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_geometry_spatial_reference_drift"):
            boem._validate(candidate)

    def test_incomplete_region_and_missing_native_id_are_rejected(self):
        captures = _fixture()
        doc = json.loads(captures["page_000"][0]); doc["features"].pop()
        captures["page_000"] = (_body(doc), captures["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
            boem._validate(captures)

        captures = _fixture(); doc = json.loads(captures["page_000"][0]); doc["features"][0]["attributes"].pop("OBJECTID")
        captures["page_000"] = (_body(doc), captures["page_000"][1])
        with self.assertRaisesRegex(ValueError, "source_feature_population_incomplete"):
            boem._validate(captures)

    def test_same_content_reuses_original_receipts_and_changed_content_versions(self):
        fixture = _fixture()
        stable_identity = {"code_commit": "a" * 40, "files": {"x.py": "b" * 64}}
        def parquet(captures, _root):
            return b"candidate:" + hashlib.sha256(captures["page_000"][0]).digest()
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            def run(observed_at, mutate=False):
                captures = copy.deepcopy(fixture)
                for _, receipt in captures.values(): receipt["observed_at"] = observed_at
                if mutate:
                    doc = json.loads(captures["page_000"][0]); doc["features"][0]["attributes"]["PROT_NAME"] = "Changed"
                    body = _body(doc); receipt = captures["page_000"][1]
                    receipt.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest(), wire_bytes=len(body), wire_sha256=hashlib.sha256(body).hexdigest())
                    captures["page_000"] = (body, receipt)
                with patch.object(boem, "_load_offline", return_value=captures), \
                     patch.object(boem, "_geoparquet", side_effect=parquet), \
                     patch.object(boem, "processing_identity", return_value=stable_identity), \
                     patch.object(boem, "validate_construction_code_identity", return_value=None), \
                     patch.object(epa_frs_common, "validate_construction_code_identity", return_value=None):
                    return boem.capture_boem_ocs_diagrams(storage, offline_capture_directory=tmp)

            first = run("2026-01-01T00:00:00Z")
            first_receipts = json.loads(storage.read_key(first["candidate_ready"].replace("metadata/candidate_ready.json", "metadata/source_manifest.json")))
            second = run("2026-01-02T00:00:00Z")
            second_manifest = json.loads(storage.read_key(second["candidate_ready"].replace("metadata/candidate_ready.json", "metadata/source_manifest.json")))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertEqual(first_receipts["inputs"], second_manifest["inputs"])
            changed = run("2026-01-03T00:00:00Z", mutate=True)
            self.assertNotEqual(first["version"], changed["version"])


if __name__ == "__main__":
    unittest.main()

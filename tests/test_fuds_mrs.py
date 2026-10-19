"""Focused native fidelity tests for USACE FUDS MRS."""
import unittest
import hashlib
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.sources.fuds_mrs import (
    EXPECTED_SCHEMA_SHA256,
    _encode_polygon_z,
    _native_parquet,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, content_derived_version, processing_identity,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)


class FudsMrsNativeFidelityTests(unittest.TestCase):
    def test_candidate_ready_is_sealed_and_cache_reuses_after_catalog(self):
        repo = Path(__file__).resolve().parents[1]
        identity = processing_identity(repo)
        dataset = "formerly-used-defense-sites-public-mrs"
        record_sha = hashlib.sha256(b"tiny FUDS source attributes").hexdigest()
        geometry_sha = hashlib.sha256(b"tiny FUDS XYZM rings").hexdigest()
        schema_sha = EXPECTED_SCHEMA_SHA256
        version = content_derived_version(record_sha, geometry_sha, schema_sha, identity)
        prefix = candidate_prefix(dataset, dataset, version)
        state_key = f"_construction-state/{dataset}/{version}.json"
        fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            body, wire = b'{"features":[{"attributes":{"OBJECTID":1}}]}', b"wire bytes"
            receipt = {"complete_body": True, "status": 200, "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(), "wire_bytes": len(wire),
                "wire_sha256": hashlib.sha256(wire).hexdigest()}
            outputs = {
                "geoparquet/native.parquet": b"tiny candidate parquet",
                "metadata/source_manifest.json": b'{"approved":false}',
                "raw/page.body": body,
                "raw/page.wire": wire,
                "raw/page.receipt.json": json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode() + b"\n",
            }
            ready_descriptors = {name: {"bytes":len(value), "sha256":hashlib.sha256(value).hexdigest()}
                for name, value in outputs.items()}
            outputs["metadata/candidate_ready.json"] = json.dumps({"approved":False,"version":version,
                "objects":ready_descriptors,"source_release_accepted":False,"freshness_accepted":False,
                "schema_contract_accepted":False,"id_contract_accepted":False,"rights_accepted":False,
                "mapping_approved":False,"release_accepted":False}, sort_keys=True, separators=(",", ":")).encode()+b"\n"
            descriptors = {name: {"bytes":len(value), "sha256":hashlib.sha256(value).hexdigest()}
                for name, value in outputs.items()}
            for name, value in outputs.items(): write_immutable(storage, f"{prefix}/{name}", value)
            seal_ready_candidate(storage, state_key, version, fingerprint, {
                "processing_identity":identity,"source_record_sha256":record_sha,
                "source_schema_sha256":schema_sha,"capture_receipts":{"page":receipt},
                "objects":descriptors,
            })
            manifest = outputs["metadata/source_manifest.json"]
            derived = {
                "metadata/quality_manifest.json":b'{"feature_count":1,"invalid_geometry_count":1,"quality_check_passed":false}',
                "metadata/data_dictionary.json":b'{"fixture":true}',
                "metadata/resolved_provenance.json":b'{"fixture":true}',
                "metadata/source/source_manifest.json":manifest,
                f"metadata/source/version_{hashlib.sha256(manifest).hexdigest()}_source_manifest.json":manifest,
            }
            for name, value in derived.items(): write_immutable(storage, f"{prefix}/{name}", value)
            # This synthetic checkout is intentionally uncommitted while the
            # test runs; production cache reuse still pins the committed blobs.
            with patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                result = verify_cached_candidate(storage, repo, state_key, prefix, version, fingerprint,
                    identity, record_sha, schema_sha, descriptors, {"page":receipt})
            self.assertEqual(result, {"version":version,"reused":True})
            candidate_ready = json.loads(storage.read_key(f"{prefix}/metadata/candidate_ready.json"))
            self.assertEqual(candidate_ready["objects"], ready_descriptors)
            self.assertEqual(set(descriptors), {name for name in outputs})

    def test_xyz_m_null_and_zero_are_preserved_in_native_source_column(self):
        ring = [
            [0.0, 0.0, 5.0, None], [0.0, 2.0, 6.0, 0.0],
            [2.0, 2.0, 7.0, None], [2.0, 0.0, 8.0, 0.0],
            [0.0, 0.0, 5.0, None],
        ]
        encoded = _encode_polygon_z([ring])
        geometry = shapely.from_wkb(encoded)
        self.assertTrue(geometry.has_z)
        self.assertEqual(shapely.get_coordinates(geometry, include_z=True).tolist(),
                         [[p[0], p[1], p[2]] for p in ring])

        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
            {"name": "FEATURENAME", "type": "esriFieldTypeString", "nullable": True},
        ]
        features = [
            {"attributes": {"OBJECTID": 1, "FEATURENAME": None},
             "geometry": {"hasZ": True, "hasM": True, "rings": [ring]}},
            {"attributes": {"OBJECTID": 2, "FEATURENAME": "No geometry"}, "geometry": None},
        ]
        table = pq.read_table(pa.BufferReader(_native_parquet(fields, features,
            {1: {"wkid": 4326, "latestWkid": 4326}, 2: {"wkid": 4326, "latestWkid": 4326}})), use_threads=False)
        self.assertEqual(table.schema.names, ["OBJECTID", "FEATURENAME", "geometry", "source_geometry"])
        self.assertEqual(table["OBJECTID"].type, pa.int64())
        self.assertIsNone(table["FEATURENAME"][0].as_py())
        self.assertEqual(table["geometry"].null_count, 1)
        source = table["source_geometry"].to_pylist()
        self.assertEqual(source[0]["rings"][0], [
            {"x": p[0], "y": p[1], "z": p[2], "m": p[3]} for p in ring])
        self.assertEqual(source[0]["spatialReference"], '{"latestWkid":4326,"wkid":4326}')
        self.assertIsNone(source[1])
        self.assertEqual(geometry_metadata(table.schema)[1]["geometry_types"], ["MultiPolygon Z"])

    def test_pinned_schema_digest_is_sha256(self):
        self.assertEqual(len(EXPECTED_SCHEMA_SHA256), 64)
        self.assertTrue(all(char in "0123456789abcdef" for char in EXPECTED_SCHEMA_SHA256))

    def test_nested_island_does_not_steal_containing_hole(self):
        def ring(points):
            return [[x, y, 0, None] for x, y in points]
        # Esri exterior rings are clockwise and holes counterclockwise.
        outer = ring([[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]])
        hole = ring([[2, 2], [8, 2], [8, 8], [2, 8], [2, 2]])
        island = ring([[4, 4], [4, 6], [6, 6], [6, 4], [4, 4]])
        geometry = shapely.from_wkb(_encode_polygon_z([outer, hole, island]))
        self.assertTrue(geometry.is_valid)
        self.assertEqual(len(geometry.geoms), 2)
        self.assertEqual(sorted(len(poly.interiors) for poly in geometry.geoms), [0, 1])


if __name__ == "__main__":
    unittest.main()

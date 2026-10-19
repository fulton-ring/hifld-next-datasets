import io
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq
import shapely

from dagster_hifld.partitions import build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_disaster_debris import (
    _native_parquet, _portal_publication_snapshot, _sha, _source_layer_stable,
)
from dagster_hifld.sources.epa_frs_common import (
    candidate_prefix, content_derived_version, processing_identity,
    seal_ready_candidate, verify_cached_candidate, write_immutable,
)


class EpaDisasterDebrisTests(unittest.TestCase):
    def test_portal_view_counter_is_the_only_ignored_publication_change(self):
        before = {"id": "item", "owner": "EPA_GEO", "url": "https://example.gov/service",
                  "title": "EPA 2026", "type": "Feature Service", "access": "public", "numViews": 12}
        after = {**before, "numViews": 13}
        self.assertEqual(_portal_publication_snapshot(before), _portal_publication_snapshot(after))
        self.assertNotEqual(_portal_publication_snapshot(before), _portal_publication_snapshot({**after, "owner": "other"}))
        layer_before = {"name": "layer", "geometryType": "esriGeometryPoint",
                        "fields": [{"name": "OBJECTID", "type": "esriFieldTypeOID"}]}
        self.assertTrue(_source_layer_stable(layer_before, dict(layer_before)))
        self.assertFalse(_source_layer_stable(layer_before, {**layer_before, "fields": [{"name": "OBJECTID", "type": "esriFieldTypeString"}]}))

    def test_native_schema_preserves_zeroes_nulls_global_ids_and_native_points(self):
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
            {"name": "Company", "type": "esriFieldTypeString", "length": 100, "nullable": True},
            {"name": "Recovery", "type": "esriFieldTypeSmallInteger", "nullable": True},
            {"name": "Latitude", "type": "esriFieldTypeDouble", "nullable": True},
            {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "nullable": False},
        ]
        features = [
            {"attributes": {"OBJECTID": 41, "Company": "00107", "Recovery": 0, "Latitude": 0.0, "GlobalID": "{aa}"},
             "geometry": {"x": -10701721.5, "y": 4207903.5}},
            {"attributes": {"OBJECTID": 42, "Company": None, "Recovery": None, "Latitude": None, "GlobalID": "{bb}"},
             "geometry": None},
        ]
        table = pq.ParquetFile(io.BytesIO(_native_parquet(fields, features))).read(use_threads=False)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [41, 42])
        self.assertEqual(table.column("Company").to_pylist(), ["00107", None])
        self.assertEqual(table.column("Recovery").to_pylist(), [0, None])
        self.assertEqual(table.column("Latitude").to_pylist(), [0.0, None])
        self.assertEqual(table.column("GlobalID").to_pylist(), ["{aa}", "{bb}"])
        point = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertEqual((point.x, point.y), (-10701721.5, 4207903.5))
        self.assertIsNone(table.column("geometry")[1].as_py())

    def test_first_candidate_seal_cache_and_partition_use_content_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True)
            identity = processing_identity(Path(__file__).parents[1])
            record_sha, geometry_sha, schema_sha = (_sha(b"records"), _sha(b"geometry"), _sha(b"schema"))
            version = content_derived_version(record_sha, geometry_sha, schema_sha, identity)
            prefix = candidate_prefix("epa-disaster-debris-recovery-data", "epa-disaster-debris-recovery-data", version)
            self.assertEqual(build_publish_partition_key("epa-disaster-debris-recovery-data", "epa-disaster-debris-recovery-data", version),
                             f"epa-disaster-debris-recovery-data/epa-disaster-debris-recovery-data/{version}")
            receipt = {"complete_body": True, "status": 200, "bytes": 4, "sha256": _sha(b"body"),
                       "wire_bytes": 4, "wire_sha256": _sha(b"wire"), "observed_at": "2026-10-19T00:00:00Z"}
            outputs = {"raw/page.body": b"body", "raw/page.wire": b"wire",
                       "raw/page.receipt.json": json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode() + b"\n",
                       "geoparquet/native.parquet": b"small synthetic native candidate"}
            descriptors = {}
            for name, body in outputs.items():
                write_immutable(storage, f"{prefix}/{name}", body)
                descriptors[name] = {"bytes": len(body), "sha256": _sha(body)}
            fingerprint = _sha(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode())
            state_key = f"_construction-state/epa-disaster-debris-recovery-data/{version}.json"
            payload = {"processing_identity": identity, "source_record_sha256": record_sha,
                       "source_schema_sha256": schema_sha, "capture_receipts": {"page": receipt},
                       "objects": descriptors}
            seal_ready_candidate(storage, state_key, version, fingerprint, payload)
            cached = verify_cached_candidate(storage, Path(__file__).parents[1], state_key, prefix, version, fingerprint,
                                             identity, record_sha, schema_sha, descriptors, {"page": receipt})
            self.assertTrue(cached["reused"])
            self.assertEqual(set(storage.list_prefix(prefix)), {f"{prefix}/{name}" for name in outputs})
            with self.assertRaisesRegex(ValueError, "immutable_staged_object_changed"):
                write_immutable(storage, f"{prefix}/raw/page.wire", b"wire-mutated")


if __name__ == "__main__":
    unittest.main()

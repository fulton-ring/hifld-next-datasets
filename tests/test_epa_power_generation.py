import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_power_generation
from dagster_hifld.sources.epa_power_generation import (
    DATASET_SLUG,
    _parquet_bytes,
    _profile_schema,
    capture_epa_power_generation,
    _canonical,
)


class EpaPowerGenerationSourceTests(unittest.TestCase):
    def test_native_geoparquet_preserves_source_ids_zeroes_strings_and_point(self):
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
            {"name": "FIPS_CODE", "type": "esriFieldTypeString", "length": 5, "nullable": True},
            {"name": "SECTOR", "type": "esriFieldTypeInteger", "nullable": True},
        ]
        source_rows = [{
            "attributes": {"OBJECTID": 17, "FIPS_CODE": "00107", "SECTOR": 0},
            "geometry": {"x": -10701721.5, "y": 4207903.5},
        }]
        body = _parquet_bytes(fields, source_rows)
        table = pq.ParquetFile(io.BytesIO(body)).read(use_threads=False)

        self.assertEqual(table.column("OBJECTID").to_pylist(), [17])
        self.assertEqual(table.column("FIPS_CODE").to_pylist(), ["00107"])
        self.assertEqual(table.column("SECTOR").to_pylist(), [0])
        primary, _column, crs = geometry_metadata(table.schema)
        self.assertEqual(primary, "geometry")
        self.assertEqual(crs.to_epsg(), 3857)
        point = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertAlmostEqual(point.x, -10701721.5)
        self.assertAlmostEqual(point.y, 4207903.5)

    def test_expected_native_profile_keeps_every_source_field(self):
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OID", "nullable": False},
            {"name": "REGISTRY_ID", "type": "esriFieldTypeString", "length": 12, "alias": "REGISTRY_ID", "nullable": True},
        ]
        plain, digest = _profile_schema({"fields": fields})
        self.assertEqual([field["name"] for field in plain], ["OBJECTID", "REGISTRY_ID"])
        self.assertEqual(len(digest), 64)


class EpaPowerGenerationSeedQuarantineTests(unittest.TestCase):
    """Fail-closed seed-drift regressions for the observed 2026-11 drift.

    The live EPA layer returned 2,327 IDs on 2026-11-08 and 2,326 on 2026-11-09
    against the pinned 2,328 roster. Hosted OBJECTIDs are reassignable, so count
    and roster drift never imply facility-level additions or removals. The guard
    must keep refusing while the failure handler preserves the rejected seed
    body, its receipt and the incomplete capture in quarantine (the RMP adapter's
    receipt/body reconstruction pattern) instead of skipping quarantine when the
    in-memory capture map is still empty.
    """

    @staticmethod
    def _seed_get(seed_body):
        def get(_client, url, capture_root, name, *, max_bytes):
            (capture_root / f"{name}.wire").write_bytes(seed_body)
            (capture_root / f"{name}.body").write_bytes(seed_body)
            receipt = {
                "method": "GET", "url": url, "final_url": url,
                "observed_at": "2026-11-09T00:00:00Z", "status": 200, "complete_body": True,
                "bytes": len(seed_body), "sha256": hashlib.sha256(seed_body).hexdigest(),
                "wire_bytes": len(seed_body), "wire_sha256": hashlib.sha256(seed_body).hexdigest(),
                "content_encoding": "identity", "content_length": len(seed_body), "error_code": None,
            }
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return seed_body, receipt
        return get

    def _assert_quarantined_seed_only(self, storage, captures_root, *, reason, seed_body):
        quarantine = storage.list_prefix("_quarantine/" + DATASET_SLUG)
        self.assertTrue(any(key.endswith("/raw/ids_seed.body") for key in quarantine))
        self.assertTrue(any(key.endswith("/raw/ids_seed.wire") for key in quarantine))
        self.assertTrue(any(key.endswith("/raw/ids_seed.receipt.json") for key in quarantine))
        failure_key = next(key for key in quarantine if key.endswith("/metadata/failure.json"))
        failure = json.loads(storage.read_key(failure_key))
        self.assertEqual(failure["reason_code"], reason)
        self.assertEqual(failure["captured_names"], ["ids_seed"])
        self.assertEqual(storage.read_key(next(k for k in quarantine if k.endswith("/raw/ids_seed.body"))), seed_body)
        self.assertEqual(storage.list_prefix(DATASET_SLUG), [])
        manifest_path = next(captures_root.glob("*/capture_manifest.json"))
        manifest = json.loads(manifest_path.read_bytes())
        self.assertFalse(manifest["complete"])
        self.assertEqual(manifest["error_code"], reason)
        self.assertIn("ids_seed", manifest["receipts"])

    def test_seed_count_drift_quarantines_rejected_seed_and_keeps_pin(self):
        seed = _canonical({"objectIds": [1, 2, 3]})
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"), use_local=True, backend="local")
            captures_root = Path(tmp) / "captures"
            with patch.object(epa_power_generation, "capture_get",
                              side_effect=self._seed_get(seed)):
                with self.assertRaisesRegex(ValueError, "source_seed_id_count_drift"):
                    capture_epa_power_generation(storage, version="synthetic-seed-drift",
                                                capture_directory=str(captures_root))
            self._assert_quarantined_seed_only(storage, captures_root,
                                               reason="ValueError", seed_body=seed)
        self.assertEqual(epa_power_generation.EXPECTED_COUNT, 2328)

    def test_incomplete_seed_fetch_preserves_partial_evidence_in_quarantine(self):
        partial = b'{"objectIds": [1, 2'
        def interrupted_get(_client, url, capture_root, name, *, max_bytes):
            capture_root.joinpath(f"{name}.wire").write_bytes(partial)
            capture_root.joinpath(f"{name}.body").write_bytes(partial)
            receipt = {
                "method": "GET", "url": url, "final_url": url,
                "observed_at": "2026-11-09T00:00:00Z", "status": 200, "complete_body": False,
                "bytes": len(partial), "sha256": hashlib.sha256(partial).hexdigest(),
                "wire_bytes": len(partial), "wire_sha256": hashlib.sha256(partial).hexdigest(),
                "content_encoding": "identity", "content_length": None,
                "error_code": "mid_stream_incomplete",
            }
            capture_root.joinpath(f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            raise ValueError(f"incomplete_source_capture:{name}")

        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"), use_local=True, backend="local")
            captures_root = Path(tmp) / "captures"
            with patch.object(epa_power_generation, "capture_get", side_effect=interrupted_get):
                with self.assertRaisesRegex(ValueError, "incomplete_source_capture:ids_seed"):
                    capture_epa_power_generation(storage, version="synthetic-incomplete-seed",
                                                capture_directory=str(captures_root))
            self._assert_quarantined_seed_only(storage, captures_root,
                                               reason="ValueError", seed_body=partial)
        self.assertEqual(epa_power_generation.EXPECTED_COUNT, 2328)


if __name__ == "__main__":
    unittest.main()

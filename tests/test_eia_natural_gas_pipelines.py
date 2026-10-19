import io
import json
import hashlib
import tempfile
import unittest
from pathlib import Path

import geopandas as gpd
import pyarrow.parquet as pq
import shapely
from shapely.geometry import LineString, MultiLineString

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.eia_natural_gas_pipelines import (
    eia_natural_gas_interstate_intrastate_pipelines,
    eia_natural_gas_interstate_intrastate_pipelines_complete,
    eia_natural_gas_pipelines_draft_schedule,
)
from dagster_hifld.sources.eia_natural_gas_pipelines import (
    SOURCE_URL,
    _canonical,
    _load_retained_capture,
    _sealed_receipt_for_same_capture,
    _table_from_frame,
)


class EiaNaturalGasPipelinesTests(unittest.TestCase):
    def test_native_fields_geometry_and_order_survive_geoparquet(self):
        source = gpd.GeoDataFrame(
            {
                "TYPEPIPE": ["Intrastate", "Interstate", "Gathering"],
                "Operator": ["Texas Pipeline", "North Pipeline", "Source row with null shape"],
                "Status": ["Operating", "Proposed", "Operating"],
                "Shape_Leng": [0.00125, 12.5, 0.0],
            },
            geometry=[
                LineString([(-94.6, 29.6), (-94.5, 29.7)]),
                MultiLineString([[(-80.0, 40.0), (-79.9, 40.1)], [(-79.8, 40.2), (-79.7, 40.3)]]),
                None,
            ],
            crs="EPSG:4326",
        )
        body, audit = _table_from_frame(source)
        table = pq.ParquetFile(io.BytesIO(_parquet(body))).read(use_threads=False)

        self.assertEqual(table.column_names, ["TYPEPIPE", "Operator", "Status", "Shape_Leng", "geometry"])
        self.assertEqual(table.column("TYPEPIPE").to_pylist(), ["Intrastate", "Interstate", "Gathering"])
        self.assertEqual(table.column("Shape_Leng").to_pylist(), [0.00125, 12.5, 0.0])
        self.assertEqual(audit["source_id_field"], None)
        self.assertEqual(audit["source_order_preserved"], True)
        self.assertEqual(audit["null_geometry_count"], 1)
        self.assertEqual(shapely.from_wkb(table.column("geometry")[0].as_py()), source.geometry.iloc[0])
        self.assertIsNone(table.column("geometry")[2].as_py())
        self.assertEqual(table.schema.field("Shape_Leng").type, __import__("pyarrow").float64())

    def test_explicit_asset_check_does_not_approve_release(self):
        result = eia_natural_gas_interstate_intrastate_pipelines_complete(
            {"feature_count": 32961}
        )
        self.assertTrue(result.passed)
        self.assertFalse(result.metadata["release_accepted"].value)
        self.assertFalse(result.metadata["id_contract_accepted"].value)
        self.assertEqual(
            eia_natural_gas_pipelines_draft_schedule.default_status,
            DefaultScheduleStatus.STOPPED,
        )

    def test_retained_capture_replay_preserves_receipt_and_rejects_tampering(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            body = b"original archive bytes"
            wire = b"original wire bytes"
            sha = lambda value: hashlib.sha256(value).hexdigest()
            receipt = {
                "method": "GET", "url": SOURCE_URL, "final_url": SOURCE_URL,
                "observed_at": "2026-10-19T12:34:56+00:00", "status": 200,
                "complete_body": True, "wire_bytes": len(wire), "wire_sha256": sha(wire),
                "bytes": len(body), "sha256": sha(body), "content_encoding": "identity",
                "content_length": len(body), "error_code": None,
            }
            (root / "eia_source_zip.body").write_bytes(body)
            (root / "eia_source_zip.wire").write_bytes(wire)
            receipt_bytes = _canonical(receipt) + b"\n"
            (root / "eia_source_zip.receipt.json").write_bytes(receipt_bytes)
            replay_body, replay_wire, replay_receipt = _load_retained_capture(root)
            self.assertEqual((replay_body, replay_wire), (body, wire))
            self.assertEqual(replay_receipt["observed_at"], receipt["observed_at"])
            self.assertEqual((root / "eia_source_zip.receipt.json").read_bytes(), receipt_bytes)
            fresh = {**receipt, "observed_at": "2026-10-19T12:35:00+00:00"}
            self.assertEqual(
                _sealed_receipt_for_same_capture(receipt, fresh, body, wire)["observed_at"],
                receipt["observed_at"],
            )
            fresh["sha256"] = "0" * 64
            with self.assertRaisesRegex(ValueError, "fresh_capture_differs_from_sealed_source"):
                _sealed_receipt_for_same_capture(receipt, fresh, body, wire)

            (root / "eia_source_zip.body").write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "retained_capture_receipt_integrity_failed"):
                _load_retained_capture(root)
        self.assertEqual(
            eia_natural_gas_interstate_intrastate_pipelines.key.path,
            ["ingest", "natural-gas-interstate-and-intrastate-pipelines",
             "natural-gas-interstate-and-intrastate-pipelines"],
        )


def _parquet(body):
    sink = io.BytesIO()
    pq.write_table(body, sink, compression="zstd")
    return sink.getvalue()


if __name__ == "__main__":
    unittest.main()

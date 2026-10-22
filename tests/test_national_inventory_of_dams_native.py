from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
import zipfile
from urllib.parse import urlencode
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from dagster_hifld.http_capture import CaptureProgress
from dagster_hifld.sources import national_inventory_of_dams as legacy
from dagster_hifld.sources import national_inventory_of_dams_native as native


class NativeNidTests(unittest.TestCase):
    def test_gdal_zip_alias_preserves_archive_bytes_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "native_archive.body"
            archive.write_bytes(b"immutable archive bytes")
            with native._gdal_zip_path(archive, "data.gdb") as vsi_path:
                alias = Path(vsi_path.split("/vsizip/", 1)[1].split("/data.gdb", 1)[0])
                self.assertEqual(alias.suffix, ".zip")
                self.assertTrue(alias.is_symlink())
                self.assertEqual(alias.resolve(), archive.resolve())
                self.assertEqual(alias.stat().st_ino, archive.stat().st_ino)
            self.assertFalse(alias.exists())
            self.assertEqual(archive.read_bytes(), b"immutable archive bytes")

    def _arrow_fixture(self):
        fields = [pa.field("OBJECTID", pa.int64(), nullable=False)]
        arrays = [pa.array([101, 102], type=pa.int64())]
        ogr_types = []
        names = []
        for name, esri_type, length in legacy.EXPECTED_FIELD_SIGNATURES:
            if name == "OBJECTID":
                continue
            typ = native._ARROW_TYPES[esri_type]
            metadata = ({b"GDAL:OGR:width": str(length).encode()}
                        if esri_type == "esriFieldTypeString" else None)
            fields.append(pa.field(name, typ, nullable=True, metadata=metadata))
            names.append(name)
            ogr_types.append(native._OGR_TYPES[esri_type])
            if name == "NIDID":
                values = ["00000101", "00000101"]
            elif name == "DATA_UPDATED":
                values = [datetime(2020, 1, 2), None]
            elif name == "SE_ANNO_CAD_DATA":
                values = [b"\x00\xffannotation", None]
            elif esri_type == "esriFieldTypeString":
                values = ["sample", None]
            elif esri_type == "esriFieldTypeInteger":
                values = [7, None]
            elif esri_type == "esriFieldTypeDouble":
                values = [1.25, None]
            else:
                values = [None, None]
            arrays.append(pa.array(values, type=typ))
        shape_field = pa.field("SHAPE", pa.binary(), nullable=True,
                               metadata={b"ARROW:extension:name": b"geoarrow.wkb"})
        fields.append(shape_field)
        wkb = [shapely.to_wkb(shapely.Point(-120.1, 39.2, 5.75)),
               shapely.to_wkb(shapely.Point(-120.2, 39.3, 0.0))]
        arrays.append(pa.array(wkb, type=pa.binary()))
        schema = pa.schema(fields)
        batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
        reader = pa.RecordBatchReader.from_batches(schema, [batch])
        info = {
            "layer_name": native.NATIVE_LAYER_NAME,
            "features": 2,
            "crs": "EPSG:4269",
            "geometry_type": "Point Z",
            "fid_column": "OBJECTID",
            "geometry_name": "SHAPE",
            "fast_feature_count": True,
            "total_bounds": [-120.2, 39.2, -120.1, 39.3],
            "fields": names,
            "ogr_types": ogr_types,
            "capabilities": {"fast_feature_count": True},
        }
        layer = {"extent": {"xmin": -120.2, "ymin": 39.2, "xmax": -120.1,
                             "ymax": 39.3, "spatialReference": {"wkid": 4269}}}
        return reader, info, layer, wkb

    def test_filegdb_schema_and_point_z_are_preserved_in_parquet(self):
        reader, info, layer, source_wkb = self._arrow_fixture()
        native._validate_native_reader(info, reader.schema, layer, 2)
        candidate, audit = native._parquet_from_native_batches(
            reader, [101, 102], {"xmin": -180, "ymin": 10, "xmax": 180, "ymax": 75},
        )
        table = pq.read_table(pa.BufferReader(candidate))
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [101, 102])
        self.assertEqual(table.column("NIDID").to_pylist(), ["00000101", "00000101"])
        self.assertEqual(table.column("DATA_UPDATED").type, pa.timestamp("ms"))
        self.assertEqual(table.column("DATA_UPDATED").to_pylist()[1], None)
        self.assertEqual(table.column("SE_ANNO_CAD_DATA").to_pylist(), [b"\x00\xffannotation", None])
        self.assertEqual(table.column("geometry").to_pylist(), source_wkb)
        geometry = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertTrue(geometry.has_z)
        self.assertEqual(geometry.coords[0], (-120.1, 39.2, 5.75))
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point Z"])
        self.assertEqual(audit["native_fid_name"], "OBJECTID")
        self.assertEqual(audit["source_feature_count"], 2)
        self.assertEqual(audit["geometry_z_min"], 0.0)
        self.assertEqual(audit["geometry_z_max"], 5.75)
        self.assertEqual(audit["geometry_z_zero_count"], 1)

    def test_capability_and_before_after_oid_roster_allow_sync_disabled(self):
        source_fields = [
            {"name": name, "type": kind, "length": length}
            for name, kind, length in legacy.EXPECTED_FIELD_SIGNATURES
        ]
        service = {
            "layers": [{"id": 0, "name": legacy.LAYER_NAME, "type": "Feature Layer"}],
            "capabilities": "Query,Extract", "supportedExportFormats": ["filegdb"],
            "syncEnabled": False,
        }
        layer = {
            "id": 0, "name": legacy.LAYER_NAME, "type": "Feature Layer",
            "geometryType": "esriGeometryPoint", "objectIdField": "OBJECTID",
            "hasZ": True, "hasM": False,
            "extent": {"spatialReference": {"wkid": 4269}},
            "fields": source_fields,
        }
        docs = {
            "service_before": service, "service_after": service,
            "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 2}, "count_after": {"count": 2},
            "ids_before": {"objectIds": [101, 102]},
            "ids_after": {"objectIds": [102, 101]},
        }
        _validated_layer, ids = native._validate_metadata_and_rosters(docs)
        self.assertEqual(ids, [101, 102])

    def test_replica_job_pending_and_in_progress_continue_without_resubmission(self):
        self.assertEqual(native._replica_status_action("Pending"), "continue")
        self.assertEqual(native._replica_status_action("InProgress"), "continue")
        self.assertEqual(native._replica_status_action("Completed"), "completed")
        with self.assertRaisesRegex(ValueError, "replica_job_failed"):
            native._replica_status_action("Failed")
        with self.assertRaisesRegex(ValueError, "status_unrecognized"):
            native._replica_status_action(["Pending"])

    def test_replica_form_validation_ignores_key_order_but_rejects_duplicates(self):
        original_order = {
            "f": "json", "replicaName": "hifld-nid-native-example",
            "layerQueries": '{"0":{"where":"1=1","outFields":"*"}}',
        }
        body = urlencode(original_order).encode("ascii")
        canonical_order = json.loads(json.dumps(original_order, sort_keys=True))
        native._validate_replica_request_body(body, canonical_order)
        duplicate = body + b"&f=json"
        with self.assertRaisesRegex(ValueError, "request_body_mismatch"):
            native._validate_replica_request_body(duplicate, canonical_order)

    def test_actual_block_budget_refuses_before_any_sink_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            progress = CaptureProgress("wire", 0, 0, 0, 0, 0)
            pending_file = root / "current.body"
            with patch.object(native, "MAX_NATIVE_CAPTURE_BYTES", 65_535), \
                    patch.object(native, "MAX_NATIVE_RECEIPT_BYTES", 1), \
                    patch.object(native, "MIN_DISK_FREE_BYTES", 0):
                with self.assertRaisesRegex(ValueError, "capture_budget_exceeded"):
                    native._reserve_capture_block(root, "current", [progress], 65_535)
            self.assertFalse(pending_file.exists())

    def test_replay_manifest_checks_exact_retained_file_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = b"sealed-export"
            (root / "sample.body").write_bytes(data)
            manifest = {"complete": True, "files": {
                "sample.body": {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()},
            }}
            (root / "native_capture_manifest.json").write_text(json.dumps(manifest))
            bodies, read_manifest = native._read_native_capture(root)
            self.assertEqual(bodies["sample.body"], data)
            self.assertTrue(read_manifest["complete"])
            (root / "sample.body").write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "digest_mismatch"):
                native._read_native_capture(root)

    def test_stale_zip_member_crc_is_rejected_before_gdal(self):
        payload = b"unique-crc-payload-for-regression"
        with tempfile.TemporaryDirectory() as tmp:
            archive = Path(tmp) / "dam-export.zip"
            with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as output:
                output.writestr("NID.gdb/member.dat", payload)
            raw = archive.read_bytes()
            offset = raw.index(payload)
            damaged = raw[:offset] + bytes([payload[0] ^ 1]) + raw[offset + 1:]
            archive.write_bytes(damaged)
            with patch.object(native.pyogrio, "read_info") as read_info:
                with self.assertRaisesRegex(ValueError, "member_crc_or_stream_invalid"):
                    native._inspect_archive(archive, 1, [1], {})
            read_info.assert_not_called()


if __name__ == "__main__":
    unittest.main()

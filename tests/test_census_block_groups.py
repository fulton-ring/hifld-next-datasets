import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
from dagster import DefaultScheduleStatus

from dagster_hifld.assets.census_block_groups import (
    census_block_groups_draft_schedule,
    census_block_groups_source_complete,
)
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.definitions import defs
from dagster_hifld.sources.census_block_groups import (
    DATASET_SLUG,
    EXPECTED_NAMES,
    SOURCE_BYTES,
    SOURCE_FEATURE_COUNT,
    SOURCE_SHA256,
    SOURCE_URL,
    CensusBlockGroupsError,
    _batch_digest,
    _manifest,
    _parse_layer_definition,
    _validate_capture,
    EXPECTED_NATIVE_FIELDS,
)


class CensusBlockGroupsTests(unittest.TestCase):
    def test_explicit_source_is_registered_with_blocking_check_and_stopped_schedule(self):
        asset_key = f"ingest/{DATASET_SLUG}/{DATASET_SLUG}"
        registered = {key.to_user_string() for definition in hifld_ingest_assets for key in definition.keys}
        self.assertIn(asset_key, registered)
        graph = defs.resolve_asset_graph()
        self.assertIn((asset_key, "census_block_groups_source_complete"), {
            (key.asset_key.to_user_string(), key.name) for key in graph.asset_check_keys
        })
        self.assertEqual(census_block_groups_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        scheduled_config = census_block_groups_draft_schedule._run_config_fn(None)
        source_config = scheduled_config["ops"]["ingest__census_block_groups_3__census_block_groups_3"]["config"]
        self.assertEqual(source_config["archive_path"], "CENSUS_BLOCK_GROUPS_ARCHIVE_PATH")
        self.assertEqual(source_config["receipt_path"], "CENSUS_BLOCK_GROUPS_RECEIPT_PATH")
        self.assertIn("census_block_groups_draft_refresh", {job.name for job in defs.resolve_all_job_defs()})
        self.assertEqual(
            defs.resolve_schedule_def("census_block_groups_draft_refresh_schedule").default_status,
            DefaultScheduleStatus.STOPPED,
        )

    def test_source_check_requires_exact_full_population_and_parquet_parity(self):
        candidate = {
            "feature_count": SOURCE_FEATURE_COUNT,
            "source_population_complete": True,
            "source_archive_bytes": SOURCE_BYTES,
            "source_archive_sha256": SOURCE_SHA256,
            "source_id_sha256": "1" * 64,
            "native_parquet_sha256": "2" * 64,
            "native_value_parity": True,
            "code_identity_scheme": "sha256-packaged-dagster_hifld-v1",
        }
        self.assertTrue(census_block_groups_source_complete(candidate).passed)
        for key, value in (("feature_count", SOURCE_FEATURE_COUNT - 1), ("native_value_parity", False),
                           ("source_archive_sha256", "0" * 64)):
            changed = dict(candidate, **{key: value})
            self.assertFalse(census_block_groups_source_complete(changed).passed)

    def test_capture_receipt_rejects_wrong_source_and_incomplete_transfer(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.zip"
            receipt = root / "receipt.json"
            source.write_bytes(b"partial")
            receipt.write_text(json.dumps({
                "url": SOURCE_URL, "final_url": SOURCE_URL, "status": 200,
                "complete_body": False, "bytes": 7,
                "sha256": hashlib.sha256(b"partial").hexdigest(),
                "wire_bytes_downloaded": 7, "requested_accept_encoding": "identity",
            }))
            with self.assertRaisesRegex(CensusBlockGroupsError, "receipt_or_archive_identity_drift"):
                _validate_capture(source, receipt)

    def test_stream_parity_digest_binds_all_columns_and_wkb(self):
        batch = pa.record_batch(
            [pa.array([1], type=pa.int64()), pa.array(["123456789012"]),
             pa.array(["1500000US123456789012"]), pa.array(["Block Group 1"], type=pa.string()),
             pa.array([3.0], type=pa.float64()), pa.array([4.0], type=pa.float64()),
             pa.array(["+00.0000000"]), pa.array(["-00.0000000"]), pa.array([b"\x01\x02"])],
            names=list(EXPECTED_NAMES),
        )
        changed = batch.set_column(8, batch.schema.field("SHAPE"), pa.array([b"\x01\x03"]))
        self.assertNotEqual(_batch_digest(batch), _batch_digest(changed))

    def test_manifest_pins_native_parquet_without_approving_release_or_other_formats(self):
        identity = {"code_commit": "packaged-sha256:" + "a" * 64,
                    "code_identity_scheme": "sha256-packaged-dagster_hifld-v1"}
        audit = {"parquet_bytes": 1234, "parquet_sha256": "b" * 64}
        receipt = {"observed_at": "2026-10-21T17:04:58.415344+00:00"}
        native_definition = {"source_shape_type": "esriGeometryPolygon"}
        manifest = _manifest(identity, audit, receipt, native_definition)
        self.assertIs(manifest["approved"], False)
        self.assertEqual(manifest["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"][0]["bytes"], 1234)
        self.assertNotIn("geopackage", manifest["prebuilt_output_policy"]["formats"])
        self.assertIn("release_date_unknown_reason", manifest["inputs"][0])

    def test_native_layer_definition_binds_physical_oid_width_and_dimensions(self):
        fields = "".join(
            f"<GPFieldInfoEx><Name>{name}</Name><FieldType>{field_type}</FieldType>"
            f"<Length>{length}</Length><IsNullable>{'false' if name == 'OBJECTID' else 'true'}</IsNullable></GPFieldInfoEx>"
            for name, (field_type, length) in EXPECTED_NATIVE_FIELDS.items()
        )
        xml = ("<DEFeatureClassInfo><OIDFieldName>OBJECTID</OIDFieldName><HasZ>false</HasZ><HasM>false</HasM>"
               "<ShapeType>esriGeometryPolygon</ShapeType><GPFieldInfoExs>" + fields + "</GPFieldInfoExs></DEFeatureClassInfo>")
        result = _parse_layer_definition(xml)
        self.assertEqual(result["source_oid_physical_type"], {
            "FieldType": "esriFieldTypeOID", "Length": "4", "IsNullable": "false",
        })
        with self.assertRaisesRegex(CensusBlockGroupsError, "physical_type_or_width_drift:OBJECTID"):
            _parse_layer_definition(xml.replace("<Length>4</Length>", "<Length>8</Length>"))


if __name__ == "__main__":
    unittest.main()

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
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
    _canonical,
    _geo_schema,
    _reuse_existing_candidate,
    SOURCE_OWNED_FILES,
    CATALOG_OWNED_METADATA,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import census_block_groups as block_groups_source


class CensusBlockGroupsTests(unittest.TestCase):
    def _cached_candidate(self, root: Path):
        identity = {"code_commit": "packaged-sha256:" + "a" * 64,
                    "code_identity_scheme": "sha256-packaged-dagster-hifld-v1"}
        receipt = {"observed_at": "2026-10-21T17:04:58.415344+00:00"}
        native_definition = {"source_shape_type": "esriGeometryPolygon"}
        fingerprint = hashlib.sha256(_canonical(identity)).hexdigest()
        version = f"v0.0.0-draft-tiger2024-{SOURCE_SHA256[:12]}-{fingerprint[:16]}"
        target = root / DATASET_SLUG / DATASET_SLUG / version
        target.mkdir(parents=True)
        count = SOURCE_FEATURE_COUNT
        table = pa.table({
            "OBJECTID": pa.array(range(1, count + 1), type=pa.int64()),
            "GEOID": pa.array(["010010201001"] * count, type=pa.string()),
            "GEOIDFQ": pa.array(["1500000US010010201001"] * count, type=pa.string()),
            "NAMELSAD": pa.array(["Block Group 1"] * count, type=pa.string()),
            "ALAND": pa.array([1.0] * count, type=pa.float64()),
            "AWATER": pa.array([0.0] * count, type=pa.float64()),
            "INTPTLAT": pa.array(["+32.0000000"] * count, type=pa.string()),
            "INTPTLON": pa.array(["-086.0000000"] * count, type=pa.string()),
            "SHAPE": pa.array([b"\x01"] * count, type=pa.binary()),
        })
        schema = _geo_schema(table.schema)
        table = table.cast(schema).replace_schema_metadata(schema.metadata)
        parquet_path = target / "geoparquet" / "census-block-groups-3.parquet"
        parquet_path.parent.mkdir()
        pq.write_table(table, parquet_path, compression="zstd", use_dictionary=True)
        parquet_sha = hashlib.sha256(parquet_path.read_bytes()).hexdigest()
        audit = {
            "approved": False, "layer": "Block_Group", "feature_count": count,
            "crs": "EPSG:4269", "geometry_type": "MultiPolygon", "invalid_geometries": 0,
            "empty_geometries": 0, "z_geometries": 0, "native_layer_definition": native_definition,
            "source_driver_geometry_type": "MultiPolygon", "FileGDB_declared_shape_type": "esriGeometryPolygon",
            "source_batch_sha256": "b" * 64, "parquet_batch_sha256": "b" * 64,
            "source_oid_arrow_representation": "OBJECTID is physical esriFieldTypeOID Length4; GDAL Arrow emits int64 with every original value retained",
            "coverage_scope": "all rows returned by the exact Block_Group layer in the pinned 2024 national substate GDB",
            "native_objectid_sha256": "c" * 64, "statefp_counts": {"01": count},
            "parquet_bytes": parquet_path.stat().st_size, "parquet_sha256": parquet_sha,
            "bounds": [-1.0, -1.0, 1.0, 1.0], "arrow_schema": str(schema),
        }
        baseline = {"schema_version": 1, "approved": False, "status": "blocked",
                    "reason_codes": ["published_baseline_and_ID_schema_contract_acceptance_pending"]}
        capture = {"archive_key": block_groups_source.RAW_KEY,
                   "receipt_key": block_groups_source.RAW_RECEIPT_KEY,
                   "archive_sha256": SOURCE_SHA256, "archive_bytes": SOURCE_BYTES,
                   "archive_members": block_groups_source.SOURCE_MEMBERS,
                   "archive_expanded_bytes": block_groups_source.SOURCE_EXPANDED_BYTES,
                   "source_url": SOURCE_URL}
        objects = {
            "metadata/processing_identity.json": identity,
            "metadata/source_capture.json": capture,
            "metadata/source_audit.json": audit,
            "metadata/source_manifest.json": _manifest(identity, audit, receipt, native_definition),
            "metadata/baseline_comparison.json": baseline,
        }
        for name, value in objects.items():
            path = target / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(_canonical(value))
        return target, identity, receipt, native_definition, version, audit

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

    def test_cached_candidate_reuses_unchanged_source_roster_without_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            target, identity, receipt, native_definition, version, _ = self._cached_candidate(Path(tmp))
            (target / "metadata/quality_manifest.json").write_text('{"catalog_owned":true}\n')
            (target / "metadata/resolved_provenance.json").write_text('{"catalog_owned":true}\n')
            source_manifest = (target / "metadata/source_manifest.json").read_bytes()
            source_dir = target / "metadata/source"
            source_dir.mkdir()
            (source_dir / "source_manifest.json").write_bytes(source_manifest)
            snapshot_name = "version_" + hashlib.sha256(source_manifest).hexdigest() + "_source_manifest.json"
            (source_dir / snapshot_name).write_bytes(source_manifest)
            before = {path.relative_to(target).as_posix(): (path.read_bytes(), path.stat().st_mtime_ns)
                      for path in target.rglob("*") if path.is_file()}
            result = _reuse_existing_candidate(
                target, identity=identity, receipt=receipt,
                native_definition=native_definition, version=version,
            )
            after = {path.relative_to(target).as_posix(): (path.read_bytes(), path.stat().st_mtime_ns)
                     for path in target.rglob("*") if path.is_file()}
            self.assertTrue(result["reused"])
            self.assertFalse(result["approved"])
            self.assertEqual(before, after)
            self.assertTrue(SOURCE_OWNED_FILES <= set(before))
            self.assertTrue(CATALOG_OWNED_METADATA & set(before))
            self.assertIn("metadata/source/source_manifest.json", before)
            self.assertIn("metadata/source/" + snapshot_name, before)

    def test_cached_candidate_refuses_source_parquet_tampering(self):
        with tempfile.TemporaryDirectory() as tmp:
            target, identity, receipt, native_definition, version, _ = self._cached_candidate(Path(tmp))
            parquet = target / "geoparquet/census-block-groups-3.parquet"
            with parquet.open("ab") as stream:
                stream.write(b"tamper")
            with self.assertRaisesRegex(CensusBlockGroupsError, "cached_candidate_native_parquet_tampered"):
                _reuse_existing_candidate(
                    target, identity=identity, receipt=receipt,
                    native_definition=native_definition, version=version,
                )

    def test_cached_candidate_refuses_each_catalog_source_manifest_snapshot_tamper(self):
        for snapshot_kind in ("source_manifest", "versioned_snapshot"):
            with self.subTest(snapshot_kind=snapshot_kind), tempfile.TemporaryDirectory() as tmp:
                target, identity, receipt, native_definition, version, _ = self._cached_candidate(Path(tmp))
                source_manifest = (target / "metadata/source_manifest.json").read_bytes()
                source_dir = target / "metadata/source"
                source_dir.mkdir()
                (source_dir / "source_manifest.json").write_bytes(source_manifest)
                snapshot_name = "version_" + hashlib.sha256(source_manifest).hexdigest() + "_source_manifest.json"
                (source_dir / snapshot_name).write_bytes(source_manifest)
                relative = ("metadata/source/source_manifest.json" if snapshot_kind == "source_manifest"
                            else "metadata/source/" + snapshot_name)
                tampered = target / relative
                tampered.write_bytes(tampered.read_bytes() + b"changed")
                with self.assertRaisesRegex(
                    CensusBlockGroupsError, "cached_candidate_catalog_source_manifest_snapshot_tampered"
                ):
                    _reuse_existing_candidate(
                        target, identity=identity, receipt=receipt,
                        native_definition=native_definition, version=version,
                    )


if __name__ == "__main__":
    unittest.main()

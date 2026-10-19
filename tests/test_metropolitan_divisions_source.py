"""Focused source contract and failure guards for the unapproved draft."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import subprocess

import pyarrow as pa
import shapely

from dagster_hifld.assets.metropolitan_divisions import metdiv_source_integrity_schedule
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.definitions import defs
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import seal_ready_candidate, verify_cached_candidate
from dagster_hifld.sources import metropolitan_divisions as source


class MetropolitanDivisionsSourceTests(unittest.TestCase):
    def setUp(self):
        self.geometry = shapely.to_wkb(
            shapely.MultiPolygon([shapely.Polygon([(0, 0), (1, 0), (0, 1), (0, 0)])])
        )
        self.rows = 37
        props = {
            "OBJECTID": list(range(1, self.rows + 1)),
            "GEOID": [str(1206012054 + i) for i in range(self.rows)],
            "GEOIDFQ": [f"314M700US{1206012054 + i}" for i in range(self.rows)],
            "NAMELSAD": [f"Division {i}" for i in range(self.rows)],
            "ALAND": [float(i + 1) for i in range(self.rows)],
            "AWATER": [float(i) for i in range(self.rows)],
            "INTPTLAT": ["+33.6457373"] * self.rows,
            "INTPTLON": ["-84.2948951"] * self.rows,
            "SHAPE": [self.geometry] * self.rows,
        }
        schema = pa.schema([pa.field(name, typ) for name, typ in zip(source.FIELDS, source.TYPES)])
        self.table = pa.Table.from_pydict(props, schema=schema)
        self.metadata = {"geometry_type": "MultiPolygon", "crs": "EPSG:4269", "fid_column": "OBJECTID"}

    def receipt(self, body=b"x"):
        return {"url": source.URL, "final_url": source.URL, "status": 200, "bytes": len(body),
                "sha256": source.digest(body), "complete_body": True, "observed_at": "2026-10-19T00:00:00Z"}

    def test_registered_source_and_refresh_stopped(self):
        from dagster_hifld.assets.metropolitan_divisions import census_metropolitan_divisions
        self.assertEqual(census_metropolitan_divisions.key.path, ["ingest", source.SLUG, source.SLUG])
        self.assertIn(census_metropolitan_divisions, hifld_ingest_assets)
        self.assertIn(census_metropolitan_divisions.key, {key for asset in defs.assets for key in asset.keys})
        self.assertEqual(metdiv_source_integrity_schedule.default_status.value, "STOPPED")
        self.assertIn(metdiv_source_integrity_schedule.name, {schedule.name for schedule in defs.schedules})

    def test_local_staging_only_and_strict_json(self):
        for storage in (PublishedStorageResource(local_dir="/tmp/never-metdiv"), StagingStorageResource(bucket="never")):
            with patch.object(source, "acquire") as acquire, self.assertRaises(source.MetdivQuarantineError):
                source.stage_metdiv_candidate(storage)
            acquire.assert_not_called()
        for body in (b'{"x":1,"x":2}', b'{"error":{}}', b"[]", b"null"):
            with self.assertRaises(ValueError):
                source.strict_json(body)

    def test_full_native_schema_ids_values_and_geometry_validate(self):
        source.validate_table(self.table, self.metadata)
        variants = [
            self.table.slice(0, 36), self.table.drop(["OBJECTID"]),
            self.table.append_column("unexpected", pa.array([1] * self.rows)),
            self.table.set_column(4, "ALAND", pa.array([1] * self.rows)),
            self.table.set_column(0, "OBJECTID", pa.array([1] * self.rows)),
            self.table.set_column(8, "SHAPE", pa.array([None] * self.rows, type=pa.binary())),
        ]
        for candidate in variants:
            with self.subTest(schema=candidate.schema), self.assertRaises(source.MetdivQuarantineError):
                source.validate_table(candidate, self.metadata)
        with self.assertRaises(source.MetdivQuarantineError):
            source.validate_table(self.table, {**self.metadata, "crs": "EPSG:4326"})
        changed = self.table.set_column(1, "GEOID", pa.array(["bad"] * self.rows))
        with self.assertRaises(source.MetdivQuarantineError):
            source.validate_table(changed, self.metadata)

    def test_missing_renamed_ids_and_geometry_coverage_drift_fail(self):
        for name in ("OBJECTID", "GEOID"):
            with self.subTest(field=name), self.assertRaises(source.MetdivQuarantineError):
                source.validate_table(self.table.drop([name]), self.metadata)
        duplicate_ids = self.table.set_column(0, "OBJECTID", pa.array([1] * self.rows, type=pa.int64()))
        with self.assertRaises(source.MetdivQuarantineError):
            source.validate_table(duplicate_ids, self.metadata)
        invalid = shapely.to_wkb(shapely.MultiPolygon([shapely.Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])]))
        bad_geometry = self.table.set_column(8, "SHAPE", pa.array([invalid] * self.rows, type=pa.binary()))
        with self.assertRaises(source.MetdivQuarantineError):
            source.validate_table(bad_geometry, self.metadata)

    def test_untrusted_or_incomplete_receipt_never_creates_candidate(self):
        changes = ({"observed_at": "not-a-date"}, {"url": "https://example.invalid"},
                   {"final_url": "https://example.invalid"}, {"status": 403}, {"bytes": 99},
                   {"sha256": "0" * 64}, {"complete_body": False})
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            protected = Path(tmp) / "prior-fixture"
            protected.write_bytes(b"keep")
            for change in changes:
                receipt = self.receipt()
                receipt.update(change)
                with patch.object(source, "acquire", return_value=(b"x", receipt)), \
                     patch.object(source, "processing_identity", return_value={"test": True}), \
                     self.assertRaises(ValueError):
                    source.stage_metdiv_candidate(storage)
                self.assertFalse((Path(tmp) / source.SLUG).exists())
                self.assertEqual(protected.read_bytes(), b"keep")

    def test_cached_outputs_are_rederived_from_original_native_rows(self):
        expected = source.expected_parquet_table(self.table)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "geopackage").mkdir()
            (root / "candidate").mkdir()
            gpkg = root / "geopackage" / f"{source.SLUG}.gpkg"
            parquet = root / "candidate" / "typed-source.parquet"
            source.write_native_gpkg(self.table, gpkg)
            source.pq.write_table(expected, parquet)
            source.reconcile_cached_native_outputs(root, self.table)
            altered = self.table.set_column(4, "ALAND", pa.array([999.0] * self.rows))
            source.pq.write_table(source.expected_parquet_table(altered), parquet)
            with self.assertRaises(source.MetdivQuarantineError):
                source.reconcile_cached_native_outputs(root, self.table)

    def test_shared_cache_verifier_accepts_catalog_metadata_but_rejects_unknown_objects(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            version = "v152-test"
            prefix = f"{source.SLUG}/{source.SLUG}/{version}"
            fingerprint = "a" * 64
            state_key = f"_construction-state/{source.SLUG}/{version}.json"
            identity = {"code_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(), "files": {}}
            object_body = b"{}\n"
            storage.write_key(f"{prefix}/metadata/source_manifest.json", object_body)
            for relative in ("metadata/quality_manifest.json", "metadata/data_dictionary.json", "metadata/source/source_manifest.json"):
                storage.write_key(f"{prefix}/{relative}", b"{}\n")
            objects = {"metadata/source_manifest.json": {"bytes": len(object_body), "sha256": source.digest(object_body)}}
            payload = {"processing_identity": identity, "source_record_sha256": "b" * 64,
                       "source_schema_sha256": "c" * 64, "capture_receipts": {}, "objects": objects}
            seal_ready_candidate(storage, state_key, version, fingerprint, payload)
            with patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                result = verify_cached_candidate(storage, Path(__file__).parents[1], state_key, prefix, version,
                    fingerprint, identity, "b" * 64, "c" * 64, objects, {})
            self.assertTrue(result["reused"])
            storage.write_key(f"{prefix}/unexpected/source.json", b"{}\n")
            with patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"), self.assertRaisesRegex(ValueError, "cached_candidate_storage_roster_changed"):
                verify_cached_candidate(storage, Path(__file__).parents[1], state_key, prefix, version,
                    fingerprint, identity, "b" * 64, "c" * 64, objects, {})

    def test_native_definition_rejects_oid_width_or_field_drift(self):
        fields = "".join(
            f"<GPFieldInfoEx><Name>{name}</Name><FieldType>{kind}</FieldType><Length>{length}</Length>"
            f"<IsNullable>{nullable}</IsNullable>{required}</GPFieldInfoEx>"
            for name, kind, length, nullable, required in [
                ("GEOID", "esriFieldTypeString", 10, "true", ""),
                ("GEOIDFQ", "esriFieldTypeString", 19, "true", ""),
                ("NAMELSAD", "esriFieldTypeString", 100, "true", ""),
                ("ALAND", "esriFieldTypeDouble", 8, "true", ""),
                ("AWATER", "esriFieldTypeDouble", 8, "true", ""),
                ("INTPTLAT", "esriFieldTypeString", 11, "true", ""),
                ("INTPTLON", "esriFieldTypeString", 12, "true", ""),
                ("OBJECTID", "esriFieldTypeOID", 4, "false", "<Required>true</Required><Editable>false</Editable>"),
                ("SHAPE", "esriFieldTypeGeometry", 0, "true", ""),
            ]
        )
        xml = f'<DEFeatureClassInfo><OIDFieldName>OBJECTID</OIDFieldName><HasZ>false</HasZ><HasM>false</HasM><GPFieldInfoExs>{fields}</GPFieldInfoExs></DEFeatureClassInfo>'
        with patch.object(source.pyogrio, "read_arrow", return_value=({}, pa.table({"FIELD_1": [xml]}))):
            declaration = source.read_native_definition(Path("/synthetic-unit-only.zip"))
        self.assertEqual(declaration["fields"]["OBJECTID"]["Length"], "4")
        self.assertEqual(source.TYPES[0], pa.int64())
        changed = xml.replace("<Length>4</Length>", "<Length>8</Length>")
        with patch.object(source.pyogrio, "read_arrow", return_value=({}, pa.table({"FIELD_1": [changed]}))), self.assertRaises(ValueError):
            source.read_native_definition(Path("/synthetic-unit-only.zip"))


if __name__ == "__main__":
    unittest.main()

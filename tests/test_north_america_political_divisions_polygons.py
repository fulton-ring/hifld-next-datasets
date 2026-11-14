import hashlib
import io
import json
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from dagster import DefaultScheduleStatus

from dagster_hifld.assets.north_america_political_divisions_polygons import (
    cec_north_america_political_divisions_polygons,
    cec_north_america_political_divisions_polygons_draft_schedule,
    cec_north_america_political_divisions_polygons_complete,
)
from dagster_hifld.definitions import defs
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import north_america_political_divisions_polygons as source_module
from dagster_hifld.sources.north_america_political_divisions_polygons import (
    BOUNDS, CODE_IDENTITY_SCHEME, DATASET_SLUG, EXPECTED_BYTES, EXPECTED_COUNTRY_COUNTS,
    EXPECTED_CPG, EXPECTED_FIELDS, EXPECTED_FEATURE_COUNT, EXPECTED_INVALID_GEOMETRY_COUNT,
    EXPECTED_INVALID_REASON, EXPECTED_LAYER, EXPECTED_MULTIPOLYGON_COUNT, EXPECTED_POLYGON_COUNT,
    EXPECTED_PRJ, EXPECTED_RING_COUNT, EXPECTED_SHA256, EXPECTED_UNCOMPRESSED_BYTES,
    EXPECTED_VERTEX_COUNT, FILE_SLUG, GEOMETRY_SHA, ID_SHA, MEMBERS, RECORD_SHA, SHP_MEMBERS,
    SOURCE_PAGE, SOURCE_URL, _capture, _inspect_archive, _schema_dbf, candidate_version,
    capture_north_america_political_divisions_polygons, processing_code_identity,
)


def _receipt(body: bytes, wire: bytes, url: str = SOURCE_URL) -> dict:
    return {
        "method": "GET", "url": url, "final_url": url, "status": 200,
        "complete_body": True, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
        "content_encoding": "identity", "wire_bytes": len(wire),
        "wire_sha256": hashlib.sha256(wire).hexdigest(), "content_length": len(body),
        "observed_at": "2026-11-13T00:00:00+00:00",
    }


class NorthAmericaPoliticalDivisionsPolygonsSourceContractTest(unittest.TestCase):
    def test_pinned_source_and_complete_native_contract(self):
        self.assertEqual(DATASET_SLUG, "north-america-political-divisions-polygons")
        self.assertEqual(FILE_SLUG, DATASET_SLUG)
        self.assertEqual(SOURCE_PAGE, "https://www.cec.org/north-american-environmental-atlas/political-boundaries-2021/")
        self.assertEqual(
            SOURCE_URL,
            "https://www.cec.org/files/atlas_layers/0_reference/0_01_political_boundaries/politicalboundaries_shapefile.zip")
        self.assertEqual(EXPECTED_BYTES, 184_774_593)
        self.assertEqual(EXPECTED_SHA256,
                         "29f3aec70604ed5be9e18c22e2d504da883fedd6d81aff27156456b4899a0c28")
        self.assertEqual(EXPECTED_UNCOMPRESSED_BYTES, 252_775_497)
        self.assertEqual(EXPECTED_FEATURE_COUNT, 98)
        self.assertEqual(EXPECTED_LAYER, "boundaries_p_2021_v3")
        self.assertEqual(EXPECTED_CPG, b"UTF-8")
        self.assertEqual(EXPECTED_FIELDS, [
            ("NAME_En", "C", 254, 0), ("NAME_Es", "C", 254, 0), ("NAME_Fr", "C", 254, 0),
            ("COUNTRY", "C", 20, 0), ("STATEABB", "C", 20, 0), ("CAPITAL", "C", 254, 0)])
        self.assertTrue(EXPECTED_PRJ.startswith('PROJCS["Sphere_ARC_INFO_Lambert_Azimuthal_Equal_Area"'))
        self.assertIn("PARAMETER[\"Central_Meridian\",-100.0]", EXPECTED_PRJ)
        self.assertEqual(EXPECTED_POLYGON_COUNT + EXPECTED_MULTIPOLYGON_COUNT, EXPECTED_FEATURE_COUNT)
        self.assertEqual(EXPECTED_RING_COUNT, 164_361)
        self.assertEqual(EXPECTED_VERTEX_COUNT, 15_722_772)
        self.assertEqual(EXPECTED_INVALID_GEOMETRY_COUNT, 1)
        self.assertEqual(EXPECTED_INVALID_REASON, "Ring Self-intersection[448287.487127878 3092671.08586955]")
        self.assertEqual(EXPECTED_COUNTRY_COUNTS, {"CAN": 13, "MEX": 32, "USA": 53})
        self.assertTrue(BOUNDS[0] < BOUNDS[2] and BOUNDS[1] < BOUNDS[3])
        for value in (ID_SHA, RECORD_SHA, GEOMETRY_SHA, EXPECTED_SHA256):
            self.assertEqual(len(value), 64)
        self.assertEqual(len(MEMBERS), 20)
        self.assertIn("politicalboundaries_shapefile/", MEMBERS)
        for suffix in ("CPG", "dbf", "prj", "sbn", "sbx", "shp", "shp.xml", "shx"):
            self.assertIn(f"politicalboundaries_shapefile/NA_PoliticalDivisions/data/boundaries_p_2021_v3.{suffix}", SHP_MEMBERS)
        self.assertIn("politicalboundaries_shapefile/Terms of use.doc", MEMBERS)

    def test_packaged_code_identity_and_candidate_version_need_no_git(self):
        with patch("subprocess.check_output", side_effect=FileNotFoundError("git")), \
             patch("subprocess.run", side_effect=FileNotFoundError("git")):
            identity = processing_code_identity()
            self.assertRegex(identity, r"^[0-9a-f]{64}$")
            self.assertRegex(candidate_version(), r"^v[0-9a-f]{32}$")

    def test_packaged_identity_resolves_from_source_package_without_dot_git(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "src"
            package = root / "dagster_hifld"
            shutil.copytree(source_module.PROCESSING_SOURCE_PACKAGE, package)
            self.assertFalse((root / ".git").exists())
            with patch.object(source_module, "PROCESSING_SOURCE_ROOT", root), \
                 patch.object(source_module, "PROCESSING_SOURCE_PACKAGE", package):
                identity = source_module.processing_code_identity()
                self.assertEqual(identity, processing_code_identity())
                self.assertEqual(source_module.candidate_version(), candidate_version())

    def test_dbf_schema_reader_rejects_truncated_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "x"
            root.mkdir()
            (root / "t.dbf").write_bytes(b"\x03" + b"\x00" * 8)
            with self.assertRaises(ValueError):
                _schema_dbf(root / "t.dbf")


class NorthAmericaPoliticalDivisionsPolygonsDriftTest(unittest.TestCase):
    def test_archive_member_roster_drift_is_rejected_before_extract(self):
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w") as archive:
            archive.writestr("unexpected.txt", "not the CEC archive")
        forged = sink.getvalue()
        pins = {
            "EXPECTED_SHA256": hashlib.sha256(forged).hexdigest(),
            "EXPECTED_BYTES": len(forged),
        }
        with patch.object(source_module, "EXPECTED_SHA256", pins["EXPECTED_SHA256"]), \
             patch.object(source_module, "EXPECTED_BYTES", pins["EXPECTED_BYTES"]):
            with self.assertRaisesRegex(ValueError, "cec_archive_member_roster_drift"):
                _inspect_archive(forged)

    def test_archive_unsafe_member_is_rejected(self):
        body = b"x" * 10
        receipt = _receipt(body, b"wire" * 10)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))

            def unsafe_capture(_client, _url, root, _name, **_kwargs):
                root.mkdir(parents=True, exist_ok=True)
                forged = io.BytesIO()
                with zipfile.ZipFile(forged, "w") as archive:
                    archive.writestr("../escape.shp", "x")
                tampered = forged.getvalue()
                saved = _receipt(tampered, tampered)
                (root / "source.body").write_bytes(tampered)
                (root / "source.wire").write_bytes(tampered)
                (root / "source.receipt.json").write_text(json.dumps(saved))
                return tampered, saved

            with patch("dagster_hifld.sources.north_america_political_divisions_polygons.capture_get",
                       side_effect=unsafe_capture):
                with self.assertRaisesRegex(ValueError, "official_cec_archive_identity_or_release_drift"):
                    _capture(None, storage, "vtest")

    def test_archive_capture_rejects_changed_url_or_bytes_and_quarantines(self):
        body = b"not the pinned CEC archive"
        wire = b"wire bytes for changed archive"
        receipt = _receipt(body, wire)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))

            def changed_capture(_client, _url, root, _name, **_kwargs):
                root.mkdir(parents=True, exist_ok=True)
                (root / "source.body").write_bytes(body)
                (root / "source.wire").write_bytes(wire)
                (root / "source.receipt.json").write_text(json.dumps(receipt))
                return body, receipt

            with patch("dagster_hifld.sources.north_america_political_divisions_polygons.capture_get",
                       side_effect=changed_capture):
                with self.assertRaisesRegex(ValueError, "official_cec_archive_identity_or_release_drift"):
                    _capture(None, storage, "vtest")

            keys = storage.list_prefix("_quarantine/north-america-political-divisions-polygons")
            self.assertEqual(len(keys), 4)
            body_key = next(key for key in keys if key.endswith("/raw/source.body"))
            wire_key = next(key for key in keys if key.endswith("/raw/source.wire"))
            receipt_key = next(key for key in keys if key.endswith("/raw/source.receipt.json"))
            failure_key = next(key for key in keys if key.endswith("/metadata/failure.json"))
            self.assertEqual(storage.read_key(body_key), body)
            self.assertEqual(storage.read_key(wire_key), wire)
            self.assertEqual(json.loads(storage.read_key(receipt_key)), receipt)
            self.assertEqual(
                json.loads(storage.read_key(failure_key))["reason_code"],
                "official_cec_archive_identity_or_release_drift")

    def test_pinned_inspection_accepts_the_exact_captured_archive(self):
        archive_path = Path("/data/workspace/scratch/cec_napd_shapefile.zip")
        if not archive_path.is_file():
            self.skipTest("pinned CEC capture not present in this environment")
        body = archive_path.read_bytes()
        audit = _inspect_archive(body)
        self.assertEqual(audit["feature_count"], 98)
        self.assertEqual(audit["source_id_sha256"], ID_SHA)
        self.assertEqual(audit["source_record_sha256"], RECORD_SHA)
        self.assertEqual(audit["source_geometry_sha256"], GEOMETRY_SHA)
        self.assertEqual(audit["bounds"], BOUNDS)
        self.assertEqual(audit["invalid_geometry_count"], 1)
        self.assertEqual(audit["invalid_geometry_reasons"], [EXPECTED_INVALID_REASON])
        self.assertEqual(audit["country_counts"], EXPECTED_COUNTRY_COUNTS)
        self.assertTrue(audit["stateabb_unique"])

    def test_disk_floor_refuses_capture_on_small_disk(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(tmp))
            with patch("dagster_hifld.sources.north_america_political_divisions_polygons.shutil.disk_usage") as usage:
                usage.return_value.free = 1
                with self.assertRaisesRegex(ValueError, "disk_floor_before_acquisition"):
                    capture_north_america_political_divisions_polygons(storage, version="vtest")


class NorthAmericaPoliticalDivisionsPolygonsRegistrationTest(unittest.TestCase):
    def test_source_asset_is_registered_with_stopped_refresh(self):
        keys = {key for definition in hifld_ingest_assets for key in definition.keys}
        self.assertIn(next(iter(cec_north_america_political_divisions_polygons.keys)), keys)
        self.assertEqual(
            cec_north_america_political_divisions_polygons_draft_schedule.default_status,
            DefaultScheduleStatus.STOPPED)
        self.assertIn(
            "cec_north_america_political_divisions_polygons_draft_refresh",
            {job.name for job in defs.resolve_all_job_defs()},
        )
        self.assertEqual(
            defs.resolve_schedule_def("cec_north_america_political_divisions_polygons_draft_refresh_schedule").default_status,
            DefaultScheduleStatus.STOPPED,
        )
        self.assertIn(
            ("ingest/north-america-political-divisions-polygons/north-america-political-divisions-polygons",
             "cec_north_america_political_divisions_polygons_complete"),
            {
                (key.asset_key.to_user_string(), key.name)
                for key in defs.resolve_asset_graph().asset_check_keys
            },
        )

    def test_source_check_requires_pinned_archive_and_complete_population(self):
        candidate = {
            "feature_count": EXPECTED_FEATURE_COUNT,
            "source_population_complete": True,
            "source_archive_bytes": EXPECTED_BYTES,
            "source_archive_sha256": EXPECTED_SHA256,
            "source_id_sha256": "0" * 64,
            "source_schema_sha256": "1" * 64,
        }
        self.assertTrue(cec_north_america_political_divisions_polygons_complete(candidate).passed)
        candidate["feature_count"] -= 1
        self.assertFalse(cec_north_america_political_divisions_polygons_complete(candidate).passed)


if __name__ == "__main__":
    unittest.main()

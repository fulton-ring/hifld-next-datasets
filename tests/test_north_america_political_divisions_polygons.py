import hashlib
import io
import json
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import fiona
import pyproj
from dagster import AssetCheckSeverity, DefaultScheduleStatus

from dagster_hifld.assets.north_america_political_divisions_polygons import (
    cec_north_america_political_divisions_polygons,
    cec_north_america_political_divisions_polygons_approved_baseline_drift,
    cec_north_america_political_divisions_polygons_draft_schedule,
    cec_north_america_political_divisions_polygons_complete,
)
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.definitions import defs
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import north_america_political_divisions_polygons as source_module
from dagster_hifld.sources.north_america_political_divisions_polygons import (
    CODE_IDENTITY_SCHEME,
    DATASET_SLUG,
    EXPECTED_CPG,
    EXPECTED_FIELDS,
    EXPECTED_LAYER,
    EXPECTED_PRJ,
    FILE_SLUG,
    MEMBERS,
    PREFIX,
    SHP_MEMBERS,
    SOURCE_FILE,
    SOURCE_PAGE,
    SOURCE_URL,
    VERSION_LABEL,
    _capture,
    _inspect_archive,
    _schema_dbf,
    capture_north_america_political_divisions_polygons,
    processing_code_identity,
)

CAPTURE_PATH = "dagster_hifld.sources.north_america_political_divisions_polygons.capture_get"


def _receipt(body: bytes, wire: bytes, url: str = SOURCE_URL,
             bytes_field: int | None = None) -> dict:
    return {
        "method": "GET", "url": url, "final_url": url, "status": 200,
        "complete_body": True, "bytes": len(body) if bytes_field is None else bytes_field,
        "sha256": hashlib.sha256(body).hexdigest(),
        "content_encoding": "identity", "wire_bytes": len(wire),
        "wire_sha256": hashlib.sha256(wire).hexdigest(), "content_length": len(body),
        "observed_at": "2026-11-13T00:00:00+00:00",
    }


def _mock_capture_get(body: bytes, url: str = SOURCE_URL, bytes_field: int | None = None):
    def fake_capture_get(_client, _requested_url, root, _name, **_kwargs):
        root.mkdir(parents=True, exist_ok=True)
        wire = b"wire:" + body
        receipt = _receipt(body, wire, url, bytes_field)
        (root / "source.body").write_bytes(body)
        (root / "source.wire").write_bytes(wire)
        (root / "source.receipt.json").write_text(json.dumps(receipt))
        return body, receipt

    return fake_capture_get


def _write_fixture_shapefile(directory: Path, feature_count: int) -> None:
    """Write one synthetic CEC-layout shapefile (exact fields/widths, PRJ, CPG)."""
    schema = {"geometry": "Polygon", "properties": {
        "NAME_En": "str:254", "NAME_Es": "str:254", "NAME_Fr": "str:254",
        "COUNTRY": "str:20", "STATEABB": "str:20", "CAPITAL": "str:254"}}
    with fiona.open(str(directory / "boundaries_p_2021_v3.shp"), "w",
                    driver="ESRI Shapefile", schema=schema,
                    crs=pyproj.CRS.from_wkt(EXPECTED_PRJ), encoding="utf-8") as c:
        for i in range(feature_count):
            x, y = (i % 10) * 1000.0, (i // 10) * 1000.0
            c.write({
                "type": "Feature",
                "properties": {
                    "NAME_En": f"Feature {i}", "NAME_Es": f"Feature Es {i}",
                    "NAME_Fr": f"Feature Fr {i}",
                    "COUNTRY": ("CAN", "MEX", "USA")[i % 3],
                    "STATEABB": f"AB{i:03d}", "CAPITAL": f"City {i}",
                },
                "geometry": {"type": "Polygon", "coordinates": [[
                    [x, y], [x + 500.0, y], [x + 500.0, y + 500.0], [x, y + 500.0], [x, y],
                ]]},
            })
    cpg = directory / "boundaries_p_2021_v3.cpg"
    if cpg.exists():
        (directory / "boundaries_p_2021_v3.CPG").write_bytes(cpg.read_bytes())
        cpg.unlink()
    (directory / "boundaries_p_2021_v3.prj").write_text(EXPECTED_PRJ)
    (directory / "boundaries_p_2021_v3.sbn").write_bytes(b"")
    (directory / "boundaries_p_2021_v3.sbx").write_bytes(b"")
    (directory / "boundaries_p_2021_v3.shp.xml").write_text("<ISO-metadata-placeholder/>")


def _fixture_archive(feature_count: int) -> bytes:
    sink = io.BytesIO()
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        _write_fixture_shapefile(data_dir, feature_count)
        with zipfile.ZipFile(sink, "w") as archive:
            for member in sorted(MEMBERS):
                if member.endswith("/"):
                    archive.writestr(member, b"")
                elif member.startswith(PREFIX):
                    archive.write(data_dir / ("boundaries_p_2021_v3." + member[len(PREFIX):]), member)
                else:
                    archive.writestr(member, b"fixture placeholder for " + member.encode("utf-8"))
    return sink.getvalue()


class NorthAmericaPoliticalDivisionsPolygonsSourceContractTest(unittest.TestCase):
    def test_maintained_source_and_complete_native_contract(self):
        self.assertEqual(DATASET_SLUG, "north-america-political-divisions-polygons")
        self.assertEqual(FILE_SLUG, DATASET_SLUG)
        self.assertEqual(SOURCE_PAGE, "https://www.cec.org/north-american-environmental-atlas/political-boundaries-2021/")
        self.assertEqual(
            SOURCE_URL,
            "https://www.cec.org/files/atlas_layers/0_reference/0_01_political_boundaries/politicalboundaries_shapefile.zip")
        self.assertEqual(VERSION_LABEL, "v20221026")
        self.assertEqual(EXPECTED_LAYER, "boundaries_p_2021_v3")
        self.assertEqual(EXPECTED_CPG, b"UTF-8")
        self.assertEqual(EXPECTED_FIELDS, [
            ("NAME_En", "C", 254, 0), ("NAME_Es", "C", 254, 0), ("NAME_Fr", "C", 254, 0),
            ("COUNTRY", "C", 20, 0), ("STATEABB", "C", 20, 0), ("CAPITAL", "C", 254, 0)])
        self.assertTrue(EXPECTED_PRJ.startswith('PROJCS["Sphere_ARC_INFO_Lambert_Azimuthal_Equal_Area"'))
        self.assertIn("PARAMETER[\"Central_Meridian\",-100.0]", EXPECTED_PRJ)
        self.assertEqual(len(MEMBERS), 20)
        self.assertIn("politicalboundaries_shapefile/", MEMBERS)
        for suffix in ("CPG", "dbf", "prj", "sbn", "sbx", "shp", "shp.xml", "shx"):
            self.assertIn(f"politicalboundaries_shapefile/NA_PoliticalDivisions/data/boundaries_p_2021_v3.{suffix}", SHP_MEMBERS)
        self.assertIn("politicalboundaries_shapefile/Terms of use.doc", MEMBERS)

    def test_no_module_level_population_or_snapshot_pins(self):
        for name in (
            "EXPECTED_SHA256", "EXPECTED_BYTES", "EXPECTED_UNCOMPRESSED_BYTES",
            "EXPECTED_FEATURE_COUNT", "EXPECTED_COUNTRY_COUNTS", "EXPECTED_RING_COUNT",
            "EXPECTED_VERTEX_COUNT", "EXPECTED_POLYGON_COUNT", "EXPECTED_MULTIPOLYGON_COUNT",
            "EXPECTED_INVALID_GEOMETRY_COUNT", "EXPECTED_INVALID_REASON",
            "ID_SHA", "RECORD_SHA", "GEOMETRY_SHA", "BOUNDS", "candidate_version",
        ):
            self.assertFalse(hasattr(source_module, name), name)

    def test_packaged_code_identity_need_no_git(self):
        with patch("subprocess.check_output", side_effect=FileNotFoundError("git")), \
             patch("subprocess.run", side_effect=FileNotFoundError("git")):
            identity = processing_code_identity()
            self.assertRegex(identity, r"^[0-9a-f]{64}$")
            self.assertEqual(CODE_IDENTITY_SCHEME, "sha256-packaged-dagster_hifld-py-v1")

    def test_packaged_identity_resolves_from_source_package_without_dot_git(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "src"
            package = root / "dagster_hifld"
            shutil.copytree(source_module.PROCESSING_SOURCE_PACKAGE, package)
            self.assertFalse((root / ".git").exists())
            with patch.object(source_module, "PROCESSING_SOURCE_ROOT", root), \
                 patch.object(source_module, "PROCESSING_SOURCE_PACKAGE", package):
                self.assertEqual(source_module.processing_code_identity(), processing_code_identity())

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
        with self.assertRaisesRegex(ValueError, "cec_archive_member_roster_drift"):
            _inspect_archive(sink.getvalue())

    def test_archive_unsafe_member_is_rejected(self):
        forged = io.BytesIO()
        with zipfile.ZipFile(forged, "w") as archive:
            archive.writestr("../escape.shp", "x")
        body = forged.getvalue()
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            with patch(CAPTURE_PATH, side_effect=_mock_capture_get(body)):
                with self.assertRaisesRegex(ValueError, "cec_archive_unsafe_member"):
                    capture_north_america_political_divisions_polygons(storage, version=VERSION_LABEL)

    def test_transport_receipt_drift_quarantines(self):
        body = _fixture_archive(2)
        wire = b"wire bytes for drifted receipt"
        receipt = _receipt(body, wire, bytes_field=len(body) + 1)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))

            def drifted_capture(_client, _url, root, _name, **_kwargs):
                root.mkdir(parents=True, exist_ok=True)
                (root / "source.body").write_bytes(body)
                (root / "source.wire").write_bytes(wire)
                (root / "source.receipt.json").write_text(json.dumps(receipt))
                return body, receipt

            with patch(CAPTURE_PATH, side_effect=drifted_capture):
                with self.assertRaisesRegex(ValueError, "official_cec_transport_receipt_drift"):
                    _capture(None, storage, VERSION_LABEL)

            keys = storage.list_prefix(f"_quarantine/{DATASET_SLUG}")
            self.assertEqual(len(keys), 4)
            failure_key = next(key for key in keys if key.endswith("/metadata/failure.json"))
            self.assertEqual(
                json.loads(storage.read_key(failure_key))["reason_code"],
                "official_cec_transport_receipt_drift")

    def test_capture_rejects_changed_url_and_quarantines(self):
        body = _fixture_archive(2)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            with patch(CAPTURE_PATH, side_effect=_mock_capture_get(
                    body, url="https://www.cec.org/some/other/path.zip")):
                with self.assertRaisesRegex(ValueError, "official_cec_transport_receipt_drift"):
                    capture_north_america_political_divisions_polygons(storage, version=VERSION_LABEL)
            keys = storage.list_prefix(f"_quarantine/{DATASET_SLUG}")
            self.assertEqual(len(keys), 4)

    def test_disk_floor_refuses_capture_on_small_disk(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(tmp))
            with patch("dagster_hifld.sources.north_america_political_divisions_polygons.shutil.disk_usage") as usage:
                usage.return_value.free = 1
                with self.assertRaisesRegex(ValueError, "disk_floor_before_acquisition"):
                    capture_north_america_political_divisions_polygons(storage, version=VERSION_LABEL)

    def test_fixture_inspection_records_capture_time_witnesses(self):
        body = _fixture_archive(98)
        audit = _inspect_archive(body)
        self.assertEqual(audit["feature_count"], 98)
        self.assertEqual(audit["source_reported_count"], 98)
        self.assertTrue(audit["source_population_complete"])
        self.assertEqual(audit["source_archive_bytes"], len(body))
        self.assertEqual(audit["source_archive_sha256"], hashlib.sha256(body).hexdigest())
        self.assertEqual(len(audit["source_id_sha256"]), 64)
        self.assertEqual(len(audit["source_record_sha256"]), 64)
        self.assertEqual(len(audit["source_geometry_sha256"]), 64)
        self.assertTrue(audit["stateabb_unique"])
        self.assertEqual(sum(audit["country_counts"].values()), 98)
        self.assertEqual(audit["null_geometry_count"], 0)
        self.assertEqual(audit["empty_geometry_count"], 0)
        self.assertEqual(audit["invalid_geometry_count"], 0)
        self.assertEqual(audit["ring_count"], 98)
        self.assertEqual(audit["vertex_count"], 98 * 5)


class NorthAmericaPoliticalDivisionsPolygonsCaptureTest(unittest.TestCase):
    def test_fixture_capture_passes_witness_and_stages_content_version(self):
        body = _fixture_archive(98)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            with patch(CAPTURE_PATH, side_effect=_mock_capture_get(body)):
                result = capture_north_america_political_divisions_polygons(storage, version=VERSION_LABEL)
            self.assertEqual(result["feature_count"], 98)
            self.assertEqual(result["source_reported_count"], 98)
            self.assertTrue(result["source_population_complete"])
            self.assertRegex(result["version"], r"^v[0-9a-f]{32}$")
            self.assertEqual(result["source_archive_sha256"], hashlib.sha256(body).hexdigest())
            self.assertEqual(result["source_archive_bytes"], len(body))
            self.assertEqual(result["source_capture_evidence_sha256"], result["source_archive_sha256"])
            prefix = f"{DATASET_SLUG}/{FILE_SLUG}/{result['version']}"
            self.assertEqual(result["code_identity_scheme"], CODE_IDENTITY_SCHEME)
            self.assertEqual(result["code_sha256"], processing_code_identity())
            self.assertTrue(storage.object_exists(f"{prefix}/shapefile/{SOURCE_FILE}"))
            self.assertEqual(storage.sha256_key(f"{prefix}/shapefile/{SOURCE_FILE}"),
                             hashlib.sha256(body).hexdigest())
            manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
            self.assertEqual(manifest["inputs"][0]["sha256"], hashlib.sha256(body).hexdigest())
            self.assertEqual(manifest["inputs"][0]["bytes"], len(body))
            audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
            self.assertEqual(audit["source_reported_count"], 98)
            self.assertTrue(audit["stateabb_unique"])
            self.assertEqual(audit["source"]["archive_sha256"], hashlib.sha256(body).hexdigest())
            self.assertTrue(cec_north_america_political_divisions_polygons_complete(result).passed)
            for flag in ("source_release_accepted", "release_accepted"):
                self.assertFalse(result[flag])

    def test_replay_of_retained_capture_directory_does_not_refetch(self):
        body = _fixture_archive(98)
        with tempfile.TemporaryDirectory() as tmp:
            capture_dir = Path(tmp) / "capture"
            wire = b"wire:" + body
            capture_dir.mkdir()
            (capture_dir / "source.body").write_bytes(body)
            (capture_dir / "source.wire").write_bytes(wire)
            (capture_dir / "source.receipt.json").write_text(
                json.dumps(_receipt(body, wire)))
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            with patch(CAPTURE_PATH, side_effect=AssertionError("replay must not refetch")):
                result = capture_north_america_political_divisions_polygons(
                    storage, version=VERSION_LABEL, capture_directory=capture_dir)
            self.assertEqual(result["feature_count"], 98)
            self.assertTrue(result["source_cache_reused"] is False)

    def test_upstream_added_feature_passes_completeness_and_warns_drift(self):
        base_body = _fixture_archive(98)
        edited_body = _fixture_archive(99)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base_storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging-base"))
            with patch(CAPTURE_PATH, side_effect=_mock_capture_get(base_body)):
                base = capture_north_america_political_divisions_polygons(base_storage, version=VERSION_LABEL)
            edited_storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging-edited"))
            with patch(CAPTURE_PATH, side_effect=_mock_capture_get(edited_body)):
                edited = capture_north_america_political_divisions_polygons(edited_storage, version=VERSION_LABEL)
        # One upstream feature added: the capture passes completeness on the
        # new population instead of failing a count pin.
        self.assertEqual(edited["feature_count"], 99)
        self.assertEqual(edited["source_reported_count"], 99)
        self.assertTrue(edited["source_population_complete"])
        self.assertTrue(cec_north_america_political_divisions_polygons_complete(edited).passed)
        self.assertNotEqual(edited["version"], base["version"])
        # The approved-baseline drift check surfaces the edit as a WARN.
        baseline = {
            "release_accepted": True,
            "version": base["version"],
            "dataset_slug": base["dataset_slug"],
            "file_slug": base["file_slug"],
            "feature_count": base["feature_count"],
            "source_id_sha256": base["source_id_sha256"],
        }
        drift = approved_baseline_drift(edited, baseline)
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["feature_count_delta"].value, 1)
        self.assertTrue(drift.metadata["native_id_roster_changed"].value)
        # The registered check reports an explicit skip without an approved baseline.
        skipped = cec_north_america_political_divisions_polygons_approved_baseline_drift(edited)
        self.assertEqual(skipped.severity, AssetCheckSeverity.WARN)
        self.assertEqual(skipped.metadata["comparison_status"].value, "skipped_no_approved_baseline")


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
        self.assertIn(
            ("ingest/north-america-political-divisions-polygons/north-america-political-divisions-polygons",
             "cec_north_america_political_divisions_polygons_approved_baseline_drift"),
            {
                (key.asset_key.to_user_string(), key.name)
                for key in defs.resolve_asset_graph().asset_check_keys
            },
        )

    def test_source_check_requires_witnessed_population(self):
        candidate = {
            "feature_count": 5,
            "source_reported_count": 5,
            "source_population_complete": True,
            "source_id_sha256": "0" * 64,
            "source_schema_sha256": "1" * 64,
        }
        self.assertTrue(cec_north_america_political_divisions_polygons_complete(candidate).passed)
        candidate["feature_count"] = 4
        self.assertFalse(cec_north_america_political_divisions_polygons_complete(candidate).passed)
        candidate["feature_count"] = 5
        candidate["source_reported_count"] = 6
        self.assertFalse(cec_north_america_political_divisions_polygons_complete(candidate).passed)
        candidate["source_reported_count"] = 5
        candidate["source_population_complete"] = False
        self.assertFalse(cec_north_america_political_divisions_polygons_complete(candidate).passed)


if __name__ == "__main__":
    unittest.main()

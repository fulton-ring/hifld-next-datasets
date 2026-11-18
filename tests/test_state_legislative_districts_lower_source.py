from __future__ import annotations

import hashlib
import io
import json
import shutil
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from dagster import DefaultScheduleStatus

from dagster_hifld.definitions import defs
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.state_legislative_districts_lower import (
    state_legislative_districts_lower_draft_schedule,
    state_legislative_districts_lower_source_complete,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import census_latest
from dagster_hifld.sources import state_legislative_districts_lower as source
from dagster_hifld.sources.state_legislative_districts_lower import (
    _capture,
    _extract_archive,
    _validate_archive,
    _validate_domain_and_ids,
    _build_geoparquet,
    _assert_full_parity,
    processing_code_identity,
)


def _release(vintage=2026):
    return census_latest.GeodatabaseRelease(
        vintage=vintage, product="legislative",
        source_file=f"tlgdb_{vintage}_us_legislative.gdb.zip",
        source_url=f"https://www2.census.gov/geo/tiger/TGRGDB{vintage % 100:02d}/tlgdb_{vintage}_us_legislative.gdb.zip",
        directory_url=f"https://www2.census.gov/geo/tiger/TGRGDB{vintage % 100:02d}/",
        listing_sha256="", listing_bytes=0,
        latest_checked_at="2026-11-15T00:00:00+00:00", annual_listing_sha256="",
    )


def _set(table, name, array):
    index = table.schema.get_field_index(name)
    return table.set_column(index, table.schema.field(index), array)


def _multi(*boxes) -> bytes:
    return shapely.to_wkb(shapely.MultiPolygon([shapely.box(*box) for box in boxes]))


class _TableFixture:
    @staticmethod
    def table(geoids=("01001", "02002"), mtfcc="G5220", lsy="2026"):
        count = len(geoids)
        names = source.EXPECTED_FIELDS
        columns = [
            pa.array([geoid[:2] for geoid in geoids], type=pa.string()),
            pa.array([geoid[2:] for geoid in geoids], type=pa.string()),
            pa.array(list(geoids), type=pa.string()),
            pa.array([f"1500000US{geoid}" for geoid in geoids], type=pa.string()),
            pa.array([f"House District {index}" for index in range(1, count + 1)], type=pa.string()),
            pa.array(["LL"] * count, type=pa.string()),
            pa.array([lsy] * count, type=pa.string()),
            pa.array([mtfcc] * count, type=pa.string()),
            pa.array(["N"] * count, type=pa.string()),
            pa.array([100.0 + index for index in range(count)], type=pa.float64()),
            pa.array([3.0 + index for index in range(count)], type=pa.float64()),
            pa.array([f"+4{index}.0000000" for index in range(count)], type=pa.string()),
            pa.array([f"-10{index}.0000000" for index in range(count)], type=pa.string()),
        ]
        boxes = [(-101.0 + index, 39.0, -100.0 + index, 40.0) for index in range(count)]
        columns.append(pa.array([_multi(box) for box in boxes], type=pa.binary()))
        return pa.Table.from_arrays(columns, names=[*names, "SHAPE"])


class StateLegislativeDistrictsLowerBuildTests(unittest.TestCase):
    def test_build_geoparquet_preserves_attributes_ids_and_native_wkb(self):
        table = _TableFixture.table()
        ids = [7, 8]
        body, schema = source._build_geoparquet(table, ids, expected_count=2, expected_bounds=[-101.0, 39.0, -99.0, 40.0])
        result = pq.read_table(pa.BufferReader(body))
        self.assertEqual(result.column_names, source.EXPECTED_OUTPUT_FIELDS)
        self.assertEqual(result.column("OBJECTID").to_pylist(), ids)
        self.assertEqual(result.column("GEOID").to_pylist(), ["01001", "02002"])
        self.assertEqual(result.column("SLDLST").to_pylist(), ["001", "002"])
        self.assertEqual(result.column("STATEFP").to_pylist(), ["01", "02"])
        self.assertEqual(result.column("ALAND").to_pylist(), table.column("ALAND").to_pylist())
        self.assertEqual(result.column("geometry").to_pylist(), table.column("SHAPE").to_pylist())
        self.assertEqual(schema["geometry_bbox"], [-101.0, 39.0, -99.0, 40.0])
        geo = json.loads(result.schema.metadata[b"geo"].decode())
        self.assertEqual(geo["primary_column"], "geometry")
        self.assertEqual(geo["columns"]["geometry"]["encoding"], "WKB")
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["MultiPolygon"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4269)

    def test_build_geoparquet_rejects_unexpected_geometry_type_and_bad_wkb(self):
        table = _TableFixture.table()
        polygon = shapely.to_wkb(shapely.box(-101, 39, -100, 40))
        replaced = _set(table, "SHAPE", pa.array([polygon, table.column("SHAPE")[1].as_py()], type=pa.binary()))
        with self.assertRaisesRegex(ValueError, "source_geometry_type_or_validity_drift"):
            _build_geoparquet(replaced, [1, 2], expected_count=2, expected_bounds=None)
        with self.assertRaisesRegex(ValueError, "source_geometry_wkb_invalid"):
            _build_geoparquet(
                _set(table, "SHAPE", pa.array([b"not-wkb", table.column("SHAPE")[1].as_py()], type=pa.binary())),
                [1, 2], expected_count=2, expected_bounds=None,
            )

    def test_build_geoparquet_rejects_wrong_coverage_extent(self):
        table = _TableFixture.table()
        with self.assertRaisesRegex(ValueError, "source_geometry_coverage_extent_drift"):
            _build_geoparquet(table, [1, 2], expected_count=2, expected_bounds=[0.0, 0.0, 1.0, 1.0])

    def test_parity_assert_detects_attribute_geometry_and_id_drift(self):
        table = _TableFixture.table()
        ids = [7, 8]
        body, _ = _build_geoparquet(table, ids, expected_count=2, expected_bounds=None)
        self.assertIsNone(_assert_full_parity(table, ids, body, expected_count=2))
        swapped = _set(table, "NAMELSAD", pa.array(["Renamed", "District 2"], type=pa.string()))
        with self.assertRaisesRegex(ValueError, "native_output_attribute_parity_drift:NAMELSAD"):
            _assert_full_parity(swapped, ids, body, expected_count=2)
        other_wkb = pa.array([table.column("SHAPE")[1].as_py(), table.column("SHAPE")[0].as_py()], type=pa.binary())
        with self.assertRaisesRegex(ValueError, "native_output_geometry_parity_drift"):
            _assert_full_parity(_set(table, "SHAPE", other_wkb), ids, body, expected_count=2)
        with self.assertRaisesRegex(ValueError, "native_output_id_parity_drift"):
            _assert_full_parity(table, [8, 7], body, expected_count=2)

    def test_output_roster_is_complete(self):
        self.assertEqual(source.EXPECTED_OUTPUT_FIELDS, ["OBJECTID", *source.EXPECTED_FIELDS, "geometry"])
        self.assertEqual(len(source.EXPECTED_FIELDS), 13)
        self.assertIn("GEOID", source.EXPECTED_FIELDS)
        self.assertIn("STATEFP", source.EXPECTED_FIELDS)
        self.assertIn("SLDLST", source.EXPECTED_FIELDS)


class StateLegislativeDistrictsLowerDomainTests(unittest.TestCase):
    def test_domain_validation_accepts_complete_rows(self):
        audit = _validate_domain_and_ids(_TableFixture.table(), _release())
        self.assertEqual(audit["state_fps"], ["01", "02"])

    def test_domain_validation_rejects_prefix_mismatch_duplication_domains_nulls_and_length(self):
        table = _TableFixture.table()
        swapped = _set(table, "STATEFP", pa.array(["02", "02"], type=pa.string()))
        with self.assertRaisesRegex(ValueError, "source_geoid_state_prefix_drift"):
            _validate_domain_and_ids(swapped, _release())
        with self.assertRaisesRegex(ValueError, "source_district_id_duplication_drift"):
            _validate_domain_and_ids(_TableFixture.table(geoids=("01001", "01001")), _release())
        with self.assertRaisesRegex(ValueError, "source_domain_value_drift:MTFCC"):
            _validate_domain_and_ids(_set(table, "MTFCC", pa.array(["G5221", "G5220"], type=pa.string())), _release())
        with self.assertRaisesRegex(ValueError, "source_domain_value_drift:LSY"):
            _validate_domain_and_ids(_set(table, "LSY", pa.array(["2024", "2026"], type=pa.string())), _release())
        names = source.EXPECTED_FIELDS
        columns = [
            pa.array([None, None], type=pa.string()) if name == "NAMELSAD" else table.column(name)
            for name in names
        ]
        with self.assertRaisesRegex(ValueError, "source_attribute_null_drift"):
            _validate_domain_and_ids(pa.Table.from_arrays([*columns, table.column("SHAPE")], names=[*names, "SHAPE"]), _release())
        with self.assertRaisesRegex(ValueError, "source_geoid_length_drift"):
            _validate_domain_and_ids(_TableFixture.table(geoids=("010001X", "020002X")), _release())


class StateLegislativeDistrictsLowerArchiveTests(unittest.TestCase):
    def _gdb_zip(self, root="tlgdb_2026_us_legislative.gdb"):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(f"{root}/gdb", b"esri gdb")
            archive.writestr(f"{root}/a00000001.gdbtable", b"table")
        return buffer.getvalue()

    def test_validate_archive_accepts_single_intact_geodatabase(self):
        _validate_archive(self._gdb_zip(), _release())

    def test_validate_archive_rejects_roster_unsafe_and_invalid_zip(self):
        with self.assertRaisesRegex(ValueError, "source_archive_geodatabase_roster_drift"):
            _validate_archive(self._gdb_zip(root="other.gdb"), _release())
        unsafe = io.BytesIO()
        with zipfile.ZipFile(unsafe, "w") as archive:
            archive.writestr("../escape.gdb/gdb", b"x")
        with self.assertRaisesRegex(ValueError, "source_archive_unsafe_member"):
            _validate_archive(unsafe.getvalue(), _release())
        with self.assertRaisesRegex(ValueError, "source_archive_invalid_zip"):
            _validate_archive(b"not a zip", _release())

    def test_extract_archive_round_trip_and_missing_gdb(self):
        root = _extract_archive(self._gdb_zip(), _release())
        try:
            self.assertTrue((root / _release().source_file[:-4]).is_dir())
        finally:
            shutil.rmtree(root, ignore_errors=True)
        empty = io.BytesIO()
        with zipfile.ZipFile(empty, "w") as archive:
            archive.writestr("empty.txt", b"x")
        with self.assertRaisesRegex(ValueError, "source_archive_geodatabase_missing"):
            _extract_archive(empty.getvalue(), _release())


class StateLegislativeDistrictsLowerCaptureTests(unittest.TestCase):
    def test_capture_drift_quarantines_archive_and_receipt(self):
        body = b"not the pinned Census archive"
        wire = b"wire bytes for changed archive"
        receipt = {
            "method": "GET", "url": _release().source_url, "final_url": _release().source_url,
            "status": 200, "complete_body": True, "bytes": len(body),
            "sha256": "0" * 64, "content_encoding": "identity",
            "wire_bytes": len(wire), "wire_sha256": hashlib.sha256(wire).hexdigest(),
            "content_length": len(body), "observed_at": "2026-11-15T00:00:00+00:00",
        }
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))

            def changed_capture(_client, _url, root, _name, **_kwargs):
                root.mkdir(parents=True, exist_ok=True)
                (root / "source.body").write_bytes(body)
                (root / "source.wire").write_bytes(wire)
                (root / "source.receipt.json").write_text(json.dumps(receipt))
                return body, receipt

            with patch.object(census_latest, "discover_geodatabase", return_value=_release()), \
                 patch.object(source, "capture_get", side_effect=changed_capture):
                with self.assertRaisesRegex(ValueError, "official_tiger_archive_identity_or_release_drift"):
                    _capture(None, storage, "vtest")

            keys = storage.list_prefix("_quarantine/state-legislative-districts-lower")
            self.assertEqual(len(keys), 4)
            body_key = next(key for key in keys if key.endswith("/raw/source.body"))
            receipt_key = next(key for key in keys if key.endswith("/raw/source.receipt.json"))
            failure_key = next(key for key in keys if key.endswith("/metadata/failure.json"))
            self.assertEqual(storage.read_key(body_key), body)
            self.assertEqual(json.loads(storage.read_key(receipt_key)), receipt)
            self.assertEqual(
                json.loads(storage.read_key(failure_key))["reason_code"],
                "official_tiger_archive_identity_or_release_drift",
            )

    def test_live_capture_discovers_the_current_release(self):
        body = b"current official archive bytes"
        release = _release(2027)
        receipt = {
            "method": "GET", "url": release.source_url, "final_url": release.source_url,
            "status": 200, "complete_body": True, "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(), "content_encoding": "identity",
            "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
            "content_length": len(body), "observed_at": "2026-11-15T00:00:00+00:00",
        }
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            with patch.object(census_latest, "discover_geodatabase", return_value=release), \
                 patch.object(source, "capture_get", return_value=(body, receipt)):
                captured, captured_receipt, discovered = _capture(None, storage, "vtest")
        self.assertEqual(captured, body)
        self.assertEqual(captured_receipt, receipt)
        self.assertEqual(discovered.vintage, 2027)
        self.assertEqual(discovered.source_url, release.source_url)

    def test_capture_transport_failure_retains_partial_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            with patch.object(census_latest, "discover_geodatabase", return_value=_release()), \
                 patch.object(source, "capture_get", side_effect=OSError("connection interrupted")):
                with self.assertRaisesRegex(ValueError, "official_tiger_source_capture_failed"):
                    _capture(None, storage, "vfail")
            keys = storage.list_prefix("_quarantine/state-legislative-districts-lower")
            self.assertTrue(any(key.endswith("/raw/source.receipt.json") for key in keys))
            failure_key = next(key for key in keys if key.endswith("/metadata/failure.json"))
            self.assertEqual(json.loads(storage.read_key(failure_key))["reason_code"], "OSError")


class StateLegislativeDistrictsLowerIdentityTests(unittest.TestCase):
    def test_release_version_is_deterministic_and_archive_pinned(self):
        first = census_latest.release_version("2026", "a" * 64, processing_code_identity())
        self.assertEqual(first, census_latest.release_version("2026", "a" * 64, processing_code_identity()))
        self.assertRegex(first, r"^v[0-9a-f]{32}$")
        self.assertNotEqual(first, census_latest.release_version("2026", "b" * 64, processing_code_identity()))

    def test_processing_code_identity_is_deterministic_without_git(self):
        with patch("subprocess.check_output", side_effect=FileNotFoundError("git")), \
             patch("subprocess.run", side_effect=FileNotFoundError("git")):
            identity = processing_code_identity()
        self.assertRegex(identity, r"^[0-9a-f]{64}$")
        self.assertEqual(identity, processing_code_identity())


class StateLegislativeDistrictsLowerAssetTests(unittest.TestCase):
    def test_source_asset_is_registered_with_stopped_refresh(self):
        keys = {key for definition in hifld_ingest_assets for key in definition.keys}
        self.assertIn(source.DATASET_SLUG, {key.path[-2] for key in keys if key.path[-2] == source.DATASET_SLUG})
        self.assertEqual(state_legislative_districts_lower_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertIn(
            "state_legislative_districts_lower_draft_refresh",
            {job.name for job in defs.resolve_all_job_defs()},
        )
        self.assertEqual(
            defs.resolve_schedule_def("state_legislative_districts_lower_draft_refresh_schedule").default_status,
            DefaultScheduleStatus.STOPPED,
        )
        self.assertIn(
            (f"ingest/{source.DATASET_SLUG}/{source.FILE_SLUG}", "state_legislative_districts_lower_source_complete"),
            {(key.asset_key.to_user_string(), key.name) for key in defs.resolve_asset_graph().asset_check_keys},
        )

    def test_source_check_requires_pinned_archive_and_complete_population(self):
        candidate = {
            "feature_count": 2,
            "source_population_complete": True,
            "source_archive_bytes": 100,
            "source_archive_sha256": "a" * 64,
            "source_id_sha256": "0" * 64,
            "source_schema_sha256": "1" * 64,
            "native_parquet_sha256": "2" * 64,
            "state_count": 51,
        }
        self.assertTrue(state_legislative_districts_lower_source_complete(candidate).passed)
        candidate["feature_count"] = 0
        self.assertFalse(state_legislative_districts_lower_source_complete(candidate).passed)


def _gdb_zip_body(release):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(f"{release.source_file[:-4]}/gdb", b"esri gdb")
    return buffer.getvalue()


def _capture_receipt(release, body):
    return {
        "method": "GET", "url": release.source_url, "final_url": release.source_url,
        "status": 200, "complete_body": True, "bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(), "content_encoding": "identity",
        "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
        "content_length": len(body), "observed_at": "2026-11-15T00:00:00+00:00",
    }


class StateLegislativeDistrictsLowerEndToEndTests(unittest.TestCase):
    def test_entry_point_stages_candidate_from_discovered_current_release(self):
        release = _release(2027)
        body = _gdb_zip_body(release)
        receipt = _capture_receipt(release, body)
        table = _TableFixture.table(lsy="2027")
        ids = [1, 2]
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            with patch.object(census_latest, "discover_geodatabase", return_value=release), \
                 patch.object(source, "capture_get", return_value=(body, receipt)), \
                 patch.object(source, "_native_layer",
                              return_value=(table, ids, {"layers": list(source.EXPECTED_LAYER_ROSTER)})):
                result = source.capture_state_legislative_districts_lower(storage)
            self.assertIsNotNone(result["version"])
            self.assertRegex(result["version"], r"^v[0-9a-f]{32}$")
            self.assertEqual(result["feature_count"], 2)
            self.assertEqual(result["source_archive_sha256"], receipt["sha256"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}"
            self.assertIn(prefix + f"/geoparquet/{source.FILE_SLUG}.parquet", result["written_keys"])
            manifest = json.loads(storage.read_key(prefix + "/metadata/source_manifest.json"))
            self.assertEqual(manifest["source_url"], release.source_url)
            self.assertEqual(manifest["date_issued"], "2027")
            audit = json.loads(storage.read_key(prefix + "/metadata/source_audit.json"))
            self.assertEqual(audit["source"]["vintage"], "2027")

    def test_release_from_receipt_accepts_legacy_a_national_name(self):
        release = _release(2026)
        parsed = source._release_from_receipt({
            "url": release.source_url.replace("_us_", "_a_us_"),
            "observed_at": "2026-11-15T00:00:00+00:00",
        })
        self.assertEqual(parsed.source_file, "tlgdb_2026_a_us_legislative.gdb.zip")
        self.assertEqual(parsed.vintage, 2026)


if __name__ == "__main__":
    unittest.main()

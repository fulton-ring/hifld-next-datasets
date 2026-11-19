"""Fail-closed discovery, schema and completeness checks for NCES public schools."""

from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import httpx
import pandas as pd
from dagster import AssetCheckSeverity, DagsterInstance, materialize
from shapely.geometry import Point

from dagster_hifld.assets.nces_public_schools import nces_public_schools
from dagster_hifld.assets import nces_public_schools as asset_module
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import nces_public_schools as source
from dagster_hifld.sources.nces_public_schools import (
    EXPECTED_FIELDS,
    _canonical,
    _inspect_archive,
    _parquet_bytes,
    _sha,
    _validate_frame,
)

STEM = "EDGE_GEOCODE_PUBLICSCH_2425"
SCHOOLYEAR = "2024-2025"
RELEASE = {
    "schoolyear": SCHOOLYEAR,
    "stem": STEM,
    "source_url": f"https://nces.ed.gov/programs/edge/data/{STEM}.zip",
}
LANDING_HTML = (
    '<main><a href="/programs/edge/data/EDGE_GEOCODE_PUBLICSCH_2324.zip">old</a>'
    f'<a href="/programs/edge/data/{STEM}.zip">new</a>'
    '<!-- <a href="~/data/EDGE_GEOCODE_PUBLICSCH_2526.zip">future</a> -->'
    "</main>"
).encode()


def _frame(schoolyear: str = SCHOOLYEAR) -> gpd.GeoDataFrame:
    rows = []
    for school_id, state, x, y in (
        ("000000000001", "AK", -149.9, 61.2),
        ("000000000002", "HI", -157.8, 21.3),
    ):
        row = {name: f"{name[:8]}" for name in EXPECTED_FIELDS}
        row.update({
            "NCESSCH": school_id,
            "STATE": state,
            "LAT": y,
            "LON": x,
            "SCHOOLYEAR": schoolyear,
        })
        rows.append(row)
    return gpd.GeoDataFrame(pd.DataFrame(rows, columns=EXPECTED_FIELDS),
                            geometry=[Point(-149.9, 61.2), Point(-157.8, 21.3)],
                            crs="EPSG:4326")


def _archive(frame: gpd.GeoDataFrame | None = None) -> tuple[bytes, gpd.GeoDataFrame]:
    frame = frame if frame is not None else _frame()
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        frame.to_file(root / f"{STEM}.shp", engine="pyogrio")
        written = gpd.read_file(root / f"{STEM}.shp", engine="pyogrio")
        text = io.StringIO()
        writer = csv.writer(text, delimiter="|", lineterminator="\n")
        for _, row in written.iterrows():
            writer.writerow([repr(float(row[name])) if name in {"LAT", "LON"} else row[name]
                             for name in EXPECTED_FIELDS])
        inner = io.BytesIO()
        with zipfile.ZipFile(inner, "w") as archive:
            for name in (f"{STEM}.shp", f"{STEM}.shx", f"{STEM}.dbf", f"{STEM}.prj", f"{STEM}.cpg"):
                archive.write(root / name, arcname=name)
            for suffix in ("sbn", "sbx", "NCESSCH.atx", "shp.xml"):
                archive.writestr(f"{STEM}.{suffix}", b"")
        outer = io.BytesIO()
        with zipfile.ZipFile(outer, "w") as archive:
            archive.writestr("Shapefile_SCH.zip", inner.getvalue())
            archive.writestr(f"{STEM}.TXT", text.getvalue().encode())
            archive.writestr(f"{STEM}.xlsx", b"x")
            archive.writestr(f"{STEM}.sas7bdat", b"s")
        return outer.getvalue(), written


def _response(body: bytes, status: int = 200) -> httpx.Response:
    return httpx.Response(status, stream=httpx.ByteStream(body),
                          headers={"Content-Length": str(len(body))})


def _client(source_response: httpx.Response) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("SchoolLocations"):
            return _response(LANDING_HTML)
        return source_response
    return httpx.Client(transport=httpx.MockTransport(handler))


class NcesPublicSchoolsSourceTest(unittest.TestCase):
    def test_population_changes_materialize_warn_and_replay_with_real_provenance(self) -> None:
        frame = _frame()
        grown = gpd.GeoDataFrame(pd.concat([frame, frame.iloc[[0]]], ignore_index=True),
                                 geometry="geometry", crs=frame.crs)
        grown.loc[2, "NCESSCH"] = "000000000003"
        grown.loc[2, "LAT"] = 47.6
        grown.loc[2, "LON"] = -122.3
        grown.loc[2, "geometry"] = Point(-122.3, 47.6)
        with tempfile.TemporaryDirectory() as directory, DagsterInstance.ephemeral() as instance:
            root = Path(directory)
            storage = StagingStorageResource(local_dir=str(root / "stage"), use_local=True)
            baseline = None
            versions = set()
            for index, population in enumerate((frame, grown, frame.iloc[:1])):
                body, written = _archive(population)
                with patch.object(source.httpx, "Client", return_value=_client(_response(body))):
                    run = materialize([nces_public_schools, *asset_module.ASSET_CHECKS],
                                      instance=instance, resources={"staging_storage": storage})
                self.assertTrue(run.success)
                candidate = run.output_for_node(nces_public_schools.op.name)
                versions.add(candidate["version"])
                self.assertEqual(candidate["feature_count"], len(written))
                self.assertEqual(candidate["source_reported_count"], len(written))
                self.assertTrue(candidate["source_population_complete"])
                self.assertFalse(candidate["release_accepted"])
                checks = {check.check_name: check for check in run.get_asset_check_evaluations()}
                self.assertTrue(checks["nces_public_schools_complete"].passed)
                skipped = checks["nces_public_schools_drift"]
                self.assertFalse(skipped.passed)
                self.assertEqual(skipped.severity, AssetCheckSeverity.WARN)
                self.assertEqual(skipped.metadata["comparison_status"].value,
                                 "skipped_no_approved_baseline")
                if baseline is None:
                    baseline = {**candidate, "version": "approved-fixture", "release_accepted": True}
                else:
                    warning = approved_baseline_drift(candidate, baseline)
                    self.assertFalse(warning.passed)
                    self.assertEqual(warning.severity, AssetCheckSeverity.WARN)
                    self.assertEqual(warning.metadata["feature_count_delta"].value, len(written) - 2)
                prefix = candidate["candidate_ready"].removesuffix("metadata/candidate_ready.json")
                audit = json.loads(storage.read_key(prefix + "metadata/source_audit.json"))
                self.assertEqual(set(audit["source_counts"].values()), {len(written)})
                parquet = gpd.read_parquet(io.BytesIO(storage.read_key(
                    prefix + "geoparquet/public-schools.parquet")))
                self.assertEqual(parquet["NCESSCH"].tolist(), written["NCESSCH"].tolist())
                self.assertEqual(parquet.geometry.to_wkb().tolist(), written.geometry.to_wkb().tolist())
                original = {key: storage.read_key(key) for key in storage.list_prefix(prefix)}
                replay_root = root / f"replay-{index}"
                replay_root.mkdir()
                for name in source.REPLAY_FILES:
                    (replay_root / name).write_bytes(storage.read_key(prefix + "raw/" + name))
                with patch.object(httpx.Client, "send", side_effect=AssertionError(
                        "retained replay must not attempt HTTP")) as send:
                    replay = source.capture_nces_public_schools(storage, capture_directory=replay_root)
                send.assert_not_called()
                self.assertTrue(replay["reused"])
                self.assertEqual(replay["version"], candidate["version"])
                self.assertEqual(replay["source_reported_count"], len(written))
                self.assertEqual(original, {key: storage.read_key(key) for key in original})
            self.assertEqual(len(versions), 3)

    def test_torn_source_counts_and_publication_witness_are_refused(self) -> None:
        body, _ = _archive()
        archive = zipfile.ZipFile(io.BytesIO(body))
        changed = io.BytesIO()
        with zipfile.ZipFile(changed, "w") as output:
            for name in archive.namelist():
                payload = archive.read(name)
                if name.endswith(".TXT"):
                    payload = payload.splitlines(keepends=True)[0]
                output.writestr(name, payload)
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            with patch.object(source.httpx, "Client", return_value=_client(_response(changed.getvalue()))):
                with self.assertRaisesRegex(ValueError, "text_population_drift"):
                    source.capture_nces_public_schools(storage)
            self._assert_quarantine(storage, changed.getvalue(), 200)

        requests = []

        def respond(request):
            if request.url.path.endswith("SchoolLocations"):
                requests.append(request)
                return _response(LANDING_HTML if len(requests) == 1 else LANDING_HTML.replace(
                    b"2425", b"2526"))
            return _response(body)

        client = httpx.Client(transport=httpx.MockTransport(respond))
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            with patch.object(source.httpx, "Client", return_value=client):
                with self.assertRaisesRegex(ValueError, "published_release_changed_during_capture"):
                    source.capture_nces_public_schools(storage)
            self._assert_quarantine(storage, body, 200)

    def test_no_module_level_release_or_count_pins(self) -> None:
        for name in ("EXPECTED_FEATURE_COUNT", "EXPECTED_COUNT", "SOURCE_VINTAGE",
                     "SOURCE_URL", "EXPECTED_BYTES", "EXPECTED_SHA256", "EXPECTED_ID_SHA256"):
            self.assertFalse(hasattr(source, name), name)

    def test_inspect_archive_uses_publisher_counts_and_records_school_year(self) -> None:
        body, frame = _archive()
        inspected, audit, roster = _inspect_archive(body, RELEASE)
        self.assertEqual(len(inspected), len(frame))
        self.assertEqual(audit["source_feature_count"], 2)
        self.assertEqual(audit["source_counts"], {"dbf": 2, "shx": 2, "shp": 2, "decoded": 2, "text": 2})
        self.assertEqual(audit["schoolyear"], SCHOOLYEAR)
        self.assertEqual(audit["publication_selection"], RELEASE)
        self.assertEqual(sorted(roster["outer"]), sorted(
            {"Shapefile_SCH.zip", f"{STEM}.TXT", f"{STEM}.xlsx", f"{STEM}.sas7bdat"}))

    def test_live_capture_discovers_newest_active_release(self) -> None:
        body, _frame_written = _archive()
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            with patch.object(source.httpx, "Client", return_value=_client(_response(body))):
                artifacts, receipt, release = source._capture(None, storage)
            self.assertEqual(release["schoolyear"], SCHOOLYEAR)
            self.assertEqual(release["source_url"], RELEASE["source_url"])
            self.assertEqual(artifacts["source.body"], body)
            self.assertIn("landing.body", artifacts)
            self.assertIn("landing_after.body", artifacts)
            self.assertEqual(receipt["status"], 200)

    def test_live_source_failure_retains_response_without_candidate(self) -> None:
        body = b"publisher temporarily unavailable"
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            with patch.object(source.httpx, "Client", return_value=_client(_response(body, 503))):
                with self.assertRaisesRegex(ValueError, "incomplete_source_capture:source"):
                    source.capture_nces_public_schools(storage)
            self._assert_quarantine(storage, body, 503)

    def test_invalid_archive_is_quarantined_after_capture(self) -> None:
        body = b"invalid zip from publisher"
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            with patch.object(source.httpx, "Client", return_value=_client(_response(body))):
                with self.assertRaisesRegex(ValueError, "nces_public_schools_archive_invalid"):
                    source.capture_nces_public_schools(storage)
            self._assert_quarantine(storage, body, 200)

    def _assert_quarantine(self, storage, body, status) -> None:
        keys = storage.list_prefix("_quarantine/public-schools")
        self.assertTrue(any(key.endswith("source.body") for key in keys), keys)
        raw = next(key.removesuffix("source.body") for key in keys if key.endswith("source.body"))
        self.assertEqual(storage.read_key(raw + "source.body"), body)
        self.assertEqual(storage.read_key(raw + "source.wire"), body)
        receipt = json.loads(storage.read_key(raw + "source.receipt.json"))
        self.assertEqual(receipt["status"], status)
        self.assertEqual(receipt["sha256"], _sha(body))
        self.assertFalse(storage.list_prefix("public-schools/public-schools"))
        self.assertFalse(storage.list_prefix("_construction-state/public-schools"))

    def test_dagster_http_refusal_retains_evidence_without_materialization(self) -> None:
        body = b"publisher temporarily unavailable"
        with tempfile.TemporaryDirectory() as directory, DagsterInstance.ephemeral() as instance:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            with patch.object(source.httpx, "Client", return_value=_client(_response(body, 503))):
                result = materialize([nces_public_schools], instance=instance,
                                     resources={"staging_storage": storage}, raise_on_error=False)
            self.assertFalse(result.success)
            self.assertFalse(result.get_asset_materialization_events())
            self._assert_quarantine(storage, body, 503)

    def test_rejected_symlink_capture_is_not_read_into_quarantine(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capture = root / "capture"
            capture.mkdir()
            private = root / "outside-capture"
            private.write_bytes(b"must not be copied from a rejected symlink")
            (capture / "source.body").symlink_to(private)
            (capture / "source.wire").write_bytes(b"wire")
            (capture / "source.receipt.json").write_bytes(b"{}")
            storage = StagingStorageResource(local_dir=str(root / "staging"), use_local=True)
            with self.assertRaisesRegex(ValueError, "capture_nonregular_file"):
                source.capture_nces_public_schools(storage, capture_directory=capture)
            self.assertEqual(storage.list_prefix(), [])

    def test_runtime_counts_and_school_year_are_enforced(self) -> None:
        frame = _frame()
        for count in (1, 3):
            with self.subTest(expected_count=count), self.assertRaisesRegex(
                    ValueError, "population_or_attribute_null_drift"):
                _validate_frame(frame, schoolyear=SCHOOLYEAR, expected_count=count)
        changed = frame.copy()
        changed["SCHOOLYEAR"] = "2025-2026"
        with self.assertRaisesRegex(ValueError, "vintage_drift"):
            _validate_frame(changed, schoolyear=SCHOOLYEAR, expected_count=2)
        duplicate = frame.copy()
        duplicate.loc[1, "NCESSCH"] = duplicate.loc[0, "NCESSCH"]
        with self.assertRaisesRegex(ValueError, "id_roster_drift"):
            _validate_frame(duplicate, schoolyear=SCHOOLYEAR, expected_count=2)
        with self.assertRaisesRegex(ValueError, "field_roster_drift"):
            _validate_frame(frame.assign(new_field="native-schema-change"),
                            schoolyear=SCHOOLYEAR, expected_count=2)

    def test_unsorted_ids_are_preserved_not_rejected(self) -> None:
        frame = _frame().iloc[::-1].reset_index(drop=True)
        audit = _validate_frame(frame, schoolyear=SCHOOLYEAR, expected_count=2)
        self.assertEqual(audit["source_id_sha256"], _sha(_canonical(frame["NCESSCH"].tolist())))

    def test_native_attributes_and_point_coordinates_roundtrip_as_geoparquet(self) -> None:
        frame = _frame()
        body = _parquet_bytes(frame)
        restored = gpd.read_parquet(io.BytesIO(body))
        self.assertEqual(list(restored.columns), [*EXPECTED_FIELDS, "geometry"])
        self.assertEqual(restored["NCESSCH"].tolist(), ["000000000001", "000000000002"])
        self.assertEqual(restored.geometry.to_wkt().tolist(), frame.geometry.to_wkt().tolist())
        self.assertEqual(restored.crs.to_epsg(), 4326)

    def test_string_identifier_and_geometry_type_are_not_coerced(self) -> None:
        frame = _frame()
        frame["ZIP"] = pd.Series([12345, 96701], dtype="int64")
        with self.assertRaisesRegex(ValueError, "string_field_type_drift"):
            _validate_frame(frame, schoolyear=SCHOOLYEAR, expected_count=2)
        frame = _frame()
        frame.geometry = [Point(-149.9, 61.2), Point(-157.8, 21.3, 7)]
        with self.assertRaisesRegex(ValueError, "geometry_drift"):
            _validate_frame(frame, schoolyear=SCHOOLYEAR, expected_count=2)


if __name__ == "__main__":
    unittest.main()

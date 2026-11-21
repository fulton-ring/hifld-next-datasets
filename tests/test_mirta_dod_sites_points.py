"""Focused MIRTA DoD Sites points source-contract and replay checks."""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import fiona
import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from dagster import AssetCheckSeverity, DagsterInstance, materialize

import dagster_hifld.assets.mirta_dod_sites_points as assets
from dagster_hifld.assets.mirta_dod_sites_points import mirta_dod_sites_points_draft_schedule
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
import dagster_hifld.sources.mirta_dod_sites_points as src
from dagster_hifld.sources.mirta_dod_sites_points import (
    DATASET_SLUG, EXPECTED_FID_COLUMN, EXPECTED_FIELDS, FIELD_ORDER,
    RELEASE_NOTES_URL, SOURCE_RELEASE, SOURCE_URL, _canonical, _load_capture,
    _parquet, _sha,
)

_GDB = "FY25_MIRTA_FinalV2.gdb"
_POINTS_LAYER = "FY25MIRTALocations_P"
_BOUNDARY_LAYER = "FY25MirtaLocations_A"
_NOTES = b"fixture release notes"


def _rows():
    return [
        {"geometry": {"type": "Point", "coordinates": (-78.6558, 35.9206)},
         "properties": {"OBJECTID": 764, "countryName": "usa", "featureDescription": "na",
                        "featureName": "Fort Example", "isCui": "no", "isFirrmaSite": "yes",
                        "isJointBase": "no", "mediaId": "na", "mirtaLocationsIdpk": " ",
                        "sdsId": None, "siteName": "Fort Example", "siteOperationalStatus": "act",
                        "siteReportingComponent": "usa", "stateNameCode": "nc"}},
        {"geometry": {"type": "Point", "coordinates": (-119.6856, 34.4214)},
         "properties": {"OBJECTID": 1604, "countryName": "usa", "featureDescription": "na",
                        "featureName": "NG Example TS", "isCui": "no", "isFirrmaSite": "no",
                        "isJointBase": "yes", "mediaId": "na", "mirtaLocationsIdpk": " ",
                        "sdsId": "{049FEB34-6C2D-4A6F-B3F3-2CF8E78ED47D}", "siteName": "NG Example TS",
                        "siteOperationalStatus": "Excs",
                        "siteReportingComponent": "armyNationalGuard", "stateNameCode": "ca"}},
    ]


def _added_rows():
    """The baseline rows plus one site the publisher added upstream."""
    rows = _rows()
    rows.append(
        {"geometry": {"type": "Point", "coordinates": (-97.3646, 32.7815)},
         "properties": {"OBJECTID": 2000, "countryName": "usa", "featureDescription": "na",
                        "featureName": "Added TS", "isCui": "no", "isFirrmaSite": "no",
                        "isJointBase": "no", "mediaId": "na", "mirtaLocationsIdpk": " ",
                        "sdsId": None, "siteName": "Added TS", "siteOperationalStatus": "act",
                        "siteReportingComponent": "armyNationalGuard", "stateNameCode": "tx"}})
    return rows


def _build_archive(root: Path, rows) -> tuple[bytes, frozenset]:
    """Build one fixture archive (gdb + top-level release notes) and its member set."""
    gdb = root / _GDB
    # Layers are written in the archive's observed roster order.
    boundary_schema = {"geometry": "Polygon", "properties": {"siteName": "str:100"}}
    with fiona.open(str(gdb), "w", driver="OpenFileGDB", layer=_BOUNDARY_LAYER,
                    crs="EPSG:4326", schema=boundary_schema) as sink:
        sink.write({"geometry": {"type": "Polygon",
                                 "coordinates": [[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]]},
                    "properties": {"siteName": "Fort Example"}})
    point_schema = {"geometry": "Point", "properties": {"OBJECTID": "int64",
                                                        **dict(EXPECTED_FIELDS)}}
    with fiona.open(str(gdb), "w", driver="OpenFileGDB", layer=_POINTS_LAYER,
                    crs="EPSG:4326", schema=point_schema) as sink:
        for row in rows:
            sink.write(row)
    archive = root / "mirta.zip"
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(zipfile.ZipInfo(_GDB + "/"), b"")
        for item in sorted(gdb.rglob("*")):
            if item.is_file():
                zf.write(item, item.relative_to(root))
        zf.writestr("release_notes.pdf", _NOTES)
    members = {info.filename.split("/", 1)[1] for info in zipfile.ZipFile(archive).infolist()
               if info.filename.startswith(_GDB + "/") and not info.is_dir()}
    return archive.read_bytes(), frozenset(members)


@contextlib.contextmanager
def _fixture(rows=None):
    """Yield the module, a fixture root and the archive bytes with the member pin patched."""
    rows = _rows() if rows is None else rows
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        body, members = _build_archive(root, rows)
        with patch.object(src, "EXPECTED_GDB_MEMBERS", members):
            yield src, root, body


def _mock_client(notes: bytes, archive: bytes) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url == SOURCE_URL:
            payload = archive
        elif request.url == RELEASE_NOTES_URL:
            payload = notes
        else:
            raise AssertionError(str(request.url))
        return httpx.Response(200, stream=httpx.ByteStream(payload),
                              headers={"Content-Length": str(len(payload))})
    return httpx.Client(transport=httpx.MockTransport(handler))


class MirtaDodSitesPointsSourceContractTest(unittest.TestCase):
    def test_pinned_source_and_complete_native_contract(self):
        self.assertEqual(DATASET_SLUG, "mirta-dod-sites-points")
        self.assertEqual(SOURCE_URL, "https://www.acq.osd.mil/eie/imr/rpid/disdi/Downloads/installations_ranges.zip")
        self.assertEqual(RELEASE_NOTES_URL, "https://www.acq.osd.mil/eie/imr/rpid/disdi/Downloads/release_notes.pdf")
        self.assertEqual(SOURCE_RELEASE, "FY25 V2")
        self.assertEqual(list(FIELD_ORDER), list(EXPECTED_FIELDS))
        self.assertEqual(EXPECTED_FIELDS["mirtaLocationsIdpk"], "str:40")
        self.assertEqual(EXPECTED_FID_COLUMN, "OBJECTID")
        self.assertEqual(src.EXPECTED_GDB_DIR, "FY25_MIRTA_FinalV2.gdb")
        self.assertEqual(src.EXPECTED_GDB_LAYER_ROSTER, [_BOUNDARY_LAYER, _POINTS_LAYER])
        self.assertEqual(src.EXPECTED_LAYER, _POINTS_LAYER)
        self.assertEqual(src.EXPECTED_BOUNDARY_LAYER, _BOUNDARY_LAYER)

    def test_no_module_level_release_or_count_pins(self):
        for name in ("EXPECTED_COUNT", "EXPECTED_FEATURE_COUNT", "EXPECTED_ZIP_BYTES",
                     "EXPECTED_ZIP_SHA256", "EXPECTED_RELEASE_NOTES_BYTES",
                     "EXPECTED_RELEASE_NOTES_SHA256", "EXPECTED_ZIP_MEMBER_DATE",
                     "EXPECTED_BOUNDS", "EXPECTED_FID_FIRST", "EXPECTED_FID_LAST",
                     "EXPECTED_CONSTANT_FIELDS", "EXPECTED_IDPK_VALUE",
                     "EXPECTED_FIRRMA_COUNTS", "EXPECTED_JOINT_BASE_COUNTS",
                     "EXPECTED_STATUS_COUNTS", "EXPECTED_COMPONENT_COUNTS",
                     "EXPECTED_STATE_SET", "EXPECTED_SDS_NULL_COUNT",
                     "EXPECTED_SDS_GUID_COUNT", "RECORD_SHA", "GEOMETRY_SHA", "ID_SHA",
                     "FID_SHA", "SCHEMA_SHA"):
            self.assertFalse(hasattr(src, name), name)

    def test_refresh_schedule_is_stopped(self):
        self.assertEqual(mirta_dod_sites_points_draft_schedule.default_status.value, "STOPPED")

    def test_release_validates_with_witnesses_and_degenerate_id_preserved(self):
        with _fixture() as (_src, _root, body):
            with tempfile.TemporaryDirectory() as tmp:
                attrs, geoms, fids, audit = src._decode(body, Path(tmp))
            self.assertEqual(len(attrs), 2)
            self.assertEqual(fids, [764, 1604])
            self.assertEqual(audit["source_feature_count"], 2)
            self.assertEqual(audit["source_reported_count"], 2)
            self.assertTrue(audit["source_population_complete"])
            self.assertEqual(audit["source_id_degenerate"], True)
            self.assertEqual(audit["source_id_unique"], False)
            self.assertEqual(audit["source_fid_column"], "OBJECTID")
            self.assertTrue(audit["source_fid_unique"])
            self.assertEqual(audit["source_fid_first"], 764)
            self.assertEqual(audit["source_fid_last"], 1604)
            self.assertEqual(audit["source_id_field"], "mirtaLocationsIdpk")
            self.assertEqual(audit["source_geometry_null_count"], 0)
            self.assertEqual(audit["source_release"], "FY25 V2")
            self.assertEqual(audit["source_release_date"], "2026-11-16")
            self.assertEqual(audit["source_crs"], "EPSG:4326")
            self.assertEqual(audit["source_null_counts"]["sdsId"], 1)
            self.assertEqual(audit["source_null_counts"]["mirtaLocationsIdpk"], 0)
            self.assertEqual(audit["source_feature_site_name_divergence_count"], 0)
            self.assertEqual(audit["source_gdb_lock_files"], [])

    def test_noncontiguous_native_fids_survive_roundtrip(self):
        with _fixture() as (_src, _root, body):
            with tempfile.TemporaryDirectory() as tmp:
                attrs, geoms, fids, _audit = src._decode(body, Path(tmp))
            self.assertEqual(fids, [764, 1604])
            self.assertNotEqual(fids[1] - fids[0], 1)
            payload = _parquet(attrs, geoms, fids)
            table = pq.read_table(io.BytesIO(payload))
            self.assertEqual(table["OBJECTID"].type, pa.int64())
            self.assertEqual(table["OBJECTID"].to_pylist(), [764, 1604])
            self.assertEqual(table["siteName"].to_pylist(), ["Fort Example", "NG Example TS"])
            self.assertEqual(table["mirtaLocationsIdpk"].to_pylist(), [" ", " "])

    def test_parquet_roundtrip_preserves_native_values(self):
        with _fixture() as (_src, _root, body):
            with tempfile.TemporaryDirectory() as tmp:
                attrs, geoms, fids, _audit = src._decode(body, Path(tmp))
            payload = _parquet(attrs, geoms, fids)
            table = pq.read_table(io.BytesIO(payload))
            self.assertEqual(table.num_rows, 2)
            self.assertEqual(table["mirtaLocationsIdpk"].to_pylist(), [" ", " "])
            self.assertEqual(table["sdsId"].to_pylist(), [None, "{049FEB34-6C2D-4A6F-B3F3-2CF8E78ED47D}"])
            self.assertEqual(table["stateNameCode"].to_pylist(), ["nc", "ca"])
            geo = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
            self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])

    def test_zip_member_roster_drift_rejected(self):
        with _fixture() as (_src, root, _body):
            out = Path(tempfile.mkdtemp(dir=root)) / "trimmed.zip"
            with zipfile.ZipFile(root / "mirta.zip") as original, zipfile.ZipFile(out, "w") as zf:
                for info in original.infolist():
                    if not info.filename.endswith("timestamps"):
                        zf.writestr(info, original.read(info.filename))
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaisesRegex(ValueError, "source_zip_member_roster_drift"):
                    src._decode(out.read_bytes(), Path(tmp))

    def test_zip_layout_drift_rejected(self):
        with _fixture() as (_src, root, _body):
            out = Path(tempfile.mkdtemp(dir=root)) / "nolayout.zip"
            with zipfile.ZipFile(root / "mirta.zip") as original, zipfile.ZipFile(out, "w") as zf:
                for info in original.infolist():
                    if info.filename != "release_notes.pdf":
                        zf.writestr(info, original.read(info.filename))
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaisesRegex(ValueError, "source_zip_layout_drift"):
                    src._decode(out.read_bytes(), Path(tmp))

    def test_zip_member_path_rejection(self):
        with _fixture() as (_src, root, _body):
            out = Path(tempfile.mkdtemp(dir=root)) / "traversal.zip"
            with zipfile.ZipFile(root / "mirta.zip") as original, zipfile.ZipFile(out, "w") as zf:
                for info in original.infolist():
                    zf.writestr(info, original.read(info.filename))
                zf.writestr("../escape.gdb/evil", b"x")
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaisesRegex(ValueError, "source_zip_member_path_rejected"):
                    src._decode(out.read_bytes(), Path(tmp))

    def test_zip_crc_integrity_rejected(self):
        with _fixture() as (_src, root, _body):
            broken = Path(tempfile.mkdtemp(dir=root)) / "broken.zip"
            raw = bytearray((root / "mirta.zip").read_bytes())
            eocd = raw.rindex(b"PK\x05\x06")
            cursor = int.from_bytes(raw[eocd + 16:eocd + 20], "little")
            patched = False
            while raw[cursor:cursor + 4] == b"PK\x01\x02":
                name_length = int.from_bytes(raw[cursor + 28:cursor + 30], "little")
                crc = int.from_bytes(raw[cursor + 16:cursor + 20], "little")
                name = bytes(raw[cursor + 46:cursor + 46 + name_length])
                if crc != 0 and not name.endswith(b"/"):
                    raw[cursor + 16:cursor + 20] = b"\x00\x00\x00\x00"
                    patched = True
                    break
                cursor += 46 + name_length + int.from_bytes(raw[cursor + 30:cursor + 32], "little") \
                    + int.from_bytes(raw[cursor + 32:cursor + 34], "little")
            self.assertTrue(patched)
            broken.write_bytes(bytes(raw))
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaisesRegex(ValueError, "source_zip_crc_invalid"):
                    src._decode(bytes(raw), Path(tmp))

    def test_gdb_layer_roster_drift_rejected(self):
        with _fixture() as (_src, _root, body):
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.object(src, "EXPECTED_GDB_LAYER_ROSTER",
                                 [_POINTS_LAYER, _BOUNDARY_LAYER, "ExtraLayer"]):
                with self.assertRaisesRegex(ValueError, "source_gdb_layer_roster_drift"):
                    src._decode(body, Path(tmp))

    def test_schema_drift_rejected(self):
        with _fixture() as (_src, _root, body):
            modified = dict(EXPECTED_FIELDS)
            modified["siteName"] = "str:80"
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.object(src, "EXPECTED_FIELDS", modified):
                with self.assertRaisesRegex(ValueError, "source_schema_drift"):
                    src._decode(body, Path(tmp))

    def test_sds_id_format_drift_rejected(self):
        rows = _rows()
        rows[1] = dict(rows[1], properties=dict(rows[1]["properties"], sdsId="not-a-guid"))
        with _fixture(rows=rows) as (_src, _root, body):
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaisesRegex(ValueError, "source_sds_id_format_drift"):
                    src._decode(body, Path(tmp))

    def test_embedded_release_notes_copy_drift_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            src_root = root / "src"
            src_root.mkdir()
            _body, members = _build_archive(src_root, _rows())
            out = root / "mutated.zip"
            with zipfile.ZipFile(src_root / "mirta.zip") as original, \
                    zipfile.ZipFile(out, "w") as zf:
                for info in original.infolist():
                    data = b"different release notes" if info.filename == "release_notes.pdf" \
                        else original.read(info.filename)
                    zf.writestr(info, data)
            storage = StagingStorageResource(local_dir=str(root / "staging"))
            with patch.object(src, "EXPECTED_GDB_MEMBERS", members), \
                    patch.object(src.httpx, "Client",
                                 return_value=_mock_client(_NOTES, out.read_bytes())):
                with self.assertRaisesRegex(ValueError, "source_release_notes_copy_drift"):
                    src._capture(root / "capture", storage, "v-test")

    def test_replay_receipt_requires_exact_get_origin(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            body = b"retained source bytes"
            digest = hashlib.sha256(body).hexdigest()
            for name in ("release_notes", "mirta_zip"):
                url = SOURCE_URL if name == "mirta_zip" else RELEASE_NOTES_URL
                for suffix in ("body", "wire"):
                    (root / f"{name}.{suffix}").write_bytes(body)
                receipt = {"method": "GET", "status": 200, "complete_body": True,
                           "bytes": len(body), "sha256": digest, "wire_bytes": len(body),
                           "wire_sha256": digest, "url": url, "final_url": url}
                (root / f"{name}.receipt.json").write_text(json.dumps(receipt))
                self.assertEqual(_load_capture(name, root)[0], body)
                for changes in ({"url": "https://example.org/mirror.zip",
                                 "final_url": "https://example.org/mirror.zip"},
                                {"method": "POST"}, {"complete_body": False}):
                    with self.subTest(name=name, changes=changes):
                        (root / f"{name}.receipt.json").write_text(json.dumps({**receipt, **changes}))
                        with self.assertRaisesRegex(ValueError, "capture_receipt_invalid"):
                            _load_capture(name, root)
                        (root / f"{name}.receipt.json").write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "unexpected_source_capture_name"):
                _load_capture("unknown", root)

    def test_added_site_captures_and_drift_warns(self):
        drift_check = assets.mirta_dod_sites_points_approved_baseline_drift
        self.assertIn(drift_check, assets.ASSET_CHECKS)
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            storage = StagingStorageResource(local_dir=str(directory / "staging"))
            baseline_root = directory / "baseline-src"
            baseline_root.mkdir()
            baseline_body, baseline_members = _build_archive(baseline_root, _rows())
            added_root = directory / "added-src"
            added_root.mkdir()
            added_body, added_members = _build_archive(added_root, _added_rows())
            with patch.object(src, "EXPECTED_GDB_MEMBERS", baseline_members), \
                    patch.object(src.httpx, "Client",
                                 return_value=_mock_client(_NOTES, baseline_body)):
                before = src.capture_mirta_dod_sites_points(
                    storage, version="v-baseline",
                    capture_directory=str(directory / "baseline-capture"))
            instance_dir = directory / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            with patch.object(src, "EXPECTED_GDB_MEMBERS", added_members), \
                    patch.object(src.httpx, "Client",
                                 return_value=_mock_client(_NOTES, added_body)), \
                    DagsterInstance.from_config(str(instance_dir)) as instance:
                result = materialize(
                    [assets.mirta_dod_sites_points, *assets.ASSET_CHECKS],
                    instance=instance, resources={"staging_storage": storage},
                    run_config={"ops": {assets.mirta_dod_sites_points.op.name: {
                        "config": {"capture_directory": str(directory / "added-capture")}}}})
            self.assertTrue(result.success)
            after = result.output_for_node(assets.mirta_dod_sites_points.op.name)
            self.assertEqual((before["feature_count"], after["feature_count"]), (2, 3))
            self.assertTrue(after["source_population_complete"])
            self.assertEqual(after["source_reported_count"], 3)
            self.assertNotEqual(before["source_id_sha256"], after["source_id_sha256"])
            self.assertFalse(after["release_accepted"])
            prefix = f"mirta-dod-sites-points/mirta-dod-sites-points/{after['version']}"
            audit = json.loads(storage.read_key(prefix + "/metadata/source_audit.json"))
            self.assertEqual(audit["source_feature_count"], 3)
            self.assertEqual(audit["source_release"], "FY25 V2")
            evaluations = {evaluation.check_name: evaluation
                           for evaluation in result.get_asset_check_evaluations()}
            self.assertTrue(evaluations["mirta_dod_sites_points_complete"].passed)
            warning = evaluations["mirta_dod_sites_points_approved_baseline_drift"]
            self.assertFalse(warning.passed)
            self.assertEqual(warning.severity, AssetCheckSeverity.WARN)
            self.assertEqual(warning.metadata["comparison_status"].value,
                             "skipped_no_approved_baseline")
            self.assertIn(build_publish_partition_key("mirta-dod-sites-points",
                                                      "mirta-dod-sites-points",
                                                      after["version"]),
                          instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME))
            drift = approved_baseline_drift(after, {**before, "release_accepted": True})
            self.assertFalse(drift.passed)
            self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
            self.assertEqual(drift.metadata["feature_count_delta"].value, 1)
            self.assertTrue(drift.metadata["native_id_roster_changed"].value)


if __name__ == "__main__":
    unittest.main()

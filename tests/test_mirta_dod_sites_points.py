"""Focused MIRTA DoD Sites points source-contract and replay checks."""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from collections import Counter
from pathlib import Path
from unittest.mock import patch

import fiona

from dagster_hifld.assets.mirta_dod_sites_points import mirta_dod_sites_points_draft_schedule
from dagster_hifld.sources.mirta_dod_sites_points import (
    DATASET_SLUG, EXPECTED_COUNT, EXPECTED_FIELDS, FIELD_ORDER, ID_SHA, RECORD_SHA,
    RELEASE_NOTES_URL, SOURCE_RELEASE, SOURCE_URL, _canonical, _decode, _load_capture,
    _parquet, _sha,
)

_GDB = "FY24_MIRTA_Final.gdb"
_POINTS_LAYER = "FY24MirtaLocations"
_BOUNDARY_LAYER = "FY24MirtaLocations_A"


def _rows():
    return [
        {"geometry": {"type": "Point", "coordinates": (-78.6558, 35.9206)},
         "properties": {"countryName": "usa", "featureDescription": "na", "featureName": "Fort Example",
                        "isCui": "no", "isFirrmaSite": "yes", "isJointBase": "no", "mediaId": "na",
                        "mirtaLocationsIdpk": " ", "sdsId": None, "siteName": "Fort Example",
                        "siteOperationalStatus": "act", "siteReportingComponent": "usa",
                        "stateNameCode": "nc"}},
        {"geometry": {"type": "Point", "coordinates": (-119.6856, 34.4214)},
         "properties": {"countryName": "usa", "featureDescription": "na", "featureName": "NG Example TS",
                        "isCui": "no", "isFirrmaSite": "no", "isJointBase": "yes", "mediaId": "na",
                        "mirtaLocationsIdpk": " ", "sdsId": "{049FEB34-6C2D-4A6F-B3F3-2CF8E78ED47D}",
                        "siteName": "NG Example TS", "siteOperationalStatus": "Excs",
                        "siteReportingComponent": "armyNationalGuard", "stateNameCode": "ca"}},
    ]


def _counts(rows, name):
    return dict(Counter(row["properties"][name] for row in rows))


def _gdb_layer_roster(gdb):
    return fiona.listlayers(str(gdb))


def _observe(rows, root: Path):
    """Mirror the module's own extraction to derive the fixture's pin set."""
    attrs, geoms = [], []
    with fiona.open(str(root / _GDB), layer=_POINTS_LAYER) as src:
        schema = dict(src.schema["properties"])
        bounds = list(map(float, src.bounds))
        for feature in src:
            row = []
            for name in FIELD_ORDER:
                value = feature["properties"][name]
                row.append(None if value is None else str(value))
            attrs.append(row)
            x, y = feature["geometry"]["coordinates"]
            geoms.append([float(x), float(y)])
    id_position = list(FIELD_ORDER).index("mirtaLocationsIdpk")
    with zipfile.ZipFile(io.BytesIO((root / "mirta.zip").read_bytes())) as z:
        infos = [i for i in z.infolist() if not (i.is_dir() or i.filename.endswith("/"))]
        members = {i.filename.split("/", 1)[1] for i in infos}
        dates = {i.date_time[:3] for i in infos}
    return attrs, geoms, {
        "EXPECTED_ZIP_BYTES": (root / "mirta.zip").stat().st_size,
        "EXPECTED_ZIP_SHA256": _sha((root / "mirta.zip").read_bytes()),
        "EXPECTED_GDB_MEMBERS": frozenset(members),
        "EXPECTED_ZIP_MEMBER_DATE": next(iter(dates)),
        "EXPECTED_GDB_LAYER_ROSTER": _gdb_layer_roster(root / _GDB),
        "EXPECTED_FIELDS": schema,
        "EXPECTED_COUNT": len(rows),
        "EXPECTED_BOUNDS": bounds,
        "EXPECTED_CONSTANT_FIELDS": {name: rows[0]["properties"][name]
                                     for name in ("countryName", "featureDescription", "isCui", "mediaId")},
        "EXPECTED_IDPK_VALUE": rows[0]["properties"]["mirtaLocationsIdpk"],
        "EXPECTED_FIRRMA_COUNTS": _counts(rows, "isFirrmaSite"),
        "EXPECTED_JOINT_BASE_COUNTS": _counts(rows, "isJointBase"),
        "EXPECTED_STATUS_COUNTS": _counts(rows, "siteOperationalStatus"),
        "EXPECTED_COMPONENT_COUNTS": _counts(rows, "siteReportingComponent"),
        "EXPECTED_STATE_SET": {row["properties"]["stateNameCode"] for row in rows},
        "EXPECTED_SDS_NULL_COUNT": sum(1 for row in rows if row["properties"]["sdsId"] is None),
        "EXPECTED_SDS_GUID_COUNT": sum(1 for row in rows if row["properties"]["sdsId"] is not None),
        "RECORD_SHA": _sha(_canonical(attrs)),
        "GEOMETRY_SHA": _sha(_canonical(geoms)),
        "ID_SHA": _sha(_canonical([row[id_position] for row in attrs])),
        "SCHEMA_SHA": _sha(_canonical([[name, schema[name]] for name in FIELD_ORDER])),
    }


class MirtaDodSitesPointsSourceContractTest(unittest.TestCase):
    @contextlib.contextmanager
    def _fixture(self, rows=None, include_boundary=True):
        import dagster_hifld.sources.mirta_dod_sites_points as src
        rows = _rows() if rows is None else rows
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            gdb = root / _GDB
            point_schema = {"geometry": "Point", "properties": dict(EXPECTED_FIELDS)}
            with fiona.open(str(gdb), "w", driver="OpenFileGDB", layer=_POINTS_LAYER,
                            crs="EPSG:4326", schema=point_schema) as sink:
                for row in rows:
                    sink.write(row)
            if include_boundary:
                boundary_schema = {"geometry": "Polygon", "properties": {"siteName": "str:100"}}
                with fiona.open(str(gdb), "w", driver="OpenFileGDB", layer=_BOUNDARY_LAYER,
                                crs="EPSG:4326", schema=boundary_schema) as sink:
                    sink.write({"geometry": {"type": "Polygon",
                                             "coordinates": [[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]]},
                                "properties": {"siteName": "Fort Example"}})
            archive = root / "mirta.zip"
            with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(zipfile.ZipInfo(_GDB + "/"), b"")
                for item in sorted(gdb.rglob("*")):
                    if item.is_file():
                        zf.write(item, item.relative_to(root))
            pins = _observe(rows, root)[2]
            with patch.multiple(src, **pins):
                yield src, root, archive.read_bytes()

    def test_pinned_source_and_complete_native_contract(self):
        self.assertEqual(DATASET_SLUG, "mirta-dod-sites-points")
        self.assertEqual(SOURCE_URL, "https://www.acq.osd.mil/eie/imr/rpid/disdi/Downloads/installations_ranges.zip")
        self.assertEqual(RELEASE_NOTES_URL, "https://www.acq.osd.mil/eie/imr/rpid/disdi/Downloads/release_notes.pdf")
        self.assertEqual(SOURCE_RELEASE, "FY24")
        self.assertEqual(EXPECTED_COUNT, 831)
        self.assertEqual(list(FIELD_ORDER), list(EXPECTED_FIELDS))
        self.assertEqual(EXPECTED_FIELDS["mirtaLocationsIdpk"], "str:40")
        self.assertEqual(ID_SHA, "919641a22c1c507a0bf9e644b75eacf95c0e790551f3789a230867bf2f502505")
        self.assertEqual(RECORD_SHA, "734ff19e5fc28e447bedae993ce34b82c8b5436fb17afad5f7cd91ab632f1b30")

    def test_refresh_schedule_is_stopped(self):
        self.assertEqual(mirta_dod_sites_points_draft_schedule.default_status.value, "STOPPED")

    def test_pinned_release_validates_with_degenerate_id_preserved(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp:
                attrs, geoms, audit = src._decode(body, Path(tmp))
            self.assertEqual(len(attrs), 2)
            self.assertEqual(audit["source_feature_count"], 2)
            self.assertEqual(audit["source_id_degenerate"], True)
            self.assertEqual(audit["source_id_unique"], False)
            self.assertEqual(audit["source_id_field"], "mirtaLocationsIdpk")
            self.assertEqual(audit["source_geometry_null_count"], 0)
            self.assertEqual(audit["source_release_date"], "2025-09-02")
            self.assertEqual(audit["source_crs"], "EPSG:4326")
            self.assertEqual(audit["source_null_counts"]["sdsId"], 1)
            self.assertEqual(audit["source_null_counts"]["mirtaLocationsIdpk"], 0)

    def test_parquet_roundtrip_preserves_native_values(self):
        with self._fixture() as (src, root, body):
            import pyarrow.parquet as pq
            with tempfile.TemporaryDirectory() as tmp:
                attrs, geoms, _ = src._decode(body, Path(tmp))
            payload = _parquet(attrs, geoms)
            table = pq.read_table(io.BytesIO(payload))
            self.assertEqual(table.num_rows, 2)
            self.assertEqual(table["mirtaLocationsIdpk"].to_pylist(), [" ", " "])
            self.assertEqual(table["sdsId"].to_pylist(), [None, "{049FEB34-6C2D-4A6F-B3F3-2CF8E78ED47D}"])
            self.assertEqual(table["stateNameCode"].to_pylist(), ["nc", "ca"])
            geo = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
            self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])
            self.assertIn(table["geometry"][0].as_py().hex().startswith("0101"), (True,))

    def test_pinned_archive_byte_drift_rejected(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp, patch.object(src, "EXPECTED_ZIP_SHA256", "0" * 64):
                with self.assertRaisesRegex(ValueError, "pinned_disdi_release_drift"):
                    src._decode(body, Path(tmp))

    def _rezip(self, root):
        """Rebuild a zip from the fixture gdb members."""
        out = Path(tempfile.mkdtemp(dir=root)) / "mutated.zip"
        with zipfile.ZipFile(root / "mirta.zip") as original, zipfile.ZipFile(out, "w") as zf:
            for info in original.infolist():
                zf.writestr(info, original.read(info.filename))
        return out

    def test_zip_member_roster_drift_rejected(self):
        with self._fixture() as (src, root, body):
            trimmed = Path(tempfile.mkdtemp(dir=root)) / "trimmed.zip"
            with zipfile.ZipFile(root / "mirta.zip") as original, zipfile.ZipFile(trimmed, "w") as zf:
                for info in original.infolist():
                    if not info.filename.endswith("timestamps"):
                        zf.writestr(info, original.read(info.filename))
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.multiple(src, EXPECTED_ZIP_BYTES=trimmed.stat().st_size,
                                   EXPECTED_ZIP_SHA256=_sha(trimmed.read_bytes())):
                with self.assertRaisesRegex(ValueError, "source_zip_member_roster_drift"):
                    src._decode(trimmed.read_bytes(), Path(tmp))

    def test_zip_member_path_rejection(self):
        with self._fixture() as (src, root, body):
            traversal = self._rezip(root)
            with zipfile.ZipFile(traversal, "a") as zf:
                zf.writestr("../escape.gdb/evil", b"x")
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.multiple(src, EXPECTED_ZIP_BYTES=traversal.stat().st_size,
                                   EXPECTED_ZIP_SHA256=_sha(traversal.read_bytes())):
                with self.assertRaisesRegex(ValueError, "source_zip_member_path_rejected"):
                    src._decode(traversal.read_bytes(), Path(tmp))

    def test_zip_member_timestamp_drift_rejected(self):
        with self._fixture() as (src, root, body):
            restamped = Path(tempfile.mkdtemp(dir=root)) / "restamped.zip"
            with zipfile.ZipFile(root / "mirta.zip") as original, zipfile.ZipFile(restamped, "w") as zf:
                for info in original.infolist():
                    info.date_time = (2020, 1, 1, 0, 0, 0)
                    zf.writestr(info, original.read(info.filename))
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.multiple(src, EXPECTED_ZIP_BYTES=restamped.stat().st_size,
                                   EXPECTED_ZIP_SHA256=_sha(restamped.read_bytes())):
                with self.assertRaisesRegex(ValueError, "source_zip_member_timestamp_drift"):
                    src._decode(restamped.read_bytes(), Path(tmp))

    def test_zip_crc_integrity_rejected(self):
        with self._fixture() as (src, root, body):
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
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.multiple(src, EXPECTED_ZIP_BYTES=len(raw),
                                   EXPECTED_ZIP_SHA256=_sha(bytes(raw))):
                with self.assertRaisesRegex(ValueError, "source_zip_crc_invalid"):
                    src._decode(bytes(raw), Path(tmp))

    def test_gdb_layer_roster_drift_rejected(self):
        rows = _rows()
        with self._fixture(rows=rows, include_boundary=False) as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.object(src, "EXPECTED_GDB_LAYER_ROSTER", [_POINTS_LAYER, _BOUNDARY_LAYER]):
                with self.assertRaisesRegex(ValueError, "source_gdb_layer_roster_drift"):
                    src._decode(body, Path(tmp))

    def test_schema_drift_rejected(self):
        with self._fixture() as (src, root, body):
            modified = dict(EXPECTED_FIELDS)
            modified["siteName"] = "str:80"
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.object(src, "EXPECTED_FIELDS", modified):
                with self.assertRaisesRegex(ValueError, "source_schema_drift"):
                    src._decode(body, Path(tmp))

    def test_feature_count_drift_rejected(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp, patch.object(src, "EXPECTED_COUNT", 999):
                with self.assertRaisesRegex(ValueError, "source_feature_count_drift"):
                    src._decode(body, Path(tmp))

    def test_bounds_drift_rejected(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.object(src, "EXPECTED_BOUNDS", [0.0, 0.0, 1.0, 1.0]):
                with self.assertRaisesRegex(ValueError, "source_bounds_drift"):
                    src._decode(body, Path(tmp))

    def test_constant_field_drift_rejected(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.object(src, "EXPECTED_CONSTANT_FIELDS", {"countryName": "mex"}):
                with self.assertRaisesRegex(ValueError, "source_constant_field_drift:countryName"):
                    src._decode(body, Path(tmp))

    def test_degenerate_id_drift_rejected(self):
        rows = _rows()
        rows[1] = dict(rows[1], properties=dict(rows[1]["properties"], mirtaLocationsIdpk="X"))
        with self._fixture(rows=rows) as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaisesRegex(ValueError, "source_id_degenerate_drift"):
                    src._decode(body, Path(tmp))

    def test_value_population_drift_rejected(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.object(src, "EXPECTED_STATUS_COUNTS", {"act": 2, "Excs": 0}):
                with self.assertRaisesRegex(ValueError, "source_value_population_drift"):
                    src._decode(body, Path(tmp))

    def test_state_roster_drift_rejected(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.object(src, "EXPECTED_STATE_SET", {"nc", "tx"}):
                with self.assertRaisesRegex(ValueError, "source_state_roster_drift"):
                    src._decode(body, Path(tmp))

    def test_sds_id_format_drift_rejected(self):
        rows = _rows()
        rows[1] = dict(rows[1], properties=dict(rows[1]["properties"], sdsId="not-a-guid"))
        with self._fixture(rows=rows) as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaisesRegex(ValueError, "source_sds_id_format_drift"):
                    src._decode(body, Path(tmp))

    def test_sds_population_drift_rejected(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.object(src, "EXPECTED_SDS_GUID_COUNT", 99):
                with self.assertRaisesRegex(ValueError, "source_sds_population_drift"):
                    src._decode(body, Path(tmp))

    def test_content_snapshot_drift_rejected(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp, patch.object(src, "RECORD_SHA", "1" * 64):
                with self.assertRaisesRegex(ValueError, "source_content_snapshot_drift"):
                    src._decode(body, Path(tmp))

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
                for changes in ({"url": "https://example.org/mirror.zip", "final_url": "https://example.org/mirror.zip"},
                                {"method": "POST"}, {"complete_body": False}):
                    with self.subTest(name=name, changes=changes):
                        (root / f"{name}.receipt.json").write_text(json.dumps({**receipt, **changes}))
                        with self.assertRaisesRegex(ValueError, "capture_receipt_invalid"):
                            _load_capture(name, root)
                    (root / f"{name}.receipt.json").write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "unexpected_source_capture_name"):
                _load_capture("unknown", root)


if __name__ == "__main__":
    unittest.main()

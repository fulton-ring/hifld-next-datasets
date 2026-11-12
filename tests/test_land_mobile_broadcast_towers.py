"""Focused contract checks for the FCC ULS land mobile broadcast towers adapter."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus

from dagster_hifld.sources import land_mobile_broadcast_towers as source

CREATION_DATE = "Sun Nov  8 09:29:39 EST 2026"
FIXTURE_ZIP_BYTES = 0


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _line(record_type, values, width):
    fields = list(values)[:width - 1] + [""] * max(0, width - 1 - len(values))
    return record_type + "|" + "|".join(fields)


def _hd_line(uid, callsign, status="A", service="RP", grant="08/11/1986", expire="04/01/2030"):
    return _line("HD", [str(uid), "0000462719", "", callsign, status, service, grant, expire], 59)


def _en_line(uid, callsign, entity_type="L", licensee_id="000123456", name="FIXTURE BROADCASTER"):
    return _line("EN", [str(uid), "", "", callsign, entity_type, licensee_id, name], 30)


def _lo_line(uid, callsign, location_type, lat=("", "", "", ""), lon=("", "", "", ""),
             loc_name="FIXTURE SITE", structure_height="43.0"):
    values = {
        "unique_system_identifier": str(uid), "uls_file_number": "0000462719", "ebf_number": "",
        "call_sign": callsign, "location_action_performed": "", "location_type_code": location_type,
        "location_class_code": "T", "location_number": "1", "site_status": "",
        "corresponding_fixed_location": "", "location_address": "100 FIXTURE STREET",
        "location_city": "HERNDON", "location_county": "LOUDOUN", "location_state": "VA",
        "radius_of_operation": "", "area_of_operation_code": "", "clearance_indicator": "",
        "ground_elevation": "12.5", "latitude_degrees": lat[0], "latitude_minutes": lat[1],
        "latitude_seconds": lat[2], "latitude_direction": lat[3], "longitude_degrees": lon[0],
        "longitude_minutes": lon[1], "longitude_seconds": lon[2], "longitude_direction": lon[3],
        "max_latitude_degrees": "", "max_latitude_minutes": "", "max_latitude_seconds": "",
        "max_latitude_direction": "", "max_longitude_degrees": "", "max_longitude_minutes": "",
        "max_longitude_seconds": "", "max_longitude_direction": "", "nepa": "N", "quiet_zone_notification_date": "", "tower_registration_number": "A0001234",
        "height_of_support_structure": structure_height,
        "overall_height_of_structure": structure_height, "structure_type": "MAST",
        "airport_id": "", "location_name": loc_name, "units_hand_held": "2", "units_mobile": "0",
        "units_temp_fixed": "0", "units_aircraft": "0", "units_itinerant": "0",
        "location_status_code": "", "location_status_date": "", "earth_station_agreement": "",
    }
    ordered = [values[name] for name, _kind in source.LOCATION_FIELDS if name != "record_type"]
    return _line("LO", ordered, 51)


def _counts_file(hd=0, en=0, lo=0):
    lines = [f"File Creation Date: {CREATION_DATE}"]
    counts = {"AN": 0, "AS": 0, "BC": 0, "BF": 0, "CO": 0, "CP": 0, "EM": 0, "EN": en,
              "FF": 0, "FR": 0, "HD": hd, "HS": 0, "LA": 0, "LF": 0, "LM": 0, "LO": lo,
              "LS": 0, "OP": 0, "SC": 0, "SF": 0, "ir": 0}
    for name, count in sorted(counts.items()):
        lines.append(f"   {count:6d} /home/pubacc/scripts/licweekzipdata/{name}.dat")
    return ("\n".join(lines) + "\n").encode("latin-1")


def _fixture_tables(hd_lines, en_lines, lo_lines):
    tables = {f"{name}.dat": b"" for name in ("AN", "AS", "BC", "BF", "CO", "CP", "EM", "FF",
                                              "FR", "HS", "LA", "LF", "LM", "LS", "OP", "SC", "SF")}
    tables["HD.dat"] = ("\n".join(hd_lines) + "\n").encode("latin-1") if hd_lines else b""
    tables["EN.dat"] = ("\n".join(en_lines) + "\n").encode("latin-1") if en_lines else b""
    tables["LO.dat"] = ("\n".join(lo_lines) + "\n").encode("latin-1") if lo_lines else b""
    tables["counts"] = _counts_file(hd=len(hd_lines), en=len(en_lines), lo=len(lo_lines))
    tables["ir.dat"] = b""
    return {name: body if body.endswith(b"\n") or body == b"" else body + b"\n"
            for name, body in tables.items()}


def _fixture_zip(tables=None, extra_members=(), missing_members=()):
    if tables is None:
        hd_lines = [_hd_line(1100664, "BLN00751", service="LP"), _hd_line(1102015, "DKQA998")]
        en_lines = [_en_line(1100664, "BLN00751"), _en_line(1102015, "DKQA998")]
        lo_lines = [
            _lo_line(1100664, "BLN00751", "F", lat=("38", "57", "35.4", "N"), lon=("77", "26", "47.0", "W")),
            _lo_line(1102015, "DKQA998", "F"),
            _lo_line(1103000, "KA2000", "M", lat=("37", "52", "7.7", "N"), lon=("122", "15", "37.9", "W"))]
        for index in range(150):
            uid = 1200000 + index
            latitude = ("45", f"{index % 60:02d}", "0.0", "N")
            longitude = ("100", f"{index % 60:02d}", "0.0", "W")
            hd_lines.append(_hd_line(uid, f"BLT{index:05d}"))
            en_lines.append(_en_line(uid, f"BLT{index:05d}"))
            lo_lines.append(_lo_line(uid, f"BLT{index:05d}", "F", lat=latitude, lon=longitude))
        tables = _fixture_tables(hd_lines, en_lines, lo_lines)
    members = {name: body for name, body in tables.items()}
    for name in extra_members:
        members[name] = b"x\n"
    for name in missing_members:
        members.pop(name, None)
    sink = io_bytes_zip(members)
    return sink


def io_bytes_zip(members):
    sink_bytes = io.BytesIO()
    with zipfile.ZipFile(sink_bytes, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return sink_bytes.getvalue()


import io  # noqa: E402  (kept close to the fixture helpers it serves)


class EmptyClient:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _fake_capture_get(bodies):
    def fake(_client, url, capture_root, name, *, max_bytes):
        body = bodies[name]
        digest = hashlib.sha256(body).hexdigest()
        receipt = {"method": "GET", "url": url, "final_url": url,
                   "observed_at": "2026-11-12T00:00:00+00:00", "status": 200,
                   "complete_body": True, "wire_bytes": len(body), "wire_sha256": digest,
                   "bytes": len(body), "sha256": digest, "content_encoding": None,
                   "content_length": str(len(body)), "error_code": None}
        (capture_root / f"{name}.body").write_bytes(body)
        (capture_root / f"{name}.wire").write_bytes(body)
        (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        return body, receipt
    return fake


class LandMobileBroadcastTowersContractTests(unittest.TestCase):
    def test_source_contract_is_fcc_producer(self):
        self.assertEqual(source.DATASET_SLUG, "land-mobile-broadcast-towers-1")
        self.assertEqual(source.FILE_SLUG, source.DATASET_SLUG)
        self.assertEqual(source.SOURCE_URL,
                         "https://data.fcc.gov/download/pub/uls/complete/l_LMbcast.zip")
        self.assertEqual(source.LANDING,
                         "https://www.fcc.gov/wireless/data/public-access-files-database-downloads")
        self.assertIn("data.fcc.gov", source.SOURCE_URL)
        self.assertEqual(source.PUBLISHER,
                         "Federal Communications Commission, Wireless Telecommunications Bureau")
        self.assertEqual(source.EXPECTED_SERVICE_CODES, {"LP", "RP", "LV"})
        self.assertEqual(len(source.EXPECTED_MEMBERS), 22)
        self.assertIn("LO.dat", source.EXPECTED_MEMBERS)
        self.assertIn("counts", source.EXPECTED_MEMBERS)
        self.assertEqual(source.FIXED_LOCATION_TYPE, "F")

    def test_official_lo_layout_is_pinned(self):
        names = [name for name, _ in source.LOCATION_FIELDS]
        self.assertEqual(len(names), 51)
        self.assertEqual(names[0], "record_type")
        self.assertEqual(names[1], "unique_system_identifier")
        self.assertEqual(names[4], "call_sign")
        self.assertEqual(names[6], "location_type_code")
        self.assertEqual(names[18], "ground_elevation")
        self.assertEqual(names[19:23], ["latitude_degrees", "latitude_minutes",
                                        "latitude_seconds", "latitude_direction"])
        self.assertEqual(names[23:27], ["longitude_degrees", "longitude_minutes",
                                        "longitude_seconds", "longitude_direction"])
        self.assertEqual(names[37], "tower_registration_number")
        self.assertEqual(names[43], "units_hand_held")
        self.assertEqual(names[-1], "earth_station_agreement")
        self.assertEqual(source.LO_EXPECTED_FIELD_COUNT, 51)
        self.assertEqual(source.HD_EXPECTED_FIELD_COUNT, 59)
        self.assertEqual(source.EN_EXPECTED_FIELD_COUNT, 30)
        self.assertEqual(source.LOCATION_TYPE_INDEX, 6)
        self.assertEqual(source.HD_JOIN_INDEXES["radio_service_code"], 6)
        self.assertEqual(source.EN_ENTITY_TYPE_INDEX, 5)

    def test_dms_to_decimal_matches_fcc_native_values(self):
        self.assertAlmostEqual(source._dms_to_decimal(38, 57, 35.4, "N", max_degrees=90),
                               38 + 57 / 60 + 35.4 / 3600)
        self.assertAlmostEqual(source._dms_to_decimal(77, 26, 47.0, "W", max_degrees=180),
                               -(77 + 26 / 60 + 47 / 3600))
        self.assertAlmostEqual(source._dms_to_decimal(170, 39, 44.9, "E", max_degrees=180),
                               170 + 39 / 60 + 44.9 / 3600)
        self.assertAlmostEqual(source._dms_to_decimal(13, 28, 0.0, "S", max_degrees=90),
                               -(13 + 28 / 60))
        self.assertIsNone(source._dms_to_decimal(38, 57, 70.0, "N", max_degrees=90))
        self.assertIsNone(source._dms_to_decimal(38, 60, 0.0, "N", max_degrees=90))
        self.assertIsNone(source._dms_to_decimal(38, 57, 35.4, "X", max_degrees=90))
        self.assertIsNone(source._dms_to_decimal(91, 0, 0.0, "N", max_degrees=90))
        self.assertIsNone(source._dms_to_decimal(181, 0, 0.0, "W", max_degrees=180))
        self.assertIsNone(source._dms_to_decimal(None, 57, 35.4, "N", max_degrees=90))

    def test_point_wkb_is_little_endian_point(self):
        self.assertEqual(source._point_wkb(1.5, -2.25).hex(),
                         "0101000000000000000000f83f00000000000002c0")


class LandMobileBroadcastTowersDecodeTests(unittest.TestCase):
    def setUp(self):
        self.zip_body = _fixture_zip()
        self.features, self.audit, self.parquet = source._decode(self.zip_body)

    def test_fixed_locations_become_tower_features_and_mobiles_are_excluded(self):
        self.assertEqual(len(self.features), 151)
        uids = [feature["unique_system_identifier"] for feature in self.features]
        self.assertEqual(uids[0], 1100664)
        self.assertNotIn(1102015, uids)
        self.assertEqual(self.audit["lo_record_count"], 153)
        self.assertEqual(self.audit["fixed_location_count"], 152)
        self.assertEqual(self.audit["mobile_location_count"], 1)
        self.assertEqual(self.audit["invalid_coordinate_record_count"], 0)
        self.assertEqual(self.audit["missing_coordinate_record_count"], 1)
        self.assertEqual(self.audit["null_geometry_count"], 0)
        self.assertEqual(self.audit["service_code_counts"], {"LP": 1, "RP": 151})
        self.assertEqual(self.audit["uls_file_creation_date"], CREATION_DATE)
        self.assertTrue(self.audit["counts_completeness_verified"])

    def test_native_dms_values_and_joined_license_fields_are_preserved(self):
        first = self.features[0]
        self.assertEqual(first["record_type"], "LO")
        self.assertEqual(first["call_sign"], "BLN00751")
        self.assertEqual(first["location_city"], "HERNDON")
        self.assertEqual(first["location_state"], "VA")
        self.assertEqual(first["ground_elevation"], 12.5)
        self.assertEqual(first["tower_registration_number"], "A0001234")
        self.assertEqual(first["height_of_support_structure"], 43.0)
        self.assertEqual(first["overall_height_of_structure"], 43.0)
        self.assertEqual(first["structure_type"], "MAST")
        self.assertEqual(first["units_hand_held"], 2)
        self.assertEqual(first["latitude_degrees"], 38)
        self.assertEqual(first["latitude_minutes"], 57)
        self.assertEqual(first["latitude_seconds"], 35.4)
        self.assertEqual(first["latitude_direction"], "N")
        self.assertEqual(first["longitude_direction"], "W")
        self.assertEqual(first["license_status"], "A")
        self.assertEqual(first["radio_service_code"], "LP")
        self.assertEqual(first["grant_date"], "08/11/1986")
        self.assertEqual(first["expired_date"], "04/01/2030")
        self.assertEqual(first["licensee_id"], "000123456")
        self.assertEqual(first["licensee_name"], "FIXTURE BROADCASTER")
        self.assertAlmostEqual(first["_longitude"], -(77 + 26 / 60 + 47 / 3600))
        self.assertAlmostEqual(first["_latitude"], 38 + 57 / 60 + 35.4 / 3600)
        second = self.features[1]
        self.assertEqual(second["call_sign"], "BLT00000")
        self.assertEqual(second["radio_service_code"], "RP")
        self.assertAlmostEqual(second["_longitude"], -(100 + 0 / 60))
        self.assertAlmostEqual(second["_latitude"], 45.0)

    def test_parquet_preserves_columns_values_and_null_geometry(self):
        table = pq.read_table(pa.BufferReader(self.parquet))
        expected_names = [name for name, _ in source.OUTPUT_FIELD_TYPES]
        self.assertEqual(table.column_names, expected_names)
        self.assertEqual(table.num_rows, 151)
        self.assertEqual(table["call_sign"].to_pylist()[:2], ["BLN00751", "BLT00000"])
        self.assertEqual(table["licensee_name"].to_pylist()[:2],
                         ["FIXTURE BROADCASTER", "FIXTURE BROADCASTER"])
        self.assertEqual(table["license_status"].to_pylist()[:2], ["A", "A"])
        self.assertEqual(table["ground_elevation"].to_pylist()[:2], [12.5, 12.5])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"][source.GEOMETRY_COLUMN]["crs"]["id"]["code"], 4269)
        self.assertEqual(geo["columns"][source.GEOMETRY_COLUMN]["geometry_types"], ["Point"])
        first = self.features[0]
        first_wkb = table[source.GEOMETRY_COLUMN][0].as_py()
        self.assertEqual(source._point_wkb(first["_longitude"], first["_latitude"]), first_wkb)
        longitude = self.features[1]["_longitude"]
        self.assertAlmostEqual(longitude, -(100 + 0 / 60))
        self.assertEqual(table[source.GEOMETRY_COLUMN][1].as_py(),
                         source._point_wkb(longitude, self.features[1]["_latitude"]))

    def test_counts_manifest_mismatch_is_rejected(self):
        tables = _fixture_tables(
            [_hd_line(1100664, "BLN00751")], [_en_line(1100664, "BLN00751")],
            [_lo_line(1100664, "BLN00751", "F", lat=("38", "57", "35.4", "N"),
                      lon=("77", "26", "47.0", "W"))])
        tables["HD.dat"] = (tables["HD.dat"].decode("latin-1")
                            + _hd_line(1109000, "BLN99999") + "\n").encode("latin-1")
        body = _fixture_zip(tables)
        with self.assertRaisesRegex(ValueError, "uls_counts_manifest_mismatch"):
            source._decode(body)

    def test_member_roster_and_record_type_drift_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "uls_archive_member_roster_drift"):
            source._decode(_fixture_zip(extra_members=("ZZ.dat",)))
        with self.assertRaisesRegex(ValueError, "uls_archive_member_roster_drift"):
            source._decode(_fixture_zip(missing_members=("SF.dat",)))
        tables = _fixture_tables(
            [_hd_line(1100664, "BLN00751")], [_en_line(1100664, "BLN00751")],
            [_lo_line(1100664, "BLN00751", "F", lat=("38", "57", "35.4", "N"),
                      lon=("77", "26", "47.0", "W"))])
        tables["LO.dat"] = (tables["LO.dat"].decode("latin-1").replace("LO|", "LX|", 1)
                            ).encode("latin-1")
        with self.assertRaisesRegex(ValueError, "uls_record_type_drift:LO"):
            source._decode(_fixture_zip(tables))

    def test_field_count_drift_is_rejected(self):
        tables = _fixture_tables(
            [_hd_line(1100664, "BLN00751")], [_en_line(1100664, "BLN00751")],
            [_lo_line(1100664, "BLN00751", "F", lat=("38", "57", "35.4", "N"),
                      lon=("77", "26", "47.0", "W"))])
        drifted_lo = tables["LO.dat"].decode("latin-1").rstrip("\n") + "|\n"
        tables["LO.dat"] = drifted_lo.encode("latin-1")
        drifted_counts = _counts_file(hd=1, en=1, lo=1)
        tables["counts"] = drifted_counts
        with self.assertRaisesRegex(ValueError, "uls_field_count_drift:LO:52"):
            source._decode(_fixture_zip(tables))

    def test_unexpected_radio_service_code_is_rejected(self):
        tables = _fixture_tables(
            [_hd_line(1100664, "BLN00751", service="SL")], [_en_line(1100664, "BLN00751")],
            [_lo_line(1100664, "BLN00751", "F", lat=("38", "57", "35.4", "N"),
                      lon=("77", "26", "47.0", "W"))])
        with self.assertRaisesRegex(ValueError, "uls_unexpected_radio_service_code"):
            source._decode(_fixture_zip(tables))

    def test_fixed_location_without_license_header_is_rejected(self):
        tables = _fixture_tables(
            [_hd_line(1100664, "BLN00751")], [_en_line(1100664, "BLN00751")],
            [_lo_line(1100664, "BLN00751", "F", lat=("38", "57", "35.4", "N"),
                      lon=("77", "26", "47.0", "W")),
             _lo_line(1107777, "BLN77777", "F", lat=("38", "57", "35.4", "N"),
                      lon=("77", "26", "47.0", "W"))])
        tables["counts"] = _counts_file(hd=1, en=1, lo=2)
        with self.assertRaisesRegex(ValueError, "uls_fixed_location_without_license_header:1"):
            source._decode(_fixture_zip(tables))

    def test_callsign_disagreement_is_rejected(self):
        tables = _fixture_tables(
            [_hd_line(1100664, "BLN00751")], [_en_line(1100664, "BLN00751")],
            [_lo_line(1100664, "BLN00099", "F", lat=("38", "57", "35.4", "N"),
                      lon=("77", "26", "47.0", "W"))])
        with self.assertRaisesRegex(ValueError, "uls_fixed_location_callsign_disagreement"):
            source._decode(_fixture_zip(tables))

    def test_excessive_null_geometry_is_rejected(self):
        tables = _fixture_tables(
            [_hd_line(1100664, "BLN00751")], [_en_line(1100664, "BLN00751")],
            [_lo_line(1100664, "BLN00751", "F")])
        with self.assertRaisesRegex(ValueError, "uls_excessive_unmappable_fixed_locations"):
            source._decode(_fixture_zip(tables))


class LandMobileBroadcastTowersLifecycleTests(unittest.TestCase):
    def test_capture_seal_catalog_then_reuse(self):
        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        zip_body = _fixture_zip()
        bodies = {"lm_bcast": zip_body}
        with tempfile.TemporaryDirectory(prefix="lmbt-e2e-") as temporary:
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.object(source, "capture_get", side_effect=_fake_capture_get(bodies)), \
                    patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_land_mobile_broadcast_towers(
                    storage, version="vfixture", capture_directory=str(root / "captures"))
                self.assertFalse(candidate["reused"])
                self.assertEqual(candidate["feature_count"], 151)
                self.assertEqual(candidate["fixed_location_count"], 152)
                self.assertEqual(candidate["source_population_complete"], True)
                self.assertEqual(candidate["null_geometry_count"], 0)
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                self.assertEqual(storage.read_key(f"{prefix}/raw/lm_bcast.body"), zip_body)
                sealed = storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet")
                snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG, candidate["version"])
                manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG,
                                                         source.FILE_SLUG, candidate["version"]).metadata
                self.assertEqual(manifest["date_issued"], "2026-11-08")
                self.assertFalse(manifest["approved"])
                self.assertEqual(manifest["inputs"][0]["producer"], source.PUBLISHER)
                self.assertEqual(manifest["inputs"][0]["release_date"], "2026-11-08")
                self.assertEqual(manifest["inputs"][0]["file_creation_timestamp_local"], CREATION_DATE)
                self.assertIn("data.fcc.gov", manifest["inputs"][0]["retrieval_url"])
                summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                   candidate["version"], source.DATASET_SLUG,
                                                   source_metadata=manifest)
                self.assertEqual(summary.quality_manifest["feature_count"], 151)
                self.assertEqual(summary.quality_manifest["null_geometry_count"], 0)
                self.assertTrue(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG,
                                       file_slug=source.FILE_SLUG, version=candidate["version"],
                                       quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_land_mobile_broadcast_towers(
                    storage, version="vfixture", capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])
                self.assertEqual(storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet"), sealed)

    def test_incomplete_capture_is_quarantined_with_retained_receipt(self):
        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            body = b"partial fixture bytes"
            digest = hashlib.sha256(body).hexdigest()
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            receipt = {"method": "GET", "url": url, "final_url": url,
                       "observed_at": "2026-11-12T00:00:00+00:00", "status": 200,
                       "complete_body": False, "wire_bytes": len(body), "wire_sha256": digest,
                       "bytes": len(body), "sha256": digest, "content_encoding": None,
                       "content_length": None, "error_code": "incomplete_source_capture"}
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            raise OSError("connection reset")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                    patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                with self.assertRaises(OSError):
                    source.capture_land_mobile_broadcast_towers(storage, version="vfixture",
                                                                capture_directory=str(root / "captures"))
            self.assertTrue(sorted(str(p) for p in (root / "staging" / "_quarantine").rglob("lm_bcast.body")))
            self.assertTrue(list((root / "staging" / "_quarantine").rglob("failure.json")))
            manifest = json.loads((root / "captures" / "capture_manifest.json").read_bytes())
            self.assertFalse(manifest["complete"])
            self.assertEqual(manifest["error_code"], "OSError")

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          f"geoparquet/{source.FILE_SLUG}.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           f"geoparquet/{source.FILE_SLUG}.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError,
                                            "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, source.DATASET_SLUG,
                                        source.FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)


class LandMobileBroadcastTowersRegistrationTests(unittest.TestCase):
    def test_schedule_is_stopped_and_pipeline_registered(self):
        from dagster_hifld.assets.land_mobile_broadcast_towers import (
            land_mobile_broadcast_towers_draft_schedule,
        )
        from dagster_hifld.definitions import defs
        self.assertEqual(land_mobile_broadcast_towers_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        slug = source.DATASET_SLUG
        graph = defs.resolve_asset_graph()
        keys = {tuple(key.path) for key in graph.get_all_asset_keys()}
        self.assertIn(("ingest", slug, slug), keys)
        self.assertTrue(any(key.name == "land_mobile_broadcast_towers_complete"
                            for key in graph.asset_check_keys))
        job = defs.resolve_job_def("land_mobile_broadcast_towers_draft_refresh")
        job_checks = list(job.asset_layer.asset_graph.asset_check_keys)
        self.assertEqual(len(job_checks), 1)
        self.assertEqual(job_checks[0].name, "land_mobile_broadcast_towers_complete")
        schedule = defs.get_schedule_def("land_mobile_broadcast_towers_draft_refresh_schedule")
        self.assertEqual(schedule.name, "land_mobile_broadcast_towers_draft_refresh_schedule")


if __name__ == "__main__":
    unittest.main()

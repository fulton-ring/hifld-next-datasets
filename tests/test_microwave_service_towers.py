import io
import json
import struct
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from dagster import DefaultScheduleStatus
import pyarrow.parquet as pq
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.microwave_service_towers import (
    microwave_service_towers, microwave_service_towers_complete,
    microwave_service_towers_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import microwave_service_towers as source
from dagster_hifld.sources.epa_frs_common import _canonical


def _sha(value):
    import hashlib
    return hashlib.sha256(value).hexdigest()


def _hd(identifier, callsign, status, service="MG"):
    row = [""] * 59
    row[0], row[1], row[4], row[5], row[6] = "HD", str(identifier), callsign, status, service
    return "|".join(row)


def _en(identifier, callsign, entity_type, name):
    row = [""] * 30
    row[0], row[1], row[4], row[5], row[7] = "EN", str(identifier), callsign, entity_type, name
    return "|".join(row)


def _lo(identifier, callsign, location_number, **values):
    row = [""] * 51
    row[0], row[1], row[4], row[8] = "LO", str(identifier), callsign, str(location_number)
    row[11], row[12], row[13], row[14] = values.get("address", "1 TOWER ROAD"), values.get("city", "TOWNSVILLE"), values.get("county", "EXAMPLE"), values.get("state", "CA")
    row[19], row[20], row[21], row[22] = str(values.get("lat_deg", 38)), str(values.get("lat_min", 30)), values.get("lat_sec", "15.0"), values.get("lat_dir", "N")
    row[23], row[24], row[25], row[26] = str(values.get("lon_deg", 121)), str(values.get("lon_min", 45)), values.get("lon_sec", "30.0"), values.get("lon_dir", "W")
    row[35], row[36], row[37] = values.get("nepa", "N"), values.get("qzone", ""), values.get("towreg", "1023448")
    row[38], row[39], row[40] = values.get("supstruc", "91.4"), values.get("allstruc", "99.0"), values.get("structype", "TOWER")
    return "|".join(row)


def _documents():
    hd = [_hd(101, "KNKA101", "A"), _hd(102, "KNKC102", "C"), _hd(103, "KNKA103", "A", service="CF"),
          _hd(104, "KNKA104", "E"), _hd(105, "", "A")]
    en = [_en(101, "KNKA101", "L", "Licensee One"), _en(102, "KNKC102", "L", "Licensee Two"),
          _en(103, "KNKA103", "L", "Licensee Three"), _en(104, "KNKA104", "L", "Licensee Four"),
          _en(105, "", "L", "Licensee Five"), _en(101, "KNKA101", "CL", "Contact One")]
    lo = [
        _lo(101, "KNKA101", 1, state="CA"),
        _lo(101, "KNKA101", 2, state="TX", towreg="", structype="", supstruc="", allstruc="", qzone="01/02/2003"),
        _lo(102, "KNKC102", 5, state="WA"),
        _lo(103, "KNKA103", 1, state="LA"),
        _lo(105, "", 1, state=""),
        # Source-observed duplicate location rows (re-filed coordinates)
        _lo(103, "KNKA103", 2, lat_deg="38", lat_min="19", lat_sec="36.5", lon_deg="122", lon_min="7", lon_sec="20.7"),
        _lo(103, "KNKA103", 2, lat_deg="38", lat_min="20", lat_sec="5.2", lon_deg="122", lon_min="6", lon_sec="20.8"),
    ]
    return hd, en, lo


def _members(hd, en, lo, extra=()):
    content = {}
    for name in source.ZIP_MEMBER_ROSTER:
        if name == "HD.dat":
            content[name] = ("\r\n".join(hd) + "\r\n").encode()
        elif name == "EN.dat":
            content[name] = ("\r\n".join(en) + "\r\n").encode()
        elif name == "LO.dat":
            content[name] = ("\r\n".join(lo) + "\r\n").encode()
        elif name == "counts":
            content[name] = b"File Creation Date: Sun Nov  8 09:47:03 EST 2026\r\n  926693 /home/pubacc/scripts/licweekzipdata/LO.dat\r\n"
        else:
            content[name] = b""
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for name in [*source.ZIP_MEMBER_ROSTER, *extra]:
            archive.writestr(name, content.get(name, b""))
    return stream.getvalue()


def _capture(body):
    return {"l_micro": (body, {"method": "GET", "url": source.L_MICRO_URL, "final_url": source.L_MICRO_URL,
                               "status": 200, "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}


class MicrowaveServiceTowersTests(unittest.TestCase):
    def _pins(self, hd, en, lo):
        active = [(101, 1), (101, 2), (103, 1), (103, 2), (103, 2), (105, 1)]
        roster = sorted(active)
        states = sorted({"CA", "TX", "LA"})
        duplicates = sorted([list(key) for key, count in
                             {key: active.count(key) for key in set(active)}.items() if count > 1])
        return {
            "EXPECTED_HD_RECORDS": len(hd), "EXPECTED_EN_RECORDS": len(en), "EXPECTED_LO_RECORDS": len(lo),
            "EXPECTED_LICENSE_STATUS_COUNTS": {"A": 3, "C": 1, "E": 1}, "EXPECTED_LICENSEE_ROWS": 5,
            "EXPECTED_ACTIVE_LOCATIONS": 6,
            "EXPECTED_ACTIVE_ID_SHA256": _sha(_canonical(roster)),
            "EXPECTED_STATE_ROSTER_SHA256": _sha(_canonical(states)),
            "EXPECTED_NULL_GEOMETRY_COUNT": 0,
            "EXPECTED_DUPLICATE_LOCATION_KEYS": len(duplicates),
            "EXPECTED_DUPLICATE_KEY_SHA256": _sha(_canonical(duplicates)),
            "EXPECTED_ACTIVE_SERVICE_ROW_COUNTS": {"MG": 3, "CF": 3},
            "EXPECTED_INVALID_DIRECTION_COUNT": 0,
        }

    def _validate(self, hd, en, lo, pins=None):
        patches = [mock.patch.object(source, name, value)
                   for name, value in (pins or self._pins(hd, en, lo)).items()]
        for patch in patches:
            patch.start()
        try:
            return source._validate(_capture(_members(hd, en, lo)))
        finally:
            for patch in patches:
                patch.stop()

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(microwave_service_towers, hifld_ingest_assets)
        self.assertEqual(microwave_service_towers_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = microwave_service_towers_complete({"feature_count": source.EXPECTED_ACTIVE_LOCATIONS,
                                                   "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = microwave_service_towers_complete({"feature_count": source.EXPECTED_ACTIVE_LOCATIONS + 1,
                                                    "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_complete_native_population_and_published_contract(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        self.assertEqual([(row["UniqSysID"], row["LocNum"]) for row in published],
                         [(101, 1), (101, 2), (103, 1), (103, 2), (103, 2), (105, 1)])
        self.assertTrue(all(row["LicStatus"] == "A" for row in published))
        self.assertEqual(audit["source_feature_count"], 6)
        self.assertEqual(audit["source_license_status_counts"], {"A": 3, "C": 1, "E": 1})
        self.assertEqual(audit["source_radio_service_codes"], sorted(source.EXPECTED_HD_RADIO_SERVICES))
        self.assertEqual(audit["source_service_row_counts"], {"CF": 3, "MG": 3})
        self.assertEqual(audit["source_state_count"], 3)
        self.assertEqual(audit["source_null_geometry_count"], 0)
        self.assertEqual(audit["source_duplicate_location_key_count"], 1)
        self.assertEqual(audit["source_invalid_direction_count"], 0)
        self.assertEqual(audit["source_file_creation_date"], "Sun Nov  8 09:47:03 EST 2026")
        self.assertEqual(audit["source_bounds"], [-122.122416667, 38.3268055556, -121.758333333, 38.5041666667])
        self.assertIn("not a census of physical towers", audit["source_coverage_scope"])
        first = published[0]
        self.assertEqual(first["Licensee"], "Licensee One")
        self.assertEqual(first["url"], source.LICENSE_URL_TEMPLATE.format(101))
        self.assertEqual(struct.unpack("<BIdd", first["geometry"]), (1, 1, -121.758333333, 38.5041666667))
        duplicate_pair = [row for row in published if (row["UniqSysID"], row["LocNum"]) == (103, 2)]
        self.assertEqual([row["latdec"] for row in duplicate_pair], [38.3268055556, 38.3347777778])
        self.assertEqual([row["londec"] for row in duplicate_pair], [-122.122416667, -122.105777778])
        empty = published[1]
        self.assertEqual(empty["TowReg"], "")
        self.assertEqual(empty["StrucType"], "")
        self.assertIsNone(empty["SupStruc"])
        self.assertIsNone(empty["AllStruc"])
        blank_callsign = published[5]
        self.assertEqual(blank_callsign["Callsign"], "")
        self.assertEqual(blank_callsign["Licensee"], "Licensee Five")
        self.assertEqual(blank_callsign["LocState"], "")

    def test_parquet_candidate_preserves_columns_types_and_verbatim_values(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        body = source._parquet_bytes(published)
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.schema.names, [name for name, _ in source.PUBLISHED_SCHEMA])
        self.assertEqual(table.num_rows, 6)
        rows = table.to_pylist()
        self.assertEqual(rows[0]["UniqSysID"], 101)
        self.assertEqual(rows[0]["TowReg"], "1023448")
        self.assertIsNone(rows[1]["SupStruc"])
        self.assertEqual(rows[4]["latdec"], 38.3347777778)
        self.assertEqual(rows[5]["Callsign"], "")
        geometry_meta = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geometry_meta["primary_column"], "geometry")
        self.assertEqual(geometry_meta["columns"]["geometry"]["geometry_types"], ["Point"])
        self.assertEqual(geometry_meta["columns"]["geometry"]["crs"]["id"]["code"], 4326)
        self.assertEqual(audit["source_record_sha256"], _sha(_canonical(
            [{key: row[key] for key, _ in source.PUBLISHED_SCHEMA if key != "geometry"} for row in published])))

    def test_null_geometry_and_out_of_nominal_directions_are_preserved(self):
        hd, en, lo = _documents()
        lo = [
            lo[0], _lo(101, "KNKA101", 2, state="TX", lat_deg="", lat_min="", lat_sec="", lat_dir="", lon_deg="", lon_min="", lon_sec="", lon_dir=""),
            lo[2], lo[3], _lo(105, "", 1, state="", lat_deg="39", lat_min="46", lat_sec="7.1", lat_dir="W", lon_deg="86", lon_min="9", lon_sec="46.0", lon_dir="N"),
            lo[5], lo[6],
        ]
        published, audit = self._validate(hd, en, lo, self._pins(hd, en, lo) | {"EXPECTED_NULL_GEOMETRY_COUNT": 1,
                                                                                "EXPECTED_INVALID_DIRECTION_COUNT": 1})
        null_row = published[1]
        self.assertIsNone(null_row["geometry"])
        self.assertIsNone(null_row["latdec"])
        self.assertEqual(null_row["LatDir"], "")
        weird = published[5]
        self.assertEqual(weird["latdec"], 39.7686388889)
        self.assertEqual(weird["londec"], 86.1627777778)
        self.assertEqual((weird["LatDir"], weird["LonDir"]), ("W", "N"))
        self.assertIsNotNone(weird["geometry"])
        self.assertEqual(audit["source_null_geometry_count"], 1)
        self.assertEqual(audit["source_invalid_direction_count"], 1)

    def test_drift_guards_fail_closed(self):
        cases = [
            ("record field count", lambda hd, en, lo: (hd, en, [lo[0] + "|", *lo[1:]]),
             "source_record_field_count_drift:LO"),
            ("record prefix", lambda hd, en, lo: ([hd[0], hd[1].replace("HD|", "XX|", 1), *hd[2:]], en, lo),
             "source_record_type_prefix_drift:HD"),
            ("record totals", lambda hd, en, lo: (hd[:4], en, lo), "source_record_population_drift"),
            ("radio service", lambda hd, en, lo: ([hd[0].replace("|MG|", "|CX|"), *hd[1:]], en, lo),
             "source_radio_service_roster_drift"),
            ("status distribution", lambda hd, en, lo: ([hd[0].replace("|A|", "|E|"), *hd[1:]], en, lo),
             "source_license_status_distribution_drift"),
            ("licensee join", lambda hd, en, lo: (hd, [_en(101, "KNKA101", "CL", "Contact Two"), *en[1:]], lo),
             "source_licensee_join_drift"),
            ("callsign join", lambda hd, en, lo: (hd, en, [_lo(101, "KNKX999", 1), *lo[1:]]),
             "source_callsign_join_drift"),
            ("missing location number", lambda hd, en, lo: (hd, en, [_lo(101, "KNKA101", ""), *lo[1:]]),
             "source_location_number_invalid"),
            ("state roster", lambda hd, en, lo: (hd, en, [lo[0].replace("|CA|", "|ZZ|"), *lo[1:]]),
             "source_state_roster_drift"),
            ("duplicate key roster", lambda hd, en, lo: (hd, en, [lo[0], lo[1], lo[2], lo[3], _lo(101, "KNKA101", 1), lo[5], _lo(102, "KNKC102", 9, state="WA")]),
             "source_duplicate_key_roster_drift"),
            ("coordinate range", lambda hd, en, lo: (hd, en, [_lo(101, "KNKA101", 1, lon_deg="181"), *lo[1:]]),
             "source_coordinate_out_of_range"),
            ("null geometry", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "KNKA101", 2, lat_deg="", lat_min="", lat_sec="", lat_dir="", lon_deg="", lon_min="", lon_sec="", lon_dir=""), *lo[2:]]),
             "source_null_geometry_count_drift"),
            ("partial dms", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "KNKA101", 2, lon_deg="", lon_min="", lon_sec="", lon_dir=""), *lo[2:]]),
             "source_partial_dms_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                pins = self._pins(*_documents())
                hd, en, lo = mutate(*_documents())
                with self.assertRaisesRegex(ValueError, expected):
                    self._validate(hd, en, lo, pins)

    def test_population_pins_fail_closed(self):
        hd, en, lo = _documents()
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_ACTIVE_LOCATIONS"] = 7
        with self.assertRaisesRegex(ValueError, "source_active_population_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_ACTIVE_ID_SHA256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "source_active_id_roster_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_STATE_ROSTER_SHA256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "source_state_roster_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_NULL_GEOMETRY_COUNT"] = 1
        with self.assertRaisesRegex(ValueError, "source_null_geometry_count_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_DUPLICATE_LOCATION_KEYS"] = 2
        with self.assertRaisesRegex(ValueError, "source_duplicate_location_key_count_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_ACTIVE_SERVICE_ROW_COUNTS"] = {"MG": 2, "CF": 4}
        with self.assertRaisesRegex(ValueError, "source_service_row_distribution_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_INVALID_DIRECTION_COUNT"] = 1
        with self.assertRaisesRegex(ValueError, "source_invalid_direction_count_drift"):
            self._validate(hd, en, lo, pins)

    def test_zip_container_and_capture_receipts_fail_closed(self):
        hd, en, lo = _documents()
        body = _members(hd, en, lo)
        with self.assertRaisesRegex(ValueError, "source_zip_member_roster_drift"):
            source._validate(_capture(_members(hd, en, lo, extra=["extra.dat"])))
        with self.assertRaisesRegex(ValueError, "source_zip_unreadable"):
            source._validate(_capture(b"not a zip archive"))
        bad_status = {"l_micro": (body, {"method": "GET", "url": source.L_MICRO_URL,
                                         "final_url": source.L_MICRO_URL, "status": 500,
                                         "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "source_capture_status_length_hash_or_completeness_drift"):
            source._validate(bad_status)
        incomplete = {"l_micro": (body, {"method": "GET", "url": source.L_MICRO_URL,
                                         "final_url": source.L_MICRO_URL, "status": 200,
                                         "complete_body": False, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "source_capture_status_length_hash_or_completeness_drift"):
            source._validate(incomplete)
        redirect = {"l_micro": (body, {"method": "GET", "url": source.L_MICRO_URL,
                                       "final_url": "https://elsewhere.example/l_micro.zip", "status": 200,
                                       "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
            source._validate(redirect)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_microwave_service_towers(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

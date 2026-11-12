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
from dagster_hifld.assets.land_mobile_private_transmission_towers import (
    land_mobile_private_transmission_towers,
    land_mobile_private_transmission_towers_complete,
    land_mobile_private_transmission_towers_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import land_mobile_private_transmission_towers as source
from dagster_hifld.sources.epa_frs_common import _canonical


def _sha(value):
    import hashlib
    return hashlib.sha256(value).hexdigest()


def _hd(identifier, callsign, status, service="IG"):
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
    hd = [
        _hd(101, "KNKA101", "A"),
        _hd(102, "KNKC102", "C", service="PW"),
        _hd(103, "KNKA103", "A", service="YG"),
        _hd(104, "", "A"),
    ]
    en = [_en(101, "KNKA101", "L", "Licensee One"), _en(102, "KNKC102", "L", "Licensee Two"),
          _en(103, "KNKA103", "L", "Licensee Three"), _en(104, "", "L", "Licensee Four"),
          _en(101, "KNKA101", "CL", "Contact One")]
    lo = [
        _lo(101, "KNKA101", 1, state="CA", qzone="01/02/2003"),
        # duplicate (UniqSysID, LocNum) filed record, preserved as a distinct row
        _lo(101, "KNKA101", 1, address="9 SECOND RECORD ROAD", towreg="N/A",
            supstruc="7.0", allstruc="9.1", structype="LTOWER"),
        # latitude present but longitude empty: excluded from the population
        _lo(101, "KNKA101", 2, lon_deg="", lon_min="", lon_sec="", lon_dir="", state="CA"),
        # inactive license (status C): excluded from the population
        _lo(102, "KNKC102", 5, state="WA"),
        # out-of-nominal filed latitude preserved verbatim and published
        _lo(103, "KNKA103", 1, lat_deg="93", lat_min="0", lat_sec=".0", lon_deg="93",
            lon_min="36", lon_sec="51.8", state="MO", supstruc="", allstruc=""),
        # southern/eastern hemisphere directions are honored
        _lo(103, "KNKA103", 2, lat_deg="14", lat_min="0", lat_sec="0", lat_dir="S",
            lon_deg="145", lon_min="20", lon_sec="0", lon_dir="E", state="GU"),
        # empty callsign on both HD and LO: joined and published verbatim
        _lo(104, "", 1, lat_deg="36", lat_min="0", lat_sec="0", lon_deg="115",
            lon_min="0", lon_sec="0", state="NV"),
        # empty direction components default to north/west in the derived columns
        _lo(104, "", 2, lat_deg="45", lat_min="0", lat_sec=".0", lat_dir="",
            lon_deg="123", lon_min="0", lon_sec=".0", lon_dir="", state="NV"),
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
            content[name] = (b"File Creation Date: Sun Nov  8 09:16:00 EST 2026\r\n"
                             b"    4 /home/pubacc/scripts/licweekzipdata/HD.dat\r\n"
                             b"    5 /home/pubacc/scripts/licweekzipdata/EN.dat\r\n"
                             b"    8 /home/pubacc/scripts/licweekzipdata/LO.dat\r\n"
                             b"   17 total\r\n")
        else:
            content[name] = b""
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for name in [*source.ZIP_MEMBER_ROSTER, *extra]:
            archive.writestr(name, content.get(name, b""))
    return stream.getvalue()


def _capture(body):
    return {"l_LMpriv": (body, {"method": "GET", "url": source.L_LMPRIV_URL, "final_url": source.L_LMPRIV_URL,
                                "status": 200, "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}


class LandMobilePrivateTowersTests(unittest.TestCase):
    def _pins(self, hd, en, lo):
        roster = sorted([[101, 1], [101, 1], [103, 1], [103, 2], [104, 1], [104, 2]])
        states = sorted({"CA", "MO", "NV", "GU"})
        return {
            "EXPECTED_COUNTS_FILE": {"HD.dat": len(hd), "EN.dat": len(en), "LO.dat": len(lo)},
            "EXPECTED_HD_RECORDS": len(hd), "EXPECTED_EN_RECORDS": len(en), "EXPECTED_LO_RECORDS": len(lo),
            "EXPECTED_RADIO_SERVICE_COUNTS": {"IG": 2, "PW": 1, "YG": 1},
            "EXPECTED_LICENSE_STATUS_COUNTS": {"A": 3, "C": 1}, "EXPECTED_LICENSEE_ROWS": 4,
            "EXPECTED_ACTIVE_LOCATION_ROWS": 7, "EXPECTED_MISSING_COORDINATE_ROWS": 1,
            "EXPECTED_ACTIVE_LOCATIONS": 6, "EXPECTED_DUPLICATE_LOCATION_KEY_COUNT": 1,
            "EXPECTED_PUBLISHED_DUPLICATE_LOCATION_KEY_COUNT": 1,
            "EXPECTED_EMPTY_CALLSIGN_ROWS": 2, "EXPECTED_OUT_OF_RANGE_COORDINATE_ROWS": 1,
            "EXPECTED_LAT_DIRECTION_COUNTS": {"N": 4, "S": 1, "": 1},
            "EXPECTED_LON_DIRECTION_COUNTS": {"W": 4, "E": 1, "": 1},
            "EXPECTED_EXTRA_FIELD_ROWS": {"HD": 0, "EN": 0, "LO": 0},
            "EXPECTED_ACTIVE_ID_SHA256": _sha(_canonical(roster)),
            "EXPECTED_STATE_ROSTER_SHA256": _sha(_canonical(states)),
            "EXPECTED_BOUNDS": [-123.0, -14.0, 145.333333333, 93.0],
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
        self.assertIn(land_mobile_private_transmission_towers, hifld_ingest_assets)
        self.assertEqual(land_mobile_private_transmission_towers_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = land_mobile_private_transmission_towers_complete(
            {"feature_count": source.EXPECTED_ACTIVE_LOCATIONS, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = land_mobile_private_transmission_towers_complete(
            {"feature_count": source.EXPECTED_ACTIVE_LOCATIONS + 1, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_complete_native_population_and_published_contract(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        self.assertEqual([(row["UniqSysID"], row["LocNum"]) for row in published],
                         [(101, 1), (101, 1), (103, 1), (103, 2), (104, 1), (104, 2)])
        self.assertTrue(all(row["LicStatus"] == "A" for row in published))
        self.assertEqual(audit["source_feature_count"], 6)
        self.assertEqual(audit["source_license_status_counts"], {"A": 3, "C": 1})
        self.assertEqual(audit["source_radio_service_codes"], ["IG", "PW", "YG"])
        self.assertEqual(audit["source_state_count"], 4)
        self.assertEqual(audit["source_empty_callsign_rows"], 2)
        self.assertEqual(audit["source_missing_coordinate_rows"], 1)
        self.assertEqual(audit["source_out_of_range_coordinate_rows"], 1)
        self.assertEqual(audit["source_latitude_direction_counts"], {"N": 4, "S": 1, "": 1})
        self.assertEqual(audit["source_longitude_direction_counts"], {"W": 4, "E": 1, "": 1})
        self.assertEqual(audit["source_extra_field_rows"], {"HD": 0, "EN": 0, "LO": 0})
        self.assertEqual(audit["source_duplicate_location_key_count"], 1)
        self.assertEqual(audit["source_published_duplicate_location_key_count"], 1)
        self.assertEqual(audit["source_file_creation_date"], "Sun Nov  8 09:16:00 EST 2026")
        self.assertEqual(audit["source_counts_file"], {"HD.dat": 4, "EN.dat": 5, "LO.dat": 8})
        self.assertEqual(audit["source_bounds"], [-123.0, -14.0, 145.333333333, 93.0])
        self.assertIn("not a census of physical towers", audit["source_coverage_scope"])
        duplicate_rows = [row for row in published if (row["UniqSysID"], row["LocNum"]) == (101, 1)]
        self.assertEqual(len(duplicate_rows), 2)
        self.assertEqual(duplicate_rows[0]["Licensee"], "Licensee One")
        self.assertEqual(duplicate_rows[0]["TowReg"], "1023448")
        self.assertEqual(duplicate_rows[1]["TowReg"], "N/A")
        self.assertEqual(duplicate_rows[1]["SupStruc"], 7.0)
        self.assertEqual(duplicate_rows[1]["StrucType"], "LTOWER")
        self.assertEqual(published[0]["url"], source.LICENSE_URL_TEMPLATE.format(101))
        self.assertEqual(struct.unpack("<BIdd", published[0]["geometry"]),
                         (1, 1, -121.758333333, 38.5041666667))
        self.assertEqual(struct.unpack("<BIdd", published[1]["geometry"]),
                         struct.unpack("<BIdd", published[0]["geometry"]))
        out_of_range = published[2]
        self.assertEqual((out_of_range["LatDeg"], out_of_range["LatMin"], out_of_range["LatSec"]),
                         (93.0, 0.0, 0.0))
        self.assertEqual(out_of_range["latdec"], 93.0)
        self.assertEqual(out_of_range["londec"], -93.6143888889)
        self.assertIsNotNone(out_of_range["geometry"])
        self.assertIsNone(out_of_range["SupStruc"])
        self.assertIsNone(out_of_range["AllStruc"])
        southern = published[3]
        self.assertEqual(southern["latdec"], -14.0)
        self.assertEqual(southern["londec"], 145.333333333)
        self.assertEqual(southern["LatDir"], "S")
        self.assertEqual(southern["LonDir"], "E")
        empty_callsign = published[4]
        self.assertEqual(empty_callsign["Callsign"], "")
        self.assertEqual(empty_callsign["latdec"], 36.0)
        self.assertEqual(empty_callsign["londec"], -115.0)
        empty_directions = published[5]
        self.assertEqual(empty_directions["LatDir"], "")
        self.assertEqual(empty_directions["LonDir"], "")
        self.assertEqual(empty_directions["latdec"], 45.0)
        self.assertEqual(empty_directions["londec"], -123.0)

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
        self.assertEqual(rows[1]["TowReg"], "N/A")
        self.assertIsNone(rows[2]["SupStruc"])
        self.assertEqual(rows[2]["latdec"], 93.0)
        self.assertEqual(rows[3]["latdec"], -14.0)
        self.assertEqual(rows[3]["londec"], 145.333333333)
        self.assertEqual(rows[4]["londec"], -115.0)
        self.assertEqual(rows[5]["LatDir"], "")
        self.assertEqual(rows[5]["londec"], -123.0)
        self.assertEqual(rows[0]["LatDeg"], 38.0)
        self.assertEqual(rows[0]["LocNum"], 1)
        geometry_meta = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geometry_meta["primary_column"], "geometry")
        self.assertEqual(geometry_meta["columns"]["geometry"]["geometry_types"], ["Point"])
        self.assertEqual(geometry_meta["columns"]["geometry"]["crs"]["id"]["code"], 4326)
        self.assertEqual(audit["source_record_sha256"], _sha(_canonical(
            [{key: row[key] for key, _ in source.PUBLISHED_SCHEMA if key != "geometry"} for row in published])))

    def test_trailing_extra_field_quirk_rows_are_pinned(self):
        hd, en, lo = _documents()
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_EXTRA_FIELD_ROWS"] = {"HD": 1, "EN": 0, "LO": 0}
        quirky_hd = [hd[0] + "|", *hd[1:]]
        published, audit = self._validate(quirky_hd, en, lo, pins)
        self.assertEqual(audit["source_extra_field_rows"], {"HD": 1, "EN": 0, "LO": 0})
        self.assertEqual(audit["source_feature_count"], 6)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_EXTRA_FIELD_ROWS"] = {"HD": 0, "EN": 2, "LO": 0}
        quirky_en = [en[0] + "|", en[1] + "|", *en[2:]]
        published, audit = self._validate(hd, quirky_en, lo, pins)
        self.assertEqual(audit["source_extra_field_rows"], {"HD": 0, "EN": 2, "LO": 0})
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_EXTRA_FIELD_ROWS"] = {"HD": 0, "EN": 0, "LO": 1}
        quirky_lo = [lo[0] + "|", *lo[1:]]
        published, audit = self._validate(hd, en, quirky_lo, pins)
        self.assertEqual(audit["source_extra_field_rows"], {"HD": 0, "EN": 0, "LO": 1})
        uncounted = self._pins(hd, en, lo)
        with self.assertRaisesRegex(ValueError, "source_extra_field_row_count_drift"):
            self._validate(quirky_hd, en, lo, uncounted)
        with self.assertRaisesRegex(ValueError, "source_extra_field_row_count_drift:LO"):
            self._validate(hd, en, quirky_lo, uncounted)
        with self.assertRaisesRegex(ValueError, "source_record_field_count_drift:HD"):
            self._validate([hd[0] + "|X", *hd[1:]], en, lo, uncounted)
        with self.assertRaisesRegex(ValueError, "source_record_field_count_drift:LO"):
            self._validate(hd, en, [lo[0] + "|X", *lo[1:]], uncounted)

    def test_drift_guards_fail_closed(self):
        cases = [
            ("record field count", lambda hd, en, lo: (hd, en, [lo[0] + "|X", *lo[1:]]),
             "source_record_field_count_drift:LO"),
            ("record prefix", lambda hd, en, lo: ([hd[0], hd[1].replace("HD|", "XX|", 1), *hd[2:]], en, lo),
             "source_record_type_prefix_drift:HD"),
            ("record totals", lambda hd, en, lo: (hd[:3], en, lo), "source_record_population_drift"),
            ("radio service", lambda hd, en, lo: ([hd[0], hd[1], hd[2].replace("|YG|", "|CX|"), hd[3]], en, lo),
             "source_radio_service_roster_drift"),
            ("status distribution", lambda hd, en, lo: ([hd[0].replace("|A|", "|E|"), *hd[1:]], en, lo),
             "source_license_status_distribution_drift"),
            ("licensee join", lambda hd, en, lo: (hd, [_en(101, "KNKA101", "CL", "Contact Two"), *en[1:]], lo),
             "source_licensee_join_drift"),
            ("duplicate license identity", lambda hd, en, lo: ([hd[0], hd[0], *hd[2:]], en, lo),
             "source_duplicate_license_identity"),
            ("duplicate licensee row", lambda hd, en, lo: (hd, [en[0], en[0], *en[2:]], lo),
             "source_duplicate_licensee_row"),
            ("callsign join", lambda hd, en, lo: (hd, en, [_lo(101, "KNKX999", 1), *lo[1:]]),
             "source_callsign_join_drift"),
            ("missing location number", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "KNKA101", ""), *lo[2:]]),
             "source_location_number_invalid"),
            ("state roster", lambda hd, en, lo: (hd, en, [lo[0].replace("|CA|", "|ZZ|"), *lo[1:]]),
             "source_state_roster_drift"),
            ("latitude direction distribution", lambda hd, en, lo: (
                hd, en, [_lo(104, "", 2, lat_deg="45", lat_min="0", lat_sec=".0", lat_dir="X",
                             lon_deg="123", lon_min="0", lon_sec=".0", lon_dir="", state="NV")
                         if index == 7 else row for index, row in enumerate(lo)]),
             "source_latitude_direction_distribution_drift"),
            ("longitude direction distribution", lambda hd, en, lo: (
                hd, en, [_lo(104, "", 2, lat_deg="45", lat_min="0", lat_sec=".0", lat_dir="",
                             lon_deg="123", lon_min="0", lon_sec=".0", lon_dir="X", state="NV")
                         if index == 7 else row for index, row in enumerate(lo)]),
             "source_longitude_direction_distribution_drift"),
            ("counts file", lambda hd, en, lo: (hd, en, lo), "source_counts_file_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                pins = self._pins(*_documents())
                if label == "counts file":
                    pins["EXPECTED_COUNTS_FILE"] = {"HD.dat": 4, "EN.dat": 5, "LO.dat": 9}
                hd, en, lo = mutate(*_documents())
                with self.assertRaisesRegex(ValueError, expected):
                    self._validate(hd, en, lo, pins)

    def test_missing_coordinate_and_population_pins_fail_closed(self):
        hd, en, lo = _documents()
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_MISSING_COORDINATE_ROWS"] = 2
        with self.assertRaisesRegex(ValueError, "source_missing_coordinate_row_count_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_ACTIVE_LOCATION_ROWS"] = 6
        with self.assertRaisesRegex(ValueError, "source_active_location_row_count_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_ACTIVE_LOCATIONS"] = 5
        with self.assertRaisesRegex(ValueError, "source_active_population_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_DUPLICATE_LOCATION_KEY_COUNT"] = 0
        with self.assertRaisesRegex(ValueError, "source_duplicate_location_key_count_drift"):
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
        pins["EXPECTED_EMPTY_CALLSIGN_ROWS"] = 3
        with self.assertRaisesRegex(ValueError, "source_empty_callsign_row_count_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_OUT_OF_RANGE_COORDINATE_ROWS"] = 0
        with self.assertRaisesRegex(ValueError, "source_out_of_range_coordinate_row_count_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_BOUNDS"] = [0.0, 0.0, 0.0, 0.0]
        with self.assertRaisesRegex(ValueError, "source_bounds_drift"):
            self._validate(hd, en, lo, pins)

    def test_zip_container_and_capture_receipts_fail_closed(self):
        hd, en, lo = _documents()
        body = _members(hd, en, lo)
        with self.assertRaisesRegex(ValueError, "source_zip_member_roster_drift"):
            source._validate(_capture(_members(hd, en, lo, extra=["extra.dat"])))
        with self.assertRaisesRegex(ValueError, "source_zip_unreadable"):
            source._validate(_capture(b"not a zip archive"))
        bad_status = {"l_LMpriv": (body, {"method": "GET", "url": source.L_LMPRIV_URL,
                                          "final_url": source.L_LMPRIV_URL, "status": 500,
                                          "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "source_capture_status_length_hash_or_completeness_drift"):
            source._validate(bad_status)
        incomplete = {"l_LMpriv": (body, {"method": "GET", "url": source.L_LMPRIV_URL,
                                          "final_url": source.L_LMPRIV_URL, "status": 200,
                                          "complete_body": False, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "source_capture_status_length_hash_or_completeness_drift"):
            source._validate(incomplete)
        redirect = {"l_LMpriv": (body, {"method": "GET", "url": source.L_LMPRIV_URL,
                                        "final_url": "https://elsewhere.example/l_LMpriv.zip", "status": 200,
                                        "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
            source._validate(redirect)
        with self.assertRaisesRegex(ValueError, "source_capture_name_roster_drift"):
            source._validate({"l_cell": (body, _capture(body)["l_LMpriv"][1])})

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_land_mobile_private_transmission_towers(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

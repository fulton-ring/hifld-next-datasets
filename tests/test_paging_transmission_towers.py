import io
import json
import struct
import tempfile
import unittest
import zipfile
import hashlib
from pathlib import Path
from unittest import mock

from dagster import DefaultScheduleStatus
import pyarrow.parquet as pq
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.paging_transmission_towers import (
    paging_transmission_towers, paging_transmission_towers_complete,
    paging_transmission_towers_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import paging_transmission_towers as source
from dagster_hifld.sources.epa_frs_common import _canonical


def _sha(value):
    return hashlib.sha256(value).hexdigest()


def _hd(identifier, callsign, status, service="CD"):
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
    hd = [_hd(101, "KNKA101", "A", service="CD"), _hd(102, "KNKC102", "C", service="CD"),
          _hd(103, "KNKA103", "A", service="CG"), _hd(104, "KNKA104", "E", service="CB"),
          _hd(105, "KNKA105", "A", service="CO")]
    en = [_en(101, "KNKA101", "L", "Licensee One"), _en(102, "KNKC102", "L", "Licensee Two"),
          _en(103, "KNKA103", "L", "Licensee Three"), _en(104, "KNKA104", "L", "Licensee Four"),
          _en(105, "KNKA105", "L", "Licensee Five"), _en(101, "KNKA101", "CL", "Contact One")]
    lo = [
        _lo(101, "KNKA101", 1, state="CA"),
        _lo(101, "KNKA101", 2, state="TX", towreg="", structype="", supstruc="", allstruc="", qzone="01/02/2003"),
        _lo(102, "KNKC102", 5, state="WA"),
        _lo(103, "KNKA103", 1, state="LA"),
        _lo(105, "KNKA105", 1, state="GU"),
    ]
    return hd, en, lo


def _members(hd, en, lo, counts_override=None, extra=()):
    content = {}
    for name in source.ZIP_MEMBER_ROSTER:
        if name == "HD.dat":
            content[name] = ("\r\n".join(hd) + "\r\n").encode()
        elif name == "EN.dat":
            content[name] = ("\r\n".join(en) + "\r\n").encode()
        elif name == "LO.dat":
            content[name] = ("\r\n".join(lo) + "\r\n").encode()
        elif name == "counts":
            pass
        else:
            content[name] = b""
    rows = {}
    total = 0
    for name in source.ZIP_MEMBER_ROSTER:
        if name == "counts":
            continue
        stripped = content[name].rstrip(b"\r\n")
        count = len(stripped.split(b"\n")) if stripped else 0
        rows[name] = count
        total += count
    lines = ["File Creation Date: Sun Nov  8 10:03:43 EST 2026"]
    for name in source.ZIP_MEMBER_ROSTER:
        if name != "counts":
            lines.append("%6d /home/pubacc/scripts/licweekzipdata/%s" % (rows[name], name))
    lines.append("%6d total" % total)
    if counts_override:
        lines = counts_override(lines)
    content["counts"] = ("\r\n".join(lines) + "\r\n").encode()
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for name in [*source.ZIP_MEMBER_ROSTER, *extra]:
            archive.writestr(name, content.get(name, b""))
    return stream.getvalue()


def _capture(body):
    return {"l_paging": (body, {"method": "GET", "url": source.L_PAGING_URL, "final_url": source.L_PAGING_URL,
                                "status": 200, "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}


class PagingTransmissionTowersTests(unittest.TestCase):
    def _pins(self, hd, en, lo):
        roster = sorted([[101, 1], [101, 2], [103, 1], [105, 1]])
        states = sorted({"CA", "TX", "LA", "GU"})
        return {
            "EXPECTED_HD_RECORDS": len(hd), "EXPECTED_EN_RECORDS": len(en), "EXPECTED_LO_RECORDS": len(lo),
            "EXPECTED_LICENSE_STATUS_COUNTS": {"A": 3, "C": 1, "E": 1}, "EXPECTED_LICENSEE_ROWS": 5,
            "EXPECTED_ACTIVE_LOCATIONS": 4,
            "EXPECTED_ACTIVE_ID_SHA256": _sha(_canonical(roster)),
            "EXPECTED_STATE_ROSTER_SHA256": _sha(_canonical(states)),
            "EXPECTED_NULL_GEOMETRY_COUNT": 0,
            "EXPECTED_DUPLICATE_LOCATION_KEYS": 0,
            "EXPECTED_DUPLICATE_KEY_SHA256": _sha(_canonical([])),
            "EXPECTED_ACTIVE_SERVICE_ROW_COUNTS": {"CD": 2, "CG": 1, "CO": 1},
            "EXPECTED_INVALID_DIRECTION_COUNT": 0,
            "EXPECTED_OUT_OF_NOMINAL_COMPONENTS": 0,
        }

    def _validate(self, hd, en, lo, pins=None, counts_override=None):
        patches = [mock.patch.object(source, name, value)
                   for name, value in (pins or self._pins(hd, en, lo)).items()]
        for patch in patches:
            patch.start()
        try:
            return source._validate(_capture(_members(hd, en, lo, counts_override=counts_override)))
        finally:
            for patch in patches:
                patch.stop()

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(paging_transmission_towers, hifld_ingest_assets)
        self.assertEqual(paging_transmission_towers_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = paging_transmission_towers_complete({"feature_count": source.EXPECTED_ACTIVE_LOCATIONS,
                                                     "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = paging_transmission_towers_complete({"feature_count": source.EXPECTED_ACTIVE_LOCATIONS + 1,
                                                      "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_pinned_edition_contract(self):
        self.assertEqual(source.L_PAGING_URL, "https://data.fcc.gov/download/pub/uls/complete/l_paging.zip")
        self.assertEqual(source.RECORD_FIELD_COUNTS, {"HD": 59, "EN": 30, "LO": 51})
        self.assertEqual(source.EXPECTED_HD_RECORDS, 9604)
        self.assertEqual(source.EXPECTED_EN_RECORDS, 17235)
        self.assertEqual(source.EXPECTED_LO_RECORDS, 31951)
        self.assertEqual(source.EXPECTED_HD_RADIO_SERVICES, ["CA", "CB", "CD", "CG", "CO", "CR"])
        self.assertEqual(source.EXPECTED_LICENSE_STATUS_COUNTS, {"A": 1137, "C": 4799, "E": 2986, "T": 682})
        self.assertEqual(source.EXPECTED_LICENSEE_ROWS, 9604)
        self.assertEqual(source.EXPECTED_ACTIVE_LOCATIONS, 6389)
        self.assertEqual(source.EXPECTED_NULL_GEOMETRY_COUNT, 23)
        self.assertEqual(source.EXPECTED_DUPLICATE_LOCATION_KEYS, 0)
        self.assertEqual(source.EXPECTED_ACTIVE_SERVICE_ROW_COUNTS,
                         {"CB": 14, "CD": 6205, "CG": 57, "CO": 4, "CR": 109})
        self.assertEqual(source.EXPECTED_OUT_OF_NOMINAL_COMPONENTS, 1)
        self.assertIn("not a census of physical paging towers", source.COVERAGE_SCOPE)

    def test_counts_file_completeness_gate_passes_and_fails_closed(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        self.assertTrue(audit["source_counts_file_verified"])
        with self.assertRaisesRegex(ValueError, "source_counts_file_row_count_drift:LO.dat"):
            self._validate(hd, en, lo, counts_override=lambda lines: [
                "     6 /home/pubacc/scripts/licweekzipdata/LO.dat" if line.endswith("/LO.dat") else line
                for line in lines])
        with self.assertRaisesRegex(ValueError, "source_counts_file_member_missing:HD.dat"):
            self._validate(hd, en, lo, counts_override=lambda lines: [
                line for line in lines if not line.endswith("/HD.dat")])
        with self.assertRaisesRegex(ValueError, "source_counts_file_unknown_member:extra.dat"):
            self._validate(hd, en, lo, counts_override=lambda lines: [*lines, "     1 /home/pubacc/scripts/licweekzipdata/extra.dat"])
        with self.assertRaisesRegex(ValueError, "source_counts_file_duplicate_entry:LO.dat"):
            self._validate(hd, en, lo, counts_override=lambda lines: [
                *lines, "     5 /home/pubacc/scripts/licweekzipdata/LO.dat"])

    def test_complete_native_population_and_published_contract(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        self.assertEqual([(row["UniqSysID"], row["LocNum"]) for row in published],
                         [(101, 1), (101, 2), (103, 1), (105, 1)])
        self.assertTrue(all(row["LicStatus"] == "A" for row in published))
        self.assertEqual(audit["source_feature_count"], 4)
        self.assertEqual(audit["source_license_status_counts"], {"A": 3, "C": 1, "E": 1})
        self.assertEqual(audit["source_radio_service_codes"], ["CA", "CB", "CD", "CG", "CO", "CR"])
        self.assertEqual(audit["source_service_row_counts"], {"CD": 2, "CG": 1, "CO": 1})
        self.assertEqual(audit["source_state_count"], 4)
        self.assertEqual(audit["source_null_geometry_count"], 0)
        self.assertEqual(audit["source_duplicate_location_key_count"], 0)
        self.assertEqual(audit["source_invalid_direction_count"], 0)
        self.assertEqual(audit["source_out_of_nominal_component_count"], 0)
        self.assertEqual(audit["source_file_creation_date"], "Sun Nov  8 10:03:43 EST 2026")
        self.assertEqual(audit["source_bounds"], [-121.758333333, 38.5041666667, -121.758333333, 38.5041666667])
        self.assertIn("not a census of physical paging towers", audit["source_coverage_scope"])
        first = published[0]
        self.assertEqual(first["Licensee"], "Licensee One")
        self.assertEqual(first["url"], source.LICENSE_URL_TEMPLATE.format(101))
        self.assertEqual(struct.unpack("<BIdd", first["geometry"]), (1, 1, -121.758333333, 38.5041666667))
        empty = published[1]
        self.assertEqual(empty["TowReg"], "")
        self.assertEqual(empty["StrucType"], "")
        self.assertEqual(empty["QZone"], "01/02/2003")
        self.assertIsNone(empty["SupStruc"])
        self.assertIsNone(empty["AllStruc"])

    def test_parquet_candidate_preserves_columns_types_and_verbatim_values(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        body = source._parquet_bytes(published)
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.schema.names, [name for name, _ in source.PUBLISHED_SCHEMA])
        self.assertEqual(table.num_rows, 4)
        rows = table.to_pylist()
        self.assertEqual(rows[0]["UniqSysID"], 101)
        self.assertEqual(rows[0]["TowReg"], "1023448")
        self.assertEqual(rows[1]["TowReg"], "")
        self.assertIsNone(rows[1]["SupStruc"])
        geometry_meta = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geometry_meta["primary_column"], "geometry")
        self.assertEqual(geometry_meta["columns"]["geometry"]["geometry_types"], ["Point"])
        self.assertEqual(geometry_meta["columns"]["geometry"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geometry_meta["columns"]["geometry"]["crs"]["name"], "NAD83")
        self.assertEqual(audit["source_record_sha256"], _sha(_canonical(
            [{key: row[key] for key, _ in source.PUBLISHED_SCHEMA if key != "geometry"} for row in published])))

    def test_null_geometry_and_source_degenerate_dms_are_preserved(self):
        hd, en, lo = _documents()
        lo = [
            lo[0], _lo(101, "KNKA101", 2, state="TX", lat_deg="", lat_min="", lat_sec="", lat_dir="", lon_deg="", lon_min="", lon_sec="", lon_dir=""),
            lo[2], lo[3], lo[4],
            # Source-observed degenerate rows: latitude seconds 70.0 (USI 4104)
            # and empty latitude minutes (USI 4372205).
            _lo(103, "KNKA103", 2, state="GA", lat_deg="37", lat_min="22", lat_sec="70.0"),
            _lo(105, "KNKA105", 2, state="MO", lat_deg="39", lat_min="", lat_sec="29.0", lon_deg="94", lon_min="33", lon_sec="29.8"),
        ]
        pins = self._pins(hd, en, lo) | {"EXPECTED_NULL_GEOMETRY_COUNT": 1,
                                         "EXPECTED_ACTIVE_LOCATIONS": 6,
                                         "EXPECTED_OUT_OF_NOMINAL_COMPONENTS": 1,
                                         "EXPECTED_ACTIVE_SERVICE_ROW_COUNTS": {"CD": 2, "CG": 2, "CO": 2},
                                         "EXPECTED_ACTIVE_ID_SHA256": "099e8462bc8f55ade3a97c9408f78288f491e05a193546343ebb0e89475c2866",
                                         "EXPECTED_STATE_ROSTER_SHA256": "2bb397f8e2c94305c353e125c5f8cc172920c199716bb2eb6ed7d7576d9e901d"}
        published, audit = self._validate(hd, en, lo, pins)
        null_row = published[1]
        self.assertIsNone(null_row["geometry"])
        self.assertIsNone(null_row["latdec"])
        self.assertEqual(null_row["LatDir"], "")
        degenerate = published[3]
        self.assertEqual((degenerate["LatDeg"], degenerate["LatMin"], degenerate["LatSec"]), (37, 22, 70.0))
        self.assertEqual(degenerate["latdec"], 37.3861111111)
        self.assertIsNotNone(degenerate["geometry"])
        missing_minutes = published[5]
        self.assertIsNone(missing_minutes["LatMin"])
        self.assertEqual(missing_minutes["latdec"], 39.0080555556)
        self.assertIsNotNone(missing_minutes["geometry"])
        self.assertEqual(audit["source_null_geometry_count"], 1)
        self.assertEqual(audit["source_out_of_nominal_component_count"], 1)

    def test_drift_guards_fail_closed(self):
        cases = [
            ("record field count", lambda hd, en, lo: (hd, en, [lo[0] + "|", *lo[1:]]),
             "source_record_field_count_drift:LO"),
            ("record prefix", lambda hd, en, lo: ([hd[0], hd[1].replace("HD|", "XX|", 1), *hd[2:]], en, lo),
             "source_record_type_prefix_drift:HD"),
            ("record totals", lambda hd, en, lo: (hd[:4], en, lo), "source_record_population_drift"),
            ("radio service", lambda hd, en, lo: ([hd[0].replace("|CD|", "|CX|"), *hd[1:]], en, lo),
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
            ("duplicate key roster", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "KNKA101", 1), lo[2], lo[3], lo[4]]),
             "source_duplicate_location_key_count_drift"),
            ("coordinate range", lambda hd, en, lo: (hd, en, [_lo(101, "KNKA101", 1, lon_deg="181"), *lo[1:]]),
             "source_coordinate_out_of_range"),
            ("null geometry", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "KNKA101", 2, state="TX", lat_deg="", lat_min="", lat_sec="", lat_dir="", lon_deg="", lon_min="", lon_sec="", lon_dir=""), *lo[2:]]),
             "source_null_geometry_count_drift"),
            ("partial dms", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "KNKA101", 2, state="TX", lon_deg="", lon_min="", lon_sec="", lon_dir=""), *lo[2:]]),
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
        pins["EXPECTED_ACTIVE_LOCATIONS"] = 5
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
        pins["EXPECTED_DUPLICATE_LOCATION_KEYS"] = 1
        with self.assertRaisesRegex(ValueError, "source_duplicate_location_key_count_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_ACTIVE_SERVICE_ROW_COUNTS"] = {"CD": 1, "CG": 1, "CO": 1}
        with self.assertRaisesRegex(ValueError, "source_service_row_distribution_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_INVALID_DIRECTION_COUNT"] = 1
        with self.assertRaisesRegex(ValueError, "source_invalid_direction_count_drift"):
            self._validate(hd, en, lo, pins)
        pins = self._pins(hd, en, lo)
        pins["EXPECTED_OUT_OF_NOMINAL_COMPONENTS"] = 1
        with self.assertRaisesRegex(ValueError, "source_out_of_nominal_component_count_drift"):
            self._validate(hd, en, lo, pins)

    def test_zip_container_and_capture_receipts_fail_closed(self):
        hd, en, lo = _documents()
        body = _members(hd, en, lo)
        with self.assertRaisesRegex(ValueError, "source_zip_member_roster_drift"):
            source._validate(_capture(_members(hd, en, lo, extra=["extra.dat"])))
        with self.assertRaisesRegex(ValueError, "source_zip_unreadable"):
            source._validate(_capture(b"not a zip archive"))
        bad_status = {"l_paging": (body, {"method": "GET", "url": source.L_PAGING_URL,
                                          "final_url": source.L_PAGING_URL, "status": 500,
                                          "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "source_capture_status_length_hash_or_completeness_drift"):
            source._validate(bad_status)
        incomplete = {"l_paging": (body, {"method": "GET", "url": source.L_PAGING_URL,
                                          "final_url": source.L_PAGING_URL, "status": 200,
                                          "complete_body": False, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "source_capture_status_length_hash_or_completeness_drift"):
            source._validate(incomplete)
        redirect = {"l_paging": (body, {"method": "GET", "url": source.L_PAGING_URL,
                                        "final_url": "https://elsewhere.example/l_paging.zip", "status": 200,
                                        "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
            source._validate(redirect)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_paging_transmission_towers(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

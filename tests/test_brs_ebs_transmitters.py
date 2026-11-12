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
from dagster_hifld.assets.brs_ebs_transmitters import (
    brs_ebs_transmitters, brs_ebs_transmitters_complete, brs_ebs_transmitters_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import brs_ebs_transmitters as source
from dagster_hifld.sources.epa_frs_common import _canonical


def _sha(value):
    import hashlib
    return hashlib.sha256(value).hexdigest()


def _hd(identifier, callsign, status, service="ED"):
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
    hd = [_hd(101, "WND562", "A"), _hd(102, "WNDK102", "C"),
          _hd(103, "WNDK103", "A", service="BR"), _hd(104, "", "E")]
    en = [_en(101, "WND562", "L", "Licensee One"), _en(102, "WNDK102", "L", "Licensee Two"),
          _en(103, "WNDK103", "L", "Licensee Three"), _en(104, "", "L", "Licensee Four"),
          _en(101, "WND562", "CL", "Contact One")]
    lo = [
        _lo(101, "WND562", 1, state="CA"),
        _lo(101, "WND562", 2, state="TX", towreg="", structype="", supstruc="", allstruc="", qzone="01/02/2003"),
        _lo(102, "WNDK102", 5, state="WA"),
        _lo(103, "WNDK103", 1, lat_sec="", lat_min="4", lat_deg="30", lon_deg="87", lon_min="57", state="LA"),
        _lo(103, "WNDK103", 2, lat_deg="14", lat_min="0", lat_sec="0", lat_dir="S", lon_deg="145", lon_min="20", lon_sec="0", lon_dir="E", state="GU"),
        _lo(104, "", 1, state="SC"),
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
            content[name] = b"File Creation Date: Sun Nov  8 10:07:38 EST 2026\r\n    6 /home/pubacc/scripts/licweekzipdata/LO.dat\r\n"
        else:
            content[name] = b""
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for name in [*source.ZIP_MEMBER_ROSTER, *extra]:
            archive.writestr(name, content.get(name, b""))
    return stream.getvalue()


def _capture(body):
    return {"l_mdsitfs": (body, {"method": "GET", "url": source.L_MDSITFS_URL, "final_url": source.L_MDSITFS_URL,
                                 "status": 200, "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}


class BrsEbsTransmittersTests(unittest.TestCase):
    def _pins(self, hd, en, lo):
        roster = sorted([[101, 1], [101, 2], [103, 1], [103, 2]])
        states = sorted({"CA", "TX", "LA", "GU"})
        return {
            "EXPECTED_HD_RECORDS": len(hd), "EXPECTED_EN_RECORDS": len(en), "EXPECTED_LO_RECORDS": len(lo),
            "EXPECTED_HD_RADIO_SERVICES": ["BR", "ED"],
            "EXPECTED_LICENSE_STATUS_COUNTS": {"A": 2, "C": 1, "E": 1}, "EXPECTED_LICENSEE_ROWS": 4,
            "EXPECTED_ACTIVE_LOCATIONS": 4, "EXPECTED_NULL_GEOMETRY_COUNT": 0,
            "EXPECTED_ACTIVE_ID_SHA256": _sha(_canonical(roster)),
            "EXPECTED_STATE_ROSTER_SHA256": _sha(_canonical(states)),
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

    def test_real_file_pins_are_pinned(self):
        self.assertEqual(source.DATASET_SLUG, "broadband-radio-service-transmitters")
        self.assertEqual(source.EXPECTED_HD_RECORDS, 18982)
        self.assertEqual(source.EXPECTED_EN_RECORDS, 44649)
        self.assertEqual(source.EXPECTED_LO_RECORDS, 10044)
        self.assertEqual(source.EXPECTED_HD_RADIO_SERVICES, ["BR", "ED"])
        self.assertEqual(source.EXPECTED_LICENSE_STATUS_COUNTS, {"A": 13732, "C": 3568, "E": 1296, "T": 386})
        self.assertEqual(source.EXPECTED_LICENSEE_ROWS, 18982)
        self.assertEqual(source.EXPECTED_ACTIVE_LOCATIONS, 4669)
        self.assertEqual(source.EXPECTED_NULL_GEOMETRY_COUNT, 3)
        self.assertIn("l_mdsitfs.zip", source.L_MDSITFS_URL)

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(brs_ebs_transmitters, hifld_ingest_assets)
        self.assertEqual(brs_ebs_transmitters_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = brs_ebs_transmitters_complete({"feature_count": source.EXPECTED_ACTIVE_LOCATIONS,
                                              "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = brs_ebs_transmitters_complete({"feature_count": source.EXPECTED_ACTIVE_LOCATIONS + 1,
                                                "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_complete_native_population_and_published_contract(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        self.assertEqual([(row["UniqSysID"], row["LocNum"]) for row in published],
                         [(101, 1), (101, 2), (103, 1), (103, 2)])
        self.assertTrue(all(row["LicStatus"] == "A" for row in published))
        self.assertEqual(audit["source_feature_count"], 4)
        self.assertEqual(audit["source_license_status_counts"], {"A": 2, "C": 1, "E": 1})
        self.assertEqual(audit["source_radio_service_codes"], ["BR", "ED"])
        self.assertEqual(audit["source_state_count"], 4)
        self.assertEqual(audit["source_null_geometry_count"], 0)
        self.assertEqual(audit["source_incomplete_seconds_count"], 1)
        self.assertEqual(audit["source_file_creation_date"], "Sun Nov  8 10:07:38 EST 2026")
        self.assertEqual(audit["source_bounds"], [-121.758333333, -14.0, 145.333333333, 38.5041666667])
        self.assertIn("not a census of physical transmitters", audit["source_coverage_scope"])
        first = published[0]
        self.assertEqual(first["Licensee"], "Licensee One")
        self.assertEqual(first["url"], source.LICENSE_URL_TEMPLATE.format(101))
        self.assertEqual(struct.unpack("<BIdd", first["geometry"]), (1, 1, -121.758333333, 38.5041666667))
        incomplete = published[2]
        self.assertIsNone(incomplete["LatSec"])
        self.assertEqual(incomplete["latdec"], 30.0666666667)
        self.assertIsNotNone(incomplete["geometry"])
        southern = published[3]
        self.assertEqual(southern["latdec"], -14.0)
        self.assertEqual(southern["londec"], 145.333333333)
        self.assertEqual(struct.unpack("<BIdd", southern["geometry"]), (1, 1, 145.333333333, -14.0))
        empty = published[1]
        self.assertEqual(empty["TowReg"], "")
        self.assertEqual(empty["StrucType"], "")
        self.assertEqual(empty["QZone"], "01/02/2003")
        self.assertIsNone(empty["SupStruc"])
        self.assertIsNone(empty["AllStruc"])

    def test_verbatim_callsign_join_tolerates_blank_inactive_license(self):
        hd, en, lo = _documents()
        published, _audit = self._validate(hd, en, lo)
        self.assertNotIn(104, [row["UniqSysID"] for row in published])
        with self.assertRaisesRegex(ValueError, "source_callsign_join_drift"):
            self._validate(hd, en, [*lo, _lo(104, "WNDX888", 2, state="SC")])
        with self.assertRaisesRegex(ValueError, "source_active_callsign_missing"):
            self._validate(
                [hd[0], hd[1], hd[2], _hd(104, "", "A")], en, lo,
                {**self._pins(hd, en, lo), "EXPECTED_LICENSE_STATUS_COUNTS": {"A": 3, "C": 1}})

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
        self.assertIsNone(rows[2]["LatSec"])
        self.assertEqual(rows[2]["latdec"], 30.0666666667)
        self.assertEqual(rows[3]["londec"], 145.333333333)
        geometry_meta = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geometry_meta["primary_column"], "geometry")
        self.assertEqual(geometry_meta["columns"]["geometry"]["geometry_types"], ["Point"])
        self.assertEqual(geometry_meta["columns"]["geometry"]["crs"]["id"]["code"], 4269)
        self.assertEqual(audit["source_record_sha256"], _sha(_canonical(
            [{key: row[key] for key, _ in source.PUBLISHED_SCHEMA if key != "geometry"} for row in published])))

    def test_geometry_declares_native_nad83_datum_without_transformation(self):
        hd, en, lo = _documents()
        published, _audit = self._validate(hd, en, lo)
        # WKB points carry the DMS-derived decimal values unchanged; the
        # GeoParquet metadata must label them with the FCC-required native
        # ULS datum (NAD83, EPSG:4269), not WGS84.
        for row in published:
            if row["geometry"] is None:
                continue
            _byte_order, _type, londec, latdec = struct.unpack("<BIdd", row["geometry"])
            self.assertEqual((londec, latdec), (row["londec"], row["latdec"]))
        body = source._parquet_bytes(published)
        crs = json.loads(pq.read_table(io.BytesIO(body)).schema.metadata[b"geo"])["columns"]["geometry"]["crs"]
        self.assertEqual(crs["id"], {"authority": "EPSG", "code": 4269})
        self.assertEqual(crs["name"], "NAD83")
        self.assertEqual(crs["datum"]["name"], "North American Datum 1983")
        self.assertEqual(crs["datum"]["ellipsoid"]["name"], "GRS 1980")
        self.assertIn("EPSG:4269", source.GEOMETRY_PROVENANCE)
        self.assertIn("no datum transformation", source.GEOMETRY_PROVENANCE)
        self.assertNotIn("EPSG:4326", source.GEOMETRY_PROVENANCE)

    def test_drift_guards_fail_closed(self):
        cases = [
            ("record field count", lambda hd, en, lo: (hd, en, [lo[0] + "|", *lo[1:]]),
             "source_record_field_count_drift:LO"),
            ("record prefix", lambda hd, en, lo: ([hd[0], hd[1].replace("HD|", "XX|", 1), *hd[2:]], en, lo),
             "source_record_type_prefix_drift:HD"),
            ("record totals", lambda hd, en, lo: (hd[:3], en, lo), "source_record_population_drift"),
            ("radio service", lambda hd, en, lo: ([hd[0].replace("|ED|", "|CX|"), *hd[1:]], en, lo),
             "source_radio_service_roster_drift"),
            ("status distribution", lambda hd, en, lo: ([hd[0].replace("|A|", "|E|"), *hd[1:]], en, lo),
             "source_license_status_distribution_drift"),
            ("licensee join", lambda hd, en, lo: (hd, [_en(101, "WND562", "CL", "Contact Two"), *en[1:]], lo),
             "source_licensee_join_drift"),
            ("duplicate keys", lambda hd, en, lo: (hd, en, [lo[0], lo[1], lo[2], lo[3], lo[4], _lo(103, "WNDK103", 1)]),
             "source_location_key_duplication_drift"),
            ("callsign join", lambda hd, en, lo: (hd, en, [_lo(101, "WNDX999", 1), *lo[1:]]),
             "source_callsign_join_drift"),
            ("missing location number", lambda hd, en, lo: (hd, en, [lo[0], lo[1], lo[2], lo[3], lo[4], _lo(103, "WNDK103", "")]),
             "source_location_number_invalid"),
            ("state roster", lambda hd, en, lo: (hd, en, [lo[0].replace("|CA|", "|ZZ|"), *lo[1:]]),
             "source_state_roster_drift"),
            ("latitude direction", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "WND562", 2, lat_dir="X"), *lo[2:]]),
             "source_latitude_direction_drift"),
            ("latitude coordinate range", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "WND562", 2, lat_deg="999"), *lo[2:]]),
             "source_coordinate_out_of_range"),
            ("coordinate range", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "WND562", 2, lon_deg="180", lon_min="59", lon_sec="59.9", lon_dir="E"), *lo[2:]]),
             "source_coordinate_out_of_range"),
            ("null geometry", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "WND562", 2, lat_deg="", lon_deg=""), *lo[2:]]),
             "source_null_geometry_count_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                pins = self._pins(*_documents())
                hd, en, lo = mutate(*_documents())
                with self.assertRaisesRegex(ValueError, expected):
                    self._validate(hd, en, lo, pins)

    def test_out_of_nominal_dms_components_are_preserved_and_converted(self):
        hd, en, lo = _documents()
        lo = [lo[0], _lo(101, "WND562", 2, lat_deg="36", lat_min="73", lat_sec="11.3", state="TX"), *lo[2:]]
        published, audit = self._validate(hd, en, lo)
        preserved = published[1]
        self.assertEqual((preserved["LatDeg"], preserved["LatMin"], preserved["LatSec"]), (36, 73, 11.3))
        self.assertEqual(preserved["latdec"], 37.2198055556)
        self.assertIsNotNone(preserved["geometry"])

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

    def test_zip_container_and_capture_receipts_fail_closed(self):
        hd, en, lo = _documents()
        body = _members(hd, en, lo)
        with self.assertRaisesRegex(ValueError, "source_zip_member_roster_drift"):
            source._validate(_capture(_members(hd, en, lo, extra=["extra.dat"])))
        with self.assertRaisesRegex(ValueError, "source_zip_unreadable"):
            source._validate(_capture(b"not a zip archive"))
        bad_status = {"l_mdsitfs": (body, {"method": "GET", "url": source.L_MDSITFS_URL,
                                          "final_url": source.L_MDSITFS_URL, "status": 500,
                                          "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "source_capture_status_length_hash_or_completeness_drift"):
            source._validate(bad_status)
        incomplete = {"l_mdsitfs": (body, {"method": "GET", "url": source.L_MDSITFS_URL,
                                          "final_url": source.L_MDSITFS_URL, "status": 200,
                                          "complete_body": False, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "source_capture_status_length_hash_or_completeness_drift"):
            source._validate(incomplete)
        redirect = {"l_mdsitfs": (body, {"method": "GET", "url": source.L_MDSITFS_URL,
                                        "final_url": "https://elsewhere.example/l_mdsitfs.zip", "status": 200,
                                        "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
            source._validate(redirect)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_brs_ebs_transmitters(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

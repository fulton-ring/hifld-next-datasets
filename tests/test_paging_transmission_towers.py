import io
import json
import struct
import tempfile
import unittest
import zipfile
import hashlib
from pathlib import Path

from dagster import AssetCheckSeverity, DefaultScheduleStatus
import pyarrow.parquet as pq
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.paging_transmission_towers import (
    paging_transmission_towers, paging_transmission_towers_complete,
    paging_transmission_towers_draft_schedule, paging_transmission_towers_drift,
)
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import paging_transmission_towers as source
from dagster_hifld.sources.capture_common import _canonical


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


def _members(hd, en, lo, extra=(), counts=None, counts_override=None,
             creation_date="Sun Nov  8 10:03:43 EST 2026"):
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
    if counts is not None:
        rows.update({table + ".dat": value for table, value in counts.items()})
        total = sum(rows.values())
    lines = ["File Creation Date: " + creation_date]
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
    def _validate(self, hd, en, lo, counts=None, counts_override=None, creation_date=None):
        kwargs = {}
        if counts is not None:
            kwargs["counts"] = counts
        if counts_override is not None:
            kwargs["counts_override"] = counts_override
        if creation_date is not None:
            kwargs["creation_date"] = creation_date
        return source._validate(_capture(_members(hd, en, lo, **kwargs)))

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(paging_transmission_towers, hifld_ingest_assets)
        self.assertEqual(paging_transmission_towers_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = paging_transmission_towers_complete({"feature_count": 4, "source_reported_count": 4,
                                                      "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = paging_transmission_towers_complete({"feature_count": 5, "source_reported_count": 4,
                                                       "source_population_complete": True})
        self.assertFalse(failed.passed)
        inconsistent = paging_transmission_towers_complete({"feature_count": 4, "source_population_complete": True})
        self.assertFalse(inconsistent.passed)
        drift = paging_transmission_towers_drift({"dataset_slug": source.DATASET_SLUG,
                                                  "file_slug": source.FILE_SLUG, "feature_count": 4,
                                                  "source_reported_count": 4,
                                                  "source_population_complete": True,
                                                  "source_id_sha256": _sha(b"roster")})
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(list(paging_transmission_towers_drift.check_specs)[0].blocking)

    def test_source_contract(self):
        self.assertEqual(source.L_PAGING_URL, "https://data.fcc.gov/download/pub/uls/complete/l_paging.zip")
        self.assertEqual(source.RECORD_FIELD_COUNTS, {"HD": 59, "EN": 30, "LO": 51})
        self.assertEqual(source.EXPECTED_HD_RADIO_SERVICES, ["CA", "CB", "CD", "CG", "CO", "CR"])
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
        self.assertEqual(audit["source_counts_witness"], {"HD": 5, "EN": 6, "LO": 5})
        self.assertEqual(audit["source_hd_record_count"], 5)
        self.assertEqual(audit["source_license_status_counts"], {"A": 3, "C": 1, "E": 1})
        self.assertEqual(audit["source_radio_service_codes"], ["CB", "CD", "CG", "CO"])
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
        published, audit = self._validate(hd, en, lo)
        self.assertEqual(audit["source_feature_count"], 6)
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
            ("radio service", lambda hd, en, lo: ([hd[0].replace("|CD|", "|CX|"), *hd[1:]], en, lo),
             "source_radio_service_roster_drift"),
            ("licensee join", lambda hd, en, lo: (hd, [_en(101, "KNKA101", "CL", "Contact Two"), *en[1:]], lo),
             "source_licensee_join_drift"),
            ("callsign join", lambda hd, en, lo: (hd, en, [_lo(101, "KNKX999", 1), *lo[1:]]),
             "source_callsign_join_drift"),
            ("missing location number", lambda hd, en, lo: (hd, en, [_lo(101, "KNKA101", ""), *lo[1:]]),
             "source_location_number_invalid"),
            ("duplicate keys", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "KNKA101", 1), lo[2], lo[3], lo[4]]),
             "source_location_key_duplication_drift"),
            ("coordinate range", lambda hd, en, lo: (hd, en, [_lo(101, "KNKA101", 1, lon_deg="181"), *lo[1:]]),
             "source_coordinate_out_of_range"),
            ("partial dms", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "KNKA101", 2, state="TX", lon_deg="", lon_min="", lon_sec="", lon_dir=""), *lo[2:]]),
             "source_partial_dms_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                hd, en, lo = mutate(*_documents())
                with self.assertRaisesRegex(ValueError, expected):
                    self._validate(hd, en, lo)

    def test_counts_witness_mismatches_fail_closed(self):
        hd, en, lo = _documents()
        # The payload's own counts member disagrees with the rows the file
        # actually holds for a table: blocking completeness fails.
        with self.assertRaisesRegex(ValueError, "source_counts_file_row_count_drift:HD.dat"):
            self._validate(hd, en, lo, counts={"HD": 4})
        with self.assertRaisesRegex(ValueError, "source_counts_file_row_count_drift:EN.dat"):
            self._validate(hd, en, lo, counts={"EN": 5})
        with self.assertRaisesRegex(ValueError, "source_counts_file_row_count_drift:LO.dat"):
            self._validate(hd, en, lo, counts={"LO": 4})
        # A counts member that lost a table's line cannot witness completeness.
        with self.assertRaisesRegex(ValueError, "source_counts_file_member_missing:HD.dat"):
            self._validate(hd, en, lo, counts_override=lambda lines: [
                line for line in lines if not line.endswith("/HD.dat")])

    def test_upstream_edition_with_added_active_license_witnesses_and_warns(self):
        hd, en, lo = _documents()
        _base_published, base_audit = self._validate(hd, en, lo)
        # The publisher's next weekly edition adds one active license with one
        # filed location; the counts member in the same file witnesses it.
        hd2 = [*hd, _hd(106, "KNKA106", "A", service="CA")]
        en2 = [*en, _en(106, "KNKA106", "L", "Licensee Six")]
        lo2 = [*lo, _lo(106, "KNKA106", 1, state="OR")]
        published, audit = self._validate(hd2, en2, lo2, creation_date="Sun Nov 15 09:39:36 EST 2026")
        self.assertEqual(audit["source_feature_count"], 5)
        self.assertEqual(audit["source_counts_witness"], {"HD": 6, "EN": 7, "LO": 6})
        self.assertEqual(audit["source_hd_record_count"], 6)
        self.assertEqual(audit["source_license_status_counts"], {"A": 4, "C": 1, "E": 1})
        self.assertEqual(audit["source_radio_service_codes"], ["CA", "CB", "CD", "CG", "CO"])
        self.assertEqual(audit["source_service_row_counts"], {"CA": 1, "CD": 2, "CG": 1, "CO": 1})
        self.assertEqual(audit["source_file_creation_date"], "Sun Nov 15 09:39:36 EST 2026")
        self.assertEqual([(row["UniqSysID"], row["LocNum"]) for row in published],
                         [(101, 1), (101, 2), (103, 1), (105, 1), (106, 1)])
        # The composition drift is recorded, not refused.
        self.assertNotEqual(audit["source_active_id_sha256"], base_audit["source_active_id_sha256"])
        self.assertNotEqual(audit["source_state_roster_sha256"], base_audit["source_state_roster_sha256"])
        candidate = {"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
                     "feature_count": audit["source_feature_count"],
                     "source_reported_count": audit["source_feature_count"],
                     "source_population_complete": True,
                     "source_id_sha256": audit["source_active_id_sha256"]}
        self.assertTrue(paging_transmission_towers_complete(candidate).passed)
        drift = paging_transmission_towers_drift(candidate)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(list(paging_transmission_towers_drift.check_specs)[0].blocking)
        baseline = {**candidate, "version": "v-approved", "release_accepted": True,
                    "feature_count": base_audit["source_feature_count"],
                    "source_id_sha256": base_audit["source_active_id_sha256"]}
        compared = approved_baseline_drift(candidate, baseline)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.severity, AssetCheckSeverity.WARN)
        self.assertEqual(compared.metadata["comparison_status"].value, "compared_approved_release")
        self.assertEqual(compared.metadata["feature_count_delta"].value, 1)
        self.assertTrue(compared.metadata["native_id_roster_changed"].value)

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

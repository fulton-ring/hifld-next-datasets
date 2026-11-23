import io
import json
import struct
import tempfile
import unittest
import zipfile
from pathlib import Path

from dagster import AssetCheckSeverity, DefaultScheduleStatus
import pyarrow.parquet as pq
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.microwave_service_towers import (
    microwave_service_towers, microwave_service_towers_complete,
    microwave_service_towers_draft_schedule, microwave_service_towers_drift,
)
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import microwave_service_towers as source
from dagster_hifld.sources.esri_common import canonical as _canonical


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


def _members(hd, en, lo, extra=(), counts=None,
             creation_date="Sun Nov  8 09:47:03 EST 2026"):
    content = {}
    for name in source.ZIP_MEMBER_ROSTER:
        if name == "HD.dat":
            content[name] = ("\r\n".join(hd) + "\r\n").encode()
        elif name == "EN.dat":
            content[name] = ("\r\n".join(en) + "\r\n").encode()
        elif name == "LO.dat":
            content[name] = ("\r\n".join(lo) + "\r\n").encode()
        elif name == "counts":
            declared = counts if counts is not None else {"HD": len(hd), "EN": len(en), "LO": len(lo)}
            content[name] = ("File Creation Date: " + creation_date + "\r\n"
                             + "\r\n".join(f"{count} /home/pubacc/scripts/licweekzipdata/{table}.dat"
                                           for table, count in sorted(declared.items()))
                             + "\r\n").encode()
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
    def _validate(self, hd, en, lo, counts=None):
        return source._validate(_capture(_members(hd, en, lo, counts=counts)))

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(microwave_service_towers, hifld_ingest_assets)
        self.assertEqual(microwave_service_towers_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = microwave_service_towers_complete({"feature_count": 6, "source_reported_count": 6,
                                                   "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = microwave_service_towers_complete({"feature_count": 7, "source_reported_count": 6,
                                                    "source_population_complete": True})
        self.assertFalse(failed.passed)
        inconsistent = microwave_service_towers_complete({"feature_count": 6,
                                                          "source_population_complete": True})
        self.assertFalse(inconsistent.passed)
        drift = microwave_service_towers_drift({"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
                                                "feature_count": 6, "source_reported_count": 6,
                                                "source_population_complete": True, "source_id_sha256": _sha(b"roster")})
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(list(microwave_service_towers_drift.check_specs)[0].blocking)

    def test_complete_native_population_and_published_contract(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        self.assertEqual([(row["UniqSysID"], row["LocNum"]) for row in published],
                         [(101, 1), (101, 2), (103, 1), (103, 2), (103, 2), (105, 1)])
        self.assertTrue(all(row["LicStatus"] == "A" for row in published))
        self.assertEqual(audit["source_feature_count"], 6)
        self.assertEqual(audit["source_counts_witness"], {"HD": 5, "EN": 6, "LO": 7})
        self.assertEqual(audit["source_license_status_counts"], {"A": 3, "C": 1, "E": 1})
        self.assertEqual(audit["source_radio_service_codes"], ["CF", "MG"])
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
        empty = published[1]
        self.assertEqual(empty["TowReg"], "")
        self.assertEqual(empty["StrucType"], "")
        self.assertIsNone(empty["SupStruc"])
        self.assertIsNone(empty["AllStruc"])
        blank_callsign = published[5]
        self.assertEqual(blank_callsign["Callsign"], "")
        self.assertEqual(blank_callsign["Licensee"], "Licensee Five")
        self.assertEqual(blank_callsign["LocState"], "")

    def test_duplicate_location_rows_are_published_verbatim(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        duplicate_pair = [row for row in published if (row["UniqSysID"], row["LocNum"]) == (103, 2)]
        self.assertEqual(len(duplicate_pair), 2)
        self.assertEqual([row["latdec"] for row in duplicate_pair], [38.3268055556, 38.3347777778])
        self.assertEqual([row["londec"] for row in duplicate_pair], [-122.122416667, -122.105777778])
        self.assertEqual(audit["source_duplicate_location_key_count"], 1)
        self.assertEqual(audit["source_duplicate_key_roster_sha256"], _sha(_canonical([[103, 2]])))

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
        self.assertEqual(geometry_meta["columns"]["geometry"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geometry_meta["columns"]["geometry"]["crs"]["name"], "NAD83")
        self.assertEqual(audit["source_record_sha256"], _sha(_canonical(
            [{key: row[key] for key, _ in source.PUBLISHED_SCHEMA if key != "geometry"} for row in published])))

    def test_upstream_edition_with_added_active_license_witnesses_and_warns(self):
        hd, en, lo = _documents()
        base_audit = self._validate(hd, en, lo)[1]
        # The publisher's next weekly edition adds one active license with one
        # filed location; the counts member in the same file witnesses it.
        hd2 = [*hd, _hd(106, "KNKD106", "A", service="CF")]
        en2 = [*en, _en(106, "KNKD106", "L", "Licensee Six")]
        lo2 = [*lo, _lo(106, "KNKD106", 1, state="OR")]
        published, audit = self._validate(hd2, en2, lo2)
        self.assertEqual(audit["source_feature_count"], 7)
        self.assertEqual(audit["source_counts_witness"], {"HD": 6, "EN": 7, "LO": 8})
        self.assertEqual(audit["source_hd_record_count"], 6)
        self.assertEqual(audit["source_license_status_counts"], {"A": 4, "C": 1, "E": 1})
        self.assertEqual([(row["UniqSysID"], row["LocNum"]) for row in published],
                         [(101, 1), (101, 2), (103, 1), (103, 2), (103, 2), (105, 1), (106, 1)])
        # The composition drift is recorded, not refused.
        self.assertNotEqual(audit["source_active_id_sha256"], base_audit["source_active_id_sha256"])
        self.assertNotEqual(audit["source_state_roster_sha256"], base_audit["source_state_roster_sha256"])
        candidate = {"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
                     "feature_count": audit["source_feature_count"],
                     "source_reported_count": audit["source_feature_count"],
                     "source_population_complete": True,
                     "source_id_sha256": audit["source_active_id_sha256"]}
        self.assertTrue(microwave_service_towers_complete(candidate).passed)
        drift = microwave_service_towers_drift(candidate)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(list(microwave_service_towers_drift.check_specs)[0].blocking)
        baseline = {**candidate, "version": "v-approved", "release_accepted": True,
                    "feature_count": base_audit["source_feature_count"],
                    "source_id_sha256": base_audit["source_active_id_sha256"]}
        compared = approved_baseline_drift(candidate, baseline)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.severity, AssetCheckSeverity.WARN)
        self.assertEqual(compared.metadata["comparison_status"].value, "compared_approved_release")
        self.assertEqual(compared.metadata["feature_count_delta"].value, 1)
        self.assertTrue(compared.metadata["native_id_roster_changed"].value)

    def test_null_geometry_and_out_of_nominal_directions_are_preserved(self):
        hd, en, lo = _documents()
        lo = [
            lo[0], _lo(101, "KNKA101", 2, state="TX", lat_deg="", lat_min="", lat_sec="", lat_dir="", lon_deg="", lon_min="", lon_sec="", lon_dir=""),
            lo[2], lo[3], _lo(105, "", 1, state="", lat_deg="39", lat_min="46", lat_sec="7.1", lat_dir="W", lon_deg="86", lon_min="9", lon_sec="46.0", lon_dir="N"),
            lo[5], lo[6],
        ]
        published, audit = self._validate(hd, en, lo)
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

    def test_counts_witness_mismatches_fail_closed(self):
        hd, en, lo = _documents()
        # Fewer decoded rows than the publisher's counts member records.
        with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
            self._validate(hd[:4], en, lo, counts={"HD": 5, "EN": 6, "LO": 7})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:EN"):
            self._validate(hd, en[:5], lo, counts={"HD": 5, "EN": 6, "LO": 7})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:LO"):
            self._validate(hd, en, lo[:6], counts={"HD": 5, "EN": 6, "LO": 7})
        # More rows declared than the file holds.
        with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
            self._validate(hd, en, lo, counts={"HD": 6, "EN": 6, "LO": 7})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:EN"):
            self._validate(hd, en, lo, counts={"HD": 5, "EN": 7, "LO": 7})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:LO"):
            self._validate(hd, en, lo, counts={"HD": 5, "EN": 6, "LO": 8})
        # A counts member that lost a table's line cannot witness completeness.
        with self.assertRaisesRegex(ValueError, "source_counts_witness_missing:HD"):
            self._validate(hd, en, lo, counts={"EN": 6, "LO": 7})
        with self.assertRaisesRegex(ValueError, "source_counts_witness_missing:HD,EN,LO"):
            self._validate(hd, en, lo, counts={})

    def test_drift_guards_fail_closed(self):
        cases = [
            ("record field count", lambda hd, en, lo: (hd, en, [lo[0] + "|", *lo[1:]]),
             "source_record_field_count_drift:LO"),
            ("record prefix", lambda hd, en, lo: ([hd[0], hd[1].replace("HD|", "XX|", 1), *hd[2:]], en, lo),
             "source_record_type_prefix_drift:HD"),
            ("radio service", lambda hd, en, lo: ([hd[0].replace("|MG|", "|CX|"), *hd[1:]], en, lo),
             "source_radio_service_roster_drift"),
            ("licensee join", lambda hd, en, lo: (hd, [_en(101, "KNKA101", "CL", "Contact Two"), *en[1:]], lo),
             "source_licensee_join_drift"),
            ("callsign join", lambda hd, en, lo: (hd, en, [_lo(101, "KNKX999", 1), *lo[1:]]),
             "source_callsign_join_drift"),
            ("missing location number", lambda hd, en, lo: (hd, en, [_lo(101, "KNKA101", ""), *lo[1:]]),
             "source_location_number_invalid"),
            ("coordinate range", lambda hd, en, lo: (hd, en, [_lo(101, "KNKA101", 1, lon_deg="181"), *lo[1:]]),
             "source_coordinate_out_of_range"),
            ("partial dms", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "KNKA101", 2, lon_deg="", lon_min="", lon_sec="", lon_dir=""), *lo[2:]]),
             "source_partial_dms_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                hd, en, lo = mutate(*_documents())
                with self.assertRaisesRegex(ValueError, expected):
                    self._validate(hd, en, lo)

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

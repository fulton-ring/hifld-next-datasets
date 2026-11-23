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
from dagster_hifld.assets.land_mobile_private_transmission_towers import (
    land_mobile_private_transmission_towers,
    land_mobile_private_transmission_towers_complete,
    land_mobile_private_transmission_towers_draft_schedule,
    land_mobile_private_transmission_towers_drift,
)
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import land_mobile_private_transmission_towers as source
from dagster_hifld.sources.esri_common import canonical as _canonical


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


def _members(hd, en, lo, extra=(), counts=None,
             creation_date="Sun Nov  8 09:16:00 EST 2026"):
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
            total = sum(declared.values())
            content[name] = ("File Creation Date: " + creation_date + "\r\n"
                             + "\r\n".join(f"{count} /home/pubacc/scripts/licweekzipdata/{table}.dat"
                                           for table, count in sorted(declared.items()))
                             + f"\r\n{total:4d} total\r\n").encode()
        else:
            content[name] = b""
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for name in [*source.ZIP_MEMBER_ROSTER, *extra]:
            archive.writestr(name, content.get(name, b""))
    return stream.getvalue()


def _capture(body):
    return {"l_LMpriv": (body, {"method": "GET", "url": source.L_LMPRIV_URL,
                                "final_url": source.L_LMPRIV_URL, "status": 200,
                                "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}


class LandMobilePrivateTowersTests(unittest.TestCase):
    def _validate(self, hd, en, lo, counts=None):
        return source._validate(_capture(_members(hd, en, lo, counts=counts)))

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(land_mobile_private_transmission_towers, hifld_ingest_assets)
        self.assertEqual(land_mobile_private_transmission_towers_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = land_mobile_private_transmission_towers_complete(
            {"feature_count": 6, "source_reported_count": 6, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = land_mobile_private_transmission_towers_complete(
            {"feature_count": 7, "source_reported_count": 6, "source_population_complete": True})
        self.assertFalse(failed.passed)
        inconsistent = land_mobile_private_transmission_towers_complete(
            {"feature_count": 6, "source_population_complete": True})
        self.assertFalse(inconsistent.passed)
        drift = land_mobile_private_transmission_towers_drift(
            {"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
             "feature_count": 6, "source_reported_count": 6,
             "source_population_complete": True, "source_id_sha256": _sha(b"roster")})
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(list(land_mobile_private_transmission_towers_drift.check_specs)[0].blocking)

    def test_complete_native_population_and_published_contract(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        self.assertEqual([(row["UniqSysID"], row["LocNum"]) for row in published],
                         [(101, 1), (101, 1), (103, 1), (103, 2), (104, 1), (104, 2)])
        self.assertTrue(all(row["LicStatus"] == "A" for row in published))
        self.assertEqual(audit["source_feature_count"], 6)
        self.assertEqual(audit["source_counts_witness"], {"HD": 4, "EN": 5, "LO": 8})
        self.assertEqual(audit["source_hd_record_count"], 4)
        self.assertEqual(audit["source_en_record_count"], 5)
        self.assertEqual(audit["source_lo_record_count"], 8)
        self.assertEqual(audit["source_licensee_rows"], 4)
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
        self.assertEqual(geometry_meta["columns"]["geometry"]["crs"]["id"]["code"], 4269)
        self.assertEqual(audit["source_record_sha256"], _sha(_canonical(
            [{key: row[key] for key, _ in source.PUBLISHED_SCHEMA if key != "geometry"} for row in published])))

    def test_geometry_declares_fcc_nad83_source_datum_without_transformation(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        body = source._parquet_bytes(published)
        table = pq.read_table(io.BytesIO(body))
        crs = json.loads(table.schema.metadata[b"geo"])["columns"]["geometry"]["crs"]
        self.assertEqual(crs["type"], "GeographicCRS")
        self.assertEqual(crs["name"], "NAD83")
        self.assertEqual(crs["id"], {"authority": "EPSG", "code": 4269})
        self.assertEqual(crs["datum"]["name"], "North American Datum 1983")
        self.assertEqual(crs["datum"]["ellipsoid"]["name"], "GRS 1980")
        self.assertEqual(crs["datum"]["ellipsoid"]["semi_major_axis"], 6378137)
        self.assertEqual(crs["datum"]["ellipsoid"]["inverse_flattening"], 298.257222101)
        self.assertEqual(audit["source_coordinate_datum"], source.SOURCE_COORDINATE_DATUM)
        self.assertEqual(audit["source_coordinate_datum"]["crs"], "EPSG:4269")
        self.assertIn("north-american-datum-1983-coordinates", audit["source_coordinate_datum"]["fcc_policy_url"])
        self.assertIn("no datum transformation", audit["source_coordinate_datum"]["statement"])
        self.assertEqual(struct.unpack("<BIdd", table.to_pylist()[0]["geometry"]),
                         (1, 1, -121.758333333, 38.5041666667))

    def test_upstream_edition_with_added_active_license_witnesses_and_warns(self):
        hd, en, lo = _documents()
        base_audit = self._validate(hd, en, lo)[1]
        # The publisher's next weekly edition adds one active license with one
        # filed location; the counts member in the same file witnesses it, and
        # every retired snapshot pin (record totals, status and service
        # distributions, active-location count, roster digests, bounds) is
        # different in the new edition — the capture must still succeed.
        hd2 = [*hd, _hd(105, "KNKD105", "A", service="PA")]
        en2 = [*en, _en(105, "KNKD105", "L", "Licensee Five")]
        lo2 = [*lo, _lo(105, "KNKD105", 1, state="OR")]
        published, audit = self._validate(hd2, en2, lo2)
        self.assertEqual(audit["source_feature_count"], 7)
        self.assertEqual(audit["source_counts_witness"], {"HD": 5, "EN": 6, "LO": 9})
        self.assertEqual(audit["source_hd_record_count"], 5)
        self.assertEqual(audit["source_licensee_rows"], 5)
        self.assertEqual(audit["source_license_status_counts"], {"A": 4, "C": 1})
        self.assertEqual(audit["source_radio_service_codes"], ["IG", "PA", "PW", "YG"])
        self.assertEqual([(row["UniqSysID"], row["LocNum"]) for row in published],
                         [(101, 1), (101, 1), (103, 1), (103, 2), (104, 1), (104, 2), (105, 1)])
        # The composition drift is recorded, not refused.
        self.assertNotEqual(audit["source_active_id_sha256"], base_audit["source_active_id_sha256"])
        self.assertNotEqual(audit["source_state_roster_sha256"], base_audit["source_state_roster_sha256"])
        candidate = {"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
                     "feature_count": audit["source_feature_count"],
                     "source_reported_count": audit["source_feature_count"],
                     "source_population_complete": True,
                     "source_id_sha256": audit["source_active_id_sha256"]}
        self.assertTrue(land_mobile_private_transmission_towers_complete(candidate).passed)
        drift = land_mobile_private_transmission_towers_drift(candidate)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(list(land_mobile_private_transmission_towers_drift.check_specs)[0].blocking)
        baseline = {**candidate, "version": "v-approved", "release_accepted": True,
                    "feature_count": base_audit["source_feature_count"],
                    "source_id_sha256": base_audit["source_active_id_sha256"]}
        compared = approved_baseline_drift(candidate, baseline)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.severity, AssetCheckSeverity.WARN)
        self.assertEqual(compared.metadata["comparison_status"].value, "compared_approved_release")
        self.assertEqual(compared.metadata["feature_count_delta"].value, 1)
        self.assertTrue(compared.metadata["native_id_roster_changed"].value)

    def test_do_not_convert_local_datum_filings_are_quarantined(self):
        hd, en, lo = _documents()
        hd = [*hd, _hd(105, "KNKA105", "A", service="PA"), _hd(106, "KNKA106", "A", service="PA")]
        en = [*en, _en(105, "KNKA105", "L", "Licensee Five"), _en(106, "KNKA106", "L", "Licensee Six")]
        # Northern Mariana Islands (MP) and US Minor Outlying Islands (UM)
        # filings: FCC do-not-convert territories, so the filed coordinates
        # remain in the applicable local datum with no FCC-published
        # conversion to NAD83. A count of two (not the retired 15-row pin)
        # must ingest: the quarantine is a policy, its count and digest are
        # observations.
        mp_row = _lo(105, "KNKA105", 1, lat_deg="15", lat_min="7", lat_sec="25.6",
                     lon_deg="179", lon_min="43", lon_sec="28.9", state="MP")
        um_row = _lo(106, "KNKA106", 1, lat_deg="14", lat_min="10", lat_sec="0",
                     lon_deg="162", lon_min="12", lon_sec="0", lon_dir="W", state="UM")
        lo = [*lo, mp_row, um_row]
        mp_record = {
            "UniqSysID": 105, "Licensee": "Licensee Five", "Callsign": "KNKA105", "LocNum": 1,
            "LatDeg": 15.0, "LatMin": 7.0, "LatSec": 25.6, "LatDir": "N",
            "LonDeg": 179.0, "LonMin": 43.0, "LonSec": 28.9, "LonDir": "W",
            "LocAdd": "1 TOWER ROAD", "LocCity": "TOWNSVILLE", "LocCounty": "EXAMPLE",
            "LocState": "MP", "Nepa": "N", "QZone": "", "TowReg": "1023448",
            "SupStruc": 91.4, "AllStruc": 99.0, "StrucType": "TOWER", "LicStatus": "A",
            "latdec": source._decimal_degrees(15.0, 7.0, 25.6, 1),
            "londec": source._decimal_degrees(179.0, 43.0, 28.9, -1),
            "url": source.LICENSE_URL_TEMPLATE.format(105),
        }
        um_record = {
            "UniqSysID": 106, "Licensee": "Licensee Six", "Callsign": "KNKA106", "LocNum": 1,
            "LatDeg": 14.0, "LatMin": 10.0, "LatSec": 0.0, "LatDir": "N",
            "LonDeg": 162.0, "LonMin": 12.0, "LonSec": 0.0, "LonDir": "W",
            "LocAdd": "1 TOWER ROAD", "LocCity": "TOWNSVILLE", "LocCounty": "EXAMPLE",
            "LocState": "UM", "Nepa": "N", "QZone": "", "TowReg": "1023448",
            "SupStruc": 91.4, "AllStruc": 99.0, "StrucType": "TOWER", "LicStatus": "A",
            "latdec": source._decimal_degrees(14.0, 10.0, 0.0, 1),
            "londec": source._decimal_degrees(162.0, 12.0, 0.0, -1),
            "url": source.LICENSE_URL_TEMPLATE.format(106),
        }
        published, audit = self._validate(hd, en, lo)
        # Both do-not-convert rows are excluded from the published layer...
        published_keys = [(row["UniqSysID"], row["LocNum"]) for row in published]
        self.assertNotIn((105, 1), published_keys)
        self.assertNotIn((106, 1), published_keys)
        self.assertEqual(audit["source_feature_count"], 6)
        # ...but preserved in the audit with all filed attributes, at the
        # observed count and digest.
        self.assertEqual(audit["source_local_datum_quarantined_rows"], 2)
        self.assertEqual(audit["source_local_datum_quarantined_state_codes"], ["MP", "UM"])
        self.assertEqual(audit["source_local_datum_quarantined_records"], [mp_record, um_record])
        self.assertEqual(audit["source_local_datum_quarantined_sha256"],
                         _sha(_canonical([mp_record, um_record])))
        # The quarantined filings do not join the published state roster or
        # extend the published bounds.
        self.assertEqual(audit["source_state_count"], 4)
        self.assertEqual(audit["source_bounds"], [-123.0, -14.0, 145.333333333, 93.0])
        parquet = pq.read_table(io.BytesIO(source._parquet_bytes(published)))
        self.assertEqual(parquet.num_rows, 6)
        self.assertTrue(all(row["geometry"] is not None for row in parquet.to_pylist()))

    def test_trailing_extra_field_policy_rows_are_tolerated(self):
        hd, en, lo = _documents()
        # One extra empty trailing field is a tolerated ULS artifact: dropped,
        # published fields unaffected, observed count recorded in the audit.
        published, audit = self._validate([hd[0] + "|", *hd[1:]], en, lo)
        self.assertEqual(audit["source_extra_field_rows"], {"HD": 1, "EN": 0, "LO": 0})
        self.assertEqual(audit["source_feature_count"], 6)
        published, audit = self._validate(hd, [en[0] + "|", en[1] + "|", *en[2:]], lo)
        self.assertEqual(audit["source_extra_field_rows"], {"HD": 0, "EN": 2, "LO": 0})
        published, audit = self._validate(hd, en, [lo[0] + "|", *lo[1:]])
        self.assertEqual(audit["source_extra_field_rows"], {"HD": 0, "EN": 0, "LO": 1})
        # Anything other than the single trailing empty field fails closed.
        with self.assertRaisesRegex(ValueError, "source_record_field_count_drift:HD"):
            self._validate([hd[0] + "|X", *hd[1:]], en, lo)
        with self.assertRaisesRegex(ValueError, "source_record_field_count_drift:EN"):
            self._validate(hd, [en[0] + "|X", *en[1:]], lo)
        with self.assertRaisesRegex(ValueError, "source_record_field_count_drift:LO"):
            self._validate(hd, en, [lo[0] + "||", *lo[1:]])

    def test_drift_guards_fail_closed(self):
        cases = [
            ("record field count", lambda hd, en, lo: (hd, en, [lo[0] + "|X", *lo[1:]]),
             "source_record_field_count_drift:LO"),
            ("record prefix", lambda hd, en, lo: ([hd[0], hd[1].replace("HD|", "XX|", 1), *hd[2:]], en, lo),
             "source_record_type_prefix_drift:HD"),
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
            ("bounds envelope longitude", lambda hd, en, lo: (
                hd, en, [lo[0].replace("|121|45|30.0|W|", "|181|45|30.0|W|"), *lo[1:]]),
             "source_bounds_drift"),
            ("bounds envelope latitude", lambda hd, en, lo: (
                hd, en, [_lo(104, "", 1, lat_deg="95", lat_min="0", lat_sec="0", lon_deg="115",
                             lon_min="0", lon_sec="0", state="NV"), *lo[2:]]),
             "source_bounds_drift"),
            ("empty published population", lambda hd, en, lo: (
                [_hd(101, "KNKA101", "A")], [_en(101, "KNKA101", "L", "Licensee One")],
                [_lo(101, "KNKA101", 1, lon_deg="", lon_min="", lon_sec="", lon_dir="")]),
             "source_active_population_empty"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                hd, en, lo = mutate(*_documents())
                with self.assertRaisesRegex(ValueError, expected):
                    self._validate(hd, en, lo)

    def test_counts_witness_mismatches_fail_closed(self):
        hd, en, lo = _documents()
        # Fewer decoded rows than the publisher's counts member records.
        with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
            self._validate(hd[:3], en, lo, counts={"HD": 4, "EN": 5, "LO": 8})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:EN"):
            self._validate(hd, en[:4], lo, counts={"HD": 4, "EN": 5, "LO": 8})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:LO"):
            self._validate(hd, en, lo[:7], counts={"HD": 4, "EN": 5, "LO": 8})
        # More rows declared than the file holds.
        with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
            self._validate(hd, en, lo, counts={"HD": 3, "EN": 5, "LO": 8})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:EN"):
            self._validate(hd, en, lo, counts={"HD": 4, "EN": 4, "LO": 8})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:LO"):
            self._validate(hd, en, lo, counts={"HD": 4, "EN": 5, "LO": 7})
        # A counts member that lost a table's line cannot witness completeness.
        with self.assertRaisesRegex(ValueError, "source_counts_witness_missing:HD"):
            self._validate(hd, en, lo, counts={"EN": 5, "LO": 8})
        with self.assertRaisesRegex(ValueError, "source_counts_witness_missing:HD,EN,LO"):
            self._validate(hd, en, lo, counts={})

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

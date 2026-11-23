import io
import json
import struct
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from dagster import AssetCheckSeverity, DefaultScheduleStatus
import pyarrow.parquet as pq
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.land_mobile_commercial_transmission_towers import (
    land_mobile_commercial_transmission_towers, land_mobile_commercial_transmission_towers_complete,
    land_mobile_commercial_transmission_towers_draft_schedule,
    land_mobile_commercial_transmission_towers_drift,
)
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import land_mobile_commercial_transmission_towers as source
from dagster_hifld.sources.esri_common import canonical as _canonical


def _sha(value):
    import hashlib
    return hashlib.sha256(value).hexdigest()


def _hd(identifier, callsign, status, service="YX"):
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
    hd = [_hd(101, "WQNZ101", "A"), _hd(102, "WPNK102", "C", service="GS"),
          _hd(103, "WNKZ103", "A", service="IK"), _hd(104, "", "E", service="YM")]
    en = [_en(101, "WQNZ101", "L", "Licensee One"), _en(102, "WPNK102", "L", "Licensee Two"),
          _en(103, "WNKZ103", "L", "Licensee Three"), _en(104, "", "L", "Licensee Four"),
          _en(101, "WQNZ101", "CL", "Contact One")]
    lo = [
        _lo(101, "WQNZ101", 1, state="CA"),
        _lo(101, "WQNZ101", 2, state="TX", towreg="", structype="", supstruc="", allstruc="", qzone="01/02/2003"),
        _lo(102, "WPNK102", 5, state="WA"),
        _lo(103, "WNKZ103", 1, lat_sec="", lat_min="4", lat_deg="30", lon_deg="87", lon_min="57", state="LA"),
        _lo(103, "WNKZ103", 2, lat_deg="14", lat_min="0", lat_sec="0", lat_dir="S", lon_deg="145", lon_min="20", lon_sec="0", lon_dir="E", state="GU"),
        _lo(103, "WNKZ103", 3, lat_deg="", lon_deg="", state="GM"),
        _lo(104, "", 7, state="NY"),
    ]
    return hd, en, lo


def _members(hd, en, lo, extra=(), counts=None,
             creation_date="Sun Nov  8 09:24:38 EST 2026"):
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
    return {"l_LMcomm": (body, {"method": "GET", "url": source.L_LMCOMM_URL, "final_url": source.L_LMCOMM_URL,
                                "status": 200, "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}


def _sealed_capture_dir(root: Path, body: bytes) -> Path:
    root.mkdir(parents=True)
    wire = b"wire:l_LMcomm"
    receipt = {"method": "GET", "url": source.L_LMCOMM_URL, "final_url": source.L_LMCOMM_URL,
               "status": 200, "complete_body": True, "bytes": len(body), "sha256": _sha(body),
               "wire_bytes": len(wire), "wire_sha256": _sha(wire),
               "observed_at": "2026-11-08T14:24:38+00:00"}
    (root / "l_LMcomm.body").write_bytes(body)
    (root / "l_LMcomm.wire").write_bytes(wire)
    (root / "l_LMcomm.receipt.json").write_bytes(_canonical(receipt) + b"\n")
    (root / "capture_manifest.json").write_bytes(_canonical({
        "complete": True, "captured_at": "2026-11-08T14:24:39+00:00",
        "receipts": {"l_LMcomm": receipt}, "artifact_roster": source.capture_roster_digests(
            root, exclude={"capture_manifest.json"})}) + b"\n")
    return root


class LandMobileCommercialTowersTests(unittest.TestCase):
    def _validate(self, hd, en, lo, counts=None):
        return source._validate(_capture(_members(hd, en, lo, counts=counts)))

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(land_mobile_commercial_transmission_towers, hifld_ingest_assets)
        self.assertEqual(land_mobile_commercial_transmission_towers_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = land_mobile_commercial_transmission_towers_complete(
            {"feature_count": 5, "source_reported_count": 5, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = land_mobile_commercial_transmission_towers_complete(
            {"feature_count": 6, "source_reported_count": 5, "source_population_complete": True})
        self.assertFalse(failed.passed)
        inconsistent = land_mobile_commercial_transmission_towers_complete(
            {"feature_count": 5, "source_population_complete": True})
        self.assertFalse(inconsistent.passed)
        drift = land_mobile_commercial_transmission_towers_drift(
            {"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
             "feature_count": 5, "source_reported_count": 5,
             "source_population_complete": True, "source_id_sha256": _sha(b"roster")})
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(list(land_mobile_commercial_transmission_towers_drift.check_specs)[0].blocking)

    def test_source_identity_contract_constants_and_retired_pins_absent(self):
        # Contracts stay: the radio service roster, the datum states and the
        # source identity. The retired population pins are gone.
        self.assertEqual(source.DATASET_SLUG, "land-mobile-commercial-transmission-towers")
        self.assertEqual(source.L_LMCOMM_URL, "https://data.fcc.gov/download/pub/uls/complete/l_LMcomm.zip")
        self.assertEqual(source.EXPECTED_HD_RADIO_SERVICES,
                         ["GM", "GR", "GS", "GX", "IK", "LN", "LW", "NC", "QD", "QO", "QT", "YK", "YM", "YS", "YX"])
        self.assertEqual(source.DATUM_DO_NOT_CONVERT_STATES, frozenset({"MP"}))
        self.assertIn("l_LMcomm.zip", source.L_LMCOMM_URL)
        retired = ("EXPECTED_HD_RECORDS", "EXPECTED_EN_RECORDS", "EXPECTED_LO_RECORDS",
                   "EXPECTED_LICENSE_STATUS_COUNTS", "EXPECTED_LICENSEE_ROWS",
                   "EXPECTED_ACTIVE_LOCATIONS", "EXPECTED_ACTIVE_ID_SHA256",
                   "EXPECTED_STATE_ROSTER_SHA256", "EXPECTED_NULL_GEOMETRY_COUNT",
                   "EXPECTED_DO_NOT_CONVERT_WITHHELD_COUNT")
        for name in retired:
            self.assertFalse(hasattr(source, name), name)

    def test_complete_native_population_and_published_contract(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        self.assertEqual([(row["UniqSysID"], row["LocNum"]) for row in published],
                         [(101, 1), (101, 2), (103, 1), (103, 2), (103, 3)])
        self.assertTrue(all(row["LicStatus"] == "A" for row in published))
        self.assertEqual(audit["source_feature_count"], 5)
        self.assertEqual(audit["source_counts_witness"], {"HD": 4, "EN": 5, "LO": 7})
        self.assertEqual(audit["source_license_status_counts"], {"A": 2, "C": 1, "E": 1})
        self.assertEqual(audit["source_radio_service_codes"], ["GS", "IK", "YM", "YX"])
        self.assertEqual(audit["source_state_count"], 5)
        self.assertEqual(audit["source_null_geometry_count"], 1)
        self.assertEqual(audit["source_do_not_convert_geometry_withheld_count"], 0)
        self.assertEqual(audit["source_incomplete_seconds_count"], 1)
        self.assertEqual(audit["source_file_creation_date"], "Sun Nov  8 09:24:38 EST 2026")
        self.assertEqual(audit["source_bounds"], [-121.758333333, -14.0, 145.333333333, 38.5041666667])
        self.assertIn("not a census of physical towers", audit["source_coverage_scope"])
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
        null_row = published[4]
        self.assertIsNone(null_row["geometry"])
        self.assertIsNone(null_row["latdec"])
        self.assertIsNone(null_row["londec"])
        self.assertIsNone(null_row["LatDeg"])
        self.assertEqual(null_row["LatDir"], "N")
        self.assertEqual(null_row["url"], source.LICENSE_URL_TEMPLATE.format(103))
        empty = published[1]
        self.assertEqual(empty["TowReg"], "")
        self.assertEqual(empty["StrucType"], "")
        self.assertEqual(empty["QZone"], "01/02/2003")
        self.assertIsNone(empty["SupStruc"])
        self.assertIsNone(empty["AllStruc"])

    def test_counts_member_differing_from_retired_snapshot_pins_succeeds(self):
        # The retired edition pins (HD 81081 / EN 153247 / LO 185230 records,
        # 25501 active locations, 7547 null-geometry rows, 17 Do-Not-Convert
        # withheld rows, the status distribution and the ID/state digests) no
        # longer gate ingest: a captured l_LMcomm.zip whose own counts member
        # differs from them validates.
        hd, en, lo = _documents()
        self.assertNotEqual((len(hd), len(en), len(lo)), (81081, 153247, 185230))
        published, audit = self._validate(hd, en, lo)
        self.assertEqual(audit["source_counts_witness"], {"HD": 4, "EN": 5, "LO": 7})
        self.assertEqual(audit["source_feature_count"], 5)
        self.assertEqual(audit["source_null_geometry_count"], 1)

    def test_ingest_replays_sealed_capture_whose_counts_differ_from_retired_pins(self):
        hd, en, lo = _documents()
        body = _members(hd, en, lo)
        with tempfile.TemporaryDirectory() as tmp:
            capture_dir = _sealed_capture_dir(Path(tmp) / "captures" / "l_LMcomm", body)
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with mock.patch.object(source, "processing_identity",
                                   lambda repo_root: {"code_commit": "0" * 40,
                                                      "code_identity_scheme": "test-fixed-identity",
                                                      "files": {}}), \
                 mock.patch.object(source, "validate_construction_code_identity",
                                   lambda *args, **kwargs: None):
                result = source.capture_land_mobile_commercial_transmission_towers(
                    storage, version="test-version", capture_directory=str(capture_dir))
            self.assertEqual(result["dataset_slug"], source.DATASET_SLUG)
            self.assertEqual(result["feature_count"], 5)
            self.assertEqual(result["source_reported_count"], 5)
            self.assertTrue(result["source_population_complete"])
            self.assertEqual(result["source_file_creation_date"], "Sun Nov  8 09:24:38 EST 2026")
            self.assertEqual(result["reused"], False)
            self.assertFalse(result["source_release_accepted"])
            self.assertFalse(result["release_accepted"])
            ready = json.loads((Path(tmp) / "stage" / result["candidate_ready"]).read_bytes())
            self.assertFalse(ready["approved"])

    def test_upstream_edition_with_added_active_license_witnesses_and_warns(self):
        hd, en, lo = _documents()
        base_audit = self._validate(hd, en, lo)[1]
        # The publisher's next weekly edition adds one active license with one
        # filed location; the counts member in the same file witnesses it.
        hd2 = [*hd, _hd(105, "WQNZ105", "A")]
        en2 = [*en, _en(105, "WQNZ105", "L", "Licensee Five")]
        lo2 = [*lo, _lo(105, "WQNZ105", 1, state="OR")]
        published, audit = self._validate(hd2, en2, lo2)
        self.assertEqual(audit["source_feature_count"], 6)
        self.assertEqual(audit["source_counts_witness"], {"HD": 5, "EN": 6, "LO": 8})
        self.assertEqual(audit["source_hd_record_count"], 5)
        self.assertEqual(audit["source_license_status_counts"], {"A": 3, "C": 1, "E": 1})
        self.assertEqual([(row["UniqSysID"], row["LocNum"]) for row in published],
                         [(101, 1), (101, 2), (103, 1), (103, 2), (103, 3), (105, 1)])
        # The composition drift is recorded, not refused.
        self.assertNotEqual(audit["source_active_id_sha256"], base_audit["source_active_id_sha256"])
        self.assertNotEqual(audit["source_state_roster_sha256"], base_audit["source_state_roster_sha256"])
        candidate = {"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
                     "feature_count": audit["source_feature_count"],
                     "source_reported_count": audit["source_feature_count"],
                     "source_population_complete": True,
                     "source_id_sha256": audit["source_active_id_sha256"]}
        self.assertTrue(land_mobile_commercial_transmission_towers_complete(candidate).passed)
        drift = land_mobile_commercial_transmission_towers_drift(candidate)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(list(land_mobile_commercial_transmission_towers_drift.check_specs)[0].blocking)
        baseline = {**candidate, "version": "v-approved", "release_accepted": True,
                    "feature_count": base_audit["source_feature_count"],
                    "source_id_sha256": base_audit["source_active_id_sha256"]}
        compared = approved_baseline_drift(candidate, baseline)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.severity, AssetCheckSeverity.WARN)
        self.assertEqual(compared.metadata["comparison_status"].value, "compared_approved_release")
        self.assertEqual(compared.metadata["feature_count_delta"].value, 1)
        self.assertTrue(compared.metadata["native_id_roster_changed"].value)

    def test_counts_witness_mismatches_fail_closed(self):
        hd, en, lo = _documents()
        # Decoded rows disagreeing with the payload's own counts member still
        # fail blocking completeness.
        with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
            self._validate(hd[:3], en, lo, counts={"HD": 4, "EN": 5, "LO": 7})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:EN"):
            self._validate(hd, en[:3], lo, counts={"HD": 4, "EN": 5, "LO": 7})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:LO"):
            self._validate(hd, en, lo[:4], counts={"HD": 4, "EN": 5, "LO": 7})
        # More rows declared than the file holds.
        with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
            self._validate(hd, en, lo, counts={"HD": 5, "EN": 5, "LO": 7})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:EN"):
            self._validate(hd, en, lo, counts={"HD": 4, "EN": 6, "LO": 7})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:LO"):
            self._validate(hd, en, lo, counts={"HD": 4, "EN": 5, "LO": 8})
        # A counts member that lost a table's line cannot witness completeness.
        with self.assertRaisesRegex(ValueError, "source_counts_witness_missing:HD"):
            self._validate(hd, en, lo, counts={"EN": 5, "LO": 7})
        with self.assertRaisesRegex(ValueError, "source_counts_witness_missing:HD,EN,LO"):
            self._validate(hd, en, lo, counts={})

    def test_active_callsign_strictness_and_verbatim_join(self):
        hd, en, lo = _documents()
        published, _audit = self._validate(hd, en, lo)
        self.assertNotIn(104, [row["UniqSysID"] for row in published])
        with self.assertRaisesRegex(ValueError, "source_callsign_join_drift"):
            self._validate(hd, en, [*lo, _lo(104, "WRQX888", 2, state="NY")])
        with self.assertRaisesRegex(ValueError, "source_active_callsign_missing"):
            self._validate([hd[0], hd[1], hd[2], _hd(104, "", "A", service="YM")], en, lo)

    def test_parquet_candidate_preserves_columns_types_and_verbatim_values(self):
        hd, en, lo = _documents()
        published, audit = self._validate(hd, en, lo)
        body = source._parquet_bytes(published)
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.schema.names, [name for name, _ in source.PUBLISHED_SCHEMA])
        self.assertEqual(table.num_rows, 5)
        rows = table.to_pylist()
        self.assertEqual(rows[0]["UniqSysID"], 101)
        self.assertEqual(rows[0]["TowReg"], "1023448")
        self.assertEqual(rows[1]["TowReg"], "")
        self.assertIsNone(rows[1]["SupStruc"])
        self.assertIsNone(rows[2]["LatSec"])
        self.assertEqual(rows[2]["latdec"], 30.0666666667)
        self.assertEqual(rows[3]["londec"], 145.333333333)
        self.assertIsNone(rows[4]["geometry"])
        self.assertIsNone(rows[4]["latdec"])
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
        self.assertIn("Do-Not-Convert", source.GEOMETRY_PROVENANCE)
        self.assertIn("Northern Mariana", source.GEOMETRY_PROVENANCE)
        self.assertIn("local datum", source.GEOMETRY_PROVENANCE)

    def test_drift_guards_fail_closed(self):
        cases = [
            ("record field count", lambda hd, en, lo: (hd, en, [lo[0] + "|", *lo[1:]]),
             "source_record_field_count_drift:LO", None),
            ("record prefix", lambda hd, en, lo: ([hd[0], hd[1].replace("HD|", "XX|", 1), *hd[2:]], en, lo),
             "source_record_type_prefix_drift:HD", None),
            ("radio service", lambda hd, en, lo: ([hd[0].replace("|YX|", "|CX|"), *hd[1:]], en, lo),
             "source_radio_service_roster_drift", None),
            ("duplicate license identity", lambda hd, en, lo: ([hd[0], hd[0], *hd[2:]], en, lo),
             "source_duplicate_license_identity", {"HD": 4, "EN": 5, "LO": 7}),
            ("licensee join", lambda hd, en, lo: (hd, [_en(101, "WQNZ101", "CL", "Contact Two"), *en[1:]], lo),
             "source_licensee_join_drift", None),
            ("duplicate keys", lambda hd, en, lo: (hd, en, [lo[0], lo[1], lo[2], lo[3], _lo(103, "WNKZ103", 1), lo[5], lo[6]]),
             "source_location_key_duplication_drift", None),
            ("callsign join", lambda hd, en, lo: (hd, en, [_lo(101, "WQNX999", 1), *lo[1:]]),
             "source_callsign_join_drift", None),
            ("missing location number", lambda hd, en, lo: (hd, en, [lo[0], lo[1], lo[2], lo[3], lo[4], lo[5], _lo(104, "", "")]),
             "source_location_number_invalid", None),
            ("latitude direction", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "WQNZ101", 2, lat_dir="X"), *lo[2:]]),
             "source_latitude_direction_drift", None),
            ("latitude coordinate range", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "WQNZ101", 2, lat_deg="999"), *lo[2:]]),
             "source_coordinate_out_of_range", None),
            ("coordinate range", lambda hd, en, lo: (hd, en, [lo[0], _lo(101, "WQNZ101", 2, lon_deg="180", lon_min="59", lon_sec="59.9", lon_dir="E"), *lo[2:]]),
             "source_coordinate_out_of_range", None),
        ]
        for label, mutate, expected, counts in cases:
            with self.subTest(label=label):
                hd, en, lo = mutate(*_documents())
                with self.assertRaisesRegex(ValueError, expected):
                    self._validate(hd, en, lo, counts=counts)

    def test_out_of_nominal_dms_components_are_preserved_and_converted(self):
        hd, en, lo = _documents()
        lo = [lo[0], _lo(101, "WQNZ101", 2, lat_deg="36", lat_min="73", lat_sec="11.3", state="TX"), *lo[2:]]
        published, _audit = self._validate(hd, en, lo)
        preserved = published[1]
        self.assertEqual((preserved["LatDeg"], preserved["LatMin"], preserved["LatSec"]), (36, 73, 11.3))
        self.assertEqual(preserved["latdec"], 37.2198055556)
        self.assertIsNotNone(preserved["geometry"])

    def test_coordinate_less_active_location_published_with_null_geometry(self):
        hd, en, lo = _documents()
        lo = [lo[0], _lo(101, "WQNZ101", 2, lat_deg="", lat_min="", lat_sec="", lat_dir="",
                         lon_deg="", lon_min="", lon_sec="", lon_dir="", state="TX"), *lo[2:]]
        published, audit = self._validate(hd, en, lo)
        row = published[1]
        self.assertIsNone(row["geometry"])
        self.assertEqual((row["LatDeg"], row["LatMin"], row["LatSec"], row["LatDir"]), (None, None, None, ""))
        self.assertEqual((row["LonDeg"], row["LonMin"], row["LonSec"], row["LonDir"]), (None, None, None, ""))
        self.assertEqual(audit["source_null_geometry_count"], 2)

    def test_northern_mariana_local_datum_rows_publish_without_geometry(self):
        hd, en, lo = _documents()
        # Mirrors the pinned-file Northern Mariana Islands sample
        # (UniqSysID, LocNum) = (1129323, 1): 15 11 10.0 N / 145 44 35.0 E.
        lo = [lo[0], _lo(101, "WQNZ101", 2, lat_deg="15", lat_min="11", lat_sec="10.0", lat_dir="N",
                         lon_deg="145", lon_min="44", lon_sec="35.0", lon_dir="E", state="MP"), *lo[2:]]
        published, audit = self._validate(hd, en, lo)
        mariana = published[1]
        self.assertIsNone(mariana["geometry"])
        # The FCC leaves Do-Not-Convert coordinates in their applicable local
        # datum, which ULS does not record; raw DMS components and derived
        # decimals are retained unchanged as evidence, only the datum-labeled
        # geometry column is withheld.
        self.assertEqual((mariana["LatDeg"], mariana["LatMin"], mariana["LatSec"], mariana["LatDir"]),
                         (15, 11, 10.0, "N"))
        self.assertEqual((mariana["latdec"], mariana["londec"]), (15.1861111111, 145.743055556))
        self.assertEqual(audit["source_do_not_convert_geometry_withheld_count"], 1)
        self.assertEqual(audit["source_null_geometry_count"], 2)
        body = source._parquet_bytes(published)
        rows = pq.read_table(io.BytesIO(body)).to_pylist()
        self.assertIsNone(rows[1]["geometry"])
        self.assertEqual((rows[1]["latdec"], rows[1]["londec"]), (15.1861111111, 145.743055556))
        # Converted-area rows (Guam, FCC Method #2) keep their NAD83 geometry.
        guam = published[3]
        self.assertIsNotNone(guam["geometry"])
        self.assertEqual(struct.unpack("<BIdd", guam["geometry"])[2:], (145.333333333, -14.0))

    def test_zip_container_and_capture_receipts_fail_closed(self):
        hd, en, lo = _documents()
        body = _members(hd, en, lo)
        with self.assertRaisesRegex(ValueError, "source_zip_member_roster_drift"):
            source._validate(_capture(_members(hd, en, lo, extra=["extra.dat"])))
        with self.assertRaisesRegex(ValueError, "source_zip_unreadable"):
            source._validate(_capture(b"not a zip archive"))
        bad_status = {"l_LMcomm": (body, {"method": "GET", "url": source.L_LMCOMM_URL,
                                          "final_url": source.L_LMCOMM_URL, "status": 500,
                                          "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "source_capture_status_length_hash_or_completeness_drift"):
            source._validate(bad_status)
        incomplete = {"l_LMcomm": (body, {"method": "GET", "url": source.L_LMCOMM_URL,
                                          "final_url": source.L_LMCOMM_URL, "status": 200,
                                          "complete_body": False, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "source_capture_status_length_hash_or_completeness_drift"):
            source._validate(incomplete)
        redirect = {"l_LMcomm": (body, {"method": "GET", "url": source.L_LMCOMM_URL,
                                        "final_url": "https://elsewhere.example/l_LMcomm.zip", "status": 200,
                                        "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
            source._validate(redirect)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_land_mobile_commercial_transmission_towers(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

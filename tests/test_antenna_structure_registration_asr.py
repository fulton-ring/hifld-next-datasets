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
from dagster_hifld.assets.antenna_structure_registration_asr import (
    antenna_structure_registration_asr, antenna_structure_registration_asr_complete,
    antenna_structure_registration_asr_draft_schedule, antenna_structure_registration_asr_drift,
)
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import antenna_structure_registration_asr as source
from dagster_hifld.sources.esri_common import canonical as _canonical


def _sha(value):
    import hashlib
    return hashlib.sha256(value).hexdigest()


def _ra(regnum, usi, **values):
    row = [""] * 49
    row[0], row[3], row[4] = "RA", regnum, usi
    row[8] = values.get("status", "C")
    row[15] = values.get("archive", "C")
    row[25] = values.get("state", "TX")
    row[9] = values.get("date_entered", "11/08/2026")
    row[33] = values.get("faa_date", "11/01/2026")
    return "|".join(row)


def _en(regnum, usi, entity_type, **values):
    row = [""] * 25
    row[0], row[3], row[4], row[5] = "EN", regnum, usi, entity_type
    row[6] = values.get("entity_type_code", "")
    row[9] = values.get("name", "ACME TOWER CO    ")
    row[17] = values.get("street", "1 TOWER ROAD")
    row[20] = values.get("city", "TOWNSVILLE")
    row[21] = values.get("state", "TX")
    row[22] = values.get("zip", "75001")
    return "|".join(row)


def _co(regnum, usi, coord_type, **values):
    row = [""] * 18
    row[0], row[3], row[4], row[5] = "CO", regnum, usi, coord_type
    row[6], row[7], row[8], row[9] = (str(values.get("lat_deg", "")), str(values.get("lat_min", "")),
                                      str(values.get("lat_sec", "")), values.get("lat_dir", ""))
    row[10] = values.get("lat_total", "")
    row[11], row[12], row[13], row[14] = (str(values.get("lon_deg", "")), str(values.get("lon_min", "")),
                                          str(values.get("lon_sec", "")), values.get("lon_dir", ""))
    row[15] = values.get("lon_total", "")
    row[16] = str(values.get("array_position", ""))
    row[17] = str(values.get("array_total", ""))
    return "|".join(row)


def _documents():
    ra = [
        _ra("1000001", "101", status="A", archive="C", state="AL"),
        _ra("1000002", "102", status="C", archive="A", state="PR"),
        _ra("1000003", "103", status="G", archive="C", state="GU"),
        _ra("1000004", "104", status="T", archive="C", state="TX"),
        _ra("1000005", "105", status="I", archive="C", state="AK"),
        _ra("1000006", "106", status="C", archive="C", state="HI"),
    ]
    en = [
        _en("1000001", "101", "O", name="ACME TOWER CO    ", entity_type_code="C"),
        _en("1000001", "101", "R", name="ACME REGISTRANT"),
        _en("1000002", "102", "O"),
        _en("1000005", "105", "O"),
        _en("1000005", "105", "R"),
    ]
    co = [
        _co("1000001", "101", "T", lat_deg="43", lat_min="13", lat_sec="54.0", lat_dir="N",
            lat_total="155634.0", lon_deg="77", lon_min="52", lon_sec=".0", lon_dir="W",
            lon_total="280320.0"),
        _co("1000002", "102", "T", lat_deg="14", lat_min="0", lat_sec="0", lat_dir="S",
            lat_total="50400.0", lon_deg="145", lon_min="20", lon_sec="0", lon_dir="E",
            lon_total="523200.0"),
        _co("1000004", "104", "T", lat_sec=".0", lon_sec=".0"),
        _co("1000005", "105", "T", lat_deg="61", lat_min="13", lat_sec="6.0", lat_dir="N",
            lat_total="220386.0", lon_deg="149", lon_min="53", lon_sec="9.0", lon_dir="W",
            lon_total="539589.0"),
        _co("1000005", "105", "A", lat_deg="61", lat_min="13", lat_sec="30.0", lat_dir="N",
            lat_total="220410.0", lon_deg="149", lon_min="53", lon_sec="8.0", lon_dir="W",
            lon_total="539588.0", array_position="2", array_total="3"),
        _co("1000006", "106", "T", lat_deg="21", lat_min="18", lat_sec="30.0", lat_dir="N",
            lat_total="76710.0", lon_deg="158", lon_dir="W", lon_total="568800.0"),
    ]
    return ra, en, co


def _members(ra, en, co, extra=(), counts=None, counts_bytes=None,
             creation_date="Sun Nov  8 06:21:02 EST 2026"):
    """Build the archive with a counts member that witnesses the fixture's own rows.

    The captured archive's counts member declares per-table line counts and its
    file creation date; the fixture generates both from the rows it holds, so a
    mutated fixture witnesses its own new edition instead of a retired pin.
    ``counts`` overrides a single declared count, ``counts_bytes`` replaces the
    whole counts member, and ``creation_date`` overrides the build stamp.
    """
    content = {}
    for name in source.ZIP_MEMBER_ROSTER:
        if name == "RA.dat":
            content[name] = ("\r\n".join(ra) + "\r\n").encode()
        elif name == "EN.dat":
            content[name] = ("\r\n".join(en) + "\r\n").encode()
        elif name == "CO.dat":
            content[name] = ("\r\n".join(co) + "\r\n").encode()
        elif name != "counts":
            content[name] = b""
    if counts_bytes is None:
        declared = {"RA": len(ra), "CO": len(co), "EN": len(en)}
        if counts is not None:
            declared.update(counts)
        lines = ["File Creation Date: " + creation_date]
        for name, count in sorted(declared.items()):
            lines.append("%6d /home/pubacc/scripts/towweekzipdata/%s.dat" % (count, name))
        counts_bytes = ("\r\n".join(lines) + "\r\n").encode()
    content["counts"] = counts_bytes
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for name in [*source.ZIP_MEMBER_ROSTER, *extra]:
            archive.writestr(name, content.get(name, b""))
    return stream.getvalue()


def _capture(body):
    return {"r_tower": (body, {"method": "GET", "url": source.R_TOWER_URL, "final_url": source.R_TOWER_URL,
                               "status": 200, "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}


def _row(record, name):
    return record[source.RECORD_VALUE_NAMES.index(name)]


class AntennaStructureRegistrationTests(unittest.TestCase):
    """Focused adapter tests over synthetic RA/CO/EN tower tables."""

    def _reference_records(self, ra, en, co):
        ra_by_key = {}
        for r in ra:
            fields = r.split("|")
            ra_by_key[(fields[3], fields[4])] = fields
        owner_by_key = {}
        for e in en:
            fields = e.split("|")
            if fields[5] == "O":
                owner_by_key[(fields[3], fields[4])] = fields
        records = []
        for c in co:
            fields = c.split("|")
            owner = owner_by_key.get((fields[3], fields[4]))
            record = [*ra_by_key[(fields[3], fields[4])], *fields,
                      *(owner if owner is not None else [None] * 25), None, None]
            lat_deg, lat_min, lat_sec, lat_dir = (fields[6].strip(), fields[7].strip(),
                                                  fields[8].strip(), fields[9].strip())
            lon_deg, lon_min, lon_sec, lon_dir = (fields[11].strip(), fields[12].strip(),
                                                  fields[13].strip(), fields[14].strip())
            if lat_deg and lon_deg and lat_dir in ("N", "S") and lon_dir in ("W", "E"):
                record[-2] = source._decimal_degrees(lat_deg, lat_min, lat_sec, lat_dir, "S")
                record[-1] = source._decimal_degrees(lon_deg, lon_min, lon_sec, lon_dir, "W")
            records.append(record)
        order = {name: index for index, name in enumerate(source.RECORD_VALUE_NAMES)}
        records.sort(key=lambda record: tuple(record[order[name]]
                                              for name in source.ROW_KEY_COLUMNS))
        return records

    def _validate(self, ra, en, co, counts=None, counts_bytes=None, creation_date=None, extra=()):
        kwargs = {"extra": extra}
        if counts is not None:
            kwargs["counts"] = counts
        if counts_bytes is not None:
            kwargs["counts_bytes"] = counts_bytes
        if creation_date is not None:
            kwargs["creation_date"] = creation_date
        return source._validate(_capture(_members(ra, en, co, **kwargs)))

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(antenna_structure_registration_asr, hifld_ingest_assets)
        self.assertEqual(antenna_structure_registration_asr_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = antenna_structure_registration_asr_complete(
            {"feature_count": 6, "source_reported_count": 6, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = antenna_structure_registration_asr_complete(
            {"feature_count": 7, "source_reported_count": 6, "source_population_complete": True})
        self.assertFalse(failed.passed)
        inconsistent = antenna_structure_registration_asr_complete(
            {"feature_count": 6, "source_population_complete": True})
        self.assertFalse(inconsistent.passed)
        drift = antenna_structure_registration_asr_drift({"dataset_slug": source.DATASET_SLUG,
                                                          "file_slug": source.FILE_SLUG, "feature_count": 6,
                                                          "source_reported_count": 6,
                                                          "source_population_complete": True,
                                                          "source_id_sha256": _sha(b"roster")})
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(list(antenna_structure_registration_asr_drift.check_specs)[0].blocking)

    def test_complete_native_population_and_published_contract(self):
        ra, en, co = _documents()
        records, audit = self._validate(ra, en, co)
        keys = [(_row(record, "ra_registration_number"), _row(record, "ra_unique_system_identifier"),
                 _row(record, "co_coordinate_type")) for record in records]
        self.assertEqual(keys, [("1000001", "101", "T"), ("1000002", "102", "T"),
                                ("1000004", "104", "T"), ("1000005", "105", "A"),
                                ("1000005", "105", "T"), ("1000006", "106", "T")])
        self.assertEqual(audit["source_feature_count"], 6)
        self.assertEqual(audit["source_counts_witness"], {"RA": 6, "CO": 6, "EN": 5})
        self.assertEqual(audit["source_null_geometry_count"], 1)
        self.assertEqual(audit["source_rows_without_owner_entity"], 2)
        self.assertEqual(audit["source_registrations_without_entity"], 3)
        self.assertEqual(audit["source_owner_entity_rows"], 3)
        self.assertEqual(audit["source_registrant_entity_rows"], 2)
        self.assertEqual(audit["source_coordinate_type_counts"], {"T": 5, "A": 1})
        self.assertEqual(audit["source_array_rows_with_position"], 1)
        self.assertEqual(audit["source_array_rows_without_position"], 0)
        self.assertEqual(audit["source_file_creation_date"], "Sun Nov  8 06:21:02 EST 2026")
        self.assertEqual(audit["source_state_count"], 6)
        self.assertIn("not a census of physical towers", audit["source_coverage_scope"])
        first = records[0]
        self.assertEqual(_row(first, "ra_status_code"), "A")
        self.assertEqual(_row(first, "en_entity_name"), "ACME TOWER CO    ")
        self.assertEqual(_row(first, "en_entity_type"), "O")
        self.assertEqual(_row(first, "co_latitude_direction"), "N")
        self.assertEqual(_row(first, "latdec"), 43.2316666667)
        self.assertEqual(_row(first, "londec"), -77.8666666667)
        self.assertEqual(_row(first, "co_longitude_seconds"), ".0")
        southern = records[1]
        self.assertEqual(_row(southern, "latdec"), -14.0)
        self.assertEqual(_row(southern, "londec"), 145.333333333)
        null_geom = records[2]
        self.assertIsNone(_row(null_geom, "latdec"))
        self.assertIsNone(_row(null_geom, "londec"))
        self.assertIsNone(_row(null_geom, "en_entity_name"))
        self.assertEqual(_row(null_geom, "ra_status_code"), "T")
        array = records[3]
        self.assertEqual(_row(array, "co_array_tower_position"), "2")
        self.assertEqual(_row(array, "co_array_total_tower"), "3")
        self.assertEqual(_row(array, "latdec"), 61.225)
        self.assertEqual(_row(array, "londec"), -149.885555556)
        tower = records[4]
        self.assertEqual(_row(tower, "latdec"), 61.2183333333)
        self.assertEqual(_row(tower, "londec"), -149.885833333)
        degrees_only = records[5]
        self.assertEqual(_row(degrees_only, "co_longitude_minutes"), "")
        self.assertEqual(_row(degrees_only, "latdec"), 21.3083333333)
        self.assertEqual(_row(degrees_only, "londec"), -158.0)
        self.assertEqual(audit["source_bounds"], [-158.0, -14.0, 145.333333333, 61.225])

    def test_parquet_candidate_preserves_columns_types_and_verbatim_values(self):
        ra, en, co = _documents()
        body = source._parquet_bytes(self._reference_records(ra, en, co))
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.schema.names, source.PUBLISHED_COLUMN_NAMES)
        self.assertEqual(len(table.schema.names), 95)
        self.assertEqual(table.num_rows, 6)
        rows = table.to_pylist()
        self.assertEqual(rows[0]["ra_registration_number"], "1000001")
        self.assertEqual(rows[0]["en_entity_name"], "ACME TOWER CO    ")
        self.assertEqual(rows[0]["co_latitude_degrees"], "43")
        self.assertEqual(struct.unpack("<BIdd", rows[0]["geometry"]),
                         (1, 1, -77.8666666667, 43.2316666667))
        self.assertIsNone(rows[2]["geometry"])
        self.assertIsNone(rows[2]["latdec"])
        self.assertIsNone(rows[2]["en_entity_name"])
        self.assertEqual(struct.unpack("<BIdd", rows[3]["geometry"]),
                         (1, 1, -149.885555556, 61.225))
        self.assertEqual(struct.unpack("<BIdd", rows[4]["geometry"]),
                         (1, 1, -149.885833333, 61.2183333333))
        geometry_meta = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geometry_meta["primary_column"], "geometry")
        self.assertEqual(geometry_meta["columns"]["geometry"]["geometry_types"], ["Point"])
        self.assertEqual(geometry_meta["columns"]["geometry"]["crs"]["id"]["code"], 4269)
        self.assertEqual(geometry_meta["columns"]["geometry"]["crs"]["name"], "NAD83")
        self.assertEqual(geometry_meta["columns"]["geometry"]["crs"]["datum"]["ellipsoid"]["name"],
                         "GRS 1980")

    def test_drift_guards_fail_closed(self):
        cases = [
            ("record field count", lambda d: (d[0], d[1], [d[2][0] + "|", *d[2][1:]]),
             "source_record_field_count_drift:CO"),
            ("record prefix", lambda d: ([d[0][0], d[0][1].replace("RA|", "XX|", 1), *d[0][2:]], d[1], d[2]),
             "source_record_type_prefix_drift:RA"),
            ("non-numeric unique system identifier",
             lambda d: ([d[0][0].replace("|101|", "|X01|", 1), *d[0][1:]], d[1], d[2]),
             "source_unique_system_identifier_invalid"),
            ("duplicate registration number", lambda d: (
                [d[0][0], _ra("1000001", "102", status="C", archive="A", state="PR"), *d[0][2:]], d[1], d[2]),
             "source_duplicate_registration_number"),
            ("duplicate unique system identifier", lambda d: (
                [d[0][0], d[0][1], _ra("1000003", "101"), *d[0][3:]], d[1], d[2]),
             "source_duplicate_unique_system_identifier"),
            ("owner entity duplication", lambda d: (d[0], [d[1][0], _en("1000001", "101", "O"), *d[1][2:]], d[2]),
             "source_duplicate_owner_entity_row"),
            ("coordinate join", lambda d: (d[0], d[1], [d[2][0].replace("1000001", "1999999", 1), *d[2][1:]]),
             "source_coordinate_join_drift"),
            ("duplicate coordinate key", lambda d: (
                d[0], d[1], [*d[2][:5], _co("1000001", "101", "T", lat_deg="43", lat_min="13", lat_sec="54.0",
                                            lat_dir="N", lon_deg="77", lon_min="52", lon_sec=".0", lon_dir="W")]),
             "source_coordinate_key_duplication_drift"),
            ("latitude direction drift", lambda d: (d[0], d[1], [d[2][0].replace("|N|", "|X|", 1), *d[2][1:]]),
             "source_coordinate_direction_drift"),
            ("longitude direction drift", lambda d: (d[0], d[1], [d[2][1], d[2][0].replace("|W|", "|Q|", 1), *d[2][2:]]),
             "source_coordinate_direction_drift"),
            ("coordinate out of range", lambda d: (d[0], d[1], [d[2][0].replace("|43|", "|99|", 1), *d[2][1:]]),
             "source_coordinate_out_of_range"),
            ("component invalid", lambda d: (d[0], d[1], [d[2][0].replace("|54.0|", "|54a0|", 1), *d[2][1:]]),
             "source_coordinate_component_invalid"),
            ("all rows null geometry", lambda d: (d[0], d[1], [
                _co("1000001", "101", "T"), _co("1000002", "102", "T"), _co("1000004", "104", "T"),
                _co("1000005", "105", "T"), _co("1000005", "105", "A", array_position="2", array_total="3"),
                _co("1000006", "106", "T")]),
             "source_published_geometry_empty"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                ra, en, co = mutate(_documents())
                with self.assertRaisesRegex(ValueError, expected):
                    self._validate(ra, en, co)

    def test_counts_witness_mismatches_fail_closed(self):
        ra, en, co = _documents()
        # The payload's own counts member disagrees with the rows the file
        # actually holds for a table: blocking completeness fails.
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:RA"):
            self._validate(ra, en, co, counts={"RA": 5})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:CO"):
            self._validate(ra, en, co, counts={"CO": 5})
        with self.assertRaisesRegex(ValueError, "source_record_population_drift:EN"):
            self._validate(ra, en, co, counts={"EN": 6})
        # A counts member missing a decoded table's line cannot witness completeness.
        with self.assertRaisesRegex(ValueError, "source_counts_witness_missing:EN"):
            self._validate(ra, en, co, counts_bytes=(
                b"File Creation Date: Sun Nov  8 06:21:02 EST 2026\r\n"
                b"      6 /home/pubacc/scripts/towweekzipdata/RA.dat\r\n"
                b"      6 /home/pubacc/scripts/towweekzipdata/CO.dat\r\n"))
        with self.assertRaisesRegex(ValueError, "source_counts_witness_missing:RA,CO,EN"):
            self._validate(ra, en, co,
                           counts_bytes=b"File Creation Date: Sun Nov  8 06:21:02 EST 2026\r\n")
        # A missing creation date stamp is still a format drift.
        with self.assertRaisesRegex(ValueError, "source_creation_date_missing"):
            self._validate(ra, en, co, counts_bytes=(
                b"      6 /home/pubacc/scripts/towweekzipdata/RA.dat\r\n"
                b"      6 /home/pubacc/scripts/towweekzipdata/CO.dat\r\n"
                b"      5 /home/pubacc/scripts/towweekzipdata/EN.dat\r\n"))

    def test_upstream_edition_with_added_registration_witnesses_and_warns(self):
        ra, en, co = _documents()
        _base_records, base_audit = self._validate(ra, en, co)
        # The publisher's next weekly edition adds one registration with its
        # owner entity and one tower coordinate; the counts member and the
        # archive's own creation stamp in the same file witness it.
        ra2 = [*ra, _ra("1000007", "107", status="A", archive="C", state="OR")]
        en2 = [*en, _en("1000007", "107", "O", name="PACIFIC TOWER LLC")]
        co2 = [*co, _co("1000007", "107", "T", lat_deg="44", lat_min="55", lat_sec="30.0", lat_dir="N",
                        lat_total="161730.0", lon_deg="124", lon_min="3", lon_sec="42.0", lon_dir="W",
                        lon_total="447822.0")]
        records, audit = self._validate(ra2, en2, co2, creation_date="Sun Nov 15 06:19:55 EST 2026")
        self.assertEqual(audit["source_feature_count"], 7)
        self.assertEqual(audit["source_counts_witness"], {"RA": 7, "CO": 7, "EN": 6})
        self.assertEqual(audit["source_ra_record_count"], 7)
        self.assertEqual(audit["source_registration_status_counts"],
                         {"A": 2, "C": 2, "G": 1, "T": 1, "I": 1})
        self.assertEqual(audit["source_owner_entity_rows"], 4)
        self.assertEqual(audit["source_registrations_without_entity"], 3)
        self.assertEqual(audit["source_file_creation_date"], "Sun Nov 15 06:19:55 EST 2026")
        keys = [(_row(record, "ra_registration_number"), _row(record, "ra_unique_system_identifier"),
                 _row(record, "co_coordinate_type")) for record in records]
        self.assertEqual(keys[-1], ("1000007", "107", "T"))
        # The composition drift is recorded, not refused.
        self.assertNotEqual(audit["source_id_sha256"], base_audit["source_id_sha256"])
        self.assertNotEqual(audit["source_state_roster_sha256"], base_audit["source_state_roster_sha256"])
        self.assertNotEqual(audit["source_record_sha256"], base_audit["source_record_sha256"])
        candidate = {"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
                     "feature_count": audit["source_feature_count"],
                     "source_reported_count": audit["source_feature_count"],
                     "source_population_complete": True,
                     "source_id_sha256": audit["source_id_sha256"]}
        self.assertTrue(antenna_structure_registration_asr_complete(candidate).passed)
        drift = antenna_structure_registration_asr_drift(candidate)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(list(antenna_structure_registration_asr_drift.check_specs)[0].blocking)
        baseline = {**candidate, "version": "v-approved", "release_accepted": True,
                    "feature_count": base_audit["source_feature_count"],
                    "source_id_sha256": base_audit["source_id_sha256"]}
        compared = approved_baseline_drift(candidate, baseline)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.severity, AssetCheckSeverity.WARN)
        self.assertEqual(compared.metadata["comparison_status"].value, "compared_approved_release")
        self.assertEqual(compared.metadata["feature_count_delta"].value, 1)
        self.assertTrue(compared.metadata["native_id_roster_changed"].value)

    def test_null_geometry_rows_published_with_null_geometry_and_counted(self):
        ra, en, co = _documents()
        # An edition where three coordinate rows lack parsable degrees and
        # directions: the rows stay published with null geometry (excluded
        # from the geometry roster, digests and bounds), and the audit counts
        # them at a different count than the retired edition snapshot.
        co2 = [co[0],
               _co("1000002", "102", "T"),
               co[2],
               _co("1000004", "104", "A"),
               co[4], co[5]]
        records, audit = self._validate(ra, en, co2)
        self.assertEqual(audit["source_null_geometry_count"], 3)
        self.assertEqual(audit["source_feature_count"], 6)
        null_rows = [record for record in records if _row(record, "latdec") is None]
        self.assertEqual(len(null_rows), 3)
        null_keys = [(_row(record, "ra_registration_number"), _row(record, "co_coordinate_type"))
                     for record in null_rows]
        self.assertEqual(null_keys, [("1000002", "T"), ("1000004", "A"), ("1000004", "T")])
        for record in null_rows:
            self.assertIsNone(_row(record, "latdec"))
            self.assertIsNone(_row(record, "londec"))
        # The null-geometry rows stay out of the geometry roster, digests and
        # the observed bounds.
        self.assertEqual(audit["source_bounds"], [-158.0, 21.3083333333, -77.8666666667, 61.225])
        self.assertEqual(audit["source_array_rows_with_position"], 1)
        self.assertEqual(audit["source_array_rows_without_position"], 1)

    def test_zip_container_and_capture_receipts_fail_closed(self):
        ra, en, co = _documents()
        body = _members(ra, en, co)
        with self.assertRaisesRegex(ValueError, "source_zip_member_roster_drift"):
            self._validate(ra, en, co, extra=["extra.dat"])
        with self.assertRaisesRegex(ValueError, "source_zip_unreadable"):
            source._validate(_capture(b"not a zip archive"))
        bad_status = {"r_tower": (body, {"method": "GET", "url": source.R_TOWER_URL,
                                         "final_url": source.R_TOWER_URL, "status": 500,
                                         "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "source_capture_status_length_hash_or_completeness_drift"):
            source._validate(bad_status)
        incomplete = {"r_tower": (body, {"method": "GET", "url": source.R_TOWER_URL,
                                         "final_url": source.R_TOWER_URL, "status": 200,
                                         "complete_body": False, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "source_capture_status_length_hash_or_completeness_drift"):
            source._validate(incomplete)
        redirect = {"r_tower": (body, {"method": "GET", "url": source.R_TOWER_URL,
                                       "final_url": "https://elsewhere.example/r_tower.zip", "status": 200,
                                       "complete_body": True, "bytes": len(body), "sha256": _sha(body)})}
        with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
            source._validate(redirect)
        stray_cr_co = [d_co.replace("|1000001|", "|1000001\r|", 1) for d_co in co]
        with self.assertRaisesRegex(ValueError, "source_record_line_invalid:CO"):
            self._validate(ra, en, stray_cr_co)

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_antenna_structure_registration_asr(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

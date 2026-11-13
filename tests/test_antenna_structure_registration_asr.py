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
from dagster_hifld.assets.antenna_structure_registration_asr import (
    antenna_structure_registration_asr, antenna_structure_registration_asr_complete,
    antenna_structure_registration_asr_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import antenna_structure_registration_asr as source
from dagster_hifld.sources.epa_frs_common import _canonical


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


_COUNTS = (b"File Creation Date: Sun Nov  8 06:21:02 EST 2026\r\n"
           b"      6 /home/pubacc/scripts/towweekzipdata/RA.dat\r\n"
           b"      6 /home/pubacc/scripts/towweekzipdata/CO.dat\r\n"
           b"      5 /home/pubacc/scripts/towweekzipdata/EN.dat\r\n")


def _members(ra, en, co, counts=_COUNTS, extra=()):
    content = {}
    for name in source.ZIP_MEMBER_ROSTER:
        if name == "RA.dat":
            content[name] = ("\r\n".join(ra) + "\r\n").encode()
        elif name == "EN.dat":
            content[name] = ("\r\n".join(en) + "\r\n").encode()
        elif name == "CO.dat":
            content[name] = ("\r\n".join(co) + "\r\n").encode()
        elif name == "counts":
            content[name] = counts
        else:
            content[name] = b""
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

    def _pins(self, ra, en, co):
        records = self._reference_records(ra, en, co)
        order = {name: index for index, name in enumerate(source.RECORD_VALUE_NAMES)}
        roster = [[record[order[name]] for name in source.ROW_KEY_COLUMNS] for record in records]
        geometry = [[record[order["ra_registration_number"]], record[order["ra_unique_system_identifier"]],
                     record[order["co_coordinate_type"]], record[order["londec"]], record[order["latdec"]]]
                    for record in records if record[order["londec"]] is not None]
        coordinates = [(record[order["londec"]], record[order["latdec"]])
                       for record in records if record[order["londec"]] is not None]
        bounds = ([min(x for x, _y in coordinates), min(y for _x, y in coordinates),
                   max(x for x, _y in coordinates), max(y for _x, y in coordinates)]
                  if coordinates else None)
        states = sorted({r.split("|")[25].strip() for r in ra if r.split("|")[25].strip()})
        status, archive, coord_types, lat_dirs, lon_dirs = {}, {}, {}, {}, {}
        for r in ra:
            fields = r.split("|")
            status[fields[8]] = status.get(fields[8], 0) + 1
            archive[fields[15]] = archive.get(fields[15], 0) + 1
        for c in co:
            fields = c.split("|")
            coord_types[fields[5]] = coord_types.get(fields[5], 0) + 1
            lat_dirs[fields[9]] = lat_dirs.get(fields[9], 0) + 1
            lon_dirs[fields[14]] = lon_dirs.get(fields[14], 0) + 1
        owner_rows = sum(1 for e in en if e.split("|")[5] == "O")
        registrant_rows = sum(1 for e in en if e.split("|")[5] == "R")
        keys_with_owner = {(e.split("|")[3], e.split("|")[4]) for e in en if e.split("|")[5] == "O"}
        return {
            "EXPECTED_RA_RECORDS": len(ra), "EXPECTED_CO_RECORDS": len(co),
            "EXPECTED_EN_RECORDS": len(en), "EXPECTED_CREATION_DATE": "Sun Nov  8 06:21:02 EST 2026",
            "EXPECTED_STATUS_COUNTS": status, "EXPECTED_ARCHIVE_COUNTS": archive,
            "EXPECTED_COORDINATE_TYPE_COUNTS": coord_types,
            "EXPECTED_LATITUDE_DIRECTIONS": lat_dirs, "EXPECTED_LONGITUDE_DIRECTIONS": lon_dirs,
            "EXPECTED_OWNER_ENTITY_ROWS": owner_rows,
            "EXPECTED_REGISTRANT_ENTITY_ROWS": registrant_rows,
            "EXPECTED_REGISTRATIONS_WITHOUT_ENTITY": len(ra) - owner_rows,
            "EXPECTED_ROWS_WITHOUT_OWNER_ENTITY": sum(1 for c in co
                                                      if (c.split("|")[3], c.split("|")[4]) not in keys_with_owner),
            "EXPECTED_ARRAY_ROWS_WITH_POSITION": sum(1 for c in co
                                                     if c.split("|")[5] == "A" and c.split("|")[16].strip()),
            "EXPECTED_PUBLISHED_ROWS": len(co), "EXPECTED_NULL_GEOMETRY_COUNT":
                sum(1 for c in co if not c.split("|")[6] or not c.split("|")[11]),
            "EXPECTED_ID_SHA256": _sha(_canonical(roster)),
            "EXPECTED_GEOMETRY_SHA256": _sha(_canonical(geometry)),
            "EXPECTED_ROWS_SHA256": _sha(_canonical(records)),
            "EXPECTED_BOUNDS": bounds,
            "EXPECTED_STATE_ROSTER_SHA256": _sha(_canonical(states)),
        }

    def _validate(self, ra, en, co, pins=None, counts=_COUNTS, extra=()):
        patches = [mock.patch.object(source, name, value)
                   for name, value in (pins or self._pins(ra, en, co)).items()]
        for patch in patches:
            patch.start()
        try:
            return source._validate(_capture(_members(ra, en, co, counts=counts, extra=extra)))
        finally:
            for patch in patches:
                patch.stop()

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(antenna_structure_registration_asr, hifld_ingest_assets)
        self.assertEqual(antenna_structure_registration_asr_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = antenna_structure_registration_asr_complete({
            "feature_count": source.EXPECTED_PUBLISHED_ROWS, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = antenna_structure_registration_asr_complete({
            "feature_count": source.EXPECTED_PUBLISHED_ROWS + 1, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_complete_native_population_and_published_contract(self):
        ra, en, co = _documents()
        records, audit = self._validate(ra, en, co)
        keys = [(_row(record, "ra_registration_number"), _row(record, "ra_unique_system_identifier"),
                 _row(record, "co_coordinate_type")) for record in records]
        self.assertEqual(keys, [("1000001", "101", "T"), ("1000002", "102", "T"),
                                ("1000004", "104", "T"), ("1000005", "105", "A"),
                                ("1000005", "105", "T"), ("1000006", "106", "T")])
        self.assertEqual(audit["source_feature_count"], 6)
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
            ("record totals", lambda d: (d[0][:5], d[1], d[2]), "source_record_population_drift"),
            ("counts file population", lambda d: (d[0], d[1], d[2]), "source_counts_file_population_drift:EN"),
            ("duplicate registration number", lambda d: (
                [d[0][0], _ra("1000001", "102", status="C", archive="A", state="PR"), *d[0][2:]], d[1], d[2]),
             "source_duplicate_registration_number"),
            ("duplicate unique system identifier", lambda d: (
                [d[0][0], d[0][1], _ra("1000003", "101"), *d[0][3:]], d[1], d[2]),
             "source_duplicate_unique_system_identifier"),
            ("status distribution", lambda d: ([_ra("1000001", "101", status="X"), *d[0][1:]], d[1], d[2]),
             "source_registration_status_distribution_drift"),
            ("state roster", lambda d: ([d[0][0].replace("|AL|", "|ZZ|"), *d[0][1:]], d[1], d[2]),
             "source_state_roster_drift"),
            ("owner entity duplication", lambda d: (d[0], [d[1][0], _en("1000001", "101", "O"), *d[1][2:]], d[2]),
             "source_duplicate_owner_entity_row"),
            ("owner entity population", lambda d: (d[0], [d[1][0], d[1][1], _en("1000002", "102", "R"), *d[1][3:]], d[2]),
             "source_owner_entity_population_drift"),
            ("registrant entity population", lambda d: (d[0], [d[1][0], _en("1000001", "101", "X"), *d[1][2:]], d[2]),
             "source_registrant_entity_population_drift"),
            ("coordinate join", lambda d: (d[0], d[1], [d[2][0].replace("1000001", "1999999", 1), *d[2][1:]]),
             "source_coordinate_join_drift"),
            ("duplicate coordinate key", lambda d: (d[0], d[1], [*d[2][:5], _co("1000001", "101", "T", lat_deg="43", lat_min="13", lat_sec="54.0", lat_dir="N", lon_deg="77", lon_min="52", lon_sec=".0", lon_dir="W")]),
             "source_coordinate_key_duplication_drift"),
            ("coordinate type distribution", lambda d: (d[0], d[1], [_co("1000001", "101", "A", lat_deg="43", lat_min="13", lat_sec="54.0", lat_dir="N", lat_total="155634.0", lon_deg="77", lon_min="52", lon_sec=".0", lon_dir="W", lon_total="280320.0"), *d[2][1:]]),
             "source_coordinate_type_distribution_drift"),
            ("latitude direction drift", lambda d: (d[0], d[1], [d[2][0].replace("|N|", "|X|", 1), *d[2][1:]]),
             "source_coordinate_direction_drift"),
            ("longitude direction drift", lambda d: (d[0], d[1], [d[2][1], d[2][0].replace("|W|", "|Q|", 1), *d[2][2:]]),
             "source_coordinate_direction_drift"),
            ("array position population", lambda d: (
                d[0], d[1], [*d[2][:4], d[2][5], _co("1000005", "105", "A", lat_deg="61", lat_min="13",
                                            lat_sec="30.0", lat_dir="N", lat_total="220410.0",
                                            lon_deg="149", lon_min="53", lon_sec="8.0", lon_dir="W",
                                            lon_total="539588.0", array_position="", array_total="3")]),
             "source_array_position_population_drift"),
            ("coordinate out of range", lambda d: (d[0], d[1], [d[2][0].replace("|43|", "|99|", 1), *d[2][1:]]),
             "source_coordinate_out_of_range"),
            ("component invalid", lambda d: (d[0], d[1], [d[2][0].replace("|54.0|", "|54a0|", 1), *d[2][1:]]),
             "source_coordinate_component_invalid"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                pins = self._pins(*_documents())
                ra, en, co = mutate(_documents())
                counts = _COUNTS.replace(b"      5 /home/pubacc/scripts/towweekzipdata/EN.dat",
                                         b"      9 /home/pubacc/scripts/towweekzipdata/EN.dat") \
                    if label == "counts file population" else _COUNTS
                with self.assertRaisesRegex(ValueError, expected):
                    self._validate(ra, en, co, pins, counts=counts)

    def test_population_pins_fail_closed(self):
        ra, en, co = _documents()
        base = self._pins(ra, en, co)
        pins = dict(base)
        pins["EXPECTED_CREATION_DATE"] = "Thu Jan  1 00:00:00 EST 1970"
        with self.assertRaisesRegex(ValueError, "source_creation_date_drift"):
            self._validate(ra, en, co, pins)
        for name, expected in (
            ("EXPECTED_RA_RECORDS", "source_record_population_drift"),
            ("EXPECTED_CO_RECORDS", "source_record_population_drift"),
            ("EXPECTED_EN_RECORDS", "source_record_population_drift"),
            ("EXPECTED_STATUS_COUNTS", "source_registration_status_distribution_drift"),
            ("EXPECTED_ARCHIVE_COUNTS", "source_archive_flag_distribution_drift"),
            ("EXPECTED_COORDINATE_TYPE_COUNTS", "source_coordinate_type_distribution_drift"),
            ("EXPECTED_LATITUDE_DIRECTIONS", "source_latitude_direction_distribution_drift"),
            ("EXPECTED_LONGITUDE_DIRECTIONS", "source_longitude_direction_distribution_drift"),
            ("EXPECTED_OWNER_ENTITY_ROWS", "source_owner_entity_population_drift"),
            ("EXPECTED_REGISTRANT_ENTITY_ROWS", "source_registrant_entity_population_drift"),
            ("EXPECTED_REGISTRATIONS_WITHOUT_ENTITY", "source_registration_without_entity_drift"),
            ("EXPECTED_ROWS_WITHOUT_OWNER_ENTITY", "source_rows_without_owner_entity_drift"),
            ("EXPECTED_ARRAY_ROWS_WITH_POSITION", "source_array_position_population_drift"),
            ("EXPECTED_PUBLISHED_ROWS", "source_published_population_drift"),
            ("EXPECTED_NULL_GEOMETRY_COUNT", "source_null_geometry_count_drift"),
            ("EXPECTED_ID_SHA256", "source_id_roster_drift"),
            ("EXPECTED_GEOMETRY_SHA256", "source_geometry_roster_drift"),
            ("EXPECTED_ROWS_SHA256", "source_record_values_drift"),
            ("EXPECTED_BOUNDS", "source_bounds_drift"),
            ("EXPECTED_STATE_ROSTER_SHA256", "source_state_roster_drift"),
        ):
            with self.subTest(pin=name):
                pins = dict(base)
                pins[name] = ([0, 0, 0, 0] if name == "EXPECTED_BOUNDS" else
                              ({} if name.endswith(("_COUNTS", "_DIRECTIONS")) else
                               ("0" * 64 if name.endswith("_SHA256") else 999999)))
                with self.assertRaisesRegex(ValueError, expected):
                    self._validate(ra, en, co, pins)

    def test_zip_container_and_capture_receipts_fail_closed(self):
        ra, en, co = _documents()
        body = _members(ra, en, co)
        pins = self._pins(ra, en, co)
        with self.assertRaisesRegex(ValueError, "source_zip_member_roster_drift"):
            self._validate(ra, en, co, pins, extra=["extra.dat"])
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
            self._validate(ra, en, stray_cr_co, pins)
        with self.assertRaisesRegex(ValueError, "source_counts_file_population_drift:RA"):
            self._validate(ra, en, co, pins,
                           counts=b"File Creation Date: Sun Nov  8 06:21:02 EST 2026\r\n")

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_antenna_structure_registration_asr(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()

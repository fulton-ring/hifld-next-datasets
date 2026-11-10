"""Focused contract checks for the USGS GNIS Antarctica source adapter."""
from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import struct
import unittest
import zipfile
from unittest.mock import patch

import pyarrow.parquet as pq

from dagster_hifld.sources import gnis_antarctica as source

OBSERVED_AT = "2026-11-09T00:00:00+00:00"
PUBDATE = "20261102"
S3_PREFIX = "StagedProducts/GeographicNames/Antarctica/"

# DATA_COLUMNS order: feature_id, antarctica_id, scar_placeid, feature_name,
# feature_class, date_created, date_edited, bgn_type, bgn_authority, bgn_date,
# prim_lat_dms, prim_long_dms, prim_lat_dec, prim_long_dec, description, history,
# bgn_type_desc, bgn_authority_desc.
NAMES_ROWS = [
    (2814090, 1, 101.5, "Aagaard Glacier", "Glacier", "1979-01-01T00:00:00", None, 2, 1,
     "1979-01-01T00:00:00", "742730S", "0663030W", -74.5, 66.5,
     "Aagaard Glacier description", "history line one\nline two", "Official", "Board Decision"),
    (2814091, None, None, "Aircraft Glacier", "Glacier", None, None, None, None, None,
     "743000S", "0663100W", -74.5, 66.55, None, None, "Official", "Board Decision"),
    (2814092, 2, 102.5, "Brien Glacier", "Glacier", "1980-02-02T00:00:00", "1990-03-03T00:00:00",
     None, None, None, "743030S", "0663130W", -74.55, 66.6, "Brien description", None,
     "Official", "Board Decision"),
]
# (prim_lat_dec, prim_long_dec) per row; native Point Z geometry matches exactly.
NAMES_COORDS = [(-74.5, 66.5), (-74.5, 66.55), (-74.55, 66.6)]

# ALLNAMES_COLUMNS order: feature_id, feature_name, feature_name_official,
# sourcecitationabbreviation, sourceoriginator, sourcereftype, title, edition,
# sourceurl, publicationdate, seriesname, seriesissue, ending_date, date_created,
# citation, feature_name_official_desc, sourcereftype_desc.
ALLNAMES_ROWS = [
    (2814090, "Aagaard Glacier", 1, None, "USGS", 4, "ACAN decisions", None, None, None,
     None, None, None, None, "cite-a", "Official", "Text"),
    (2814090, "Aagard Fiord", 2, None, "USGS", 4, "Variant doc", None, None, None,
     None, None, None, None, "cite-b", "Not Official", "Text"),
    (2814091, "Aircraft Glacier", 1, None, "USGS", 4, "ACAN decisions", None, None, None,
     None, None, None, None, "cite-c", "Official", "Text"),
    (2814092, "Brien Glacier", 1, None, "USGS", 4, "ACAN decisions", None, None, None,
     None, None, None, None, "cite-d", "Official", "Text"),
]

BGN_TYPE_TEXT = {2: "Official"}
BGN_AUTHORITY_TEXT = {1: "Board Decision"}


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def _gpkg_blob(lon: float, lat: float, srs: int = 4326) -> bytes:
    header = b"GP\x00\x01" + struct.pack("<i", srs)
    wkb = b"\x01" + struct.pack("<I", 1001) + struct.pack("<ddd", lon, lat, 0.0)
    return header + wkb


def _fgdc_xml(suffix: str, pubdate: str = PUBDATE) -> bytes:
    return (f"<?xml version=\"1.0\"?><metadata><idinfo><citation><citeinfo>"
            f"<pubdate>{pubdate}</pubdate>"
            f"<origin>U.S. Geological Survey, National Geospatial Program</origin>"
            f"<title>Geographic Names Information System (GNIS) Full Model Antarctica "
            f"(published {pubdate}) {suffix}</title>"
            f"</citeinfo></citation></idinfo></metadata>").encode()


def _zip_bytes(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in members.items():
            archive.writestr(name, body)
    return buffer.getvalue()


def _pipe_document(header, rows) -> bytes:
    import csv
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, delimiter="|", quotechar='"', lineterminator="\r\n")
    writer.writerow(header)
    writer.writerows(rows)
    return b"\xef\xbb\xbf" + buffer.getvalue().encode("utf-8")


def _iso_to_text(value):
    return "" if value is None else f"{value[5:7]}/{value[8:10]}/{value[0:4]}"


def _names_text_rows(rows):
    converted = []
    for values in rows:
        converted.append([
            str(values[0]),
            "" if values[1] is None else str(values[1]),
            "" if values[2] is None else str(values[2]),
            values[3], values[4],
            _iso_to_text(values[5]), _iso_to_text(values[6]),
            "" if values[7] is None else BGN_TYPE_TEXT[values[7]],
            "" if values[8] is None else BGN_AUTHORITY_TEXT[values[8]],
            _iso_to_text(values[9]),
            values[10], values[11],
            str(values[12]), str(values[13]),
            values[14] or "", values[15] or "",
        ])
    return converted


def _allnames_text_rows(rows):
    return [[row[0], row[1], row[15], "" if row[3] is None else str(row[3]), row[4],
             str(row[5]), row[6], "" if row[7] is None else str(row[7]),
             "" if row[8] is None else str(row[8]), "" if row[9] is None else str(row[9]),
             "" if row[10] is None else str(row[10]), "" if row[11] is None else str(row[11]),
             "" if row[12] is None else str(row[12]), "" if row[13] is None else str(row[13]),
             row[14] or ""] for row in rows]


def _build_gpkg(names_rows, allnames_rows, coords=NAMES_COORDS) -> bytes:
    connection = sqlite3.connect(":memory:")
    cursor = connection.cursor()
    cursor.execute(
        f"CREATE TABLE {source.NAMES_TABLE} (fid INTEGER PRIMARY KEY AUTOINCREMENT, "
        "feature_id MEDIUMINT, antarctica_id MEDIUMINT, scar_placeid REAL, "
        "feature_name TEXT(120), feature_class TEXT(50), date_created DATETIME, "
        "date_edited DATETIME, bgn_type MEDIUMINT, bgn_authority MEDIUMINT, "
        "bgn_date DATETIME, prim_lat_dms TEXT(50), prim_long_dms TEXT(50), "
        "prim_lat_dec REAL, prim_long_dec REAL, description TEXT(3000), "
        "history TEXT(3000), bgn_type_desc TEXT(50), bgn_authority_desc TEXT(50), "
        "Shape MULTIPOINT)")
    cursor.execute(
        f"CREATE TABLE {source.ALLNAMES_TABLE} (fid INTEGER PRIMARY KEY AUTOINCREMENT, "
        "feature_id MEDIUMINT, feature_name TEXT(120), feature_name_official MEDIUMINT, "
        "sourcecitationabbreviation TEXT(100), sourceoriginator TEXT(120), "
        "sourcereftype MEDIUMINT, title TEXT(300), edition TEXT(50), sourceurl TEXT(300), "
        "publicationdate DATETIME, seriesname TEXT(100), seriesissue TEXT(50), "
        "ending_date DATETIME, date_created DATETIME, citation TEXT(300), "
        "feature_name_official_desc TEXT(50), sourcereftype_desc TEXT(50))")
    cursor.execute("CREATE TABLE gpkg_contents (table_name TEXT, data_type TEXT, srs_id MEDIUMINT)")
    cursor.execute("CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT, "
                   "geometry_type_name TEXT, srs_id MEDIUMINT)")
    cursor.execute("CREATE TABLE gpkg_data_columns (table_name TEXT, column_name TEXT, "
                   "constraint_name TEXT)")
    cursor.execute("CREATE TABLE gpkg_data_column_constraints (constraint_name TEXT, "
                   "constraint_type TEXT, value TEXT, description TEXT)")
    cursor.executemany("INSERT INTO gpkg_contents VALUES (?,?,?)", [
        (source.NAMES_TABLE, "features", 4326), (source.ALLNAMES_TABLE, "attributes", 0)])
    cursor.execute("INSERT INTO gpkg_geometry_columns VALUES (?,?,?,?)",
                   (source.NAMES_TABLE, "Shape", "MULTIPOINT", 4326))
    cursor.executemany("INSERT INTO gpkg_data_columns VALUES (?,?,?)", [
        (source.NAMES_TABLE, "bgn_type", "BGNDecisionType Domain"),
        (source.NAMES_TABLE, "bgn_authority", "BGNDecisionAuthority Domain"),
        (source.ALLNAMES_TABLE, "feature_name_official", "NameStatus Domain"),
        (source.ALLNAMES_TABLE, "sourcereftype", "SourceRefType Domain")])
    cursor.executemany("INSERT INTO gpkg_data_column_constraints VALUES (?,?,?,?)", [
        ("BGNDecisionType Domain", "enum", "2", "Official"),
        ("BGNDecisionAuthority Domain", "enum", "1", "Board Decision"),
        ("NameStatus Domain", "enum", "1", "Official"),
        ("NameStatus Domain", "enum", "2", "Not Official"),
        ("SourceRefType Domain", "enum", "4", "Text")])
    for values, (lat, lon) in zip(names_rows, coords):
        cursor.execute(
            f"INSERT INTO {source.NAMES_TABLE} ({','.join(source.DATA_COLUMNS)}, Shape) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (*values, _gpkg_blob(lon, lat)))
    for row in allnames_rows:
        cursor.execute(
            f"INSERT INTO {source.ALLNAMES_TABLE} ({','.join(source.ALLNAMES_COLUMNS)}) "
            "VALUES (" + ",".join("?" * len(source.ALLNAMES_COLUMNS)) + ")", row)
    connection.commit()
    return connection.serialize()


def _listing_xml(sizes: dict[str, int]) -> bytes:
    parts = ['<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>']
    for key, size in sizes.items():
        parts.append(f"<Contents><Key>{key}</Key><Size>{size}</Size>"
                     "<LastModified>2026-11-02T20:37:44.000Z</LastModified>"
                     "<ETag>fixture-etag</ETag></Contents>")
    parts.append("</ListBucketResult>")
    return "".join(parts).encode()


def _receipt(name, body):
    url = source.CAPTURE_URLS[name]
    digest = hashlib.sha256(body).hexdigest()
    return {"method": "GET", "url": url, "final_url": url, "observed_at": OBSERVED_AT,
            "status": 200, "complete_body": True, "wire_bytes": len(body),
            "wire_sha256": digest, "bytes": len(body), "sha256": digest,
            "content_encoding": None, "content_length": len(body), "error_code": None,
            "error_type": None}


def _build_captures(gpkg_body, names_rows=NAMES_ROWS, allnames_rows=ALLNAMES_ROWS,
                    text_names_rows=None, download_page=None, gpkg_members=None,
                    s3_after=None, pubdate=PUBDATE):
    names_text = _pipe_document(list(source.DATA_COLUMNS[:16]),
                                text_names_rows or _names_text_rows(names_rows))
    allnames_text = _pipe_document(list(source.ALLNAMES_COLUMNS[:15]),
                                   _allnames_text_rows(allnames_rows))
    gpkg_zip = _zip_bytes(gpkg_members or {
        "Gazetteer_Antarctica_GPKG.gpkg": gpkg_body,
        "Gazetteer_Antarctica_GPKG.jpg": b"jpg-bytes",
        "Gazetteer_Antarctica_GPKG.xml": _fgdc_xml("GeoPackage", pubdate)})
    text_zip = _zip_bytes({
        "Gazetteer_Antarctica_Text.jpg": b"jpg-bytes",
        "Gazetteer_Antarctica_Text.xml": _fgdc_xml("pipes", pubdate),
        "Text/AntarcticaNames_Antarctica.txt": names_text,
        "Text/AntarcticaAllNames_Antarctica.txt": allnames_text})
    sizes = {S3_PREFIX + name: 1000 for name in
             ("Gazetteer_Antarctica_GDB.jpg", "Gazetteer_Antarctica_GDB.xml",
              "Gazetteer_Antarctica_GDB.zip", "Gazetteer_Antarctica_GPKG.jpg",
              "Gazetteer_Antarctica_GPKG.xml", "Gazetteer_Antarctica_Text.jpg",
              "Gazetteer_Antarctica_Text.xml")}
    sizes[S3_PREFIX + "Gazetteer_Antarctica_GPKG.zip"] = len(gpkg_zip)
    sizes[S3_PREFIX + "Gazetteer_Antarctica_Text.zip"] = len(text_zip)
    listing = _listing_xml({S3_PREFIX: 0, **sizes})
    bodies = {
        "s3_before": listing,
        "download_page": download_page or (b"<html>GNIS download page: Antarctica names at "
                                           + S3_PREFIX.encode() + b" and GNIS_file_format.pdf</html>"),
        "antarctic_names_page": b"<html>Advisory Committee on Antarctic Names approves names at "
                                 + S3_PREFIX.encode() + b"</html>",
        "file_format_pdf": b"%PDF-1.7 fixture format document",
        "gpkg_zip": gpkg_zip,
        "text_zip": text_zip,
        "s3_after": s3_after if s3_after is not None else listing,
    }
    return {name: (body, _receipt(name, body)) for name, body in bodies.items()}


def _schema_sha(gpkg_body):
    connection = sqlite3.connect(":memory:")
    connection.deserialize(gpkg_body)
    try:
        return source._sha(source._canonical(source._gpkg_schema(connection.cursor())))
    finally:
        connection.close()


def _pins(names_rows=NAMES_ROWS, allnames_rows=ALLNAMES_ROWS, gpkg_body=None,
          pubdate=PUBDATE):
    gpkg_body = gpkg_body if gpkg_body is not None else _build_gpkg(names_rows, allnames_rows)
    records, geometry_records, bounds = [], [], None
    null_counts = dict.fromkeys(source.DATA_COLUMNS, 0)
    seen = set()
    for values, (lat, lon) in zip(sorted(names_rows, key=lambda row: row[0]), NAMES_COORDS):
        blob = _gpkg_blob(lon, lat)
        wkb = blob[8:].hex()
        records.append([list(values), wkb])
        geometry_records.append([values[0], wkb])
        seen.add(values[0])
        for index, column in enumerate(source.DATA_COLUMNS):
            if values[index] is None:
                null_counts[column] += 1
        x, y = lon, lat
        bounds = [x, y, x, y] if bounds is None else [
            min(bounds[0], x), min(bounds[1], y), max(bounds[2], x), max(bounds[3], y)]
    allnames_records = sorted([list(row) for row in allnames_rows],
                              key=lambda row: (row[0], source._canonical(row[1:]).decode()))
    allnames_nulls = dict.fromkeys(source.ALLNAMES_COLUMNS, 0)
    for row in allnames_rows:
        for index, column in enumerate(source.ALLNAMES_COLUMNS):
            if row[index] is None:
                allnames_nulls[column] += 1
    fixture_keys = sorted([S3_PREFIX] + [S3_PREFIX + name for name in (
        "Gazetteer_Antarctica_GDB.jpg", "Gazetteer_Antarctica_GDB.xml",
        "Gazetteer_Antarctica_GDB.zip", "Gazetteer_Antarctica_GPKG.jpg",
        "Gazetteer_Antarctica_GPKG.xml", "Gazetteer_Antarctica_GPKG.zip",
        "Gazetteer_Antarctica_Text.jpg", "Gazetteer_Antarctica_Text.xml",
        "Gazetteer_Antarctica_Text.zip")])
    return dict(
        EXPECTED_PUBDATE=pubdate,
        EXPECTED_S3_KEYS=tuple(fixture_keys),
        EXPECTED_COUNT=len(names_rows),
        EXPECTED_ALLNAMES_COUNT=len(allnames_rows),
        EXPECTED_ID_SHA256=source._sha(source._canonical(sorted(seen))),
        EXPECTED_ALLNAMES_ID_SHA256=source._sha(source._canonical(sorted(
            row[0] for row in allnames_rows))),
        EXPECTED_RECORD_SHA256=source._list_sha(records),
        EXPECTED_ALLNAMES_RECORD_SHA256=source._list_sha(allnames_records),
        EXPECTED_GEOMETRY_SHA256=source._list_sha(geometry_records),
        EXPECTED_SCHEMA_SHA256=_schema_sha(gpkg_body),
        EXPECTED_OBSERVED_BOUNDS=bounds,
        EXPECTED_NULL_COUNTS=null_counts,
        EXPECTED_ALLNAMES_NULL_COUNTS=allnames_nulls,
    )


class GnisAntarcticaSourceTests(unittest.TestCase):
    def test_streamed_list_sha_matches_canonical_digest(self):
        items = [[1, "a"], [2, "b"], [3, None]]
        self.assertEqual(source._list_sha(items), source._sha(_canonical(items)))
        self.assertEqual(source._list_sha([]), source._sha(_canonical([])))

    def test_full_capture_seals_candidate_and_reuses_cache(self):
        import tempfile
        from pathlib import Path

        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        captures = _build_captures(gpkg_body)
        bodies = {name: body for name, (body, _) in captures.items()}

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            body = bodies[name]
            receipt = _receipt(name, body)
            (Path(capture_root) / f"{name}.body").write_bytes(body)
            (Path(capture_root) / f"{name}.wire").write_bytes(body)
            (Path(capture_root) / f"{name}.receipt.json").write_bytes(
                source._canonical(receipt) + b"\n")
            return body, receipt

        class EmptyClient:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        with tempfile.TemporaryDirectory(prefix="gnis-antarctica-e2e-") as temporary:
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True,
                                                   local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins()), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *a, **k: EmptyClient()):
                candidate = source.capture_gnis_antarctica(storage, version="vfixture",
                                                           capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 3)
                self.assertEqual(candidate["variant_name_count"], 4)
                self.assertFalse(candidate["release_accepted"])
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                for name in source.CAPTURE_NAMES:
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.body"), bodies[name])
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.wire"), bodies[name])
                staged = set(storage.list_prefix(prefix))
                self.assertIn(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet", staged)
                self.assertIn(f"{prefix}/metadata/source_manifest.json", staged)
                self.assertIn(f"{prefix}/metadata/source_audit.json", staged)
                self.assertIn(f"{prefix}/metadata/candidate_ready.json", staged)
                reused = source.capture_gnis_antarctica(storage, version="vfixture",
                                                       capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])

    def test_parquet_preserves_native_point_z_and_source_values(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        with patch.multiple(source, **_pins()):
            extracted = source._validate_captures(_build_captures(gpkg_body))
            body = source._parquet_bytes(extracted)
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(table.column("feature_id").to_pylist(), [2814090, 2814091, 2814092])
        self.assertEqual(table.column("feature_id").type.__str__(), "int32")
        self.assertEqual(table.column("scar_placeid").type.__str__(), "double")
        self.assertEqual(table.column("date_created").type.__str__(), "timestamp[ms]")
        self.assertEqual(table.column("feature_name").to_pylist()[2], "Brien Glacier")
        self.assertEqual(table.column("history").to_pylist()[0], "history line one\nline two")
        shapes = table.column("Shape").to_pylist()
        self.assertEqual(shapes, [_gpkg_blob(row[13], row[12])[8:] for row in NAMES_ROWS])
        for blob in shapes:
            self.assertEqual(blob[0:5], b"\x01" + struct.pack("<I", 1001))
            self.assertEqual(struct.unpack("<d", blob[-8:])[0], 0.0)
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["primary_column"], "Shape")
        self.assertEqual(geo["columns"]["Shape"]["geometry_types"], ["Point Z"])
        self.assertEqual(geo["columns"]["Shape"]["crs"]["id"]["code"], 4326)

    def test_schema_contract_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        connection = sqlite3.connect(":memory:")
        connection.deserialize(gpkg_body)
        connection.execute(f"ALTER TABLE {source.NAMES_TABLE} ADD COLUMN surprise TEXT")
        connection.commit()
        mutated = connection.serialize()
        connection.close()
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_captures(_build_captures(mutated))

    def test_record_value_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        connection = sqlite3.connect(":memory:")
        connection.deserialize(gpkg_body)
        connection.execute(f"UPDATE {source.NAMES_TABLE} SET feature_name = 'Brien Glacier X' "
                           "WHERE feature_id = 2814092")
        connection.commit()
        mutated = connection.serialize()
        connection.close()
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(_build_captures(mutated))

    def test_geometry_attribute_delta_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        connection = sqlite3.connect(":memory:")
        connection.deserialize(gpkg_body)
        row = connection.execute(
            f"SELECT Shape FROM {source.NAMES_TABLE} WHERE feature_id = 2814090").fetchone()
        blob = bytearray(row[0])
        x = struct.unpack("<d", bytes(blob[13:21]))[0]
        blob[13:21] = struct.pack("<d", x + 1e-6)
        connection.execute(f"UPDATE {source.NAMES_TABLE} SET Shape = ? WHERE feature_id = 2814090",
                           (bytes(blob),))
        connection.commit()
        mutated = connection.serialize()
        connection.close()
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "source_geometry_attribute_delta_drift"):
                source._validate_captures(_build_captures(mutated))

    def test_geometry_type_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        connection = sqlite3.connect(":memory:")
        connection.deserialize(gpkg_body)
        connection.execute(f"UPDATE {source.NAMES_TABLE} SET Shape = ? WHERE feature_id = 2814091",
                           (_gpkg_blob(66.55, -74.5).replace(struct.pack("<I", 1001),
                                                              struct.pack("<I", 2001)),))
        connection.commit()
        mutated = connection.serialize()
        connection.close()
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "source_geometry_type_drift"):
                source._validate_captures(_build_captures(mutated))

    def test_count_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        connection = sqlite3.connect(":memory:")
        connection.deserialize(gpkg_body)
        connection.execute(f"DELETE FROM {source.NAMES_TABLE} WHERE feature_id = 2814091")
        connection.commit()
        mutated = connection.serialize()
        connection.close()
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "source_count_drift"):
                source._validate_captures(_build_captures(mutated))

    def test_duplicate_variant_row_is_rejected(self):
        duplicated = ALLNAMES_ROWS + [ALLNAMES_ROWS[0]]
        gpkg_body = _build_gpkg(NAMES_ROWS, duplicated)
        with patch.multiple(source, **_pins(allnames_rows=duplicated)):
            with self.assertRaisesRegex(ValueError, "source_allnames_duplicate_row_drift"):
                source._validate_captures(_build_captures(gpkg_body, allnames_rows=duplicated))

    def test_allnames_roster_drift_is_rejected(self):
        reduced = [row for row in ALLNAMES_ROWS if row[0] != 2814092]
        gpkg_body = _build_gpkg(NAMES_ROWS, reduced)
        with patch.multiple(source, **_pins(allnames_rows=reduced)):
            with self.assertRaisesRegex(ValueError, "source_allnames_roster_drift"):
                source._validate_captures(_build_captures(gpkg_body, allnames_rows=reduced))

    def test_variant_multiplicity_pin_detects_id_roster_drift(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        pins = _pins()
        pins["EXPECTED_ALLNAMES_ID_SHA256"] = "0" * 64
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_allnames_id_roster_drift"):
                source._validate_captures(_build_captures(gpkg_body))

    def test_text_cross_format_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        rows = _names_text_rows(NAMES_ROWS)
        rows[2][3] = "Brien Glacier X"
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "text_field_drift"):
                source._validate_captures(_build_captures(gpkg_body, text_names_rows=rows))

    def test_publication_change_during_capture_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        captures = _build_captures(gpkg_body, s3_after=b"<ListBucketResult>changed</ListBucketResult>")
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "source_publication_changed_during_capture"):
                source._validate_captures(captures)

    def test_download_page_link_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        captures = _build_captures(gpkg_body, download_page=b"<html>unrelated</html>")
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "official_download_page_link_drift"):
                source._validate_captures(captures)

    def test_fgdc_publication_date_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        captures = _build_captures(gpkg_body, pubdate="20261101")
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "gpkg_publication_date_drift"):
                source._validate_captures(captures)

    def test_zip_member_roster_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        captures = _build_captures(gpkg_body, gpkg_members={
            "Gazetteer_Antarctica_GPKG.gpkg": gpkg_body,
            "Gazetteer_Antarctica_GPKG.jpg": b"jpg-bytes",
            "Gazetteer_Antarctica_GPKG.xml": _fgdc_xml("GeoPackage"),
            "surprise.txt": b"unexpected"})
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "gpkg_member_roster_drift"):
                source._validate_captures(captures)

    def test_incomplete_capture_is_quarantined_with_retained_evidence(self):
        import tempfile
        from pathlib import Path

        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        captures = _build_captures(gpkg_body)
        bodies = {name: body for name, (body, _) in captures.items()}
        completed = set(source.CAPTURE_NAMES[:source.CAPTURE_NAMES.index("text_zip")])

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            body = bodies[name]
            receipt = _receipt(name, body)
            if name not in completed:
                receipt = dict(receipt, status=None, complete_body=False, bytes=0,
                               sha256=hashlib.sha256(b"").hexdigest(), error_code="request_error")
                (Path(capture_root) / f"{name}.body").write_bytes(b"")
                (Path(capture_root) / f"{name}.wire").write_bytes(b"")
                (Path(capture_root) / f"{name}.receipt.json").write_bytes(
                    source._canonical(receipt) + b"\n")
                raise ValueError(f"incomplete_source_capture:{name}")
            (Path(capture_root) / f"{name}.body").write_bytes(body)
            (Path(capture_root) / f"{name}.wire").write_bytes(body)
            (Path(capture_root) / f"{name}.receipt.json").write_bytes(
                source._canonical(receipt) + b"\n")
            return body, receipt

        class EmptyClient:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        with tempfile.TemporaryDirectory(prefix="gnis-antarctica-quarantine-") as temporary:
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True,
                                                   local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins()), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *a, **k: EmptyClient()):
                with self.assertRaisesRegex(ValueError, "incomplete_source_capture:text_zip"):
                    source.capture_gnis_antarctica(storage, version="vfixture",
                                                  capture_directory=str(root / "captures"))
            quarantine = sorted(storage.list_prefix("_quarantine/antarctica"))
            self.assertEqual(len(quarantine), 1 + (len(completed) + 1) * 3)
            failure_key = next(key for key in quarantine if key.endswith("metadata/failure.json"))
            failure = json.loads(storage.read_key(failure_key))
            self.assertEqual(failure["reason_code"], "ValueError")
            self.assertEqual(failure["captured_names"], sorted(completed | {"text_zip"}))
            for name in completed:
                base = quarantine[0].rsplit("/", 2)[0]
                self.assertEqual(storage.read_key(f"{base}/raw/{name}.body"), bodies[name])

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        import os
        import tempfile

        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          f"geoparquet/{source.FILE_SLUG}.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           f"geoparquet/{source.FILE_SLUG}.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError,
                                            "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, source.DATASET_SLUG,
                                        source.FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)


class GnisAntarcticaRegistrationTests(unittest.TestCase):
    def test_registered_job_check_and_stopped_schedule(self):
        from dagster import DefaultScheduleStatus
        from dagster_hifld.assets.gnis_antarctica import (
            gnis_antarctica_draft_schedule,
            usgs_gnis_antarctica,
            usgs_gnis_antarctica_complete,
        )
        from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
        from dagster_hifld.definitions import defs

        self.assertIn(usgs_gnis_antarctica, hifld_ingest_assets)
        repo = defs.get_repository_def()
        job = repo.get_job("gnis_antarctica_draft_refresh")
        self.assertEqual(job.asset_layer.executable_asset_keys, usgs_gnis_antarctica.keys)
        self.assertTrue(set(usgs_gnis_antarctica_complete.check_keys)
                        <= defs.resolve_asset_graph().asset_check_keys)
        self.assertTrue(next(iter(usgs_gnis_antarctica_complete.check_specs)).blocking)
        schedule = repo.get_schedule_def("gnis_antarctica_draft_refresh_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.execution_timezone, "America/New_York")
        self.assertEqual(schedule.cron_schedule, "0 6 1 * *")


if __name__ == "__main__":
    unittest.main()

"""Focused contract checks for the USGS GNIS Antarctica source adapter."""
from __future__ import annotations

import hashlib
import gzip
import io
import json
import sqlite3
import struct
import unittest
import zipfile
import httpx
from unittest.mock import patch

import pyarrow.parquet as pq

from dagster_hifld.sources import gnis_antarctica as source

_REAL_HTTPX_CLIENT = httpx.Client

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
    (2814090, "Aagaard Glacier", 1, None, "USGS", 4, "ACAN decisions", None, None,
     "2022-07-19T00:00:00", None, None, None, None, "cite-a", "Official", "Text"),
    (2814090, "Aagard Fiord", 2, None, "USGS", 4, "Variant doc", None, None, None,
     None, None, None, "2023-04-18T00:00:00", "cite-b", "Not Official", "Text"),
    (2814091, "Aircraft Glacier", 1, None, "USGS", 4, "ACAN decisions", None, None, None,
     None, None, None, None, "cite-c", "Official", "Text"),
    (2814092, "Brien Glacier", 1, None, "USGS", 4, "ACAN decisions", None, None, None,
     None, None, None, None, "cite-d", "Official", "Text"),
]

BGN_TYPE_TEXT = {2: "Official"}
BGN_AUTHORITY_TEXT = {1: "Board Decision"}
SOURCE_REF_TYPE_TEXT = {4: "Text"}


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
    return [[str(row[0]), row[1], row[15], "" if row[3] is None else str(row[3]), row[4],
             "" if row[5] is None else SOURCE_REF_TYPE_TEXT[row[5]], row[6],
             "" if row[7] is None else str(row[7]), "" if row[8] is None else str(row[8]),
             _iso_to_text(row[9]),
             "" if row[10] is None else str(row[10]), "" if row[11] is None else str(row[11]),
             _iso_to_text(row[12]), _iso_to_text(row[13]), row[14] or ""] for row in rows]


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
                    text_names_rows=None, allnames_text_rows=None, download_page=None,
                    gpkg_members=None, s3_after=None, pubdate=PUBDATE):
    names_text = _pipe_document(list(source.DATA_COLUMNS[:16]),
                                text_names_rows or _names_text_rows(names_rows))
    allnames_text = _pipe_document(list(source.ALLNAMES_COLUMNS[:15]),
                                   allnames_text_rows or _allnames_text_rows(allnames_rows))
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


def _client_factory(captures, encoding="identity"):
    bodies = {source.CAPTURE_URLS[name]: body for name, (body, _) in captures.items()}

    def handler(request):
        body = bodies[str(request.url)]
        if encoding == "gzip":
            body = gzip.compress(body)
        return httpx.Response(200, headers={"Content-Encoding": encoding, "Content-Length": str(len(body))},
                              stream=httpx.ByteStream(body))

    return lambda *args, **kwargs: _REAL_HTTPX_CLIENT(transport=httpx.MockTransport(handler))


def _pins(names_rows=NAMES_ROWS, allnames_rows=ALLNAMES_ROWS, gpkg_body=None):
    gpkg_body = gpkg_body if gpkg_body is not None else _build_gpkg(names_rows, allnames_rows)
    return {"EXPECTED_SCHEMA_SHA256": _schema_sha(gpkg_body)}


class GnisAntarcticaSourceTests(unittest.TestCase):
    def test_changed_population_is_complete_when_both_producer_formats_agree(self):
        names = NAMES_ROWS[:2]
        variants = ALLNAMES_ROWS[:3]
        gpkg = _build_gpkg(names, variants)
        with patch.object(source, "EXPECTED_SCHEMA_SHA256", _schema_sha(gpkg)):
            extracted = source._validate_captures(_build_captures(gpkg, names_rows=names, allnames_rows=variants))
        self.assertEqual(extracted["audit"]["source_feature_count"], 2)
        self.assertEqual(extracted["audit"]["source_allnames_count"], 3)

    def test_additions_new_edition_and_variant_multiplicity_are_observed(self):
        added = list(NAMES_ROWS[-1])
        added[0], added[3], added[12], added[13] = 2814093, "Delta Glacier", -75.0, 67.0
        names = NAMES_ROWS + [tuple(added)]
        variant = list(ALLNAMES_ROWS[-1])
        variant[0], variant[1] = added[0], added[3]
        extra = list(ALLNAMES_ROWS[1])
        extra[1] = "Extra variant"
        variants = ALLNAMES_ROWS + [tuple(variant), tuple(extra)]
        gpkg = _build_gpkg(names, variants, coords=[(row[12], row[13]) for row in names])
        with patch.multiple(source, **_pins()):
            extracted = source._validate_captures(_build_captures(gpkg, names_rows=names,
                allnames_rows=variants, pubdate="20260901"))
        self.assertEqual(extracted["audit"]["source_feature_count"], 4)
        self.assertEqual(extracted["audit"]["source_allnames_count"], 6)
        self.assertEqual(extracted["audit"]["source_release_date"], "2026-09-01")
        self.assertEqual(extracted["audit"]["source_id_sha256"], source._sha(_canonical([row[0] for row in names])))
        self.assertEqual(extracted["audit"]["observed_bounds"], [66.5, -75.0, 67.0, -74.5])

    def test_unequal_producer_edition_dates_are_rejected(self):
        gpkg = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        captures = _build_captures(gpkg, gpkg_members={
            "Gazetteer_Antarctica_GPKG.gpkg": gpkg,
            "Gazetteer_Antarctica_GPKG.jpg": b"jpg-bytes",
            "Gazetteer_Antarctica_GPKG.xml": _fgdc_xml("GeoPackage", "20260901")})
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "gpkg_publication_date_drift"):
                source._validate_captures(captures)

    def test_changed_publication_seals_a_new_candidate_without_rewriting_the_first(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp, patch.multiple(source, **_pins()):
            storage = source.StagingStorageResource(use_local=True, local_dir=tmp + "/stage", prefix="")
            previous, versions = {}, []
            for names, variants in ((NAMES_ROWS, ALLNAMES_ROWS), (NAMES_ROWS[:2], ALLNAMES_ROWS[:3])):
                gpkg = _build_gpkg(names, variants)
                captures = _build_captures(gpkg, names_rows=names, allnames_rows=variants)
                with patch.object(source.httpx, "Client", side_effect=_client_factory(captures)):
                    candidate = source.capture_gnis_antarctica(storage, version="changed", capture_directory=tmp + "/captures")
                self.assertEqual(candidate["feature_count"], len(names))
                self.assertEqual(candidate["variant_name_count"], len(variants))
                self.assertTrue(candidate["source_population_complete"])
                self.assertNotIn(candidate["version"], versions)
                versions.append(candidate["version"])
                for key, snapshot in previous.items():
                    self.assertEqual((storage.read_key(key), storage.object_snapshot(key)), snapshot)
                previous = {key: (storage.read_key(key), storage.object_snapshot(key))
                            for key in storage.list_prefix("")}

    def test_retained_identity_and_gzip_bytes_bind_to_decoded_publication(self):
        import tempfile
        from pathlib import Path

        gpkg = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        captures = _build_captures(gpkg)
        for encoding in ("identity", "gzip"):
            with self.subTest(encoding=encoding), tempfile.TemporaryDirectory() as tmp, \
                    patch.multiple(source, **_pins()):
                storage = source.StagingStorageResource(use_local=True, local_dir=tmp + "/stage", prefix="")
                with patch.object(source.httpx, "Client", side_effect=_client_factory(captures, encoding)):
                    candidate = source.capture_gnis_antarctica(storage, version="wire", capture_directory=tmp + "/captures")
                root = Path(candidate["capture_directory"])
                originals = {key: (storage.read_key(key), storage.object_snapshot(key))
                             for key in storage.list_prefix("")}
                with patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                    replay = source.capture_gnis_antarctica(storage, version="replay", capture_directory=str(root))
                self.assertEqual(replay["version"], candidate["version"])
                body = (root / "gpkg_zip.body").read_bytes() + b" "
                (root / "gpkg_zip.body").write_bytes(body)
                receipt = json.loads((root / "gpkg_zip.receipt.json").read_bytes())
                receipt.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
                (root / "gpkg_zip.receipt.json").write_bytes(_canonical(receipt))
                manifest = json.loads((root / "capture_manifest.json").read_bytes())
                manifest["receipts"]["gpkg_zip"] = receipt
                manifest["artifact_roster"] = source.capture_roster_digests(root, exclude={"capture_manifest.json"})
                (root / "capture_manifest.json").write_bytes(_canonical(manifest))
                with patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                    with self.assertRaisesRegex(ValueError, "retained_capture_wire_body_mismatch"):
                        source.capture_gnis_antarctica(storage, version="forged", capture_directory=str(root))
                self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key))
                                  for key in storage.list_prefix("")}, originals)

    def test_retained_roster_request_and_cached_parquet_integrity(self):
        import tempfile
        from pathlib import Path

        gpkg = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        captures = _build_captures(gpkg)
        with tempfile.TemporaryDirectory() as tmp, patch.multiple(source, **_pins()):
            storage = source.StagingStorageResource(use_local=True, local_dir=tmp + "/stage", prefix="")
            with patch.object(source.httpx, "Client", side_effect=_client_factory(captures)):
                candidate = source.capture_gnis_antarctica(storage, version="custody", capture_directory=tmp + "/captures")
            root = Path(candidate["capture_directory"])
            (root / "extra.body").write_bytes(b"unbound")
            with self.assertRaisesRegex(ValueError, "capture_roster_mismatch"):
                source._read_replay(root)
            (root / "extra.body").unlink()
            receipt_path, manifest_path = root / "gpkg_zip.receipt.json", root / "capture_manifest.json"
            original_receipt, original_manifest = receipt_path.read_bytes(), manifest_path.read_bytes()
            receipt = json.loads(original_receipt)
            receipt.update(url=source.TEXT_URL, final_url=source.TEXT_URL)
            receipt_path.write_bytes(_canonical(receipt))
            manifest = json.loads(original_manifest)
            manifest["receipts"]["gpkg_zip"] = receipt
            manifest["artifact_roster"] = source.capture_roster_digests(root, exclude={"capture_manifest.json"})
            manifest_path.write_bytes(_canonical(manifest))
            with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
                source._read_replay(root)
            receipt_path.write_bytes(original_receipt)
            manifest_path.write_bytes(original_manifest)
            with patch.object(source, "MAX_CAPTURE_BYTES", 1):
                with self.assertRaisesRegex(ValueError, "retained_capture_budget_invalid"):
                    source._read_replay(root)
            parquet = candidate["candidate_ready"].replace("metadata/candidate_ready.json", "geoparquet/antarctica.parquet")
            storage.write_key(parquet, b"corrupt parquet")
            originals = {key: (storage.read_key(key), storage.object_snapshot(key))
                         for key in storage.list_prefix("")}
            with patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                with self.assertRaisesRegex(ValueError, "cached_candidate_object_integrity_failed"):
                    source.capture_gnis_antarctica(storage, version="corrupt", capture_directory=str(root))
            self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key))
                              for key in storage.list_prefix("")}, originals)

    def test_streamed_list_sha_matches_canonical_digest(self):
        items = [[1, "a"], [2, "b"], [3, None]]
        self.assertEqual(source._list_sha(items), source._sha(_canonical(items)))
        self.assertEqual(source._list_sha([]), source._sha(_canonical([])))

    def test_corrupt_receipt_is_recorded_not_dropped(self):
        import tempfile
        from pathlib import Path

        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        captures = _build_captures(gpkg_body)
        real_capture_get = source.capture_get

        def corrupting_capture_get(client, url, capture_root, name, *, max_bytes):
            body, receipt = real_capture_get(client, url, capture_root, name,
                                             max_bytes=max_bytes)
            if name == "gpkg_zip":
                (capture_root / f"{name}.receipt.json").write_bytes(b"{")
            return body, receipt

        with tempfile.TemporaryDirectory(prefix="gnis-antarctica-corrupt-") as temporary:
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True,
                                                    local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins()), \
                 patch.object(source.httpx, "Client", side_effect=_client_factory(captures)), \
                 patch.object(source, "capture_get", side_effect=corrupting_capture_get), \
                 patch.object(source, "_validate_captures",
                              side_effect=ValueError("source_schema_contract_drift")):
                with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                    source.capture_gnis_antarctica(storage, version="vfixture",
                                                   capture_directory=str(root / "captures"))
            quarantined = sorted((root / "staging" / "_quarantine" / source.DATASET_SLUG).glob("*"))
            self.assertTrue(quarantined)
            failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
            self.assertEqual(failure["reason_code"], "ValueError")
            not_retained = {entry["name"]: entry["reason"]
                            for entry in failure["not_retained"]}
            self.assertIn("gpkg_zip", not_retained)
            self.assertTrue(not_retained["gpkg_zip"].startswith(
                "retained_capture_receipt_unreadable:gpkg_zip"))
            self.assertIn("text_zip", failure["captured_names"])

    def test_full_capture_seals_candidate_and_reuses_cache(self):
        import tempfile
        from pathlib import Path

        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        captures = _build_captures(gpkg_body)
        bodies = {name: body for name, (body, _) in captures.items()}

        with tempfile.TemporaryDirectory(prefix="gnis-antarctica-e2e-") as temporary:
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True,
                                                   local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins()), \
                 patch.object(source.httpx, "Client", side_effect=_client_factory(captures)):
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
                originals = {key: (storage.read_key(key), storage.object_snapshot(key))
                             for key in storage.list_prefix("")}
                capture_root = Path(candidate["capture_directory"])
                retained_manifest = (capture_root / "capture_manifest.json").read_bytes()
                with patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP forbidden")):
                    reused = source.capture_gnis_antarctica(storage, version="vfixture",
                                                           capture_directory=str(capture_root))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])
                refresh = source.capture_gnis_antarctica(storage, version="refresh", capture_directory=str(root / "captures"))
                self.assertTrue(refresh["reused"])
                self.assertEqual(refresh["version"], candidate["version"])
                self.assertNotEqual(refresh["capture_directory"], candidate["capture_directory"])
                self.assertEqual((capture_root / "capture_manifest.json").read_bytes(), retained_manifest)
                self.assertEqual({key: (storage.read_key(key), storage.object_snapshot(key))
                                  for key in storage.list_prefix("")}, originals)

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
            with self.assertRaisesRegex(ValueError, "text_field_drift"):
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
            with self.assertRaisesRegex(ValueError, "source_allnames_roster_drift"):
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

    def test_cross_format_variant_multiplicity_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        text_rows = _allnames_text_rows(ALLNAMES_ROWS)
        text_rows[1][0] = 2814091
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "text_allnames_field_drift"):
                source._validate_captures(_build_captures(gpkg_body, allnames_text_rows=text_rows))

    def test_text_cross_format_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        rows = _names_text_rows(NAMES_ROWS)
        rows[2][3] = "Brien Glacier X"
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "text_field_drift"):
                source._validate_captures(_build_captures(gpkg_body, text_names_rows=rows))

    def test_allnames_duplicate_substitution_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        text_rows = _allnames_text_rows(ALLNAMES_ROWS)
        text_rows[1] = text_rows[0]
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "text_allnames_field_drift"):
                source._validate_captures(
                    _build_captures(gpkg_body, allnames_text_rows=text_rows))

    def test_allnames_unchecked_field_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        text_rows = _allnames_text_rows(ALLNAMES_ROWS)
        text_rows[2][6] = "CORRUPTED TITLE"
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "text_allnames_field_drift"):
                source._validate_captures(
                    _build_captures(gpkg_body, allnames_text_rows=text_rows))

    def test_allnames_date_drift_is_rejected(self):
        gpkg_body = _build_gpkg(NAMES_ROWS, ALLNAMES_ROWS)
        text_rows = _allnames_text_rows(ALLNAMES_ROWS)
        text_rows[0][9] = "07/20/2022"
        with patch.multiple(source, **_pins()):
            with self.assertRaisesRegex(ValueError, "text_allnames_field_drift"):
                source._validate_captures(
                    _build_captures(gpkg_body, allnames_text_rows=text_rows))

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
        captures = _build_captures(gpkg_body, pubdate="20260230")
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
            self.assertEqual(len(quarantine), 1 + len(completed) * 3)
            failure_key = next(key for key in quarantine if key.endswith("metadata/failure.json"))
            failure = json.loads(storage.read_key(failure_key))
            self.assertEqual(failure["reason_code"], "ValueError")
            self.assertEqual(failure["captured_names"], sorted(completed))
            self.assertEqual(failure["not_retained"],
                             [{"name": "text_zip",
                               "reason": "retained_capture_evidence_mismatch:text_zip"}])
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
        from dagster import AssetCheckSeverity, DefaultScheduleStatus
        from dagster_hifld.capture_completeness import approved_baseline_drift
        from dagster_hifld.assets.gnis_antarctica import (
            gnis_antarctica_draft_schedule,
            usgs_gnis_antarctica,
            usgs_gnis_antarctica_complete,
            usgs_gnis_antarctica_baseline_drift,
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
        candidate = {"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
                     "feature_count": 4, "variant_name_count": 6, "source_id_sha256": "new",
                     "source_population_complete": True, "cross_format_verified": True,
                     "candidate_ready": "fixture/metadata/candidate_ready.json"}
        self.assertTrue(usgs_gnis_antarctica_complete(candidate).passed)
        self.assertFalse(usgs_gnis_antarctica_complete({**candidate, "cross_format_verified": False}).passed)
        self.assertFalse(next(iter(usgs_gnis_antarctica_baseline_drift.check_specs)).blocking)
        skipped = usgs_gnis_antarctica_baseline_drift(candidate)
        self.assertEqual(skipped.severity, AssetCheckSeverity.WARN)
        self.assertEqual(skipped.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        reference = {**candidate, "version": "approved-fixture", "release_accepted": True,
                     "feature_count": 3, "source_id_sha256": "old"}
        compared = approved_baseline_drift(candidate, reference)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.severity, AssetCheckSeverity.WARN)
        self.assertEqual(compared.metadata["feature_count_delta"].value, 1)
        schedule = repo.get_schedule_def("gnis_antarctica_draft_refresh_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.execution_timezone, "America/New_York")
        self.assertEqual(schedule.cron_schedule, "0 6 1 * *")


if __name__ == "__main__":
    unittest.main()

"""Contract tests for the authoritative NOAA MarineCadastre ATON source."""
from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import unittest
import zipfile
import hashlib
from pathlib import Path

from dagster_hifld.sources.noaa_nautical_navaids import (
    COUNT_URL, DOWNLOAD_URL, EXPECTED_COUNT, EXPECTED_FIELDS, INPORT_URL,
    CAPTURE_URLS, LAYER_URL, PORTAL_ITEM_ID, SERVICE_URL, _replay_capture_bundle, _validate,
)


def _fixture(*, aid_name_type: str = "TEXT(96)"):
    ids = list(range(1, EXPECTED_COUNT + 1))
    portal = {"id": PORTAL_ITEM_ID, "owner": "marinecadastre_noaa", "title": "Aids to Navigation",
              "url": SERVICE_URL, "access": "public", "description":
              f"U.S. Coast Guard {DOWNLOAD_URL} {INPORT_URL}"}
    field_specs = [("objectid", "esriFieldTypeOID", 4), ("lightlistnumber", "esriFieldTypeDouble", None),
                   ("aidname", "esriFieldTypeString", 96), ("longitude", "esriFieldTypeString", 16),
                   ("latitude", "esriFieldTypeString", 16), ("characteristics", "esriFieldTypeString", 24),
                   ("aidheight", "esriFieldTypeSmallInteger", None), ("aidrange", "esriFieldTypeSmallInteger", None),
                   ("structure", "esriFieldTypeString", 160), ("remark", "esriFieldTypeString", 512)]
    fields = [{"name": name, "type": typ, "length": length} for name, typ, length in field_specs]
    layer = {"serviceItemId": "internal-noaa-service-id", "fields": fields,
             "geometryType": "esriGeometryPoint", "extent": {"spatialReference": {"latestWkid": 3857}}}
    service = {"serviceItemId": layer["serviceItemId"],
               "layers": [{"id": 0, "name": "Aids to Navigation"}]}
    ids_doc = {"objectIds": ids, "exceededTransferLimit": False}
    docs = {
        "portal_item": portal, "service_before": service, "layer_before": layer,
        "count_before": {"count": EXPECTED_COUNT}, "ids_before": ids_doc,
        "official_download": {}, "layer_after": layer, "service_after": service,
        "count_after": {"count": EXPECTED_COUNT}, "ids_after": ids_doc,
    }
    captures = {name: (json.dumps(doc).encode(), {"status": 200}) for name, doc in docs.items()}
    db = tempfile.NamedTemporaryFile(suffix=".gpkg")
    conn = sqlite3.connect(db.name)
    conn.executescript(f"""
      CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT, geometry_type_name TEXT, srs_id INTEGER, z INTEGER, m INTEGER);
      INSERT INTO gpkg_geometry_columns VALUES ('AtoN','spatial','POINT',4269,0,0);
      CREATE TABLE gpkg_contents (table_name TEXT, data_type TEXT, identifier TEXT, description TEXT, last_change TEXT, min_x REAL, min_y REAL, max_x REAL, max_y REAL, srs_id INTEGER);
      INSERT INTO gpkg_contents (table_name,data_type,min_x,min_y,max_x,max_y,srs_id) VALUES ('AtoN','features',-130,14,-63,50,4269);
      CREATE TABLE AtoN (OBJECTID INTEGER NOT NULL PRIMARY KEY, spatial POINT,
        lightListNumber DOUBLE, aidName {aid_name_type}, longitude TEXT(16), latitude TEXT(16),
        characteristics TEXT(24), aidHeight SMALLINT, aidRange SMALLINT, structure TEXT(160), remark TEXT(512));
      CREATE TABLE rtree_AtoN_spatial (id INTEGER PRIMARY KEY, minx REAL, maxx REAL, miny REAL, maxy REAL);
    """)
    conn.executemany("INSERT INTO AtoN (OBJECTID,spatial) VALUES (?,?)", [(i,b"\x00") for i in ids])
    conn.executemany("INSERT INTO rtree_AtoN_spatial VALUES (?,?,?, ?,?)", [(i,-10,10,-5,5) for i in ids])
    conn.commit(); conn.close()
    db.seek(0); native = db.read(); db.close()
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_STORED) as z:
        z.writestr("AtoN.gpkg", native)
    captures["official_download"] = (out.getvalue(), {"status": 200})
    return captures, out.getvalue()


def _mutate_archive(archive: bytes, statement: str) -> bytes:
    native = zipfile.ZipFile(io.BytesIO(archive)).read("AtoN.gpkg")
    with tempfile.NamedTemporaryFile(suffix=".gpkg") as temp:
        temp.write(native); temp.flush()
        conn = sqlite3.connect(temp.name)
        conn.execute(statement); conn.commit(); conn.close()
        temp.seek(0); changed = temp.read()
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_STORED) as z:
        z.writestr("AtoN.gpkg", changed)
    return out.getvalue()


class NoaaNauticalNavaidsTests(unittest.TestCase):
    def test_retained_replay_requires_complete_receipted_capture_roster(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); refs={}
            for name,url in CAPTURE_URLS.items():
                body=b'{"retained":true}'; wire=body
                receipt={"method":"GET","url":url,"final_url":url,"observed_at":"2026-10-19T23:00:00+00:00",
                    "status":200,"complete_body":True,"wire_bytes":len(wire),"wire_sha256":hashlib.sha256(wire).hexdigest(),
                    "bytes":len(body),"sha256":hashlib.sha256(body).hexdigest(),"error_code":None}
                (root/f"{name}.body").write_bytes(body);(root/f"{name}.wire").write_bytes(wire)
                receipt_bytes=json.dumps(receipt,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()+b"\n"
                (root/f"{name}.receipt.json").write_bytes(receipt_bytes)
                refs[name]={"body_sha256":hashlib.sha256(body).hexdigest(),"wire_sha256":hashlib.sha256(wire).hexdigest(),"receipt_sha256":hashlib.sha256(receipt_bytes).hexdigest()}
            (root/"capture_manifest.json").write_text(json.dumps({"schema_version":1,"captures":refs}))
            self.assertEqual(set(_replay_capture_bundle(root)),set(CAPTURE_URLS))
            (root/"portal_item.wire").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError,"noaa_replay_capture_integrity_failed:portal_item"):
                _replay_capture_bundle(root)

    def test_complete_official_archive_matches_bracketed_service_population(self):
        captures, archive = _fixture()
        audit = _validate(captures, archive, expected_archive_sha256=None)
        self.assertEqual(audit["feature_count"], EXPECTED_COUNT)
        self.assertEqual(audit["source_ids_sha256"], audit["service_ids_sha256"])
        self.assertEqual(audit["native_crs_epsg"], 4269)
        self.assertEqual([f["name"] for f in audit["source_fields"]], [f[0] for f in EXPECTED_FIELDS])
        self.assertFalse(audit["published_candidate_mismatch"]["schema_mapping_approved"])

    def test_missing_or_renamed_service_identifier_is_quarantined(self):
        captures, archive = _fixture()
        for name in ("layer_before", "layer_after"):
            doc = json.loads(captures[name][0]); doc["fields"][0]["name"] = "source_id"
            captures[name] = (json.dumps(doc).encode(), captures[name][1])
        with self.assertRaisesRegex(ValueError, "noaa_service_field_roster_drift"):
            _validate(captures, archive, expected_archive_sha256=None)

    def test_changed_population_between_id_brackets_is_quarantined(self):
        captures, archive = _fixture()
        doc = json.loads(captures["ids_after"][0]); doc["objectIds"][-1] += 100000
        captures["ids_after"] = (json.dumps(doc).encode(), captures["ids_after"][1])
        with self.assertRaisesRegex(ValueError, "noaa_service_id_bracket_incomplete_or_changed"):
            _validate(captures, archive, expected_archive_sha256=None)

    def test_truncated_id_bracket_is_quarantined(self):
        captures, archive = _fixture()
        doc = json.loads(captures["ids_before"][0]); doc["exceededTransferLimit"] = True
        captures["ids_before"] = (json.dumps(doc).encode(), captures["ids_before"][1])
        with self.assertRaisesRegex(ValueError, "noaa_service_id_roster_truncated"):
            _validate(captures, archive, expected_archive_sha256=None)

    def test_extra_native_archive_member_is_quarantined(self):
        captures, archive = _fixture()
        native = zipfile.ZipFile(io.BytesIO(archive)).read("AtoN.gpkg")
        altered = io.BytesIO()
        with zipfile.ZipFile(altered, "w") as z:
            z.writestr("AtoN.gpkg", native); z.writestr("unexpected.txt", "extra")
        with self.assertRaisesRegex(ValueError, "noaa_official_archive_roster_drift"):
            _validate(captures, altered.getvalue(), expected_archive_sha256=None)

    def test_native_physical_type_change_is_quarantined(self):
        captures, archive = _fixture(aid_name_type="TEXT(100)")
        with self.assertRaisesRegex(ValueError, "noaa_native_geopackage_schema_or_crs_drift"):
            _validate(captures, archive, expected_archive_sha256=None)

    def test_native_extra_column_is_quarantined(self):
        captures, archive = _fixture()
        changed = _mutate_archive(archive, 'ALTER TABLE AtoN ADD COLUMN unexpected TEXT')
        with self.assertRaisesRegex(ValueError, "noaa_native_geopackage_schema_or_crs_drift"):
            _validate(captures, changed, expected_archive_sha256=None)

    def test_native_geometry_crs_drift_is_quarantined(self):
        captures, archive = _fixture()
        changed = _mutate_archive(archive, "UPDATE gpkg_geometry_columns SET srs_id=4326")
        with self.assertRaisesRegex(ValueError, "noaa_native_geopackage_schema_or_crs_drift"):
            _validate(captures, changed, expected_archive_sha256=None)

    def test_incomplete_native_geometry_index_is_quarantined(self):
        captures, archive = _fixture()
        changed = _mutate_archive(archive, "DELETE FROM rtree_AtoN_spatial WHERE id=1")
        with self.assertRaisesRegex(ValueError, "noaa_native_geopackage_population_or_id_drift"):
            _validate(captures, changed, expected_archive_sha256=None)


if __name__ == "__main__":
    unittest.main()

import unittest

import pyarrow as pa

from dagster_hifld.sources.usace_offices import (
    USACEOfficesSourceError,
    _source_contract,
    _table,
    _validate_rows,
)


def fixture():
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
        {"name": "NAME", "type": "esriFieldTypeString", "nullable": True, "length": 3000},
        {"name": "SE_ANNO_CAD_DATA", "type": "esriFieldTypeBlob", "nullable": True},
    ]
    metadata = {"fields": fields, "objectIdField": "OBJECTID", "geometryType": "esriGeometryPoint",
                "spatialReference": {"wkid": 4326}}
    features = [{"attributes": {"OBJECTID": 7, "NAME": "Example", "SE_ANNO_CAD_DATA": None},
                 "geometry": {"x": -77.0, "y": 39.0, "spatialReference": {"wkid": 4326}}}]
    return metadata, features


class USACEOfficesSourceTests(unittest.TestCase):
    def test_native_table_retains_declared_nullable_blob_and_all_values(self):
        metadata, features = fixture()
        table, errors = _table(metadata, features)
        self.assertEqual(errors, [])
        self.assertEqual(table.schema.names, ["OBJECTID", "NAME", "SE_ANNO_CAD_DATA", "geometry"])
        self.assertEqual(table.schema.field("SE_ANNO_CAD_DATA").type, pa.binary())
        self.assertTrue(table.schema.field("SE_ANNO_CAD_DATA").nullable)
        self.assertEqual(table.column("NAME").to_pylist(), ["Example"])

    def test_full_source_ids_and_native_point_validate(self):
        metadata, features = fixture()
        self.assertEqual(_validate_rows(features, ("OBJECTID", "NAME", "SE_ANNO_CAD_DATA"), "OBJECTID", [7]), [])
        audit = _source_contract(metadata, 1, 1, [7], [7], features)
        self.assertTrue(audit["id_roster_preserved"])
        self.assertEqual(audit["feature_count"], 1)
        self.assertEqual(audit["quality_errors"], [])

    def test_missing_native_id_or_geometry_fails_closed(self):
        metadata, features = fixture()
        features[0]["attributes"]["OBJECTID"] = None
        features[0]["geometry"] = None
        self.assertEqual(_validate_rows(features, ("OBJECTID", "NAME", "SE_ANNO_CAD_DATA"), "OBJECTID", [7]),
                         ["full_source_id_roster_mismatch", "invalid_object_id", "missing_or_unexpected_native_point_geometry"])

    def test_blob_base64_is_decoded_to_declared_binary_and_invalid_encoding_fails(self):
        metadata, features = fixture()
        features[0]["attributes"]["SE_ANNO_CAD_DATA"] = "AQI="
        table, _ = _table(metadata, features)
        self.assertEqual(table.column("SE_ANNO_CAD_DATA").to_pylist(), [b"\x01\x02"])
        features[0]["attributes"]["SE_ANNO_CAD_DATA"] = "not base64!"
        with self.assertRaisesRegex(USACEOfficesSourceError, "source_blob_base64_decode_failure"):
            _table(metadata, features)

    def test_physical_field_width_is_retained_in_source_metadata(self):
        metadata, _ = fixture()
        self.assertEqual(metadata["fields"][1]["length"], 3000)


if __name__ == "__main__":
    unittest.main()

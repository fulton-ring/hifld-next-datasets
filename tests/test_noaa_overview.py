import hashlib
import json
import unittest

import pyarrow as pa

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.noaa import noaa_maritime_limits_overview, noaa_overview_draft_refresh_schedule
from dagster_hifld.noaa_overview import _local_only, _table_and_geometries, validate_snapshot
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource


class NoaaOverviewValidationTests(unittest.TestCase):
    def setUp(self):
        self.fields = [{"name": "OBJECTID", "type": "esriFieldTypeOID"}]
        self.signature = [("OBJECTID", "esriFieldTypeOID", None)]
        self.ids = [23, 24]
        self.features = {
            "fields": self.fields,
            "geometryType": "esriGeometryPolyline",
            "spatialReference": {"wkid": 4326},
            "features": [
                {"attributes": {"OBJECTID": oid},
                 "geometry": {"paths": [[[float(oid), 1], [float(oid), 2]]]}}
                for oid in self.ids
            ],
        }

    def validate(self, **overrides):
        args = {
            "count": 2,
            "ids": self.ids,
            "features": self.features,
            "page_end": {"features": []},
            "count_after": 2,
            "ids_after": list(reversed(self.ids)),
            "expected_count": 2,
            "expected_ids_sha256": hashlib.sha256(
                json.dumps(self.ids, separators=(",", ":")).encode()
            ).hexdigest(),
            "expected_fields_sha256": hashlib.sha256(
                json.dumps(self.signature, separators=(",", ":")).encode()
            ).hexdigest(),
            "expected_fields": tuple(self.signature),
        }
        args.update(overrides)
        return validate_snapshot(**args)

    def test_complete_roster_accepts_reordered_independent_id_witness(self):
        self.assertEqual(len(self.validate()), 2)

    def test_missing_renamed_or_duplicated_ids_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "count/ID/feature"):
            self.validate(ids=[23])
        with self.assertRaisesRegex(ValueError, "feature IDs"):
            self.features["features"][1]["attributes"]["OBJECTID"] = 23
            self.validate()

    def test_nonempty_terminal_page_and_schema_drift_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "terminal page"):
            self.validate(page_end={"features": [{"attributes": {"OBJECTID": 25}}]})
        self.features["fields"] = [{"name": "OID", "type": "esriFieldTypeOID"}]
        with self.assertRaisesRegex(ValueError, "schema or field-order drift"):
            self.validate()

    def test_physical_types_nulls_whitespace_and_source_geometry_survive_table_build(self):
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID"},
            {"name": "BOUND_ID", "type": "esriFieldTypeString", "length": 5},
            {"name": "PUB_DATE", "type": "esriFieldTypeDate", "length": 8},
            {"name": "NOTE", "type": "esriFieldTypeString", "length": 1000},
            {"name": "UNILATERAL", "type": "esriFieldTypeDouble"},
            {"name": "RULEID", "type": "esriFieldTypeInteger"},
        ]
        rows = [
            {"attributes": {"OBJECTID": 99, "BOUND_ID": "B0099", "PUB_DATE": 1781884800000,
                             "NOTE": " ", "UNILATERAL": 0.12500000000000003, "RULEID": 4},
             "geometry": {"paths": [[[-179.5, 5.25], [-179.4, 5.3]]]}},
            {"attributes": {"OBJECTID": 100, "BOUND_ID": "B0100", "PUB_DATE": None,
                             "NOTE": None, "UNILATERAL": 0.0, "RULEID": 3},
             "geometry": {"paths": [[[10.0, -4.0], [11.0, -4.5]]]}}]
        table, geometries = _table_and_geometries(rows, fields)
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table["OBJECTID"].type, pa.int64())
        self.assertEqual(table["RULEID"].type, pa.int32())
        self.assertEqual(table["UNILATERAL"].type, pa.float64())
        self.assertEqual(table["PUB_DATE"].type, pa.timestamp("ms", tz="UTC"))
        self.assertEqual(table["PUB_DATE"].to_pylist()[0].timestamp() * 1000, 1781884800000)
        self.assertIsNone(table["PUB_DATE"].to_pylist()[1])
        self.assertEqual(table["NOTE"].to_pylist(), [" ", None])
        self.assertEqual(table["UNILATERAL"].to_pylist()[0], 0.12500000000000003)
        self.assertEqual(len(geometries), 2)
        self.assertEqual(list(geometries[0].coords), [(-179.5, 5.25), (-179.4, 5.3)])

    def test_asset_is_registered_and_refresh_is_stopped(self):
        keys = {asset.key for asset in hifld_ingest_assets}
        self.assertIn(noaa_maritime_limits_overview.key, keys)
        self.assertEqual(noaa_overview_draft_refresh_schedule.default_status.value, "STOPPED")

    def test_draft_adapter_refuses_published_or_remote_storage(self):
        with self.assertRaisesRegex(ValueError, "isolated local candidate storage"):
            _local_only(PublishedStorageResource(local_dir="/tmp/published", use_local=True))
        with self.assertRaisesRegex(ValueError, "isolated local candidate storage"):
            _local_only(StagingStorageResource(bucket="production-like", use_local=False))


if __name__ == "__main__":
    unittest.main()

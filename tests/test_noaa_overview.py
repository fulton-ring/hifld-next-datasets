import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa

from dagster_hifld import noaa_overview
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.noaa import noaa_maritime_limits_overview, noaa_overview_draft_refresh_schedule
from dagster_hifld.noaa_overview import (
    EXPECTED_SERVICE_DESCRIPTION_PREFIX, EXPECTED_SERVICE_TITLE, _field_signature,
    _local_only, _table_and_geometries, _url, _validate_service_edition,
    stage_candidate, validate_snapshot,
)
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

    def test_conflicting_source_crs_representations_are_rejected(self):
        self.features["spatialReference"] = {"wkid": 4326, "latestWkid": 3857}
        with self.assertRaisesRegex(ValueError, "feature response CRS code drift"):
            self.validate()
        self.features["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        self.features["features"][0]["geometry"]["spatialReference"] = {"wkid": 3857}
        with self.assertRaisesRegex(ValueError, "feature geometry CRS code drift"):
            self.validate()

    def test_only_native_finite_xy_coordinates_are_accepted(self):
        point = self.features["features"][0]["geometry"]["paths"][0][0]
        self.features["features"][0]["geometry"]["paths"][0][0] = [point[0], point[1], 7]
        with self.assertRaisesRegex(ValueError, "not finite native XY"):
            self.validate()
        self.features["features"][0]["geometry"]["paths"][0][0] = point
        self.features["features"][0]["geometry"]["hasM"] = True
        with self.assertRaisesRegex(ValueError, "hasM geometry"):
            self.validate()

    def test_service_version_and_release_marker_are_pinned(self):
        service = {
            "documentInfo": {"Title": EXPECTED_SERVICE_TITLE},
            "serviceDescription": EXPECTED_SERVICE_DESCRIPTION_PREFIX + " more producer description",
        }
        _validate_service_edition(service)
        service["documentInfo"]["Title"] = "U.S. Maritime Limits and Boundaries (version 5.0)"
        with self.assertRaisesRegex(ValueError, "published service edition changed"):
            _validate_service_edition(service)

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


class NoaaOverviewRepeatCaptureTests(unittest.TestCase):
    """Repeat staging of identical response bodies must not collide.

    The candidate version identity covers the full immutable output content:
    response bodies, capture receipts (their checked_at values are embedded in
    provenance) and the capture quarantine prefix. A synthetic small capture is
    used so the staging path runs offline; the real producer roster/count pins
    stay in production and drift rejection is covered by the validation tests
    above, which run unpatched.
    """

    IDS = [23, 24]
    FIELDS = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID"},
        {"name": "BOUND_ID", "type": "esriFieldTypeString", "length": 5},
        {"name": "PUB_DATE", "type": "esriFieldTypeDate", "length": 8},
        {"name": "NOTE", "type": "esriFieldTypeString", "length": 1000},
        {"name": "UNILATERAL", "type": "esriFieldTypeDouble"},
        {"name": "RULEID", "type": "esriFieldTypeInteger"},
    ]

    def _features(self):
        features = []
        for oid in self.IDS:
            features.append({
                "attributes": {"OBJECTID": oid, "BOUND_ID": f"B{oid:04d}",
                               "PUB_DATE": 1781884800000 if oid == 23 else None,
                               "NOTE": f"note {oid}", "UNILATERAL": 0.5 * oid,
                               "RULEID": oid % 5},
                "geometry": {"paths": [[[float(oid), 1.0], [float(oid), 2.0]]]},
            })
        return {"objectIdFieldName": "OBJECTID", "features": features, "fields": self.FIELDS,
                "geometryType": "esriGeometryPolyline", "spatialReference": {"wkid": 4326},
                "exceededTransferLimit": False}

    def _write_capture(self, directory: Path, checked_at: str) -> None:
        directory.mkdir(parents=True)
        bodies = {
            "service": {
                "currentVersion": "11.4",
                "serviceDescription": EXPECTED_SERVICE_DESCRIPTION_PREFIX + " producer description",
                "documentInfo": {"Title": EXPECTED_SERVICE_TITLE},
                "spatialReference": {"wkid": 4326},
                "layers": [{"id": 0, "name": "Overview"}],
            },
            "count": {"count": len(self.IDS)},
            "ids": {"objectIdFieldName": "OBJECTID", "objectIds": list(self.IDS)},
            "features": self._features(),
            "page_end": {"features": []},
            "count_after": {"count": len(self.IDS)},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": list(reversed(self.IDS))},
        }
        for name, value in bodies.items():
            body = json.dumps(value, separators=(",", ":")).encode()
            (directory / f"{name}.body").write_bytes(body)
            expected_url = _url(name, count=noaa_overview.EXPECTED_COUNT)
            receipt = {"url": expected_url, "final_url": expected_url,
                       "checked_at": checked_at, "status": 200,
                       "content_type": "application/json", "bytes": len(body),
                       "sha256": hashlib.sha256(body).hexdigest(), "truncated": False}
            (directory / f"{name}.json").write_text(json.dumps(receipt, sort_keys=True))

    def _recheck_receipts(self, directory: Path, checked_at: str) -> None:
        for path in sorted(directory.glob("*.json")):
            receipt = json.loads(path.read_text())
            receipt["checked_at"] = checked_at
            path.write_text(json.dumps(receipt, sort_keys=True))

    def _synthetic_validator(self, count, ids, features, page_end, count_after, ids_after, **_):
        return validate_snapshot(
            count, ids, features, page_end, count_after, ids_after,
            expected_count=count,
            expected_ids_sha256=hashlib.sha256(
                json.dumps(sorted(ids), separators=(",", ":")).encode()).hexdigest(),
            expected_fields_sha256=hashlib.sha256(
                json.dumps(_field_signature(features["fields"]), separators=(",", ":")).encode()).hexdigest(),
            expected_fields=tuple(_field_signature(features["fields"])),
        )

    def test_identical_bodies_with_new_receipts_stage_as_distinct_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            storage = StagingStorageResource(local_dir=str(root / "staging"), use_local=True)
            capture_first = root / "capture-first"
            capture_again = root / "capture-again"
            with patch.object(noaa_overview, "EXPECTED_COUNT", len(self.IDS)), \
                    patch.object(noaa_overview, "EXPECTED_IDS_SHA256", hashlib.sha256(
                        json.dumps(sorted(self.IDS), separators=(",", ":")).encode()).hexdigest()), \
                    patch.object(noaa_overview, "validate_snapshot", self._synthetic_validator):
                self._write_capture(capture_first, "2026-11-13T12:00:00Z")
                shutil.copytree(capture_first, capture_again)
                first = stage_candidate(storage, capture_first)
                replay = stage_candidate(storage, capture_again)
                first_provenance = json.loads(storage.read_key(
                    f"{first['staging_prefix']}/metadata/provenance.json"))
                first_before = storage.read_key(
                    f"{first['staging_prefix']}/metadata/provenance.json")
                self._recheck_receipts(capture_again, "2026-11-13T12:30:00Z")
                second = stage_candidate(storage, capture_again)
            self.assertEqual(first["version"], replay["version"])
            self.assertNotEqual(second["version"], first["version"])
            self.assertEqual(first["feature_count"], len(self.IDS))
            self.assertFalse(first["promotion_allowed"])
            first_gpkg = storage.read_key(f"{first['staging_prefix']}/geopackage/overview.gpkg")
            second_gpkg = storage.read_key(f"{second['staging_prefix']}/geopackage/overview.gpkg")
            self.assertEqual(first_gpkg, second_gpkg)
            second_provenance = json.loads(storage.read_key(
                f"{second['staging_prefix']}/metadata/provenance.json"))
            self.assertEqual(first_provenance["captured_at"], "2026-11-13T12:00:00Z")
            self.assertEqual(second_provenance["captured_at"], "2026-11-13T12:30:00Z")
            self.assertNotEqual(first_provenance["raw_capture_prefix"],
                                second_provenance["raw_capture_prefix"])
            self.assertEqual(storage.read_key(
                f"{first['staging_prefix']}/metadata/provenance.json"), first_before)
            for name in ("service", "count", "ids", "features", "page_end", "count_after", "ids_after"):
                first_receipt = json.loads(storage.read_key(f"{first['staging_prefix']}/raw/{name}.json"))
                second_receipt = json.loads(storage.read_key(f"{second['staging_prefix']}/raw/{name}.json"))
                self.assertEqual(first_receipt["checked_at"], "2026-11-13T12:00:00Z")
                self.assertEqual(second_receipt["checked_at"], "2026-11-13T12:30:00Z")


if __name__ == "__main__":
    unittest.main()

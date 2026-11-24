from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import usace_pylon_locations as source

COUNT = 4079


class ArcGISFixture(httpx.BaseTransport):
    def __init__(self, *, changed_after=False, schema_drift=False, changed_page_crs=False, changed_feature_crs=False, geometry_z=False):
        self.ids = list(range(1, COUNT + 1))
        self.changed_after = changed_after
        self.schema_drift = schema_drift
        self.changed_page_crs = changed_page_crs
        self.changed_feature_crs = changed_feature_crs
        self.geometry_z = geometry_z
        self.id_calls = 0

    def handle_request(self, request):
        query = request.url.params
        path = request.url.path
        if path.endswith("/MapServer/21"):
            fields = [{"name": name, "type": typ, "alias": name, **({"length": 254} if typ == "esriFieldTypeString" else {})} for name, typ in source.SOURCE_SCHEMA]
            if self.schema_drift:
                fields[1]["type"] = "esriFieldTypeInteger"
            data = {"id": 21, "name": "PYLONS_POINT", "geometryType": "esriGeometryPoint",
                    "maxRecordCount": 2000, "spatialReference": {"wkid": 4326}, "fields": fields}
        elif query.get("returnIdsOnly") == "true":
            self.id_calls += 1
            ids = self.ids[:-1] + [COUNT + 1] if self.changed_after and self.id_calls == 2 else self.ids
            data = {"objectIdFieldName": "OBJECTID", "objectIds": ids}
        elif query.get("returnCountOnly") == "true":
            data = {"count": COUNT}
        else:
            requested = [int(value) for value in query["objectIds"].split(",")]
            features = []
            for identity in requested:
                attrs = {name: (identity if name == "OBJECTID" else 123 if name == "Scale_Minimum" else "" if name != "Feature_ID" else f"id-{identity}") for name, _ in source.FIELDS}
                geometry = {"x": -79.5 + identity / 100000, "y": 40.0,
                            "spatialReference": {"wkid": 3857 if self.changed_feature_crs else 4326}}
                if self.geometry_z:
                    geometry["z"] = 100.0
                features.append({"attributes": attrs, "geometry": geometry})
            data = {"features": features, "exceededTransferLimit": False,
                    "geometryType": "esriGeometryPoint", "spatialReference": {"wkid": 3857 if self.changed_page_crs else 4326},
                    "fields": [{"name": name, "type": typ} for name, typ in source.FIELDS]}
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(data).encode()), request=request, headers={"content-type": "application/json"})


class PylonLocationCaptureTests(unittest.TestCase):
    def _capture(self, transport):
        temporary = tempfile.TemporaryDirectory(prefix="pylon-source-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            result = source.capture(storage, "v20261021T000000Z", client=client)
        return storage, result

    def test_complete_capture_preserves_native_fields_xy_and_page_raw_wire(self):
        storage, result = self._capture(ArcGISFixture())
        self.assertEqual(result["feature_count"], 4079)
        self.assertFalse(result["release_accepted"])
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", "")
        manifest = json.loads(storage.read_key(prefix + "metadata/native_capture.json"))
        self.assertEqual(len(manifest["pages"]), 21)
        self.assertEqual(manifest["pages"][-1]["count"], 79)
        raw_name = prefix + "raw/page_0000000.json"
        wire_name = prefix + "raw/page_0000000.wire"
        receipt_name = prefix + "raw/page_0000000.receipt.json"
        self.assertTrue(storage.object_exists(raw_name))
        self.assertTrue(storage.object_exists(wire_name))
        receipt = json.loads(storage.read_key(receipt_name))
        self.assertEqual(receipt["sha256"], __import__("hashlib").sha256(storage.read_key(raw_name)).hexdigest())
        parquet_path = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        parquet_path.close()
        self.addCleanup(lambda: __import__("os").unlink(parquet_path.name))
        with open(parquet_path.name, "wb") as out:
            out.write(storage.read_key(prefix + f"metadata/native_candidate/{source.FILE_SLUG}.parquet"))
        table = pq.read_table(parquet_path.name)
        self.assertEqual(table.num_rows, 4079)
        self.assertEqual(table.column("OBJECTID").to_pylist(), list(range(1, 4080)))
        self.assertEqual(table.column("native_x")[0].as_py(), -79.5 + 0.00001)
        self.assertEqual(table.column("native_y")[0].as_py(), 40.0)
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["primary_column"], "geometry")
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
        self.assertEqual(geo["columns"]["geometry"]["crs"]["name"], "WGS 84")
        from shapely import from_wkb
        first = from_wkb(table.column("geometry").to_pylist()[0])
        self.assertEqual((round(first.x, 9), round(first.y, 9)), (round(-79.5 + 0.00001, 9), 40.0))

    def test_before_after_id_drift_refuses_a_candidate(self):
        transport = ArcGISFixture(changed_after=True)
        temporary = tempfile.TemporaryDirectory(prefix="pylon-drift-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_population_changed_during_capture"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_schema_drift_refuses_before_feature_fetch(self):
        transport = ArcGISFixture(schema_drift=True)
        temporary = tempfile.TemporaryDirectory(prefix="pylon-schema-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_schema_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertEqual(transport.id_calls, 0)
        self.assertTrue(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", "metadata/quarantine.json")))

    def test_conflicting_page_crs_refuses_before_candidate(self):
        transport = ArcGISFixture(changed_page_crs=True)
        temporary = tempfile.TemporaryDirectory(prefix="pylon-page-crs-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_crs_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_conflicting_feature_crs_refuses_before_candidate(self):
        transport = ArcGISFixture(changed_feature_crs=True)
        temporary = tempfile.TemporaryDirectory(prefix="pylon-feature-crs-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_crs_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_unexpected_z_ordinate_refuses_instead_of_dropping_it(self):
        transport = ArcGISFixture(geometry_z=True)
        temporary = tempfile.TemporaryDirectory(prefix="pylon-z-ordinate-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, "source_dimension_drift"):
                source.capture(storage, "v20261021T000000Z", client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261021T000000Z", f"metadata/native_candidate/{source.FILE_SLUG}.parquet")))

    def test_ingest_asset_materializes_the_native_candidate_through_dagster(self):
        """Dagster path: the ingest asset runs the real point capture and the
        shared writer emits the staged candidate with the EPSG:4326 contract."""
        from dagster import DagsterInstance, materialize
        from dagster_hifld.assets.usace_pylon_locations import usace_pylon_locations_usace_ienc
        real_client = httpx.Client
        def fixture_client(*args, **kwargs):
            return real_client(transport=ArcGISFixture(), **kwargs)
        with tempfile.TemporaryDirectory(prefix="pylon-dagster-path-") as directory:
            dagster_home = Path(directory) / "dagster-home"
            dagster_home.mkdir()
            storage = StagingStorageResource(use_local=True, local_dir=directory)
            with patch("dagster_hifld.sources.usace_pylon_locations.httpx.Client",
                       side_effect=fixture_client), \
                    DagsterInstance.from_config(str(dagster_home)) as instance:
                result = materialize([usace_pylon_locations_usace_ienc],
                                     resources={"staging_storage": storage},
                                     instance=instance)
                self.assertTrue(result.success)
                self.assertEqual(len(result.get_asset_materialization_events()), 1)
            versions = {key.split("/")[2] for key in storage.list_prefix(
                f"{source.DATASET_SLUG}/{source.FILE_SLUG}") if key.count("/") >= 2}
            self.assertEqual(len(versions), 1)
            version = versions.pop()
            table = pq.read_table(io.BytesIO(storage.read_key(
                f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{version}/metadata/native_candidate/{source.FILE_SLUG}.parquet")))
            self.assertEqual(table.num_rows, COUNT)
            geo = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
            self.assertEqual(geo["columns"]["geometry"]["crs"]["name"], "WGS 84")
            self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])
            from shapely import from_wkb
            first = from_wkb(table.column("geometry").to_pylist()[0])
            self.assertEqual((round(first.x, 9), round(first.y, 9)), (round(-79.5 + 0.00001, 9), 40.0))

    def test_normal_repository_registers_pylon_refresh_and_preserves_existing_refreshes(self):
        from dagster import AssetKey, DefaultScheduleStatus
        from dagster_hifld.definitions import defs

        repository = defs.get_repository_def()
        expected = {
            "faa_airspace_draft_refresh",
            "noaa_eez_draft_refresh",
            "noaa_contiguous_draft_refresh",
            "pylon_locations_usace_ienc_draft_refresh",
        }
        self.assertTrue(expected.issubset({job.name for job in repository.get_all_jobs()}))
        job = repository.get_job("pylon_locations_usace_ienc_draft_refresh")
        self.assertEqual(set(job.asset_layer.executable_asset_keys), {
            AssetKey(["ingest", source.DATASET_SLUG, source.FILE_SLUG])
        })
        schedule = repository.get_schedule_def("pylon_locations_draft_refresh_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()

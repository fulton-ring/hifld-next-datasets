"""Tiny complete source capture, immutable cache, catalog, and drift fixtures."""
from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq
import shapely
from dagster import DagsterInstance, materialize

from dagster_hifld.assets.bts_air_truck import (
    bts_intermodal_freight_air_to_truck,
    bts_intermodal_freight_air_to_truck_complete,
)
from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import bts_air_truck as source

_HTTPX_CLIENT = httpx.Client
_ASSET_OP = "ingest__intermodal_freight_facilities_air_to_truck__intermodal_freight_facilities_air_to_truck"


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _fixture_factory(mode="ok"):
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "nullable": False, "domain": None},
        {"name": "LOCID", "type": "esriFieldTypeString", "alias": "LOCID", "length": 3, "nullable": True, "domain": None},
        {"name": "FACILITY_C", "type": "esriFieldTypeString", "alias": "FACILITY_C", "length": 200, "nullable": True, "domain": None},
        {"name": "DATE", "type": "esriFieldTypeString", "alias": "DATE", "length": 10, "nullable": True, "domain": None},
        {"name": "EST_AREA", "type": "esriFieldTypeDouble", "alias": "EST_AREA", "nullable": True, "domain": None},
    ]
    ids = [] if mode == "empty" else ([1, 2] if mode != "id" else [1, 3])
    if mode == "schema":
        fields[-1]["type"] = "esriFieldTypeInteger"
    desc = (
        "The dataset was updated from the Bureau of Transportation Statistics "
        "(BTS) and is part of the National Transportation Atlas Database (NTAD). "
        "This is the Air-to-Truck facilities layer."
    )
    item = {"id": source.ITEM_ID, "owner": "USDOT_BTS", "title": source.LAYER_NAME,
            "url": source.SERVICE_URL, "access": "public", "description": desc,
            "licenseInfo": "U.S. public domain"}
    service = {"serviceItemId": source.ITEM_ID, "layers": [{"id": 0, "name": source.LAYER_NAME}]}
    layer = {
        "name": source.LAYER_NAME, "geometryType": "esriGeometryPoint", "objectIdField": "OBJECTID",
        "hasZ": False, "hasM": False,
        "extent": {"spatialReference": {"latestWkid": 4326}},
        "fields": fields,
        "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
    }
    rows = [
        {"attributes": {"OBJECTID": 1, "LOCID": "JFK", "FACILITY_C": "Carrier A", "DATE": "09/2018", "EST_AREA": 74000.0},
         "geometry": {"x": -73.7781, "y": 40.6413}},
        {"attributes": {"OBJECTID": 2, "LOCID": "LAX", "FACILITY_C": None, "DATE": "09/2018", "EST_AREA": 2000.0},
         "geometry": {"x": -118.4085, "y": 33.9416}},
    ]

    def handler(request):
        path = request.url.path
        query = parse_qs(request.url.query.decode())
        if "/sharing/rest/content/items/" in path and path.endswith("/data"):
            result = {"fixture": "archive lineage only"}
        elif "/sharing/rest/content/items/" in path:
            result = item
        elif path.endswith("/FeatureServer"):
            result = service
        elif path.endswith("/FeatureServer/0"):
            result = layer
        elif path.endswith("/query"):
            if query.get("returnCountOnly") == ["true"]:
                result = {"count": 1 if mode == "count" else 2}
            elif query.get("returnIdsOnly") == ["true"]:
                result = {"objectIds": ids}
            else:
                result = {"spatialReference": {"latestWkid": 4326}, "features": rows}
                if mode == "drop":
                    result["features"] = rows[:1]
            if "error" in query:
                result = {"error": {"message": "fixture error"}}
        else:
            return httpx.Response(404, request=request)
        body = _canonical(result)
        return httpx.Response(200, stream=httpx.ByteStream(body),
                              headers={"content-length": str(len(body))}, request=request)

    return lambda: _HTTPX_CLIENT(transport=httpx.MockTransport(handler), follow_redirects=False)


class BtsAirTruckTests(unittest.TestCase):
    def _synthetic_pins(self):
        return patch.multiple(
            source,
            EXPECTED_SOURCE_COUNT=2,
            EXPECTED_SOURCE_ID_SHA256=hashlib.sha256(_canonical([1, 2])).hexdigest(),
            EXPECTED_SOURCE_SCHEMA_SHA256=hashlib.sha256(_canonical([
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "nullable": False, "domain": None},
                {"name": "LOCID", "type": "esriFieldTypeString", "alias": "LOCID", "length": 3, "nullable": True, "domain": None},
                {"name": "FACILITY_C", "type": "esriFieldTypeString", "alias": "FACILITY_C", "length": 200, "nullable": True, "domain": None},
                {"name": "DATE", "type": "esriFieldTypeString", "alias": "DATE", "length": 10, "nullable": True, "domain": None},
                {"name": "EST_AREA", "type": "esriFieldTypeDouble", "alias": "EST_AREA", "nullable": True, "domain": None},
            ])).hexdigest(),
        )

    def test_complete_capture_seal_catalog_refresh_and_drift_quarantine(self):
        keys = {key for asset_def in hifld_ingest_assets for key in asset_def.keys}
        self.assertIn(bts_intermodal_freight_air_to_truck.key, keys)

        with tempfile.TemporaryDirectory() as tmp, self._synthetic_pins(), patch(
            "dagster_hifld.assets.bts_air_truck.EXPECTED_SOURCE_COUNT", 2
        ):
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            instance_dir = Path(tmp) / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            factory = _fixture_factory()
            with patch.object(source.httpx, "Client", side_effect=lambda *a, **kw: factory()):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    first_result = materialize(
                        [bts_intermodal_freight_air_to_truck],
                        run_config={"ops": {_ASSET_OP: {
                            "config": {"capture_directory": str(Path(tmp) / "capture")}}}},
                        resources={"staging_storage": storage}, instance=instance,
                    )
                    self.assertTrue(first_result.success)
                    first = first_result.output_for_node(_ASSET_OP)
                    self.assertEqual(first["feature_count"], 2)
                    self.assertTrue(first["source_population_complete"])
                    version = first["version"]
                    prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{version}"
                    audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
                    manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
                    ready = json.loads(storage.read_key(f"{prefix}/metadata/candidate_ready.json"))
                    self.assertEqual([field["name"] for field in audit["source_fields"]],
                                     ["OBJECTID", "LOCID", "FACILITY_C", "DATE", "EST_AREA"])
                    self.assertFalse(manifest["approved"])
                    self.assertFalse(ready["approved"])
                    self.assertEqual(manifest["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"][0]["path"], f"geoparquet/{source.DATASET_SLUG}.parquet")
                    table = pq.ParquetFile(io.BytesIO(storage.read_key(f"{prefix}/geoparquet/{source.DATASET_SLUG}.parquet"))).read()
                    self.assertEqual(table.column_names, ["OBJECTID", "LOCID", "FACILITY_C", "DATE", "EST_AREA", "geometry"])
                    self.assertEqual([str(table.schema.field(n).type) for n in table.column_names], ["int64", "string", "string", "string", "double", "binary"])
                    self.assertEqual(table.column("OBJECTID").to_pylist(), [1, 2])
                    self.assertEqual(table.column("LOCID").to_pylist(), ["JFK", "LAX"])
                    _primary, _geometry_column, crs = geometry_metadata(table.schema)
                    self.assertEqual(crs.to_epsg(), 4326)
                    point = shapely.from_wkb(table.column("geometry")[0].as_py())
                    self.assertEqual((point.x, point.y), (-73.7781, 40.6413))
                    staged = set(storage.list_prefix(prefix))
                    self.assertIn(f"{prefix}/raw/page_000.body", staged)
                    self.assertIn(f"{prefix}/raw/page_000.wire", staged)
                    self.assertIn(f"{prefix}/raw/page_000.receipt.json", staged)
                    partition = build_publish_partition_key(source.DATASET_SLUG, source.FILE_SLUG, version)
                    self.assertIn(partition, instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME))
                    result = materialize(
                        [publish_catalog], partition_key=partition,
                        resources={"staging_storage": storage}, instance=instance,
                    )
                    self.assertTrue(result.success)
                    with patch.object(source.httpx, "Client", side_effect=lambda *a, **kw: factory()):
                        cached_result = materialize(
                            [bts_intermodal_freight_air_to_truck],
                            run_config={"ops": {_ASSET_OP: {
                                "config": {"capture_directory": str(Path(tmp) / "capture2")}}}},
                            resources={"staging_storage": storage}, instance=instance,
                        )
                    self.assertTrue(cached_result.success)
                    cached = cached_result.output_for_node(_ASSET_OP)
                self.assertTrue(cached["reused"])
                self.assertEqual(cached["version"], version)
                check_fn = bts_intermodal_freight_air_to_truck_complete.op.compute_fn.decorated_fn
                self.assertTrue(check_fn(cached).passed)

        for mode, reason in (("count", "source_count_or_id_roster_drift"),
                             ("schema", "source_schema_drift"),
                             ("id", "source_id_roster_drift"),
                             ("empty", "source_seed_id_roster_missing_or_invalid"),
                             ("drop", "source_page_ids_do_not_match_requested_slice")):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp, self._synthetic_pins():
                storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
                factory = _fixture_factory(mode)
                with patch.object(source.httpx, "Client", side_effect=lambda *a, **kw: factory()):
                    with self.assertRaisesRegex(ValueError, reason):
                        source.capture_bts_air_truck(storage, capture_directory=str(Path(tmp) / "capture"))
                quarantine_keys = storage.list_prefix(f"_quarantine/{source.DATASET_SLUG}")
                self.assertTrue(quarantine_keys)
                failure_key = next(key for key in quarantine_keys if key.endswith("/metadata/failure.json"))
                self.assertEqual(json.loads(storage.read_key(failure_key))["reason_code"], reason)


if __name__ == "__main__":
    unittest.main()

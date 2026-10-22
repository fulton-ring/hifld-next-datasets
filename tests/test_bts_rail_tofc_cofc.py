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

from dagster_hifld.assets.bts_rail_tofc_cofc import (
    bts_intermodal_freight_rail_tofc_cofc,
    bts_intermodal_freight_rail_tofc_cofc_complete,
)
from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import bts_rail_tofc_cofc as source

_HTTPX_CLIENT = httpx.Client
_ASSET_OP = "ingest__intermodal_freight_facilities_rail_tofc_cofc__intermodal_freight_facilities_rail_tofc_cofc"


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _rail_fixture_fields():
    names = ["OBJECTID", "TERMINAL", "TERM_ADDRESS", "CITY", "STATE", "ZIP_CODE", "PORT", "RAIL_CO", "EQUIPMENT", "ACCESS_RD", "SPLC", "LAT", "LON"]
    fields = []
    for name in names:
        kind = "esriFieldTypeOID" if name == "OBJECTID" else "esriFieldTypeDouble" if name in {"LAT", "LON"} else "esriFieldTypeString"
        field = {"name": name, "type": kind, "alias": name, "nullable": name != "OBJECTID", "domain": None}
        if kind == "esriFieldTypeString":
            field["length"] = 255
        fields.append(field)
    return fields

def _fixture_factory(mode="ok"):
    fields = _rail_fixture_fields()
    ids = [] if mode == "empty" else ([1, 2] if mode != "id" else [1, 3])
    if mode == "schema":
        fields[-1]["type"] = "esriFieldTypeInteger"
    desc = (
        "The dataset was compiled on November 15, 2024 and updated on February 28, 2025 "
        "from the Bureau of Transportation Statistics "
        "(BTS) and is part of the National Transportation Atlas Database (NTAD). "
        "This is the Rail TOFC/COFC facilities layer."
    )
    item = {"id": source.ITEM_ID, "owner": "USDOT_BTS", "title": source.LAYER_NAME,
            "url": source.SERVICE_URL, "access": "public", "description": desc,
            "licenseInfo": "U.S. public domain", "created": 1720644100000,
            "modified": 1784137096000}
    service = {"serviceItemId": source.ITEM_ID, "layers": [{"id": 0, "name": source.LAYER_NAME}]}
    layer = {
        "name": source.LAYER_NAME, "geometryType": "esriGeometryPoint", "objectIdField": "OBJECTID",
        "hasZ": False, "hasM": False,
        "extent": {"spatialReference": {"latestWkid": 4326}},
        "editingInfo": {"lastEditDate": 1734380202378,
                         "schemaLastEditDate": 1720644927074,
                         "dataLastEditDate": 1720644111319},
        "fields": fields,
        "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
    }
    rows = [
        {"attributes": {"OBJECTID": 1, "TERMINAL": "Terminal A", "TERM_ADDRESS": "1 Main St", "CITY": "City A", "STATE": "NY", "ZIP_CODE": "10001", "PORT": "Port A", "RAIL_CO": "Rail A", "EQUIPMENT": "TOFC", "ACCESS_RD": "Main Rd", "SPLC": "012345678", "LAT": 40.0, "LON": -73.0}, "geometry": {"x": -73.0, "y": 40.0}},
        {"attributes": {"OBJECTID": 2, "TERMINAL": "Terminal B", "TERM_ADDRESS": None, "CITY": "City B", "STATE": "CA", "ZIP_CODE": None, "PORT": None, "RAIL_CO": "UP", "EQUIPMENT": "COFC", "ACCESS_RD": None, "SPLC": "000000001", "LAT": 33.0, "LON": -118.0}, "geometry": {"x": -118.0, "y": 33.0}},
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


class BtsRailTofcCofcTests(unittest.TestCase):
    def _synthetic_pins(self):
        return patch.multiple(
            source,
            EXPECTED_SOURCE_COUNT=2,
            EXPECTED_SOURCE_ID_SHA256=hashlib.sha256(_canonical([1, 2])).hexdigest(),
            EXPECTED_SOURCE_SCHEMA_SHA256=hashlib.sha256(_canonical(_rail_fixture_fields())).hexdigest(),
        )

    def test_complete_capture_seal_catalog_refresh_and_drift_quarantine(self):
        keys = {key for asset_def in hifld_ingest_assets for key in asset_def.keys}
        self.assertIn(bts_intermodal_freight_rail_tofc_cofc.key, keys)

        with tempfile.TemporaryDirectory() as tmp, self._synthetic_pins(), patch(
            "dagster_hifld.assets.bts_rail_tofc_cofc.EXPECTED_SOURCE_COUNT", 2
        ):
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            instance_dir = Path(tmp) / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            factory = _fixture_factory()
            with patch.object(source.httpx, "Client", side_effect=lambda *a, **kw: factory()):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    first_result = materialize(
                        [bts_intermodal_freight_rail_tofc_cofc],
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
                    expected_names = [field["name"] for field in _rail_fixture_fields()]
                    self.assertEqual([field["name"] for field in audit["source_fields"]], expected_names)
                    self.assertFalse(manifest["approved"])
                    self.assertFalse(ready["approved"])
                    self.assertEqual(manifest["source_vintage_evidence"]["portal_item_modified_epoch_ms"], 1784137096000)
                    self.assertIn("updated on February 28, 2025", manifest["source_vintage_evidence"]["portal_description"])
                    self.assertEqual(manifest["source_vintage_evidence"]["layer_editing_info"]["dataLastEditDate"], 1720644111319)
                    self.assertIsNone(manifest["source_vintage_evidence"]["release_date_selected"])
                    self.assertFalse(manifest["source_vintage_evidence"]["freshness_accepted"])
                    self.assertEqual(manifest["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"][0]["path"], f"geoparquet/{source.DATASET_SLUG}.parquet")
                    table = pq.ParquetFile(io.BytesIO(storage.read_key(f"{prefix}/geoparquet/{source.DATASET_SLUG}.parquet"))).read()
                    self.assertEqual(table.column_names, [*expected_names, "geometry"])
                    self.assertEqual([str(table.schema.field(n).type) for n in table.column_names], ["int64", *(["string"] * 10), "double", "double", "binary"])
                    self.assertEqual(table.column("OBJECTID").to_pylist(), [1, 2])
                    self.assertEqual(table.column("PORT").to_pylist(), ["Port A", None])
                    _primary, _geometry_column, crs = geometry_metadata(table.schema)
                    self.assertEqual(crs.to_epsg(), 4326)
                    point = shapely.from_wkb(table.column("geometry")[0].as_py())
                    self.assertEqual((point.x, point.y), (-73.0, 40.0))
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
                            [bts_intermodal_freight_rail_tofc_cofc],
                            run_config={"ops": {_ASSET_OP: {
                                "config": {"capture_directory": str(Path(tmp) / "capture2")}}}},
                            resources={"staging_storage": storage}, instance=instance,
                        )
                    self.assertTrue(cached_result.success)
                    cached = cached_result.output_for_node(_ASSET_OP)
                self.assertTrue(cached["reused"])
                self.assertEqual(cached["version"], version)
                check_fn = bts_intermodal_freight_rail_tofc_cofc_complete.op.compute_fn.decorated_fn
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
                        source.capture_bts_rail_tofc_cofc(storage, capture_directory=str(Path(tmp) / "capture"))
                quarantine_keys = storage.list_prefix(f"_quarantine/{source.DATASET_SLUG}")
                self.assertTrue(quarantine_keys)
                failure_key = next(key for key in quarantine_keys if key.endswith("/metadata/failure.json"))
                self.assertEqual(json.loads(storage.read_key(failure_key))["reason_code"], reason)


if __name__ == "__main__":
    unittest.main()

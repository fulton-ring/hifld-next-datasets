"""Shared fixture data and layer test base for the BTS NTAD Intermodal
Freight Facilities thin test modules.

``LAYER_FIXTURES`` holds each layer's source/asset objects and offline
responses; ``BtsIntermodalLayerTests`` binds one ``LAYER`` and provides the
full capture, seal, catalog, refresh, drift-quarantine and check coverage.
"""
from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq
import shapely
from dagster import DagsterInstance, materialize

from dagster_hifld.assets import bts_air_truck as air_assets
from dagster_hifld.assets import bts_marine_roro as marine_assets
from dagster_hifld.assets import bts_rail_tofc_cofc as rail_assets
from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import bts_air_truck as air_source
from dagster_hifld.sources import bts_marine_roro as marine_source
from dagster_hifld.sources import bts_rail_tofc_cofc as rail_source

_HTTPX_CLIENT = httpx.Client


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _air_fields():
    return [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "nullable": False, "domain": None},
        {"name": "LOCID", "type": "esriFieldTypeString", "alias": "LOCID", "length": 3, "nullable": True, "domain": None},
        {"name": "FACILITY_C", "type": "esriFieldTypeString", "alias": "FACILITY_C", "length": 200, "nullable": True, "domain": None},
        {"name": "DATE", "type": "esriFieldTypeString", "alias": "DATE", "length": 10, "nullable": True, "domain": None},
        {"name": "EST_AREA", "type": "esriFieldTypeDouble", "alias": "EST_AREA", "nullable": True, "domain": None},
    ]


def _marine_fields():
    return [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "nullable": False, "domain": None},
        {"name": "PORT", "type": "esriFieldTypeString", "alias": "PORT", "length": 254, "nullable": True, "domain": None},
        {"name": "TERMINAL", "type": "esriFieldTypeString", "alias": "TERMINAL", "length": 254, "nullable": True, "domain": None},
        {"name": "OPERATED_BY", "type": "esriFieldTypeString", "alias": "OPERATED_BY", "length": 255, "nullable": True, "domain": None},
        {"name": "TERM_ADDRESS", "type": "esriFieldTypeString", "alias": "TERM_ADDRESS", "length": 255, "nullable": True, "domain": None},
        {"name": "CITY", "type": "esriFieldTypeString", "alias": "CITY", "length": 254, "nullable": True, "domain": None},
        {"name": "STATE", "type": "esriFieldTypeString", "alias": "STATE", "length": 254, "nullable": True, "domain": None},
        {"name": "ZIP_CODE", "type": "esriFieldTypeString", "alias": "ZIP_CODE", "length": 254, "nullable": True, "domain": None},
        {"name": "RAIL_CO", "type": "esriFieldTypeString", "alias": "RAIL_CO", "length": 254, "nullable": True, "domain": None},
        {"name": "EQUIPMENT", "type": "esriFieldTypeString", "alias": "EQUIPMENT", "length": 254, "nullable": True, "domain": None},
        {"name": "ACCESS_RD", "type": "esriFieldTypeString", "alias": "ACCESS_RD", "length": 254, "nullable": True, "domain": None},
        {"name": "LAT", "type": "esriFieldTypeDouble", "alias": "LAT", "nullable": True, "domain": None},
        {"name": "LON", "type": "esriFieldTypeDouble", "alias": "LON", "nullable": True, "domain": None},
        {"name": "NAV_UNIT_ID", "type": "esriFieldTypeString", "alias": "NAV_UNIT_ID", "length": 255, "nullable": True, "domain": None},
    ]


def _rail_fields():
    names = ["OBJECTID", "TERMINAL", "TERM_ADDRESS", "CITY", "STATE", "ZIP_CODE", "PORT", "RAIL_CO", "EQUIPMENT", "ACCESS_RD", "SPLC", "LAT", "LON"]
    fields = []
    for name in names:
        kind = "esriFieldTypeOID" if name == "OBJECTID" else "esriFieldTypeDouble" if name in {"LAT", "LON"} else "esriFieldTypeString"
        field = {"name": name, "type": kind, "alias": name, "nullable": name != "OBJECTID", "domain": None}
        if kind == "esriFieldTypeString":
            field["length"] = 255
        fields.append(field)
    return fields


LAYER_FIXTURES = {
    "air_truck": {
        "source": air_source,
        "asset": air_assets.bts_intermodal_freight_air_to_truck,
        "check": air_assets.bts_intermodal_freight_air_to_truck_complete,
        "asset_module": "dagster_hifld.assets.bts_air_truck",
        "asset_op": "ingest__intermodal_freight_facilities_air_to_truck__intermodal_freight_facilities_air_to_truck",
        "capture": air_source.capture_bts_air_truck,
        "fields": _air_fields,
        "description": (
            "The dataset was updated from the Bureau of Transportation Statistics "
            "(BTS) and is part of the National Transportation Atlas Database (NTAD). "
            "This is the Air-to-Truck facilities layer."
        ),
        "item_extra": {},
        "layer_extra": {},
        "rows": [
            {"attributes": {"OBJECTID": 1, "LOCID": "JFK", "FACILITY_C": "Carrier A", "DATE": "09/2018", "EST_AREA": 74000.0},
             "geometry": {"x": -73.7781, "y": 40.6413}},
            {"attributes": {"OBJECTID": 2, "LOCID": "LAX", "FACILITY_C": None, "DATE": "09/2018", "EST_AREA": 2000.0},
             "geometry": {"x": -118.4085, "y": 33.9416}},
        ],
        "column_types": ["int64", "string", "string", "string", "double", "binary"],
        "sample_column": "LOCID",
        "sample_values": ["JFK", "LAX"],
        "first_geometry": (-73.7781, 40.6413),
        "vintage_evidence": False,
    },
    "marine_roro": {
        "source": marine_source,
        "asset": marine_assets.bts_intermodal_freight_marine_roll_on_roll_off,
        "check": marine_assets.bts_intermodal_freight_marine_roll_on_roll_off_complete,
        "asset_module": "dagster_hifld.assets.bts_marine_roro",
        "asset_op": "ingest__intermodal_freight_facilities_marine_roll_on_roll_off__intermodal_freight_facilities_marine_roll_on_roll_off",
        "capture": marine_source.capture_bts_marine_roro,
        "fields": _marine_fields,
        "description": (
            "The dataset was updated from the Bureau of Transportation Statistics "
            "(BTS) and is part of the National Transportation Atlas Database (NTAD). "
            "This is the Marine Roll-on/Roll-off facilities layer."
        ),
        "item_extra": {},
        "layer_extra": {},
        "rows": [
            {"attributes": {"OBJECTID": 1, "PORT": "Port A", "TERMINAL": "Terminal A", "OPERATED_BY": "Operator A", "TERM_ADDRESS": "1 Main St", "CITY": "City A", "STATE": "NY", "ZIP_CODE": "10001", "RAIL_CO": None, "EQUIPMENT": "Ro/Ro", "ACCESS_RD": "Main Rd", "LAT": 40.0, "LON": -73.0, "NAV_UNIT_ID": "NYC1"}, "geometry": {"x": -73.0, "y": 40.0}},
            {"attributes": {"OBJECTID": 2, "PORT": "Port B", "TERMINAL": "Terminal B", "OPERATED_BY": None, "TERM_ADDRESS": None, "CITY": "City B", "STATE": "CA", "ZIP_CODE": None, "RAIL_CO": "UP", "EQUIPMENT": "Ro/Ro", "ACCESS_RD": None, "LAT": 33.0, "LON": -118.0, "NAV_UNIT_ID": "LAX1"}, "geometry": {"x": -118.0, "y": 33.0}},
        ],
        "column_types": ["int64", *(["string"] * 10), "double", "double", "string", "binary"],
        "sample_column": "PORT",
        "sample_values": ["Port A", "Port B"],
        "first_geometry": (-73.0, 40.0),
        "vintage_evidence": False,
    },
    "rail_tofc_cofc": {
        "source": rail_source,
        "asset": rail_assets.bts_intermodal_freight_rail_tofc_cofc,
        "check": rail_assets.bts_intermodal_freight_rail_tofc_cofc_complete,
        "asset_module": "dagster_hifld.assets.bts_rail_tofc_cofc",
        "asset_op": "ingest__intermodal_freight_facilities_rail_tofc_cofc__intermodal_freight_facilities_rail_tofc_cofc",
        "capture": rail_source.capture_bts_rail_tofc_cofc,
        "fields": _rail_fields,
        "description": (
            "The dataset was compiled on November 15, 2024 and updated on February 28, 2025 "
            "from the Bureau of Transportation Statistics "
            "(BTS) and is part of the National Transportation Atlas Database (NTAD). "
            "This is the Rail TOFC/COFC facilities layer."
        ),
        "item_extra": {"created": 1720644100000, "modified": 1784137096000},
        "layer_extra": {"editingInfo": {"lastEditDate": 1734380202378,
                                        "schemaLastEditDate": 1720644927074,
                                        "dataLastEditDate": 1720644111319}},
        "rows": [
            {"attributes": {"OBJECTID": 1, "TERMINAL": "Terminal A", "TERM_ADDRESS": "1 Main St", "CITY": "City A", "STATE": "NY", "ZIP_CODE": "10001", "PORT": "Port A", "RAIL_CO": "Rail A", "EQUIPMENT": "TOFC", "ACCESS_RD": "Main Rd", "SPLC": "012345678", "LAT": 40.0, "LON": -73.0}, "geometry": {"x": -73.0, "y": 40.0}},
            {"attributes": {"OBJECTID": 2, "TERMINAL": "Terminal B", "TERM_ADDRESS": None, "CITY": "City B", "STATE": "CA", "ZIP_CODE": None, "PORT": None, "RAIL_CO": "UP", "EQUIPMENT": "COFC", "ACCESS_RD": None, "SPLC": "000000001", "LAT": 33.0, "LON": -118.0}, "geometry": {"x": -118.0, "y": 33.0}},
        ],
        "column_types": ["int64", *(["string"] * 10), "double", "double", "binary"],
        "sample_column": "PORT",
        "sample_values": ["Port A", None],
        "first_geometry": (-73.0, 40.0),
        "vintage_evidence": True,
    },
}


def _fixture_factory(layer, mode="ok"):
    fixture = LAYER_FIXTURES[layer]
    source = fixture["source"]
    fields = fixture["fields"]()
    ids = [] if mode == "empty" else ([1, 2] if mode != "id" else [1, 3])
    if mode == "schema":
        fields[-1]["type"] = "esriFieldTypeInteger"
    item = {"id": source.ITEM_ID, "owner": "USDOT_BTS", "title": source.LAYER_NAME,
            "url": source.SERVICE_URL, "access": "public", "description": fixture["description"],
            "licenseInfo": "U.S. public domain", **fixture["item_extra"]}
    service = {"serviceItemId": source.ITEM_ID, "layers": [{"id": 0, "name": source.LAYER_NAME}]}
    layer_doc = {
        "name": source.LAYER_NAME, "geometryType": "esriGeometryPoint", "objectIdField": "OBJECTID",
        "hasZ": False, "hasM": False,
        "extent": {"spatialReference": {"latestWkid": 4326}},
        "fields": fields,
        "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
        **fixture["layer_extra"],
    }
    rows = fixture["rows"]

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
            result = layer_doc
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


class BtsIntermodalLayerTests(unittest.TestCase):
    LAYER = None

    def _synthetic_pins(self):
        fixture = LAYER_FIXTURES[self.LAYER]
        return patch.multiple(
            fixture["source"],
            EXPECTED_SOURCE_COUNT=2,
            EXPECTED_SOURCE_ID_SHA256=hashlib.sha256(_canonical([1, 2])).hexdigest(),
            EXPECTED_SOURCE_SCHEMA_SHA256=hashlib.sha256(
                _canonical(fixture["fields"]())).hexdigest(),
        )

    def test_complete_capture_seal_catalog_refresh_and_drift_quarantine(self):
        if self.LAYER is None:
            self.skipTest("bind LAYER in a thin test module")
        fixture = LAYER_FIXTURES[self.LAYER]
        source = fixture["source"]
        asset = fixture["asset"]
        check = fixture["check"]
        keys = {key for asset_def in hifld_ingest_assets for key in asset_def.keys}
        self.assertIn(asset.key, keys)

        with tempfile.TemporaryDirectory() as tmp, self._synthetic_pins(), patch(
            f"{fixture['asset_module']}.EXPECTED_SOURCE_COUNT", 2
        ):
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            instance_dir = Path(tmp) / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            factory = _fixture_factory(self.LAYER)
            with patch.object(source.httpx, "Client", side_effect=lambda *a, **kw: factory()):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    first_result = materialize(
                        [asset],
                        run_config={"ops": {fixture["asset_op"]: {
                            "config": {"capture_directory": str(Path(tmp) / "capture")}}}},
                        resources={"staging_storage": storage}, instance=instance,
                    )
                    self.assertTrue(first_result.success)
                    first = first_result.output_for_node(fixture["asset_op"])
                    self.assertEqual(first["feature_count"], 2)
                    self.assertTrue(first["source_population_complete"])
                    version = first["version"]
                    prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{version}"
                    audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
                    manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
                    ready = json.loads(storage.read_key(f"{prefix}/metadata/candidate_ready.json"))
                    expected_names = [field["name"] for field in fixture["fields"]()]
                    self.assertEqual([field["name"] for field in audit["source_fields"]], expected_names)
                    self.assertFalse(manifest["approved"])
                    self.assertFalse(ready["approved"])
                    if fixture["vintage_evidence"]:
                        self.assertEqual(manifest["source_vintage_evidence"]["portal_item_modified_epoch_ms"], 1784137096000)
                        self.assertIn("updated on February 28, 2025", manifest["source_vintage_evidence"]["portal_description"])
                        self.assertEqual(manifest["source_vintage_evidence"]["layer_editing_info"]["dataLastEditDate"], 1720644111319)
                        self.assertIsNone(manifest["source_vintage_evidence"]["release_date_selected"])
                        self.assertFalse(manifest["source_vintage_evidence"]["freshness_accepted"])
                    else:
                        self.assertNotIn("source_vintage_evidence", manifest)
                    self.assertEqual(manifest["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"][0]["path"], f"geoparquet/{source.DATASET_SLUG}.parquet")
                    table = pq.ParquetFile(io.BytesIO(storage.read_key(f"{prefix}/geoparquet/{source.DATASET_SLUG}.parquet"))).read()
                    self.assertEqual(table.column_names, [*expected_names, "geometry"])
                    self.assertEqual([str(table.schema.field(n).type) for n in table.column_names], fixture["column_types"])
                    self.assertEqual(table.column("OBJECTID").to_pylist(), [1, 2])
                    self.assertEqual(table.column(fixture["sample_column"]).to_pylist(), fixture["sample_values"])
                    _primary, _geometry_column, crs = geometry_metadata(table.schema)
                    self.assertEqual(crs.to_epsg(), 4326)
                    point = shapely.from_wkb(table.column("geometry")[0].as_py())
                    self.assertEqual((point.x, point.y), fixture["first_geometry"])
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
                            [asset],
                            run_config={"ops": {fixture["asset_op"]: {
                                "config": {"capture_directory": str(Path(tmp) / "capture2")}}}},
                            resources={"staging_storage": storage}, instance=instance,
                        )
                    self.assertTrue(cached_result.success)
                    cached = cached_result.output_for_node(fixture["asset_op"])
                self.assertTrue(cached["reused"])
                self.assertEqual(cached["version"], version)
                check_fn = check.op.compute_fn.decorated_fn
                self.assertTrue(check_fn(cached).passed)

        for mode, reason in (("count", "source_count_or_id_roster_drift"),
                             ("schema", "source_schema_drift"),
                             ("id", "source_id_roster_drift"),
                             ("empty", "source_seed_id_roster_missing_or_invalid"),
                             ("drop", "source_page_ids_do_not_match_requested_slice")):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp, self._synthetic_pins():
                storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
                factory = _fixture_factory(self.LAYER, mode)
                with patch.object(source.httpx, "Client", side_effect=lambda *a, **kw: factory()):
                    with self.assertRaisesRegex(ValueError, reason):
                        fixture["capture"](storage, capture_directory=str(Path(tmp) / "capture"))
                quarantine_keys = storage.list_prefix(f"_quarantine/{source.DATASET_SLUG}")
                self.assertTrue(quarantine_keys)
                failure_key = next(key for key in quarantine_keys if key.endswith("/metadata/failure.json"))
                self.assertEqual(json.loads(storage.read_key(failure_key))["reason_code"], reason)

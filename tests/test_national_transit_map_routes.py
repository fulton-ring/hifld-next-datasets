"""Synthetic checks for the current BTS NTAD routes source adapter."""
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

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.national_transit_map_routes import (
    national_transit_map_routes,
    national_transit_map_routes_complete,
    national_transit_map_routes_draft_schedule,
)
from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import national_transit_map_routes as source

_HTTPX_CLIENT = httpx.Client


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def _fields():
    rows = []
    for name, kind in zip(source.EXPECTED_FIELD_NAMES, source.EXPECTED_FIELD_TYPES):
        field = {"name": name, "type": kind, "alias": name,
                 "nullable": name != "OBJECTID", "domain": None}
        if kind == "esriFieldTypeString":
            field["length"] = 254
        rows.append(field)
    return rows


def _fixture_factory():
    fields = _fields()
    schema_sha = hashlib.sha256(_canonical(fields)).hexdigest()
    ids_sha = hashlib.sha256(_canonical([1, 2])).hexdigest()
    item = {
        "id": source.ITEM_ID,
        "owner": "USDOT_BTS",
        "orgId": "xOi1kZaI0eWDREZv",
        "title": source.LAYER_NAME,
        "url": source.SERVICE_URL,
        "access": "public",
        "licenseInfo": "Creative Commons Attribution 3.0 United States (CC-BY-3.0)",
        "description": (
            "The National Transit Map Routes dataset was compiled on September 23, 2026 "
            "from the Bureau of Transportation Statistics (BTS) and is part of the "
            "National Transportation Atlas Database (NTAD)."
        ),
    }
    service = {
        "serviceItemId": source.ITEM_ID,
        "layers": [{"id": 0, "name": source.LAYER_NAME,
                    "type": "Feature Layer", "geometryType": "esriGeometryPolyline"}],
        "maxRecordCount": 1000,
    }
    layer = {
        "name": source.LAYER_NAME,
        "geometryType": "esriGeometryPolyline",
        "objectIdField": "OBJECTID",
        "hasZ": False,
        "hasM": False,
        "extent": {"spatialReference": {"wkid": 4326, "latestWkid": 4326}},
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "maxRecordCount": 2000,
        "fields": fields,
        "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
    }
    features = []
    for oid, routeshort, pathsets, length in (
        (1, "R1", [[[0.0, 1.0], [2.0, 3.0]], [[4.0, 5.0], [6.0, 7.0]]], 4.0),
        (2, "R2", [[[8.0, 9.0], [10.0, 11.0]]], 2.0),
    ):
        attrs = {field["name"]: None for field in fields}
        attrs.update({"OBJECTID": oid, "ntd_id": f"NTD{oid}", "feed_id": f"feed{oid}",
                      "shape_id": f"shape{oid}", "route_id": f"route{oid}",
                      "agency_id": f"agency{oid}", "route_short_name": routeshort,
                      "route_type": "3", "route_type_text": "Bus", "download_date": "2026-09-23",
                      "Shape__Length": length})
        features.append({"attributes": attrs, "geometry": {"paths": pathsets}})

    def factory():
        def handler(request):
            query = parse_qs(request.url.query.decode())
            path = request.url.path
            if path.endswith("/query") and query.get("returnCountOnly") == ["true"]:
                result = {"count": 2}
            elif path.endswith("/query") and query.get("returnIdsOnly") == ["true"]:
                result = {"objectIdFieldName": "OBJECTID", "objectIds": [1, 2]}
            elif path.endswith("/query"):
                result = {"spatialReference": {"wkid": 4326, "latestWkid": 4326},
                          "features": features}
            elif path.endswith("/FeatureServer/0"):
                result = layer
            elif path.endswith("/FeatureServer"):
                result = service
            elif path.endswith("/sharing/rest/content/items/" + source.ITEM_ID):
                result = item
            else:
                return httpx.Response(404, request=request)
            body = _canonical(result)
            return httpx.Response(200, stream=httpx.ByteStream(body), request=request,
                                  headers={"content-length": str(len(body))})
            return _HTTPX_CLIENT(transport=httpx.MockTransport(handler), follow_redirects=False)

    return factory, fields, features, schema_sha, ids_sha


class NationalTransitMapRoutesTests(unittest.TestCase):
    def test_mocked_capture_preserves_schema_and_native_multiline_paths(self):
        factory, fields, features, schema_sha, ids_sha = _fixture_factory()
        patchers = (
            patch.object(source, "EXPECTED_SOURCE_COUNT", 2),
            patch.object(source, "EXPECTED_SOURCE_SCHEMA_SHA256", schema_sha),
            patch.object(source, "EXPECTED_SOURCE_ID_SHA256", ids_sha),
            patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: factory()),
        )
        with tempfile.TemporaryDirectory(dir="/workspace/work/board-supervisor") as tmp:
            for p in patchers:
                p.start()
            self.addCleanup(lambda: [p.stop() for p in reversed(patchers)])
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            instance_dir = Path(tmp) / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            capture_root = Path(tmp) / "capture"
            with DagsterInstance.from_config(str(instance_dir)) as instance:
                result = materialize(
                    [national_transit_map_routes],
                    run_config={"ops": {national_transit_map_routes.op.name: {
                        "config": {"capture_directory": str(capture_root)}}}},
                    resources={"staging_storage": storage}, instance=instance,
                )
            self.assertTrue(result.success)
            candidate = result.output_for_node(national_transit_map_routes.op.name)
            self.assertEqual(candidate["feature_count"], 2)
            self.assertTrue(candidate["source_population_complete"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
            audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
            self.assertEqual(audit["source_schema_sha256"], schema_sha)
            self.assertEqual([item["name"] for item in audit["source_fields"]],
                             [field["name"] for field in fields])
            self.assertEqual(audit["source_fields"][-1]["name"], "Shape__Length")
            self.assertEqual(audit["source_geometry_type"], "esriGeometryPolyline")
            self.assertFalse(audit["rights_accepted"])
            self.assertFalse(audit["release_accepted"])
            parquet_key = f"{prefix}/geoparquet/{source.DATASET_SLUG}.parquet"
            table = pq.read_table(io.BytesIO(storage.read_key(parquet_key)))
            self.assertEqual(table.column_names, source.EXPECTED_FIELD_NAMES + ["geometry"])
            self.assertEqual(table.column("Shape__Length").to_pylist(), [4.0, 2.0])
            _primary, _geometry_col, crs = geometry_metadata(table.schema)
            self.assertEqual(crs.to_epsg(), 4326)
            geom = shapely.from_wkb(table.column("geometry")[0].as_py())
            self.assertEqual(geom.geom_type, "MultiLineString")
            self.assertEqual(list(geom.geoms[0].coords), [(0.0, 1.0), (2.0, 3.0)])
            self.assertEqual(list(geom.geoms[1].coords), [(4.0, 5.0), (6.0, 7.0)])
            self.assertIn(f"{prefix}/raw/page_000.body", storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/raw/ids_before.body", storage.list_prefix(prefix))

    def test_registered_draft_is_stopped_and_gate_keeps_approvals_false(self):
        self.assertIn(national_transit_map_routes, hifld_ingest_assets)
        self.assertEqual(national_transit_map_routes_draft_schedule.default_status.value, "STOPPED")
        check = national_transit_map_routes_complete.node_def.compute_fn.decorated_fn
        result = check({"source_population_complete": True, "feature_count": 2,
                        "source_null_geometry_count": 0, "source_invalid_geometry_count": 0})
        self.assertFalse(result.passed)
        self.assertFalse(result.metadata["rights_accepted"].value)
        self.assertFalse(result.metadata["release_accepted"].value)


if __name__ == "__main__":
    unittest.main()

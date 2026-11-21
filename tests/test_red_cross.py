"""Focused native-drift tests for the rebuilt Red Cross divisions adapter."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import urlencode

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.sources import red_cross as source


def _json(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def _ring(shift, scale=1.0):
    x, y = shift
    return [[x, y], [x + 5 * scale, y], [x + 5 * scale, y + 5 * scale], [x, y + 5 * scale], [x, y]]


def _bowtie(shift):
    x, y = shift
    return [[x, y], [x + 4, y + 4], [x + 4, y], [x, y + 4], [x, y]]


def _fixture_rows():
    rows = []
    for index, identity in enumerate(source.EXPECTED_IDS):
        attributes = {"OBJECTID": identity, "DCODE": f"D2{index + 1}",
                      "Division": f"Division {identity}", "States": f"ST-{identity}",
                      "Acres": float(identity) * 1000.5, "SQ_MI": float(identity) * 1.5,
                      "Division_DCODE": f"D2{index + 1} - Division {identity}",
                      "Shape__Area": float(identity) * 2.25, "Shape__Length": float(identity) * 6.0}
        if identity == 6:
            geometry = {"rings": [_bowtie((100 + identity, 100 + identity))]}
        else:
            geometry = {"rings": [_ring((10 * identity, 10 * identity))]}
        rows.append({"attributes": attributes, "geometry": geometry})
    return rows


def _fixture():
    fields = [dict(zip(("name", "type", "nullable", "length"), field))
              for field in source.FIELDS]
    layer = {"id": 1, "name": source.EXPECTED_LAYER, "geometryType": "esriGeometryPolygon",
             "objectIdField": "OBJECTID", "serviceItemId": source.ITEM_ID,
             "hasZ": False, "hasM": False,
             "spatialReference": {"wkid": 102100, "latestWkid": 3857}, "fields": fields}
    item = {"id": source.ITEM_ID, "owner": source.PUBLISHER_ACCOUNT, "access": "public",
            "url": source.SERVICE, "orgId": source.ORGANIZATION_ID}
    owner = {"username": source.PUBLISHER_ACCOUNT}
    rows = _fixture_rows()
    ids = list(source.EXPECTED_IDS)
    bodies = {
        "layer_before": _json(layer), "layer_after": _json(layer),
        "item": _json(item), "owner": _json(owner),
        "count_before": _json({"count": len(ids)}),
        "ids_before": _json({"objectIdFieldName": "OBJECTID", "objectIds": ids}),
        "count_after": _json({"count": len(ids)}),
        "ids_after": _json({"objectIdFieldName": "OBJECTID", "objectIds": ids}),
        "page_0000": _json({"objectIdFieldName": "OBJECTID", "geometryType": "esriGeometryPolygon",
                            "spatialReference": layer["spatialReference"],
                            "fields": [{k: v for k, v in field.items() if k not in {"nullable", "editable"}}
                                       for field in fields],
                            "features": rows, "exceededTransferLimit": False}),
        "tail": _json({"features": [], "exceededTransferLimit": False}),
    }
    receipts = {}
    for name, body in bodies.items():
        if name.startswith("layer_"):
            params, url = {"f": "pjson"}, source.SERVICE_URL
        elif name == "item":
            params, url = {"f": "json"}, source.ITEM_API_URL
        elif name == "owner":
            params, url = {"f": "json"}, source.OWNER_API_URL
        elif name.startswith("count_"):
            params, url = {"f": "json", "where": "1=1", "returnCountOnly": "true"}, source.SERVICE_URL + "/query"
        elif name.startswith("ids_"):
            params, url = {"f": "json", "where": "1=1", "returnIdsOnly": "true"}, source.SERVICE_URL + "/query"
        elif name == "page_0000":
            params, url = {"f": "json", "where": "1=1", "outFields": "*", "returnGeometry": "true",
                           "orderByFields": "OBJECTID ASC", "resultOffset": 0, "resultRecordCount": 2000}, source.SERVICE_URL + "/query"
        else:
            params, url = {"f": "json", "where": "1=1", "outFields": "OBJECTID", "returnGeometry": "false",
                           "orderByFields": "OBJECTID ASC", "resultOffset": source.EXPECTED_COUNT,
                           "resultRecordCount": 2000}, source.SERVICE_URL + "/query"
        full_url = url + "?" + urlencode(params)
        receipts[name] = {"status": 200, "url": full_url, "final_url": full_url,
                          "fetched_at": "2026-11-12T11:00:00.000000+00:00",
                          "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                          "complete_body": True}
    return {name: {"body": body, "receipt": receipts[name], "receipt_body": _json(receipts[name]),
                   "retrieved_at": receipts[name]["fetched_at"],
                   "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
            for name, body in bodies.items()}


def _patched_censuses(captures):
    page = json.loads(captures["page_0000"]["body"])
    rows = page["features"]
    geometry_census = hashlib.sha256(source._CANONICAL(
        [[row["attributes"]["OBJECTID"], row["geometry"]] for row in rows])).hexdigest()
    attribute_census = hashlib.sha256(source._CANONICAL(
        [[row["attributes"]["OBJECTID"], row["attributes"]] for row in rows])).hexdigest()
    previous = (source.GEOMETRY_SHA256, source.ATTRIBUTE_SHA256)
    source.GEOMETRY_SHA256, source.ATTRIBUTE_SHA256 = geometry_census, attribute_census
    return previous


class RedCrossDivisionsNativeTests(unittest.TestCase):
    def setUp(self):
        self._previous_censuses = _patched_censuses(_fixture())

    def tearDown(self):
        source.GEOMETRY_SHA256, source.ATTRIBUTE_SHA256 = self._previous_censuses

    def test_complete_typed_candidate_keeps_rings_and_reports_source_validity(self):
        captures = _fixture()
        layer, rows, ids, geometries = source._validate_population(captures)
        parquet, geo = source._geoparquet(source._schema(layer), rows, geometries)
        table = pq.read_table(pa.BufferReader(parquet))
        self.assertEqual(table.num_rows, 6)
        self.assertEqual(table.column("OBJECTID").to_pylist(), ids)
        self.assertEqual(table.column("geometry").null_count, 0)
        self.assertEqual(table.column("geometry").to_pylist(),
                         [geometry.wkb for geometry in geometries])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 3857)
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Polygon"])
        self.assertEqual(source._invalid_geometry_ids(rows, geometries), [6])
        manifest = source._manifest(captures, layer, source._schema(layer), parquet, geo,
                                    captures, source._CANONICAL({"schema_version": 1, "rows": []}), [6])
        self.assertFalse(manifest["promotion_allowed"])
        self.assertEqual(manifest["lineage"]["invalid_geometry_ids"], [6])

    def test_identifier_geometry_and_ring_drift_fail_closed(self):
        captures = _fixture()
        page = json.loads(captures["page_0000"]["body"])
        page["features"][0]["attributes"]["OBJECTID"] = 999
        captures["page_0000"]["body"] = _json(page)
        with self.assertRaisesRegex(ValueError, "ordered_identifier_census_drift"):
            source._validate_population(captures)

        captures = _fixture()
        page = json.loads(captures["page_0000"]["body"])
        page["features"][2]["geometry"]["rings"][0][1][1] = 99.0
        captures["page_0000"]["body"] = _json(page)
        with self.assertRaisesRegex(ValueError, "geometry_census_drift_requires_review"):
            source._validate_population(captures)

        captures = _fixture()
        page = json.loads(captures["page_0000"]["body"])
        page["features"][1]["geometry"]["rings"][0].pop()
        captures["page_0000"]["body"] = _json(page)
        with self.assertRaisesRegex(ValueError, "unclosed_or_short_ring"):
            source._validate_population(captures)

        captures = _fixture()
        page = json.loads(captures["page_0000"]["body"])
        page["features"][0]["attributes"]["Division"] = "Renamed Division"
        captures["page_0000"]["body"] = _json(page)
        with self.assertRaisesRegex(ValueError, "attribute_census_drift_requires_review"):
            source._validate_population(captures)

    def test_request_scope_and_population_drift_fail_closed(self):
        captures = _fixture()
        captures["tail"]["receipt"]["url"] = source.SERVICE_URL + "/query?f=json"
        captures["tail"]["receipt"]["final_url"] = captures["tail"]["receipt"]["url"]
        with self.assertRaisesRegex(ValueError, "request_scope_drift"):
            source._validate_population(captures)

        captures = _fixture()
        captures["count_after"]["body"] = _json({"count": 7})
        with self.assertRaisesRegex(ValueError, "population_changed_during_capture"):
            source._validate_population(captures)

        captures = _fixture()
        captures["tail"]["body"] = _json({"error": {"code": 400, "message": "Cannot perform query."}})
        with self.assertRaisesRegex(ValueError, "error_response:400"):
            source._validate_population(captures)

        captures = _fixture()
        layer = json.loads(captures["layer_before"]["body"])
        layer["fields"][3]["length"] = 10
        captures["layer_before"]["body"] = _json(layer)
        with self.assertRaisesRegex(ValueError, "native_field_declaration_drift"):
            source._validate_population(captures)

    def test_capture_directory_replay_stages_complete_candidate(self):
        captures = _fixture()
        with tempfile.TemporaryDirectory() as temporary:
            capture_dir = Path(temporary) / "capture"
            capture_dir.mkdir()
            for name in source.CAPTURE_NAMES:
                (capture_dir / f"{name}.body").write_bytes(captures[name]["body"])
                (capture_dir / f"{name}.receipt.json").write_bytes(captures[name]["receipt_body"])
            captures_loaded = source._load_capture(capture_dir)
            layer, rows, ids, geometries = source._validate_population(captures_loaded)
            from dagster_hifld.resources import StagingStorageResource
            staging = StagingStorageResource(use_local=True, local_dir=temporary, prefix="", backend="local")
            result = source.capture_and_stage(staging, "v0.0.0-fixture", str(capture_dir))
            self.assertEqual(result["feature_count"], 6)
            self.assertEqual(result["source_invalid_geometry_ids"], [6])
            self.assertFalse(result["promotion_allowed"])
            self.assertFalse(result["source_authority_accepted"])
            staged = staging.list_keys(source.DATASET_SLUG, source.FILE_SLUG, "v0.0.0-fixture")
            expected = {f"raw_arcgis/{name}.{suffix}" for name in source.CAPTURE_NAMES
                        for suffix in ("body", "receipt.json")}
            expected |= {f"geoparquet/{source.FILE_SLUG}.parquet",
                         "metadata/source_feature_presence.json", "metadata/source_manifest.json"}
            self.assertEqual(set(staged), {f"{source.DATASET_SLUG}/{source.FILE_SLUG}/v0.0.0-fixture/{name}"
                                           for name in expected})
            with self.assertRaisesRegex(ValueError, "version_prefix_already_exists"):
                source.capture_and_stage(staging, "v0.0.0-fixture", str(capture_dir))

    def test_capture_roster_mismatch_refuses(self):
        captures = _fixture()
        with tempfile.TemporaryDirectory() as temporary:
            capture_dir = Path(temporary) / "capture"
            capture_dir.mkdir()
            for name in source.CAPTURE_NAMES:
                (capture_dir / f"{name}.body").write_bytes(captures[name]["body"])
                (capture_dir / f"{name}.receipt.json").write_bytes(captures[name]["receipt_body"])
            (capture_dir / "extra.body").write_bytes(b"unlisted")
            with self.assertRaisesRegex(ValueError, "capture_roster_mismatch"):
                source._load_capture(capture_dir)


class RegistrationTests(unittest.TestCase):
    def test_ingest_check_job_and_stopped_schedule_are_registered(self):
        from dagster import DefaultScheduleStatus
        from dagster_hifld.assets.red_cross import (red_cross_divisions,
                                                    red_cross_divisions_draft_job,
                                                    red_cross_divisions_draft_schedule,
                                                    red_cross_divisions_source_population)
        from dagster_hifld.definitions import defs
        repo = defs.get_repository_def()
        self.assertEqual(
            repo.get_job("red_cross_divisions_draft_refresh").asset_layer.executable_asset_keys,
            red_cross_divisions.keys)
        self.assertTrue(set(red_cross_divisions_source_population.check_keys)
                        <= defs.resolve_asset_graph().asset_check_keys)
        schedule = repo.get_schedule_def("red_cross_divisions_draft_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()

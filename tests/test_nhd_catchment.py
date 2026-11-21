"""Focused tests for the USGS 3DHP_all Catchment draft pipeline."""
import io
import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import httpx
from dagster import AssetSpec
import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.assets.publish import verify_release
from dagster_hifld.partitions import build_publish_partition_key
from dagster_hifld.resources import PublishedStorageResource
from dagster_hifld.assets.nhd_catchment import nhd_catchment, nhd_catchment_complete
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import capture_common
from dagster_hifld.sources import nhd_catchment as src

FIXTURE_COUNT = 7
FIXTURE_OBJECTIDS = tuple(range(1, FIXTURE_COUNT + 1))


def _fixture_attrs(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    return {
        "OBJECTID": oid,
        "id3dhp": f"{index + 1:07X}",
        "featuredate": 1709563014000 + index,
        "mainstemid": f"0000{index}X" if index != 1 else None,
        "areasqkm": 0.0317 + index if index != 2 else None,
        "totestdrainareasqkm": 1.25 + index,
        "workunitid": "07060001",
        "Shape__Length": 6388.035227755926 + index,
        "Shape__Area": 461231.6270300725 + index,
    }


def _cw_square(x0: float, y0: float, size: float, z: float = 0.0) -> list:
    return [[x0, y0, z], [x0 + size, y0, z], [x0 + size, y0 - size, z],
            [x0, y0 - size, z], [x0, y0, z]]


def _ccw_square(x0: float, y0: float, size: float, z: float = 0.0) -> list:
    return [[x0, y0, z], [x0, y0 - size, z], [x0 + size, y0 - size, z],
            [x0 + size, y0, z], [x0, y0, z]]


def _fixture_geometry(oid: object) -> object:
    if oid == FIXTURE_OBJECTIDS[0]:
        rings = [_cw_square(0.0, 0.0, 1.0)]
    elif oid == FIXTURE_OBJECTIDS[1]:
        rings = [_cw_square(0.0, 0.0, 1.0), _ccw_square(0.25, -0.5, 0.25)]
    elif oid == FIXTURE_OBJECTIDS[2]:
        rings = [_cw_square(0.0, 0.0, 1.0), _cw_square(2.0, 0.0, 1.0)]
    elif oid == FIXTURE_OBJECTIDS[3]:
        rings = [_cw_square(0.0, -1.0, 3.0), _cw_square(0.5, -1.5, 1.0),
                 _ccw_square(0.75, -1.75, 0.5)]
    elif oid == FIXTURE_OBJECTIDS[4]:
        return None
    elif oid == FIXTURE_OBJECTIDS[5]:
        rings = [[["NaN", "NaN", "NaN"], [1.0, 0.0, 0.0], [1.0, -1.0, 0.0],
                  [0.0, -1.0, 0.0], ["NaN", "NaN", "NaN"]]]
    else:
        rings = [[[0.0, 0.0, 0.0], [0.0, 0.0, 0.0], [1.0, 1.0, 0.0], [1.0, 1.0, 0.0], [0.0, 0.0, 0.0]]]
    return {"hasZ": True, "rings": rings}


def _fixture_rows() -> list[dict]:
    return [{"attributes": _fixture_attrs(oid), "geometry": _fixture_geometry(oid)}
            for oid in FIXTURE_OBJECTIDS]


def _fixture_layer_fields() -> list[dict]:
    fields = []
    for name, esri_type, nullable, length in src.NATIVE_FIELDS:
        field = {"name": name, "alias": name if name != "Shape__Length" else "shape_Length",
                 "type": esri_type, "nullable": nullable, "domain": None}
        if length is not None:
            field["length"] = length
        fields.append(field)
    return fields


def _fixture_layer_doc(fields: list[dict]) -> dict:
    return {"id": src.LAYER_ID, "name": src.LAYER_NAME, "type": "Feature Layer",
            "geometryType": src.GEOMETRY_TYPE, "hasZ": True, "hasM": False,
            "maxRecordCount": src.MAX_RECORD_COUNT,
            "advancedQueryCapabilities": {"supportsPagination": True},
            "sourceSpatialReference": {"wkid": src.NATIVE_LAYER_SR["wkid"],
                                       "latestWkid": src.NATIVE_LAYER_SR["latestWkid"]},
            "extent": {**src.EXPECTED_LAYER_EXTENT,
                       "spatialReference": {"latestWkid": src.NATIVE_LAYER_SR["latestWkid"]}},
            "fields": fields}


def _fixture_service_doc() -> dict:
    return {"copyrightText": src.EXPECTED_COPYRIGHT_TEXT,
            "serviceDescription": "The USGS 3D Hydrography Program (3DHP) 3DHP_all service.",
            "spatialReference": {"wkid": 102100, "latestWkid": 3857},
            "layers": [{"id": layer_id, "name": name}
                       for layer_id, name in src.EXPECTED_SERVICE_LAYERS]}


def _fixture_alias_service_doc() -> dict:
    doc = _fixture_service_doc()
    core = len(src.EXPECTED_SERVICE_LAYERS) - len(src.SPATIAL_METADATA_IDS)
    doc["layers"] = doc["layers"][:core] + [
        {"id": 90, "name": "Spatial Metadata - DEM LimitationArea"},
        {"id": 92, "name": "Spatial Metadata - EDH Workunit Area"},
        {"id": 94, "name": "Spatial Metadata - Elevation Workunit Area"},
    ]
    return doc


class JSONStream(httpx.SyncByteStream):
    def __init__(self, payload):
        if isinstance(payload, bytes):
            self.body = payload
        else:
            self.body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()

    def __iter__(self):
        yield self.body

    def close(self):
        return None


def make_responder(rows: list[dict], *, layer_fields=None, count=None, ids=None,
                   ids_seed=None, service=None, alias=None, layer=None, doc_page=None,
                   refuse_counts=(), page_has_m=None, layer_after_mutate=None) -> httpx.MockTransport:
    count = count if count is not None else len(rows)
    objectids = ids if ids is not None else list(FIXTURE_OBJECTIDS)
    seed_ids = ids_seed if ids_seed is not None else objectids
    layer_fields = layer_fields if layer_fields is not None else _fixture_layer_fields()
    layer_doc_calls = [0]
    ids_calls = [0]

    def layer_doc():
        if callable(layer):
            return layer()
        if layer is not None:
            return layer
        return _fixture_layer_doc(layer_fields)

    service_doc = service if service is not None else _fixture_service_doc()
    alias_doc = alias if alias is not None else _fixture_alias_service_doc()

    def respond(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path
        query = parse_qs(urlparse(str(request.url)).query)
        if path.startswith("/3d-hydrography-program/"):
            return httpx.Response(200, stream=JSONStream(doc_page or
                b"<html>3DHP access: https://3dhp.nationalmap.gov/arcgis/rest/services"
                b"/usgs_3dhp_all/FeatureServer</html>"),
                headers={"content-type": "text/html"})
        if path.rstrip("/").endswith("FeatureServer"):
            document = alias_doc if request.url.host == "hydro.nationalmap.gov" else service_doc
            return httpx.Response(200, stream=JSONStream(document),
                                  headers={"content-type": "application/json"})
        if path.rstrip("/").endswith(f"/{src.LAYER_ID}"):
            layer_doc_calls[0] += 1
            document = layer_doc()
            if layer_after_mutate is not None and layer_doc_calls[0] > 1:
                document = layer_after_mutate(document)
            return httpx.Response(200, stream=JSONStream(document),
                                  headers={"content-type": "application/json"})
        if query.get("returnCountOnly") == ["true"]:
            return httpx.Response(200, stream=JSONStream({"count": count}),
                                  headers={"content-type": "application/json"})
        if query.get("returnIdsOnly") == ["true"]:
            ids_calls[0] += 1
            # The module fetches the seed roster first, then the before
            # bracket; keep them independently controllable so the
            # seed-vs-before drift is exercisable.
            served = seed_ids if ids_calls[0] == 1 else objectids
            return httpx.Response(200, stream=JSONStream(
                {"objectIdFieldName": "OBJECTID", "objectIds": served}),
                headers={"content-type": "application/json"})
        if query.get("resultOffset") is not None:
            offset = int(query["resultOffset"][0])
            record_count = int(query["resultRecordCount"][0])
            if record_count in refuse_counts:
                return httpx.Response(200, stream=JSONStream(
                    {"error": {"code": 500, "message": "Error performing query operation",
                               "details": []}}),
                    headers={"content-type": "application/json"})
            slice_ids = objectids[offset:offset + record_count]
            features = rows[offset:offset + record_count]
            page = {"objectIdFieldName": "OBJECTID",
                    "spatialReference": {"wkid": src.SOURCE_CRS_WKID,
                                         "latestWkid": src.SOURCE_CRS_WKID},
                    "hasZ": True, "features": features,
                    "exceededTransferLimit": False}
            if page_has_m is not None:
                page["hasM"] = page_has_m
            return httpx.Response(200, stream=JSONStream(page),
                                  headers={"content-type": "application/json"})
        raise AssertionError(f"unrouted request {request.url}")

    return httpx.MockTransport(respond)


@contextmanager
def fixture_mode(schema_sha: str, transport: httpx.MockTransport, *, segment_size=2):
    contexts = [
        patch.object(src, "MIN_FREE_BYTES", 0),
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(src, "SEGMENT_START_RECORDS", segment_size),
        patch.object(src, "SEGMENT_PROBE_BASE_DELAY_SECONDS", 0.0),
        patch.object(src, "DOC_CAPTURE_BASE_DELAY_SECONDS", 0.0),
        patch.object(src, "_default_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_doc_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_code_identity",
                     return_value={"code_identity_scheme": "test", "code_commit": "synthetic",
                                   "files": [], "working_tree_status": "unknown"}),
        patch.object(src, "validate_construction_code_identity"),
        patch.object(capture_common, "validate_construction_code_identity"),
    ]
    for context in contexts:
        context.start()
    try:
        yield
    finally:
        for context in reversed(contexts):
            context.stop()


def _staging(root: Path) -> StagingStorageResource:
    return StagingStorageResource(use_local=True, local_dir=str(root / "staging"),
                                  prefix="", backend="local")


class NhdCatchmentTests(unittest.TestCase):
    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "nhd-catchment")
        self.assertEqual(src.SERVICE_URL,
                         "https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all"
                         "/FeatureServer")
        self.assertEqual(src.LAYER_ID, 80)
        self.assertEqual(src.LAYER_NAME, "Catchment")
        self.assertEqual(src.GEOMETRY_TYPE, "esriGeometryPolygon")
        self.assertEqual(src.MAX_RECORD_COUNT, 2500)
        self.assertEqual(src.SEGMENT_START_RECORDS, 2000)
        self.assertEqual(src.EXPECTED_SCHEMA_SHA256,
                         "1b32b55650559d19c1d0fe2e3c876940d179925bf2d0f3bbe2d2ac262c7a57a5")
        self.assertRegex(src.EXPECTED_SCHEMA_SHA256, "^[0-9a-f]{64}$")
        keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertTrue(any(key.path == ["ingest", src.DATASET_SLUG, src.DATASET_SLUG]
                            for key in keys))

    def test_segment_query_contract_is_roster_bounded(self):
        url = src._segment_query(150000, 2000)
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["where"], ["1=1"])
        self.assertEqual(query["orderByFields"], ["OBJECTID ASC"])
        self.assertEqual(query["resultOffset"], ["150000"])
        self.assertEqual(query["resultRecordCount"], ["2000"])
        self.assertEqual(query["outSR"], [str(src.SOURCE_CRS_WKID)])
        self.assertEqual(query["returnGeometry"], ["true"])
        self.assertEqual(query["returnZ"], ["true"])
        self.assertEqual(query["returnM"], ["false"])
        self.assertLess(len(url), 400)
        self.assertEqual(parse_qs(urlparse(src._ids_url()).query)["returnIdsOnly"], ["true"])
        self.assertEqual(parse_qs(urlparse(src._count_url()).query)["returnCountOnly"], ["true"])

    def test_native_geometry_structural_drift_is_still_refused(self):
        base = _fixture_geometry(1)
        cases = {
            "unexpected_source_geometry_members": {**base, "curveRings": []},
            "source_polygon_geometry_invalid": {"hasZ": True, "rings": None},
            "source_polygon_ring_too_short": {"hasZ": True, "rings": [[[1, 2, 3]]]},
            "source_polygon_ordinate_contract_drift": {
                "hasZ": True, "rings": [[v[:2] for v in base["rings"][0]]]},
            "source_polygon_ordinate_not_numeric": {
                "hasZ": True, "rings": [[["bad"] + v[1:] for v in base["rings"][0]]]},
            "source_polygon_ordinate_not_finite": {
                "hasZ": True, "rings": [[v[:2] + [float("inf")] for v in base["rings"][0]]]},
        }
        names = tuple(field[0] for field in src.NATIVE_FIELDS)
        for reason, geometry in cases.items():
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(src.NhdCatchmentError, reason):
                    src._validate_feature({"attributes": _fixture_attrs(1), "geometry": geometry},
                                          names, 1)

    def test_native_invalid_geometries_are_preserved_with_compatibility_diagnostics(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_catchment(_staging(root), version="invalid-retained",
                                                 capture_directory=str(capture_root))
            records = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet").to_pylist()
            self.assertEqual([r["OBJECTID"] for r in records], list(FIXTURE_OBJECTIDS))
            for index in (3, 6):
                geometry = from_wkb(records[index]["geometry"])
                self.assertEqual(geometry.is_valid, index == 3)
                self.assertEqual({k: v for k, v in records[index].items() if k != "geometry"},
                                 _fixture_attrs(index + 1))
            self.assertEqual(result["source_unusable_geometry_counts"], {})
            self.assertEqual(sum(result["source_ogc_invalid_geometry_counts"].values()), 1)
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            audit = json.loads((root / "staging" / prefix / "metadata/source_audit.json").read_bytes())
            self.assertEqual(audit["ogc_invalid_geometry_counts"],
                             result["source_ogc_invalid_geometry_counts"])
            self.assertEqual(audit["ogc_invalid_geometry_sample_objectids"], [7])

    def test_complete_fixture_capture_materializes_and_repeats(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                storage = _staging(root)
                first = src.capture_nhd_catchment(storage, version="fixture-run",
                                                  capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                self.assertFalse(first["source_release_accepted"])
                second = src.capture_nhd_catchment(storage, version="fixture-run-2",
                                                   capture_directory=str(capture_root))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertTrue((capture_root / "computed_baseline.json").exists())
            parquet_path = capture_root / f"{src.FILE_SLUG}.parquet"
            table = pq.read_table(parquet_path)
            self.assertEqual(table.num_rows, FIXTURE_COUNT)
            self.assertEqual(table.schema.names[-1], "geometry")
            self.assertEqual(table.column("OBJECTID").to_pylist(), list(FIXTURE_OBJECTIDS))
            self.assertEqual(table.column("id3dhp").to_pylist(),
                             [row["attributes"]["id3dhp"] for row in rows])
            nulls = table.column("geometry").to_pylist()
            self.assertIsNotNone(nulls[3])
            self.assertTrue(from_wkb(nulls[3]).is_valid)
            self.assertIsNone(nulls[4])
            self.assertIsNone(nulls[5])
            self.assertIsNotNone(nulls[6])
            self.assertFalse(from_wkb(nulls[6]).is_valid)
            for index in (0, 1, 2):
                self.assertIsNotNone(nulls[index])
                self.assertTrue(from_wkb(nulls[index]).is_valid)
            geo_metadata = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                             ["Polygon Z", "MultiPolygon Z"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["crs"].get("name", ""),
                             "WGS 84")

            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            from dagster import DagsterInstance, materialize
            with fixture_mode(schema_sha, transport):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    result = materialize(
                        [nhd_catchment, nhd_catchment_complete],
                        resources={"staging_storage": _staging(root)},
                        instance=instance,
                        run_config={"ops": {"ingest__nhd_catchment__nhd_catchment":
                                            {"config": {"capture_directory":
                                                        str(capture_root)}}}},
                    )
                    self.assertTrue(result.success)
                    checks = {check.check_name: check
                              for check in result.get_asset_check_evaluations()}
                    self.assertTrue(checks["nhd_catchment_complete"].passed)
                    candidate = result.output_for_node("ingest__nhd_catchment__nhd_catchment")
                    self.assertEqual(candidate["feature_count"], FIXTURE_COUNT)
                    self.assertTrue(candidate["reused"])
                    partition = build_publish_partition_key(src.DATASET_SLUG, src.FILE_SLUG,
                                                            candidate["version"])
                    catalog = materialize([publish_catalog], instance=instance,
                                          partition_key=partition,
                                          resources={"staging_storage": _staging(root)})
                    self.assertTrue(catalog.success)
                    quality = json.loads(_staging(root).read_key(
                        f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{candidate['version']}/metadata/quality_manifest.json"))
                    self.assertEqual(quality["feature_count"], FIXTURE_COUNT)
                    self.assertEqual(quality["invalid_geometry_count"], 1)
                    cached = src.capture_nhd_catchment(_staging(root), version="after-catalog",
                                                       capture_directory=str(capture_root))
                    self.assertTrue(cached["reused"])
                    published = PublishedStorageResource(use_local=True, local_dir=str(root / "published"))
                    published.write(src.DATASET_SLUG, src.FILE_SLUG, "accepted-existing",
                                    "geoparquet/existing.parquet", b"accepted-inventory-sentinel")
                    before = {key: (published.object_snapshot(key), published.read_key(key))
                              for key in published.list_prefix()}
                    with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                        refusal = materialize(
                            [verify_release, *[AssetSpec(key) for key in verify_release.dependency_keys]],
                            instance=instance, partition_key=partition, raise_on_error=False,
                            resources={"staging_storage": _staging(root), "published_storage": published})
                    self.assertFalse(refusal.success)
                    self.assertIn("missing_independent_release_approval_registry",
                                  str(refusal.get_step_failure_events()[0].event_specific_data.error))
                    after = {key: (published.object_snapshot(key), published.read_key(key))
                             for key in published.list_prefix()}
                    self.assertEqual(after, before)

    def test_retained_geoparquet_seals_into_staging(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_catchment(_staging(root), version="seal-run",
                                                   capture_directory=str(capture_root))
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            sealed = json.loads((root / "staging" / prefix / "metadata"
                                 / "source_audit.json").read_bytes())
            self.assertEqual(sealed["source_feature_count"], FIXTURE_COUNT)
            self.assertEqual(sealed["pagination"]["mode"],
                             "enumerated_roster_bisected_segments")
            self.assertEqual(sealed["pagination"]["segment_start_records"], 2)
            self.assertTrue(sealed["pagination"]["resumable"])
            self.assertFalse(sealed["release_accepted"])
            self.assertEqual(sealed["null_geometry_count"], 1)
            self.assertEqual(sealed["nan_geometry_count"], 1)
            self.assertEqual(sealed["unusable_geometry_counts"],
                             {})
            manifest = json.loads((root / "staging" / prefix / "metadata"
                                   / "source_manifest.json").read_bytes())
            self.assertFalse(manifest["approved"])
            self.assertEqual(manifest["agency"], "U.S. Geological Survey")
            hosts = {entry["host"] for entry in manifest["inputs"]}
            self.assertTrue(all(host in ("3dhp.nationalmap.gov", "hydro.nationalmap.gov",
                                         "www.usgs.gov") for host in hosts))
            parquet_bytes = (root / "staging" / prefix / "geoparquet"
                             / f"{src.DATASET_SLUG}.parquet").read_bytes()
            self.assertEqual(pq.read_table(io.BytesIO(parquet_bytes)).num_rows, FIXTURE_COUNT)

    def test_refused_pages_bisect_down_to_singles(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_counts={2})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_catchment(_staging(root), version="bisect-run",
                                                   capture_directory=str(capture_root))
                self.assertEqual(result["feature_count"], FIXTURE_COUNT)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                self.assertEqual(sorted((entry["offset"], entry["count"])
                                        for entry in state["segments"]),
                                 [(offset, 1) for offset in range(FIXTURE_COUNT)])
                resumed = src.capture_nhd_catchment(_staging(root), version="bisect-run-2",
                                                    capture_directory=str(capture_root))
                self.assertTrue(resumed["reused"])

    def test_unservable_single_feature_refuses_explicitly(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_counts={1, 2})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(src.NhdCatchmentError,
                                            "source_single_feature_unservable"):
                    src.fetch_nhd_catchment_pages(root / "capture")
                self.assertTrue(sorted((root / "capture").glob("page_*.error-*.body")))

    def test_drift_and_provenance_failures_are_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]

        def layer_after_maxrecordcount(document):
            document = dict(document)
            document["maxRecordCount"] = 2000
            return document

        cases = {
            # The producer's count answer and its enumerated OBJECTID roster
            # must agree at capture time; a count naming more records than the
            # roster fails the witness (not a pin) and quarantines the attempt.
            "source_count_or_id_roster_drift": dict(count=FIXTURE_COUNT + 1),
            "source_layer_metadata_changed_during_capture": dict(
                layer_after_mutate=layer_after_maxrecordcount),
            "source_schema_drift": dict(layer_fields=_fixture_layer_fields()
                                        + [{"name": "EXTRA", "type": "esriFieldTypeString",
                                            "alias": "EXTRA", "nullable": True, "length": 4,
                                            "domain": None}]),
            "source_schema_baseline_drift": dict(
                layer_fields=[{**field, "length": (field.get("length") or 0) + 1}
                              if field.get("length") else field
                              for field in _fixture_layer_fields()]),
            "usgs_3dhp_service_identity_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "copyrightText": "Data refreshed October, 2026."}),
            "usgs_3dhp_service_layer_roster_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "layers": _fixture_service_doc()["layers"][:-1]}),
            "usgs_3dhp_service_description_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "serviceDescription": "legacy NHD service."}),
            "exact_3dhp_catchment_layer_metadata_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()),
                       "geometryType": "esriGeometryPolyline"}),
            "source_layer_z_or_m_contract_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "hasM": True}),
            "source_page_z_or_m_flag_missing": dict(page_has_m=True),
            "source_record_limit_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "maxRecordCount": 2000}),
            "source_layer_extent_drift": dict(
                layer=lambda: {**_fixture_layer_doc(_fixture_layer_fields()),
                               "extent": {**src.EXPECTED_LAYER_EXTENT,
                                          "xmax": 1.0,
                                          "spatialReference": {
                                              "latestWkid": src.NATIVE_LAYER_SR["latestWkid"]}}}),
            "source_id_population_drift": dict(
                ids=list(FIXTURE_OBJECTIDS)[:-1], ids_seed=list(FIXTURE_OBJECTIDS)),
            "unexpected_source_geometry_members": dict(rows=[
                {**rows[0], "geometry": {**rows[0]["geometry"], "curves": []}},
                *rows[1:]]),
            "source_polygon_ordinate_contract_drift": dict(rows=[
                {**rows[0], "geometry": {"hasZ": True,
                                         "rings": [[v[:2] for v in
                                                    rows[0]["geometry"]["rings"][0]]]}},
                *rows[1:]]),
            "source_coordinate_range_drift": dict(rows=[
                {**rows[0], "geometry": {"hasZ": True, "rings": [
                    [[200.0, v[1], v[2]] if v[0] == 0.0 else v
                     for v in rows[0]["geometry"]["rings"][0]]]}},
                *rows[1:]]),
            "source_page_id_slice_incomplete_or_reordered": dict(rows=[
                rows[0], rows[0], *rows[1:]], count=FIXTURE_COUNT),
            "source_record_schema_or_geometry_drift": dict(rows=[
                {**rows[0], "attributes": {"OBJECTID": 1, "id3dhp": "0000001"}},
                *rows[1:]]),
            "source_native_type_changed:areasqkm": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "areasqkm": "0.03"}},
                *rows[1:]]),
            "source_id3dhp_value_drift": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "id3dhp": "not base36!"}},
                *rows[1:]]),
            "usgs_service_documentation_link_drift": dict(
                doc_page=b"<html>an unrelated page without the service host</html>"),
        }
        for expected_error, kwargs in cases.items():
            with self.subTest(expected_error):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    capture_root = root / "capture"
                    case_rows = kwargs.pop("rows", rows)
                    transport = make_responder(case_rows, **kwargs)
                    with fixture_mode(schema_sha, transport):
                        storage = _staging(root)
                        with self.assertRaisesRegex(src.NhdCatchmentError, expected_error):
                            src.capture_nhd_catchment(storage, version="drift-run",
                                                      capture_directory=str(capture_root))
                    quarantined = sorted(
                        (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
                    self.assertTrue(quarantined, f"no quarantine for {expected_error}")
                    failure = json.loads((quarantined[0] / "metadata" / "failure.json")
                                         .read_bytes())
                    self.assertTrue(failure["reason_code"])

    def test_geometry_gates_are_reported_not_pinned(self):
        # The fixture carries one null-geometry and one NaN-ordinate feature;
        # the capture-time witness reports those counts in the audit instead of
        # gating on pinned values.
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_catchment(_staging(root), version="gate-run",
                                                   capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
            self.assertEqual(result["source_reported_count"], FIXTURE_COUNT)
            self.assertTrue(result["source_population_complete"])
            self.assertEqual(result["source_null_geometry_count"], 1)
            self.assertEqual(result["source_nan_geometry_count"], 1)
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            audit = json.loads((root / "staging" / prefix / "metadata"
                                / "source_audit.json").read_bytes())
            self.assertEqual(audit["source_feature_count"], FIXTURE_COUNT)
            self.assertEqual(audit["source_reported_count"], FIXTURE_COUNT)
            self.assertEqual(audit["null_geometry_count"], 1)
            self.assertEqual(audit["nan_geometry_count"], 1)
            self.assertNotIn("baseline_pending_note", audit)

    def test_upstream_added_feature_passes_and_warns_on_unapproved_drift(self):
        # Acceptance: an upstream edit that adds one feature must pass the
        # capture-time completeness witness (the count answers and the
        # enumerated roster both move together) and surface as an
        # approved-baseline drift warning, never as a capture failure.
        rows = _fixture_rows() + [{
            "attributes": {
                "OBJECTID": 8, "id3dhp": "0000008", "featuredate": 1709563014000 + 7,
                "mainstemid": "00007X", "areasqkm": 0.0317 + 7,
                "totestdrainareasqkm": 1.25 + 7, "workunitid": "07060001",
                "Shape__Length": 6388.035227755926 + 7,
                "Shape__Area": 461231.6270300725 + 7,
            },
            "geometry": {"hasZ": True, "rings": [_cw_square(3.0, 0.0, 1.0)]},
        }]
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, ids=[*FIXTURE_OBJECTIDS, 8])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            storage = _staging(root)
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_catchment(storage, version="added-run",
                                                   capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT + 1)
            self.assertEqual(result["source_reported_count"], FIXTURE_COUNT + 1)
            self.assertTrue(result["source_population_complete"])
            table = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet")
            self.assertEqual(table.column("OBJECTID").to_pylist(),
                             [*FIXTURE_OBJECTIDS, 8])
            from dagster import AssetCheckSeverity
            from dagster_hifld.assets.nhd_catchment import nhd_catchment_drift
            candidate = {"dataset_slug": src.DATASET_SLUG, "file_slug": src.FILE_SLUG,
                         "feature_count": FIXTURE_COUNT + 1,
                         "source_id_sha256": result["source_id_sha256"],
                         "release_accepted": False}
            drift = nhd_catchment_drift(candidate)
            self.assertFalse(drift.passed)
            self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
            self.assertEqual(drift.metadata["comparison_status"].value,
                             "skipped_no_approved_baseline")


def _segment_failures_transport(inner: httpx.MockTransport, *, fail_first: int | None):
    """Fail the first ``fail_first`` (or every) segment-page request with a reset.

    Returns the wrapped transport and the count of segment-page requests seen,
    so a test can assert the shared retry helper's bounded attempt budget.
    """
    seen = {"segment_requests": 0}

    def respond(request: httpx.Request) -> httpx.Response:
        query = parse_qs(urlparse(str(request.url)).query)
        if query.get("resultOffset") is not None:
            seen["segment_requests"] += 1
            if fail_first is None or seen["segment_requests"] <= fail_first:
                raise httpx.ConnectError("connection reset by peer")
        return inner.handler(request)

    return httpx.MockTransport(respond), seen


class NhdCatchmentRetainedStateFailureTests(unittest.TestCase):
    """Failure paths of the retained-capture primitives this adapter binds.

    The checkpoint/receipt/retry helpers live in
    ``dagster_hifld.sources.arcgis_layer``; every case here drives them through
    this adapter's own error type over retained fixtures: corrupt checkpoint
    JSON, missing and changed segment receipts, incomplete transport captures
    retried by the shared helper, that helper's bounded retry budget, and a
    foreign checkpoint whose coverage claims are never trusted.
    """

    def _first_capture(self, root: Path, capture_root: Path, schema_sha: str,
                       transport: httpx.MockTransport, version: str) -> None:
        with fixture_mode(schema_sha, transport):
            result = src.capture_nhd_catchment(
                _staging(root), version=version, capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)

    def _row_ids(self, capture_root: Path) -> list[int]:
        table = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet")
        return table.column("OBJECTID").to_pylist()

    def test_corrupt_checkpoint_json_refuses_and_retains_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "state-run")
            corrupted = b'{"segments": [ truncated'
            (capture_root / "capture_state.json").write_bytes(corrupted)
            before = sorted(path.name for path in capture_root.iterdir())
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(src.NhdCatchmentError,
                                            "capture_state_invalid"):
                    src.fetch_nhd_catchment_pages(capture_root)
            self.assertEqual((capture_root / "capture_state.json").read_bytes(), corrupted)
            self.assertEqual(sorted(path.name for path in capture_root.iterdir()), before)

    def test_missing_segment_receipt_refuses_read_and_refetch_restores_rows(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "receipt-run")
            name = src._segment_name(0, 2)
            (capture_root / f"{name}.receipt.json").unlink()
            with self.assertRaisesRegex(src.NhdCatchmentError,
                                        "retained_capture_receipt_invalid:" + name):
                src._read_capture(capture_root, name)
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_catchment(
                    _staging(root), version="receipt-run-2", capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
            self.assertEqual(self._row_ids(capture_root), list(FIXTURE_OBJECTIDS))
            self.assertTrue(sorted(capture_root.glob(f"{name}.error-*.body")))
            self.assertTrue(sorted(capture_root.glob(f"{name}.error-*.wire")))
            state = json.loads((capture_root / "capture_state.json").read_bytes())
            self.assertIn(name, [entry["name"] for entry in state["segments"]])

    def test_changed_segment_receipt_refuses_read_and_refetch_restores_rows(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "tamper-run")
            name = src._segment_name(2, 2)
            receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
            receipt["sha256"] = "0" * 64
            (capture_root / f"{name}.receipt.json").write_bytes(json.dumps(receipt).encode())
            with self.assertRaisesRegex(src.NhdCatchmentError,
                                        "retained_capture_receipt_invalid:" + name):
                src._read_capture(capture_root, name)
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_catchment(
                    _staging(root), version="tamper-run-2", capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
            self.assertEqual(self._row_ids(capture_root), list(FIXTURE_OBJECTIDS))
            self.assertEqual(len(self._row_ids(capture_root)), len(set(self._row_ids(capture_root))))
            for suffix in ("body", "wire", "receipt.json"):
                self.assertTrue(sorted(capture_root.glob(f"{name}.error-*.{suffix}")))

    def test_incomplete_transport_capture_is_retried_with_retained_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport, seen = _segment_failures_transport(make_responder(rows), fail_first=1)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_catchment(
                    _staging(root), version="retry-run", capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
            name = src._segment_name(0, 2)
            attempt_receipt = json.loads(
                (capture_root / f"{name}.attempt-01.receipt.json").read_bytes())
            self.assertFalse(attempt_receipt["complete_body"])
            self.assertIsNone(attempt_receipt["status"])
            self.assertEqual(attempt_receipt["error_code"], "request_error")
            for suffix in ("body", "wire", "receipt.json"):
                self.assertTrue((capture_root / f"{name}.attempt-01.{suffix}").exists())
            self.assertFalse(list(capture_root.glob(f"{name}.attempt-02.*")))
            self.assertEqual(self._row_ids(capture_root), list(FIXTURE_OBJECTIDS))

    def test_retry_budget_exhaustion_is_bounded_and_retains_failure_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport, seen = _segment_failures_transport(make_responder(rows), fail_first=None)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(src.NhdCatchmentError,
                                            "source_capture_failed:ValueError"):
                    src.fetch_nhd_catchment_pages(capture_root)
            self.assertEqual(seen["segment_requests"],
                             src.SEGMENT_PROBE_ATTEMPTS + src.DOC_CAPTURE_ATTEMPTS)
            segment = src._segment_name(0, 2)
            self.assertTrue((capture_root / f"{segment}.attempt-01.receipt.json").exists())
            self.assertTrue((capture_root / f"{segment}.attempt-02.receipt.json").exists())
            self.assertFalse(list(capture_root.glob(f"{segment}.attempt-03.*")))
            single = src._segment_name(0, 1)
            for attempt in range(1, src.DOC_CAPTURE_ATTEMPTS):
                for suffix in ("body", "wire", "receipt.json"):
                    self.assertTrue(
                        (capture_root / f"{single}.attempt-{attempt:02d}.{suffix}").exists(),
                        f"missing attempt evidence {single}.attempt-{attempt:02d}.{suffix}")
            self.assertFalse(
                list(capture_root.glob(f"{single}.attempt-{src.DOC_CAPTURE_ATTEMPTS:02d}.*")))
            final = json.loads((capture_root / f"{single}.receipt.json").read_bytes())
            self.assertFalse(final["complete_body"])
            self.assertEqual(final["error_code"], "request_error")
            self.assertFalse((capture_root / "capture_state.json").exists())
            self.assertFalse((capture_root / "computed_baseline.json").exists())

    def test_foreign_checkpoint_coverage_is_never_trusted(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "foreign-run")
            foreign = {"segments": [{"name": "foreign_product_segment", "offset": 0,
                                     "count": FIXTURE_COUNT, "features": FIXTURE_COUNT,
                                     "captured_at": "1999-01-01T00:00:00+00:00",
                                     "body_sha256": "0" * 64}], "complete": True}
            (capture_root / "capture_state.json").write_bytes(json.dumps(foreign).encode())
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(src.NhdCatchmentError,
                                            "source_capture_roster_incomplete_or_unexpected"):
                    src.capture_nhd_catchment(
                        _staging(root), version="foreign-run-2",
                        capture_directory=str(capture_root))
            self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
            quarantined = sorted(
                (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
            self.assertTrue(quarantined)
            failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
            self.assertEqual(failure["reason_code"],
                             "source_capture_roster_incomplete_or_unexpected")


if __name__ == "__main__":
    unittest.main()

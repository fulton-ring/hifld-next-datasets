"""Focused tests for the USGS 3DHP Waterbody Lake (large-scale) draft pipeline."""
import io
import json
import struct
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import httpx
import pyarrow.parquet as pq
import shapely

from dagster_hifld.assets.nhd_waterbody_large_scale import (
    nhd_waterbody_large_scale, nhd_waterbody_large_scale_complete)
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_frs_common
from dagster_hifld.sources import nhd_waterbody_large_scale as src

FIXTURE_COUNT = 5
# The Lake population's OBJECTIDs are not contiguous inside layer 60; the
# fixture mirrors that with gaps between its ascending OIDs.
FIXTURE_OBJECTIDS = (40, 42, 43, 45, 48)


def _fixture_attrs(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    return {
        "OBJECTID": oid,
        "id3dhp": f"{index + 1:07X}",
        "featuredate": 1695340800000 + index,
        "mainstemid": None,
        "gnisid": 694899 + index if index != 2 else None,
        "gnisidlabel": "Fixture Lake" if index != 2 else None,
        "featuretype": 3,
        "featuretypelabel": "Lake",
        "areasqkm": 1.24430194 + index,
        "workunitid": "NHD",
        "Shape__Length": 61227.14663080581 + index,
        "Shape__Area": 1244301.94 + index,
    }


def _ring(oid: int, offset: float) -> list:
    """Clockwise (exterior) ring under the producer's verified semantics."""
    index = FIXTURE_OBJECTIDS.index(oid)
    x0 = -89.96433399428682 + index * 0.01 + offset
    y0 = 31.89971254081809 + index * 0.01 + offset
    return [[x0, y0, 5.754000000000815],
            [x0, y0 + 0.001, 5.154],
            [x0 + 0.001, y0 + 0.001, 5.054],
            [x0 + 0.001, y0, 5.754000000000815],
            [x0, y0, 5.754000000000815]]


def _fixture_geometry(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    if oid == FIXTURE_OBJECTIDS[1]:
        # One exterior with a clockwise-ordered hole: the hole ring above is
        # counter-clockwise in the builder, so flip it to exercise hole
        # assignment under the producer's verified semantics.
        exterior = _ring(oid, 0.0)
        hole = _ring(oid, 0.0001)[::-1]
        rings = [exterior, hole]
    elif oid == FIXTURE_OBJECTIDS[2]:
        rings = [_ring(oid, 0.0), _ring(oid, 0.01)]
    else:
        rings = [_ring(oid, 0.0)]
    return {"hasZ": True, "rings": rings}


def _fixture_rows(count: int = FIXTURE_COUNT) -> list[dict]:
    return [{"attributes": _fixture_attrs(oid),
             "geometry": _fixture_geometry(oid)} for oid in FIXTURE_OBJECTIDS[:count]]


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
            "maxRecordCount": src.PAGE_SIZE,
            "advancedQueryCapabilities": {"supportsPagination": True},
            "sourceSpatialReference": dict(src.NATIVE_LAYER_SR),
            "extent": {**src.EXPECTED_LAYER_EXTENT,
                       "spatialReference": dict(src.NATIVE_LAYER_SR)},
            "fields": fields}


def _fixture_service_doc() -> dict:
    return {"copyrightText": src.EXPECTED_COPYRIGHT_TEXT,
            "serviceDescription": "The USGS 3D Hydrography Program (3DHP) 3DHP_all service.",
            "spatialReference": {"wkid": 102100, "latestWkid": 3857},
            "layers": [{"id": layer_id, "name": name}
                       for layer_id, name in src.EXPECTED_SERVICE_LAYERS]}


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


def make_responder(rows: list[dict], *, layer_fields=None, count=None, service=None,
                   layer=None, geometry=None, doc_page=None, refuse_geometry_for=None) \
        -> httpx.MockTransport:
    count = count if count is not None else len(rows)
    layer_fields = layer_fields if layer_fields is not None else _fixture_layer_fields()

    def layer_doc():
        if callable(layer):
            return layer()
        if layer is not None:
            return layer
        return _fixture_layer_doc(layer_fields)

    service_doc = service if service is not None else _fixture_service_doc()

    def respond(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path
        query = parse_qs(urlparse(str(request.url)).query)
        if path.startswith("/3d-hydrography-program/"):
            return httpx.Response(200, stream=JSONStream(doc_page or
                b"<html>3DHP access: https://3dhp.nationalmap.gov/arcgis/rest/services"
                b"/usgs_3dhp_all/FeatureServer</html>"),
                headers={"content-type": "text/html"})
        if path.rstrip("/").endswith("FeatureServer"):
            return httpx.Response(200, stream=JSONStream(service_doc),
                                  headers={"content-type": "application/json"})
        if path.rstrip("/").endswith(f"/{src.LAYER_ID}"):
            return httpx.Response(200, stream=JSONStream(layer_doc()),
                                  headers={"content-type": "application/json"})
        if query.get("returnCountOnly") == ["true"]:
            return httpx.Response(200, stream=JSONStream({"count": count}),
                                  headers={"content-type": "application/json"})
        if "objectIds" in query:
            wanted = int(query["objectIds"][0])
            matches = [row for row in rows
                       if row["attributes"]["OBJECTID"] == wanted]
            wants_geometry = query.get("returnGeometry") == ["true"]
            if wants_geometry and refuse_geometry_for is not None \
                    and wanted == refuse_geometry_for:
                return httpx.Response(200, stream=JSONStream(
                    {"error": {"code": 500, "message": "Error performing query operation"}}),
                    headers={"content-type": "application/json"})
            payload_rows = []
            for row in matches:
                if wants_geometry:
                    payload_rows.append(row)
                else:
                    payload_rows.append({"attributes": row["attributes"], "geometry": None})
            return httpx.Response(200, stream=JSONStream(
                {"objectIdFieldName": "OBJECTID",
                 "spatialReference": {"wkid": src.SOURCE_CRS_WKID,
                                      "latestWkid": src.SOURCE_CRS_WKID},
                 "hasZ": True, "features": payload_rows}),
                headers={"content-type": "application/json"})
        where = query.get("where", [""])[0]
        if where.startswith(f"{src.POPULATION_WHERE} AND OBJECTID>") \
                and query.get("returnGeometry") == ["false"]:
            # The producer's working attributes-only keyset channel.
            last = int(where.split(">")[1])
            matches = [row for row in rows if row["attributes"]["OBJECTID"] > last][:1]
            return httpx.Response(200, stream=JSONStream(
                {"objectIdFieldName": "OBJECTID", "features": matches,
                 "exceededTransferLimit": False}),
                headers={"content-type": "application/json"})
        if not where.startswith(f"{src.POPULATION_WHERE} AND OBJECTID>"):
            return httpx.Response(200, stream=JSONStream({"count": count}),
                                  headers={"content-type": "application/json"})
        last = int(where.split(">")[1])
        size = int(query.get("resultRecordCount", ["0"])[0]) or src.PAGE_SIZE
        page_rows = [row for row in rows
                     if (isinstance(row["attributes"]["OBJECTID"], int)
                         and row["attributes"]["OBJECTID"] > last)
                     or not isinstance(row["attributes"]["OBJECTID"], int)][:size]
        if refuse_geometry_for is not None and any(
                row["attributes"]["OBJECTID"] == refuse_geometry_for for row in page_rows):
            return httpx.Response(200, stream=JSONStream(
                {"error": {"code": 500, "message": "Error performing query operation"}}),
                headers={"content-type": "application/json"})
        if geometry is not None:
            page_rows = [dict(row, geometry=geometry(row)) for row in page_rows]
        page_oids = [row["attributes"]["OBJECTID"] for row in page_rows
                     if isinstance(row["attributes"]["OBJECTID"], int)]
        tail_oids = [row["attributes"]["OBJECTID"] for row in rows
                     if isinstance(row["attributes"]["OBJECTID"], int)]
        more = bool(page_oids) and max(page_oids) < max(tail_oids)
        return httpx.Response(200, stream=JSONStream(
            {"objectIdFieldName": "OBJECTID",
             "spatialReference": {"wkid": src.SOURCE_CRS_WKID, "latestWkid": src.SOURCE_CRS_WKID},
             "hasZ": True, "features": page_rows,
             "exceededTransferLimit": more}),
            headers={"content-type": "application/json"})

    return httpx.MockTransport(respond)


@contextmanager
def fixture_mode(schema_sha: str, transport: httpx.MockTransport, *, page_size=None,
                 min_objectid=None, max_objectid=None, null_geometry_count=None,
                 refused_ids=None, record_sha=None, geometry_sha=None, ids_sha=None,
                 real_identity: bool = False):
    import dagster_hifld.assets.nhd_waterbody_large_scale as asset_module
    contexts = [
        patch.object(src, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(src, "EXPECTED_MIN_OBJECTID",
                     FIXTURE_OBJECTIDS[0] if min_objectid is None else min_objectid),
        patch.object(src, "EXPECTED_MAX_OBJECTID",
                     FIXTURE_OBJECTIDS[-1] if max_objectid is None else max_objectid),
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(src, "EXPECTED_RECORD_SHA256",
                     "" if record_sha is None else record_sha),
        patch.object(src, "EXPECTED_GEOMETRY_SHA256",
                     "" if geometry_sha is None else geometry_sha),
        patch.object(src, "EXPECTED_IDS_SHA256",
                     "" if ids_sha is None else ids_sha),
        patch.object(src, "EXPECTED_NULL_GEOMETRY_COUNT", null_geometry_count),
        patch.object(src, "EXPECTED_REFUSED_IDS", refused_ids),
        patch.object(src, "EXPECTED_FEATURETYPE_DISTRIBUTION", {3: FIXTURE_COUNT}),
        patch.object(asset_module, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(src, "_default_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_doc_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        # CI runners hold far less than the pinned capture floor, so the floor
        # gate is evaluated against a stub that reports exactly the pinned
        # requirement; the refusal itself is exercised by the dedicated floor
        # test below.
        patch.object(src.shutil, "disk_usage",
                     return_value=SimpleNamespace(free=src.MIN_FREE_BYTES)),
        patch.object(src.time, "sleep"),
    ]
    if not real_identity:
        contexts.extend([
            patch.object(src, "_code_identity",
                         return_value={"code_identity_scheme": "test", "code_commit": "synthetic",
                                       "files": [], "working_tree_status": "unknown"}),
            patch.object(src, "validate_construction_code_identity"),
            patch.object(epa_frs_common, "validate_construction_code_identity"),
        ])
    if page_size is not None:
        contexts.append(patch.object(src, "PAGE_SIZE", page_size))
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


class NhdWaterbodyLargeScaleTests(unittest.TestCase):
    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "nhd-waterbody-large-scale-2")
        self.assertEqual(src.SERVICE_URL,
                         "https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all"
                         "/FeatureServer")
        self.assertEqual(src.LAYER_ID, 60)
        self.assertEqual(src.LAYER_NAME, "Waterbody")
        self.assertEqual(src.POPULATION_WHERE, "featuretype=3")
        self.assertEqual(src.GEOMETRY_TYPE, "esriGeometryPolygon")
        self.assertEqual(src.EXPECTED_COUNT, 7_063_520)
        self.assertEqual(src.EXPECTED_MIN_OBJECTID, 40)
        self.assertEqual(src.EXPECTED_MAX_OBJECTID, 7_064_395)
        self.assertEqual(src.PAGE_SIZE, 2500)
        self.assertEqual(len(src.NATIVE_FIELDS), 12)
        self.assertEqual(src.EXPECTED_FEATURETYPE_LABELS, {3: "Lake"})
        self.assertEqual(src.EXPECTED_NULL_GEOMETRY_COUNT, None)
        self.assertEqual(src.EXPECTED_REFUSED_IDS, None)
        self.assertEqual(src.EXPECTED_RECORD_SHA256, "")
        self.assertEqual(src.EXPECTED_GEOMETRY_SHA256, "")
        self.assertEqual(src.EXPECTED_IDS_SHA256, "")
        self.assertTrue(src.BASELINE_PENDING_NOTE)
        keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertTrue(any(key.path == ["ingest", src.DATASET_SLUG, src.DATASET_SLUG]
                            for key in keys))

    def test_keyset_pagination_contract_is_ordered_and_population_filtered(self):
        url = src._page_url(1000000, src.PAGE_SIZE)
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["where"], ["featuretype=3 AND OBJECTID>1000000"])
        self.assertEqual(query["orderByFields"], ["OBJECTID ASC"])
        self.assertEqual(query["resultRecordCount"], [str(src.PAGE_SIZE)])
        self.assertEqual(query["outSR"], [str(src.SOURCE_CRS_WKID)])
        self.assertEqual(query["returnGeometry"], ["true"])
        self.assertEqual(query["returnZ"], ["true"])
        self.assertNotIn("returnM", query)
        self.assertLess(len(url), 400)
        count_url = parse_qs(urlparse(src._count_url()).query)
        self.assertEqual(count_url["where"], ["featuretype=3"])
        attrs_url = parse_qs(urlparse(src._attrs_keyset_url(7)).query)
        self.assertEqual(attrs_url["where"], ["featuretype=3 AND OBJECTID>7"])
        self.assertEqual(attrs_url["returnGeometry"], ["false"])

    def test_polygon_wkb_keeps_z_and_ring_order(self):
        single = src._polygon_wkb(_fixture_geometry(FIXTURE_OBJECTIDS[0]))
        order, kind = struct.unpack_from("<BI", single, 0)
        self.assertEqual((order, kind), (1, 0x80000003))
        geometry = shapely.from_wkb(single)
        self.assertIsInstance(geometry, shapely.Polygon)
        ring = _ring(FIXTURE_OBJECTIDS[0], 0.0)
        self.assertEqual(shapely.get_coordinates(geometry, include_z=True).tolist(),
                         [list(vertex) for vertex in ring])
        multi = src._polygon_wkb(_fixture_geometry(FIXTURE_OBJECTIDS[2]))
        order, kind = struct.unpack_from("<BI", multi, 0)
        self.assertEqual((order, kind), (1, 0x80000006))
        multi_geometry = shapely.from_wkb(multi)
        self.assertIsInstance(multi_geometry, shapely.MultiPolygon)
        self.assertEqual(shapely.get_num_geometries(multi_geometry), 2)
        with_hole = src._polygon_wkb(_fixture_geometry(FIXTURE_OBJECTIDS[1]))
        with_hole_geometry = shapely.from_wkb(with_hole)
        self.assertIsInstance(with_hole_geometry, shapely.Polygon)
        self.assertEqual(len(with_hole_geometry.interiors), 1)
        self.assertEqual((with_hole_geometry.exterior.length,
                          with_hole_geometry.interiors[0].length),
                         (shapely.Polygon(_ring(FIXTURE_OBJECTIDS[1], 0.0)).length,
                          shapely.Polygon(list(reversed(_ring(FIXTURE_OBJECTIDS[1], 0.0001))))
                          .length))

    def test_polygon_geometry_drift_is_refused(self):
        base = _fixture_geometry(FIXTURE_OBJECTIDS[0])
        cases = {
            "unexpected_source_geometry_members": {**base, "curves": []},
            "source_geometry_z_flag_missing": {"hasZ": False, "rings": base["rings"]},
            "source_polygon_geometry_invalid": {"hasZ": True, "rings": []},
            "source_polygon_ring_invalid": {"hasZ": True,
                                            "rings": [base["rings"][0][:2]]},
            "source_polygon_ring_not_closed": {"hasZ": True,
                                               "rings": [base["rings"][0][:-1]]},
            "source_polygon_ordinate_contract_drift": {
                "hasZ": True,
                "rings": [[vertex[:2] for vertex in base["rings"][0]]]},
            "source_polygon_ordinate_not_numeric": {
                "hasZ": True,
                "rings": [[["NaN"] + vertex[1:] for vertex in base["rings"][0]]]},
            "source_polygon_ordinate_not_finite": {
                "hasZ": True,
                "rings": [[vertex[:2] + [float("inf")] for vertex in base["rings"][0]]]},
            "source_coordinate_range_drift": {
                "hasZ": True,
                "rings": [[[200.0] + vertex[1:] for vertex in base["rings"][0]]]},
            "source_polygon_ring_degenerate": {"hasZ": True, "rings": [
                [base["rings"][0][0], base["rings"][0][1],
                 base["rings"][0][1], base["rings"][0][0]]]},
        }
        for expected_error, geometry in cases.items():
            with self.subTest(expected_error):
                with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError, expected_error):
                    src._polygon_wkb(geometry)

    def test_disk_floor_refuses_before_any_fetch(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        fetches = {"count": 0}
        real_send = httpx.Client.send

        def counting_send(client, request, **kwargs):
            fetches["count"] += 1
            return real_send(client, request, **kwargs)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport):
                with patch.object(src.shutil, "disk_usage",
                                  return_value=SimpleNamespace(free=src.MIN_FREE_BYTES - 1)), \
                        patch.object(httpx.Client, "send", counting_send):
                    with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                                "disk_floor_before_acquisition"):
                        src.capture_nhd_waterbody_large_scale(
                            _staging(root), version="floor-run",
                            capture_directory=str(root / "capture"))
            self.assertEqual(fetches["count"], 0)
            self.assertFalse((root / "capture").exists())
            self.assertFalse(any((root / "staging").iterdir()))

    def test_complete_fixture_capture_materializes_and_repeats(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                first = src.capture_nhd_waterbody_large_scale(
                    storage, version="fixture-run", capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                self.assertFalse(first["source_release_accepted"])
                second = src.capture_nhd_waterbody_large_scale(
                    storage, version="fixture-run-2", capture_directory=str(capture_root))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertTrue((capture_root / "computed_baseline.json").exists())
            parquet_path = capture_root / f"{src.FILE_SLUG}.parquet"
            table = pq.read_table(parquet_path)
            self.assertEqual(table.num_rows, FIXTURE_COUNT)
            self.assertEqual(table.schema.names[-1], "geometry")
            self.assertEqual(table.column("OBJECTID").to_pylist(), list(FIXTURE_OBJECTIDS))
            self.assertEqual(table.column("featuretypelabel").to_pylist(),
                             ["Lake"] * FIXTURE_COUNT)
            self.assertEqual(table.column("featuretype").to_pylist(), [3] * FIXTURE_COUNT)
            geometries = [shapely.from_wkb(wkb) for wkb in table.column("geometry").to_pylist()]
            self.assertEqual([type(geometry).__name__ for geometry in geometries],
                             ["Polygon", "Polygon", "MultiPolygon", "Polygon", "Polygon"])
            self.assertEqual([len(geometry.interiors) for geometry in geometries[:2]], [0, 1])
            geo_metadata = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                             ["Polygon Z", "MultiPolygon Z"])

            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            from dagster import DagsterInstance, materialize
            with fixture_mode(schema_sha, transport, page_size=2):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    result = materialize(
                        [nhd_waterbody_large_scale, nhd_waterbody_large_scale_complete],
                        resources={"staging_storage": _staging(root)},
                        instance=instance,
                        run_config={"ops": {
                            "ingest__nhd_waterbody_large_scale_2__nhd_waterbody_large_scale_2": {
                                "config": {"capture_directory": str(capture_root)}}}},
                    )
                    self.assertTrue(result.success)
                    checks = {check.check_name: check
                              for check in result.get_asset_check_evaluations()}
                    check = checks["nhd_waterbody_large_scale_complete"]
                    self.assertTrue(check.passed)
                    candidate = result.output_for_node(
                        "ingest__nhd_waterbody_large_scale_2__nhd_waterbody_large_scale_2")
                    self.assertEqual(candidate["feature_count"], FIXTURE_COUNT)
                    self.assertTrue(candidate["reused"])

    def test_retained_geoparquet_seals_into_staging(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                result = src.capture_nhd_waterbody_large_scale(
                    _staging(root), version="seal-run", capture_directory=str(capture_root))
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            sealed = json.loads((root / "staging" / prefix / "metadata"
                                 / "source_audit.json").read_bytes())
            self.assertEqual(sealed["source_feature_count"], FIXTURE_COUNT)
            self.assertTrue(sealed["baseline_pending_complete_capture"])
            self.assertEqual(sealed["pagination"]["mode"], "objectid_keyset_asc_with_bisection")
            self.assertEqual(sealed["pagination"]["population_where"], "featuretype=3")
            self.assertTrue(sealed["pagination"]["resumable"])
            self.assertFalse(sealed["release_accepted"])
            self.assertEqual(sealed["source_min_objectid"], FIXTURE_OBJECTIDS[0])
            self.assertEqual(sealed["source_max_objectid"], FIXTURE_OBJECTIDS[-1])
            manifest = json.loads((root / "staging" / prefix / "metadata"
                                   / "source_manifest.json").read_bytes())
            self.assertFalse(manifest["approved"])
            self.assertEqual(manifest["agency"], "U.S. Geological Survey")
            self.assertIn("featuretype=3", manifest["transformations"][0]["parameters"]
                          ["population"])
            self.assertIn("nhd/waterbody-large-scale-2", manifest["lineage"])
            hosts = {entry["host"] for entry in manifest["inputs"]}
            self.assertTrue(all(host in ("3dhp.nationalmap.gov",
                                         "hydro.nationalmap.gov", "www.usgs.gov")
                                for host in hosts))
            parquet_bytes = (root / "staging" / prefix / "geoparquet"
                             / f"{src.DATASET_SLUG}.parquet").read_bytes()
            self.assertEqual(pq.read_table(io.BytesIO(parquet_bytes)).num_rows, FIXTURE_COUNT)

    def test_seal_and_reuse_stream_parquet_without_reading_it_into_memory(self):
        """The candidate parquet is sealed and verified as a streamed file.

        Reading the candidate parquet through read_bytes would buffer the
        complete population geometry (tens of GB) in memory during the seal,
        contradicting this module's bounded streaming contract.
        """
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                first = src.capture_nhd_waterbody_large_scale(
                    storage, version="stream-seal-run", capture_directory=str(capture_root))
                parquet_path = capture_root / f"{src.FILE_SLUG}.parquet"
                real_read_bytes = Path.read_bytes

                def refuse_parquet_read_bytes(path: Path):
                    if path == parquet_path:
                        raise AssertionError("candidate parquet was read into memory")
                    return real_read_bytes(path)

                with patch.object(Path, "read_bytes", refuse_parquet_read_bytes):
                    reused = src.capture_nhd_waterbody_large_scale(
                        storage, version="stream-seal-run-2", capture_directory=str(capture_root))
                    fresh = src.capture_nhd_waterbody_large_scale(
                        _staging(root / "b"), version="stream-seal-run",
                        capture_directory=str(capture_root))
            self.assertTrue(reused["reused"])
            self.assertFalse(fresh["reused"])
            self.assertEqual(fresh["version"], first["version"])
            self.assertEqual(fresh["feature_count"], FIXTURE_COUNT)
            sealed = (f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{first['version']}"
                      f"/geoparquet/{src.DATASET_SLUG}.parquet")
            self.assertEqual((root / "staging" / sealed).read_bytes(),
                             parquet_path.read_bytes())
            self.assertEqual((root / "b" / "staging" / sealed).read_bytes(),
                             parquet_path.read_bytes())

    def test_dense_head_error_documents_bisect_to_accepted_pages(self):
        """Oversized windows that the producer refuses with its error document
        are bisected down to accepted sizes without changing the population."""
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]

        def geometry(row: dict) -> dict:
            return _fixture_geometry(row["attributes"]["OBJECTID"])

        def responder_factory(base):
            def responder(request: httpx.Request) -> httpx.Response:
                query = parse_qs(urlparse(str(request.url)).query)
                where = query.get("where", [""])[0]
                if (where.startswith(f"{src.POPULATION_WHERE} AND OBJECTID>")
                        and query.get("returnGeometry") == ["true"]
                        and "returnCountOnly" not in query
                        and query.get("resultRecordCount", ["0"])[0] != "1"):
                    last = int(where.split(">")[1])
                    span = [row["attributes"]["OBJECTID"] for row in rows
                            if row["attributes"]["OBJECTID"] > last][:src.PAGE_SIZE]
                    if len(span) > 1:
                        return httpx.Response(200, stream=JSONStream(
                            {"error": {"code": 500,
                                       "message": "Error performing query operation"}}),
                            headers={"content-type": "application/json"})
                return base.handler(request)
            return responder

        transport = httpx.MockTransport(responder_factory(make_responder(rows, geometry=geometry)))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                result = src.capture_nhd_waterbody_large_scale(
                    _staging(root), version="bisect-run", capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
            state = json.loads((capture_root / "capture_state.json").read_bytes())
            sizes = sorted({entry["size"] for entry in state["pages"]})
            self.assertEqual(sizes, [1, 2])
            self.assertGreaterEqual(sum(1 for entry in state["pages"] if entry["size"] == 1), 3)
            baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
            self.assertEqual(baseline["source_refused_ids"], [])

    def test_transport_failure_page_is_witnessed_and_resume_refetches(self):
        """A persistent non-200 transport failure is not bisectable: it is
        retried a bounded number of times, every failed attempt is retained as
        witnessed evidence, and the failure propagates; a resumed session
        refetches the position and completes."""
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        base = make_responder(rows)
        injected = {"failures": 0}

        def responder(request: httpx.Request) -> httpx.Response:
            query = parse_qs(urlparse(str(request.url)).query)
            where = query.get("where", [""])[0]
            if (where.startswith(f"{src.POPULATION_WHERE} AND OBJECTID>")
                    and "returnCountOnly" not in query
                    and query.get("returnGeometry") != ["false"]
                    and injected["failures"] < 10):
                injected["failures"] += 1
                return httpx.Response(502, stream=JSONStream(b"upstream busy"),
                                      headers={"content-type": "text/plain"})
            return base.handler(request)

        transport = httpx.MockTransport(responder)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                            "source_page_fetch_failed"):
                    src.fetch_nhd_waterbody_large_scale_pages(capture_root)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
            with fixture_mode(schema_sha, make_responder(rows), page_size=2):
                result = src.fetch_nhd_waterbody_large_scale_pages(capture_root)
                self.assertTrue(result["complete"])
                storage = _staging(root)
                sealed = src.capture_nhd_waterbody_large_scale(
                    storage, version="witness-run", capture_directory=str(capture_root))
                self.assertEqual(sealed["feature_count"], FIXTURE_COUNT)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))

    def test_layer_bracket_ignores_volatile_drawing_info(self):
        """Volatile display metadata must not fail the before/after brackets.

        The producer's live layer document regenerates
        drawingInfo.renderer.authoringInfo.colorRamp per request (observed on
        the 2026-11-13 full capture, which refused to complete on this field
        alone while the data contract stayed byte-identical).
        """
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        calls = {"n": 0}

        def volatile_layer():
            calls["n"] += 1
            doc = _fixture_layer_doc(_fixture_layer_fields())
            if calls["n"] >= 2:
                doc["drawingInfo"] = {
                    "renderer": {"type": "uniqueValue", "field1": "featuretype",
                                 "authoringInfo": {"colorRamp": [
                                     {"type": "algorithmic",
                                      "algorithm": "esriCIELabAlgorithm",
                                      "fromColor": [240, 212, 252, 255],
                                      "toColor": [240, 212, 252, 255]}]}}}
            return doc

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, make_responder(rows, layer=volatile_layer),
                              page_size=2):
                result = src.capture_nhd_waterbody_large_scale(
                    _staging(root), version="bracket-run", capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
            self.assertFalse(result["release_accepted"])

    def test_layer_bracket_contract_change_is_still_refused(self):
        """Bracket comparison stays fail-closed for every non-display field."""
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        calls = {"n": 0}

        def drifting_layer():
            calls["n"] += 1
            doc = _fixture_layer_doc(_fixture_layer_fields())
            if calls["n"] >= 2:
                doc["maxRecordCount"] = 2000
            return doc

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, make_responder(rows, layer=drifting_layer),
                              page_size=2):
                with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                            "source_metadata_or_count_changed_during_capture"):
                    src.capture_nhd_waterbody_large_scale(
                        _staging(root), version="bracket-drift-run",
                        capture_directory=str(capture_root))
            quarantined = sorted(
                (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
            self.assertTrue(quarantined)
            failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
            self.assertEqual(failure["reason_code"],
                             "source_metadata_or_count_changed_during_capture")

    def test_single_record_refusal_uses_attrs_channel_and_gates_null_geometry(self):
        """A producer-refused single record keeps its captured attributes with
        explicitly gated null candidate geometry and a pinned refusal set."""
        refused_oid = FIXTURE_OBJECTIDS[2]
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_geometry_for=refused_oid)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                result = src.capture_nhd_waterbody_large_scale(
                    storage, version="refusal-run", capture_directory=str(capture_root))
                baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
                self.assertEqual(baseline["source_refused_ids"], [refused_oid])
                self.assertEqual(baseline["source_null_geometry_count"], 1)
                sealed_audit_name = (root / "staging" / f"{src.DATASET_SLUG}/{src.FILE_SLUG}"
                                     / result["version"] / "metadata" / "source_audit.json")
                sealed_audit = json.loads(sealed_audit_name.read_bytes())
                self.assertEqual(sealed_audit["source_refused_ids"], [refused_oid])
                self.assertEqual(baseline["source_null_geometry_count"], 1)
                prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
                table = pq.read_table(root / "staging" / prefix / "geoparquet"
                                      / f"{src.DATASET_SLUG}.parquet")
                self.assertIsNone(table.column("geometry").to_pylist()[
                    FIXTURE_OBJECTIDS.index(refused_oid)])
                self.assertEqual(table.num_rows, FIXTURE_COUNT)
                # Pin the discovered refusal set: a pure-local revalidation passes.
                with fixture_mode(schema_sha, transport, page_size=2,
                                  refused_ids=(refused_oid,)):
                    again = src.capture_nhd_waterbody_large_scale(
                        storage, version="refusal-run-2", capture_directory=str(capture_root))
                    self.assertTrue(again["reused"])
                # A changed refusal set fails closed.
                with fixture_mode(schema_sha, transport, page_size=2,
                                  refused_ids=(refused_oid + 1,)):
                    with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                                "source_refused_ids_drift"):
                        src.capture_nhd_waterbody_large_scale(
                            storage, version="refusal-run-drift",
                            capture_directory=str(capture_root))
                # An unpinned refusal appearing on a later pinned capture fails closed.
                clean_transport = make_responder(_fixture_rows())
                with fixture_mode(schema_sha, clean_transport, page_size=2,
                                  refused_ids=(refused_oid,)):
                    with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                                "source_refused_ids_drift"):
                        src.capture_nhd_waterbody_large_scale(
                            _staging(root / "b"), version="refusal-run-clean",
                            capture_directory=str(root / "b" / "capture"))

    def test_min_and_max_objectid_pins_are_enforced(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport, page_size=2, min_objectid=41):
                with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                            "source_page_population_incomplete"):
                    src.capture_nhd_waterbody_large_scale(
                        _staging(root), version="min-pin-run",
                        capture_directory=str(root / "capture"))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport, page_size=2, max_objectid=47):
                with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                            "source_page_population_incomplete"):
                    src.capture_nhd_waterbody_large_scale(
                        _staging(root), version="max-pin-run",
                        capture_directory=str(root / "capture"))

    def test_featuretype_outside_population_is_refused(self):
        rows = [dict(_fixture_rows()[0])]
        rows[0] = {**rows[0], "attributes":
                   {**rows[0]["attributes"], "featuretype": 1, "featuretypelabel": "River"}}
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, count=FIXTURE_COUNT)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport, page_size=2):
                with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                            "source_featuretype_outside_population"):
                    src.capture_nhd_waterbody_large_scale(
                        _staging(root), version="ftype-run",
                        capture_directory=str(root / "capture"))

    def test_served_null_geometry_is_pinned_when_transcribed(self):
        rows = _fixture_rows()
        rows[1] = {**rows[1], "geometry": None}
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport, page_size=2, null_geometry_count=1):
                result = src.capture_nhd_waterbody_large_scale(
                    _staging(root), version="null-run", capture_directory=str(root / "capture"))
                self.assertEqual(result["source_null_geometry_count"], 1)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport, page_size=2, null_geometry_count=0):
                with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                            "source_null_geometry_count_drift"):
                    src.capture_nhd_waterbody_large_scale(
                        _staging(root), version="null-drift-run",
                        capture_directory=str(root / "capture"))

    def test_capture_requires_explicit_local_staging(self):
        from dagster_hifld.resources import PublishedStorageResource
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                        "explicit_local_staging_resource_required"):
                src.capture_nhd_waterbody_large_scale(
                    PublishedStorageResource(use_local=True, local_dir=str(root)),
                    version="v20261113")
            bucketed = StagingStorageResource(use_local=False, bucket="some-bucket")
            with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                        "explicit_local_staging_resource_required"):
                src.capture_nhd_waterbody_large_scale(bucketed, version="v20261113")
            prefixed = StagingStorageResource(use_local=True, local_dir=str(root), prefix="sub")
            with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                        "explicit_local_staging_resource_required"):
                src.capture_nhd_waterbody_large_scale(prefixed, version="v20261113")
            with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError, "invalid_version"):
                src.capture_nhd_waterbody_large_scale(
                    StagingStorageResource(use_local=True, local_dir=str(root), prefix=""),
                    version="v/invalid")

    def test_pinned_baseline_drift_is_refused_and_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                storage = _staging(root)
                src.capture_nhd_waterbody_large_scale(
                    storage, version="pin-run", capture_directory=str(capture_root))
                baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
                with fixture_mode(schema_sha, transport, page_size=2,
                                  record_sha=baseline["source_record_sha256"]):
                    again = src.capture_nhd_waterbody_large_scale(
                        storage, version="pin-run-verify", capture_directory=str(capture_root))
                    self.assertTrue(again["reused"])
                with fixture_mode(schema_sha, transport, page_size=2,
                                  ids_sha="0" * 64):
                    with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                                "source_id_roster_drift"):
                        src.capture_nhd_waterbody_large_scale(
                            storage, version="pin-run-drift", capture_directory=str(capture_root))
                quarantined = sorted((root / "staging" / "_quarantine" / src.DATASET_SLUG)
                                     .glob("*"))
                self.assertTrue(quarantined)
                failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
                self.assertEqual(failure["reason_code"], "source_id_roster_drift")

    def test_drift_and_provenance_failures_are_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        cases = {
            "source_count_drift": dict(count=FIXTURE_COUNT + 1),
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
            "exact_3dhp_waterbody_layer_metadata_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()),
                       "geometryType": "esriGeometryPolyline"}),
            "source_layer_z_or_m_contract_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "hasM": True}),
            "source_record_limit_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "maxRecordCount": 2000}),
            "source_layer_extent_drift": dict(
                layer=lambda: {**_fixture_layer_doc(_fixture_layer_fields()),
                               "extent": {**src.EXPECTED_LAYER_EXTENT,
                                          "xmax": 1.0,
                                          "spatialReference": dict(src.NATIVE_LAYER_SR)}}),
            "source_page_transfer_truncated": dict(
                geometry=lambda row: _fixture_geometry(row["attributes"]["OBJECTID"]),
                truncate_short_page=True),
            "unexpected_source_geometry_members": dict(
                geometry=lambda row: {**_fixture_geometry(row["attributes"]["OBJECTID"]),
                                      "curves": []}),
            "source_coordinate_range_drift": dict(
                geometry=lambda row: {"hasZ": True, "rings": [
                    [[200.0] + vertex[1:] for vertex in
                     _fixture_geometry(row["attributes"]["OBJECTID"])["rings"][0]]]}),
            "source_object_ids_not_strictly_increasing": dict(rows=[
                dict(rows[0]), dict(rows[0])], count=FIXTURE_COUNT),
            "source_object_id_type_changed": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "OBJECTID": None}},
                *rows[1:]]),
            "source_native_type_changed:gnisid": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "gnisid": "694899"}},
                *rows[1:]]),
            "source_native_type_changed:areasqkm": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "areasqkm": float("nan")}},
                *rows[1:]]),
            "source_id3dhp_value_drift": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "id3dhp": "not base36!"}},
                *rows[1:]]),
            "source_record_schema_or_geometry_drift": dict(rows=[
                {**rows[0], "attributes": {"OBJECTID": 40, "id3dhp": "1"}},
                *rows[1:]]),
            "source_featuretype_label_drift": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "featuretypelabel": "River"}},
                *rows[1:]]),
            "usgs_service_documentation_link_drift": dict(
                doc_page=b"<html>an unrelated page without the service host</html>"),
        }
        for expected_error, kwargs in cases.items():
            with self.subTest(expected_error):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    capture_root = root / "capture"
                    truncate_short_page = kwargs.pop("truncate_short_page", False)
                    case_rows = kwargs.pop("rows", rows)
                    base_transport = make_responder(case_rows, **kwargs)

                    def responder(request: httpx.Request, _base=base_transport,
                                  _truncate=truncate_short_page) -> httpx.Response:
                        query = parse_qs(urlparse(str(request.url)).query)
                        response = _base.handler(request)
                        if _truncate and "where" in query \
                                and query["where"][0].startswith(
                                    f"{src.POPULATION_WHERE} AND OBJECTID>") \
                                and "returnCountOnly" not in query:
                            payload = json.loads(response.read())
                            if len(payload["features"]) < src.PAGE_SIZE:
                                payload["exceededTransferLimit"] = True
                            return httpx.Response(200, stream=JSONStream(payload),
                                                  headers={"content-type": "application/json"})
                        return response

                    transport = httpx.MockTransport(responder)
                    with fixture_mode(schema_sha, transport, page_size=2):
                        storage = _staging(root)
                        with self.assertRaisesRegex(src.NhdWaterbodyLargeScaleError,
                                                    expected_error):
                            src.capture_nhd_waterbody_large_scale(
                                storage, version="drift-run", capture_directory=str(capture_root))
                    quarantined = sorted(
                        (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
                    self.assertTrue(quarantined, f"no quarantine for {expected_error}")
                    failure = json.loads((quarantined[0] / "metadata" / "failure.json")
                                         .read_bytes())
                    self.assertTrue(failure["reason_code"])

    def test_stream_validate_bounds_are_running_and_memory_bounded(self):
        """Bounds stay four running extrema; per-vertex retention is refused."""
        import tracemalloc

        pages = 8
        features_per_page = 2
        vertices_per_feature = 4000
        count = pages * features_per_page
        oids = list(range(40, 40 + count * 2, 2))[:count]

        def geometry(row: dict) -> dict:
            index = oids.index(row["attributes"]["OBJECTID"])
            x_left = -170.0 + index * 0.1
            x_right = x_left + 1.0
            y_base = 80.0 - index * 0.1 - 1e-6
            y_top = 80.0 - index * 0.1
            tooth = 1e-7
            ring = [[x_left, y_base, 1.0]]
            teeth = vertices_per_feature - 3
            for k in range(1, teeth + 1):
                ring.append([x_left + (x_right - x_left) * k / teeth,
                             y_top - (k % 2) * tooth, 1.0])
            ring.append([x_right, y_base, 1.0])
            ring.append([x_left, y_base, 1.0])
            return {"hasZ": True, "rings": [ring]}

        rows = []
        for oid in oids:
            attrs = dict(_fixture_attrs(FIXTURE_OBJECTIDS[0]))
            attrs["OBJECTID"] = oid
            rows.append({"attributes": attrs, "geometry": None})
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, geometry=geometry)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2,
                              min_objectid=oids[0], max_objectid=oids[-1]), \
                    patch.object(src, "EXPECTED_COUNT", count), \
                    patch.object(src, "EXPECTED_FEATURETYPE_DISTRIBUTION", {3: count}):
                src.fetch_nhd_waterbody_large_scale_pages(capture_root)
                tracemalloc.start()
                try:
                    extracted = src._stream_validate(capture_root, parquet_sink=None)
                finally:
                    _current, peak = tracemalloc.get_traced_memory()
                    tracemalloc.stop()
            last = count - 1
            self.assertEqual(extracted["observed_bounds"], {
                "xmin": -170.0,
                "ymin": 80.0 - last * 0.1 - 1e-6,
                "xmax": -170.0 + last * 0.1 + 1.0,
                "ymax": 80.0,
            })
            accumulation_bytes = count * vertices_per_feature * 3 * 32
            self.assertLess(peak, accumulation_bytes)

    def test_baseline_builder_reports_computed_contract(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport, page_size=2):
                src.capture_nhd_waterbody_large_scale(
                    _staging(root), version="baseline-run", capture_directory=str(capture_root))
                baseline = src.build_nhd_waterbody_large_scale_baseline(str(capture_root))
            self.assertEqual(baseline["source_feature_count"], FIXTURE_COUNT)
            self.assertEqual(baseline["source_min_objectid"], FIXTURE_OBJECTIDS[0])
            self.assertEqual(baseline["source_max_objectid"], FIXTURE_OBJECTIDS[-1])
            self.assertEqual(baseline["source_null_geometry_count"], 0)
            self.assertEqual(baseline["source_refused_ids"], [])
            self.assertEqual(baseline["source_featuretype_distribution"], {3: FIXTURE_COUNT})
            self.assertTrue(baseline["source_record_sha256"])
            self.assertTrue(baseline["source_geometry_sha256"])
            self.assertTrue(baseline["source_ids_sha256"])
            self.assertEqual(baseline["observed_bounds"]["xmin"], -89.96433399428682)


if __name__ == "__main__":
    unittest.main()
